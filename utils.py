from constants import *
from bitstring import BitArray, ConstBitStream
from timeit import default_timer as time

def readPage(pageNum, fpt, pageSize):
    """record time required to retrieve the page

    return a page of bytes objects
    -the function is being timed
    
    - store each elapsed time for every page type
    - store number of time certain page type is read
    
        @param pageNum: the absolute offset of the page  
        @param fpt: the file pointer of the db file
    """
    fpt.seek(pageSize * (pageNum - 1), 0)

    startTime = time()
    page = fpt.read(pageSize)
    elapsedTime = (time() - startTime) * 1000
    
    # accumulate the page access time for each query operation
    pageAccessTimer.accumulatePageAccessTime(elapsedTime)

    return page

def parseRootPage(fpt, pageSize):
    """
    parse necessary information about the database file and
    return a dictionary of table/index names and its corresponding root page

        @param fpt: the file pointer of the database file
        @param pageSize: the page size of the database
    """
    # read in the whole root page into memory
    bitstream = ConstBitStream(readPage(1, fpt, pageSize))
    readCounts(-1)

    numPages = bitstreamReadAtOffset(bitstream, int, 'bytes:4', 28)
    # skip th db header and thus locate btree page header from the beginning
    bitstream.bytepos = DATABASE_FILE_HEADER_SIZE

    # get the type of page from offset relative to DATABASE_FILE_HEADER_SIZE and read one byte only; reset back to offset DATABASE_FILE_HEADER_SIZE
    pageFlag = bitstreamReadAtOffset(bitstream, int, 'bytes:1', DATABASE_FILE_HEADER_SIZE)
    # read number of cells inside the rootpage at offset 3 relative to the begining of page header and read two bytes; reset back to offset DATABASE_FILE_HEADER_SIZE
    numTables = bitstreamReadAtOffset(bitstream, int, 'bytes:2', DATABASE_FILE_HEADER_SIZE + 3)
    tables = {}

    # jump to the cell pointer array relative to offset DATABASE_FILE_HEADER_SIZE
    for i in range(0, numTables):
        # read two bytes at a time as a pointer from the cell pointer array relative to the beginning of the array; reset back to the beginning of cell pointer array each iteration
        cellPosition = bitstreamReadAtOffset(bitstream, int, 'bytes:2', LEAF_BTREE_PAGE_HEADER_SIZE + DATABASE_FILE_HEADER_SIZE + i * 2)

        # goes to the sqlite master table and find out the root page number; sqlite_master table is a table btree page
        _,record = parse_cell_content(cellPosition, bitstream, pageFlag, fpt, pageSize, isSqliteMaster=True)
        
        # store the table/index name and its root page
        tables.setdefault(record[1], record[-2])

    # return the root page num
    return tables

def parse_cell_content(cellOffset, bitstream, pageFlag, fpt, pageSize, isSqliteMaster=False):
    """
    parse the cell contents into a tuple like (child pointer if exists, record itself)

        @param cellOffset: the cell offset within the page
        @param bitstream: a ConstBitStream page that the cell is in
        @param pageFlag: the type of page
        @param fpt: the file pointer to the db file
        @param pageSize: the page size of the database
        @isSqliteMaster: whether the page being search is a sqlite master tables
    """
    # seek to the begining of the cell position and construct a bit reader to parse the record
    originalPos = bitstream.bytepos

    bitstreamSeek(bitstream, cellOffset, 0)

    leftChildPointer = None
    record = None
    if pageFlag == INTERIROR_TABLE_BTREE_PAGE_FLAG:

        # get the pointer at the begining of the cell relative to absCellPos
        leftChildPointer = bitstreamReadAtOffset(bitstream, int, 'bytes:{}'.format(POINTER_SIZE), cellOffset)
        
    elif pageFlag == LEAF_TABLE_BTREE_PAGE_FLAG:

        # read the first varint: total payload size within the cell in bytes
        payloadSize, _,numBytes = readVarintAtOffset(cellOffset, bitstream)

        # relative to the position above, skip the rowid varint, which is the record payload position
        _, recordPayloadOffset, _ = readVarintAtOffset(cellOffset + numBytes, bitstream)

        # parse the record
        _, _, record = parseRecord(recordPayloadOffset, bitstream, payloadSize, pageFlag, fpt, pageSize, isSqliteMaster)

    elif pageFlag == LEAF_INDEX_BTREE_PAGE_FLAG:

        # read the first varint: the key payload size, used for overflow page
        keyPayloadSize, keyPayloadAbsBytePos, _ = readVarintAtOffset(cellOffset, bitstream)

        # parse the record
        _, _, record = parseRecord(keyPayloadAbsBytePos, bitstream, keyPayloadSize, pageFlag, fpt, pageSize, isSqliteMaster)

    elif pageFlag == INTERIOR_INDEX_BTREE_PAGE_FLAG:
        
        leftChildPointer = bitstreamReadAtOffset(bitstream, int, 'bytes:{}'.format(POINTER_SIZE), cellOffset)

        # get the total number of bytes in keyPayload including the keypayload header
        keyPayloadSize, keyPayloadAbsBytePos, _ = readVarintAtOffset(cellOffset + POINTER_SIZE, bitstream)

        # parse the keypayload itself including the payload header
        _, _, record = parseRecord(keyPayloadAbsBytePos, bitstream, keyPayloadSize, pageFlag, fpt, pageSize, isSqliteMaster)

    else:
        print("Invalid page type!")

    # reset back to the original position
    bitstreamSeek(bitstream, originalPos, 0)

    return (leftChildPointer, record)


def absPageOffset(pageNum, pageSize):
    """
    get the absolute page offset (relative to the beginning of the file) of the page num

        @param: pageNum: the page number
        @pageSize: the page size of the database
    """
    return (pageNum - 1) * pageSize

def parseRecord(recordOffset, bitstream, totalRecordSize, pageType, fpt, pageSize, isSqliteMaster=False):
    """
    return a triple (payloadHeaderSize, serialMapper, record)

    note: record is a list of record in the order of payload
    assume the bitstream points to the start of the cell

        @param recordOffset: the record offset of the beginning of the record (the payload header)
        @param bitstream: the stream reader of the current page
        @param totalRecordSize: the total record size including the size of record header and overflow
        @param pageType: the type of page the record is in
        @param fpt: the file pointer of the database
        @param pageSize: the page size of each page in the database
        @isSqliteMaster: whether the page being search is a sqlite master tables
    """
    originalPos = bitstream.bytepos

    # read the first varint at the beginning of the record: payload header size (including varint itself), may takes up more than one bytes
    payloadHeaderSize, _, headerVarintSize = readVarintAtOffset(recordOffset, bitstream)

    # read the column size accroding tot he varint
    serialMapper = []

    def _serialToByteSize(serialType):
        """
        return the the tuple of (serialType, size) refers to the table
            @param serialType: the serial type in the record header
        """
        if serialType >= 0 and serialType <= 4:
            size = serialType
        elif serialType == 5 or serialType == 7:
            size = serialType + 1
        elif serialType == 6:
            size = 8
        elif serialType in [0, 8 ,9 ,12, 13]:
            size = 0
        elif serialType >= 12 and serialType % 2 == 0:
            size = (serialType - 12) / 2
        elif serialType >= 13 and serialType % 2 == 1:
            size = (serialType - 13) / 2
        
        return (serialType, size)

    # there are payloadHeaderSize - headerVarintSize remaining bytes for serial types in the header and parse the serial num in the record
    headerRemainingBytes = payloadHeaderSize - headerVarintSize
    currentReadOffset = recordOffset + headerVarintSize
    while headerRemainingBytes > 0:
        
        serialNum, recordBodyOffset, usedBytes = readVarintAtOffset(currentReadOffset, bitstream)
        # store the corresponding value of the serialNum
        serialMapper.append(_serialToByteSize(serialNum))
        headerRemainingBytes -= usedBytes
        
        # update next offset position to be read
        currentReadOffset += usedBytes
    record = []

    # get the payload size within the cell and in the overflow pages, including the payload header
    inCellPayload, overflowPayload = determineinCellPayload(pageType, totalRecordSize, pageSize)
    
    # offset to the payload body position
    bitstreamSeek(bitstream, recordBodyOffset, 0)
    
    # parse the record body into a list in which each index represents a column value in the row record
    recordBodySize = inCellPayload - payloadHeaderSize
    record = parseRecordBody(recordBodySize, overflowPayload, serialMapper, bitstream, fpt, pageType, isSqliteMaster)

    # reset back to original position
    bitstreamSeek(bitstream, originalPos, 0)

    return payloadHeaderSize, serialMapper, record

def parseRecordBody(recordBodySize, overflowPayload, serialMapper, bitstream, fpt, pageSize, pageType, isSqliteMaster=False):
    """
    parse the record *body* into a list

        @param: recordBodySize: the record body size in bytes
        @param overflowPayload: size of overflow payload of the record
        @param serialMapper: mapper of serial code : decimal
        @param bitstream: the stream of bits of the page that the record is in
        @param fpt: the file pointer of the db
        @param pageSize: the page size of each page in the db
        @param pageType: the page type of the record is in
        @isSqliteMaster: whether the page being search is a sqlite master tables
    """
    _record = []

    # read the whole concatenated record body bytes in the payload body and the overflow pages
    recordBodyStream = ConstBitStream(bitstream.read('bytes:{}'.format(recordBodySize)))

    if overflowPayload > 0:
       
        # read the last four bytes at offsets recordBodySize relative to the recordBodyAbsOffset ==> read the last four bytes
        rootOverflowPagePointer = bitstreamReadAtOffset(bitstream, int, "bytes:{}".format(POINTER_SIZE), recordBodySize)
    
        # get the bitstream of the root overflow page at PAGE_SIZE at a time
        overflowBitstream = ConstBitStream(readPage(absPageOffset(rootOverflowPagePointer, pageSize), fpt, pageSize))
        readCounts(pageType)

        # traverse the chain of overflow pages
        while overflowPayload != 0:
            
            # get the next overflow page from the beginning of the overflow page (the first four bytes); resetLocation is false
            nxtOverflowPage = bitstreamReadAtOffset(overflowBitstream, int, 'bytes:{}'.format(POINTER_SIZE), 0)
            
            # seek to the data region of the stream/page
            bitstreamSeek(overflowBitstream, POINTER_SIZE, 0)

            # read the rest of the data inside the current overflow page
            overflowDataWithinPage = min(overflowPayload, pageSize - POINTER_SIZE - RESERVED_PER_PAGE)
            recordBodyStream += ConstBitStream(overflowBitstream.read('bytes:{}'.format(overflowDataWithinPage)))
            overflowPayload -= overflowDataWithinPage
            
            # get the next overflow page bitstream if available
            if nxtOverflowPage > 0:
                overflowBitstream = ConstBitStream(readPage(absPageOffset(nxtOverflowPage, pageSize), fpt, pageSize))
                readCounts(pageType)


    # traverse the recordBodyStream object to parse the record
    for i, (serialType, attributeLength) in enumerate(serialMapper):
        attributeLength = int(attributeLength)
        formatStr = "bytes:{}".format(attributeLength)

        # handle special case of serial type of 8 and 9 ==> value is integer 0 and 1
        if serialType == 8:
            _record.append(0)
        elif serialType == 9:
            _record.append(1)
        elif (i == 0 and not isSqliteMaster) or (i == 3 and isSqliteMaster) or (serialType > 0 and serialType < 8):
            formatStr = "uintbe:{}".format(attributeLength * 8)
            _record.append(recordBodyStream.read(formatStr))
        else:
            _record.append(recordBodyStream.read(formatStr).decode('utf-8'))

    return _record

def readVarintAtOffset(offset, bitStream, streamFormat="uintbe:8"):
    """
    decode the varint at offset inside the bitStream

    return (varint.unit, varint ending position, how many bytes the varint occupies)

        @param offset: starting location to read the offset in the bitStream
        @param bitStream: a bitstream file pointer to read bits
        @param streamFormat: determine the conversion of bits to type i wnat
    """
    origin = bitStream.bytepos

    # go the correct byte position to read the varint
    bitStream.bytepos = offset

    # make the reader read one byte of big endian integer at a time
    varint = BitArray(bytes([bitStream.read(streamFormat)]))
    counter = 1
    temp = varint

    # while the most significant bit is true = 1
    while temp[0]:

        # advance the file pointer one byte at a time by reading
        temp = BitArray(bytes([bitStream.read(streamFormat)]))

        # detect the maximum size of the varint
        if counter == 9:
            varint.append(temp)
            break

        # only use the lower-order 7 bits
        varint += temp[1:]
        counter += 1

    newOffset = bitStream.bytepos
    
    # always reset back to the orignal position
    bitStream.bytepos = origin

    # return the integer version of varint, the absolute byte position after read the varint, and the number of bytes that the varint occpuies
    return varint[1:].uint, newOffset, counter

def determineinCellPayload(pageType, P, pageSize):
    """
    return (in record payload, overflow payload)
        @param pageType: the page type to find the recordsize threshold
        @param P: the payload size of the record from the cell header
        @param pageSize: the page size the program is currently in
    """

    U = pageSize - RESERVED_PER_PAGE

    if pageType == INTERIROR_TABLE_BTREE_PAGE_FLAG or pageType == LEAF_TABLE_BTREE_PAGE_FLAG:
        X = U - 35
    else:
        X = ((U - 12)* 64 / 255) - 23

    M =  ((U - 12) * 32 / 255) - 23

    K = M + (( P - M) % ( U - 4))

    # return (in payload size, bytes store in the overflow page)
    if P <= X:
        return (P , 0)
    if P > X and K <= X:
        return (K, P - K)
    if P > X and K > X:
        return (M, P - M)

def bitstreamSeek(bitstream, offset, relative):
    """ 
    seek the bitstream pointer to certain offset of a page
        @param bitstream: the stream reader
        @param offset: the desired offset to be in
        @param relative: relative to current position or the beginnning of the stream pointer
    """
    if relative == 0:
        bitstream.bytepos = offset
    else:
        bitstream.bytepos += offset

def converstionFromBytes(_bytes, convertTo):
    """
    convert the byte into a desired type (int or string)
        @param _bytes: the bytes that want to be converted to other data type
        @param convertTo: the desired format to be converted to
    """
    if isinstance(convertTo, str):
        return _bytes.decode('ASCII')
    return int.from_bytes(_bytes, byteorder="big")

def bitstreamRead(bitstream, convertion, num):
    """
    read num of *bytes* from tbe bitstream, always relative to the previous location of bitstream

        @param bitstream: the bitstream object
        @param convertion: type of conversion we want
        @param num: the number of bytes to be read from the bitstream
    """
    results = bitstream.read("bytes:{}".format(num))
    return converstionFromBytes(results, convertion)

def bitstreamReadAtOffset(bitstream, _type, readFormat, absoluteOffset):
    """
    read bytes_read number of byte at a time at at_offset using the bitstream

    the absolute offset is relative to each page

        @parm bitstream: the disk page to be read
        @param _type: convertion to type
        @param readFormat: type and number of bytes to be read
    """
    original = bitstream.bytepos
    bitstreamSeek(bitstream, absoluteOffset, 0)

    _bytes = bitstream.read(readFormat)

    bitstream.bytepos = original

    return converstionFromBytes(_bytes, _type)

def printEmpIDFullname(record):
    """
    print the employee id and full name of the record
    """
    print("Emp ID: {}, Full Name: {} {} {}".format(record[EMP_ID_INDEX], 
                                                    record[FIRST_NAME_INDEX], 
                                                    record[MIDDLE_NAME_INDEX], 
                                                    record[LAST_NAME_INDEX]))
                                            
def printFullnameOnly(record):
    """
    print the full name of the record
    """
    print("Full Name: {} {} {}".format( record[FIRST_NAME_INDEX], 
                                        record[MIDDLE_NAME_INDEX], 
                                        record[LAST_NAME_INDEX]))


def readCounts(pageFlag):
    """
    increment the readcounts according to the page type
        @param pageFlag: the page type to determine the readcounts of the object
    """
    # a special case for root page read counts
    if pageFlag < 0:
        headerPageType.incrementReadCounts()
    
    # TODO: check if need to count for interiro table btree page
    if pageFlag == INTERIROR_TABLE_BTREE_PAGE_FLAG or pageFlag == LEAF_TABLE_BTREE_PAGE_FLAG:
        dataPageType.incrementReadCounts()
    elif pageFlag == LEAF_INDEX_BTREE_PAGE_FLAG:
        indexLeafPageType.incrementReadCounts()
    elif pageFlag == INTERIOR_INDEX_BTREE_PAGE_FLAG:
        indexInternalPageType.incrementReadCounts()

# bookkeeping the number of reads per page type
class page:
    def __init__(self):
        self.readCounts = 0

    def incrementReadCounts(self):
        self.readCounts += 1
    
    def resetReadCounts(self):
        self.readCounts = 0

    def getReadCounts(self):
        return self.readCounts

class pageAccesingTime:
    def __init__(self):
        self.pageAccesingTime = 0
        self.pagesRead = 0
    
    def getPageAccessTime(self):
        return self.pageAccesingTime

    def accumulatePageAccessTime(self, time):
        self.pageAccesingTime += time
        self.pagesRead += 1
    
    def getAvgPageAccessTime(self):
        return self.pageAccesingTime / self.pagesRead
    
    def resetAll(self):
        self.pageAccesingTime = 0
        self.pagesRead = 0

headerPageType = page()
dataPageType = page()
indexInternalPageType = page()
indexLeafPageType = page()
# store all the page access time into this list and perform the average
pageAccessTimer = pageAccesingTime()
