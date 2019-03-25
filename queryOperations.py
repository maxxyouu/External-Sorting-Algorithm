from bitstring import BitArray, ConstBitStream
from constants import *
from utils import *
import sys


def _pageInfo(currentPageBitstream):
    """
    return useful information about the page we desired
        @param currentPageBitstream: the page bitstream reader currently in

    """
    # read the type of pages from offset 0 of the header
    pageType = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:1', 0)

    # get number of cells at offset 3 of the header
    numCells = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:2', 3)

    rightMostPointer = None
    
    if pageType == LEAF_TABLE_BTREE_PAGE_FLAG:

        toPosition = LEAF_BTREE_PAGE_HEADER_SIZE
    
    elif pageType == LEAF_INDEX_BTREE_PAGE_FLAG:
    
        toPosition = LEAF_BTREE_PAGE_HEADER_SIZE
    
    elif pageType == INTERIOR_INDEX_BTREE_PAGE_FLAG:
    
        toPosition = INTERIOR_BTREE_PAGE_HEADER_SIZE
        rightMostPointer = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:4', 8)
    else:
        toPosition = INTERIOR_BTREE_PAGE_HEADER_SIZE
        rightMostPointer = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:4', 8)

    return (pageType, numCells, toPosition, rightMostPointer)

def btreeScan(currentPageBitstream, fpt, ops, pageSize):
    """
    -scan operation for all query and databases
    -this operation only search for the rowid table btrees index btree only for WITHOUT ROWID table
    -all four database only search the clustered btree, no need to search for the index btree

    Scan operation for database (a)(b)(c)

        @param currentPageBitstream: the bitstream reader of a page
        @param fpt: the file pointer of a page
        @param ops: operation function for each record
        @param pageSize: the page size of the db
    """
    pageType, numCells, toPosition, rightMostPointer = _pageInfo(currentPageBitstream)
    readCounts(pageType)

    # read each cell offset within the page from the cell pointer array
    for i in range(0, numCells):

        cellOffset = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:2', i * 2 + toPosition)

        # read the cell from cellOffset according to the pageType
        nxtChildPage, record = parse_cell_content(cellOffset, currentPageBitstream, pageType, fpt, pageSize)

        # recursively traverse
        if nxtChildPage:
            nxtPagebitstream = ConstBitStream(readPage(nxtChildPage, fpt, pageSize))
            if btreeScan(nxtPagebitstream, fpt, ops, pageSize):
                return record

        # in the leaf/interior page, try to find the matching query condition: LAST_NAME
        if ops(record):
            return record

    if rightMostPointer:
        nxtPagebitstream = ConstBitStream(readPage(rightMostPointer, fpt, pageSize))
        if btreeScan(nxtPagebitstream, fpt, ops, pageSize):
            return record
    return None

def tableBtreeEqualitySearch(currentPageBitstream, fpt, rowid, pageSize):
    """
    -equality search in a table btree for (a,a) and (a, b)
        based on the Emp_ID (the indexed column)
    -the other two only need to scan, no performance enhancement
    -only the leaf pages have the data

        @param currentPageBitstream: the bitstream reader of a page
        @param fpt: the file pointer of a page
        @param rowid: look for a record with this rowid
        @param pageSize: the page size of the db
    """
    pageType, numCells, toPosition, rightMostPointer = _pageInfo(currentPageBitstream)
    readCounts(pageType)
    # read each cell offset within the page from the cell pointer array
    for i in range(0, numCells):

        cellOffset = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:2', i * 2 + toPosition)

        # read the cell from cellOffset 
        nxtChildPage, record = parse_cell_content(cellOffset, currentPageBitstream, pageType, fpt, pageSize)
        
        # interior cell cases
        if nxtChildPage:
            
            # get the rowid
            currentRowid, _, _ = readVarintAtOffset(cellOffset + POINTER_SIZE, currentPageBitstream)
            
            if rowid <= currentRowid:
                # keep traverse to the left of this cell
                nxtPagebitstream = ConstBitStream(readPage(nxtChildPage, fpt, pageSize))
                tableBtreeEqualitySearch(nxtPagebitstream, fpt, rowid, pageSize)
                return 
            # when the rowid > currentRowid ==> iterate nxt cell to try
            continue

        '''in the leaf page'''

        # get the rowid of the cell
        _, _, varintBytes = readVarintAtOffset(cellOffset, currentPageBitstream)
        currentRowid, _, _ = readVarintAtOffset(cellOffset + varintBytes, currentPageBitstream)
       
        if currentRowid == rowid:
            # found the record
            print("Full Name: {} {} {}".format(record[FIRST_NAME_INDEX],
                                               record[MIDDLE_NAME_INDEX],
                                               record[LAST_NAME_INDEX]))
            return
        elif currentRowid > rowid: # ==> the rest of cell in this page has greater rowid
            return
         # iterate the nxt cell to check equality

    if rightMostPointer:
        nxtPagebitstream = ConstBitStream(readPage(rightMostPointer, fpt, pageSize))
        tableBtreeEqualitySearch(nxtPagebitstream, fpt, rowid, pageSize)
    else:
        # sanity check for debug
        print("record not found")

def indexBtreeEqualitySearch(currentPageBitstream, fpt, empID, ops, pageSize):
    """
    equality search in the index btree (c,b) and (d, c)
        may need to search through this to get the rowid then
        go back to the table btree to get the actual record

    return the rowid of empID record
        @param currentPageBitstream: the bitstream for the index page; 
                start from the root page of a index tree
        @param fpt: the file pointer of a page
        @param empID: the condition to be search; 
                assume empID is the indexed column; should be sorted in the index btree
        @param ops: the operation to be done for each record
        @param pageSize: the page size of the db
    """

    pageType, numCells, toPosition, rightMostPointer = _pageInfo(currentPageBitstream)
    readCounts(pageType)
    # store the child pointer of the previous cell
    result = -1
    # determine the direction of traversing the cells
    start, end, step = 0, numCells, 1

    # read each cell offset within the page from the cell pointer array
    for i in range(start, end, step):

        cellOffset = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:2', i * 2 + toPosition)
        # read the cell from cellOffset 
        nxtChildPage, record = parse_cell_content(cellOffset, currentPageBitstream, pageType, fpt, pageSize)

        # for debug
        if not record:
            print("This is not a index btree page")
            break

        # if found a matching record ==> no need to search
        result = ops(record)
        if result:
            break

        # need to traversethe pointer of the cell since we want to find the best matching        
        if empID < record[0]:

            # by the sorted properties and in the leaf page ==> empID << record for all cells
            if not nxtChildPage:
                break
            nxtPagebitstream = ConstBitStream(readPage(nxtChildPage, fpt, pageSize))
            return indexBtreeEqualitySearch(nxtPagebitstream, fpt, empID, ops, pageSize)

        # if empID > record[0], iterate the nxt cell; let the cell key get closer to the empID from the left

    if rightMostPointer:
        nxtPagebitstream = ConstBitStream(readPage(rightMostPointer, fpt, pageSize))
        return indexBtreeEqualitySearch(nxtPagebitstream, fpt, empID, ops, pageSize)

    return result

def indexBtreeRangeSearch(currentPageBitstream, fpt, lower, upper, ops, pageSize):
    """
    range search in a index btree for (c,c) and (d, c)
    find the smallest rowid that is bigger than or equal to lowerbound

        @param currentPageBitstream: the bitstream for the index page; 
                start from the root page of a index tree
        @param fpt: the file pointer of a page
        @param lower: lower bound of the range search
        @param upper: upper bound of the range search
        @param ops: the operation to be done for each record
        @param pageSize: the page size of the db 
    """
    result = []

    pageType, numCells, toPosition, rightMostPointer = _pageInfo(currentPageBitstream)
    readCounts(pageType)
    # determine the direction of traversing the cells
    start, end, step = 0, numCells, 1

    # read each cell offset within the page from the cell pointer array
    for i in range(start, end, step):

        cellOffset = bitstreamReadAtOffset(currentPageBitstream, int, 'bytes:2', i * 2 + toPosition)

        # read the cell from cellOffset 
        nxtChildPage, record = parse_cell_content(cellOffset, currentPageBitstream, pageType, fpt, pageSize)

        if nxtChildPage:
            if lower <= record[0] or upper <= record[0]:
                nxtPagebitstream = ConstBitStream(readPage(nxtChildPage, fpt, pageSize))
                result.extend(indexBtreeRangeSearch(nxtPagebitstream, fpt, lower, upper, ops, pageSize))
            elif record[0] < lower or record[0] < upper:
                # try the next cell within the same page
                continue
        
        result.extend(ops(record))

        # since the keys are sorted in ascending order ==> no need to search anymore
        if upper < record[0]:
            break
        # if lower > records ==> iterate the nxt cell in the same page and keep checking
    
    # also look for the extra pointer within each interiro page
    if rightMostPointer:
        nxtPagebitstream = ConstBitStream(readPage(rightMostPointer, fpt, pageSize))
        result.extend(indexBtreeRangeSearch(nxtPagebitstream, fpt, lower, upper, ops, pageSize))
    return result


def lastNameMatching(record):
    """there may be multiple record with the same last name"""
    if record and LAST_NAME == record[LAST_NAME_INDEX]:
        printEmpIDFullname(record)
    return None

def empidMatching(record):
    """there exactly one record with the right EMP_ID"""
    if record and EMP_ID == record[0]:
        printFullnameOnly(record)
        return record
    return None

def empidRangeMatching(record):
    """there are multiple record within the range"""
    if record and EMP_ID_RANGE[0] <= record[0] and record[0] <= EMP_ID_RANGE[1]:
        print("Emp ID: {}, Full Name: {} {} {}".format(record[EMP_ID_INDEX], 
                                                        record[FIRST_NAME_INDEX], 
                                                        record[MIDDLE_NAME_INDEX], 
                                                        record[LAST_NAME_INDEX]))
    return None

def readResetBookkeepings():
    """read all the bookkeeping datastrcutures and reset for the nxt query if there are any"""
    global pageAccessTimer
    global headerPageType
    global dataPageType
    global indexInternalPageType
    global indexLeafPageType

    print("     Header page read counts: {}".format(headerPageType.getReadCounts()))
    print("     Data page read counts: {}".format(dataPageType.getReadCounts()))
    print("     Index internal page read counts: {}".format(indexInternalPageType.getReadCounts()))
    print("     Index leaf page read counts: {}".format(indexLeafPageType.getReadCounts()))
    print("     Average page accessing time in miliseconds: {}ms".format(pageAccessTimer.getAvgPageAccessTime()))

    headerPageType.resetReadCounts()
    dataPageType.resetReadCounts()
    indexInternalPageType.resetReadCounts()
    indexLeafPageType.resetReadCounts()
    pageAccessTimer.resetAll()    

'''The following are the 12 query operations (3 queries for each of the 4 databases)'''

def db_A_Query_A(pageSize):
    """
    DB: Without any index with page size of 4KB
    Ops: Query and print the employee id and full name of anybody whose last name is "Rowe" (this will be a Scan operation)
    """

    print("DB: Without any index with page size of 4KB")
    print("Query and print the employee id and full name of anybody whose last name is \"Rowe\"; (this will be a Scan operation)")

    with open(DB_PATH1, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, lastNameMatching, pageSize)


def db_A_Query_B(pageSize):
    """
    DB: Without any index with page size of 4KB
    Ops Query and print the full name of employee #181162 (this is an Equality search)
    """
    print("DB: Without any index with page size of 4KB")
    print("Query and print the full name of employee #181162 (this is an Equality search)")

    with open(DB_PATH1, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, empidMatching, pageSize)
    
def db_A_Query_C(pageSize):
    """
    DB: Without any index with page size of 4KB
    Ops print the employee id and full name of all employees with "Emp ID" between #171800 and #171899 (This is a Range search)
    """
    print("DB: Without any index with page size of 4KB")
    print("Query and print the employee id and full name of all employees with \"Emp ID\" between #171800 and #171899 (This is a Range search)")

    with open(DB_PATH1, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary,pageSize))
        btreeScan(tablePageBitstream, db_binary, empidRangeMatching, pageSize)
    

def db_B_Query_A(pageSize):
    """
    DB: Without any index but with page size of 16KB bytes
    Ops Query and print the employee id and full name of anybody whose last name is "Rowe" (this will be a Scan operation)
    """

    print("DB: Without any index but with page size of 16KB bytes")
    print("Query and print the employee id and full name of anybody whose last name is \"Rowe\" (this will be a Scan operation)")

    with open(DB_PATH2, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, lastNameMatching, pageSize)

def db_B_Query_B(pageSize):
    """
    DB: Without any index but with page size of 16KB bytes
    Ops Query and print the full name of employee #181162 (this is an Equality search)
    """
    
    print("DB: Without any index but with page size of 16KB bytes")
    print("Ops Query and print the full name of employee #181162 (this is an Equality search)")

    with open(DB_PATH2, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, empidMatching, pageSize)
    
def db_B_Query_C(pageSize):
    """
    DB: Without any index but with page size of 16KB bytes
    Ops print the employee id and full name of all employees with "Emp ID" between #171800 and #171899 (This is a Range search)
    """

    print("DB: Without any index but with page size of 16KB bytes")
    print("Query and print the employee id and full name of all employees with \"Emp ID\" between #171800 and #171899 (This is a Range search)")
    
    with open(DB_PATH2, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, empidRangeMatching, pageSize)
    
def db_C_Query_A(pageSize):
    """
    DB: With primary index on "Emp ID" column (Unclusterd Index) with page size of 4KB
    Ops: Query and print the employee id and full name of anybody whose last name is "Rowe" (this will be a Scan operation)
    """
    print("DB: With primary index on \"Emp ID\" column (Unclusterd Index) with page size of 4KB")
    print("Query and print the employee id and full name of anybody whose last name is \"Rowe\" (this will be a Scan operation)")

    with open(DB_PATH3, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, lastNameMatching, pageSize)

def db_C_Query_B(pageSize):
    """
    DB: With primary index on "Emp ID" column (Unclusterd Index) with page size of 4KB
    Ops Query and print the full name of employee #181162 (this is an Equality search)
    """

    def _findMatchingEmpID_rowidTable(record):
        if EMP_ID == record[0]:
            return record[1]
        return  None

    print("DB: With primary index on \"Emp ID\" column (Unclusterd Index) with page size of 4KB")
    print("Query and print the full name of employee #181162 (this is an Equality search)")

    with open(DB_PATH3, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        indexPagestream = ConstBitStream(readPage(employeeTableRootPage['sqlite_autoindex_Employee_1'], db_binary, pageSize))
        # get the rowid of the record first
        rowid = indexBtreeEqualitySearch(indexPagestream, db_binary, EMP_ID, _findMatchingEmpID_rowidTable, pageSize)
        # find the record corresponding to that rowid
        tableBtreeEqualitySearch(tablePageBitstream, db_binary, rowid, pageSize)

def db_C_Query_C(pageSize):
    """
    DB: With primary index on "Emp ID" column (Unclusterd Index) with page size of 4KB
    Ops print the employee id and full name of all employees with "Emp ID" between #171800 and #171899 (This is a Range search)

    """
    def _rangeSearchIndex_regular(record):
        if record and EMP_ID_RANGE[0] <= record[0] and record[0] <= EMP_ID_RANGE[1]:
            return [record[1]]
        return []
    
    print("DB: With primary index on \"Emp ID\" column (Unclusterd Index) with page size of 4KB")
    print("Query and print the employee id and full name of all employees with \"Emp ID\" between #171800 and #171899 (This is a Range search)")
    
    
    with open(DB_PATH3, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        indexPagestream = ConstBitStream(readPage(employeeTableRootPage['sqlite_autoindex_Employee_1'], db_binary, pageSize))
        # use index to find the rowid of the corresponding EMP_ID
        rowids = indexBtreeRangeSearch(indexPagestream, db_binary, EMP_ID_RANGE[0], EMP_ID_RANGE[1], _rangeSearchIndex_regular, pageSize)
        # use the rowid to find the record in the table btree one at a time
        for rowid in rowids:
            tableBtreeEqualitySearch(tablePageBitstream, db_binary, rowid, pageSize)

def db_D_Query_A(pageSize):
    """
    DB: With primary index on "Emp ID" column but defined as clustered (use CREATE INDEX WITHOUT ROWID) with page size of 4KB
    ops: Query and print the employee id and full name of anybody whose last name is "Rowe" (this will be a Scan operation)
    
    """
    print("DB: With primary index on \"Emp ID\" column but defined as clustered with page size of 4KB")
    print("Query and print the employee id and full name of anybody whose last name is \"Rowe\" (this will be a Scan operation)")

    with open(DB_PATH4, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        btreeScan(tablePageBitstream, db_binary, lastNameMatching, pageSize)
    # find a record with last name where the index btree is sorted in EMP_ID ==> use scan operation

def db_D_Query_B(pageSize):
    """
    DB: With primary index on "Emp ID" column but defined as clustered (use CREATE INDEX WITHOUT ROWID) with page size of 4KB
    ops: Query and print the full name of employee #181162 (this is an Equality search)
    
    """
    print("DB: With primary index on \"Emp ID\" column but defined as clustered with page size of 4KB")
    print("Query and print the full name of employee #181162 (this is an Equality search)")

    def _findMatchingEmpID_withoutrowid(record):
        if EMP_ID == record[0]:
            printFullnameOnly(record)
            return record
        return None

    with open(DB_PATH4, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))        
        indexBtreeEqualitySearch(tablePageBitstream, db_binary, EMP_ID, _findMatchingEmpID_withoutrowid, pageSize)
    

def db_D_Query_C(pageSize):
    """
    DB: With primary index on "Emp ID" column but defined as clustered (use CREATE INDEX WITHOUT ROWID) with page size of 4KB
    ops: Query and print the employee id and full name of all employees with "Emp ID" between #171800 and #171899 (This is a Range search)
    """
    print("DB: With primary index on \"Emp ID\" column but defined as clustered with page size of 4KB")
    print("Query and print the employee id and full name of all employees with \"Emp ID\" between #171800 and #171899 (This is a Range search)")
    
    def _rangeSearchIndex_withoutrowid(record):
        if record and EMP_ID_RANGE[0] <= record[0] and record[0] <= EMP_ID_RANGE[1]:
            printEmpIDFullname(record)
            return [record]
        return []

    with open(DB_PATH4, "rb") as db_binary:
        employeeTableRootPage = parseRootPage(db_binary, pageSize)
        tablePageBitstream = ConstBitStream(readPage(employeeTableRootPage['Employee'], db_binary, pageSize))
        indexBtreeRangeSearch(tablePageBitstream, db_binary, EMP_ID_RANGE[0], EMP_ID_RANGE[1], _rangeSearchIndex_withoutrowid, pageSize)

if __name__ == "__main__":
    # redirect all the print outputs to a file
    sys.stdout = open('./output.txt', 'w')

    db_A_Query_A(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_A_Query_B(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_A_Query_C(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_B_Query_A(PAGE_SIZE_16K)
    readResetBookkeepings()
    print("")
    db_B_Query_B(PAGE_SIZE_16K)
    readResetBookkeepings()
    print("")
    db_B_Query_C(PAGE_SIZE_16K)
    readResetBookkeepings()
    print("")
    db_C_Query_A(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_C_Query_B(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_C_Query_C(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_D_Query_A(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_D_Query_B(PAGE_SIZE_4K)
    readResetBookkeepings()
    print("")
    db_D_Query_C(PAGE_SIZE_4K)
    readResetBookkeepings()