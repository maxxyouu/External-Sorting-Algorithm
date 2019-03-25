#!/usr/bin/env python

import sqlite3
import csv
from constants import *

# Answer for question 2.3

# determine column size of the csv file
def build_db_abstraction(csv_file_path):
    """
    populate the datastructure that contains all the data
    @return a list of dictionary that contains all data from each columns,
    a list of dictionary that contains the max size value for each column
    """
    # column_dict : store a list of values for each column
    # max_attribute_length:  store the max value size for each column
    column_dict, max_attribute_length, unique_emp = {}, {}, {}

    with open(csv_file_path, 'r') as data_file:
        iterator = csv.reader(data_file, delimiter=',')

        # store a list of attribute names in the csv
        col_names = []

        # initialize a dictionary for each column to store a list of value
        for name in iterator.__next__():
            column_dict[name] = []
            # default to the length of the column name to begin with, update later on if necessary
            max_attribute_length[name] = 0
            col_names.append(name)
        
        # populate the data to each column
        for row_data in iterator:
            
            # populate the data for each colmn first
            for index, col_data in enumerate(row_data):
                
                # add the employee id into the dictionary
                if index == 0:
                    unique_emp.setdefault(col_data, 0)
                    
                # get the key(the column name) that corresponds to its dictionary
                column_name = col_names[index]

                # update the list of values for that particular column
                column_dict[column_name].append(col_data)

                # bookkeeping the length
                size, new_size = max_attribute_length[column_name], len(col_data)
                if new_size > size:
                    max_attribute_length[column_name] = new_size
            
    return column_dict, max_attribute_length, col_names, unique_emp

def cleaned_version(name):
    """
    make a clean versino of the name so that no error occurs in the CREATE TABLE statement
    """

    result = '_'.join(name.split('.'))
    result = '_'.join(result.split("'"))
    result = '_'.join(result.split(" "))
    result = '_'.join(result.split("("))
    result = '_'.join(result.split(")"))
    result = '_'.join(result.split("%"))
    return result

# create table
def create_db(with_index, clustered, page_size, db_path, table_name, col_names, attr_size):
    """
    create a table definition according to parameter values

        @param with_index: create a index file for the db
        @param unclustered: whether to create a clustered db or not
        @param page_size: in bytes and at 2 boundary
        @param db_path: path to the database file
        @param table_name: the wanted table name
        @param col_names: a list of column names in the csv file
        @param attr_size: a dictionary of max value size for each column
    """
    # http://www.sqlitetutorial.net/sqlite-python/creating-database/
    
    connection = sqlite3.connect(db_path)

    # for execute sqlte commands
    cursor = connection.cursor()

    # setting page size
    cursor.execute('PRAGMA page_size={};'.format(page_size))
    connection.commit()

    # build skeleton sql string
    table_definition = "CREATE TABLE Employee("

    cleaned_col_names = []

    for name in col_names:

        cleaned_name = cleaned_version(name)
        cleaned_col_names.append(cleaned_col_names)

        # column name definition
        table_definition += cleaned_name

        # column data typ definition  
        if(name != "Emp ID"):
            data_type = " CHAR({})".format(attr_size[name])
        else:
            # A WITHOUT ROWID table is a table that uses a Clustered Index as the primary key.
            data_type = " INT"

        table_definition += data_type

        # in case of a clustered index on Emp_ID
        # if clustered and name == "Emp ID":
        #     table_definition += " PRIMARY KEY"
        if with_index and name == "Emp ID":
            table_definition += " PRIMARY KEY"    

        # delimiter to separte the column definition
        table_definition += ","
    table_definition = table_definition[:-1] + ")"

    # handle cluster
    if clustered:
        table_definition += " WITHOUT ROWID"
    table_definition += ";"

    cursor.execute(table_definition)
    connection.commit()
    
    connection.close()


# insert data row by row into the database
def populate_data_to_db(col_names, col_dicts, db_path, table, unique_employees):
    """
    insert record into the database

        @param col_names: a list of original column names
        @param col_dicts: {col_name: [list of values in order of the csv file]}
        @param db_path: the database path that want to be populated with data
        @param table: the table name
        @param unique_employees: all the unique employee
    """
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
  
    total_rows = len(col_dicts["Emp ID"])

    # INSERT INTO prod_mast VALUES(3,'Pakora', 48, 'OK');
    for row_index in range(0, total_rows):

        insert_definition = 'INSERT INTO {} VALUES('.format(table)

        # build up the row values to be inserted
        placeholder = ",".join(['?'] * len(col_names))
        insert_definition += placeholder
        insert_definition += ');'
        
        # get the employee id
        emp_id = col_dicts["Emp ID"][row_index]

        # if the employee has been seen ==> skip to the nxt record
        if unique_employees[emp_id] == 1:
            continue
        unique_employees[emp_id] = 1

        # get the rest of the values of the record
        values = [col_dicts[col_name][row_index] for col_name in col_names]

        # execute the sql statement each time
        cursor.execute(insert_definition, tuple(values))

    # commit the changes each time
    connection.commit()
    connection.close()
    
if __name__ == "__main__":

    # create db four times with different settings
    table_name = "Employee"
    db1_path = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\db1.db"
    db2_path = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\db2.db"
    db3_path = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\db3.db"
    db4_path = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\db4.db"
    csv_path = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\data.csv"
    dbs = [db1_path, db2_path, db3_path, db4_path]

    # store all the records inside python data structures
    col_dict, col_size_dict, origin_col_names, unique_emp = build_db_abstraction(csv_path)

    # create database for each
    create_db(False, False, PAGE_SIZE_4K, db1_path, table_name, origin_col_names, col_size_dict)
    create_db(False, False, PAGE_SIZE_16K, db2_path, table_name, origin_col_names, col_size_dict)
    create_db(True, False, PAGE_SIZE_4K, db3_path, table_name, origin_col_names, col_size_dict)
    create_db(True, True, PAGE_SIZE_4K, db4_path, table_name, origin_col_names, col_size_dict)

    # populate data for the database
    for db in dbs:
        populate_data_to_db(origin_col_names, col_dict, db, table_name, unique_emp)
        # reset all value inside the dictionary
        for emp in unique_emp:
            unique_emp[emp] = 0
