
from tran_total_states_energy import (transform_data, transform_data2)
import sys

import psycopg2

from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import numpy as np

from timeit import default_timer as timer

edf = transform_data()
edf2 = transform_data2()

params_dic = {
    "host"      : "localhost",
    "user"      : "postgres",
    "password"  : "postgres",
    "port"      : "5432"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
conn = connect(params_dic)

#install psycopg2-binary for MacOs if you don't have it
    #!pip install psycopg2-binary

# Define a function that handles and parses psycopg2 exceptions
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()    
    # get the line number when exception occured
    line_n = traceback.tb_lineno    
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)    
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def create_table(cursor):
    try:
        # 
        cursor.execute("DROP TABLE IF EXISTS energy;")
        sql = '''CREATE TABLE energy(
        id INT NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        state VARCHAR NOT NULL, 
        producer VARCHAR NOT NULL,
        season VARCHAR NOT NULL,
        source VARCHAR NOT NULL, 
        generated FLOAT NOT NULL,
        CONSTRAINT pk_energy PRIMARY KEY (
        id)
        )'''
        # Creating a table
        cursor.execute(sql);
        print("energy table is created successfully...............")  
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None
        
def create_table2(cursor):
    try:
        # 
        cursor.execute("DROP TABLE IF EXISTS energy_renew;")
        sql = '''CREATE TABLE energy_renew(
        ids INT NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        state VARCHAR NOT NULL, 
        producer VARCHAR NOT NULL,
        season VARCHAR NOT NULL,
        source VARCHAR NOT NULL, 
        generated FLOAT NOT NULL,
        CONSTRAINT pk_energy_renew PRIMARY KEY (
        ids)
        )'''
        # Creating a table
        cursor.execute(sql);
        print("energy table 2 is created successfully...............")  
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None

# Define function using copy_from() with StringIO to insert the dataframe
def copy_from_dataFile_StringIO(conn, datafrm, table):
    
    #save dataframe to an in memory buffer
    buffer = StringIO()
    datafrm.to_csv(buffer, header=False, index = False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        print("Data inserted using copy_from_datafile_StringIO() successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()

conn = connect(params_dic)

# We set autocommit=True so every command we execute will produce results immediately.
conn.autocommit = True
cursor = conn.cursor()

create_table(cursor)
copy_from_dataFile_StringIO(conn, edf, 'energy')

create_table2(cursor)
copy_from_dataFile_StringIO(conn, edf2, 'energy_renew')


def query_data():

    conn.autocommit = True
    cursor = conn.cursor()
  
    sql = '''SELECT source, season, SUM(generated)
            FROM energy
            GROUP BY source, season
                ;''' 
  
    cursor.execute(sql)
    results = cursor.fetchall()
    df = pd.DataFrame (results, columns = ['Season', 'Source', 'Total Generated(MWh)'])
    conn.commit()
    return df

def query_data2():

    conn.autocommit = True
    cursor = conn.cursor()
  
    sql = '''SELECT source, season, SUM(generated)
            FROM energy_renew
            GROUP BY source, season
                ;''' 
  
    cursor.execute(sql)
    results = cursor.fetchall()
    df = pd.DataFrame (results, columns = ['Season', 'Source', 'Total Generated(MWh)'])
    conn.commit()
    return df
