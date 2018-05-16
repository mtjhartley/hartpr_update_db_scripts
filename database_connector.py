import pyodbc
import struct
import uuid
import sys

CONNECTION_STRING = (r'Driver={ODBC Driver 17 for SQL Server};'
r'Server=(localdb)\MSSQLLocalDB;'
r'Database=HartPRDB;'
r'Trusted_Connection=yes;'
r'QuotedID=NO;')

def create_connection(connection_string):
    print ("Attempting to establish connection to database...")
    return pyodbc.connect(connection_string)

def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)
