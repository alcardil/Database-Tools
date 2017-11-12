from sqlalchemy import create_engine, Column, Integer, Text, Date, text
from sqlalchemy.orm import sessionmaker
import sqlite3
import os
import csv

class SqlColumn(Object):
	'''
	Convenience class for working with columns. Info is immutable so that columns aren't altered
	'''
	def __init__(self, name='', data_type='', primary=False):
		self.name = name
		self.data_type = data_type
		self.primary = primary


class SqlTable(Object):
	'''
	Convenience class for working with tables. Columns is a dict where the key is
	the column name and the data is the column's info
	ie. columns[col.name] = col
	'''
	def __init__(self, name='', columns={}):
		self.name = name
		self.columns = columns

class SqlDatabase(Object):
	'''
	Convenience class for working with databases
	'''
	def __init__(self, db_type='', name='', connection_string=''):
		self.db_type = db_type
		self.name = name
		self.connection_string = connection_string

def generate_mysql_string(username='', password='', servername='', dbname=''):
	'''
	Generates a mysql connection string
	'''
	connection_string = "mysql+pymysql://" + username + ":" + password + "@" + servername + "/" + dbname
	return connection_string

def generate_sqlite_string(path=''):
	'''
	Generates a sqlite connection string
	'''
	return "sqlite:///" + path

def generate_postgres_string(username='', password='', servername='', port='5432', dbname=''):
	'''
	Generates a pgsql connection string
	'''
	connection_string = "postgresql://" + username + ":" + password + "@" + servername + ":" + port + "/" + dbname
	return connection_string

def create_db(connection_type, connection_string): #refactor now that connection_string is implemented
	'''
	creates a new database, if the destination is an sqlite database
	it will generate a test table since sqlite databases can not be empty
	'''
	if connection_type == "postgresql":
		connection_string = 'postgresql://' + connection_string
		raise ValueError('A very specific bad thing happened: pgsql not implemented yet')
	elif connection_type == 'sqlite':
		conn = sqlite3.connect(connection_string.split(":///")[1])
		conn.close()
		engine = create_engine(connection_string, echo=True)
		session = sessionmaker()
		session.configure(bind=engine)
		s = session()
		s.execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
		s.close

	elif connection_type == 'mysql':
		print("Enter database name: ")
		dbname = input()
		engine = create_engine(connection_string, echo=True)
		engine.execute("CREATE DATABASE " + dbname) #create db
		engine.execute("USE " + dbname)
		session = sessionmaker()
		session.configure(bind=engine)

def create_table(tablename, columns): #probably need to add session/db object to inputs
	'''
	creates a table in a database
	'''
	primary = ''
	sql_text = 'CREATE TABLE ' + tablename + ' (\n'
	for index, column in enumerate(columns):
		if column.primary:
			primary = column.name
		if index == 0:
			sql_text += "\t" + column.name + " " + column.data_type
		else:
			sql_text += ",\n\t" + column.name + " " + column.data_type
	sql_text += ",\n\t PRIMARY KEY (" + primary + "))"
	sql = text(sql_text)
	session = sessionmaker()
	session.configure(bind=engine)
	s = session()
	s.execute(sql)
	s.close()

def add_column(tablename, column): #probably need to add session/db object to inputs
	'''
	adds a column to a table
	'''
	session = sessionmaker()
	session.configure(bind=engine)
	s = session()
	s.execute("ALTER TABLE " + tablename + " ADD COLUMN " + column.name + " {" + column.data_type + "}")
	s.close()

def get_pgsql_schema(connection_string=''):
	'''
	get the schema from a pgsql database
	'''
    connection = sqlalchemy.create_engine(connection_string, client_encoding='utf8')
    meta = sqlalchemy.MetaData(bind=connection, reflect=True)
    tables = connection.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print(tables)
    schema = {}
    for table in tables:
        print("\n***" + str(table[0]) + "***\n")
        columns = connection.execute("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '" + str(table[0]) + "';")
        schema[table[0]] = {}
        for column in columns:
            schema[table[0]][column[0]] = []
            print(str(column))

def get_csv_schema(path):
	'''
	gets the schema from a csv by examining the first row
	'''
    sql_columns = []
    with open(path, 'rt') as f:
        reader = csv.reader(f)
        columns = next(reader)
        sql_columns = []
        for column in columns:
            sql_columns.append(str(column))
            print(str(column))
    print(sql_columns)

def get_sqlite_schema(path):
	'''
	gets the schema from a sqlite database
	'''
    url = "sqlite:///" + path
    engine = sqlalchemy.create_engine(url)
    connection = engine.connect()
    schema = {}
    tables = connection.execute("select name from sqlite_master where type='table';")
    for table in tables:
        print("\n***" + str(table[0]) +"***\n")
        schema[str(table[0])] = {}
        columns = connection.execute("PRAGMA table_info("+str(table[0])+");")
        for column in columns:
            schema[str(table[0])][str(column[1])] = (str(column[1]), str(column[2]))
            print(str(column[1]) + ", " + str(column[2]))

def get_mysql_schema(connection_string=''):
	'''
	gets the schema from a mysql database
	'''
	engine = create_engine(connection_string)
	session = sessionmaker()
	session.configure(bind=engine)
	s = session()
	schema = {}
	tables = s.execute("show tables")
	for table in tables:
		schema[str(table[0])] = {}
		columns = s.execute("show columns from " + table[0])
		for column in columns:
			schema[str(table[0])][str(column[0])] = (str(column[1]), str(column[2]))
	return schema

def get_schema(db_type, connection_string=''):
	'''
	generalized schema analysis function
	'''
	pass

def map_data(source_db, destination_db, source_table, destination_table, source_column, destination_column):
	#sql_text = "SELECT " + source_column + " FROM " + source_table
	#sql_text = text(sql_text)
	#column_data = source_db.execute(sql_text)
	#for db in destination_db:
	#THERE IS A BETTER WAY TO DO THIS...
