from sqlalchemy import create_engine, Column, Integer, Text, Date, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlite3

session = sessionmaker(expire_on_commit=False)

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

def create_db(connection_type, connection_string):
	if connection_type == "postgresql":
		connection_string = 'postgresql://' + connection_string
		raise ValueError('A very specific bad thing happened: pgsql not implemented yet')
	elif connection_type == 'sqlite':
		conn = sqlite3.connect(connection_string)
		conn.close()
		connection_string = connection_type + ':///' + connection_string
		engine = create_engine(connection_string, echo=True)
		session.configure(bind=engine)
		s = session()
		s.execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY)")
		s.close

	elif connection_type == 'mysql':
		connection_string = connection_type + '://' + connection_string
		print("Enter database name: ")
		dbname = input()
		engine = create_engine(connection_string, echo=True)
		engine.execute("CREATE DATABASE " + dbname) #create db
		engine.execute("USE " + dbname)
		session.configure(bind=engine)

def create_table(tablename, columns):
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
	s = session()
	s.execute(sql)
	s.close()

def add_column(tablename, column):
	s = session()
	s.execute("ALTER TABLE " + tablename + " ADD COLUMN " + column.name + " {" + column.data_type + "}")
	s.close()

def get_schema():
	pass

def map_data(source_db, destination_db, source_table, destination_table, source_column, destination_column):
	pass
#create_db('sqlite', "C:\\Users\\654763\\test.db")
#create_db('mysql', 'root:adminP@55w0rd@localhost')
