import sqlite3
import pandas as pd

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# функция считывания CSV в SQl

def csv2sql(filePath, tableName):
	df = pd.read_csv(filePath)
	df.to_sql(tableName, con=conn, if_exists='replace', index=False)


# функция преобразования из sql в csv
def sql2csv(tableName, filePath):
	df = pd.read_sql(sql=f'SELECT * FROM {tableName}', con=conn)
	df.to_csv(filePath, con=conn, index=False)

# функция отображения таблицы

def showTable(tableName):
	cursor.execute(f'SELECT * FROM {tableName}')
	
	for row in cursor.fetchall():
		print(row)


# csv2sql('store/data.csv', 'tmp_auto')

# showTable('tmp_auto')

sql2csv('tmp_auto', 'test.csv')