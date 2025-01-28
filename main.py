import sqlite3
import pandas as pd

# создаем подключение к нашей базе данных
conn = sqlite3.connect('database.db')

# создаем обьект курсор для работы с базой данных
cursor = conn.cursor()

# функция считывания CSV в SQl

def csv2sql(filePath, tableName):
	df = pd.read_csv(filePath)
	df.to_sql(tableName, con=conn, if_exists='replace', index=False)


# функция преобразования из sql в csv
def sql2csv(tableName, filePath):
	df = pd.read_sql(sql=f'SELECT * FROM {tableName}', con=conn)
	df.to_csv(filePath, index=False)

# функция отображения таблицы

def showTable(tableName):
	cursor.execute(f'SELECT * FROM {tableName}')
	print('_-' * 10)
	print(tableName)
	print('_-' * 10)
	for row in cursor.fetchall():
		print(row)
	print('_-' * 10 + '\n')


# создаем таблицу hist_auto где будут ханиться все измененные и удаленные данные автомобилей
# erfpsdftv if not exist чтобы она не падала если будет запущена функция повторно
def init():
	cursor.execute('''
		CREATE TABLE IF NOT EXISTS hist_auto(
			id integer primary key autoincrement,
			model varchar(128),
			transmission varchar(128),
			body_type varchar(128),
			drive_type varchar(128),
			color varchar(128),
			production_year integer,
			auto_key integer,
			engine_capacity number(2, 1),
			horsepower integer,
			engine_type varchar(128),
			price integer,
			milage integer,
			deleted_flg integer default 0,
			start_dttm datetime default current_timestamp, 
			end_dttm datetime default (datetime('2999-12-31 23:59:59'))
			)
	''')

# создаем нтабличку новых данных из tmp_auto (условный срез данных за какой то промежуток времени) через
# left join

def new_rows():
	cursor.execute("""
		CREATE TABLE tmp_new_rows as
		SELECT 
			*
		FROM tmp_auto
		WHERE auto_key not in (select auto_key from hist_auto)
		""")

# определить удаленные из tmp_auto (в последствии мы на них будем вешать новые dttm и deleted_flg)
def deleted_rows():
	cursor.execute("""
		CREATE TABLE tmp_deleted_rows as
		SELECT 
			*
		FROM hist_auto
		WHERE auto_key not in (select auto_key from tmp_auto)
		""")

# создаем функцию котоаря создает таблицу измененных записей
def changed_rows():
	cursor.execute('''
		CREATE TABLE tmp_changed_rows as 
		SELECT 
			 t1.*
		FROM tmp_auto t1
		INNER JOIN hist_auto t2
		on t1.auto_key = t2.auto_key
		WHERE 	t1.model 		   <> t2.model
			or t1.transmission     <> t2.transmission
			or t1.body_type        <> t2.body_type
			or t1.drive_type       <> t2.drive_type
			or t1.color            <> t2.color
			or t1.production_year  <> t2.production_year
			or t1.engine_capacity  <> t2.engine_capacity
			or t1.horsepower       <> t2.horsepower
			or t1.engine_type      <> t2.engine_type
			or t1.price            <> t2.price
			or t1.milage           <> t2.milage
	''')

# функция удаления временных таблиц чтобы небыло конфликта при их создании

def delete_tmpTables():
	cursor.execute('DROP TABLE if exists tmp_auto')
	cursor.execute('DROP TABLE if exists tmp_new_rows')
	cursor.execute('DROP TABLE if exists tmp_changed_rows')
	cursor.execute('DROP TABLE if exists tmp_deleted_rows')




delete_tmpTables()

init()
csv2sql('store/data_1.csv', 'tmp_auto')
new_rows()
deleted_rows()
changed_rows()

showTable('tmp_auto')
showTable('tmp_new_rows')
showTable('tmp_changed_rows')
showTable('tmp_deleted_rows')
showTable('hist_auto')


