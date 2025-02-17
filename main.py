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


# создаем таблицу hist_auto где будут храниться все измененные и удаленные данные автомобилей
#  if not exist чтобы она не падала если будет запущена функция повторно
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


# создаем вью на основе hist_auto чтобы проверять актуальные (с флагом 0 ) позиции иначе он будет добавлять и плодить дубли
# каждый раз при отсутствии этих значений в тмп авто
	cursor.execute('''
		CREATE VIEW IF NOT EXISTS v_hist_auto as
		SELECT
			id,
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
		FROM hist_auto
		WHERE deleted_flg = 0
		AND current_timestamp between start_dttm and end_dttm
	''')


# создаем табличку новых данных из tmp_auto (условный срез данных за какой то промежуток времени)

def new_rows():
	cursor.execute("""
		CREATE TABLE tmp_new_rows as
		SELECT 
			*
		FROM tmp_auto
		WHERE auto_key not in (select auto_key from v_hist_auto) 
		""")

# определить удаленные из tmp_auto (в последствии мы на них будем вешать новые dttm и deleted_flg)

def deleted_rows():
	cursor.execute("""
		CREATE TABLE tmp_deleted_rows as
		SELECT 
			*
		FROM v_hist_auto
		WHERE auto_key not in (select auto_key from tmp_auto)
		""")

# создаем функцию которая создает таблицу измененных записей

def changed_rows():
	cursor.execute('''
		CREATE TABLE tmp_changed_rows as 
		SELECT 
			 t1.*
		FROM tmp_auto t1
		INNER JOIN v_hist_auto t2
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


# добавляем записи в таблицу хист авто кроме тех которые заполняются автоматически по условию создании таблицы

def change_hist_auto():

# Добавляем новые записи в hist_auto из таблицы tmp_new_rows(создали ранее)
	cursor.execute("""
		INSERT INTO hist_auto (
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				auto_key,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
				)
		SELECT 
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				auto_key,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
				
		FROM tmp_new_rows
		""")


 # Изменяем в hist_auto end_dttm старые  значения которые есть в tmp_changed_rows на текущее время
 # (у новых измененных start_dttm назначится автоматически и end_dttm тех бесконечность)
 # указываем доп условие AND end_dttm = datetime('2999-12-31 23:59:59') чтобы изменять end_dttm только у последней записи

	cursor.execute('''
		UPDATE hist_auto
		SET end_dttm = datetime('now', '-1 second')         
		WHERE auto_key in (select auto_key from tmp_changed_rows)
		AND end_dttm = datetime('2999-12-31 23:59:59')
		''')

#  и так же добавляем эти запись в нашу hist_auto(вот  у них старт и энд дттм заполнистя автоматически)
	cursor.execute("""
		INSERT INTO hist_auto (
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
			)
		SELECT 
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
			
		FROM tmp_changed_rows
		""")

# работаем с таблицей удаленных записей, сначала нам нужно изменть их end_dttm на текущий момент
# но только там где datetime('2999-12-31 23:59:59') иначе будет меняться везде
# и добавляем их в в хист авто изменяя флаг на 1
# указываем доп условие AND end_dttm = datetime('2999-12-31 23:59:59') чтобы изменять end_dttm только у последней записи

	cursor.execute('''
		UPDATE hist_auto
		SET end_dttm = datetime('now', '-1 second') 
		WHERE auto_key in (select auto_key from tmp_deleted_rows)
		AND end_dttm = datetime('2999-12-31 23:59:59')
		''')
	

	cursor.execute("""
		INSERT INTO hist_auto (
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage,
			deleted_flg
			)
		SELECT 
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage,
			1
			
		FROM tmp_deleted_rows
		""")
	conn.commit()



delete_tmpTables()

init()
csv2sql('store/data_3.csv', 'tmp_auto')
new_rows()
deleted_rows()
changed_rows()

change_hist_auto()

showTable('tmp_auto')
showTable('v_hist_auto')
showTable('tmp_new_rows')
showTable('tmp_changed_rows')
showTable('tmp_deleted_rows')
showTable('hist_auto')


