import psycopg2, logging
import wsettings as s
import xml.etree.ElementTree as xmlE
from datetime import datetime
from traceback import format_exc
# import wchecker as WChecker
import os
import json
import base64


class WSQLshell():
	def __init__(self, dbname, user, password, host):
		self.dbname = dbname
		self.user = user
		self.password = password
		self.host = host
		self.cursor, self.conn = self.create_get_cursor(mode=2)

	def get_cursor(self):
		'''Возвращает курсор'''
		return self.cursor

	def get_all(self, tablename):
		'''Возвращает все содержимое таблицы (tablename)'''
		cursor = self.create_get_cursor()
		cursor.execute('select * from {}'.format(tablename))
		records = cursor.fetchall()
		cursor.close()
		return records

	def get_all_ident(self, tablename, ident):
		cursor = self.create_get_cursor()
		cursor.execute('select * from {} where {}'.format(tablename, ident))
		records = cursor.fetchall()
		cursor.close()
		return records

	def get_all_2idents(self, tablename, ident1, ident2):
		cursor = self.create_get_cursor()
		cursor.execute('select * from {} where {}'.format(tablename,
			ident1, ident2))
		records = cursor.fetchall()
		cursor.close()
		return records

	def get_special_ident(self, tablename, spec, ident):
		'''Возвращает spec значение по равенству ident из таблицы tablename'''
		cursor = self.create_get_cursor()
		cursor.execute('select {} from {} where {}'.format(spec,
			tablename, ident))
		record = cursor.fetchall()
		cursor.close()
		return record

	def get_special_2idents(self, tablename, spec, ident1, ident2):
		'''Возвращает spec значение по двум равенствам ident1 и
		ident2 из таблицы tablename'''
		cursor = self.create_get_cursor()
		cursor.execute('select {} from {} where {} and {}'.format(spec,
			tablename, ident1, ident2))
		record = cursor.fetchall()
		cursor.close()
		return record

	def check_presence(self, value, tablename, column):
		'''Проверяет присутствие значения (value) в
		таблице (tablename), в столбце (column)'''
		print('Проверяю наличие',value,'в',tablename,'pos',column)
		records = self.get_all(tablename)
		for record in records:
			if str(value) == str(record[column]):
				#print('Yep')
				return True

	def create_get_cursor(self, mode=1):
		conn = psycopg2.connect(dbname = self.dbname, user = self.user,
			password = self.password, host = self.host)
		cursor = conn.cursor()
		if mode == 2:
			return cursor, conn
		else:
			return cursor

	def check_car_inside(self, carnum, tablename):
		'''Проверяет находится ли машина на территории предприятия'''
		# self.check_presence(carnum, tablename, column)
		cursor = self.create_get_cursor()
		cursor.execute("select * from {} where car_number='{}' and inside='yes'".format(tablename,
		carnum))
		record = cursor.fetchall()
		cursor.close()
		if len(record) == 0:
			return False
		else:
			return True

	def check_access(self, rfid):
		'''Проверяет, разрешается ли машине въезд'''
		if self.check_presence(rfid, s.auto, 2):
			return True

	def add_weight(self, name, weight, carnum):
		cursor,conn = self.create_get_cursor(mode=2)
		cursor.execute(
			"update {} set {}='{}' where car_number='{}' and inside='Yes'".format(
			s.book, name, weight, carnum))
		conn.commit()

	def create_str(self, tablename, template, values):
		'''Создает новую строку в БД, получает кортеж-шаблон, и кортеж
		значений, а так-же возвращает id записи'''
		cursor,conn = self.create_get_cursor(mode=2)
		cursor.execute('insert into {} {} values {} returning id'.format(tablename,
			template, values))
		conn.commit()
		rec_id = cursor.fetchall()
		rec_id = rec_id[0][0]
		return rec_id

	def update_str_one(self, tablename, template, values, ident):
		cursor, conn = self.create_get_cursor(mode=2)
		cursor.execute("update {} set {}='{}' where {}".format(tablename,
			template, values, ident))
		conn.commit()

	def execute(self, cursor, conn, command):
		#cursor, conn = self.create_get_cursor(mode=2)
		cursor.execute(command)
		conn.commit()

	def get_log_name(self):
		date_now = datetime.now()
		date_now = str(date_now).split('.')[0]
		date_now = date_now.replace(':','-')
		fullname = s.rfid_logs_dir + '/' + date_now + '.txt'
		#print(fullname)
		return fullname

	def save_cm_events_json(self, cursor, poligon_id):
		"""Функция сохранения всех клиентов в формате JSON"""
		records, column_names = self.get_records(cursor, s.cm_events_log_table)
		records_list = self.get_records_list(records, column_names, poligon_id)
		self.save_json(records_list, s.cm_events_json, poligon_id)

	def save_json_report(self, cursor, poligon_id, tablename, filename):
		""" Функция для сохранения отчетов (filename) в формате JSON состоящей из ключей-значений из
		таблицы (tablename)"""
		if tablename == 'records':
			records, column_names = self.get_reports(cursor)
		else:
			records, column_names = self.get_records(cursor, tablename)
		records_list = self.get_records_list(records, column_names, poligon_id)
		timenow = datetime.now()
		self.mark_record(records, tablename, 'wserver_sent', timenow)
		self.save_json(records_list, filename, poligon_id)

	def save_clients_json(self, cursor, poligon_id):
		"""Функция сохранения всех клиентов в формате JSON"""
		records, column_names = self.get_records(cursor, s.clients_table)
		records_list = self.get_records_list(records, column_names, poligon_id)
		self.save_json(records_list, s.clients_json, poligon_id)

	def save_cars_json(self, cursor, poligon_id):
		"""Функция сохранения всех машин в формате JSON"""
		records, column_names = self.get_records(cursor, s.auto)
		records_list = self.get_records_list(records, column_names, poligon_id)
		self.save_json(records_list, s.cars_json, poligon_id)

	def get_records(self, cursor, tablename, command='none'):
		if command == 'none':
			#command = "SELECT * FROM {}".format(tablename)
			command= "SELECT * FROM {} WHERE NOT (wserver_get is not null)".format(tablename)
		records, column_names = self.get_records_columns(cursor, command)
		return records, column_names

	def save_json(self, object, filepath, mode='usual'):
		"""Делает дамп объекта в файл в формате JSON"""
		with open(filepath, 'w') as fobj:
			if mode == 'str':
				json.dump(object, fobj)
			else:
				json.dump(object, fobj, default=str)
	
	def save_reports_json(self, cursor, start_date, poligon_id):
		"""Функция сохранения отчетов в формате JSON"""
		records, column_names = self.get_reports(cursor, start_date)
		records_list = self.get_records_list(records, column_names, poligon_id)
		self.save_json(records_list, s.reports_json)

	def get_records_list(self, records, column_names, poligon_id):
		"""Получает запись из БД и название полей, сохраняет их в словарь вида поле:запись и
		добавляет словарь в listname, впоследствии возвращает listname"""
		listname = []
		for record in records:
			record_dict = {}
			count = 0
			for column in column_names:
				try:
					record_dict[column] = record[count]
					count += 1
				except:
					record_dict[column] = ''
					print('ERROR', column_names, record)
					#pass
			record_dict['poligon'] = poligon_id 
			listname.append(record_dict)
		return listname

	def get_reports(self, cursor):
		"""Получить записи заездов с таблицы records с даты (start_date) по сегодняшний день"""
		request = 'id,car_number,brutto,tara,cargo, to_char("time_in",\'DD/MM/YY HH24:MI:SS\') as time_in'
		request += ',to_char("time_out",\'DD/MM/YY HH24:MI:SS\') as time_out,inside,alerts,carrier,trash_type'
		request += ',trash_cat,notes,operator,checked'
		request += ',(SELECT model FROM auto WHERE auto.car_number=records.car_number LIMIT 1)'
		request += ',(SELECT alerts FROM disputs WHERE disputs.records_id=records.id LIMIT 1)'
		comm = "SELECT {} FROM {} WHERE NOT (wserver_get is not null) and time_in > '14.11.2020' and not tara is null".format(
			request, s.records_table)
		#comm = "SELECT {} FROM {} WHERE wserver_sent = False".format(request, s.records_table)
		records, column_names =  self.get_records_columns(cursor, comm, mode='reports')
		records = self.expand_reports_list(records)
		column_names = self.expand_column_names(column_names)
		#except:
			#print(format_exc())
		return records, column_names

	def expand_reports_list(self, records):
		#records = list(records)
		new_records = []
		for rec in records:
			try:
				rec = list(rec)
				record_id = str(rec[0])
				photo_in_data = self.get_photodata(record_id + 'IN.jpg')
				photo_out_data = self.get_photodata(record_id + 'OUT.jpg')
				rec += [photo_in_data, photo_out_data]
				new_records.append(rec)
			except:
				print('Не удалось сохранить фото!')
				print(format_exc())
		return new_records

	def get_photodata(self, photoname):
		print('Попытка достать', photoname)
		full_name = os.sep.join((s.pics_folder, photoname))
		if not os.path.exists(full_name):
			full_name = os.sep.join((s.pics_folder, 'not_found.jpg'))
		with open(full_name, 'rb') as fobj:
			#photodata = fobj.read()
                        photodata = str(base64.b64encode(fobj.read()))
		return photodata

	def expand_column_names(self, column_names):
		column_names = list(column_names)
		#for column_name in column_names:
		column_names += ['photo_in', 'photo_out']
		return column_names

	def get_records_columns(self, cursor, command, mode='usual'):
		records, column_names = self.tryExecuteGet(cursor, command, mode='colnames')
		return records, column_names

	def mark_record(self, records, tablename, column, value):
		cursor, conn = self.create_get_cursor(mode=2)
		for rec in records:
			command = "UPDATE {} SET {}='{}' WHERE id={}".format(tablename, column, value, rec[0])
			self.tryExecute(cursor, conn, command)

	def save_db_txt(self, tablename):
		log_name = self.get_log_name()
		filename = open(log_name, 'w', encoding='cp1251')
		cursor = self.create_get_cursor()
		#cursor.execute('select * from {}'.format(tablename))
		request = 'id,car_number,brutto,tara,cargo, to_char("time_in",\'DD-MM-YY\')'
		request += ',to_char("time_out",\'DD-MM-YY\'),inside,alerts,carrier,trash_type'
		request += ',trash_cat,notes,operator,checked'
		#print('request -', request)
		td = log_name.split('/')[-1].split(' ')[0]
		comm = "select {} from {} where time_in >= '{}' or time_out >= '{}'".format(request, tablename, td, td)
		#print('comm is', comm)
		cursor.execute(comm)
		data = cursor.fetchall()
		cursor.close()
		allCarsDict = self.getAllCarsDict('Auto')
		for stringname in data:
			strname = str(stringname)
			strname = strname.replace('(','')
			strname = strname.replace(')','')
			carmodel = self.determineCarModel(allCarsDict, stringname[1])
			strname += ', ' + carmodel
			#print(strname)
			#print(strname)
			filename.write(strname)
			filename.write('\n')
		print('Сохранение отчета для диспутов завершено')
		filename.close()

	def saveDbTxt(self, tablename, dates=[]):
		filename = open(s.rfid_logs_dir_1с, 'w', encoding='cp1251')
		cursor = self.create_get_cursor()
		st_date = dates[-1]
		end_date = dates[0]
		print('st and end dates', st_date, end_date)
		request = 'id,car_number,brutto,tara,cargo, to_char("time_in",\'DD/MM/YY HH24:MI:SS\')'
		request += ',to_char("time_out",\'DD/MM/YY HH24:MI:SS\'),inside,alerts,carrier,trash_type'
		request += ',trash_cat,notes,operator,checked'
		comm = "select {} from {} where time_in >= '{}'".format(request, tablename, st_date)
		cursor.execute(comm)
		data = cursor.fetchall()
		cursor.close()
		allCarsDict = self.getAllCarsDict('Auto')
		tid = 'TID | ' + str(datetime.now())
		filename.write(tid)
		filename.write('\n')
		for stringname in data:
			strname = 'Events | ' + str(stringname)
			strname = strname.replace('(','')
			strname = strname.replace(')','')
			strlist = strname.split(',')
			carmodel = self.determineCarModel(allCarsDict, stringname[1])
			if carmodel == None:
				carmodel = 'Модель не опознана'
			strname += ', ' + carmodel
			filename.write(strname)
			filename.write('\n')
		print('Сохранение отчета для 1с завершено')
		filename.close()

	def saveDbXML(self, tablename, dates=[]):
		'''Save database to XML file'''
		# filename = open(s.rfid_logs_dir_1с_xml, 'w', encoding='cp1251')
		cursor = self.create_get_cursor()
		st_date = dates[-1]
		end_date = dates[0]
		print('st and end dates', st_date, end_date)
		request = 'id,car_number,brutto,tara,cargo, to_char("time_in",\'DD/MM/YY HH24:MI:SS\')'
		request += ',to_char("time_out",\'DD/MM/YY HH24:MI:SS\'),inside,alerts,carrier,trash_type'
		request += ',trash_cat,notes,operator,checked,tara_state,brutto_state'
		comm = "select {} from {} where time_in >= '{}'".format(request, tablename, st_date)
		cursor.execute(comm)
		data = cursor.fetchall()
		allCarsDict = self.getAllCarsDict('Auto')
		root = xmlE.Element('uploads')
		root.set('TID', str(datetime.now().strftime("%d/%m/%y %H:%M:%S")))
		root.set("Poligon", "1")
		upload = xmlE.SubElement(root, 'upload')
		upload_name = xmlE.SubElement(upload, 'upload_name')
		upload_name.text = 'Date after ' + str(st_date)
		for stringname in data:
			appt = xmlE.SubElement(upload, "appointment")
			type = xmlE.SubElement(appt, 'type')
			type.text = str("events")
			id = xmlE.SubElement(appt, 'id')
			id.text = str(stringname[0])
			car_number = xmlE.SubElement(appt, 'car_number')
			car_number.text = str(stringname[1])
			brutto = xmlE.SubElement(appt, 'brutto')
			brutto.text = str(stringname[2])
			tara = xmlE.SubElement(appt, 'tara')
			tara.text = str(stringname[3])
			cargo = xmlE.SubElement(appt, 'cargo')
			cargo.text = str(stringname[4])
			date_in = xmlE.SubElement(appt, 'date_in')
			date_in.text = str(stringname[5])
			date_out = xmlE.SubElement(appt, 'date_out')
			date_out.text = str(stringname[6])
			inside = xmlE.SubElement(appt, 'inside')
			inside.text = str(stringname[7])
			alerts = xmlE.SubElement(appt, 'alerts')
			alerts.text = str(stringname[8])
			carrier = xmlE.SubElement(appt, 'carrier')
			carrier.text = str(stringname[9])
			trash_type = xmlE.SubElement(appt, 'trash_type')
			trash_type.text = str(stringname[10])
			trash_cat = xmlE.SubElement(appt, 'trash_cat')
			trash_cat.text = str(stringname[11])
			notes = xmlE.SubElement(appt, 'notes')
			notes.text = str(stringname[12])
			operator = xmlE.SubElement(appt, 'operator')
			operator.text = str(stringname[13])
			checked = xmlE.SubElement(appt, 'checked')
			checked.text = str(stringname[14])
			tara_state = xmlE.SubElement(appt, 'tara_state')
			tara_state.text = str(stringname[15])
			brutto_state = xmlE.SubElement(appt, 'brutto_state')
			brutto_state.text = str(stringname[16])
			carmodel = xmlE.SubElement(appt, 'carmodel')
			carmodel.text = str(self.determineCarModel(allCarsDict, stringname[1]))
		tree = xmlE.ElementTree(root)
		with open(s.rfid_logs_1c_xml, "wb") as fn:
			tree.write(fn, encoding="cp1251")
			# fn.write(xmlE.tostring(tree).decode("utf-8"))
		with open(s.rfid_logs_1c_xml_1pol, "wb") as fn:
			tree.write(fn, encoding="cp1251")
			# fn.write(xmlE.tostring(tree).decode("utf-8"))
		print('Сохранение отчета в XML завершено')

	def saveDbXMLext(self, tablename, dates=[]):
		'''Save database to XML file'''
		cursor = self.create_get_cursor()
		st_date = dates[-1]
		end_date = dates[0]
		request = 'id,car_number,brutto,tara,cargo, to_char("time_in",\'DD/MM/YY HH24:MI:SS\')'
		request += ',to_char("time_out",\'DD/MM/YY HH24:MI:SS\'),inside,alerts,carrier,trash_type'
		request += ',trash_cat,notes,operator,checked,tara_state,brutto_state'
		comm = "select {} from {} where time_in >= '{}'".format(request, tablename, st_date)
		cursor.execute(comm)
		data = cursor.fetchall()
		allCarsDict = self.getAllCarsDict('Auto')
		root = xmlE.Element('uploads')
		root.set('TID', str(datetime.now().strftime("%d/%m/%y %H:%M:%s")))
		root.set("Poligon", "1")
		upload = xmlE.SubElement(root, 'upload')
		upload_name = xmlE.SubElement(upload, 'upload_name')
		upload_name.text = 'Date after ' + str(st_date)
		for stringname in data:
			appt = xmlE.SubElement(upload, "appointment")
			type = xmlE.SubElement(appt, 'type')
			type.text = str("events")
			id = xmlE.SubElement(appt, 'id')
			id.text = str(stringname[0])
			car_number = xmlE.SubElement(appt, 'car_number')
			car_number.text = str(stringname[1])
			brutto = xmlE.SubElement(appt, 'brutto')
			brutto.text = str(stringname[2])
			tara = xmlE.SubElement(appt, 'tara')
			tara.text = str(stringname[3])
			cargo = xmlE.SubElement(appt, 'cargo')
			cargo.text = str(stringname[4])
			date_in = xmlE.SubElement(appt, 'date_in')
			date_in.text = str(stringname[5])
			date_out = xmlE.SubElement(appt, 'date_out')
			date_out.text = str(stringname[6])
			inside = xmlE.SubElement(appt, 'inside')
			inside.text = str(stringname[7])
			alerts = xmlE.SubElement(appt, 'alerts')
			alerts.text = str(stringname[8])
			carrier = xmlE.SubElement(appt, 'carrier')
			if stringname[9] is not None:
				request = 'SELECT id, id_1c, full_name , inn from clients where id_1c = {}'.format(stringname[9])
				cursor.execute(request)
				carrier_data = cursor.fetchall()
				carrier_data_id = xmlE.SubElement(carrier, 'id')
				carrier_data_id.text = str(carrier_data[0][0])
				carrier_data_id_1c = xmlE.SubElement(carrier, 'id_1c')
				carrier_data_id_1c.text = str(carrier_data[0][1])
				carrier_data_full_name = xmlE.SubElement(carrier, 'full_name')
				carrier_data_full_name.text = str(carrier_data[0][2])
				carrier_data_inn = xmlE.SubElement(carrier, 'inn')
				carrier_data_inn.text = str(carrier_data[0][3])
			else:
				carrier.text = str(stringname[9])
			trash_type = xmlE.SubElement(appt, 'trash_type')
			if stringname[10] is not None:
				request = 'select name, id , type_id from trash_types where type_id = {}'.format(stringname[10])
				cursor.execute(request)
				trash_t = cursor.fetchall()
				trash_type_name = xmlE.SubElement(trash_type,'name')
				trash_type_name.text = str(trash_t[0][0])
				trash_type_id = xmlE.SubElement(trash_type, 'id')
				trash_type_id.text = str(trash_t[0][1])
				trash_type_type_id = xmlE.SubElement(trash_type, 'type_id')
				trash_type_type_id.text = str(trash_t[0][2])
			else:
				trash_type.text = str(stringname[10])
			trash_cat = xmlE.SubElement(appt, 'trash_cat')
			if stringname[11] is not None:
				request = 'select cat_name, id , cat_id from trash_cats where cat_id = {}'.format(stringname[11])
				cursor.execute(request)
				trash_c = cursor.fetchall()
				trash_cat_name = xmlE.SubElement(trash_cat, 'name')
				trash_cat_name.text = str(trash_c[0][0])
				trash_cat_id = xmlE.SubElement(trash_cat, 'id')
				trash_cat_id.text = str(trash_c[0][1])
				trash_cat_cat_id = xmlE.SubElement(trash_cat, 'cat_id')
				trash_cat_cat_id.text = str(trash_c[0][2])
			else:
				trash_cat.text = str(stringname[11])
			notes = xmlE.SubElement(appt, 'notes')
			notes.text = str(stringname[12])
			operator = xmlE.SubElement(appt, 'operator')
			if stringname[13] is not None:
				request = 'select username, id , user_id from users where user_id = {}'.format(stringname[13])
				cursor.execute(request)
				operator_r = cursor.fetchall()
				operator_username = xmlE.SubElement(operator,'username')
				operator_username.text = str(operator_r[0][0])
				operator_id = xmlE.SubElement(operator, 'id')
				operator_id.text = str(operator_r[0][1])
				operator_user_id = xmlE.SubElement(operator, 'user_id')
				operator_user_id.text = str(operator_r[0][2])
			else:
				operator.text = str(stringname[13])
			checked = xmlE.SubElement(appt, 'checked')
			checked.text = str(stringname[14])
			tara_state = xmlE.SubElement(appt, 'tara_state')
			tara_state.text = str(stringname[15])
			brutto_state = xmlE.SubElement(appt, 'brutto_state')
			brutto_state.text = str(stringname[16])
			carmodel = xmlE.SubElement(appt, 'carmodel')
			carmodel.text = str(self.determineCarModel(allCarsDict, stringname[1]))
		print("xml")
		tree = xmlE.ElementTree(root)
		# , encoding = 'cp1251'
		with open(s.rfid_logs_1c_xml_ext, "wb") as fn:
			tree.write(fn, encoding="cp1251")
		# fn.write(xmlE.tostring(tree).decode("utf-8"))
		with open(s.rfid_logs_1c_xml_ext_1pol, "wb") as fn:
			tree.write(fn, encoding="cp1251")
		# fn.write(xmlE.tostring(tree).decode("utf-8"))
		print('Сохранение отчета в XML_ext завершено \n')

	def getAllCarsDict(self, tablename):
		allCarsList = self.get_all(tablename)
		allCarsDict = {}
		for car in allCarsList:
			allCarsDict[car[1]] = car[3]
		return allCarsDict

	def determineCarModel(self, allcars, carnum):
		try:
			carmodel = allcars[carnum]	
		except KeyError:
			carmodel = 'Модель не опознана'
		return carmodel

	def get_frmt_db_date(self, date):
		date = date.split(' ')[0]
		full = date.replace('.','-')
		return full

	def update_str_two(self,tablename, values, ident1, ident2):
		'''Обновляет строку по двум идентификаторам и возвращает id изменяемой записи'''
		cursor, conn = self.create_get_cursor(mode=2)
		cursor.execute('update {} set {} where {} and {} returning id'.format(tablename,
			values, ident1, ident2))
		conn.commit()
		rec_id = cursor.fetchall()
		rec_id = rec_id[0][0]
		return rec_id

	def get_last_visit(self, tablename,  ident1, ident2):
		values = self.get_all_2idents(tablename, ident1, ident2)
		listname = []
		for v in values:
			listname.append(v[0])
		for v in values:
			if v[0] == max(listname):
				return v

	def getLastId(self, tablename):
		cursor = self.create_get_cursor()
		cursor.execute("select max(id) from {}".format(tablename))
		record = cursor.fetchall()
		cursor.close()
		return record

	def getLastVisit(self, tablename, ident, value):
		cursor = self.create_get_cursor()
		comm = 'SELECT * FROM {} where {} ORDER BY {} DESC LIMIT 1'.format(tablename,
			ident, value)
		cursor.execute(comm)
		record = cursor.fetchall()
		cursor.close()
		return record

	def getExecComm(self, comm):
		cursor = self.create_get_cursor()
		cursor.execute(comm)
		record = cursor.fetchall()
		cursor.close()
		return record
	
	def tryExecute(self, command, returning=True):
		'''Попытка исполнить команду через заданный курсор'''
		if returning:
			command += 'RETURNING id'
		print('\nПопытка выполнить комманду', command)
		try:
			self.cursor.execute(command)
			self.conn.commit()
			if returning:
				rec_id = self.cursor.fetchall()
				return rec_id
			print('\tУспешно!')
		except:
			self.transactionFail(self.cursor)
	
	def transactionFail(self, cursor):
		''' При неудачной транзакции - logging & rollback'''
		print(format_exc())
		logging.error(format_exc())
		cursor.execute("ROLLBACK")
		print('\tТранзакция провалилась. Откат.')

	def tryExecuteGet(self, command, mode='usual'):
		'''Попытка исполнить команду и вернуть ответ через заданный курсор'''
		try:
			self.cursor.execute(command)
			record = self.cursor.fetchall()
			if mode == 'usual':
				print('\tДанные получены -', record)
				return record
			elif mode == 'colnames':
				colnames = [desc[0] for desc in self.cursor.description]
				return record, colnames
		except:
			self.transactionFail(self.cursor)

	def addAlerts(self, cursor, conn, alerts, rec_id):
		'''Добавляет строку в таблицу disputs, где указываются данные об инциденте'''
		print('\n###Добавляем новые алерты к записи###')
		print('\talerts -', alerts)
		if len(alerts) > 0:
			print('\tи это больше нуля')
			timenow = datetime.now()
			command = "insert into {} ".format(s.disputs_table)
			command += "(date, records_id, alerts) "
			command += "values ('{}', {}, '{}') ".format(timenow, rec_id, alerts)
			command += "on conflict (records_id) do update "
			command += "set alerts = disputs.alerts || '{}'".format(alerts)
			try:
				cursor.execute(command)
				conn.commit()
				print('\tУспешно!')
			except:
				self.transactionFail(cursor)

	def updLastEvents(self, carnum, carrier, trash_type, trash_cat):
		print('\t\tОбновление таблицы lastEvents')
		cursor, conn = self.create_get_cursor(mode=2)
		carId = "select id from auto where car_number='{}' LIMIT 1".format(carnum)
		comm = 'insert into {} '.format(s.last_events_table)
		comm += '(car_id, carrier, trash_type, trash_cat) '
		comm += "values (({}),{},{},{}) ".format(carId, carrier, trash_type, trash_cat)
		comm += 'on conflict (car_id) do update '
		comm += 'set carrier={}, trash_cat={}, trash_type={}'.format(carrier, trash_cat, trash_type)
		cursor.execute(comm)
		conn.commit()
