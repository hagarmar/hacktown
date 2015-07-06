from pymongo import MongoClient
import pandas as pd
import os.path, time 
import datetime
from os import listdir

xls_filetypes = (".xls", ".xlsx")
csv_filetypes = ".csv"

def import_excel_list(file_name, sheet_name=0, header=0):

	response = None

	try:
		df = pd.read_excel(file_name, sheetname=sheet_name, header=header, encoding="utf-8")
	except IOError:
		return response
	
	date_created = time.ctime(os.path.getmtime(file_name))
	df["last_modified"] = datetime.datetime.strptime(date_created, "%a %b %d %H:%M:%S %Y")
	df.columns = [x.lower().replace(' ', '_') for x in df.columns]

	connection = MongoClient('localhost', 27017)
	db = connection.hacktown

	if not file_name in db.collection_names():
	    records = df.to_dict(orient="records")

		# change "." for "_" in top keys of records
	    new_dict_list=[]   
	    for record in records:
	        new_record = dict((k.replace(".", "_"),v) for k,v in record.items())
	        new_dict_list.append(new_record)

	    try:
	    	response = db[file_name].insert(new_dict_list)
	    except:
	        print "could not save document %s" % file_name
	
	connection.close()

	return response

def import_all_lists_in_path(path_name, include_subdirs = True):

	count_files_uploaded = 0
	dirs_to_check = [path_name]

	# get all files in a path
	# go over all files: we are looking for xls/xlsx or csv
	while dirs_to_check:
		current_path_name = dirs_to_check.pop()
		all_files_in_path = [f for f in listdir(current_path_name) if os.path.isfile(os.path.join(current_path_name,f))]
	    
		for file_name in all_files_in_path:
			if file_name.endswith(xls_filetypes):
				response = import_excel_list(current_path_name + file_name)
				if response:
					count_files_uploaded += 1

		if include_subdirs: # if including sub directories
			all_dirs_in_path = [current_path_name + d + '/' for d in listdir(current_path_name) if os.path.isdir(os.path.join(current_path_name,d))]
			dirs_to_check.extend(all_dirs_in_path)

	return count_files_uploaded
