# makes a unified db of all the lead collections (Files)
def make_unified_db():

	# replacements dictionary - normalize df column names
	# these are the only columns to go into the aggregated db
	replacements = {
	    "name" : ["lead_name", "contact_name", "full_name"],
	    "first_name" : "first",
	    "last_name" : "last",
	    "company" : "company_name",
	    "company_url" : ["company_site", "site", "url", "comp_url", "filter", "company_website"],
	    "company_phone" : ["phone", "hq_phone", "comp_phone", "hq"],
	    "direct_number" : ["direct_nr", "direct_num", "personal_phone", "direct_phone"],
	    "title" : ["position", "job", "job_title"],
	    "email" : ["e-mail", "mail", "direct_email", "direct_mail"],
	    "source" : ["lead_source", "researcher", "lead:"],
	    "company_type" : ["type"],
	    "location" : ["genera_location", "linkedin_location", "locality", "linkedin_locality"] ,
	    # this is general location, usually acquired from linkedin API
	    "lead_profile_linkedin" : ["profile", "linkedin_profile"],
	    "company_profile_linkedin" : ["company_profile", "company_linkedin_profile", "linkedin_company_profile"],
	    "state" : ["person_state", "lead_state"],
	    "country" : ["person_country", "company_country", "lead_country"]
	}

	insert_counter = 0

	connection = MongoClient('localhost', 27017)
	db = connection.hacktown
	insert_into_db = "hacktown_final_leads_db"
	insert_into_collection = "all_leads_database"

	all_collection_names = db.collection_names()

	for collection in all_collection_names: # go over all collections (files) and get their names
		# if the collection name ends in the correct file type and was not previously inserted to db:
	    if collection.endswith(xls_filetypes) and not connection[insert_into_db][insert_into_collection].find_one({"original_collection_name":collection}):

	        df = pd.DataFrame(list(db[collection].find())) # put into df
	        
	        # rename columns + add to the df the collection the leads came from
	        df.rename(columns={el:k for k,v in replacements.iteritems() for el in v}, inplace=True)
	        df["original_collection_name"] = collection

	        # special cases for rename
	        if (("first_name" in df.columns) and ("last_name" in df.columns)):
	            df["name"] = str(df["first_name"]) + " " + str(df["last_name"])
	        elif ("last_name" in df.columns) and ("name"in df.columns):
	            df["name"] = str(df["name"]) + " " + str(df["last_name"])
	        elif ("name" in df.columns) and ("first_name"in df.columns):
	            df["name"] = str(df["first_name"]) + " " + str(df["name"])
	            
	        # insert only the wanted columns from "replacements" + the collection name and last modified
	        columns_to_insert = [k for k,v in replacements.items() if (not (k=="first_name")) and (not (k=="last_name")) and (k in df.columns.values)]
	        columns_to_insert.extend(["last_modified", "original_collection_name"])	   
	        records = df[columns_to_insert].to_dict(orient="records")
	        connection[insert_into_db][insert_into_collection].insert(records) # insert to new db
	        insert_counter += 1 # count how many collections were inserted to db

	connection.close()
	return insert_counter