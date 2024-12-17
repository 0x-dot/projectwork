import os
import sys
import datetime
import sqlite3
import pandas as pd
import argparse
import importlib.util

#directory 
directory = "generated_data"
log_directory="migration_log"
log_file = os.path.join(log_directory,"migration.log")

default_db_name = "migration.db"

LOG_FILE_NOT_FOUND = "log_file_not_found"
FILE_NOT_MIGRATED ="file_not_migrated"
FILE_MIGRATED= "file_migrated"



def check_requirements():
    
    requiremetntsNotInstalled = []
    try:
        with open("./requirement.txt") as f:
            requirements = f.readlines()


        for requirement in requirements:
            
            try:
                importlib.util.find_spec(requirement.strip())
            except ModuleNotFoundError:
                print(f"Requirement {requirement} not found")
                requiremetntsNotInstalled.append(requirement)          
        
        if requiremetntsNotInstalled:
            print(f"Please install the following requirements: {requiremetntsNotInstalled}")
            return False
        else:
            print("All requirements are installed")
            return True
        
    except Exception as e:
        print(f"Error during requirement check: {e}")
        return False




#Initialize logger directory and file
def initialize_logger():   
   try:  
       if not os.path.exists(log_directory):
           os.makedirs(log_directory)
           print("Directory ", log_directory, " Created ")
      
       if not os.path.exists(log_file):
           with open(log_file,'w') as log:
               log.write(f"List of migrated file: \n")
      
       else:
           print("Logger file ", log_file)
      
          
   except Exception as e:
       print(f"Error during logger initialization: {e}")



#Write log file
def write_log(fileName):
    try:
        with open(log_file,'a') as log:
            log.write(f"{fileName}\n")                
    
    except Exception as e:
        print(f"Error during log writing: {e}")


#Check migration file  
def check_exist_migration_file(file_path):

    if not os.path.isabs(file_path):
       file_path = os.path.join(os.getcwd(),file_path)


    if not os.path.exists(file_path):
       print(f"File {file_path} not found.")
       return None
  
    if not file_path.endswith(".xlsx"):
       print(f"File {file_path} not an .xlsx file.")
       return None


    file_name= os.path.basename(file_path)


    return file_name,file_path


#retunr status of migration file 
def migration_status(file_name, log_file):
   statusMap ={}
      
   if os.path.exists(log_file):
       with open(log_file,'r') as log:
           fileMigrated = {line.strip() for line in log.readlines()}           
                 
           if file_name in fileMigrated:
               statusMap[file_name]= FILE_MIGRATED
           else:
               statusMap[file_name]=FILE_NOT_MIGRATED      
          
   else:       
           statusMap[file_name]=LOG_FILE_NOT_FOUND
    
   return statusMap

#Get name of file 
def get_name_of_file(filename):    
    if "@"  not in filename:
        raise ValueError("Invalid file name {filename}, missing '@' flag")
    
    return filename.split("@",1)[0]
    


#Migrate data to db
def migrate_to_db(file,db_name,table_name):
    print(f"Migration in progress..")
  
    if db_name is None:
        db_name = default_db_name
        print(f"Database name not provided, using default database {default_db_name}")
    if table_name is None:
        table_name = 'testpw'
        print(f"Table name not provided, using default table name {table_name}")


    filename= os.path.basename(file)

    try:   
        with sqlite3.connect(db_name) as conn:
            c = conn.cursor()
                    
            data= pd.read_excel(file)       
            
            create_table_on_db(c,table_name,data)       
            insert_data_on_db(conn,c,table_name,data)
            write_log(filename)
  
    except Exception as e:
        print(f"Error during data migration: {e}")
    
      
   
    
#Create table on db
def create_table_on_db(c,table_name,data):
    print(f"Create SQL table...")
    
    try:
        columns= "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        columns += ", ".join([f"{col.replace(' ','_').lower()} TEXT" for col in data.columns])
        print(f"Columns: {columns}")       
        create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        c.execute(create_table)
        print(f"Succesfully created SQL table {table_name}")
        
    except Exception as e:
        print(f"Error during table {table_name} creation: {e}")  
    
    print(f"End of SQL table creation..")
    
    

#Insert data on db
def insert_data_on_db (conn,c,table_name,data):
    print(f"Insert data into SQL table {table_name}...")
    try:
        conn.execute("BEGIN TRANSACTION;")       


        columns = ", ".join([col.replace(' ', '_').lower() for col in data.columns])
        
        for row in data.itertuples(index=False,name=None):
            placeholders = ", ".join("?"*len(row))
            insert=f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            print(f"Inserting data: {insert}")
            c.execute(insert,row)           
        conn.commit()
        print(f"Successfully inserted data into SQL table {table_name}")
        
    except Exception as e:
        conn.rollback()
        print(f"Error during insert data into SQL table {table_name}: {e}")
   
         
    
#Migration phase
def migration_phase(file,statusMap,db_name,table_name):   
    filename = os.path.basename(file)
    
    status = statusMap.get(filename)
        
    if status == LOG_FILE_NOT_FOUND:
            print(f"Log file not found, migration aborted")
            return
        
    elif status == FILE_NOT_MIGRATED:
            print(f"File {filename} to be migrated")
            migrate_to_db(file,db_name,table_name)           
    else:
            print(f"File {filename} already migrated, skipping")


def get_data_migrated(db_name,table_name):
    try:
            with sqlite3.connect(db_name) as conn:
                c = conn.cursor()
                c.execute(f"SELECT * FROM {table_name};")
                data = c.fetchall()
                return data
    except Exception as e:
        print(f"Error during file name extraction: {e}")
        return []
    
def get_data_from_file(file):
    try:
        data= pd.read_excel(file)
        return data
    except Exception as e:
        print(f"Error during data extraction: {e}")
        return []    


def get_name_of_table_on_DB(db_name):   
    if db_name is None:
        db_name = default_db_name
        print(f"Database name not provided, using default database {default_db_name}")   

    print(f"Get name of table on database {db_name}...")
    try:
        with sqlite3.connect(db_name) as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = c.fetchall()
            tables = [table[0] for table in tables]
            return tables
    except Exception as e:
        print(f"Error during table name extraction: {e}")
        return []
    


def consistency_check(file,db_name,table_name):
    print(f"Consistency check in progress...")
    tablesName = get_name_of_table_on_DB(db_name)
    print(f"Tables in database: {tablesName}")     
    
    if table_name not in tablesName:
        print(f"Table {table_name} not found in database")
        return       
    else:
        print(f"Table {table_name} found in database")
    
    data_from_file = get_data_from_file(file)
    data_from_db = get_data_migrated(db_name,table_name)
    
    converted_data_from_db = pd.DataFrame(data_from_db)
    converted_data_from_db = converted_data_from_db.iloc[:,:-1]
    
    converted_data_from_db.columns = data_from_file.columns
    

    
    #tobecompleteeeeeesss....
    if data_from_file.equals(converted_data_from_db):
        print(f"Data from file and data from database are consistent")
    else:
        print(f"Data from file and data from database are not consistent")
        missing_data = data_from_file[~data_from_file.isin(converted_data_from_db).all(axis=1)]
        print(f"Missing data: {missing_data}")
   

     
        
#Main function
def main():
    if not check_requirements():
        print("Requirements not installed, please install requirements")
        return

    parser = argparse.ArgumentParser(description="Migrate data from Excel to SQL")


    parser.add_argument("-f","--file",help="Path to the file .xlsx to be migrated")
    parser.add_argument("-t","--table",help="Name of the table to be created or update in the database")
    parser.add_argument("-d","--database",help="Path to the SQLite database file")


    args = parser.parse_args()
        
    
    #Initialize logger
    initialize_logger()


    file_to_be_migrate,file_path = check_exist_migration_file(args.file)


    if file_to_be_migrate is not None and file_path is not None:
        print("Ready for migration..\n")
        print(f"File to be migrated: {file_to_be_migrate}")
        print(f"File path: {file_path}")
        print(f"Log file: {log_file}")
        print(f"Database name : {args.database} if database is none, default database will be used")
        print(f"Table name : {args.table} if table name args is none, default table name will be used\n")


        tobemigrate = input("Are you sure you want to proceed? (y/n): ")
            
        if tobemigrate == "y":
            status_map = migration_status(file_to_be_migrate,log_file)
            migration_phase(file_path,status_map, args.database, args.table)    
            print("Migration completed")
            tobeConsistencyCheck = input("Do you want to perform a consistency check? (y/n): ")
            if tobeConsistencyCheck == "y":
                consistency_check(file_path,args.database,args.table)
                sys.exit(0)
            else:
                print("Consistency check aborted")
                    
        else:
            print("Migration aborted")
    else:
        print("No file to be migrated")

    

if __name__ == "__main__":
   main()