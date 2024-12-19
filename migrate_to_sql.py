import os
import sys
import datetime
import sqlite3
import pandas as pd
import argparse
import re
import importlib.util
import hashlib

#log directory
log_directory="migration_log"
log_file = os.path.join(log_directory,"migration.log")

default_db_name = "migration.db"
default_table_name = "testpw"
LOG_FILE_NOT_FOUND = "log_file_not_found"
FILE_NOT_MIGRATED ="file_not_migrated"
FILE_MIGRATED= "file_migrated"


def check_table_name_is_valid(table_name):
    response=False
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    if re.match(pattern,table_name):        
        response=True
    
    return response

    


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
def write_log(fileName,table_name):    
    try:
        with open(log_file,'a') as log:
            log.write(f"{fileName} in to table {table_name}\n")                
    
    except Exception as e:
        print(f"Error during log writing: {e}")


#Check migration file  
def check_exist_migration_file(file_path):

    if not os.path.isabs(file_path):
       print(os.getcwd()) 
       file_path = os.path.join(os.getcwd(),file_path)
       


    if not os.path.exists(file_path):
       print(f"File {file_path} not found.")
       return None,None
  
    if not file_path.endswith(".xlsx"):
       print(f"File {file_path} not an .xlsx file.")
       return None,None


    file_name= os.path.basename(file_path)


    return file_name,file_path


#retunr status of migration file 
def migration_status(table_name,file_name, log_file):
   statusMap ={}
   if table_name is None:
         table_name = default_table_name
         print(f"Table name not provided, using default table name {default_table_name}")
   
   
   if os.path.exists(log_file):
       with open(log_file,'r') as log:
           fileMigrated = {line.strip() for line in log.readlines()}           
           
           lines = [line for line in fileMigrated if file_name in line]
           if lines:
               tablesMigrated=[line.split(" in to table ")[-1].strip() for line  in lines] #mi serve nel caso in cui si vuole migrare un file già migrato in un'altra tabella
               if table_name in tablesMigrated:
                    statusMap[file_name]= FILE_MIGRATED                   
               else:
                  statusMap[file_name]=FILE_NOT_MIGRATED
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
        table_name = default_table_name
        print(f"Table name not provided, using default table name {table_name}")


    filename= os.path.basename(file)

    try:   
        with sqlite3.connect(db_name) as conn:
            c = conn.cursor()
                    
            data= pd.read_excel(file)       
            
            create_table_on_db(c,table_name,data)       
            insert_data_on_db(conn,c,table_name,data)
            write_log(filename,table_name)
  
    except Exception as e:
        print(f"Error during data migration: {e}")
    

def get_data_type_column(data,col):
    try:
        if data[col].dtype == 'int64':
            return "INTEGER"
        elif data[col].dtype == 'float64':
            return "REAL"
        else:
            return "TEXT"
    except Exception as e:
        print(f"Error during column data type extraction: {e}")
        return "TEXT"
   
    
#Create table on db
def create_table_on_db(c,table_name,data):
    print(f"Create SQL table...")
    
    try:
        columns= "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        columns += ", ".join([f"{col.replace(' ','_').lower()} {get_data_type_column(data,col)}" for col in data.columns])
             
        create_table = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        c.execute(create_table)
        print(f"Succesfully created SQL table {table_name}")
        
    except Exception as e:
        print(f"Error during table {table_name} creation: {e}")  
    
    print(f"End of SQL table creation..")
    
    
def apply_hash(s):
    return hashlib.sha256(s.encode()).hexdigest()

#Insert data on db
def insert_data_on_db (conn,c,table_name,data):
    print(f"Insert data into SQL table {table_name}...")
    try:
        conn.execute("BEGIN TRANSACTION;")

        columns = ", ".join([col.replace(' ', '_').lower() for col in data.columns])
        
        for row in data.itertuples(index=False,name=None):
            #applico hash alle prime tre colonne
            row = list(row)
            row[0] = apply_hash(row[0])
            row[1] = apply_hash(row[1])
            row[2] = apply_hash(row[2])            
            
            placeholders = ", ".join("?"*len(row))
            insert=f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"            
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
            print(f"File name: {filename} to be migrated....")
            migrate_to_db(file,db_name,table_name)           
    else:
            print(f"File name: {filename} already migrated, skipping")


def get_data_migrated(db_name,table_name):
    
    if db_name is None:
        db_name = default_db_name
        print(f"Database name not provided, using default database {default_db_name}")
        
    try:
        with sqlite3.connect(db_name) as conn:                
            c = conn.cursor()
                
            columns = c.execute(f"PRAGMA table_info({table_name});")
            columns = [col[1] for col in columns]
            columns_select = [col for col in columns if col != 'id']            
                
                
            c.execute(f"SELECT {', '.join(columns_select)} FROM {table_name} ORDER BY id ASC;")
            data = c.fetchall()
             #normalizzazione dati per confronto
            convert_data = [{columns_select[i]:row[i] for i in range(len(row))} for row in data]
                
            return convert_data
    except Exception as e:
        print(f"Error during file name extraction: {e}")
        return []
    
def get_data_from_file(file):
    try:
        data= pd.read_excel(file)
        #normalizzazion dati per confronto
        convert_data = data.to_dict(orient='records')
        #applico hash alle prime tre colonne
        for  row in convert_data:
            for i,key in enumerate(row.keys()):
                if i <= 2:                
                    row[key] = apply_hash(row[key]) 
                    
        return convert_data
    except Exception as e:
        print(f"Error during data extraction: {e}")
        return []    


def get_name_of_table_on_DB(db_name):   
    if db_name is None:
        db_name = default_db_name
        print(f"Database name not provided, using default database {default_db_name}")   

    print(f"Get table names on {db_name}...")
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
    



def set_permissions_ownwer(file):
    try:
        st = os.stat(file)
        current_permissions = st.st_mode & 0o777
        if current_permissions == 0o600:
            print(f"Permissions for {file} are already 600. Only the owner can read and write the file")
            return
        else:
            os.chmod(file,0o600) # solo il proprietario puo' leggere e scrivere il file
            print(f"Permissions for {file} have been set to 600. Only the owner can read and write the file")
    
    except Exception as e:
        print(f"Error during setting permissions: {e}")


def consistency_check(file,db_name,table_name):
    
    if table_name is None:
        table_name = default_table_name
        print(f"Table name not provided, using default table name {default_table_name}")
    
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
    
   
    missing_data = []
    
    for row in data_from_file:        
        if row not in data_from_db:
                missing_data.append(row)       
       
    
    if not missing_data:
        print(f"Consistency check PASSED, data from file and data from database MATCH")
    else:
        print(f"Consistency check FAILED, Data from file and data from database do NOT MATCH")
        print(f"Missing data: {missing_data}")
        
         
    
   

     
        
#Main function
def main():
    parser = argparse.ArgumentParser(description="Migrate data from Excel to SQL")

    parser.add_argument("-f","--file",help="Path to the file .xlsx to be migrated")
    parser.add_argument("-t","--table",help="Name of the table to be created on the database")
    args = parser.parse_args()        
    
    #Initialize logger
    initialize_logger()
    if args.file is None:
        print("No file to be migrated.")
        print("---------------------")
        print("Usage:\n     python migrate_to_sql.py -f <file_path> -t <table_name>")
        print("---------------------")
        print("Example:\n     python migrate_to_sql.py -f path/data.xlsx -t test")
        print("---------------------")
        print("-f, --file : Path to the file .xlsx to be migrated")
        print("-t, --table : Name of the table to be created on the database")
        print("---------------------")
        sys.exit(0)

    if args.table is not None:
        if not check_table_name_is_valid(args.table):
            print(f"Invalid table name {args.table}")
            print("Table name must start with a letter and contain only letters, numbers, and underscores")
            sys.exit(0)
    
    file_to_be_migrate,file_path = check_exist_migration_file(args.file)
    

    if file_to_be_migrate is not None and file_path is not None:
        print("Ready for migration..\n")
        print(f"File to be migrated: {file_to_be_migrate}")
        print(f"File path: {file_path}")
        print(f"Log file: {log_file}")
        print(f"Database name : {default_db_name}")
        if args.table is not None:
            print(f"Table name : {args.table}")
        else:            
            print(f"Table name is not provided, using default table name {default_table_name}\n")

        tobemigrate = input("Are you sure you want to proceed? (y/n): ")
            
        if tobemigrate == "y":
            status_map = migration_status(args.table,file_to_be_migrate,log_file)
            migration_phase(file_path,status_map, None, args.table)    
            print("Migration phase completed...")
            tobeConsistencyCheck = input("Do you want to run a consistency check? (y/n): ")
            if tobeConsistencyCheck == "y":
                consistency_check(file_path,None,args.table)
                
                set_permissions= input(f"Do you want to set permissions to {default_db_name} file? (y/n): ")
                if set_permissions == "y":
                    set_permissions_ownwer(default_db_name)                   
                    sys.exit(0)
                else:
                    print("Permissions not set to file")
                    sys.exit(0)
            else:
                print("Consistency check aborted")
                sys.exit(0)
                    
        elif tobemigrate == "n":
                print("Migration aborted")
                sys.exit(0)
        else :
                print("Invalid input\n")
                print("Migration aborted")
                sys.exit(0)
    else:
        print("No file to be migrated")
        sys.exit(0)

    

if __name__ == "__main__":
   main()