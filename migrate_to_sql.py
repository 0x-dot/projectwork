import os
import sys
import datetime
import sqlite3
import pandas as pd

#directory 
directory = "generated_data"
log_directory="migration_log"
log_file = os.path.join(log_directory,"migration.log")

db_name = "migration.db"

LOG_FILE_NOT_FOUND = "log_file_not_found"
FILE_NOT_MIGRATED ="file_not_migrated"
FILE_MIGRATED= "file_migrated"



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
def check_exist_migration_file():    
    if not os.path.exists(directory):       
       print(f"Directory {directory} not found.")
       return []
       
    filesToMigrate = [ file for file in os.listdir(directory) if file.endswith(".xlsx")]

    if not filesToMigrate:
        print("No file avaiable for migration.")
        return []
        
    print(f"Number {len(filesToMigrate)} files were detected in the directory")
    for file in filesToMigrate:
        print(" --- ",file)
    
    return filesToMigrate


#retunr status of migration file 
def migration_status(fileList, log_file):
    statusMap ={}
        
    if os.path.exists(log_file):
        with open(log_file,'r') as log:
            fileMigrated = {line.strip() for line in log.readlines()}
            
        for filename in fileList:           
            if filename in fileMigrated:
                statusMap[filename]= FILE_MIGRATED
            else:
                statusMap[filename]=FILE_NOT_MIGRATED       
            
    else:
        for filename in fileList:
            statusMap[filename]=LOG_FILE_NOT_FOUND
    
    
    return statusMap

#Get name of file 
def get_name_of_file(filename):    
    if "@"  not in filename:
        raise ValueError("Invalid file name {filename}, missing '@' flag")
    
    return filename.split("@",1)[0]
    


#Migrate data to db
def migrate_to_db(filename):
    print(f"Migration in progress..")
    
    try:    
       with sqlite3.connect(db_name) as conn:
        c = conn.cursor()
                
        data= pd.read_excel(os.path.join(directory,filename))
        table_name= get_name_of_file(filename)
        
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
def migration_phase(fileList,statusMap):    
    for filename in fileList:        
        status = statusMap.get(filename)
        
        if status == LOG_FILE_NOT_FOUND:
            print(f"Log file not found, migration aborted")
            return
        
        elif status == FILE_NOT_MIGRATED:
            print(f"File {filename} to be migrated")
            migrate_to_db(filename)            
        else:
            print(f"File {filename} already migrated, skipping")


def get_name_of_files_migrated():
    try:
        with open(log_file,'r') as log:
            file_migrated = {line.strip() for line in log.readlines()}
            return file_migrated
    except Exception as e:
        print(f"Error during file name extraction: {e}")
        return []

def get_name_of_table_on_DB():
    print(f"Get name of table on database {db_name}...")
    try:
        with sqlite3.connect(db_name) as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = c.fetchall()
            return tables
    except Exception as e:
        print(f"Error during table name extraction: {e}")
        return []
    


def consistency_check():
    print(f"Consistency check in progress...")
    tablesName = get_name_of_table_on_DB()
    print(f"Tables in database: {tablesName}")
    migratedFiles = get_name_of_files_migrated()    
    
    for file in migratedFiles:
        tableName = get_name_of_file(file)
        if tableName not in tablesName:
            print(f"Table {tableName} not found in database")
        else:
            print(f"Table {tableName} found in database")
    
    
    
    
    
    
    
    

     
        
#Main function
def main():
    initialize_logger()
    files_to_be_migrate = check_exist_migration_file()

    if files_to_be_migrate:
        print("Ready for migration..")
        tobemigrate = input("Are you sure you want to proceed? (y/n): ")
         
        if tobemigrate == "y":
            status_map = migration_status(files_to_be_migrate,log_file)
            migration_phase(files_to_be_migrate,status_map)     
            print("Migration completed")
            tobeConsistencyCheck = input("Do you want to perform a consistency check? (y/n): ")
            if tobeConsistencyCheck == "y":
                consistency_check()
            else:
                print("Consistency check aborted")
                   
        else:
            print("Migration aborted")

    

if __name__ == "__main__":
   main()