import os
import sys
import datetime

#directory 
directory = "generated_data"
logDirectory="migration_log"
logFile = os.path.join(logDirectory,"migration.log")


LOG_FILE_NOT_FOUND = "log_file_not_found"
FILE_NOT_MIGRATED ="file_not_migrated"
FILE_MIGRATED= "file_migrated"


#inizializzazione logger file
def initialize_logger():
    
    try:   
        if not os.path.exists(logDirectory):
            os.makedirs(logDirectory)
            print("Directory ", logDirectory, " Created ")
        
        if not os.path.exists(logFile):
            with open(logFile,'w') as log:
                log.write(f"Log di migrazione {datetime.datetime.now()}\n")
        
        else:
            print("Logger file ", logFile, " already exists")
        
            
    except Exception as e:
        print(f"Errore durante inizializzazione logger: {e}")


def write_log(fileName):
    try:
        with open(logFile,'a') as log:
                log.write(f"{fileName}\n")
    
    except Exception as e:
        print(f"Errore durante la scrittura del file di log: {e}")


#Questa funzione controlla se esistono file .xlxs da migrare e restituisce
#la lista dei file .
def check_exist_migration_file():
    #controllo esistenza directory 
    if not os.path.exists(directory):       
       print("Directory not exist")
       return []
   
        #lista file .xlxs
    filesToMigrate = [ file for file in os.listdir(directory) if file.endswith(".xlsx")]

    if not filesToMigrate:
        print("Non sono presenti file .xlsx")
        return []
        
    print(f"Sono stati trovati numero {len(filesToMigrate)} file nella directory")

    for file in filesToMigrate:
        print(" --- ",file)
    
    return filesToMigrate



def migration_status(fileList, logFile):
    statusMap ={}
    
    if os.path.exists(logFile):
        with open(logFile,'r') as log:
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



def migrate_to_db():
    """ TBD.. """
    
    

def migratio_phase(fileList,statusMap):
    
    for filename in fileList:
        
        status = statusMap.get(filename)
        
        if status == LOG_FILE_NOT_FOUND:
            print(f"file di log non trovato impossibile continuare")
        
        elif status == FILE_NOT_MIGRATED:
            print(f"file {filename} da migrare")
            print(f"Migrazione in corso..")
            write_log(filename)
        else:
            print(f"file {filename} gia migrato, nessuna azione prevista")
        
        
        
    



def main():
    initialize_logger()
    filesToBeMigrate = check_exist_migration_file()

    if filesToBeMigrate:
        print("pronto per la migrazione")
        tobemigrate = input("vuoi procedere con la migrazione? (y/n): ")
         
        if tobemigrate == "s" or "S":
            statusMap = migration_status(filesToBeMigrate,logFile)
            migratio_phase(filesToBeMigrate,statusMap)            
        else:
            print("Migrazione annullata")

    



if __name__ == "__main__":
   main()