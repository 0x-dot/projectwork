# Project work finale Tema n.2 Privacy e sicurezza aziendale
Traccia del Project work n.2.1  
Ruolo della privacy e sull'importanza dl GDPR

Linguaggio utilizzato  
Python version --->  **3.10.12**  
Database Utilizzato ---->  **SQLite**

Primo Script: **generate_data.py**  
  Crea un file **.xlsx** a partire da dati generati casualmente.  
  Lo Script accetta un parametro opzionale per il nome del file da generare.  
  
     -n nomefile : con questo flag si può personalizzare il nome del file.  
     
      Se non viene passato nessun input lo script genererà automaticamente un nome univoco per i file da migrare.
      
  Il file generato sarà salvato all'interno della cartealla **generated_data**.

    
Secondo Script **migrate_to_sql.py**  
  Legge i dati dal file **.xlsx** crea una tabella **SQL** con i dati presenti all'interno del file e ne valida la correttezza .
  
      Se l'operazione riesce il nome del file verrà registrato in un file di log.
  
  Il file di log potranno essere visualizzati all'interno della cartella **migration_log**.


