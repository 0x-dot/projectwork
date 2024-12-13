
import os
import sys
import re
from faker import Faker
import pandas as pd
import datetime
import argparse

#directory utilizzata per salvare i file .xlsx
directory = "generated_data"

fake = Faker()


#funzione per creare la directory
def create_directory():
   if not os.path.exists(directory):
       os.makedirs(directory)
       print("Directory ", directory, " Created ")
   else:
       print("Directory ", directory, " already exists")


#funzione per notificare all'utete che il nome passato come argomento contiene caratteri non validi
#l'espressione regolare
def invalid_name(filename):
   #controlliamo se filename contiene caratteri non validi e notifichiamo l'utente   
   if not re.match(r'^[a-zA-Z0-9_-]+$',filename):
       print("Nome file non valido, il nome del file deve contenere solo caratteri alfanumerici")
       return True
   return False


def generate_name(filename):    
    return f"{filename}_{datetime.datetime.now().strftime('%d-%m-%Y_%H-%M_%S')}"



#funzione per generare dataset
def generate_dataset():
   dataset =[]
   for _ in range(10):
       entry = {
           'nome':fake.first_name(),
           'cognome':fake.last_name(),
           'email':fake.email(),
           'telefono':fake.numerify(text='###-###-####') #cosi scelgo il tipo di formattazione del numero "
       }
       dataset.append(entry)   
   return dataset

#crezione file
def create_file(filename, dataset):

    if invalid_name(filename):
        return
    if not dataset:
        print("Dataset non valido")
        return
    

    uniqueFileName=generate_name(filename)

    path = os.path.join(directory, f"{uniqueFileName}.xlsx")
    print("path",path)

    try:    
        df=pd.DataFrame(dataset)
        df.to_excel(path, index=False)
        print("File creato con successo")
        print("Percorso del file", path)
    except Exception as e:
        print("Errore durante la creazione del file",e)



def parser():
    parser = argparse.ArgumentParser(description="Generatore di file .xlsx con dati casuali.")
    parser.add_argument('-n', '--name', type=str, help="Nome del file da generare", default="file")
    return parser.parse_args()
    

def main():
   create_directory()
   
   arg=parser()

   dataset = generate_dataset()
  

   create_file(arg.name, dataset)




if __name__ == "__main__":
   main()