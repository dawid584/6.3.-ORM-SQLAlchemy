import csv
from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy import create_engine

def create_enginee():
   engine = create_engine('sqlite:///database.db' )
   return engine 

def table(engine):
   meta = MetaData()   
   students = Table(
   'address', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('country', String),
   Column('state', String))
   meta.create_all(engine)
   engine.table_names()
   return engine , students , meta

def table_2(engine):
   meta = MetaData()   
   students_2 = Table(
   'address_2', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', String),
   Column('tobs', String))
   meta.create_all(engine)
   engine.table_names()
   return engine , students_2 , meta
   

def input_data(engine ,students ,students_2):
   
   
   with open('C:\Kodilla\learning-git-10\cleanmeasure.csv', 'r') as read_file:
# first table
      csv_1= csv.reader(read_file , delimiter=',')
      b=0
      for row in csv_1:
         if b==0:
            b +=1
            continue
         else:    
            ins = students_2.insert().values(station=row[0], date=row[1] ,precip=row[2] ,tobs=row[3])
            conn = engine.connect()
            result = conn.execute(ins)

   with open('clean_stations.csv', 'r') as read_file:
#second table
   
      csvreader = csv.reader(read_file, delimiter=',')
      a =0
      for row in csvreader:
         if a==0:
            a +=1
            continue
         else:   
            ins = students.insert().values(station=row[0], latitude=row[1] ,longitude=row[2] ,elevation=row[3] ,name=row[4],country=row[5] , state=row[6])
            conn = engine.connect()
            result = conn.execute(ins)
   
    

def result():
   rt = engine.execute("SELECT * FROM address").fetchall()
   for r in rt:
      print(r)

if __name__ == "__main__": 
   engine  = create_enginee()
   table(engine)
   table_2(engine)
   engine , students , meta = table(engine)
   engine , students_2 , meta = table_2(engine)
   input_data(engine , students, students_2)
   result()
   