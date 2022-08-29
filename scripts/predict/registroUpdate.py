#!/root/prediccionService/venv/bin/python

import os
import datetime
import numpy as np
import pandas
import sqlalchemy
import pymysql

# obteniendo ultima hora guardada por el registroUpdater en /assets/img/wsm.csv

try:
    #credentials = np.genfromtxt("../viz/scripts/pass",dtype='str')
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
except:
    print("error al conectar a db")

os.chdir("/var/www/clima.uta.cl/src/assets/img/")
os.system("head -1 WEATHER_MEASUREMENT.csv > wsm2.csv")
os.system("tail -n 1 WEATHER_MEASUREMENT.csv >> wsm2.csv")
a = pandas.read_csv('wsm2.csv',sep=',',quotechar='"')
#print(a["serverDate"])
#query = "SELECT * FROM WEATHER_MEASUREMENT WHERE serverDate > '"+a.serverDate.tolist()[0]+"';"
#df = pandas.read_sql(query,mydb)

try:
    query = "SELECT * FROM WEATHER_MEASUREMENT WHERE serverDate > '"+a.serverDate.tolist()[0]+"';"
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() #close the connectionexcept Exception as e:
    #print('Error en conexion a base de datos')

df.to_csv('WEATHER_MEASUREMENT.csv',mode='a',header=False,index=False,quotechar='"',quoting=1)

mydb.close()
engine.dispose()
## script ready to be callbable
