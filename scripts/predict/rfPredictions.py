#!/root/prediccionService/venv/bin/python

import os
import datetime
#import matplotlib as mpl
#import matplotlib.pyplot as plt
import numpy as np
import pandas
#import tensorflow as tf
import sqlalchemy
import pymysql
#from sklearn.ensemble import RandomForestRegressor
import joblib

def createTimeFeatures (dfToExpand):
    dfToExpand['hour'] = pandas.to_datetime(dfToExpand['utc']).dt.hour
    dfToExpand['day'] = pandas.to_datetime(dfToExpand['utc']).dt.day
    dfToExpand['month'] = pandas.to_datetime(dfToExpand['utc']).dt.month
    dfToExpand['year'] = pandas.to_datetime(dfToExpand['utc']).dt.year
    return dfToExpand

try:
    #credentials = np.genfromtxt("../viz/scripts/pass",dtype='str')
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
    # Obteniendo ultimos 720 (60*12) registros, equivalentes a una hora de datos, o mas, en caso de que haya faltantes
    # y evitar lecturas fluctuantes
    query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 720;"
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() #close the connectionexcept Exception as e:
    #print('Error en conexion a base de datos')

# Inversion del dataframe 
df = df.iloc[::-1] 
df["utc"] = pandas.to_datetime(df["serverDate"],format='%Y-%m-%d %H:%M:%S')
# Por ahora se ignora id de sensor
df = df[['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','utc']]
# Generando variables de tiempo para alimentar el modelo
df2 = df.groupby(pandas.Grouper(key="utc",freq='H')).mean()

# Obteniendo la hora actual para establecerla como inicio de las predicciones
# now = datetime.datetime.now()
# lastHour = df2[-1:].copy()
# lastHour.hour = now.hour
# lastHour.day = now.day
# lastHour.month = now.month
# df2 = df2.append(lastHour)
# Abandonado: Se decide iniciar las predicciones desde el momento del ultimo registro
df2 = df2.reset_index()
now = pandas.to_datetime(df2[-1:]['utc'])

# El modelo random forest al no ser entrenado con valores normalizados no necesita que los nuevos valores a evaluar tengan este tratamiento
# Si el orden de los datos debe ser el mismo
# Temperatura - Humedad - Presion - hora - dia - mes - año
df2 = createTimeFeatures(df2)
df2 = df2[['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month','year']]

# Carga de modelo
# Caracteristicas: Input: 7 variables, Output: 3 variables, Predicciones: Pasos de 1 hora
rfMultivar=joblib.load('../../../models/rf')

deltaStack = pandas.DataFrame()
stackPreds = pandas.DataFrame()
x = rfMultivar.predict(df2[-1:])

#stackPreds = pandas.DataFrame(columns=row.keys())
# para predecir mas horas basta con cambiar 72 a un valor mas alto
for i in range(0,72):
    delta = now + datetime.timedelta(0,i*3600)
    deltaStack = deltaStack.append(pandas.DataFrame(delta))
    #temp = np.array([x,y,z,delta.hour,delta.day,delta.month],dtype="float32")
    temp = np.array([x[0,0],x[0,1],x[0,2],delta.dt.hour,delta.dt.day,delta.dt.month,delta.dt.year],dtype="float32")
    stackPreds = stackPreds.append(pandas.DataFrame(temp).transpose())
    # Formateando registro para alimentar el modelo
    temp_RE = np.reshape(temp,(-1,7))
    x = rfMultivar.predict(temp_RE)

stackPreds = stackPreds.reset_index(drop=True)
deltaStack= deltaStack.reset_index(drop=True)
#stackPreds.plot(subplots=True)
#plt.show()

# Formateo de datos para subida a base de datos
stackPreds.columns = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month','year']

stackPreds.AMBIENT_TEMPERATURE = stackPreds.AMBIENT_TEMPERATURE.round(3)
stackPreds.HUMIDITY = stackPreds.HUMIDITY.round(3)
stackPreds.AIR_PRESSURE = stackPreds.AIR_PRESSURE.round(3)
stackPreds['utc'] = deltaStack
stackPreds.pop('hour')
stackPreds.pop('day')
stackPreds.pop('month')
stackPreds.pop('year')

stackPreds['utc'] = pandas.to_datetime(stackPreds['utc'])

try:
    stackPreds.to_sql('rfPredictions',mydb,if_exists='append',index=False)
    query = "SHOW COLUMNS FROM `rfPredictions` LIKE 'id';"
    a = mydb.execute(query)
    if a.fetchall(): 
        print("Columna id existente")
    else: 
        print("Añadiendo columna id")
        query = "ALTER TABLE rfPredictions ADD id INT PRIMARY KEY AUTO_INCREMENT;"
        mydb.execute(query)
except:
    mydb.close() #close the connectionexcept Exception as e:
    #print('Error en conexion a base de datos')

mydb.close()
engine.dispose()
## script ready to be callbable
