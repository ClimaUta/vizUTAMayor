#!/root/prediccionService/venv/bin/python

import os
import datetime
#import matplotlib as mpl
#import matplotlib.pyplot as plt
import numpy as np
import pandas
import tensorflow as tf
import keras as keras
import sqlalchemy
import pymysql

try:
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
    query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 34000;"
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() 
    print("error conexion a db")

df["utc"] = pandas.to_datetime(df["serverDate"],format='%Y-%m-%d %H:%M:%S')
df = df[['AMBIENT_TEMPERATURE','AIR_PRESSURE','HUMIDITY','utc']]
# Agrupando parametros segun hora para obtener un dataset de 24 filas
df2 = df.groupby(pandas.Grouper(key="utc",freq='H')).mean()
df2 = df2.reset_index()

# Rellenar gaps de hora si es que existen una serie de meotodos utilizables
# solo por ahora, rellenamos los periodos de las ultimas 24 horas faltantes mediante interpolado lineal
# Separamos las fechas-horas para codificarlas aparte para el modelo
times_df = df2.pop("utc")
df2 = df2.interpolate(method='linear')

# Generador de fechas futuras
## Como el modelo transformer es de tipo secuencia a secuencia no se necesita explicitar el tiempo como input para el modelo
now = pandas.to_datetime(times_df[-1:])
deltaStack = []
for i in range(72):
    delta = now + datetime.timedelta(0,i*3600)
    deltaStack.append(delta)
    
deltaStack = pandas.DataFrame(deltaStack)
deltaStack = deltaStack.reset_index(drop=True)
deltaStack.columns = ["utc"]

# Normalizando manualmente
# el modelo fue entrenado con valores normalizados generados en base a metricas -promedio, desviacion estandar- del dataset de 
# entrenamiento, por lo tanto, estas metricas han de calcularse también para respetar la normalizacion (promedios y dev_est)
# con los que fue entrenado el modelo
df = pandas.read_csv("../dataPreprocessed.csv")
df = df[["Ts_Valor","HR_Valor","QFE_Valor"]]
train_mean = df.mean()
train_std = df.std()
train_mean.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE']
train_std.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE']

# Cargado de modelo
transformer = tf.keras.models.load_model('../../../models/transformer/')

# Generando predicciones
## Preparación de tiempo codificado e inputs para el modelo
winSize = 24
timestamp_s = times_df[-winSize:].map(pandas.Timestamp.timestamp)
day = 24*60*60
year = (365.2425)*day
timeEncode = pandas.DataFrame()
timeEncode['Day sin'] = np.sin(timestamp_s * (2 * np.pi / day))
timeEncode['Year sin'] = np.sin(timestamp_s * (2 * np.pi / year))

df3 = df2[-winSize:]
df3 = (df3 - train_mean)/train_std
df3['dsin']=timeEncode['Day sin']
df3['ysin']=timeEncode['Year sin']

# Elaboración de predicciones
num_features = 5
df3 = df3.to_numpy()
df3 = df3.reshape((-1,winSize,num_features))
df3.shape
x = transformer.predict(df3)
x.shape

# Como son solo dos pasos al futuro, se realiza de forma no iterativa
a = transformer.predict(x)
b = transformer.predict(a)
x = np.hstack((x,a))
x = np.hstack((x,b))
x = x.reshape((72,5))

# Formateo de predicciones para db
predictions = pandas.DataFrame(x)
predictions.columns = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','dsin','ysin']
predictions.pop('dsin')
predictions.pop('ysin')
predictions = predictions * train_std + train_mean 
predictions = pandas.concat([predictions,deltaStack],axis=1)

predictions.AMBIENT_TEMPERATURE = predictions.AMBIENT_TEMPERATURE.round(3)
predictions.HUMIDITY = predictions.HUMIDITY.round(3)
predictions.AIR_PRESSURE = predictions.AIR_PRESSURE.round(3)


try:
    predictions.to_sql('transformerPredictions',mydb,if_exists='append',index=False)
    query = "SHOW COLUMNS FROM `transformerPredictions` LIKE 'id';"
    a = mydb.execute(query)
    if a.fetchall(): 
        print("Columna id existente")
    else: 
        print("Añadiendo columna id")
    query = "ALTER TABLE transformerPredictions ADD id INT PRIMARY KEY AUTO_INCREMENT;"
    mydb.execute(query)
except:
    mydb.close() #close the connectionexcept Exception as e:
    print('Error en conexion a base de datos')

mydb.close()
engine.dispose()
## script ready to be callbable
