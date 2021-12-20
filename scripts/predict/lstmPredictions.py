#!/root/prediccionService/venv/bin/python
# Predictor maker
# To be run at least one hour after retraining
import os
import datetime
#import matplotlib as mpl
#import matplotlib.pyplot as plt
import numpy as np
import pandas
import tensorflow as tf
import sqlalchemy
import pymysql

def createTimeFeatures (dfToExpand):
    dfToExpand['hour'] = pandas.to_datetime(dfToExpand['utc']).dt.hour
    dfToExpand['day'] = pandas.to_datetime(dfToExpand['utc']).dt.day
    dfToExpand['month'] = pandas.to_datetime(dfToExpand['utc']).dt.month
    dfToExpand['year'] = pandas.to_datetime(dfToExpand['utc']).dt.year
    return dfToExpand

try:
    # para rescatar las ultimas 24 horas: 24 * 60 * 12
    # credentials = np.genfromtxt("../viz/scripts/pass",dtype='str')
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
    #stackPreds = pandas.read_sql(query,mydb)
    #query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 17280;"
    query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 34000;"
    # el ultimo dia solo hubo 5442 registros q no alcanzan pa 24 horas
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() 
    print("error conexion a db")

df = df.iloc[::-1] # dando vuelta el dataframe
df["utc"] = pandas.to_datetime(df["serverDate"],format='%Y-%m-%d %H:%M:%S')
df = df[['AMBIENT_TEMPERATURE','AIR_PRESSURE','HUMIDITY','utc']]

# Agrupando parametros segun hora para obtener un dataset de 24 filas
df2 = df.groupby(pandas.Grouper(key="utc",freq='H')).mean()

# Rellenar gaps de hora si es que existen una serie de meotodos utilizables
# solo por ahora, rellenamos los periodos de las ultimas 24 horas faltantes mediante interpolado lineal
df2 = df2.interpolate(method='linear')

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

# Generando variables de tiempo para alimentar los modelos
# basta con que se genere 1 registro cada una hora para poder alimentar el modelo
df2 = createTimeFeatures(df2)

# Normalizando manualmente
# el modelo fue entrenado con valores normalizados generados en base a metricas -promedio, desviacion estandar- del dataset de 
# entrenamiento, por lo tanto, estas metricas han de calcularse también para respetar la normalizacion (promedios y dev_est)
# con los que fue entrenado el modelo

df = pandas.read_csv("../dataPreprocessed.csv")
df = createTimeFeatures(df)
dates_df = df.pop('year')
dates_df = df.pop('utc')
train_mean = df.mean()
train_std = df.std()
df = 0
train_mean.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']
train_std.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']

# you can straight substract a list from dataframe without giving key values!
df3 = df2[['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']]
# se ocuparan unicamente las ultimas 24 horas para la generación de pronosticos
df3 = df3[-24:]
df3 = df3.reset_index(drop=True)
df3 = (df3 - train_mean) / train_std 
df3 = df3.to_numpy()
# reshape a valores actuales
# to meet what input_shape the model expects
winSize = 24 
numFeatures = 6 
df3 = np.reshape(df3,(-1,winSize,numFeatures))
# df3 just became a 3 dimensional array jesus...

# Cargado de modelo
lstmModel = tf.keras.models.load_model('../../../models/lstm')

stackPreds = pandas.DataFrame()
deltaStack = pandas.DataFrame()
lastTrainBatch = np.array(df3)
lastTrainBatch = lastTrainBatch.reshape((1,winSize,numFeatures))
# preparando variables para entrar al loop autoregresivo
x = lstmModel.predict(lastTrainBatch)

def norm(value,index):
    #value = (value - modelMean[index]) / modelStd[index]
    value = (value - train_mean[index]) / train_std[index]
    return value

for i in range(0,72):
    delta = now + datetime.timedelta(0,i*3600)
    deltaStack = deltaStack.append(pandas.DataFrame(delta))
    #temp = np.array([x,y,z,delta.hour,delta.day,delta.month],dtype="float32")
    # temp = np.array([x,y,z,norm(delta.hour,3),norm(delta.day,4),norm(delta.month,5)],dtype="float32")
    temp = np.array([x[0,0],x[0,1],x[0,2],norm(delta.dt.hour,3),norm(delta.dt.day,4),norm(delta.dt.month,5)],dtype="float32")
    print(delta.dt.hour,delta.dt.day,delta.dt.month)
    stackPreds = stackPreds.append(pandas.DataFrame(temp).transpose())
    # df3 = np.vstack((df3[0],temp))
    # df4 = np.vstack((df4,temp))
    cde = np.reshape(df3,(24,6))
    df3 = np.vstack((cde,temp))
    df3 = np.delete(df3, (0), axis=0)
    df3 = np.reshape(df3,(-1,24,6))
    x = lstmModel.predict(df3)

stackPreds = stackPreds.reset_index(drop=True)
deltaStack = deltaStack.reset_index(drop=True)
stackPreds.columns = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']

#stackPreds = stackPreds.reset_index()

# de-normalizando predicciones
stackPreds =stackPreds*train_std+train_mean
# stackPreds.plot(subplots=True)
# plt.show()
# plt.plot(stackPreds['AMBIENT_TEMPERATURE'])

# Formateo de datos para carga de estos a DB
stackPreds.AMBIENT_TEMPERATURE = stackPreds.AMBIENT_TEMPERATURE.round(3)
stackPreds.HUMIDITY = stackPreds.HUMIDITY.round(3)
stackPreds.AIR_PRESSURE = stackPreds.AIR_PRESSURE.round(3)
stackPreds['utc'] = deltaStack
stackPreds.pop('hour')
stackPreds.pop('day')
stackPreds.pop('month')

stackPreds['utc'] = pandas.to_datetime(stackPreds['utc'])

try: 
    stackPreds.to_sql('predictions',mydb,if_exists='append',index=False)
    query = "SHOW COLUMNS FROM `predictions` LIKE 'id';"
    a = mydb.execute(query)
    if a.fetchall(): 
        print("Columna id existente")
    else: 
        print("Añadiendo columna id")
        query = "ALTER TABLE predictions ADD id INT PRIMARY KEY AUTO_INCREMENT;"
        mydb.execute(query)
except:
    print("error en conexion a db")

#print(stackPreds)
mydb.close()
engine.dispose()

