#!/root/prediccionService/venv/bin/python

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
    # A pesar de estar definida como una funcion que crea y retorna una copia del dataset que se le es proveida
    # aun así el dataset original es modificado, no se por que
    dfToExpand['hour'] = pandas.to_datetime(dfToExpand['utc']).dt.hour
    dfToExpand['day'] = pandas.to_datetime(dfToExpand['utc']).dt.day
    dfToExpand['month'] = pandas.to_datetime(dfToExpand['utc']).dt.month
    dfToExpand['year'] = pandas.to_datetime(dfToExpand['utc']).dt.year
    return dfToExpand

# El modelo DNN existente solo necesita horas, días y minutos arbitrarios para poder generar un pronóstico relativamente fiable 
# -MSE de 41, 5 y 1.5 para HR, QFE y TS respectivamente-
# por lo tanto si se desea hacer pronostico del clima para las proximas 24 horas, basta con obtener la fecha y hora actual, generar los pasos de tiempo
# de una hora y descomponerla en caracteristicas de tiempo significativas
# aunque dado el input con el que fue entrenado el modelo, se le puede ser ingresado cualquier momento en el tiempo arbitrario sin necesidad de ser
# consecutivo

# hour day month in that order
try:
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
    query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 1;"
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() 
    print("error conexion a db")

df["utc"] = pandas.to_datetime(df["serverDate"],format='%Y-%m-%d %H:%M:%S')
df = df[['AMBIENT_TEMPERATURE','AIR_PRESSURE','HUMIDITY','utc']]
# Agrupando parametros segun hora para obtener un dataset de 24 filas
df2 = df.groupby(pandas.Grouper(key="utc",freq='H')).mean()
df2 = df2.reset_index()

# Generando fechas futuras
# Se extremadamente cuidados con los tipos de datos de now y delta
# delta hereda de now, y now dependiendo del metodo por el que se obtiene puede ser un pandas.timestamp o un datetime.datetime
# ambos tienen forma de acceso a sus atributos distintos, por lo tanto cuidar esto
# now = datetime.datetime.now()
now = pandas.to_datetime(df2[-1:]['utc'])
stackPredictors = pandas.DataFrame()
deltaStack = pandas.DataFrame()
for i in range(72):
    delta = now + datetime.timedelta(0,i*3600)
    deltaStack = deltaStack.append(pandas.DataFrame(delta))
    # deltaStack = deltaStack.append([delta])
    # temp = np.array([delta.hour,delta.day,delta.month],dtype="float32")
    temp = np.array([delta.dt.hour,delta.dt.day,delta.dt.month],dtype="float32")
    stackPredictors = stackPredictors.append(pandas.DataFrame(temp).transpose())
    
stackPredictors = stackPredictors.reset_index(drop=True)
deltaStack = deltaStack.reset_index(drop=True)
stackPredictors.columns = ['hour','day','month']

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

train_mean.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']
train_std.index = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE','hour','day','month']
stackPredictors = (stackPredictors - train_mean[3:]) / train_std[3:]

# Cargado de modelo
# dnnMultivar = tf.keras.models.load_model('../../pyCNN/deepNN-multiV2')
dnnMultivar = tf.keras.models.load_model('../../../models/dnn/')
# Realizando pronosticos
stackPreds = dnnMultivar.predict(stackPredictors)
stackPreds = pandas.DataFrame(stackPreds)
stackPreds.columns = ['AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE']
# Construyendo tabla de predicciones final, conteniendo predictores y Predicciones
stackPreds = pandas.concat([stackPreds,stackPredictors],axis=1)
# De-normalizando valores
stackPreds = stackPreds * train_std + train_mean 
# El año no se considero en el entrenamiento pues disminuia la varianza de las predicciones demasiado
stackPreds['utc'] = deltaStack

#stackPreds.plot(subplots=True)
#plt.plot(df4[0,0:24,1])
#plt.plot(df4[0,0:24,2])
#plt.grid()
#plt.show()

stackPreds.AMBIENT_TEMPERATURE = stackPreds.AMBIENT_TEMPERATURE.round(3)
stackPreds.HUMIDITY = stackPreds.HUMIDITY.round(3)
stackPreds.AIR_PRESSURE = stackPreds.AIR_PRESSURE.round(3)
#stackPreds.hour = stackPreds.hour.round(0).astype(int)
#stackPreds.day = stackPreds.day.round(0).astype(int)
#stackPreds.month = stackPreds.month.round(0).astype(int)
stackPreds.pop('hour')
stackPreds.pop('day')
stackPreds.pop('month')

# stackPreds['utc'] =stackPreds.year.astype(str)+'-'+stackPreds.month.astype(str)+'-'+stackPreds.day.astype(str)+' '+stackPreds.hour.astype(str)+':00:00'
stackPreds['utc'] = pandas.to_datetime(stackPreds['utc'])

try:
    #credentials = np.genfromtxt("../viz/scripts/pass",dtype='str')
    # credentials = np.genfromtxt("../pass",dtype='str')
    # engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    # mydb = engine.connect()
    stackPreds.to_sql('dnnPredictions',mydb,if_exists='append',index=False)
    query = "SHOW COLUMNS FROM `dnnPredictions` LIKE 'id';"
    a = mydb.execute(query)
    if a.fetchall(): 
        print("Columna id existente")
    else: 
        print("Añadiendo columna id")
        query = "ALTER TABLE dnnPredictions ADD id INT PRIMARY KEY AUTO_INCREMENT;"
        mydb.execute(query)
except:
    mydb.close() #close the connectionexcept Exception as e:
    print('Error en conexion a base de datos')

mydb.close()
engine.dispose()
## script ready to be callbable
