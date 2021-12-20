#!/root/prediccionservice/venv/bin/python

import os
import datetime
#import matplotlib.pyplot as plt
import numpy as np
import pandas
import sqlalchemy
import pymysql
from statsmodels.tsa.arima.model import ARIMA
#import joblib

def createTimeFeatures (dfToExpand):
    dfToExpand['hour'] = pandas.to_datetime(dfToExpand['utc']).dt.hour
    dfToExpand['day'] = pandas.to_datetime(dfToExpand['utc']).dt.day
    dfToExpand['month'] = pandas.to_datetime(dfToExpand['utc']).dt.month
    dfToExpand['year'] = pandas.to_datetime(dfToExpand['utc']).dt.year
    return dfToExpand

# Carga y creacion de caracteristicas del dataset
#df = pandas.read_csv("../data/local/dataPreprocessed.csv")
# Explicitar como index, la columna que contiene las marcas de tiempo de las lecturas para facilitar su tratamiento como series de tiempo
#df = pandas.read_csv('../data/local/dataPreprocessed.csv', parse_dates=[3], index_col=3)
#df['utc'] = pandas.to_datetime(df['utc'])
#df.plot(subplots=True)
#plt.show()
# Leer desde un set de datos preprocesado es no recomendado en el modelo ARIMA pues su uso de recursos es intensivo y resulta en un modelo pesado (~2gb)

### Por lo tanto se carga y predice a corto-plazo desde la db ###

try:
    #credentials = np.genfromtxt("../viz/scripts/pass",dtype='str')
    credentials = np.genfromtxt("../pass",dtype='str')
    engine = sqlalchemy.create_engine("mysql+pymysql://"+credentials[0]+":"+credentials[1]+"@"+credentials[2]+"/"+credentials[3] )
    mydb = engine.connect()
    # para rescatar las ultimas 24 horas: 24 * 60 * 12
    #query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 17280;"
    query = "SELECT * FROM WEATHER_MEASUREMENT ORDER BY ID DESC LIMIT 34560;"
    # la representatividad de los ultimos registros segun hora puede variar por lo que es muy probable que haya que preprocesar
    df = pandas.read_sql(query,mydb)
except:
    mydb.close() 
    print("Error de conexion a base de datos")

df = df.iloc[::-1] # dando vuelta el dataframe
df["utc"] = pandas.to_datetime(df["serverDate"],format='%Y-%m-%d %H:%M:%S')
df = df[['AMBIENT_TEMPERATURE','AIR_PRESSURE','HUMIDITY','utc']]
df.head()
# Agrupando registros por hora y fecha según promedio de registros, a la vez que detectar e inferir aquellos gaps de horas faltantes, 
# ademas transformar la columna de fechas en index
# Este es el mejor preprocesamiento de dato que hayas hecho, traspasar a dataPreprocessing.py
df2 = df.groupby(pandas.Grouper(key="utc",freq='H')).mean()
# Rellenar gaps de hora, existe una serie de meotodos utilizables
df2 = df2.interpolate(method='linear')
# USA EL METODO .copy() PARA COPIAR DATAFRAMES, DE LO CONTRARIO LA VARIABLE CREADA ES UNA REFERENCIA AL DATAFRAME ORIGINAL NO UNA COPIA INDEPENDIENTE
series = df2.copy()
series.index = series.index.to_period('H')

stackPreds = pandas.DataFrame()

# Los modelos ARIMA -y SARIMAX- solo son univariables, por lo debemos parsear solo parte de la serie de tiempo para poder crear los modelos

model = ARIMA(series['AMBIENT_TEMPERATURE'], order=(24,1,1))
model_fit = model.fit()

# Uno de las mejores caracteristicas de los modelos ARIMA (o al menos su implementacion en pyhon) es que puede hacer bulk de predicciones con tan solo un
# valor futuro fijo de la serie de tiempo (o sea una fecha futura arbitraria), sin embargo al mismo tiempo dependera complemente de los ultimos 
# steps que el mismo modelo va creando al paso en vez de valerse de su 'memoria' sobre la estacionalidad de los eventos con los que fue alimentado el 
# modelo (capacidad similar a los metodos .predict de modelos ML alimentados de forma exclusiva por caracteristicas de tiempo, como mes y horas)

# Definiendo horas en el futuro a predecir
future = 72

stackPreds['AMBIENT_TEMPERATURE'] = model_fit.forecast(future)
model = ARIMA(series['HUMIDITY'], order=(24,1,1))
model_fit = model.fit()
stackPreds['HUMIDITY'] = model_fit.forecast(future)
model = ARIMA(series['AIR_PRESSURE'], order=(24,1,1))
model_fit = model.fit()
stackPreds['AIR_PRESSURE'] = model_fit.forecast(future)

# El index actual posee ciertas restricciones de dato al ser timestamp + periodico, por lo tanto tenemos que crear las caracteristicas de tiempo
# de forma manual, y luego dropear la fila con el tipo de dato 'date'
stackPreds = stackPreds.reset_index()
stackPreds.columns = ['utc','AMBIENT_TEMPERATURE','HUMIDITY','AIR_PRESSURE']

stackPreds.AMBIENT_TEMPERATURE = stackPreds.AMBIENT_TEMPERATURE.round(3)
stackPreds.HUMIDITY = stackPreds.HUMIDITY.round(3)
stackPreds.AIR_PRESSURE = stackPreds.AIR_PRESSURE.round(3)

#stackPreds['utc'] = pandas.to_datetime(stackPreds['utc'])

try:
    stackPreds.to_sql('arimaPredictions',mydb,if_exists='append',index=False)
    query = "SHOW COLUMNS FROM `arimaPredictions` LIKE 'id';"
    a = mydb.execute(query)
    if a.fetchall(): 
        print("Columna id existente")
    else: 
        print("Añadiendo columna id")
        query = "ALTER TABLE arimaPredictions ADD id INT PRIMARY KEY AUTO_INCREMENT;"
        mydb.execute(query)
except:
    mydb.close() #close the connectionexcept Exception as e:
    print('Error en conexion a base de datos')

mydb.close()
engine.dispose()
## script ready to be callable
