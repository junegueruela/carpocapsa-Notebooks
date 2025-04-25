#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import pandas as pd
import warnings
import datetime
import sqlalchemy
from sqlalchemy import create_engine, text
##Ignoramos los warnings
warnings.filterwarnings('ignore')
## Utilizamos mejor sqlalchemy
__engine = create_engine('mysql+mysqlconnector://Plagas:plagas@127.0.0.1:3306/Plagas')
##__engine = create_engine('mssql+pyodbc://plagas:Plagas@DESKTOP-DOEKCPE\\SQLEXPRESS/Plagas?driver=ODBC+Driver+17+for+SQL+Server')
date_format= '%Y-%m-%d'


## Primitivas genéricas: Insert, select...
## Me inserta un dataframe en una tabla
def insertaTabla(tabla, df):
    df.to_sql(tabla, con=__engine, if_exists='append', index=False)

## Funcion genérica que me ejecuta una query 
def ejecuteQuery(query):
    df = pd.read_sql_query(query, con=__engine)
    return df


## GESTIÓN DE LOS VUELOS
## Borramos un vuelo por su id
def borraVuelo(id):
    sql="delete from VuelosCarpo where idVuelo = :id"
    with __engine.connect() as connection:
        connection.execute(text(sql), {"id": id})
        connection.commit()  # Asegúrate de hacer commit si es necesariOS

## Le pasamos un dataframe con los de una captura.
def insertarVuelo(df):
    insertaTabla('VuelosCarpo',df)

## Nos devuelve un dataframe con los datos de vuelos para un Municipio  dado entre fechaMin y fechaMax
def getDatosVueloMunicipio(municipio,fechaMin='2005-01-01',fechaMax=datetime.datetime.today().strftime("%Y-%m-%d")):
    queryDatos="SELECT idMunicipio, fecha, avg(valor) as numVuelos" \
    + ' FROM VuelosCarpo VC, Terminos T' \
    +      " WHERE  T.IdTermino = VC.IdTermino and IdMunicipio="+municipio+" AND fecha between '"+fechaMin+"' AND '"+fechaMax+"' group by fecha, IdMunicipio order by fecha;"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_DatosTiempo['fecha']=pd.to_datetime(df_DatosTiempo['fecha'])
    return df_DatosTiempo

## Nos devuelve un dataframe con los datos de vuelos para un Municipio  dado entre fechaMin y fechaMax
def getDatosVueloMunicipioTermino(municipio,fechaMin='2005-01-01',fechaMax=datetime.datetime.today().strftime("%Y-%m-%d")):
    queryDatos="SELECT T.idMunicipio idMunicipio, T.idTermino idTermino, fecha, valor as numVuelos" \
    + ' FROM VuelosCarpo VC, Terminos T' \
    +      " WHERE  T.IdTermino = VC.IdTermino and T.IdMunicipio="+municipio+" AND fecha between '"+fechaMin+"' AND '"+fechaMax+"' order by fecha;"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_DatosTiempo['fecha']=pd.to_datetime(df_DatosTiempo['fecha'])
    return df_DatosTiempo

## Nos devuelve un dataframe con los datos de vuelos para un término dado entre fechaMin y fechaMax
def getDatosVuelos(termino,fechaMin='2005-01-01',fechaMax=datetime.datetime.today().strftime("%Y-%m-%d")):
    queryDatos="SELECT fecha, valor as vuelos" \
    + ' FROM VuelosCarpo' \
    +      " WHERE idTermino="+termino+" AND fecha between '"+fechaMin+"' AND '"+fechaMax+"' order by fecha desc;"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_DatosTiempo['fecha']=pd.to_datetime(df_DatosTiempo['fecha'])
    return df_DatosTiempo


## GESTIÓN DE LOS DATOS CLIMÁTICOS
## Le pasamos un dataframe con los datos meteorológicos a insertar y nos los inserta.
def insertarDatosTiempo(df):
    insertaTabla('TemperaturasDiarias',df)


### Obtención de la mayor fecha con datos para una estación en concreto
def getFechaMaxima(estacion):
    date_format= '%Y-%m-%d'
    maxT='select max(fecha) as fecha_MAX from TemperaturasDiarias where Estacion='+estacion
    df_fecha=ejecuteQuery( maxT)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df_fecha["fecha_MAX"][0]


### Obtención de la mayor fecha con datos para una estación en concreto
def getFechaMinima(estacion):
    date_format= '%Y-%m-%d'
    minT='select min(fecha) as fecha_MIN from TemperaturasDiarias where Estacion='+estacion
    df_fecha=ejecuteQuery( minT)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df_fecha["fecha_MIN"][0]

## Nos devuelve un dataframe con los datos metereológicos de la estación indicada en el periodo entre fechaMin y fechaMax
def getDatosTiempo(estacion,fechaMin='2005-01-01',fechaMax=datetime.datetime.today().strftime("%Y-%m-%d")):
    queryDatos="SELECT Estacion, fecha, TMax, TMed, TMin, TsMax, TsMed, TsMin, HrMax, HrMed, HrMin, PAc, RgAc, VVMax, VVMed" \
    + ' FROM TemperaturasDiarias' \
    +      " WHERE Estacion="+estacion+" AND fecha between '"+fechaMin+"' AND '"+fechaMax+"';"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_DatosTiempo['fecha']=pd.to_datetime(df_DatosTiempo['fecha'])
    return df_DatosTiempo

### GESTIÓN DE LAS ESTACIONES CLIMÁTICAS
### Obtención de las estaciones que tenemos en base de datos
def getEstaciones():
    query='select * from Estaciones' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df

### Obtención de la mayor fecha con datos para una estación en concreto
def getEstacion(estacion):
    date_format= '%Y-%m-%d'
    sqlEstacion='select * from Estaciones where estacion='+estacion
    df=ejecuteQuery( sqlEstacion)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


### Obtención de los municipios que tenemos en base de datos
def getMunicipios():
    query='select * from Municipios' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df



## GESTIÓN DE LOS MUNICIPIOS
### Obtención de los municipios que tenemos en base de datos
def getMunicipiosConVuelos():
    query='select distinct(idMunicipio) municipio from vVuelosMedias' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df

## GESTIÓN DE LAS PREDICCIONES AEMET
## Procedimiento que inserta un df de la predicción meterorológica.
def insertarPrediccion(df):
    insertaTabla('PrediccionAEMET',df)

## Procedimiento para borrar la predicción de un municipio
## Por ejemplo, si los datos no son completos.
def borraPrediccion(idMunicipio):
    date_format= '%Y-%m-%d'
    sql="delete from PrediccionAEMET where idMunicipio=:id"
    with __engine.connect() as connection:
        connection.execute(text(sql), {"id": idMunicipio})

## Nos devuelve un dataframe con la predicción del municipio indicado
def getPrediccion(municipio):
    queryDatos="SELECT  fecha, TMax, TMed, TMin, HrMax, HrMed, HrMin, ProbPrecip, VVMed" \
    + ' FROM PrediccionAEMET' \
    +      " WHERE idMunicipio="+municipio+";"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_DatosTiempo['fecha']=pd.to_datetime(df_DatosTiempo['fecha'])
    return df_DatosTiempo


## MODELO DE APRENDIZAJE
## Le pasamos un dataframe con el modelo de aprendizaje
def insertarModelo(df):
    insertaTabla('modelo',df)
## Recuperamos el modelo para entrenar
def getTodoModelo():
    queryDatos = "select * from modelo;"
    df_Todo=ejecuteQuery(queryDatos)
    df_Todo['fecha']=pd.to_datetime(df_Todo['fecha'])
    return df_Todo
## Insertamos las predicciones
def insertarPrediccion(df):
    insertaTabla('predicciones',df)
## Obtenemos las predicciones
def getTodasPredicciones():
    queryDatos = "select idMunicipio, fecha, probabilidad, prediccion from predicciones;"
    df_Todo=ejecuteQuery(queryDatos)
    df_Todo['fecha']=pd.to_datetime(df_Todo['fecha'])
    return df_Todo

##  Borramos las predicciones por fecha
def borraPrediccionPlaga(fecha):
    date_format= '%Y-%m-%d'
    fecha_dt = pd.to_datetime(fecha)
    sql="delete from predicciones where  DATE(fecha) = :id"
    with __engine.connect() as connection:
        connection.execute(text(sql), {"id": fecha_dt.date()})
        connection.commit()

