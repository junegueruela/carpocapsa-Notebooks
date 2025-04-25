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
## Utilizamos mejor sqlalchemy Defiinimos la fuente. Aquí está definida para un mySql local
__engine = create_engine('mysql+mysqlconnector://Plagas:plagas@127.0.0.1:3306/Plagas')
date_format= '%Y-%m-%d'


# In[2]:


## Borramos un vuelo por su id
def borraVuelo(id):
    sql="delete from VuelosCarpo where idVuelo = :id"
    with __engine.connect() as connection:
        connection.execute(text(sql), {"id": id})
        connection.commit()  # Asegúrate de hacer commit si es necesario


# In[ ]:


## Le pasamos un dataframe con los de una captura.
def insertarVuelo(df):
    insertaTabla('VuelosCarpo',df)


# In[ ]:


def insertaTabla(tabla, df):
    df.to_sql(tabla, con=__engine, if_exists='append', index=False)


# In[1]:


## Le pasamos un dataframe con los datos meteorológicos a insertar y nos los inserta.
def insertarDatosTiempo(df):
    insertaTabla('TemperaturasDiarias',df)


# In[ ]:


## Le pasamos un dataframe con los datos meteorológicos a insertar y nos los inserta.
def insertarModelo(df):
    insertaTabla('Modelo',df)


# In[25]:


## Funcion genérica que me ejecuta una query 
def ejecuteQuery(query):
    df = pd.read_sql_query(query, con=__engine)
    return df


# In[26]:


### Obtención de la mayor fecha con datos para una estación en concreto
def getFechaMaxima(estacion):
    date_format= '%Y-%m-%d'
    maxT='select max(fecha) as fecha_MAX from TemperaturasDiarias where Estacion='+estacion
    df_fecha=ejecuteQuery( maxT)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df_fecha["fecha_MAX"][0]


# In[1]:


### Obtención de la mayor fecha con datos para una estación en concreto
def getFechaMinima(estacion):
    date_format= '%Y-%m-%d'
    minT='select min(fecha) as fecha_MIN from TemperaturasDiarias where Estacion='+estacion
    df_fecha=ejecuteQuery( minT)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df_fecha["fecha_MIN"][0]


# In[3]:


### Obtención de las estaciones que tenemos en base de datos
def getEstaciones():
    query='select * from Estaciones' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


# In[1]:


### Obtención de la mayor fecha con datos para una estación en concreto
def getEstacion(estacion):
    date_format= '%Y-%m-%d'
    sqlEstacion='select * from Estaciones where estacion='+estacion
    df=ejecuteQuery( sqlEstacion)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


# In[ ]:


### Obtención de las estaciones que tenemos en base de datos
def getModeloCapturas():
    query='select * from ModeloCapturas' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


# In[1]:


### Obtención de los municipios que tenemos en base de datos
def getMunicipios():
    query='select * from Municipios' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


# In[ ]:


### Obtención de los municipios que tenemos en base de datos
def getMunicipiosConVuelos():
    query='select distinct(municipio) municipio from VuelosMedia' 
    df=ejecuteQuery(query)
    #fechaDate=convertirDate(df_fecha["fecha_MAX"][0])
    return df


# In[ ]:


## Procedimiento que inserta un df de la predicción meterorológica.
def insertarPrediccion(df):
    insertaTabla('PrediccionAEMET',df)


# In[ ]:


## Procedimiento para borrar la predicción de un municipio
## Por ejemplo, si los datos no son completos.
def borraPrediccion(idMunicipio):
    date_format= '%Y-%m-%d'
    sql="delete from PrediccionAEMET where idMunicipio=:id"
    with __engine.connect() as connection:
        connection.execute(text(sql), {"id": idMunicipio})
        connection.commit()  # Asegúrate de hacer commit si es necesario


# In[ ]:


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


# In[ ]:


## Obtenemos la fecha de la primera generación de una estación y año
def getPrimeraGeneracion (estacion, anyo):
    queryDatos="SELECT fechaGeneracion FROM FechasGeneracion WHERE Estacion = " + estacion + " AND anyoGeneracion =" + anyo + ";"
    df_Primera=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_Primera ['fechaGeneracion']=pd.to_datetime(df_Primera['fechaGeneracion'])
    return df_Primera['fechaGeneracion'][0]


# In[4]:


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


# In[ ]:


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


# In[ ]:


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


# In[3]:


def getTermino(idVuelo):
    queryDatos="SELECT idTermino" \
    + ' FROM VuelosCarpo' \
    +      " WHERE idVuelo="+idVuelo+";"
    #print(queryDatos)
    df_DatosTiempo=ejecuteQuery(queryDatos)
    return df_DatosTiempo['idTermino'][0]


# In[ ]:


## Obtiene los datos medios de los últimos x días (22, por defecto)
def getModelo(municipio,dias=22):
    queryDatos="select TD1.fecha fecha, M.IdMunicipio idMunicipio, round(sum(CASE when TD2.TMed>10 then TD2.Tmed-10 ELSE 0 END),2) DiasGrado," \
    + "   round(avg(TD2.TMed),2) TMedia," \
    + "   round(avg(TD2.HrMed),2) HrMed," \
    + "   round(sum(TD2.RgAc),2) RgAc," \
    + "	   round(sum(TD2.Pac),2) Pac, round(avg(TD2.VVMed),2) VVMed" \
    + " from TemperaturasDiarias TD1, TemperaturasDiarias TD2, Municipios M" \
    + "  where TD1.Estacion = TD2.Estacion" \
    + "   and M.idMunicipio=" + municipio \
    + "   and TD2.fecha <=TD1.fecha" \
    + "   and TD2.fecha >  date_add(TD1.fecha,interval - " + str(dias) +"  day)" \
    + "   and M.Estacion=TD1.Estacion" \
    + " group by TD1.fecha,  M.IdMunicipio , distancia, M.Altitud, difAltitud"
    df_Modelo=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_Modelo ['fecha']=pd.to_datetime(df_Modelo['fecha'])
    return df_Modelo    


# In[ ]:


## Obtiene los dias grado acumulados desde principio de año
def getModelo2(municipio):
    queryDatos = "select M.idMunicipio idMunicipio, VM.fecha fecha , ROUND(SUM(CASE when TMed>10 then Tmed-10 ELSE 0 END),2) DiasGradoAcum" \
    + " FROM Municipios M,  TemperaturasDiarias TD, TemperaturasDiarias TD2" \
    + "WHERE VM.municipio=M.idMunicipio " \
    + "and TD.estacion=M.estacion " \
    + "and M.idMunicipio = " + municipio \
    + " and TD2.TMed>10 " \
    + "and TD2.fecha <= TD.fecha " \
    + " and year(TD.fecha) = year(TD.fecha) " \
    + "group by M.idMunicipio, VM.fecha, valor"
    df_Modelo2=ejecuteQuery(queryDatos)
    ## Convierto la fecha a fecha
    df_Modelo2 ['fecha']=pd.to_datetime(df_Modelo2['fecha'])
    return df_Modelo2       


# In[1]:


def getTodoModelo():
    queryDatos = "select * from Modelo;"
    df_Todo=ejecuteQuery(queryDatos)
    df_Todo['fecha']=pd.to_datetime(df_Todo['fecha'])
    return df_Todo


# In[ ]:




