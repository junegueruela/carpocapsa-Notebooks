#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
Módulo obtenerDatos
Nos permite comunicarnos con la API de la CAR para recuperar la información de su red de estaciones climáticas
"""
import pandas as pd
import numpy as np
import json
import requests
import http.client
import datetime
import conexionSGBD as cS
import util as ut
__apiKey__="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqdW5lZ3VlcnVlbGFAZ21haWwuY29tIiwianRpIjoiYjMzZmVkYTYtODEzOS00ZDdjLWEyOTktN2Q0M2NiYzMwZDA5IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3MTM1NDYzODEsInVzZXJJZCI6ImIzM2ZlZGE2LTgxMzktNGQ3Yy1hMjk5LTdkNDNjYmMzMGQwOSIsInJvbGUiOiIifQ.hZT8YCEAuhb8tZmrTdBf7KFi6k3U5cV_ln-AdXiBFxk"


# In[29]:


def getPrediccionAemet(municipio):
    conn = http.client.HTTPSConnection("opendata.aemet.es")

    headers = {
        'cache-control': "no-cache"
        }
    #https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/16125 -> Es la de Rincón 
    ##Obtenemos la predicción climática por municipio
    conn.request("GET", "/opendata/api/prediccion/especifica/municipio/diaria/26"+municipio.zfill(3)+"/?api_key="+__apiKey__, headers=headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    json_response=json.loads(data.decode("utf-8"))
    URL=json_response['datos']
    ## Llamanos a la URL que está en la clave datos y nos devuelve un json con las predicciones
    response_t = requests.get(URL)
    data=response_t.json()
    pPrediccion=data[0]['prediccion']['dia']
    ## Obtenemos un dataframe con la temperatura máxima y mínima
    temperaturas = []
    for dia in pPrediccion:
        fecha = dia['fecha']
        maxima = dia['temperatura']['maxima']
        minima = dia['temperatura']['minima']
        ## La temperatura media, guarda datos cada x horas los primeros dos días.
        ## Así que hago una media de esos valores.
        ## Si no existen, hago la media entre máxima y mínima
        tms=[tm['value'] for tm  in dia['temperatura']['dato']]
        media=sum(tms)/len(tms) if tms else (maxima+minima)/2
        hrMaxima=dia['humedadRelativa']['maxima']
        hrMinima=dia['humedadRelativa']['minima']
        ## Lo mismo me pasa con las presiones
        hrs=[hr['value'] for hr  in dia['humedadRelativa']['dato']]
        hrMedia=sum(hrs)/len(hrs) if hrs else (hrMaxima+hrMinima)/2
        uvMax=100
        ## Calculo las precicipationes, similar al viento.
        pacs= [pac['value'] for pac in dia['probPrecipitacion']]
        pPr = sum(pacs)/len(pacs) if pacs else 0
        ## Para el viento, hay varios valores, por lo que obtengo la media
        velocidades = [viento['velocidad'] for viento in dia['viento']]
        vViento= sum(velocidades) / len(velocidades) if velocidades else None
        temperaturas.append({"fecha": fecha, "Tmax": maxima, 'TMed': media, "Tmin": minima,'HrMax':hrMaxima,'HrMed':hrMedia, 'HrMin':hrMinima, 'ProbPrecip':pPr, 'VVMed':vViento})
    df = pd.DataFrame(temperaturas)
    df['VVMed']=df['VVMed'].round(2)
    df['ProbPrecip']=df['ProbPrecip'].round(2)
    return df


# In[ ]:


def getDatosClimaticosCAR(fIni, fFin, estacion):
    """ 
    Obtenemos los datos relativos a
        Temperatura T: Máxima, media y mínima
        Temperatura del suelo TS: Máxima, media y mínima.
        Humedad relativa Hr: Máxima, media y mínima
        Precipitación acumulada PAc
        Índice de radiación acumulada RgAc
        Velocidad del viento en km/h VV: Máxima y media.
    Args: 
         fIni: fecha de incio de la consulta
         fFin: Fecha fin de la consulta
         estacion: Código de estación agroclimática que queremos consultar
    Returns:
        Un dataframe con todos los datos
    """
    URL="https://ias1.larioja.org/apiSiar/servicios/v2/datos-climaticos/"+estacion+ "/D?fecha_inicio="+fIni+"&fecha_fin="+fFin \
    +"&dato_valido=True&parametro=TS&parametro=T"
    print(URL)
    ## Creo un dataFrame vacío por si falla
    dfT = pd.DataFrame(columns=['fecha','TMax', 'TMed','Tmin','TsMax', 'TsMed','Tsmin'])
    try:
        response = requests.get(URL,timeout=90)
        j_data=response.json()
    ## De la respuesta me quedon con los datos.
        df=pd.DataFrame(j_data['datos'])
        ## Substituyo lo - por nulos
        df = df.replace('-','')
        ## A veces da varias medidas para un día de un parámetro, así que selecciono la media
        df['valor']=df['valor'].astype('float')
        df=df.groupby(['parametro','fecha','funcion_agregacion'])['valor'].mean().reset_index()
        df['valor']=df['valor'].round(2)
        ## Recupero las temperaturas del aire y las agrupo en un dataframe
        dfTMax=df[(df['parametro']=='T')&(df['funcion_agregacion']=='Max')][['fecha','valor']].rename(columns={'valor':'TMax'})
        dfTMed=df[(df['parametro']=='T')&(df['funcion_agregacion']=='Med')][['fecha','valor']].rename(columns={'valor':'TMed'})
        dfTMin=df[(df['parametro']=='T')&(df['funcion_agregacion']=='Min')][['fecha','valor']].rename(columns={'valor':'TMin'})
        dfTe=pd.merge(dfTMax,dfTMed, on='fecha',how='left')
        dfTe=pd.merge(dfTe,dfTMin, on='fecha',how='left')
        ## Recupero las temperaturas del suelo y las agrupo en un dataframe
        ## Como las temperatus del suelo tienen varias medidas, hago una única agrupación por fecha primero
        dfTsMax=df[(df['parametro']=='TS')&(df['funcion_agregacion']=='Max')][['fecha','valor']].rename(columns={'valor':'TsMax'})
        dfTsMed=df[(df['parametro']=='TS')&(df['funcion_agregacion']=='Med')][['fecha','valor']].rename(columns={'valor':'TsMed'})
        dfTsMin=df[(df['parametro']=='TS')&(df['funcion_agregacion']=='Min')][['fecha','valor']].rename(columns={'valor':'TsMin'})
        dfTs=pd.merge(dfTsMax,dfTsMed, on='fecha',how='left')
        dfTs=pd.merge(dfTs,dfTsMin, on='fecha',how='left')
        ## hago un merge de la Temperatura y la Temperatura máxima
        dfT=pd.merge(dfTe,dfTs,on='fecha',how='left')
    except requests.exceptions.Timeout:
        print ("Time out al consultar las temperaturas")
    except requests.exceptions.InvalidJSONError:
         print ("Error al consultar las temperaturas")
    except Exception as e:
        print ("No ha devuelto datos la consulta")
    URL="https://ias1.larioja.org/apiSiar/servicios/v2/datos-climaticos/"+estacion+ "/D?fecha_inicio="+fIni+"&fecha_fin="+fFin \
    +"&dato_valido=True&parametro=Hr"
    print(URL)
    ## Creo un dataFrame vacío por si falla
    dfHr = pd.DataFrame(columns=['fecha','HrMax', 'HrMed','Hrmin'])
    try:
        response = requests.get(URL,timeout=90)
        j_data=response.json()
        ## De la respuesta me quedon con los datos.
        df=pd.DataFrame(j_data['datos'])
        ## Substituyo lo - por nulos
        df = df.replace('-','')
        ## A veces da varias medidas para un día de un parámetro, así que selecciono la media
        df['valor']=df['valor'].astype('float')
        df=df.groupby(['parametro','fecha','funcion_agregacion'])['valor'].mean().reset_index()
        df['valor']=df['valor'].round(2)
        ## Recupero las humedades relativas y las agrupo en un dataframe
        dfHrMax=df[(df['parametro']=='Hr')&(df['funcion_agregacion']=='Max')][['fecha','valor']].rename(columns={'valor':'HrMax'})
        dfHrMed=df[(df['parametro']=='Hr')&(df['funcion_agregacion']=='Med')][['fecha','valor']].rename(columns={'valor':'HrMed'})
        dfHrMin=df[(df['parametro']=='Hr')&(df['funcion_agregacion']=='Min')][['fecha','valor']].rename(columns={'valor':'HrMin'})
        dfHr=pd.merge(dfHrMax,dfHrMed, on='fecha',how='outer')
        dfHr=pd.merge(dfHr,dfHrMin, on='fecha',how='left')
    except requests.exceptions.Timeout:
        print ("Time out al consultar la humedad relativa")
    except requests.exceptions.InvalidJSONError:
         print ("Error al consultar la humedad relativa")
    except Exception as e:
        print ("No ha devuelto datos la consulta")
    ## Hago la tercera petición
    URL="https://ias1.larioja.org/apiSiar/servicios/v2/datos-climaticos/"+estacion+ "/D?fecha_inicio="+fIni+"&fecha_fin="+fFin \
    +"&dato_valido=True&parametro=P&parametro=Rg"
    ## Creo un dataFrame vacío por si falla
    dfR = pd.DataFrame(columns=['fecha','Pac', 'RgAc'])
    try:
        response = requests.get(URL,timeout=90)
        j_data=response.json()
        ## De la respuesta me quedon con los datos.
        df=pd.DataFrame(j_data['datos'])
        ## Substituyo lo - por nulos
        df = df.replace('-','')
        ## A veces da varias medidas para un día de un parámetro, así que selecciono la media
        df['valor']=df['valor'].astype('float')
        df=df.groupby(['parametro','fecha','funcion_agregacion'])['valor'].mean().reset_index()
        df['valor']=df['valor'].round(2)
        ## Obtengo el dataframe para la precipitación acumulada
        dfPAc=df[(df['parametro']=='P')][['fecha','valor']].rename(columns={'valor':'PAc'})
        ## Recupero los índice de radiación acumulada
        dfRgAc=df[(df['parametro']=='Rg')&(df['funcion_agregacion']=='Ac')][['fecha','valor']].rename(columns={'valor':'RgAc'})
        dfR=pd.merge(dfPAc,dfRgAc,on='fecha',how='left') 
    except requests.exceptions.Timeout:
        print ("Time out al consultar las precipitaciones")
    except requests.exceptions.InvalidJSONError:
         print ("Error al consultar las precipitaciones")
    except Exception as e:
        print ("No ha devuelto datos la consulta")
    ## Hago la cuarta petición
    URL="https://ias1.larioja.org/apiSiar/servicios/v2/datos-climaticos/"+estacion+ "/D?fecha_inicio="+fIni+"&fecha_fin="+fFin \
    +"&dato_valido=True&parametro=VV"
    ## Creo un dataFrame vacío por si falla
    dfVV = pd.DataFrame(columns=['fecha','VVMax', 'VVMed'])
    try:
        response = requests.get(URL,timeout=90)
        j_data=response.json()
        ## De la respuesta me quedon con los datos.
        df=pd.DataFrame(j_data['datos'])
        ## Substituyo lo - por nulos
        df = df.replace('-','')
        ## A veces da varias medidas para un día de un parámetro, así que selecciono la media
        df['valor']=df['valor'].astype('float')
        ## Como es el viento, lo convierto en km/h
        df['valor']=df['valor']*3.6
        df=df.groupby(['parametro','fecha','funcion_agregacion'])['valor'].mean().reset_index()
        df['valor']=df['valor'].round(2)
        ## Recupero la velocidad del viento máximo y media y las agrupo en un dataframe
        dfVVMax=df[(df['parametro']=='VV')&(df['funcion_agregacion']=='Max')][['fecha','valor']].rename(columns={'valor':'VVMax'})
        dfVVMed=df[(df['parametro']=='VV')&(df['funcion_agregacion']=='Med')][['fecha','valor']].rename(columns={'valor':'VVMed'})
        dfVV=pd.merge(dfVVMax,dfVVMed,on='fecha',how='left')
    except requests.exceptions.Timeout:
        print ("Time out al consultar la velocidad del viento")
    except requests.exceptions.InvalidJSONError:
        print ("Error al consultar la velocidad del viento")
    except Exception as e:
        print ("No ha devuelto datos la consulta")
    ## Los junto todos
    dfT=pd.merge(dfT,dfHr,on='fecha',how='left')
    dfT=pd.merge(dfT,dfR,on='fecha',how='left')
    dfT=pd.merge(dfT,dfVV,on='fecha',how='left')    
    ##Añado la columna estación y la pongo al principio
    dfT['estacion']=estacion
    dfT.replace('',np.nan)
    dfT = dfT[['estacion'] + [col for col in dfT.columns if col != 'estacion']]
    return dfT

    


# In[ ]:


def actualizarEstacion(estacion):
    strEstacion=str(estacion)
    fechaInicio=cS.getFechaMaxima(strEstacion) + datetime.timedelta(days=1)
    fechaFin=(datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    dfSalida=getDatosClimaticosCAR(str(fechaInicio), str(fechaFin),strEstacion)
    try:
        cS.insertarDatosTiempo(dfSalida)
        print ("Estacion "+strEstacion+" actualizada correctamente")
    except Exception as e:
       print('Error al insertar datos de la estación ' + strEstacion + ' para el rango de fecha de '+ str(fechaInicio) + ' a ' + str(fechaFin))


# In[ ]:


def actualizarTodasEstaciones():
    dfEstaciones=cS.getEstaciones()
    for estacion in dfEstaciones['estacion']:
        actualizarEstacion(estacion)


# In[ ]:


## Dada un municipio, obtengo la predicción
## Y los guardo en base de datos.
def actualizarPrediccion(municipio):
    strMunicipio=str(municipio)
    dfSalida=getPrediccionAemet(strMunicipio)
    dfSalida.insert(0,'idMunicipio',municipio)
    dfSalida['fecha']=dfSalida['fecha'].apply(lambda x: ut.convertirString(x,'AEMET'))
    try:
        cS.borraPrediccion(municipio)
        cS.insertarPrediccion(dfSalida)
       
    except Exception as e:
       print('Error al insertar datos del municipio '+strMunicipio)


# In[ ]:


## Actualizo la predicción de todos los Municipios
def actualizarPrediccionMunicipios():
    dfMunicipios=cS.getMunicipios()
    for municipio in dfMunicipios['idMunicipio']:
        actualizarPrediccion(municipio)


# In[ ]:

def gradosDia(tMax, tMin):
    tUmbralSuperior=34.4
    tUmbralInferior=10.1
    gradosDia=0
    if (tMax > tUmbralSuperior) and (tMin > tUmbralSuperior): ## Si la temperatura máxima y mínima están por encima de 34.4.
        gradosDia=tUmbralSuperior-tUmbralInferior
    elif (tMax < tUmbralInferior) and (tMin < tUmbralInferior): ## Si la máxima y la mínima están por debajo de 10.1
        gradosDia=0
    elif (tMax <= tUmbralSuperior) and (tMin < tUmbralInferior):  #Si la mínima no supera el umbral inferior y la máxima está por debajo de 34.4
        gradosDia=(tMax+tMin-2*tUmbralInferior)/2
    elif (tMax <= tUmbralSuperior) and (tMin >= tUmbralInferior): #Si máxima y mínima están dentro de los umbrales
        gradosDia=(tMax-tUmbralInferior)**2/(2*(tMax-tMin))
    elif (tMax > tUmbralSuperior) and (tMin < tUmbralInferior): ## SI la máxima no supera el umbral superior y la mínima está por debajo del inferior
        gradosDia=(tMax+tMin-2*tUmbralInferior - (tMax-tUmbralSuperior)**2/(tMax-tMin))/2
    else: ## Si la máxima supera el umbral superior y la mínima el inferior.
        gradosDia=((tMax-tUmbralInferior)**2 - (tMax-tUmbralSuperior)**2)/(2*(tMax-tMin))
    return gradosDia

def calcularModelo(dias):
    dfMunicipios=cS.getMunicipiosConVuelos()
    dfMuni=cS.getMunicipios()
    for municipio in dfMunicipios['municipio']:
        estacion=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Estacion'].values[0]
        df=cS.getDatosTiempo(str(estacion))
        df=df[['Estacion','fecha','TMed','TMax','TMin','HrMed','PAc','RgAc','VVMed']]
        df = df.sort_values('fecha')
        df['TMed'] = round(df['TMed'].rolling(window=dias, min_periods=1).mean(),1)
        df['HrMed'] = round(df['HrMed'].rolling(window=dias, min_periods=1).mean(),1)
        df['PAc'] = round(df['PAc'].rolling(window=dias, min_periods=1).sum(),1)
        df['RgAc'] = round(df['RgAc'].rolling(window=dias, min_periods=1).sum(),1)
        df['VVMed'] = round(df['VVMed'].rolling(window=dias, min_periods=1).mean(),1)
        #df['diasGrado']=round(df['TMed'].apply(lambda x: 0 if x <= 10 else x-10),1)
        df['diasGrado'] = round(df.apply(lambda row: gradosDia(row['TMax'], row['TMin']), axis=1),1)
        df['inicio_febrero'] = df['fecha'].apply(lambda x: pd.Timestamp(year=x.year, month=2, day=1))
        df['DiasGradoAc'] = round(df.apply(lambda row: df[(df['fecha'] >= row['inicio_febrero']) & \
                                               (df['fecha'] <= row['fecha'])]['diasGrado'].sum(), axis=1),1)
        df['generacion']=df['DiasGradoAc'].apply(lambda x: x//585 +1)
        
        #df['diasGrado60']= round(df['diasGrado'].rolling(window=60, min_periods=1).sum(),1)
        dfM=cS.getDatosVueloMunicipio(str(municipio))
        dfM = dfM.sort_values('fecha')
        dfM['numVuelos']=round(dfM['numVuelos'].rolling(window=3, min_periods=1).mean(),1)
        dfM['incidencia']=dfM['numVuelos'].apply(lambda x: 0 if x == 0 else 1 if 0 < x <= 0.5 else 2 if 0.5 < x <= 2 else 3)
        #dfM['incidencia']=dfM['numVuelos'].apply(lambda x: 0 if  0 <= x <= 0.5 else 1 if 0.5 < x <= 2  else 2)
        dfMerged=  pd.merge(dfM, df, on=['fecha'], how='inner') 
        dfMerged=dfMerged.drop(['Estacion','diasGrado','inicio_febrero','TMax','TMin'],axis=1)
        cS.insertarModelo(dfMerged)

# In[1]:





# In[ ]:




