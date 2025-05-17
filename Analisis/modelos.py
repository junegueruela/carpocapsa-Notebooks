import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import conexionSGBD as cS
import util as ut

## Función para calcular los dís grado
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
    return (lambda x: max(0, x))(gradosDia)

## Calculo la medias de días grado entre dos fechas
## Dada la fecha actual  y un intervalo inferior y superior de días
## Calcula la temperatda media, dias grado medios, humedad relativa y radiación acumulada medias del periodo
def calcular_medias (fecha_actual,df, dias_desde = 30, dias_hasta=15):
    # Definir el rango de fechas
    inicio = fecha_actual - pd.Timedelta(days=dias_desde)
    fin = fecha_actual - pd.Timedelta(days=dias_hasta)
    # Filtrar el DataFrame en el rango de fechas
    df_filtrado = df[(df['fecha'] >= inicio) & (df['fecha'] <= fin)].copy()
    # Obtener la generación de la fecha máxima dentro del rango
    generacion_fin = df.loc[df['fecha'] == fin, 'generacion'].values
    # Si hay valores en `generacion_fin`, tomamos el primero; si no, usamos un valor por defecto
    generacion_fin = generacion_fin[0] if len(generacion_fin) > 0 else None
    # Si la generación de una fila es diferente a la de `fin`, ponemos `dias_grado_ac` en 0
    if generacion_fin is not None:
        df_filtrado.loc[df_filtrado['generacion'] != generacion_fin, 'dias_grado_ac'] = 0
    # Calcular los valores agregados
    dias_grado_ac = df_filtrado['dias_grado_ac'].mean()
    media_tm=df_filtrado['TMed'].mean()
    media_rg = df_filtrado['RgAc'].mean()
    media_hr = df_filtrado['HrMed'].mean()
    return [dias_grado_ac, media_tm, media_rg, media_hr]


# Obtención del modelo de predicción. Este era el modelo inicial con datos climáticos agrupados en Time series cada 15 días
# Para ello realizamos cáculos basados en la bibliografía

def calcularModelo():
    dfMunicipios=pd.DataFrame()
    dfListaMunicipios=cS.getMunicipiosConVuelos()
    dfMuni=cS.getMunicipios()
    argumentos=[]
    for municipio in dfListaMunicipios['municipio']:
        estacion=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Estacion'].values[0]
        df=cS.getDatosTiempo(str(estacion))
        df=df[['Estacion','fecha','TMed','TMax','TMin','HrMed','RgAc','VVMax']]
        df = df.sort_values('fecha')
        # Calculo el año y la semana
        df['anyo'] = df['fecha'].dt.year
        df['semana']=df['fecha'].dt.isocalendar().week
        
        # Calculamos el modelo de límites
        df['dias_grado'] = round(df.apply(lambda row: gradosDia(row['TMax'],row['TMin']), axis=1),1)
        df['dias_grado_ac'] = df.groupby(df['fecha'].dt.year)['dias_grado'].cumsum()
        df['generacion']=df['dias_grado_ac'].apply(lambda x: x//600 +1)
        
        # Me quedo con los días acumulados por generación
        df['dias_grado_ac']=df['dias_grado_ac'].apply(lambda x: x%600)
        
        # Calculo el día dentro del año si días grado > 0
        df['dia']=df['dias_grado'].apply(lambda x: 0 if x <=0 else 1)
        df['dia_g'] = df.groupby(['anyo', 'generacion'])['dia'].cumsum()
        df['TCrep']=df['TMin'] + (df['TMax']-df['TMin'])*0.7
        
        ## Calculamos hr media y RgAc de los últimos 7 días
        ## Calculamos la temperatura crepuscular pues las polillas tienen un vuelo crepuscular
        ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
        dias=7
        df['t_min'] = df['TMin'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['t_max'] = df['TMax'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['t_cresp'] = df['TCrep'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['t_med'] = df['TMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['hr_med'] = df['HrMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['rg_ac'] = df['RgAc'].rolling(window=dias, min_periods=1).mean().fillna(0)
        #df['vv_med'] = df['VVMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        ## Calculamos la temperatura crepuscular pues las polillas tienen un vuelo crepuscular
        ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
        df['t_min_p'] = df['TMin'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['t_max_p'] = df['TMax'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['t_cresp_p'] = df['TCrep'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['t_med_p'] = df['TMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['hr_med_p'] = df['HrMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['rg_ac_p'] = df['RgAc'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        #df['vv_med_p'] = df['VVMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
         
        # Calculo los días grados de las ultimas 5 quincenas
        # Lo hacemos así porque las condiciones ambientales determinan los distintos estadios de desarrollo
        # del insecto, desde el huevo a la larva, siendo un ciclo completo de varias semanas
        df[['dias_grado_ac_7','t_med_7','rg_md_7','hr_md_7']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 21, 7), axis=1, result_type='expand')
        df[['dias_grado_ac_21','t_med_21','rg_md_21','hr_md_21']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 35, 22), axis=1, result_type='expand')
        df[['dias_grado_ac_35','t_med_35','rg_md_35','hr_md_35']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 49, 36), axis=1, result_type='expand')
        df[['dias_grado_ac_49','t_med_49','rg_md_49','hr_md_49']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 63, 50), axis=1, result_type='expand')
        df[['dias_grado_ac_63','t_med_63','rg_md_63','hr_md_63']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 77, 64), axis=1, result_type='expand')
        
        # Obtenenos los datos de vuelo para el municipio en cuestión y todos sus términos
        dfM=cS.getDatosVueloMunicipioTermino(str(municipio))
        dfM = dfM.sort_values(['idTermino','fecha'])
        dfM = dfM.sort_values(['fecha'])
        dfM['anyo']=dfM['fecha'].dt.year
        
        # Las muestras se realizan lunes, miércoles y viernes, se estima que hay riesto alto cuando la suma de una semana supera los tres vuelos
        dfM['num_vuelos_n']=dfM['numVuelos'].rolling(window=3, min_periods=1,center=False).sum()
        
        # Normalizamos a 3 porque no nos proporciona ninguna información saber el número exacto
        dfM['num_vuelos_n']=dfM['num_vuelos_n'].apply(lambda x: x if x <3 else 3)
       
        # Si la menor de las últimas muestras es del año pasado, inicializamos contador
        dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(3).fillna(0), 'num_vuelos_n'] = 0
        
        # Como no hacemos un rolling, es 0 si es menor que 1, 1 entre y 3 y 2 si mayor que 3.
        # Dependiendo del número de clases especificado sacamos tres o dos incidencias
        dfM['incidencia']=dfM['num_vuelos_n'].apply(lambda x: 0 if x <=2 else 1) 
        
        # Calculamos la incidencia normalizada como el máximo de incidencia de las cinco muestras posteriores, entre 9 y 10 días
        # Pues en un periodo hay riesgo si se ha detectado en uno de sus días
        dfM['incidencia'] = dfM['incidencia'].rolling(window=4, min_periods=1).max().shift(-3).fillna(0)
        #dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(2).fillna(0), 'incidencia'] = 0

        # Nos quedamos con el máximo de incidencia y de vuelos para cada fecha en cada municipio
        # pues es como trabajan en la denominación
        dfM = dfM.groupby(['idMunicipio',  'fecha','anyo']).agg({
                    'incidencia': 'max',       # Máximo de incidencia
                    'num_vuelos_n': 'max'}).reset_index()
        
        # Le añadimos la incidencia de la semana pasada y la anterior para emular un time series
        for i in range(1,3):
            dfM[f'incidencia-{i}'] = dfM['incidencia'].shift(i*3+2).fillna(0)
            dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(i*3+1).fillna(0), f'incidencia-{i}'] = 0
        
        dfMerged= pd.merge_asof(df, dfM, on=['fecha'], direction='backward', tolerance=pd.Timedelta(days=7)) 
        #dfMerged=dfMerged.drop(['Estacion','dias_grado'],axis=1)
        dfMerged['anyo']=dfMerged['anyo_x']
        dfMerged = dfMerged.dropna(subset=["incidencia"])
        dfMunicipios= pd.concat([dfMunicipios, dfMerged], axis=0)
    return dfMunicipios[['idMunicipio','fecha','anyo','generacion', \
                          'semana','dia_g','dias_grado_ac','num_vuelos_n','incidencia','t_min','t_max','t_cresp','hr_med', \
                         't_min_p','t_max_p','t_cresp_p','hr_med_p', \
                         'dias_grado_ac_7','rg_md_7', 'hr_md_7','t_med_7',\
                         'dias_grado_ac_21','rg_md_21', 'hr_md_21','t_med_21',\
                         'dias_grado_ac_35','rg_md_35', 'hr_md_35','t_med_35',\
                         'dias_grado_ac_49','t_med_49','rg_md_49','hr_md_49', \
                         'dias_grado_ac_63','t_med_63','rg_md_63','hr_md_63', \
                         'incidencia-1','incidencia-2']]
    # Obtención del modelo de predicción

# Éste modelo es más científico.
def calcularModeloSimple():
    dfMunicipios=pd.DataFrame()
    dfListaMunicipios=cS.getMunicipiosConVuelos()
    dfMuni=cS.getMunicipios()
    argumentos=[]
    for municipio in dfListaMunicipios['municipio']:
        estacion=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Estacion'].values[0]
        Altitud=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Altitud'].values[0]
        df=cS.getDatosTiempo(str(estacion))
        df=df[['Estacion','fecha','TMed','TMax','TMin','HrMed','RgAc','VVMed']]
        df['altitud']=Altitud
        df = df.sort_values('fecha')
        # Calculo el año y la semana
        df['anyo'] = df['fecha'].dt.year
        df['semana']=df['fecha'].dt.isocalendar().week
        
        # Calculamos el modelo de límites
        df['dias_grado'] = round(df.apply(lambda row: gradosDia(row['TMax'],row['TMin']), axis=1),1)
        df['dias_grado_ac'] = df.groupby(df['fecha'].dt.year)['dias_grado'].cumsum()
        df['generacion']=df['dias_grado_ac'].apply(lambda x: (x-100)//600 +1)
        df[df['generacion']==0]['generacion']=1
        
        # Me quedo con los días acumulados por generación
        #df['dias_grado_ac']=df['dias_grado_ac'].apply(lambda x: x%600)
        
        # Calculo el día dentro del año si días grado > 0
        df['dia']=df['dias_grado'].apply(lambda x: 0 if x <=0 else 1)
        df['dia_g'] = df.groupby(['anyo', 'generacion'])['dia'].cumsum()
        #df['TCrep']=df['TMin'] + (df['TMax']-df['TMin'])*0.7
        
        ## Calculamos hr media y RgAc de los últimos 7 días
        ## Calculamos la temperatura crepuscular pues las polillas tienen un vuelo crepuscular
        ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
        dias=7
        df['t_min'] = df['TMin'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['t_max'] = df['TMax'].rolling(window=dias, min_periods=1).mean().fillna(0)
        #df['t_cresp'] = df['TCrep'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['t_med'] = df['TMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['hr_med'] = df['HrMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['rg_ac'] = df['RgAc'].rolling(window=dias, min_periods=1).mean().fillna(0)
        df['vv_med'] = df['VVMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
        ## Calculamos la temperatura crepuscular pues las polillas tienen un vuelo crepuscular
        dias=7
        ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
        df['t_min_p'] = df['TMin'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['t_max_p'] = df['TMax'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        #df['t_cresp_p'] = df['TCrep'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['t_med_p'] = df['TMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['hr_med_p'] = df['HrMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['rg_ac_p'] = df['RgAc'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
        df['vv_med_p'] = df['VVMed'].rolling(window=dias, min_periods=1).mean().shift(-6).fillna(0)
         
        # Calculo los días grados de las ultimas 5 quincenas
        # Lo hacemos así porque las condiciones ambientales determinan los distintos estadios de desarrollo
        # del insecto, desde el huevo a la larva, siendo un ciclo completo de varias semanas
        df[['dias_grado_ac_7','t_med_7','rg_md_7','hr_md_7']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 28, 7), axis=1, result_type='expand')
        df[['dias_grado_ac_28','t_med_28','rg_md_28','hr_md_28']] =  df.apply(lambda row: calcular_medias(row['fecha'], df, 64, 29), axis=1, result_type='expand')
        
        # Obtenenos los datos de vuelo para el municipio en cuestión y todos sus términos
        dfM=cS.getDatosVueloMunicipioTermino(str(municipio))
        dfM = dfM.sort_values(['idTermino','fecha'])
        dfM = dfM.sort_values(['fecha'])
        dfM['anyo']=dfM['fecha'].dt.year
        
        # Las muestras se realizan lunes, miércoles y viernes, se estima que hay riesto alto cuando la suma de una semana supera los tres vuelos
        dfM['num_vuelos_n']=dfM['numVuelos'].rolling(window=3, min_periods=1,center=False).sum()
        
        # Normalizamos a 3 porque no nos proporciona ninguna información saber el número exacto
        dfM['num_vuelos_n']=dfM['num_vuelos_n'].apply(lambda x: x if x <3 else 3)
       
        # Si la menor de las últimas muestras es del año pasado, inicializamos contador
        dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(3).fillna(0), 'num_vuelos_n'] = 0
        
        # Como no hacemos un rolling, es 0 si es menor que 1, 1 entre y 3 y 2 si mayor que 3.
        # Dependiendo del número de clases especificado sacamos tres o dos incidencias
        if clases==3:
         # Introducimos un estado intermedio porque mejora la capacida de recall de la alerta (3)
            dfM['incidencia']=dfM['num_vuelos_n'].apply(lambda x: 0 if x <=0.5 else 1 if 0.5 < x < 2.8 else 2)
        else:
            dfM['incidencia']=dfM['num_vuelos_n'].apply(lambda x: 0 if x <=2 else 1) 
        
        # Calculamos la incidencia normalizada como el máximo de incidencia de las cinco muestras posteriores, entre 9 y 10 días
        # Pues en un periodo hay riesgo si se ha detectado en uno de sus días
        dfM['incidencia'] = dfM['incidencia'].rolling(window=4, min_periods=1).max().shift(-3).fillna(0)
        #dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(2).fillna(0), 'incidencia'] = 0

        # Nos quedamos con el máximo de incidencia y de vuelos para cada fecha en cada municipio
        # pues es como trabajan en la denominación
        dfM = dfM.groupby(['idMunicipio',  'fecha','anyo']).agg({
                    'incidencia': 'max',       # Máximo de incidencia
                    'num_vuelos_n': 'max',
                    'numVuelos':'max'}).reset_index()
        
        # Le añadimos la incidencia de la semana pasada y la anterior para emular un time series
        for i in range(1,7):
            dfM[f'num_vuelos_{i}'] = dfM['numVuelos'].shift(i-1).fillna(0)
        
        dfMerged= pd.merge_asof(df, dfM, on=['fecha'], direction='backward', tolerance=pd.Timedelta(days=7)) 
        #dfMerged=dfMerged.drop(['Estacion','dias_grado'],axis=1)
        dfMerged['anyo']=dfMerged['anyo_x']
        dfMerged = dfMerged.dropna(subset=["incidencia"])
        dfMunicipios= pd.concat([dfMunicipios, dfMerged], axis=0)
    return dfMunicipios[['idMunicipio','altitud','fecha','anyo','generacion', \
                          'semana','dia_g','dias_grado_ac','num_vuelos_n','incidencia','t_min','t_max','t_med','hr_med','vv_med', \
                         't_min_p','t_max_p','t_med_p','hr_med_p','vv_med_p', \
                         'dias_grado_ac_7','rg_md_7', 'hr_md_7','t_med_7',\
                         'dias_grado_ac_28','rg_md_28', 'hr_md_28','t_med_28',\
                         'num_vuelos_1','num_vuelos_2','num_vuelos_3','num_vuelos_4','num_vuelos_5','num_vuelos_6']]
    # Obtención del modelo de predicción

## Normalizamos una sola file
def calcularModeloMunicipio(municipio,fecha_prediccion,estimar='N'):
    #Obtengo la altitud del municipio la estación a la que pertenece
    dfMuni=cS.getMunicipios()
    estacion=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Estacion'].values[0]
    Altitud=dfMuni.loc[dfMuni['idMunicipio'] == municipio, 'Altitud'].values[0]
    # Obtener el año de la fecha
    anyo= pd.to_datetime(fecha_prediccion).year
    # Crear la fecha con formato 'YYYY-01-01'
    fecha_inicio= f"{anyo}-01-01"

    df=cS.getDatosTiempo(str(estacion),fecha_inicio,fecha_prediccion)
    df=df[['Estacion','fecha','TMed','TMax','TMin','HrMed','RgAc','VVMed']]
    df['altitud']=Altitud
    df = df.sort_values('fecha')
    
    # Calculo el año y la semana
    df['anyo'] = anyo
    df['semana']=df['fecha'].dt.isocalendar().week
    df['dias_grado'] = round(df.apply(lambda row: gradosDia(row['TMax'],row['TMin']), axis=1),1)
    df['dias_grado_ac'] = df.groupby(df['fecha'].dt.year)['dias_grado'].cumsum()                                           
    df['generacion']=df['dias_grado_ac'].apply(lambda x: (x-100)//600 +1)

    # Calculo el día dentro del año si días grado > 0
    df['dia']=df['dias_grado'].apply(lambda x: 0 if x <=0 else 1)
    df['dia_g'] = df.groupby(['anyo', 'generacion'])['dia'].cumsum()
    
    ## Calculamos hr media y RgAc de los últimos 7 días
    ## Calculamos la temperatura crepuscular pues las polillas tienen un vuelo crepuscular
    ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
    dias=7
    df['t_min'] = df['TMin'].rolling(window=dias, min_periods=1).mean().fillna(0)
    df['t_max'] = df['TMax'].rolling(window=dias, min_periods=1).mean().fillna(0)
    #df['t_cresp'] = df['TCrep'].rolling(window=dias, min_periods=1).mean().fillna(0)
    df['t_med'] = df['TMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
    df['hr_med'] = df['HrMed'].rolling(window=dias, min_periods=1).mean().fillna(0)
    df['rg_ac'] = df['RgAc'].rolling(window=dias, min_periods=1).mean().fillna(0)
    df['vv_med'] = df['VVMed'].rolling(window=dias, min_periods=1).mean().fillna(0)

    #Nos quedamos con el último
    df_municipio = df[df['fecha'] == df['fecha'].max()]
    # Calculo los datos climáticos de la semana siguiente prevista,
    # los datos de la semana en curso y
    # Los datos de los 15 días anteriores a la semana en curso, que me calculará las condiciones de pupa
    # Y las cinco semanas anteriores a éstas, que son las condiciones para el desarrollo larvario
    # Lo hacemos así porque las condiciones ambientales determinan los distintos estadios de desarrollo
    # del insecto, desde el huevo a la larva, siendo un ciclo completo de varias semanas
    df_municipio[['dias_grado_ac_7','t_med_7','rg_md_7','hr_md_7']] =  calcular_medias(pd.to_datetime(fecha_prediccion), df, 28, 7)
    df_municipio[['dias_grado_ac_28','t_med_28','rg_md_28','hr_md_28']] =  calcular_medias(pd.to_datetime(fecha_prediccion),  df, 64, 29)
    # Nos quedamos con la última fecha.
    # Obtenemos la predicción de la AEMET
    df_pred=cS.getPrediccion(str(municipio))
    ## Y la media de rachas de viento máxima pues el vuelo se ve influenciado por él
    df_municipio['t_min_p'] = df_pred['TMin'].mean()
    df_municipio['t_max_p'] = df_pred['TMax'].mean()
    df_municipio['t_med_p'] = df_pred['TMed'].mean()
    df_municipio['vv_med_p'] = df_pred['VVMed'].mean()
    df_municipio['hr_med_p']= df_pred['HrMed'].mean()

    # Obtenenos los datos de vuelo para el municipio en cuestión y todos sus términos de los últimos 10 días
    dfM=cS.getDatosVueloMunicipioTermino(str(municipio), (pd.to_datetime(fecha_prediccion) - timedelta(days=30)).strftime("%Y-%m-%d"), fecha_prediccion)
    if dfM.empty:
        df_municipio['idMunicipio']=municipio
        if estimar=='N':
            df_municipio[['num_vuelos_1','num_vuelos_2','num_vuelos_3','num_vuelos_4','num_vuelos_5','num_vuelos_6']] = 0
        else:
            df_municipio[['num_vuelos_1','num_vuelos_2','num_vuelos_3','num_vuelos_4','num_vuelos_5','num_vuelos_6']] = np.nan
        dfMunicipios=df_municipio

    else:
        dfM = dfM.sort_values(['idTermino','fecha'])
        dfM = dfM.sort_values(['fecha'])
        dfM['anyo']=anyo
        
        # Las muestras se realizan lunes, miércoles y viernes, se estima que hay riesto alto cuando la suma de una semana supera los tres vuelos
        dfM['num_vuelos_n']=dfM['numVuelos'].rolling(window=3, min_periods=1,center=False).sum()
       
        # Si la menor de las últimas muestras es del año pasado, inicializamos contador
        dfM.loc[dfM['anyo'] !=dfM['anyo'].shift(3).fillna(0), 'num_vuelos_n'] = 0
    
        # Nos quedamos con el máximo de incidencia y de vuelos para cada fecha en cada municipio
        # pues es como trabajan en la denominación
        dfM = dfM.groupby(['idMunicipio',  'fecha','anyo']).agg({
                    'numVuelos':'max'}).reset_index()
        
        # Le añadimos la incidencia de la semana pasada y la anterior para emular un time series
        for i in range(1, 7):
            if len(dfM) >= i:
                dfM[f'num_vuelos_{i}'] = dfM['numVuelos'].shift(i - 1)
            else:
                dfM[f'num_vuelos_{i}'] = np.nan
        
        dfMerged= pd.merge_asof(df_municipio, dfM, on=['fecha'], direction='backward', tolerance=pd.Timedelta(days=7)) 
        # Aseguramos que todas las columnas de vuelos existen
        for i in range(1, 7):
            col = f'num_vuelos_{i}'
            if col not in dfMerged.columns:
                dfMerged[col] = np.nan
        dfMerged['anyo']=dfMerged['anyo_x']
        #dfMerged = dfMerged.dropna(subset=["incidencia"])
        dfMunicipios= pd.concat([df_municipio, dfMerged], axis=0)
        dfMunicipios=dfMunicipios[dfMunicipios['idMunicipio']==municipio]
    return dfMunicipios[['idMunicipio','altitud','fecha','anyo','generacion', \
                          'semana','dia_g','dias_grado_ac','t_min','t_max','t_med','hr_med','vv_med', \
                         't_min_p','t_max_p','t_med_p','hr_med_p','vv_med_p', \
                         'dias_grado_ac_7','rg_md_7', 'hr_md_7','t_med_7',\
                         'dias_grado_ac_28','rg_md_28', 'hr_md_28','t_med_28',\
                         'num_vuelos_1','num_vuelos_2','num_vuelos_3','num_vuelos_4','num_vuelos_5','num_vuelos_6']]
# Obtención del modelo de predicción

