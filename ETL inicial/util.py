#!/usr/bin/env python
# coding: utf-8

# In[3]:


import datetime
import numpy as np
import math
import os
os.chdir('D:\\Plagas')


# In[10]:


## Trabajar con fechas, para insertar en sql hay que convertir a dd-mm-YYYY
## Me devuelve una fecha con un formato string para poder insertarlo en sql server (que me hace la conversión él)
## Lo usaremos si es un CSV
def convertirString(fecha,origen):
    out_format= '%Y-%m-%d'
    if (origen=='CSV'):
        in_format='%d/%m/%Y'
    elif (origen=='CSVL'):
          in_format='%Y%m%d'
    elif (origen=='BD'):
        in_format='%Y-%m-%d'
    elif (origen=='AEMET'):
        in_format='%Y-%m-%dT%H:%M:%S'
    else:
        in_format='%Y-%m-%d %H:%M:%S'
    ##in_format='%Y-%m-%d %H:%M:%S'
    fecha=datetime.datetime.strptime(fecha, in_format)
    strfecha=fecha.strftime(out_format)
    return strfecha


# In[13]:


## Función que me calcula la distancia entre dos puntos (con latitud y longitud)
def haversine(lat1, lon1, lat2, lon2):
    # Radio de la Tierra en kilómetros
    R = 6371.0

    # Convertir las coordenadas de grados a radianes
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Diferencias de coordenadas
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Fórmula del Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c

    return distance


# In[ ]:




