# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 17:55:56 2021

@author: Raska
"""


import findspark
findspark.init()
import pyspark
findspark.find()
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)



# In[2]:

#reading the csv into a spark dataframe
data = spark.read.csv('data.csv', header=True, inferSchema=True)

#%% Defining continents for UDF

import numpy as np
import pandas as pd
from matplotlib import path
def getContinent(lon, lat):

    # Coordinates - America
    LonNAm = np.array([90,       90,  78.13,      57.5,  15,  15,  1.25,  1.25,  51,  60,    60])
    LatNAm = np.array([-168.75, -10,    -10,     -37.5, -30, -75, -82.5, -105,  -180, -180, -168.75])
    LatNA2 = np.array([51,    51,  60])
    LonNA2 = np.array([166.6, 180, 180])
    LatNA3 = np.array([22, 18, 19, 23])#hawaii
    LonNA3 = np.array([-160, -160, -153, -153])#hawaii
    LonSAm = np.array([1.25,  1.25,   15,  15, -60, -60])
    LatSAm = np.array([-105, -82.5,  -75, -30, -30, -105])

    # Coordinates - Europe
    LonEur = np.array([90,   90,  42.5, 42.5, 40.79, 41, 40.55, 40.40, 40.05, 39.17, 35.687499, 35.46, 33,   38,  35.42, 28.25, 15,  57.5,  78.13])
    LatEur = np.array([-10, 77.5, 48.8, 30,   28.81, 29, 27.31, 26.75, 26.36, 25.19, 13.911866, 27.91, 27.5, 10, -10,  -13,   -30, -37.5, -10])
    LatEu1 = np.array([14.150906, 14.090299, 14.811997, 14.826364])
    LonEu1 = np.array([36.304948, 35.741447, 35.710506, 36.195053])
    
    #Coordinates - Africa
    LonAfr = np.array([15,  28.25, 35.42, 35.687499, 38, 33,   31.74, 29.54, 27.78, 11.3, 12.5, -60, -60])
    LatAfr = np.array([-30, -13,   13.911866,-10, 10, 27.5, 34.58, 34.92, 34.46, 44.3, 52,    75, -30])
    LonAf1 = np.array([32.035586, 32.035586, 31.338941, 31.338941])
    LatAf1 = np.array ([-6.00000, - 8.338103, -8.338103, -6.00000])
    
    #Coordinates - Asia
    LonAsi = np.array([90,   42.5, 42.5, 40.79, 41, 40.55, 40.4,  40.05, 39.17, 35.46, 33,   31.74, 29.54, 27.78, 11.3, 12.5, -60, -60, -31.88, -11.88, -10.27, 33.13, 51,    60,  90])
    LatAsi = np.array([77.5, 48.8, 30,   28.81, 29, 27.31, 26.75, 26.36, 25.19, 27.91, 27.5, 34.58, 34.92, 34.46, 44.3, 52,   75,  110,  110,  110,    140,    140,   166.6, 180, 180])
    LatAs2 = np.array([90,    90,      60,      60,])
    LonAs2 = np.array([-180, -168.75, -168.75, -180,])
    
    #Coordinates - Antarctica
    LonAnt = np.array([-60, -60, -90, -90])
    LatAnt = np.array([-180, 180, 180, -180])
    
    def inContinent(xq, yq, xv, yv):
        xq = np.array(xq)
        yq = np.array(yq)
        xv = np.array(xv)
        yv = np.array(yv)
        shape = xq.shape
        xq = xq.reshape(-1)
        yq = yq.reshape(-1)
        xv = xv.reshape(-1)
        yv = yv.reshape(-1)
        q = [(xq[i], yq[i]) for i in range(xq.shape[0])]
        p = path.Path([(xv[i], yv[i]) for i in range(xv.shape[0])])
        return p.contains_points(q).reshape(shape)

    def inNA(lat, lon):
        if (lat==0 and lon==0) or (pd.isna(lat)==True or pd.isna(lon)==True):
            return True
        else:
            return False
    
    #Checking truth values
    inIntl = inNA(lat, lon)
    
    if inIntl==True:
        return 'Not applicable'
    else: 
        inNAm = inContinent(lon, lat, LonNAm, LatNAm)
        inNA2 = inContinent(lon, lat, LonNA2, LatNA2)
        inNA3 = inContinent(lon, lat, LonNA3, LatNA3)
        inSAm = inContinent(lon, lat, LonSAm, LatSAm)
    
    if inNAm==True or inNA2==True or inNA3==True or inSAm==True:
        return 'America'
    else: 
        inEur = inContinent(lon, lat, LonEur, LatEur)
        inEu1 = inContinent(lon, lat, LonEu1, LatEu1)
    
    if inEur==True or inEu1==True:
        return 'Europe'
    
    else:
        inAsi = inContinent(lon, lat, LonAsi, LatAsi)
        inAs2 = inContinent(lon, lat, LonAs2, LatAs2)
    
    if inAsi==True or inAs2==True:
        return 'Asia'
    else:
        inAfr = inContinent(lon, lat, LonAfr, LatAfr)
        inAf1 = inContinent(lon, lat, LonAf1, LatAf1)
    
    if inAfr==True or inAf1==True:
        return 'Africa'
    else:
        inAnt = inContinent(lon, lat, LonAnt, LatAnt)
    
    if inAnt==True:
        return 'Antarctica'
    else:
        return 'Oceania'

#%%
# 
from pyspark.sql.functions import udf
continentLabeler = udf(lambda lon, lat: getContinent(lon, lat))
spark.udf.register("continentLabeler", continentLabeler)

data = data.withColumn('Continent', continentLabeler('Lat', 'Long'))

