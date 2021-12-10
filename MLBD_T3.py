# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 17:23:31 2021

@author: Raska
"""

# In[1]:
# TODO: KMEans for pyspark

import findspark
findspark.init()
import pyspark
findspark.find()
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit

#%%reading the csv into a spark dataframe
data = spark.read.csv('data.csv', header=True, inferSchema=True)
#%%

def getMonthlyIncreases(data):
    
    #dropping unwanted columns and grouping by countries
    df = data
    w = Window.partitionBy(lit(1)).orderBy(lit(1))
    country_region = df.select(df.columns[:4])
    data_by_dates = df.select(df.columns[4:])
    
    
    
    #Renaming date columns after groupby
    headers_before_groupby =  data.select(data.columns[4:]).columns
    headers_after_groupby = data_by_dates.columns
    
    mapping = dict(zip(headers_after_groupby,headers_before_groupby))
    renamed_frame_afterGroupby = data_by_dates.select([F.col(c).alias(mapping.get(c, c)) for c in data_by_dates.columns])
    
    #Getting the last date in a month as the other days are redundant for finding the mean
    lastdates = []
    for i in range(len(headers_before_groupby)):
        try:
            if headers_before_groupby[i+1].split("/")[0] != headers_before_groupby[i].split("/")[0]:
                lastdates.append(headers_before_groupby[i])
        except:
            lastdates.append(headers_before_groupby[-1])
    #concatenating the number of days in each month    
    lstdat = []
    for i in lastdates:
        if i == '1/31/20':
            lstdat.append(i+"-8"+"-days")
        else:
            lstdat.append(i+"-"+i.split("/")[1]+"-days")
            
    Data_datesLast = renamed_frame_afterGroupby.select(lastdates)
    #renaming multiple columns

    mapping = dict(zip(lastdates,lstdat))
    Last_dates_data = Data_datesLast.select([F.col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])
    
    #initializing variables to assist in joining dataframes
    ls = Last_dates_data.columns
    fin = Last_dates_data.select(Last_dates_data.columns[0])
    DF1 = fin.withColumn("row_id", row_number().over(w))
    
    #looping though the Dataframe and calculating the mean, and then joining
    for i in range(len(ls)):
        if i == 0:
            pass
        else:
            increases_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1])))
            increases_singleframe = increases_fullframe.select(increases_fullframe.columns[i])
            DF3 = increases_singleframe.withColumn("row_id", row_number().over(w))
            DF1 = DF1.join(DF3, ("row_id"))
    
    #adding an index to country/region to join
    country = country_region.withColumn("row_id", row_number().over(w))
    
    #joining the country with the remaining dates dataframe
    monthly_increases = DF1.join(country, on="row_id", how='full').drop("row_id")
    
    #Rearranging the columns and getting the final result
    lst = monthly_increases.columns
    monthly_increases = monthly_increases.select(lst[-4:] + lst[:-4])
    monthly_increases = monthly_increases.toPandas()
    return monthly_increases

def getMonthlyAverage(data):
    
    #dropping unwanted columns and grouping by countries
    df = data
    w = Window.partitionBy(lit(1)).orderBy(lit(1))
    country_region = df.select(df.columns[:4])
    data_by_dates = df.select(df.columns[4:])
    
    
    
    #Renaming date columns after groupby
    headers_before_groupby =  data.select(data.columns[4:]).columns
    headers_after_groupby = data_by_dates.columns
    
    mapping = dict(zip(headers_after_groupby,headers_before_groupby))
    renamed_frame_afterGroupby = data_by_dates.select([F.col(c).alias(mapping.get(c, c)) for c in data_by_dates.columns])
    
    #Getting the last date in a month as the other days are redundant for finding the mean
    lastdates = []
    for i in range(len(headers_before_groupby)):
        try:
            if headers_before_groupby[i+1].split("/")[0] != headers_before_groupby[i].split("/")[0]:
                lastdates.append(headers_before_groupby[i])
        except:
            lastdates.append(headers_before_groupby[-1])
    #concatenating the number of days in each month    
    lstdat = []
    for i in lastdates:
        if i == '1/31/20':
            lstdat.append(i+"-8"+"-days")
        else:
            lstdat.append(i+"-"+i.split("/")[1]+"-days")
            
    Data_datesLast = renamed_frame_afterGroupby.select(lastdates)
    #renaming multiple columns
    
    mapping = dict(zip(lastdates,lstdat))
    Last_dates_data = Data_datesLast.select([col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])
    
    #initializing variables to assist in joining dataframes
    ls = Last_dates_data.columns
    fin = Last_dates_data.select(Last_dates_data.columns[0])
    DF1 = fin.withColumn("row_id", row_number().over(w))
    
    #looping though the Dataframe and calculating the mean, and then joining
    for i in range(len(ls)):
        if i == 0:
            pass
        else:
            increases_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1]))/int(ls[i].split("-")[1]))
            increases_singleframe = increases_fullframe.select(increases_fullframe.columns[i])
            DF3 = increases_singleframe.withColumn("row_id", row_number().over(w))
            DF1 = DF1.join(DF3, ("row_id"))
    
    #adding an index to country/region to join
    country = country_region.withColumn("row_id", row_number().over(w))
    
    #joining the country with the remaining dates dataframe
    mean_values = DF1.join(country, on="row_id", how='full').drop("row_id")
    
    #Rearranging the columns and getting the final result
    lst = mean_values.columns
    mean_values = mean_values.select(lst[-4:] + lst[:-4])
    mean_values = mean_values.toPandas()
    return mean_values


monthly_increases = getMonthlyIncreases(data)
mean_values = getMonthlyAverage(data)
monthly_increases = spark.createDataFrame(monthly_increases)
mean_values = spark.createDataFrame(mean_values)
df_vals = data.drop('Province/State', 'Country/Region', 'Lat', 'Long')
df_headers = data.select('Province/State', 'Country/Region', 'Lat', 'Long')
# monthly_increases.printSchema()

# In[15]:
from pyspark.sql.functions import udf
from pyspark.sql import types as T
import pandas as pd
#Define standard trendline function
def linearTrendlineCoefficient(*args):
    from sklearn.linear_model import LinearRegression
    import numpy as np
    X = []
    for value in args:
        X.append(value)
    X = np.array(X)
    y = np.arange(len(X))
    X = X.reshape((-1,1))
    y = y.reshape((-1,1))
    reg = LinearRegression().fit(y, X)
    coef_array = reg.coef_
    out = coef_array[0]
    return float(out)

#Convert to UDF
getLinearTrendlineCoef = udf(lambda *args: linearTrendlineCoefficient(*args), T.FloatType())

#Selecting columns for trendline
w = Window.partitionBy(lit(1)).orderBy(lit(1))
df_coef = monthly_increases.select(monthly_increases.columns[4:])
#Fitting trendline 
df_coef = df_coef.withColumn('linear_coef', getLinearTrendlineCoef(*[F.col(i) for i in df_coef.columns]))
df_coef = df_coef.withColumn("row_id", row_number().over(w))
df_vals = df_vals.withColumn("row_id", row_number().over(w))
df_headers = df_headers.withColumn("row_id", row_number().over(w))
mean_values = mean_values.withColumn("row_id", row_number().over(w))
df_headers = df_headers.join(df_coef.select('linear_coef','row_id'), on='row_id', how='full_outer')

df_mean = df_headers.join(mean_values.drop('Province/State', 'Country/Region', 'Lat', 'Long'), on="row_id", how='full_outer').drop("row_id")
df_mean = df_mean.sort(F.col("linear_coef").desc())

#%% Following cells are done to get the mean
top50 = df_mean.limit(50)

#%%
top50_p = top50.toPandas()
top50_p = top50_p.drop(columns=['Province/State', 'Country/Region', 'Lat', 'Long', 'linear_coef'])
top50_p = top50_p.T
top50_T = spark.createDataFrame(top50_p)

#%%

def kMeansFit(*args):
    import numpy as np
    import pandas as pd
    """
    Fits model to data

    Parameters
    ----------
    df : pandas dataframe
        data of features to be fitted.
    k : int, optional
        Desired number of k-clusters in k-means. The default is 4.

    Returns
    -------
    None.

    """
    k=4
    def __normaliseValues(df):
        out = []
        for label, content in df.items():
            feature = df[label]
            numerator = feature - np.min(feature)
            denominator = np.max(feature) - np.min(feature)
            output = numerator/denominator
            out.append(output)
        out = np.array(out).T
        out = pd.DataFrame.from_records(out)
        out.columns = df.columns
        return out    

    def __updateCentroid(y, centroids, centroids_pointwise):
        centroid_y = 0
        count = 0
        centroid_update = []
        for i in range(len(centroids)):
            for n in range(len(centroids_pointwise)):
                if centroids_pointwise[n] == centroids[i]:
                    centroid_y += y[n]
                    count +=1
            centroid_final = centroid_y/count
            centroid_update.append(centroid_final)
        return np.array(centroid_update)
            
    def __getPointwiseCentroid(y, centroids):
        centroids_pointwise = []
        for n in range(len(y)):
            distance_list = []
            y_i = y[n]
            for i in range(len(centroids)):
                distance = np.sqrt((y_i-centroids[i])**2)
                distance_list.append(distance)
            distance_list = np.array(distance_list)
            assigned_centroid = centroids[np.argmin(distance_list)]
            centroids_pointwise.append(assigned_centroid)
        return np.array(centroids_pointwise)
    df = []
    for value in args:
        df.append(value)
    df = pd.DataFrame(df)
    df = __normaliseValues(df)
    y = df.mean(axis=1)
    centroids = y.sample(k)
    centroids.reset_index(drop=True, inplace=True)
    count = 0
    tol = None
    # convergenceHistory = []

    while True:
        centroids_pointwise = __getPointwiseCentroid(y, centroids)
        centroids = __updateCentroid(y, centroids, centroids_pointwise)
        avg = np.mean(centroids)
        if tol is not None and avg == tol:
            break
        tol = avg
        # convergenceHistory.append(tol)
        count+=1 
    # convergenceHistory  = pd.DataFrame(convergenceHistory, columns=['centroid mean'])
    # convergenceHistory.insert(loc=0, column='iter', value=np.arange(1, count+1))
    out = __getPointwiseCentroid(y, centroids)
    _, out = np.unique(out, return_inverse=True)
    out = list(out)
    out = str(out)
    return out

getKmeansCluster = udf(lambda *args: kMeansFit(*args), T.StringType())
#Fitting cluster 
top50_T = top50_T.withColumn('cluster_id', getKmeansCluster(*[F.col(i) for i in top50_T.columns]))

#%%
clusters = top50_T.select('cluster_id').toPandas()

#%%
import numpy as np
def convertClusteringOutput(clusters):
    cluster_array = np.array(clusters)
    cluster_array = list(cluster_array)
    out = []
    for i in cluster_array:
        strsize = len(i[0])
        x = i[0][1:(strsize-1)]
        x = list(map(int, x.split(',')))
        out.append(x)
    out = pd.DataFrame(out)
    out = out.T
    out.set_axis(top50.columns[5:], axis=1, inplace=True)
    return out

df_clusterID = convertClusteringOutput(clusters)
df_clusterID = spark.createDataFrame(df_clusterID)
df_clusterID = df_clusterID.withColumn("row_id", row_number().over(w))
top50_headers = top50.select('Province/State', 'Country/Region', 'Lat', 'Long', 'linear_coef').withColumn("row_id", row_number().over(w))
df_clusterID = top50_headers.join(df_clusterID, on='row_id', how='full').drop('row_id')
aa = df_clusterID.toPandas()
