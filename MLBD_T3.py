# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 17:23:31 2021

@author: Raska
"""

# In[1]:


import findspark
findspark.init()
import pyspark
findspark.find()
from pyspark.sql.functions import col
import dateutil.parser
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession,types
from pyspark.sql.functions import col,regexp_replace
from pyspark.sql import functions as F 
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit


# In[2]:

#reading the csv into a spark dataframe
data = spark.read.csv('data.csv', header=True, inferSchema=True)

# In[3]:
    
#dropping unwanted columns and grouping by countries
df = data

# .groupBy("Province/State").sum()
# In[4]:

w = Window.partitionBy(lit(1)).orderBy(lit(1))

#separating the country column and dates column
country_region = df.select(df.columns[:4])
data_by_dates = df.select(df.columns[4:])


# In[5]:


#Renaming date columns after groupby
headers_before_groupby =  data.select(data.columns[4:]).columns
headers_after_groupby = data_by_dates.columns

mapping = dict(zip(headers_after_groupby,headers_before_groupby))
renamed_frame_afterGroupby = data_by_dates.select([col(c).alias(mapping.get(c, c)) for c in data_by_dates.columns])


# In[6]:


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


# In[7]:


#renaming multiple columns
from pyspark.sql.functions import col

mapping = dict(zip(lastdates,lstdat))
Last_dates_data = Data_datesLast.select([col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])


# In[8]:


#initializing variables to assist in joining dataframes
ls = Last_dates_data.columns
fin = Last_dates_data.select(Last_dates_data.columns[0])
DF1 = fin.withColumn("row_id", row_number().over(w))


# In[9]:


#looping though the Dataframe and calculating the mean, and then joining
for i in range(len(ls)):
    if i == 0:
        pass
    else:
        mean_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1])))
        mean_singleframe = mean_fullframe.select(mean_fullframe.columns[i])
        DF3 = mean_singleframe.withColumn("row_id", row_number().over(w))
        
        DF1 = DF1.join(DF3, ("row_id"))
# -F.col(ls[i-1])

# In[10]:


#adding an index to country/region to join
country = country_region.withColumn("row_id", row_number().over(w))


# In[11]:


#joining the country with the remaining dates dataframe
Final = DF1.join(country, on="row_id", how='full').drop("row_id")


# In[12]:


#Rearranging the columns and getting the final result
lst = Final.columns
Final = Final.select(lst[-4:] + lst[:-4])


# In[13]:
Final.printSchema()


# In[15]:
from pyspark.sql.functions import udf
from pyspark.sql import types as T

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
df_coef = Final.select(Final.columns[4:])
#Fitting trendline 
df_coef = df_coef.withColumn('Linear Coef', getLinearTrendlineCoef(*[F.col(i) for i in df_coef.columns]))

df_coef = df_coef.withColumn("row_id", row_number().over(w))
df_coef = df_coef.join(country, on="row_id", how='full_outer').drop("row_id")
df_coef = df_coef.select(df_coef.columns[-5:] + df_coef.columns[:-5])
coef_pandas = df_coef.toPandas()
coef_transposed = coef_pandas.T