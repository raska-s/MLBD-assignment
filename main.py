# -*- coding: utf-8 -*-
"""
Created on Thu Nov 25 10:39:11 2021

@author: Raska
"""
#%% Importing libraries
import pandas as pd
# import numpy as np
import findspark
findspark.init()
from pyspark.sql import SparkSession

#%% Loading and cleaning data

# Loading data & removing index
fname = 'data.csv'
df = pd.read_csv(fname)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Splitting data into headers & values
df_headers = df[['Province/State', 'Country/Region', 'Lat', 'Long']]
df_vals = df.drop(columns=['Province/State', 'Country/Region', 'Lat', 'Long'])

# Date conversion
dates = pd.DataFrame(df_vals.columns)
dates = dates[0].map(lambda x : pd.to_datetime(x))

#convert monthly datetime format into string
year_month_str= dates.dt.to_period('M').dt.strftime('%Y-%m')
#convert daily datetime format into string
year_month_date_str = dates.dt.to_period('D').dt.strftime('%Y-%m-%d')

#getting list of months since first date
unique_months = list(set(year_month_str))
unique_months.sort()

#fixing format of dates in original dates
df_vals.columns = year_month_date_str
df_headers = df_headers.reset_index(drop=True)
df = df_headers.join(df_vals)
#%% Pandas method for [1]
# running calculations for mean
avg_list = []
for months in unique_months:
    selected_data = [s for s in df_vals.columns if months in s]
    ndays = len(selected_data)
    total = df_vals[selected_data].sum(axis=1)
    avg = total/ndays
    avg_list.append(avg)
# concatenating output
avg_list = pd.concat(avg_list, axis=1)
avg_list.columns = unique_months
df_monthly_avg = df_headers.join(avg_list)

#%% Starting spark
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#%% Initialise Spark method for [1]
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark.sparkContext)

# creating spark DF 
df_vals_spark=spark.createDataFrame(df_vals) 
output = []
#%% Run Spark method for [1]
# running calculations for mean
for months in unique_months:
    days_in_month = [s for s in df_vals_spark.columns if months in s]
    selected_values = df_vals_spark.select(days_in_month)
    tot = selected_values.withColumn(months, sum(selected_values[col] for col in selected_values.columns)).select(months)
    avg = tot.withColumn(months, tot[months]/ndays)
    avg = avg.withColumn("key", monotonically_increasing_id())
    output.append(avg)

# concatenating output (this is the most expensive process)
df_monthly_avg_spark = output[0]
for df_next in output[1:]:
    df_monthly_avg_spark = df_monthly_avg_spark.join(df_next,on='key',how='inner')
df_monthly_avg_spark.show()

