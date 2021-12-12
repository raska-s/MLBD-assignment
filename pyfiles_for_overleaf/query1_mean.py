#!/usr/bin/env python
# coding: utf-8

# In[1]:
import findspark
findspark.init()
import pyspark
from pyspark.sql.functions import row_number, lit
findspark.find()
from pyspark.sql import SparkSession

### Cluster configuration
# conf = pyspark.SparkConf()
# conf.setMaster("spark://login1-sinta-hbc:7077").setAppName("jupyter") #comment out if not using cluster

# spark = pyspark.sql.SparkSession.builder \
#     .master("spark://login1-sinta-hbc:7077") \
#     .appName("jupyter") \
#     .getOrCreate()

conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import functions as F 
from pyspark.sql.window import Window

# In[2]:
###This function takes in 1 parameter data(the dataset used for this project), and returns a pyspark dataframe containing the average daily cases for each month per country.
def mean_over_month(data):
    #dropping unwanted columns and grouping by countries
    df = data.drop("Province/State","Lat","Long").groupBy("Country/Region").sum()
    #separating the country column and dates column
    country_region = df.select(df.columns[0])
    data_by_dates = df.select(df.columns[1:])
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
    from pyspark.sql.functions import col

    mapping = dict(zip(lastdates,lstdat))
    Last_dates_data = Data_datesLast.select([col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])
    #initializing variables to assist in joining dataframes
    w = Window.partitionBy(lit(1)).orderBy(lit(1))
    ls = Last_dates_data.columns
    fin = Last_dates_data.select(Last_dates_data.columns[0])
    DF1 = fin.withColumn("row_id", row_number().over(w))
    #looping though the Dataframe and calculating the mean, and then joining
    for i in range(len(ls)):
        if i == 0:
            pass
        else:
            mean_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1]))/int(ls[i].split("-")[1]))
            mean_singleframe = mean_fullframe.select(mean_fullframe.columns[i])
            DF3 = mean_singleframe.withColumn("row_id", row_number().over(w))
            DF1 = DF1.join(DF3, ("row_id"))
    #adding an index to country/region to join
    country = country_region.withColumn("row_id", row_number().over(w))
    #joining the country with the remaining dates dataframe
    Final = DF1.join(country, on="row_id", how='full').drop("row_id")
    #Rearranging the columns and getting the final result
    lst = Final.columns
    Mean_perCountry_perMonth = Final.select(lst[-1:] + lst[:-1])
    return Mean_perCountry_perMonth

# In[3]:
###This function plots a horizontal bar graph and takes in 2 parameters, data(the pandas dataframe to be plotted) and the title(the title of the plot)
def plot_func_bar(data,title):
    ax = data.plot.barh(stacked=True,figsize=(10,10),title=title)
    ax.set_xlabel('AVERAGE CASES PER DAY')
    ax.set_ylabel('MONTHS')
###This function plots a line graph and takes in 2 parameters, data(the pandas dataframe to be plotted) and the title(the title of the plot)    
def plot_func_line(data,title):
    ax = data.plot.line(figsize=(10,10),title=title)
    ax.set_xlabel('MONTHS')
    ax.set_ylabel('AVERAGE CASES PER DAY')

# In[4]:
##Reading the csv from the local
data = spark.read.csv('data.csv', header=True, inferSchema=True)

# In[5]:
#uncomment if you need to add the cases of summer olympics 2020 to Japan
#data = data.withColumn("Country/Region",when(col("Country/Region") == "Summer Olympics 2020","Japan").otherwise(col("Country/Region")))

# In[6]:
#Calling the function to get the mean values
mean_values = mean_over_month(data)

# In[7]:
#presenting the data in a better format
pretty_mean = mean_values.toPandas()
pretty_mean.rename(columns={'1/31/20-8-days': '1/20', '2/29/20-29-days': '2/20', '3/31/20-31-days': '3/20', '4/30/20-30-days': '4/20', '5/31/20-31-days': '5/20', '6/30/20-30-days': '6/20','7/31/20-31-days':'7/20','8/31/20-31-days':'8/20','9/30/20-30-days':'9/20','10/31/20-31-days':'10/20','11/30/20-30-days':'11/20','12/31/20-31-days':'12/20','1/31/21-31-days':'1/21','2/28/21-28-days':'2/21','3/31/21-31-days':'3/21','4/30/21-30-days':'4/21','5/31/21-31-days':'5/21','6/30/21-30-days':'6/21','7/31/21-31-days':'7/21','8/31/21-31-days':'8/21','9/30/21-30-days':'9/21','10/31/21-31-days':'10/21','11/23/21-23-days':'11/21'}, inplace=True)
final = pretty_mean.set_index('Country/Region').T
# In[8]:
#selecting the countries with the highest and the lowest number of cases
most_cases = final[['US','India','Brazil','United Kingdom','Russia','Turkey','France','Germany','Iran','Argentina']]
least_cases = final[['Micronesia','Tonga','Kiribati','Samoa','Marshall Islands','Vanuatu','Palau','MS Zaandam','Solomon Islands','Holy See']]

# In[9]:
##plotting the graph for the countries with the highest number of cases by calling plot_func function
plot_func_bar(most_cases,"DISTRIBUTION OF AVERAGE DAILY CASES PER MONTH FOR THE MOST AFFECTED COUNTRIES")

# In[10]:
###plotting the graph for the countries with the highest number of cases by calling plot_func function
plot_func_bar(least_cases,"DISTRIBUTION OF AVERAGE DAILY CASES PER MONTH FOR THE LEAST AFFECTED COUNTRIES")

# In[11]:
##plotting the graph for the countries with the highest number of cases by calling plot_func function
plot_func_line(most_cases,"AVERAGE DAILY CASES PER MONTH FOR THE MOST AFFECTED COUNTRIES")

# In[12]:
###plotting the graph for the countries with the least number of cases by calling plot_func function
plot_func_line(least_cases,"AVERAGE DAILY CASES PER MONTH FOR THE LEAST AFFECTED COUNTRIES")
