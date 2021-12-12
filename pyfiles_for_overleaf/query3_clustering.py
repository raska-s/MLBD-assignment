# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 17:23:31 2021

@author: Raska
"""

#%% Importing libraries
import numpy as np
import findspark
findspark.init()
import pyspark
findspark.find()
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

### Cluster configuration
# conf = pyspark.SparkConf()
# conf.setMaster("spark://login1-sinta-hbc:7077").setAppName("jupyter") #comment out if not using cluster

# spark = pyspark.sql.SparkSession.builder \
#     .master("spark://login1-sinta-hbc:7077") \
#     .appName("jupyter") \
#     .getOrCreate()

## Local configuration
conf = pyspark.SparkConf().setAppName('SparkApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

from pyspark.sql import functions as F 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.functions import udf
from pyspark.sql import types as T
import pandas as pd

#%% Defining all functions
def getMonthlyIncreases(data):
    ### Wrapper function to run PySpark calculations of monthly increases
    # Dropping unwanted columns and grouping by countries
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
    #Concatenating the number of days in each month    
    lstdat = []
    for i in lastdates:
        if i == '1/31/20':
            lstdat.append(i+"-8"+"-days")
        else:
            lstdat.append(i+"-"+i.split("/")[1]+"-days")
            
    Data_datesLast = renamed_frame_afterGroupby.select(lastdates)
    #Renaming multiple columns
    mapping = dict(zip(lastdates,lstdat))
    Last_dates_data = Data_datesLast.select([F.col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])
    
    #Initializing variables to assist in joining dataframes
    ls = Last_dates_data.columns
    fin = Last_dates_data.select(Last_dates_data.columns[0])
    DF1 = fin.withColumn("row_id", row_number().over(w))
    
    #Looping though the Dataframe and calculating the mean, and then joining
    for i in range(len(ls)):
        if i == 0:
            pass
        else:
            increases_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1])))
            increases_singleframe = increases_fullframe.select(increases_fullframe.columns[i])
            DF3 = increases_singleframe.withColumn("row_id", row_number().over(w))
            DF1 = DF1.join(DF3, ("row_id"))
    
    #Adding an index to country/region to join
    country = country_region.withColumn("row_id", row_number().over(w))
    
    #Joining the country with the remaining dates dataframe
    monthly_increases = DF1.join(country, on="row_id", how='full').drop("row_id")
    
    #Rearranging the columns and getting the final result
    lst = monthly_increases.columns
    monthly_increases = monthly_increases.select(lst[-4:] + lst[:-4])
    monthly_increases = monthly_increases.toPandas()
    return monthly_increases

def getMonthlyAverage(data):
    ### Wrapper function to run PySpark calculations of monthly average
    #Dropping unwanted columns and grouping by countries
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
    #Concatenating the number of days in each month    
    lstdat = []
    for i in lastdates:
        if i == '1/31/20':
            lstdat.append(i+"-8"+"-days")
        else:
            lstdat.append(i+"-"+i.split("/")[1]+"-days") 
    Data_datesLast = renamed_frame_afterGroupby.select(lastdates)
    #Renaming multiple columns
    mapping = dict(zip(lastdates,lstdat))
    Last_dates_data = Data_datesLast.select([col(c).alias(mapping.get(c, c)) for c in Data_datesLast.columns])
    
    #Initializing variables to assist in joining dataframes
    ls = Last_dates_data.columns
    fin = Last_dates_data.select(Last_dates_data.columns[0])
    DF1 = fin.withColumn("row_id", row_number().over(w))
    
    #Looping though the Dataframe and calculating the mean, and then joining
    for i in range(len(ls)):
        if i == 0:
            pass
        else:
            increases_fullframe = Last_dates_data.withColumn(ls[i],(F.col(ls[i])-F.col(ls[i-1]))/int(ls[i].split("-")[1]))
            increases_singleframe = increases_fullframe.select(increases_fullframe.columns[i])
            DF3 = increases_singleframe.withColumn("row_id", row_number().over(w))
            DF1 = DF1.join(DF3, ("row_id"))
    
    #Adding an index to country/region to join
    country = country_region.withColumn("row_id", row_number().over(w))
    
    #Joining the country with the remaining dates dataframe
    mean_values = DF1.join(country, on="row_id", how='full').drop("row_id")
    
    #Rearranging the columns and getting the final result
    lst = mean_values.columns
    mean_values = mean_values.select(lst[-4:] + lst[:-4])
    mean_values = mean_values.toPandas()
    return mean_values

def linearTrendlineCoefficient(*args):
    """
    Wrapper function of linear regression optimised for PySpark data

    Parameters
    ----------
    *args : tuple
        Unbounded list of data points.

    Returns
    -------
    float
        Linear coefficient of linear trendline fit of data points.

    """
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

def kMeansFit(*args):
    """
    Function to cluster data points

    Parameters
    ----------
    *args : tuple
        Unbounded list of data points.

    Returns
    -------
    out : string
        String of cluster IDs, in place of list to support conversion into PySpark UDF.

    """
    import numpy as np
    import pandas as pd
    k=4
    np.random.seed(42)
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
        centroid_update = []
        for i in range(len(centroids)):
            centroid_y = 0
            count = 1
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
    count = 1
    tol = None
    while True:
        centroids_pointwise = __getPointwiseCentroid(y, centroids)
        centroids = __updateCentroid(y, centroids, centroids_pointwise)
        avg = np.mean(centroids)
        if tol is not None and avg == tol:
            break
        tol = avg
        count+=1 
    out = __getPointwiseCentroid(y, centroids)
    _, out = np.unique(out, return_inverse=True)
    out = list(out)
    out = str(out)
    return out

def convertClusteringOutput(clusters):
    """
    Converts string output from kMeansFit into array.

    Parameters
    ----------
    clusters : string
        String form of cluster identification.

    Returns
    -------
    out : Pandas dataframe
        Dataframe form of cluster identification.

    """
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
    out.set_axis(['01/20', '02/20', '03/20', '04/20', '05/20', '06/20', '07/20', '08/20', '09/20','10/20', '11/20', '12/20', '01/21', '02/21', '03/21', '04/21', '05/21', '06/21','07/21', '08/21', '09/21', '10/21', '11/21'], axis=1, inplace=True)
    return out

def normaliseValuesStd(df):
    """
    Normalises values between 0 and 1

    Parameters
    ----------
    df : pandas Dataframe
        Data points to be normalised.

    Returns
    -------
    out : pandas Dataframe
        Normalised data points.

    """
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

#%% Reading the csv into a PySpark dataframe
data = spark.read.csv('data.csv', header=True, inferSchema=True)
#%% Obtainining monthly increases and monthly averages and  cleaning
monthly_increases = getMonthlyIncreases(data)
mean_values = getMonthlyAverage(data)
monthly_increases = spark.createDataFrame(monthly_increases)
mean_values = spark.createDataFrame(mean_values)
df_vals = data.drop('Province/State', 'Country/Region', 'Lat', 'Long')
df_headers = data.select('Province/State', 'Country/Region', 'Lat', 'Long')

#%% Applying linear trendline coefficient calculation and sorting
#Convert to UDF
getLinearTrendlineCoef = udf(lambda *args: linearTrendlineCoefficient(*args), T.FloatType())
#Selecting columns for trendline
w = Window.partitionBy(lit(1)).orderBy(lit(1))
df_coef = monthly_increases.select(monthly_increases.columns[4:])
#Fitting trendline 
df_coef = df_coef.withColumn('linear_coef', getLinearTrendlineCoef(*[F.col(i) for i in df_coef.columns]))
#Sorting output
df_coef = df_coef.withColumn("row_id", row_number().over(w))
df_vals = df_vals.withColumn("row_id", row_number().over(w))
df_headers = df_headers.withColumn("row_id", row_number().over(w))
mean_values = mean_values.withColumn("row_id", row_number().over(w))
df_headers = df_headers.join(df_coef.select('linear_coef','row_id'), on='row_id', how='full_outer')
df_mean = df_headers.join(mean_values.drop('Province/State', 'Country/Region', 'Lat', 'Long'), on="row_id", how='full_outer').drop("row_id")
df_mean = df_mean.sort(F.col("linear_coef").desc())

#%% Taking only top 50 values
top50 = df_mean.limit(50)

#%% Transposing output out of PySpark to ease computation of Kmeans
top50_p = top50.toPandas()
top50_p = top50_p.drop(columns=['Province/State', 'Country/Region', 'Lat', 'Long', 'linear_coef'])
top50_p = top50_p.T
top50_T = spark.createDataFrame(top50_p)

#%% K-means clustering
getKmeansCluster = udf(lambda *args: kMeansFit(*args), T.StringType())
# Fitting cluster 
top50_T = top50_T.withColumn('cluster_id', getKmeansCluster(*[F.col(i) for i in top50_T.columns]))
# Sorting cluster output
clusters = top50_T.select('cluster_id').toPandas()

#%% Converting output and joining with data
df_clusterID = convertClusteringOutput(clusters)
df_clusterID = spark.createDataFrame(df_clusterID)
df_clusterID = df_clusterID.withColumn("row_id", row_number().over(w))
top50_headers = top50.select('Province/State', 'Country/Region', 'Lat', 'Long', 'linear_coef').withColumn("row_id", row_number().over(w))
df_clusterID = top50_headers.join(df_clusterID, on='row_id', how='full').drop('row_id')
pdClusterID = df_clusterID.toPandas()

#%% Writing  output to CSV
months = ['01/20', '02/20', '03/20', '04/20', '05/20', '06/20', '07/20', '08/20', '09/20','10/20', '11/20', '12/20', '01/21', '02/21', '03/21', '04/21', '05/21', '06/21','07/21', '08/21', '09/21', '10/21', '11/21']
pdClusterID.to_csv('cluster_out.csv')

#%% Sorting output for plotting
import matplotlib.pyplot as plt

monthlyIncTop50 = df_headers.join(df_coef.drop('linear_coef'), on="row_id", how='full_outer').drop("row_id")
monthlyIncTop50 = monthlyIncTop50.sort(F.col("linear_coef").desc()).limit(50)
pdMonthlyInc = monthlyIncTop50.toPandas()
pdMonthlyMean= top50.toPandas()

#%% Visualisation 
vis_month_selection = -1
cluster_colour = pdClusterID.iloc[:, vis_month_selection]
vis_data = pd.concat((pdMonthlyMean['linear_coef'], pdMonthlyMean.iloc[:, vis_month_selection]), axis=1)
# print(pdClusterID.iloc[:, vis_month_selection].name)
vis_data = np.array(vis_data)
from scipy.spatial import ConvexHull

#Plot 1: Monthly scatterplot
def drawclusters(ax,  X, labels, colours, ncluster=4):
    """
    Draws clusters and fits a convex hull to ease visualisation. A convex hull
    is the smallest convex boundary of the cluster cloud.

    Parameters
    ----------
    ax : plt object
        matplotlib object instance.
    X : numpy array
        scatter data to be plotted.
    labels : numpy array
        data labels for each point.
    colours : list
        list of selected clusters.
    ncluster : int, optional
        Number of convex hull instances to generate. The default is 4.

    Returns
    -------
    None.

    """
    for i in range(ncluster):
        points = X[labels == i]
        ax.scatter(points[:, 0], points[:, 1], s=30, c=colours[i], label=f'Cluster {i}')
        ax.legend()
        hull = ConvexHull(points)
        vert = np.append(hull.vertices, hull.vertices[0])  # close the polygon by appending the first point at the end
        ax.plot(points[vert, 0], points[vert, 1], '--', c=colours[i])
        ax.fill(points[vert, 0], points[vert, 1], c=colours[i], alpha=0.2)
        ax.set_xlabel('Overall daily linear coefficient')
        ax.set_ylabel('Mean of daily cases in a month')
        ax.set_title('Clustering of daily rates of month' )
        
fig, ax = plt.subplots(1, figsize=(7, 5))
colours = ['red', 'green', 'blue', 'orange']
drawclusters(ax, vis_data, cluster_colour, colours, ncluster=4)

#Plot 2: Seaborn heatmap plotting
import seaborn as sns
heatmap_out = pdClusterID.drop(columns=['Country/Region', 'Province/State', 'Lat', 'Long', 'linear_coef'])
plt.figure(figsize=(16, 10))

cmap = sns.color_palette("coolwarm", 4)
g = sns.heatmap(heatmap_out, cmap=cmap, linewidth=0.05, linecolor='lightgrey',  cbar_kws={"ticks":[0, 1, 2, 3]}, square=True)
g.set_yticklabels(pdClusterID['Country/Region'])
g.set_xticklabels(months, rotation = 80)
plt.xlabel('')
plt.ylabel('')
plt.savefig('globalclust.pdf')