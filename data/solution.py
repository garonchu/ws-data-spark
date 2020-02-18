# by: Garon Chu
# EQ Works sample
# Feb 2020

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from math import sin, cos, sqrt, atan2, radians, pi
from pyspark.sql.functions import log
from pyspark.sql.functions import stddev_pop
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as func


dataSample = spark.read.csv("/tmp/data/DataSample.csv", header=True) #read in data sample
dataSample = dataSample.withColumnRenamed(" TimeSt", "TimeSt") #clean up column name
dataSampleNoDupe = dataSample.dropDuplicates(['TimeSt', 'Country', 'Province', 'City', 'Latitude', 'Longitude'])  #solution 1, remove dups based on time and location

poi = spark.read.csv("/tmp/data/POIList.csv", header=True) #read in POI list
poi = poi.withColumnRenamed(" Latitude", "Latitude") #clean up column name
poiDict = poi.rdd.map(lambda row: row.asDict()).collect() #convert POI list to a dictionary

#function to get distance between 2 points in KM
def distanceToPoi(row):
	R = 6371.0
	distance = 0
	poi = ''
	lat1 = radians(float(row.Latitude))
	lon1 = radians(float(row.Longitude))
	for i in range(0, len(poiDict)):  #loop thru the list of POI, calculate distance, keep shortest distance POI
		lat2 = radians(float(poiDict[i]['Latitude']))
		lon2 = radians(float(poiDict[i]['Longitude']))
		dlon = lon2 - lon1
		dlat = lat2 - lat1
		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
		c = 2 * atan2(sqrt(a), sqrt(1 - a))
		tempDist = R * c
		if tempDist < distance or distance == 0:
			distance = tempDist
			poi = poiDict[i]['POIID']
	return (row._ID ,poi, distance) 
	
# Note that POI1 and POI2 are the same, therefore POI2 will be not appear from here on
poiDist = dataSampleNoDupe.rdd.map(distanceToPoi)

df_schema = StructType([
    StructField("_ID", StringType()),
    StructField("POI", StringType()),
    StructField("Distance_km", FloatType())
])

requestToClosestPoi = spark.createDataFrame(poiDist, df_schema) #Q2 df


#3-1, 2
average = requestToClosestPoi.groupBy('POI').avg()  #get average
std = requestToClosestPoi.groupBy('POI').agg(stddev_pop("Distance_km"))  #get stddev of population
radius = requestToClosestPoi.groupBy('POI').max()  #radius = distance to further request
countReq = requestToClosestPoi.groupBy('POI').count()  # count number of request per POI, 100% of requests will fall within the circle area given above radius
poiDetails = average.join(std, ['POI']).join(countReq, ['POI']).join(radius, ['POI'])  #get all metrics in a single df
poiDetails = poiDetails.withColumn('density',  func.round(poiDetails['count'] / (pi * poiDetails['max(Distance_km)']**2), 10))  #calculate density
poiDetails = poiDetails.drop('count')   #clean up
poiDetails = (poiDetails  
       .withColumnRenamed("avg(Distance_km)","avg_dist_km")
	   .withColumnRenamed("stddev_pop(Distance_km)","stddev_dist_km")
       .withColumnRenamed("max(Distance_km)", "radius"))   #clean up
	   

	
	
	
# Q1
dataSampleNoDupe.show(5)
dataSampleNoDupe.count()


# Q2
requestToClosestPoi.show(5)
	
	
#Q3
poiDetails.show()  #note POI1 = POI2, therefore POI2 is not included here
	

	
	
	
	
	