from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark_app = SparkSession.builder.appName('ViajesCount').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['startingAirport', 'destinationAirport', 'elapsedDays']

# fit the dataframe
df = df[validC]
df = df.withColumn('elapsedDays', col('elapsedDays').cast("float"))\
       .groupBy('startingAirport', 'destinationAirport').avg('elapsedDays')\
       .orderBy('startingAirport', 'destinationAirport')
df.show()
