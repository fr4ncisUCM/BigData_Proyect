from pyspark.sql.functions import col, substring
import sys
from pyspark.sql import SparkSession

spark_app = SparkSession.builder.appName('empresas').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['flightDate', 'totalFare']

# fit the dataframe
df = df[validC]


df1 = df.withColumn('day', substring('flightDate', 6, 5))\
    .dropna()\
    .groupBy('day').count()\
    .withColumnRenamed('count(flightDate)', 'flightsPerDay')\
    .orderBy("day", ascending=True)\
    .drop('flightDate')

