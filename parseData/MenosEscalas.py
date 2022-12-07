from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark_app = SparkSession.builder.appName('ViajesCount').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['startingAirport', 'destinationAirport', 'isNonStop']

# fit the dataframe
df = df[validC]
df = df.withColumn('isNonStop',col('isNonStop').cast('boolean'))\
       .filter(df.isNonStop == False)\
       .groupBy('startingAirport', 'destinationAirport', 'isnonStop').count()\
       .orderBy('startingAirport', 'destinationAirport')
df.show()
