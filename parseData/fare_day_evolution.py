from pyspark.sql.functions import col, substring
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType


spark_app = SparkSession.builder.appName('empresas').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['flightDate', 'totalFare']

# fit the dataframe
df = df[validC]


df1 = df.withColumn('day', substring('flightDate', 6, 5))\
    .withColumns({'totalFare': df.totalFare.cast(DoubleType())})\
    .groupBy('day').avg('totalFare')\
    .withColumnRenamed('avg(totalFare)', 'totalFarePerDay')\
    .orderBy("day", ascending=True)\
    .drop('flightDate')

# Convert to a pandas Dataframe
pandas_df = df1.toPandas()

# Create a line plot
fig = pandas_df.plot().figure

# Save the plot to a file
fig.savefig("lineplot_fare.png")

