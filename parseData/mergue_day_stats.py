from pyspark.sql.functions import col, substring
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType


spark_app = SparkSession.builder.appName('mergePlots').getOrCreate()

dfFare = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])
dfCount = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validc_fare = ['flightDate', 'totalFare']
validc_count = ['flightDate']

# fit the dataframe
dfFare = dfFare[validc_fare]
dfCount = dfCount[validc_count]

# First dataframe
df1 = dfCount.withColumn('day', substring('flightDate', 6, 5))\
    .groupBy('day').count() \
    .withColumns({'countSoft': col('count')/50})\
    .drop('count')

# Second dataframe
df2 = dfFare.withColumn('day', substring('flightDate', 6, 5))\
    .withColumns({'totalFare': dfFare.totalFare.cast(DoubleType())})\
    .groupBy('day').avg('totalFare')\
    .withColumnRenamed('avg(totalFare)', 'totalFarePerDay')\
    .orderBy("day", ascending=True)\
    .drop('flightDate')

# Join both
df3 = df1.join(df2, df1.day == df2.day, "inner")

df3.show()

# Convert to a pandas Dataframe
pandas_df = df3.toPandas()
# Create a line plot
fig = pandas_df.plot().figure

# Save the plot to a file
fig.savefig("lineplot_merge.png")


