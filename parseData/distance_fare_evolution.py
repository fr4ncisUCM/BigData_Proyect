# Distance X fare
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.functions import concat, lit, col, asc
from pyspark.sql.types import IntegerType, DoubleType

spark_app = SparkSession.builder.appName('DistanceXFare').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['startingAirport', 'destinationAirport', 'totalTravelDistance', 'totalFare']

# fit the dataframe
df = df[validC]

df1 = df.dropna()\
    .select("*", concat(substring('startingAirport', 0, 3), lit(" "), substring('destinationAirport', 0, 3)).alias("trip"))\
    .withColumns({'totalFare': df.totalFare.cast(DoubleType())})\
    .withColumns({'totalTravelDistance': df.totalTravelDistance.cast(IntegerType())})\
    .groupBy('trip').max('totalTravelDistance', 'totalFare')\
    .drop('startingAirport')\
    .drop('destinationAirport')\
    .sort("max(totalTravelDistance)")
df1.show()

# Convert to a pandas Dataframe
pandas_df = df1.toPandas()

# Create a line plot
fig = pandas_df.plot().figure

# Save the plot to a file
fig.savefig("evolution.png")

