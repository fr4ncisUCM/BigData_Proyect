from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc

spark_app = SparkSession.builder.appName('ViajesCount').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# How many trips do we want
# number_of_trips = sys.argv[1]

# Valid columns
validC = ['startingAirport', 'destinationAirport']

# fit the dataframe
df = df[validC]


def join_airports(dataframe):
    a = str(dataframe.destinationAirport)
    b = str(dataframe.startingAirport)
    if a < b:
        return "startingAirport", "destinationAirport"
    else:
        return "destinationAirport", "startingAirport"


df1 = df.select("*", concat(col(join_airports(df)[0]), lit(" "), col(join_airports(df)[1])).alias("trip")) \
    .groupBy("trip").count() \
    .orderBy("count", ascending=False)
df1.show()
