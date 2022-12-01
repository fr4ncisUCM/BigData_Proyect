from pyspark.sql.functions import udf
import sys
from pyspark.sql.functions import col, substring
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc

spark_app = SparkSession.builder.appName('ViajesCount').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])
dfIni = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])
dfFin = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# How many trips do we want
# number_of_trips = sys.argv[1]

# Valid columns
validC = ['startingAirport', 'destinationAirport']
validCIni = ['startingAirport']
validCFin = ['destinationAirport']
# fit the dataframe
df = df[validC]
dfIni = dfIni[validCIni]
dfFin = dfFin[validCFin]


def join_airports(ini, fin):
    # AC -- KN
    # KN -- AC
    if ini < fin:
        return "startingAirport", "destinationAirport"
    else:
        return "destinationAirport", "startingAirport"


df1 = df.select("*", concat(substring('startingAirport', 0, 3), lit(" "), substring('destinationAirport', 0, 3)).alias("trip")) \
    .groupBy("trip").count() \
    .orderBy("count", ascending=False)
df1.show()


df2 = dfIni.groupBy('startingAirport').count()\
        .orderBy("count", ascending=False)
df2.show()

df3 = dfFin.groupBy('destinationAirport').count()\
        .orderBy("count", ascending=False)
df3.show()