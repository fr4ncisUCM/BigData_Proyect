import sys
from pyspark.sql import SparkSession

spark_app = SparkSession.builder.appName('ViajesCount').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# How many trips do we want
# number_of_trips = sys.argv[1]

# Valid columns
validC = ['legId', 'startingAirport', 'destinationAirport']

# fit the dataframe
df = df[validC]


def join_airports(dataframe):
    return dataframe.startingAirport + '-' + dataframe.destinationAirport \
        if dataframe.startingAirport < dataframe.destinationAirport \
        else dataframe.destinationAirport + '-' + dataframe.startingAirport


dFinal = df.groupBy('legId') \
    .withColumns('trayecto', join_airports(df)) \
    .groupBy('legId').groupBy('trayecto').count()

dFinal.show()







