import sys
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession

spark_app = SparkSession.builder.appName('covid').getOrCreate()

dfFlights = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

dfCovid = spark_app.read.format("csv").option("header", "true").load("USA_Covid_Data.csv")

# Valid columns
validCFights = ['destinationAirport']
validCovid = ['State', 'Active', 'Population']  # try to find active % cases

# Fit the dataframe
dfFlights = dfFlights[validCFights]
dfCovid = dfCovid[validCovid]

# Count the times that each state was visited
dfFlights2 = dfFlights.dropna() \
    .groupBy('destinationAirport').count()

# Delete rows with null values
dfCovid2 = dfCovid.dropna() \
    .withColumns({'Population': dfCovid.Population.cast(IntegerType())}) \
    .withColumns({'Active': dfCovid.Active.cast(DoubleType()) / 10 })

df3 = dfFlights2.join(dfCovid2, dfFlights2.destinationAirport == dfCovid2.State, "inner") \
    .drop('State')
pandas_df = df3.toPandas()
# plotting graph
fig = pandas_df.plot(x="destinationAirport", y=['Active', 'count'], kind="bar").figure
# Save the plot to a file
fig.savefig("compare.png")
