# Show the average fare of each company
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc, split
from pyspark.sql.types import IntegerType, DoubleType

spark_app = SparkSession.builder.appName('CompanyEmpty').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

validC = ['seatsRemaining', 'segmentsAirlineCode']

df = df[validC]

print(df.describe('seatsRemaining'))

df1 = df.withColumn('CodeCompany', split(df['segmentsAirlineCode'], '\\|\\|').getItem(0)).drop('segmentsAirlineCode') \
    .withColumns({'seatsRemaining': df.seatsRemaining.cast(DoubleType())})\
    .groupBy('CodeCompany').avg('seatsRemaining') \
    .orderBy('avg(seatsRemaining)') \
    .drop('seatsRemaining')

pandas_df = df1.toPandas()
# plotting graph
fig = pandas_df.plot(x="CodeCompany", y=['avg(seatsRemaining)'], kind="bar").figure
# Save the plot to a file
fig.savefig("compareSeats.png")
