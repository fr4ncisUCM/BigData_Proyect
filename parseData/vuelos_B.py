import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc, split

spark_app = SparkSession.builder.appName('DateFly').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

validC = ['startingAirport', 'destinationAirport', 'totalFare', 'segmentsAirlineCode']

df = df[validC]

df1 = df.withColumn('CodeCompany', split(df['segmentsAirlineCode'], '\\|\\|').getItem(0)).drop('segmentsAirlineCode')

df2 = df1.withColumn("totalFare", col("totalFare").cast('float'))

df3 = df2.groupBy('StartingAirport', 'destinationAirport', 'CodeCompany').avg('totalFare')

df3.sort('startingAirport', 'destinationAirport', 'CodeCompany').show()
