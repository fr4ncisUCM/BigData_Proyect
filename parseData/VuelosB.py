from pyspark.sql.functions import col, substring
from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc, split

spark_app = SparkSession.builder.appName('precio_Compañia').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

validC = ['startingAirport', 'destinationAirport', 'totalFare', 'segmentsAirlineName']

df = df[validC]

df1 = df.withColumn('CodeCompany', split(df['segmentsAirlineCode'], '\\|\\|').getItem(0).drop('segmentsAirlineCode')

df2 = df1.withColumn("totalFare", col("totalFare").cast('float'))

df3 = df2.groupBy('StartingAirport', 'destinationAirport', 'CodeCompany').avg('totalFare')

df2.printSchema()
df3.sort('startingAirport', 'destinationAirport', 'CodeCompany').show()
