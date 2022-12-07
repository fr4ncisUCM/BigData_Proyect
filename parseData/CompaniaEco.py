from pyspark.sql.functions import col, substring
from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc, split

spark_app = SparkSession.builder.appName('CompaniaEco').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

validC = ['totalFare', 'segmentsAirlineCode']

df = df[validC]

df.printSchema()

df1 = df.withColumn('CodeCompany', split(df['segmentsAirlineCode'], '\\|\\|').getItem(0)).drop('segmentsAirlineCode')

df2 = df1.withColumn("totalFare", col("totalFare").cast('float'))

df3 = df2.groupBy('CodeCompany').avg('totalFare')

df2.printSchema()
df3.sort('CodeCompany').show()