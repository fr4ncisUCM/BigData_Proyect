# Show the average fare of each company
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc, split

spark_app = SparkSession.builder.appName('CompaniaEco').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])
df2 = df

validC = ['totalFare', 'segmentsAirlineCode']
validC2 = ['segmentsAirlineCode']

df = df[validC]
df2 = df2[validC2]

df1 = df.withColumn('CodeCompany', split(df['segmentsAirlineCode'], '\\|\\|').getItem(0)).drop('segmentsAirlineCode') \
    .withColumn("totalFare", col("totalFare").cast('float')) \
    .groupBy('CodeCompany').avg('totalFare')

df3 = df2.withColumn('CodeCompany1', split(df2['segmentsAirlineCode'], '\\|\\|').getItem(0)).drop('segmentsAirlineCode') \
    .groupBy('CodeCompany1').count() \
    .withColumn("count", col("count") / 150)

df1.show()
df3.show()
# Join both

df4 = df1.join(df3, df1.CodeCompany == df3.CodeCompany1, "inner") \
    .drop('CodeCompany1') \
    .filter("count > 30")

# df4.show()
pandas_df = df4.toPandas()
# plotting graph
fig = pandas_df.plot(x='CodeCompany', y=['avg(totalFare)', 'count'], kind="bar").figure
# Save the plot to a file
fig.savefig("compareSoft.png")
