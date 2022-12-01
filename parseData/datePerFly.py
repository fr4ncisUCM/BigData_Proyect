from pyspark.sql.functions import col, substring
from pyspark.sql.functions import udf
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc

spark_app = SparkSession.builder.appName('DateFly').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['flightDate']

# fit the dataframe
df = df[validC]

# df.withColumn('year', substring('date', 1,4))\

df1 = df.withColumn('month', substring('flightDate', 6, 2))\
    .groupBy('month').count()

df1.show()