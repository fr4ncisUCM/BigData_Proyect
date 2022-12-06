from pyspark.sql.functions import col, substring
from pyspark.sql.functions import udf
import sys
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, asc
from pyspark.sql.types import IntegerType, DoubleType

spark_app = SparkSession.builder.appName('empresas').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['segmentsDepartureAirportCode']

# fit the dataframe
df = df[validC]

# df.withColumn('year', substring('date', 1,4))\
airlanesNames = {}


def complete_dict(names):
    list_names = names.split('||')
    for name in list_names:
        if name not in airlanesNames:
            airlanesNames[name] = 0
        airlanesNames[name] += 1


my_list = df.select("segmentsDepartureAirportCode").rdd.flatMap(lambda x: x).collect()


# Complete the dictionary
with open("paso1.txt", "w") as file:
        for valor in my_list:
                complete_dict(valor)
space = " "
# Write a file with the dict values
with open("paso2.txt", "w") as file:
	for key, value in airlanesNames.items():
    		file.write("{}: {}\n".format(key, value))
