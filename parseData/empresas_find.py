from matplotlib import pyplot as plt
import sys
from pyspark.sql import SparkSession

spark_app = SparkSession.builder.appName('empresas').getOrCreate()

df = spark_app.read.format("csv").option("header", "true").load(sys.argv[1])

# Valid columns
validC = ['segmentsAirlineCode']

# fit the dataframe
df = df[validC]

# df.withColumn('year', substring('date', 1,4))\
airlanesNames = {}


def complete_dict(names):
    list_names = names.split('||')
    my_set = set()
    for name2 in list_names:
        my_set.add(name2)
    for name in my_set:
        if name not in airlanesNames:
            airlanesNames[name] = 0
        airlanesNames[name] += 1


my_list = df.select("segmentsAirlineCode").rdd.flatMap(lambda x: x).collect()

# Complete the dictionary
for valor in my_list:
    complete_dict(valor)

# Create a list of the labels for the pie chart
labels = list(airlanesNames.keys())

# Create a list of the values for the pie chart
values = list(airlanesNames.values())

# Create the pie chart
plt.pie(values, labels=labels)
plt.savefig('businessPie.png')
