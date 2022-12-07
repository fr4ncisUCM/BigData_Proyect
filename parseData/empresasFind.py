from matplotlib import pyplot as plt
import sys
from pyspark.sql import SparkSession

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
for valor in my_list:
    complete_dict(valor)
# Write a file with the dict values
with open("paso2.txt", "w") as file:
    for key, value in airlanesNames.items():
        file.write("{}: {}\n".format(key, value))

# Create a list of the labels for the pie chart
labels = list(airlanesNames.keys())

# Create a list of the values for the pie chart
values = list(airlanesNames.values())

# Create the pie chart
plt.pie(values, labels=labels)
plt.savefig('businessPie.png')
