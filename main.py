import pandas as pd
from pyspark import SQLContext
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType
import data.process_initial_file as pif

#pif.list_workclass

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Define information
nullable = True
schema = StructType([
    StructField("age", IntegerType(), nullable),
    StructField("workclass", IntegerType(), nullable),
    StructField("fnlwgt", IntegerType(), nullable),
    StructField("education", IntegerType(), nullable),
    StructField("marital-status", IntegerType(), nullable),
    StructField("occupation", IntegerType(), nullable),
    StructField("relationship", IntegerType(), nullable),
    StructField("race", IntegerType(), nullable),
    StructField("sex", IntegerType(), nullable),
    StructField("capital-gain", IntegerType(), nullable),
    StructField("capital-loss", IntegerType(), nullable),
    StructField("hours-per-week", IntegerType(), nullable),
    StructField("native-country", IntegerType(), nullable),
    StructField("is-upper-than-50k", IntegerType(), nullable)
])
# Connect to bdd
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

# Read file
df = sqlContext.read.csv("data/adult_processed_data.data", header=True, sep=",", schema=schema)

# Display all columns
# print(df.collect())

# Display columns
print(df.columns)

# df.select("is-upper-than-50k").show()
df.select("*").show()

# Create features column, assembling together the numeric data
col1_name = 'education'
col2_name = 'sex'
col3_name = 'is-upper-than-50k'
inputCols = [col1_name, col2_name, col3_name]
#inputCols = ['education', 'marital-status', 'sex', 'is-upper-than-50k']
vecAssembler = VectorAssembler(
    inputCols=inputCols,
    outputCol="features")
adults_with_features = vecAssembler.transform(df)

# Do K-means
k = 3
kmeans_algo = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans_algo.fit(adults_with_features)
centers = model.clusterCenters()

# Assign clusters to flowers
# Cluster prediction, named prediction and used after for color
adults_with_clusters = model.transform(adults_with_features)

# Display Centers
print("Centers: '{}'".format(centers))

# Convert Spark Data Frame to Pandas Data Frame
adults_for_viz = adults_with_clusters.toPandas()
print("STARTING PRINTING ADULTS_for")
print(adults_for_viz)

# Vizualize
# Marker styles are calculated from Iris species
# Marker styles are calculated from Iris species
# print("END PRINTING ADULTS_FOR_VIZ")
# setosaI = adults_for_viz['species'] == 'Iris-setosa'
# setosa = adults_for_viz[setosaI]
# versicolorI = adults_for_viz['species'] == 'Iris-versicolor'
# versicolor = adults_for_viz[versicolorI]
# virginicaI = adults_for_viz['species'] == 'Iris-virginica'
# virginica = adults_for_viz[virginicaI]

# Colors code k-means results, cluster numbers
colors = {0: 'red', 1: 'green', 2: 'blue'}

fig = plt.figure().gca(projection='3d')
fig.scatter(adults_for_viz[col1_name],
            adults_for_viz[col2_name],
            adults_for_viz[col3_name],
            c=adults_for_viz.prediction.map(colors),
            marker='s')

plt.show()
