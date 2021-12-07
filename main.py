import matplotlib.pyplot as plt
import numpy as np
import pylab as pl
import pandas as pd
from pyspark import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

from data.process_initial_file import dict_education, list_education, list_race


def elbow_method_evaluation(df):
    # Calculate cost and plot
    cost = np.zeros(10)

    for k in range(2, 10):
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df)
        cost[k] = model.summary.trainingCost

    # Plot the cost
    df_cost = pd.DataFrame(cost[2:])
    df_cost.columns = ["cost"]
    new_col = [2, 3, 4, 5, 6, 7, 8, 9]
    df_cost.insert(0, 'cluster', new_col)

    pl.plot(df_cost.cluster, df_cost.cost)
    pl.xlabel('Number of Clusters')
    pl.ylabel('Score')
    pl.title('Elbow Curve')
    pl.show()


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
col2_name = 'capital-gain'
col3_name = 'race'
col4_name = 'hours-per-week'
inputCols = [col1_name, col2_name, col3_name]
vecAssembler = VectorAssembler(
    inputCols=inputCols,
    outputCol="features")
adults_with_features = vecAssembler.transform(df)

# Figure 1
# Do K-means
# Evaluate number of clusters with the elbow method
elbow_method_evaluation(adults_with_features)

k = 3
kmeans_algo = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans_algo.fit(adults_with_features)
centers = model.clusterCenters()

# Assign clusters to adults
# Cluster prediction, named prediction and used after for color
adults_with_clusters = model.transform(adults_with_features)

# Display Centers
print("Centers: '{}'".format(centers))

# Convert Spark Data Frame to Pandas Data Frame
adults_for_viz = adults_with_clusters.toPandas()
print("STARTING PRINTING ADULTS_for")
print("adults_for_viz.prediction.value_counts(): '{}'".format(adults_for_viz.prediction.value_counts()))

# Vizualize
A = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 0]
B = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 1]

# Colors code k-means results, cluster numbers
colors = {0: 'red', 1: 'blue', 2: 'orange'}

# Draw dots
fig = plt.figure().add_subplot()
fig.scatter(A[col1_name],
            A[col2_name],
            c=A.prediction.map(colors),
            marker='.')
fig.scatter(B[col1_name],
            B[col2_name],
            c=B.prediction.map(colors),
            marker='x')

# Draw grid
plt.grid()

# Set text
plt.title("Combined Statistics 1")
plt.xlabel(col1_name)
plt.ylabel(col2_name)
# TODO To change in case col1_name is changed
plt.xticks(range(0, len(list_education)), list_education, rotation='vertical')
plt.legend(['is-upper-than-50k: False', 'is-upper-than-50k: True'])

# Save figure
plt.savefig("picture1.png", bbox_inches='tight')

# Show fig
plt.show()

# Figure 2
# Draw dots
fig = plt.figure().add_subplot()
fig.scatter(A[col1_name],
            A[col2_name],
            c=A.prediction.map(colors),
            marker='.')
fig.scatter(B[col1_name],
            B[col2_name],
            c=B.prediction.map(colors),
            marker='x')
# fig.set_yscale('log', base=2)

# Draw grid
plt.grid()

# Set text
plt.title("Combined Statistics 2")
plt.xlabel(col1_name)
plt.ylabel(col2_name)
plt.xticks(range(0, len(list_education)), list_education, rotation='vertical')
plt.legend(['is-upper-than-50k: False', 'is-upper-than-50k: True'])

# Save figure
plt.savefig("picture2.png", bbox_inches='tight')

# Show fig
plt.show()

# Figure 3
inputCols = [col2_name, col3_name]
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
print("adults_for_viz.prediction.value_counts(): '{}'".format(adults_for_viz.prediction.value_counts()))

# Vizualize
A = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 0]
B = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 1]

# Colors code k-means results, cluster numbers
colors = {0: 'red', 1: 'blue', 2: 'orange'}

# Draw dots
fig = plt.figure().add_subplot()
fig.scatter(A[col3_name],
            A[col2_name],
            c=A.prediction.map(colors),
            marker='.')
fig.scatter(B[col3_name],
            B[col2_name],
            c=B.prediction.map(colors),
            marker='x')
# fig.set_yscale('log', base=2)

# Draw grid
plt.grid()

# Set text
plt.title("Combined Statistics 3")
plt.xlabel(col3_name)
plt.ylabel(col2_name)
plt.xticks(range(0, len(list_race)), list_race, rotation='vertical')
plt.legend(['is-upper-than-50k: False', 'is-upper-than-50k: True'])

# Save figure
plt.savefig("picture3.png", bbox_inches='tight')

# Show fig
plt.show()

# TODO PUT HERE

# Figure 4
inputCols = [col1_name, col3_name, col4_name]
vecAssembler = VectorAssembler(
    inputCols=inputCols,
    outputCol="features")
adults_with_features = vecAssembler.transform(df)

elbow_method_evaluation(adults_with_features)

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
print("adults_for_viz.prediction.value_counts(): '{}'".format(adults_for_viz.prediction.value_counts()))

# Vizualize
A = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 0]
B = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 1]

# Colors code k-means results, cluster numbers
colors = {0: 'red', 1: 'blue', 2: 'orange'}

# Draw dots
fig_3d = plt.figure()
ax = plt.axes(projection='3d')

ax.set_xlabel(col1_name)
ax.set_ylabel(col3_name)
ax.set_zlabel(col4_name)
ax.set_xticks(range(0, len(list_education)))
ax.set_xticklabels(list_education, rotation=90,
                   verticalalignment='baseline',
                   horizontalalignment='left')
ax.set_yticks(range(0, len(list_race)))
ax.set_yticklabels(list_race, rotation=-15,
                   verticalalignment='baseline',
                   horizontalalignment='left')
# Data for three-dimensional scattered points
ax.scatter3D(A[col1_name], A[col3_name], A[col4_name], c=A.prediction.map(colors), cmap='Greens', marker='.')
ax.scatter3D(B[col1_name], B[col3_name], B[col4_name], c=B.prediction.map(colors), cmap='Greens', marker='x')

# Save figure
plt.savefig("picture4.png", bbox_inches='tight')

plt.show()

# DEBUG: Display stats
print("k: '{}'".format(k))
print("A.prediction.value_counts(): '{}'".format(A.prediction.value_counts()))
print("B.prediction.value_counts(): '{}'".format(B.prediction.value_counts()))
