import matplotlib.pyplot as plt
from pyspark import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

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
inputCols = [col1_name, col2_name]
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
A = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 0]
B = adults_for_viz[adults_for_viz["is-upper-than-50k"] == 1]

# Colors code k-means results, cluster numbers
colors = {0: 'red', 1: 'blue', 2: 'orange'}

# Draw dots
fig = plt.figure().gca()
fig.scatter(A[col1_name],
            A[col2_name],
            c=A.prediction.map(colors),
            marker='.')
fig.scatter(B[col1_name],
            B[col2_name],
            c=B.prediction.map(colors),
            marker='x')
fig.set_yscale('log', base=2)

# Draw grid
plt.grid()

# Set text
plt.title("Combined Statistics")
plt.xlabel("Education")
plt.ylabel("Capital-gain")
plt.legend(['is-upper-than-50k: False', 'is-upper-than-50k: True'])

# Show fig
plt.show()

# DEBUG: Display stats
print("k: '{}'".format(k))
print("A.prediction.value_counts(): '{}'".format(A.prediction.value_counts()))
print("B.prediction.value_counts(): '{}'".format(B.prediction.value_counts()))
