from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType



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
#print(df.collect())

# Display columns
print(df.columns)

# df.select("is-upper-than-50k").show()
df.select("*").show()








