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
    StructField("workclass", StringType(), nullable),
    StructField("fnlwgt", IntegerType(), nullable),
    StructField("education", StringType(), nullable),
    StructField("education-num", StringType(), nullable),
    StructField("marital-status", StringType(), nullable),
    StructField("occupation", StringType(), nullable),
    StructField("relationship", StringType(), nullable),
    StructField("race", StringType(), nullable),
    StructField("sex", StringType(), nullable),
    StructField("capital-gain", IntegerType(), nullable),
    StructField("capital-loss", StringType(), nullable),
    StructField("hours-per-week", StringType(), nullable),
    StructField("native-country", StringType(), nullable),
    StructField("is-upper-than-50k", StringType(), nullable)
])
# Connect to bdd
sqlContext = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

# Read file
df = sqlContext.read.csv("data/adult.data", header=False, sep=", ", schema=schema)

# Display all columns
#print(df.collect())

# Display columns
print(df.columns)

# df.select("is-upper-than-50k").show()
df.select("*").show()




