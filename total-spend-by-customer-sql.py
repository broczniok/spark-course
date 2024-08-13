from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ \
    StructField("customerID", StringType(), True), \
    StructField("unknown", IntegerType(), True), \
    StructField("cost", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

customer_cost = df.select("customerID", "cost")
grouped_customer_cost = customer_cost.groupBy("customerID").sum("cost")

grouped_customer_cost_sorted = grouped_customer_cost.withColumn("cost", func.col("sum(cost)")).select("customerID", "cost").sort("cost")

results = grouped_customer_cost_sorted.collect()

for result in results:
    print(result)

spark.stop()