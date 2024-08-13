from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("fakefriends-header.csv")

age_friends = people.select("age", "friends")

age_friends.groupBy("age").avg("friends").sort("age").show()

spark.stop()



