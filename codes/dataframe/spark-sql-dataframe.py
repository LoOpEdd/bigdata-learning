from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("./data/fakefriends-header.csv")

print("Inferred schema:")
people.printSchema()

print("Name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age:")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()