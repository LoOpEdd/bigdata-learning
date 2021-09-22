from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructType, StructField
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("TotalSpentDF").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("total", FloatType(), True),
])

df = spark.read.schema(schema).csv("./data/customer-orders.csv")

# How much in total each customer spent?
df.select("customer_id", "total").groupBy("customer_id").agg(
    f.round(f.sum("total"), 2).alias("total_spent")
).sort("total_spent", ascending=False).show()

# using SQL
df.createOrReplaceTempView('orders')

spark.sql("SELECT customer_id, round(SUM(total), 2) as total_spent FROM orders GROUP BY customer_id ORDER BY 2 DESC").show()

spark.stop()