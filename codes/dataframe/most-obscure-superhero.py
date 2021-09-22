from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("PopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

names = spark.read.schema(schema).option("sep", " ").csv("./data/marvel/marvel_names.txt")
graph = spark.read.text("./data/marvel/marvel_graph.txt")

connections = graph.withColumn("id", f.split(graph.value, " ")[0])\
    .withColumn("connections", f.size(f.split(graph.value, " ")) - 1)\
    .groupBy("id").agg(f.sum("connections").alias("connections")).select("id", "connections")

connections_sorted  = connections.orderBy("connections")
min_connection = connections_sorted.agg({"connections": "min"}).collect()[0][0]

min_connections = connections_sorted.filter(connections_sorted["connections"] == min_connection).join(names, "id").show()

spark.stop()