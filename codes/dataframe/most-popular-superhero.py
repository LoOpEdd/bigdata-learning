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

connections = graph.withColumn("id", f.split(f.col("value"), " ")[0])\
    .withColumn("connections", f.size(f.split(f.col("value"), " ")) - 1)\
    .groupBy("id").agg(f.sum("connections").alias("connections"))

# getting the df joined
# most_popular = connections.sort("connections", ascending=False)
# most_popular_names = most_popular.join(names, on="id", how="left").sort("connections", ascending=False)
# most_popular_names.show()

# getting the first
most_popular = connections.sort("connections", ascending=False).first()
most_popular_name = names.filter(most_popular[0] == names["id"]).select("name").first()
print(most_popular_name[0] + " is the most popular superhero with " + str(most_popular[1]) + " co-apperances")
