from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, IntegerType, StructType, StructField

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.read.schema(schema).option("sep", "\t").csv("./ml-100k/u.data")

df.groupBy("movie_id").count().orderBy("count", ascending=False).show()

# using SQL
df.createOrReplaceTempView("ratings")
spark.sql("SELECT movie_id, COUNT(movie_id) AS count FROM ratings GROUP BY 1 ORDER BY 2 DESC").show()

spark.stop()

