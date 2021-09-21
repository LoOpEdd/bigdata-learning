from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("num_friends", IntegerType(), True),  
])

df = spark.read.schema(schema).csv("./data/fakefriends.csv")

df.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
teenagers.show()

df.groupBy("age").count().orderBy("age").show()

spark.stop()