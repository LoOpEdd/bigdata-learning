from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, IntegerType, StructType, StructField
from pyspark.sql import functions as f
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("./ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema_ratings = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

schema_movies = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("link", StringType(), True)
])

movies_df = spark.read.schema(schema_ratings).option("sep", "\t").csv("./ml-100k/u.data")
movies_names_df = spark.read.schema(schema_movies).option("sep", "|").csv("./ml-100k/u.item")

movie_counts = movies_df.groupBy("movie_id").count()

def lookupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = f.udf(lookupName)

movies_with_names = movie_counts.withColumn("movie_title", lookupNameUDF(f.col("movie_id")))

movies_with_names.orderBy("count", ascending=False).show()

# using SQL
movies_df.createOrReplaceTempView("ratings")
movies_names_df.createOrReplaceTempView("movies_names")
spark.sql("""SELECT a.movie_id, b.name, COUNT(a.movie_id) AS count 
                FROM ratings a 
                JOIN movies_names b 
                    ON a.movie_id = b.movie_id 
            GROUP BY 1, 2 
            ORDER BY 3 DESC"""
         ).show()

spark.stop()

