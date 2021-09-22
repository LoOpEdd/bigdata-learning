from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("FriendsByAgeDF").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("./data/fakefriends-header.csv")

# select only age and friends columns
friendsByAge = people.select("age", "friends")

# group by age and calculate the average of friends
friendsByAge.groupBy("age").avg("friends").show()

# sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# formatted
friendsByAge.groupBy("age").agg(f.round(f.avg("friends"), 2)).sort("age").show()

# formatted with alias
friendsByAge.groupBy("age").agg(f.round(f.avg("friends"), 2).alias("friends_avg")).sort("age").show()

# with SQL
people.createOrReplaceTempView("people")

ages_sql = spark.sql("SELECT age, round(AVG(friends), 2) AS friends_avg FROM people GROUP BY 1 ORDER BY 1")
ages_sql.show()

spark.stop()