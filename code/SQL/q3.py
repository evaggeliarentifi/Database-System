from  pyspark.sql import SparkSession
import time,sys

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/ratings.csv")
movie_genres= spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_genres.csv")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

sqlString= "select t.genre as genre_movie,count(*) as num_movies, avg(t.avg_rate) as average_rate from" + \
           "(select " + \
           " mg._c0 as id ,mg._c1 as genre, avg(r._c2) as avg_rate " + \
           "from movie_genres mg join ratings  r " + \
           "on mg._c0=r._c1  group by mg._c0,mg._c1) t " + \
           "group by t.genre "

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time:",t2-t1)

