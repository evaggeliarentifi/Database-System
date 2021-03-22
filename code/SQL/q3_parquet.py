from  pyspark.sql import SparkSession
import sys,time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

ratings = spark.read.format("parquet").options(header='true', inferSchema='false').load("hdfs://master:9000/ratings.parquet")
movie_genres= spark.read.format("parquet").options(header='true', inferSchema='false').load("hdfs://master:9000/movie_genres.parquet")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

sqlString= "select t.genre as genre_movie,count(*) as num_movies, avg(t.avg_rate) as average_rate from" + \
           "(select " + \
           " mg.col1 as id ,mg.col2 as genre, avg(r.col3) as avg_rate " + \
           "from movie_genres mg join ratings  r " + \
           "on mg.col1=r.col2  group by mg.col1,mg.col2) t " + \
           "group by t.genre  order by t.genre"

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time : ",t2-t1)

