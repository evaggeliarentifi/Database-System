from pyspark.sql import SparkSession
import time,sys

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

ratings = spark.read.format("parquet").options(header='true', inferSchema='false').load("hdfs://master:9000/ratings.parquet")
movie_genres= spark.read.format("parquet").options(header='true', inferSchema='false').load("hdfs://master:9000/movie_genres.parquet")
movies = spark.read.format("parquet").options(header='true', inferSchema='false').load("hdfs://master:9000/movies.parquet")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")

sqlString =         " select  q5.category as category,first(q5.user) as user,first(q5.count) as count,first(q5.title) as title,first(q5.rate) as rate from " + \
                    "(select  q4.category as category,q4.user as user,q4.count as count,mv.col2 as title,q4.rate as rate ,mv.col8 as popularity from " + \
                          "(select q2.category as category,q2.user as user,q2.rates_count as count,q3.movie_id as movie_id,q3.rate as rate   from  " + \
                               "( select  r.col1 as  user , " + \
                               " mg.col2 as category ," + \
                               " count(*) as rates_count" + \
                               " from movie_genres mg , ratings r " + \
                               " where mg.col1==r.col2 group by r.col1 , mg.col2) q2" + \
                               " inner  join" + \
                               " ( select  r.col1 as  user , " + \
                               " mg.col2 as category ," + \
                               " r.col2 as movie_id ,"+ \
                               " r.col3 as rate " + \
                               " from  ratings r , movie_genres mg where  " + \
                               "  mg.col1==r.col2 ) q3" + \
                               " on q2.user=q3.user and q2.category=q3.category order by q2.category,q3.rate desc) q4 "+ \
                         " inner join movies mv on mv.col1=q4.movie_id " + \
                          " order by q4.category,q4.count desc , q4.rate desc,mv.col8 desc) q5 " + \
                         " group by  q5.category order by q5.category"

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time : " , t2 - t1)
