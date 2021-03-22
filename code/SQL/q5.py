from pyspark.sql import SparkSession
import sys,time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/ratings.csv")
movie_genres= spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_genres.csv")
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies.csv")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")

sqlString =         " select  q5.category as category,first(q5.user) as user,first(q5.count) as count,first(q5.title) as title,first(q5.rate) as rate from " + \
                    "(select  q4.category as category,q4.user as user,q4.count as count,mv._c1 as title,q4.rate as rate ,mv._c7 as popularity from " + \
                          "(select q2.category as category,q2.user as user,q2.rates_count as count,q3.movie_id as movie_id,q3.rate as rate   from  " + \
                               "( select  r._c0 as  user , " + \
                               " mg._c1 as category ," + \
                               " count(*) as rates_count" + \
                               " from movie_genres mg , ratings r " + \
                               " where mg._c0==r._c1 group by r._c0 , mg._c1) q2" + \
                               " inner join" + \
                               " ( select  r._c0 as  user , " + \
                               " mg._c1 as category ," + \
                               " r._c1 as movie_id ,"+ \
                               " r._c2 as rate " + \
                               " from  ratings r , movie_genres mg where  " + \
                               "  mg._c0==r._c1 ) q3" + \
                               " on q2.user=q3.user and q2.category=q3.category order by q2.category,q3.rate desc) q4 "+ \
                         " inner  join movies mv on mv._c0=q4.movie_id " + \
                          " order by q4.category,q4.count desc , q4.rate desc,mv._c7 desc) q5 " + \
                         " group by  q5.category order by q5.category"

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time : " , t2-t1)
