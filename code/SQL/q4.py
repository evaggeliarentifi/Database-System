from  pyspark.sql import SparkSession
import time,sys

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()



def which_5_year (release_year):
    if(release_year>1999 and release_year<2005):
        return "2000-2004"
    elif (release_year>2004 and release_year<2010):
        return "2005-2009"
    elif(release_year>2009 and release_year<2015):
        return "2010-2014"
    elif (release_year > 2014 and release_year < 2020):
        return "2015-2019"

spark.udf.register("which_5_year", which_5_year)

movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies.csv")
movie_genres= spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movie_genres.csv")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")

sqlString= " select " + \
            " t.percentage as per, " + \
            " avg(t.summary_length)  as average_length from " + \
            " (select " + \
            " which_5_year(year(m._c3)) as percentage ," + \
            " m. _c0 as id, " + \
            " if(length(m._c2)<>0, ( length(m._c2) - length(replace(m._c2, ' ', '')) + 1),0) as summary_length " + \
            " from movies m," + \
            " (select _c0  as id from movie_genres where _c1 like  '%Drama%') n " + \
            " where year(m._c3)>1999  and year(m._c3)<2020 and year(m._c3) is not null and n.id==m._c0 ) t "+\
           " group by t.percentage"

t1=time.time()           
res = spark.sql(sqlString)
res.show()
t2=time.time()


print("time : ",t2-t1)
