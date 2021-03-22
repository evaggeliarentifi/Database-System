from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/movies.csv")

movies.registerTempTable("movies")

sqlString =  " select movies._c1 as title,t.yyear as _year,t.Profit as profit " +\
              "from (select  year(m._c3) as yyear, "  + \
              "max((m._c6 - m._c5)*100/m._c5) as Profit " + \
              "from movies as m  " + \
              "where m._c3 IS NOT NULL and m._c5 <>0 and m._c6<>0" + \
              " group by YEAR(m._c3)     " + \
              " having YEAR(m._c3)>1999) t, movies " + \
              "where YEAR(movies._c3)=t.yyear and t.Profit=( (movies._c6 - movies._c5)*100/movies._c5)"  
t1 = time.time()
res = spark.sql(sqlString)
res.show()
t2 = time.time()

print("time:",t2-t1)
