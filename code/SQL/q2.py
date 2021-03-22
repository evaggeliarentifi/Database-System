from  pyspark.sql import SparkSession
import sys, time


spark = SparkSession.builder.appName("query-q1").getOrCreate()

ratings = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/ratings.csv")
ratings.registerTempTable("ratings")


sqlString= "select(( select count(*) " +\
           " from (select _c0 from ratings" + \
            " group by _c0 having AVG(_c2)>3))*100/(select count(*) from (select distinct _c0 from ratings))) as percentage  "

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time: " ,t2-t1)

