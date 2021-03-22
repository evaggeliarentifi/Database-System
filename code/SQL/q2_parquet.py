
from pyspark.sql import SparkSession
import sys,time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
ratings = spark.read.format("parquet").options(header='true',inferSchema='false').load("hdfs://master:9000/ratings.parquet")
ratings.registerTempTable("ratings")

sqlString= "select(( select count(*) " +\
           " from (select col1 from ratings" + \
            " group by col1 having AVG(col3)>3))*100/(select count(*) from (select distinct col1 from ratings))) as percentage  "
t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time :",t2-t1)
