from pyspark.sql import SparkSession
import sys,time

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
movies = spark.read.format("parquet").options(header='true',inferSchema='false').load("hdfs://master:9000/movies.parquet")
movies.registerTempTable("movies")

sqlString = "select m.col2 as title,t.yyear as year,t.Profit as profit " +\
                "from (select  year(col4) as yyear, "  + \
                "max((col7- col6)*100/col6) as Profit " + \
                "from movies   " + \
                "where col4 IS NOT NULL and col6<>0 and col7<>0" + \
                " group by YEAR(col4)     " + \
                " having YEAR(col4)>1999) t, movies m " + \
            "where YEAR(m.col4)=t.yyear and t.Profit=( (m.col7 - m.col6)*100/m.col6) order by t.yyear"

t1=time.time()
res = spark.sql(sqlString)
res.show()
t2=time.time()

print("time :",t2-t1)
