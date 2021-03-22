from pyspark.sql import SparkSession
from datetime import datetime, timezone
import itertools
from io import StringIO
import csv,time


def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 4.9854841232299805
movies = sc.textFile("hdfs://master:9000/sub_data/ratings.csv") \
        .map(lambda line: split_complex(line))\
        .map(lambda line : ( int(line[0]) , [ [] , [line]] ))
# opote einai (movieId, [ [] , [movie]  ] )


movies_genres = sc.textFile("hdfs://master:9000/sub_data/movie_genres.csv") \
        .map(lambda line : line.split(','))\
        .map(lambda line :  (  int(line[0]) , [ [line[1]] , [] ]  )   )
# opote einai (movieId, [ [genre] , []  ]

# ara menei na kanw union ta 2 panw kai reduceByKey kai na prosthetw ta arrays mesa
# tous wste na exw  teliko  (movieId, [genre1,genre2,...] , [movie1,movie2,...]
# sto telos kanw to ginomeno twn duo panw wste na exw tis telikes touples

t1 = time.time()
def f (all):
    for it in all:
        for i in it:
            yield i

union = movies.union(movies_genres).sortByKey(False)

union = union\
        .reduceByKey(lambda before,after:  [before[0] + after[0] , before[1] + after[1]  ]) \
        .map(lambda line: itertools.product(*(line[1]))) \
        .mapPartitions(f)
# Opote prepei an kanw to cartesiano ginomeno

for i in union.take(30):
    print(i)

# for i in union.take(30):
#     print(i)

t2 = time.time()


print("Total time: " , t2 - t1)
