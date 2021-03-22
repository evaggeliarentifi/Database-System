from pyspark.sql import SparkSession
from datetime import datetime, timezone
import itertools
import itertools
from io import StringIO
import csv, time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_esoda(x):
        return int(x[6])
def get_eksoda(x):
        return int(x[5])
#  1.7114307880401611
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

#rdd1 == movies
rdd_to_be_broadcasted = sc.textFile("hdfs://master:9000/sub_data/ratings.csv") \
        .map(lambda line: split_complex(line))\
        .map(lambda line : ( int(line[0]) ,[ [int(line[0])] + line[1:]] ))\
    .sortByKey()
# opote einai (movieId, [movie] )

#rdd2 == movie_genres
rdd2 = sc.textFile("hdfs://master:9000/sub_data/movie_genres.csv") \
        .map(lambda line : line.split(','))\
        .map(lambda line :  (  int(line[0]) , [line]    )   )\

# opote einai (movieId, genre)

t1 = time.time()

if(rdd_to_be_broadcasted.count() > rdd2.count()):
    temp = rdd_to_be_broadcasted
    rdd_to_be_broadcasted = rdd2
    rdd2 = temp


rdd2 = rdd2.reduceByKey(lambda x,y : x+y)
# to collectAsMap den m dinei duplicates, gia auto kanw reduceByKey prin to collectAsMap
rdd_to_be_broadcasted  = rdd_to_be_broadcasted.reduceByKey(lambda x, y : x + y).collectAsMap()

rdd1_hashable = sc.broadcast(rdd_to_be_broadcasted)

# for i in rdd1_hashable.value:
#     print(i)

print("%%%%%%%%%%%%%%%%%%%%")

# .map(lambda line: [line] + [rdd1_hashable.value[line[0]]] if len(rdd1_hashable.value) > line[0] else [])\

print(" $$$$$$$$$$$$$$$$$$$$$$$ ")

def f (all):
    for it in all:
        for i in it:
            yield i

rdd2 = rdd2\
        .map(lambda line : [line] + [ [] if line[0] not in rdd1_hashable.value else rdd1_hashable.value[line[0]]] )\
        .filter(lambda line:line!=[])\
        .map(lambda line : [   line[0][1]]  + [line[1]]) \
        .map(lambda line: itertools.product(line[0], line[1])) \
        .mapPartitions(f)\
        .map(lambda line: (line[1] , line[0]))
#
for i in rdd2.take(200):
    print(i)
#
# for i in rdd2.take(60):
#     for k in i:
#         print(k)

t2 = time.time()

print("******************")
print("Total time: ", t2-t1)

