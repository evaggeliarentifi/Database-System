from pyspark.sql import SparkSession
from io import StringIO
import csv,time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("q3").getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("ERROR")

# ratings
ratings = sc.textFile("hdfs://master:9000/ratings.csv") \
        .map(lambda  line : line.split(','))\
        .map(lambda line: [line[1],[line[2],1]]) # ta morfopoiw gia to join
#[movieId, [movieId,rating] ]

# sto prwto map exoume [movieId, [rating,1]]
ratings = ratings\
        .reduceByKey(lambda x,y : [ float(x[0]) + float(y[0]), x[1] + y[1] ] )\
        .map(lambda line : [ int(line[0]) ,  float(line[1][0]) / float(line[1][1])])

movieGenres = sc.textFile("hdfs://master:9000/movie_genres.csv") \
        .map(lambda  line : line.split(','))\
        .map(lambda line: [int(line[0]),line[1]])
# [movieId, genre]

total_join = movieGenres.join(ratings)\
        .map(lambda line : [line[0], line[1][1], line[1][0]])
# [movieId, ratingAverage, genre]

t1 = time.time()

# we reduceBykey with movieId and find the average of ratingAverage
genre_mean = total_join\
        .map(lambda line: [line[2],  [ line[1],1 ]      ])\
        .reduceByKey(lambda x,y :  [ x[0] + y[0], x[1] + y[1] ])\
        .map(lambda line: (line[0], line[1][0]/line[1][1], line[1][1]) )



for j in genre_mean.take(50):
    print(j)


t2 = time.time()
print("**************")
print("Total time: ", t2-t1)

