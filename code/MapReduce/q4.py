from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]




dates = [[2000,2004],[2005,2009], [2010,2014], [2015,2019] ]
# dates = [  [2010,2014], [2015,2019] ]
t1 = time.time()
for i in dates:
        spark = SparkSession.builder.appName("q4").getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        date_start =i[0]
        date_end = i[1]
        # Diavazoume ta rati
        genres = sc.textFile("hdfs://master:9000/movie_genres.csv") \
                .map(lambda  line : line.split(','))\
                .filter(lambda line : line[1] == 'Drama')\
                .map(lambda line : (line[0], line[1] ) )


        movies = sc.textFile("hdfs://master:9000/movies.csv") \
                .map(lambda  line : split_complex(line) )\
                .filter(lambda line : line[3] != ""   and    int( line[3].split("-")[0]) >= date_start and int( line[3].split("-")[0]) <= (date_end)  )\
                .map(lambda line : (line[0], line[0:]))


        only_drama_movies_with_in_date = genres.join(movies)\
                .map(lambda line : line[1][1])\
                .map(lambda line : (1 ,   [ len(line[2].split(" ")) ,1  ]   ))\
                .reduceByKey(lambda x,y : (x[0] + y[0], x[1]+y[1]) )

        final_data = only_drama_movies_with_in_date.take(1)
        # print(final_data)
        # print(" *********** ")
        mean  = final_data[0][1][0] / final_data[0][1][1]
        print("From ", str(date_start), " until ", str(date_end), " mean is ", mean)

t2 = time.time()
print("**************")
print("Total time: ", t2-t1)

