from pyspark.sql import SparkSession
from datetime import datetime, timezone
from io import StringIO
import csv,time

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_esoda(x):
        return int(x[6])
def get_eksoda(x):
        return int(x[5])

spark = SparkSession.builder.appName("q1").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# kanoume filter kai meta reduceByKey (me key to year) kai kratame tin tainia me to
# megalitero esodo
movies = sc.textFile("hdfs://master:9000/movies.csv") \
        .map(lambda  line : split_complex(line))\
        .filter(lambda line : line[5] != "0" and line[6] != "0" and line[3] != "")\
        .map(lambda line : line +  [int( line[3].split("-")[0] )])\
        .map(lambda line : line +  [((get_esoda(line) - get_eksoda(line))  * 100 )/(get_eksoda(line))])\
        .filter( lambda line:  line[8]>= 2000)\
        .map(lambda line : ( line[8],line)   )\
        .reduceByKey(lambda x,y: x if x[9]>y[9] else y )\
        .map(lambda x: (x[1][8], x))\
        .sortByKey(False)


t1 = time.time()

for i in movies.collect():
        print("Year: ", int(i[0]), "---->", i[1][1])

t2 = time.time()

print("******************")
print("Total time: ", t2-t1)