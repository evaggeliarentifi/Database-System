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

spark = SparkSession.builder.appName("q2").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

t1= time.time()
# se oles tis touples rating kanoume reduceByKey me key to userId kai athrizoume ola ta ratings tou
# alla kai tous assous sto reduceByKey
# sto telos filtraroume wste to median_rating > 3.0 kai metrame posoi xristes einai
numberOfUsersWithMedianRatingGreaterThanThree = sc.textFile("hdfs://master:9000/ratings.csv") \
        .map(lambda  line : line.split(','))\
        .map(lambda rating: (rating[0], [rating[2] ,1] ))\
        .reduceByKey(lambda x,y: [float(x[0]) + float(y[0]),x[1]+1]  )\
        .map(lambda x : [ float( x[1][0]) / float( x[1][1]), x[0]  ])\
        .filter(lambda x : x[0] > 3.0 )\
        .sortByKey()\
        .count()

# telos briskoyme ton sinoliko arithmo ton distinct users gia ton upologisto sto telos
numberOfAllUsers = sc.textFile("hdfs://master:9000/ratings.csv") \
        .map(lambda  line : line.split(','))\
        .map(lambda rating: (rating[0])   )\
        .distinct()\
        .count()




print("Number Greater than 3: ", numberOfUsersWithMedianRatingGreaterThanThree)
print("Number of all Users: ", numberOfAllUsers)

# kai to pososto ::
print("RESULT ::::::  ", int(numberOfUsersWithMedianRatingGreaterThanThree) /   int(numberOfAllUsers))

t2 = time.time()
print("**************")
print("Total time: ", t2-t1)
