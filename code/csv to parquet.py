from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from io import StringIO
import csv

def split_complex(x):
 return list(csv.reader(StringIO(x), delimiter=','))[0]



if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
        StructField("col4", StringType(), True)])

    rdd = sc.textFile("hdfs://master:9000/ratings.csv").map(lambda line: line.split(','))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('./ratings.parquet')