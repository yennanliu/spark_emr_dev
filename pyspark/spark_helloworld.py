# spark 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

##### config #####
conf = SparkConf().setAppName("SPARK_HELLO_WORLD")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print ("------------- spark process -------------")
print (sc)
rdd1 = sc.parallelize([1,2,3,4,5])
rdd2 = sc.parallelize(["apple", "car", "park"])
print (rdd1.collect())
print (rdd2.collect())
print ("------------- spark process -------------")
