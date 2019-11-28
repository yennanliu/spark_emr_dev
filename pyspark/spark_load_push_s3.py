# spark 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import os 
##### config #####
conf = SparkConf().setAppName("SPARK_HELLO_WORLD")
#sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
# s3 
AWSAccessKeyId = os.environ['AWSAccessKeyId']
AWSSecretKey = os.environ['AWSSecretKey']
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', AWSAccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', AWSSecretKey)

print ("------------- spark process -------------")
filename = "s3a://sample-etl-data/movie_ratings.csv"
data = sc.textFile(filename).map(lambda line: line.split(","))
data.collect()
df = data.toDF()
df.show()
print ("------------- spark process -------------")
