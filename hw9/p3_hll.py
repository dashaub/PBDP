from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import approx_count_distinct
import os

conf = SparkConf().setAppName('p3_hll').setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 5)


def extract_user(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the user.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    return user

print 'Apprxoimate unique users in window'
lines = ssc.textFileStream('data_input')
users = lines.map(extract_user)
# This returns an int that needs to be transformed into something Spark can print each window
approx_unique = users.foreachRDD(lambda rdd: rdd.countApproxDistinct(relativeSD=0.01))
# We now attempt to take this int and place it into a RDD and then dstream so we can print the results
#approx_rdd = ssc.queueStream([sc.parallelize([approx_unique])])
#approx_rdd.pprint()
approx_unique.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()
