from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import approx_count_distinct
import os

conf = SparkConf().setAppName('p3_hll').setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 1)


def extract_user(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the user.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    return user

print 'Apprxoimate unique users in window'
lines = ssc.textFileStream('data_input')
users = lines.map(extract_user).window(5, 5)
approx_unique = users.foreachRDD(lambda rdd: rdd.countApproxDistinct(relativeSD=0.01))
approx_unique.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()
