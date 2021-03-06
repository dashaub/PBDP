from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

conf = SparkConf().setAppName('p3_aggregation').setMaster("local[*]")
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

print 'Unique users in window'
lines = ssc.textFileStream('data_input')
users = lines.map(extract_user).window(30, 30).transform(lambda rdd: rdd.distinct())
users.pprint(100)
users.count().pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()
