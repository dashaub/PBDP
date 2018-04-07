from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

conf = SparkConf().setAppName('p5').setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 1)


def extract_user(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the user.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    return url

print 'Count of each URL in window'
lines = ssc.textFileStream('data_input')
url_count = lines.map(extract_user).window(5, 5).transform(lambda rdd: rdd.distinct()).reduceByKey(lambda x, y: x + y)
url_count.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()
