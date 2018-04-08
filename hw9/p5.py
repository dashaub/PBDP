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
    return (url, 1)

print 'Count of each URL in window'
lines = ssc.textFileStream('data_input')
distinct_records = lines.transform(lambda rdd: rdd.distinct())
url_count = distinct_records.map(extract_user).window(60, 1).reduceByKey(lambda x, y: x + y)
url_count.pprint(40)

ssc.start()
ssc.awaitTermination()
ssc.stop()
