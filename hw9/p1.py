from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

conf = SparkConf().setAppName('p1')#.setMaster("local[*]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)


def extract_url(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the URL.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    #hour = timestamp[0:14]
    return (url, 1)



lines = ssc.textFileStream('input_data')

sc.setLogLevel('ERROR')


url_count = lines.map(extract_url).reduceByKey(lambda x, y: x + y)
url_count.pprint()



ssc.start()
ssc.awaitTermination()
ssc.stop()
