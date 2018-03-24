"""
Get count of unique URLs by hour
Only perform the counts for the URLs specified in "filter_list" that is broadcast
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("p5_t2")
sc = SparkContext(conf = conf)

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {}'.format(hourpart, url)

# Only include these URLs--brodcast this to the Spark cluster
filter_list = ['http://example.com/?url=0',
               'http://example.com/?url=9',
               'http://example.com/?url=3']
bc_list = sc.broadcast(filter_list)

logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
# Convert full timestamp to only hour and return only needed fields
hour_url = logs.map(extract_hourpart_url)

# Filter the logs using the broadcast data
hour_url = hour_url.filter(lambda x: any([i in x for i in bc_list.value]))

# Form key/values, and get grouped counts
# Note: duplicates are present here but will be allowed for this problem
tuples = hour_url.map(lambda x: tuple([x.split(' ')[0], 1]))
q1 = tuples.reduceByKey(lambda x, y: x + y)

q1.coalesce(1).saveAsTextFile("output_p5_t2/")
