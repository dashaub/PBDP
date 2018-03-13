"""
Get count of unique URLs by hour
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setMaster("local[*]").setAppName("p1_q1")
sc = SparkContext(conf = conf)

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {}'.format(hourpart, url)

logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
# Convert full timestamp to only hour and return only needed fields
hour_url = logs.map(extract_hourpart_url)
# Remove duplicates, form key/values, and get grouped counts
dedup = hour_url.distinct()
tuples = dedup.map(lambda x: tuple([x.split(' ')[0], 1]))
q1 = tuples.reduceByKey(lambda x, y: x + y)

q1.coalesce(1).saveAsTextFile("output_p1_q1/")
