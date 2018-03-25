"""
Get count of unique URLs by hour--this time with deduplication
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("p2_q1")
sc = SparkContext(conf = conf)

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {}'.format(hourpart, url)

# Load data and remove duplicate UUIDs
logs = sc.textFile('s3://aws-logs-607380799823-us-east-2/hw7/hw7_logs*.txt').repartition(200).distinct()
# Convert full timestamp to only hour and return only needed fields and remove duplicate URLs
hour_url = logs.map(extract_hourpart_url).distinct()
# Form key/values, and get grouped counts
tuples = hour_url.map(lambda x: tuple([x.split(' ')[0], 1]))
q1 = tuples.reduceByKey(lambda x, y: x + y)

q1.coalesce(1).saveAsTextFile("hdfs:/output_p3_reducebykey/")
