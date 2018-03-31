"""
Get number of distinct clicks by URL by hour by user
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("p1_q3")
sc = SparkContext(conf = conf)

def extract_hourpart_url_user(dat):
    """
    Return timestamp (up to the hour), URL, and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {} {}'.format(hourpart, url, user)

logs = sc.textFile('hw7_logs*.txt')
# Extract hour, url, and user and remove duplicates
hour_url_user = logs.map(extract_hourpart_url_user).distinct()

# Form key/values and get grouped counts
tuples = hour_url_user.map(lambda x: tuple([' '.join(x.split(' ')), 1]))
q2 = tuples.reduceByKey(lambda x, y: x + y)

q2.coalesce(1).saveAsTextFile("output_p1_q3/")
