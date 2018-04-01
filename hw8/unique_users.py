"""
Get unique visitors by URLs by hour
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("unique_users")
sc = SparkContext(conf = conf)

def extract_hourpart_user(dat):
    """
    Return timestamp (up to the hour) and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13] + ':00:00'
    return '{} {}'.format(hourpart, user)

logs = sc.textFile('hw7_logs*.txt')
# Extract hour, url, and user and remove duplicates
hour_user = logs.map(extract_hourpart_user).distinct()

hour_user.coalesce(1).saveAsTextFile("output_unique_users/")
