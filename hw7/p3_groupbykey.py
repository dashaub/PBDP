"""
Get count of unique visitors by URLs by hour
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("p3_groupbykey")
sc = SparkContext(conf = conf)

def extract_hourpart_url_user(dat):
    """
    Return timestamp (up to the hour), URL, and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {} {}'.format(hourpart, url, user)

logs = sc.textFile('s3://aws-logs-607380799823-us-east-2/hw7/hw7_logs*.txt').repartition(200).distinct()
# Extract hour, url, and user and remove duplicates
hour_url_user = logs.map(extract_hourpart_url_user).distinct()

# Form key/values and get grouped counts
tuples = hour_url_user.map(lambda x: tuple([x.split(' ')[0] + ' ' + x.split(' ')[1], 1]))
q2 = tuples.groupByKey().mapValues(sum)

q2.coalesce(1).saveAsTextFile("hdfs:/output_p3_groupbykey/")
