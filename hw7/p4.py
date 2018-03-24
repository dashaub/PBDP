"""
Join the community and logs dataset and get count of clicks per URL per communityID
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setMaster("local[*]").setAppName("p4")
sc = SparkContext(conf = conf)

def extract_hourpart_url_user(dat):
    """
    Return timestamp (up to the hour), URL, and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {} {}'.format(hourpart, url, user)

# Load the logs and community datasets
logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
community = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_community.txt')

# Form tuples for joining
community_tuples = community.map(lambda x: tuple(x.split('\t')))
logs_tuples = logs.map(lambda x: tuple([x.split(' ')[3], None]))

# Join the RDDs, form tuples with community as keys, and get grouped counts
joined = logs_tuples.join(community_tuples)
community_keys = joined.map(lambda x: tuple([x[1][1], 1]))
counts = community_keys.reduceByKey(lambda x, y: x + y)

counts.coalesce(1).saveAsTextFile("output_p4/")
