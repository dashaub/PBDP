"""
Get unique URLs by hour
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from datetime import datetime


conf = SparkConf().setAppName("unique_url")
sc = SparkContext(conf = conf)

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13] + ':00:00'
    return '{} {}'.format(hourpart, url)

logs = sc.textFile('hw7_logs*.txt')
# Convert full timestamp to only hour and return only needed fields and remove duplicate URLs
hour_url = logs.map(extract_hourpart_url).distinct()

# Prepare the results in a dataframe
hour_url = hour_url.map(lambda x: Row(hour=x.split(' ')[0], url=x.split(' ')[1]))
df = sqlContext.createDataFrame(hour_url)

# Save the results to MariaDB
{'user':'root', 'driver':'org.mariadb.jdbc.Driver'}
table='hour_url'
db_locatio='jdbc:mysql://localhost:3306/hw8_spark'
df.write.mode('overwrite').jdbc(url=db_locatio, table=table, mode='overwrite', properties=properties)
