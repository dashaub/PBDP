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

# Prepare the results in a dataframe
hour_user = hour_user.map(lambda x: Row(hour=x.split(' ')[0], user=x.split(' ')[1]))
df = sqlContext.createDataFrame(hour_user)

# Save the results to MariaDB
{'user':'root', 'driver':'org.mariadb.jdbc.Driver'}
table='hour_user'
db_locatio='jdbc:mysql://localhost:3306/hw8_spark'
df.write.mode('overwrite').jdbc(url=db_locatio, table=table, mode='overwrite', properties=properties)

