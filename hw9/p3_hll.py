from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import approx_count_distinct
import os

conf = SparkConf().setAppName('p3_hll').setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 1)


def extract_user(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the user.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    return user

def write_results(count):
    """
    Write the integer to a file
    """
    filename = 'hll_results'
    with open(filename, 'a') as res:
        res.write(str(count) + '\n')

print 'Apprxoimate unique users in window'
lines = ssc.textFileStream('data_input')
users = lines.map(extract_user).window(5, 5)

# countApproxDistinct returns an integer which we cannot print to the console using pprint()
# Instead we will pass this to a function that writes the results to a file
approx_unique = users.foreachRDD(lambda rdd: write_results(rdd.countApproxDistinct(relativeSD=0.8)))

ssc.start()
ssc.awaitTermination()
ssc.stop()
