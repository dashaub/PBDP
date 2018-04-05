from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

def functionToCreateContext():
    """
    Setup checkpointing
    """
    conf = SparkConf().setAppName('p2').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint('checkpoints')
    return ssc

ssc = StreamingContext.getOrCreate('checkpoints', functionToCreateContext)

def updateFunction(newValues, runningCount):
    """
    Update the running count
    :param newValues: the number of records processed in the current window
    :param runningCount: the current running sum to increment
    """
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def extract_url(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the URL.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    #hour = timestamp[0:14]
    return (url, 1)



print 'Counts in window'
lines = ssc.textFileStream('data_input')
url_count = lines.window(5, 5).map(extract_url).reduceByKey(lambda x, y: x + y)
url_count.pprint()

print 'Cumulative counts'
running_counts = url_count.updateStateByKey(updateFunction)
running_counts.pprint()


ssc.start()
ssc.awaitTermination()
ssc.stop()
