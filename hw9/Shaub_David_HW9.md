---
title: Homework 9
author: David Shaub
geometry: margin=2cm
date: 2018-04-07
---


## Problem 1

Our Spark job `p1.py` for counting URLs is reproduced below. Note that this job produces *both* the counts of URLs in the current one second window and the running cumualtive count per URL.
```
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

def functionToCreateContext():
    """
    Setup checkpointing
    """
    conf = SparkConf().setAppName('p1').setMaster("local[*]")
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
url_count = lines.map(extract_url).reduceByKey(lambda x, y: x + y)
url_count.pprint()

print 'Cumulative counts'
running_counts = url_count.updateStateByKey(updateFunction)
running_counts.pprint()


ssc.start()
ssc.awaitTermination()
ssc.stop()

```

The first few lines from launching the job:
```
$ spark-submit p1.py 
2018-04-04 21:05:31 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2018-04-04 21:05:32 INFO  SparkContext:54 - Running Spark version 2.3.0
2018-04-04 21:05:32 INFO  SparkContext:54 - Submitted application: p1
2018-04-04 21:05:32 INFO  SecurityManager:54 - Changing view acls to: david.shaub
2018-04-04 21:05:32 INFO  SecurityManager:54 - Changing modify acls to: david.shaub
2018-04-04 21:05:32 INFO  SecurityManager:54 - Changing view acls groups to: 
2018-04-04 21:05:32 INFO  SecurityManager:54 - Changing modify acls groups to: 
2018-04-04 21:05:32 INFO  SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(david.shaub); groups with view permissions: Set(); users  with modify permissions: Set(david.shaub); groups with modify permissions: Set()
2018-04-04 21:05:32 INFO  Utils:54 - Successfully started service 'sparkDriver' on port 55529.
2018-04-04 21:05:32 INFO  SparkEnv:54 - Registering MapOutputTracker
2018-04-04 21:05:32 INFO  SparkEnv:54 - Registering BlockManagerMaster

```

And the last lines of the job output (excluding the window and cumulative counts that appear below):
```
2018-04-04 21:05:32 INFO  Executor:54 - Starting executor ID driver on host localhost
2018-04-04 21:05:32 INFO  Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 55530.
2018-04-04 21:05:32 INFO  NettyBlockTransferService:54 - Server created on usmac2752dshau.local:55530
2018-04-04 21:05:32 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
2018-04-04 21:05:32 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, usmac2752dshau.local, 55530, None)
2018-04-04 21:05:32 INFO  BlockManagerMasterEndpoint:54 - Registering block manager usmac2752dshau.local:55530 with 366.3 MB RAM, BlockManagerId(driver, usmac2752dshau.local, 55530, None)
2018-04-04 21:05:32 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, usmac2752dshau.local, 55530, None)
2018-04-04 21:05:32 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, usmac2752dshau.local, 55530, None)
2018-04-04 21:05:33 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7936f741{/metrics/json,null,AVAILABLE,@Spark}
Counts in window
Cumulative counts
```

We will expect the cumulative count to approach 420,000 for each URL:
```
$ awk '{print $3}' data_stage/hw9_logs_*.txt | sort | uniq -c
420000 http://example.com/?url=0
420000 http://example.com/?url=1
420000 http://example.com/?url=2
420000 http://example.com/?url=3
420000 http://example.com/?url=4
420000 http://example.com/?url=5
420000 http://example.com/?url=6
420000 http://example.com/?url=7
420000 http://example.com/?url=8
420000 http://example.com/?url=9
```

The results of running the job appear below. Notice that some windows do not have any new streaming data entering (e.g. 21:05:55), but we still print out the cumulative count after every window. The first block of results for each second represents the URL counts in that window, and the second block of results for each second represents the cumulative counts.
```
-------------------------------------------
Time: 2018-04-04 21:05:54
-------------------------------------------
(u'http://example.com/?url=6', 12000)
(u'http://example.com/?url=7', 12000)
(u'http://example.com/?url=4', 12000)
(u'http://example.com/?url=5', 12000)
(u'http://example.com/?url=2', 12000)
(u'http://example.com/?url=3', 12000)
(u'http://example.com/?url=8', 12000)
(u'http://example.com/?url=0', 12000)
(u'http://example.com/?url=9', 12000)
(u'http://example.com/?url=1', 12000)

-------------------------------------------
Time: 2018-04-04 21:05:54
-------------------------------------------
(u'http://example.com/?url=6', 164000)
(u'http://example.com/?url=7', 164000)
(u'http://example.com/?url=4', 164000)
(u'http://example.com/?url=5', 164000)
(u'http://example.com/?url=2', 164000)
(u'http://example.com/?url=3', 164000)
(u'http://example.com/?url=8', 164000)
(u'http://example.com/?url=0', 164000)
(u'http://example.com/?url=9', 164000)
(u'http://example.com/?url=1', 164000)

-------------------------------------------
Time: 2018-04-04 21:05:55
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:05:55
-------------------------------------------
(u'http://example.com/?url=6', 164000)
(u'http://example.com/?url=7', 164000)
(u'http://example.com/?url=4', 164000)
(u'http://example.com/?url=5', 164000)
(u'http://example.com/?url=2', 164000)
(u'http://example.com/?url=3', 164000)
(u'http://example.com/?url=8', 164000)
(u'http://example.com/?url=0', 164000)
(u'http://example.com/?url=9', 164000)
(u'http://example.com/?url=1', 164000)

-------------------------------------------
Time: 2018-04-04 21:05:56
-------------------------------------------
(u'http://example.com/?url=6', 16000)
(u'http://example.com/?url=7', 16000)
(u'http://example.com/?url=4', 16000)
(u'http://example.com/?url=5', 16000)
(u'http://example.com/?url=2', 16000)
(u'http://example.com/?url=3', 16000)
(u'http://example.com/?url=8', 16000)
(u'http://example.com/?url=0', 16000)
(u'http://example.com/?url=9', 16000)
(u'http://example.com/?url=1', 16000)

-------------------------------------------
Time: 2018-04-04 21:05:56
-------------------------------------------
(u'http://example.com/?url=6', 180000)
(u'http://example.com/?url=7', 180000)
(u'http://example.com/?url=4', 180000)
(u'http://example.com/?url=5', 180000)
(u'http://example.com/?url=2', 180000)
(u'http://example.com/?url=3', 180000)
(u'http://example.com/?url=8', 180000)
(u'http://example.com/?url=0', 180000)
(u'http://example.com/?url=9', 180000)
(u'http://example.com/?url=1', 180000)

-------------------------------------------
Time: 2018-04-04 21:05:57
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:05:57
-------------------------------------------
(u'http://example.com/?url=6', 180000)
(u'http://example.com/?url=7', 180000)
(u'http://example.com/?url=4', 180000)
(u'http://example.com/?url=5', 180000)
(u'http://example.com/?url=2', 180000)
(u'http://example.com/?url=3', 180000)
(u'http://example.com/?url=8', 180000)
(u'http://example.com/?url=0', 180000)
(u'http://example.com/?url=9', 180000)
(u'http://example.com/?url=1', 180000)

-------------------------------------------
Time: 2018-04-04 21:05:58
-------------------------------------------
(u'http://example.com/?url=6', 8000)
(u'http://example.com/?url=7', 8000)
(u'http://example.com/?url=4', 8000)
(u'http://example.com/?url=5', 8000)
(u'http://example.com/?url=2', 8000)
(u'http://example.com/?url=3', 8000)
(u'http://example.com/?url=8', 8000)
(u'http://example.com/?url=0', 8000)
(u'http://example.com/?url=9', 8000)
(u'http://example.com/?url=1', 8000)

-------------------------------------------
Time: 2018-04-04 21:05:58
-------------------------------------------
(u'http://example.com/?url=6', 188000)
(u'http://example.com/?url=7', 188000)
(u'http://example.com/?url=4', 188000)
(u'http://example.com/?url=5', 188000)
(u'http://example.com/?url=2', 188000)
(u'http://example.com/?url=3', 188000)
(u'http://example.com/?url=8', 188000)
(u'http://example.com/?url=0', 188000)
(u'http://example.com/?url=9', 188000)
(u'http://example.com/?url=1', 188000)

-------------------------------------------
Time: 2018-04-04 21:05:59
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:05:59
-------------------------------------------
(u'http://example.com/?url=6', 188000)
(u'http://example.com/?url=7', 188000)
(u'http://example.com/?url=4', 188000)
(u'http://example.com/?url=5', 188000)
(u'http://example.com/?url=2', 188000)
(u'http://example.com/?url=3', 188000)
(u'http://example.com/?url=8', 188000)
(u'http://example.com/?url=0', 188000)
(u'http://example.com/?url=9', 188000)
(u'http://example.com/?url=1', 188000)

-------------------------------------------
Time: 2018-04-04 21:06:00
-------------------------------------------
(u'http://example.com/?url=6', 28000)
(u'http://example.com/?url=7', 28000)
(u'http://example.com/?url=4', 28000)
(u'http://example.com/?url=5', 28000)
(u'http://example.com/?url=2', 28000)
(u'http://example.com/?url=3', 28000)
(u'http://example.com/?url=8', 28000)
(u'http://example.com/?url=0', 28000)
(u'http://example.com/?url=9', 28000)
(u'http://example.com/?url=1', 28000)

-------------------------------------------
Time: 2018-04-04 21:06:00
-------------------------------------------
(u'http://example.com/?url=6', 216000)
(u'http://example.com/?url=7', 216000)
(u'http://example.com/?url=4', 216000)
(u'http://example.com/?url=5', 216000)
(u'http://example.com/?url=2', 216000)
(u'http://example.com/?url=3', 216000)
(u'http://example.com/?url=8', 216000)
(u'http://example.com/?url=0', 216000)
(u'http://example.com/?url=9', 216000)
(u'http://example.com/?url=1', 216000)

-------------------------------------------
Time: 2018-04-04 21:06:01
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:06:01
-------------------------------------------
(u'http://example.com/?url=6', 216000)
(u'http://example.com/?url=7', 216000)
(u'http://example.com/?url=4', 216000)
(u'http://example.com/?url=5', 216000)
(u'http://example.com/?url=2', 216000)
(u'http://example.com/?url=3', 216000)
(u'http://example.com/?url=8', 216000)
(u'http://example.com/?url=0', 216000)
(u'http://example.com/?url=9', 216000)
(u'http://example.com/?url=1', 216000)

-------------------------------------------
Time: 2018-04-04 21:06:02
-------------------------------------------
(u'http://example.com/?url=6', 20000)
(u'http://example.com/?url=7', 20000)
(u'http://example.com/?url=4', 20000)
(u'http://example.com/?url=5', 20000)
(u'http://example.com/?url=2', 20000)
(u'http://example.com/?url=3', 20000)
(u'http://example.com/?url=8', 20000)
(u'http://example.com/?url=0', 20000)
(u'http://example.com/?url=9', 20000)
(u'http://example.com/?url=1', 20000)

-------------------------------------------
Time: 2018-04-04 21:06:02
-------------------------------------------
(u'http://example.com/?url=6', 236000)
(u'http://example.com/?url=7', 236000)
(u'http://example.com/?url=4', 236000)
(u'http://example.com/?url=5', 236000)
(u'http://example.com/?url=2', 236000)
(u'http://example.com/?url=3', 236000)
(u'http://example.com/?url=8', 236000)
(u'http://example.com/?url=0', 236000)
(u'http://example.com/?url=9', 236000)
(u'http://example.com/?url=1', 236000)

-------------------------------------------
Time: 2018-04-04 21:06:03
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:06:03
-------------------------------------------
(u'http://example.com/?url=6', 236000)
(u'http://example.com/?url=7', 236000)
(u'http://example.com/?url=4', 236000)
(u'http://example.com/?url=5', 236000)
(u'http://example.com/?url=2', 236000)
(u'http://example.com/?url=3', 236000)
(u'http://example.com/?url=8', 236000)
(u'http://example.com/?url=0', 236000)
(u'http://example.com/?url=9', 236000)
(u'http://example.com/?url=1', 236000)

-------------------------------------------
Time: 2018-04-04 21:06:04
-------------------------------------------
(u'http://example.com/?url=6', 32000)
(u'http://example.com/?url=7', 32000)
(u'http://example.com/?url=4', 32000)
(u'http://example.com/?url=5', 32000)
(u'http://example.com/?url=2', 32000)
(u'http://example.com/?url=3', 32000)
(u'http://example.com/?url=8', 32000)
(u'http://example.com/?url=0', 32000)
(u'http://example.com/?url=9', 32000)
(u'http://example.com/?url=1', 32000)

-------------------------------------------
Time: 2018-04-04 21:06:04
-------------------------------------------
(u'http://example.com/?url=6', 268000)
(u'http://example.com/?url=7', 268000)
(u'http://example.com/?url=4', 268000)
(u'http://example.com/?url=5', 268000)
(u'http://example.com/?url=2', 268000)
(u'http://example.com/?url=3', 268000)
(u'http://example.com/?url=8', 268000)
(u'http://example.com/?url=0', 268000)
(u'http://example.com/?url=9', 268000)
(u'http://example.com/?url=1', 268000)
```


When all files are finished processing, we reach a cumulative count of 400,000, missing some 20,000 examples per URL that we expected from our count with command line tools:
```
-------------------------------------------
Time: 2018-04-04 21:06:35
-------------------------------------------
(u'http://example.com/?url=6', 400000)
(u'http://example.com/?url=7', 400000)
(u'http://example.com/?url=4', 400000)
(u'http://example.com/?url=5', 400000)
(u'http://example.com/?url=2', 400000)
(u'http://example.com/?url=3', 400000)
(u'http://example.com/?url=8', 400000)
(u'http://example.com/?url=0', 400000)
(u'http://example.com/?url=9', 400000)
(u'http://example.com/?url=1', 400000)

-------------------------------------------
Time: 2018-04-04 21:06:36
-------------------------------------------

-------------------------------------------
Time: 2018-04-04 21:06:36
-------------------------------------------
(u'http://example.com/?url=6', 400000)
(u'http://example.com/?url=7', 400000)
(u'http://example.com/?url=4', 400000)
(u'http://example.com/?url=5', 400000)
(u'http://example.com/?url=2', 400000)
(u'http://example.com/?url=3', 400000)
(u'http://example.com/?url=8', 400000)
(u'http://example.com/?url=0', 400000)
(u'http://example.com/?url=9', 400000)
(u'http://example.com/?url=1', 400000)
```

## Problem 2

We modify the job whereby counts within a window occur every 5 seconds with a 5 second offset so we achieve the tumbled, non-overlaping windows. Since the cumulative counts are themselves "forked" from the streams of individual counts, the cumulative counts are also automatically generated every 5 seconds. Our job is in `p2.py`:
```
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

```


We launch the job:
```
$ spark-submit p2.py 
2018-04-05 14:35:08 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2018-04-05 14:35:08 INFO  SparkContext:54 - Running Spark version 2.3.0
2018-04-05 14:35:08 INFO  SparkContext:54 - Submitted application: p2
2018-04-05 14:35:08 INFO  SecurityManager:54 - Changing view acls to: david.shaub
2018-04-05 14:35:08 INFO  SecurityManager:54 - Changing modify acls to: david.shaub
2018-04-05 14:35:08 INFO  SecurityManager:54 - Changing view acls groups to: 
2018-04-05 14:35:08 INFO  SecurityManager:54 - Changing modify acls groups to: 
2018-04-05 14:35:08 INFO  SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(david.shaub); groups with view permissions: Set(); users  with modify permissions: Set(david.shaub); groups with modify permissions: Set()
2018-04-05 14:35:09 INFO  Utils:54 - Successfully started service 'sparkDriver' on port 63721.
2018-04-05 14:35:09 INFO  SparkEnv:54 - Registering MapOutputTracker
2018-04-05 14:35:09 INFO  SparkEnv:54 - Registering BlockManagerMaster
2018-04-05 14:35:09 INFO  BlockManagerMasterEndpoint:54 - Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information

```

Once again the job counts 400,000 results per URL, but this time we see the results for both the window and cumulative counts are returned every 5 seconds:
```
2018-04-05 14:35:09 INFO  Executor:54 - Starting executor ID driver on host localhost
2018-04-05 14:35:09 INFO  Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 63722.
2018-04-05 14:35:09 INFO  NettyBlockTransferService:54 - Server created on usmac2752dshau.schq.secious.com:63722
2018-04-05 14:35:09 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
2018-04-05 14:35:09 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, usmac2752dshau.schq.secious.com, 63722, None)
2018-04-05 14:35:09 INFO  BlockManagerMasterEndpoint:54 - Registering block manager usmac2752dshau.schq.secious.com:63722 with 366.3 MB RAM, BlockManagerId(driver, usmac2752dshau.schq.secious.com, 63722, None)
2018-04-05 14:35:09 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, usmac2752dshau.schq.secious.com, 63722, None)
2018-04-05 14:35:09 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, usmac2752dshau.schq.secious.com, 63722, None)
2018-04-05 14:35:09 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@63009f6e{/metrics/json,null,AVAILABLE,@Spark}
Counts in window
Cumulative counts
-------------------------------------------
Time: 2018-04-05 14:35:15
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 14:35:15
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 14:35:20
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 14:35:20
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 14:35:25
-------------------------------------------
(u'http://example.com/?url=6', 44000)
(u'http://example.com/?url=7', 44000)
(u'http://example.com/?url=4', 44000)
(u'http://example.com/?url=5', 44000)
(u'http://example.com/?url=2', 44000)
(u'http://example.com/?url=3', 44000)
(u'http://example.com/?url=8', 44000)
(u'http://example.com/?url=0', 44000)
(u'http://example.com/?url=9', 44000)
(u'http://example.com/?url=1', 44000)

-------------------------------------------
Time: 2018-04-05 14:35:25
-------------------------------------------
(u'http://example.com/?url=6', 44000)
(u'http://example.com/?url=7', 44000)
(u'http://example.com/?url=4', 44000)
(u'http://example.com/?url=5', 44000)
(u'http://example.com/?url=2', 44000)
(u'http://example.com/?url=3', 44000)
(u'http://example.com/?url=8', 44000)
(u'http://example.com/?url=0', 44000)
(u'http://example.com/?url=9', 44000)
(u'http://example.com/?url=1', 44000)

-------------------------------------------
Time: 2018-04-05 14:35:30
-------------------------------------------
(u'http://example.com/?url=6', 56000)
(u'http://example.com/?url=7', 56000)
(u'http://example.com/?url=4', 56000)
(u'http://example.com/?url=5', 56000)
(u'http://example.com/?url=2', 56000)
(u'http://example.com/?url=3', 56000)
(u'http://example.com/?url=8', 56000)
(u'http://example.com/?url=0', 56000)
(u'http://example.com/?url=9', 56000)
(u'http://example.com/?url=1', 56000)

-------------------------------------------
Time: 2018-04-05 14:35:30
-------------------------------------------
(u'http://example.com/?url=6', 100000)
(u'http://example.com/?url=7', 100000)
(u'http://example.com/?url=4', 100000)
(u'http://example.com/?url=5', 100000)
(u'http://example.com/?url=2', 100000)
(u'http://example.com/?url=3', 100000)
(u'http://example.com/?url=8', 100000)
(u'http://example.com/?url=0', 100000)
(u'http://example.com/?url=9', 100000)
(u'http://example.com/?url=1', 100000)

-------------------------------------------
Time: 2018-04-05 14:35:35
-------------------------------------------
(u'http://example.com/?url=6', 64000)
(u'http://example.com/?url=7', 64000)
(u'http://example.com/?url=4', 64000)
(u'http://example.com/?url=5', 64000)
(u'http://example.com/?url=2', 64000)
(u'http://example.com/?url=3', 64000)
(u'http://example.com/?url=8', 64000)
(u'http://example.com/?url=0', 64000)
(u'http://example.com/?url=9', 64000)
(u'http://example.com/?url=1', 64000)

-------------------------------------------
Time: 2018-04-05 14:35:35
-------------------------------------------
(u'http://example.com/?url=6', 164000)
(u'http://example.com/?url=7', 164000)
(u'http://example.com/?url=4', 164000)
(u'http://example.com/?url=5', 164000)
(u'http://example.com/?url=2', 164000)
(u'http://example.com/?url=3', 164000)
(u'http://example.com/?url=8', 164000)
(u'http://example.com/?url=0', 164000)
(u'http://example.com/?url=9', 164000)
(u'http://example.com/?url=1', 164000)

-------------------------------------------
Time: 2018-04-05 14:35:40
-------------------------------------------
(u'http://example.com/?url=6', 24000)
(u'http://example.com/?url=7', 24000)
(u'http://example.com/?url=4', 24000)
(u'http://example.com/?url=5', 24000)
(u'http://example.com/?url=2', 24000)
(u'http://example.com/?url=3', 24000)
(u'http://example.com/?url=8', 24000)
(u'http://example.com/?url=0', 24000)
(u'http://example.com/?url=9', 24000)
(u'http://example.com/?url=1', 24000)

-------------------------------------------
Time: 2018-04-05 14:35:40
-------------------------------------------
(u'http://example.com/?url=6', 188000)
(u'http://example.com/?url=7', 188000)
(u'http://example.com/?url=4', 188000)
(u'http://example.com/?url=5', 188000)
(u'http://example.com/?url=2', 188000)
(u'http://example.com/?url=3', 188000)
(u'http://example.com/?url=8', 188000)
(u'http://example.com/?url=0', 188000)
(u'http://example.com/?url=9', 188000)
(u'http://example.com/?url=1', 188000)

-------------------------------------------
Time: 2018-04-05 14:35:45
-------------------------------------------
(u'http://example.com/?url=6', 48000)
(u'http://example.com/?url=7', 48000)
(u'http://example.com/?url=4', 48000)
(u'http://example.com/?url=5', 48000)
(u'http://example.com/?url=2', 48000)
(u'http://example.com/?url=3', 48000)
(u'http://example.com/?url=8', 48000)
(u'http://example.com/?url=0', 48000)
(u'http://example.com/?url=9', 48000)
(u'http://example.com/?url=1', 48000)

-------------------------------------------
Time: 2018-04-05 14:35:45
-------------------------------------------
(u'http://example.com/?url=6', 236000)
(u'http://example.com/?url=7', 236000)
(u'http://example.com/?url=4', 236000)
(u'http://example.com/?url=5', 236000)
(u'http://example.com/?url=2', 236000)
(u'http://example.com/?url=3', 236000)
(u'http://example.com/?url=8', 236000)
(u'http://example.com/?url=0', 236000)
(u'http://example.com/?url=9', 236000)
(u'http://example.com/?url=1', 236000)

-------------------------------------------
Time: 2018-04-05 14:35:50
-------------------------------------------
(u'http://example.com/?url=6', 76000)
(u'http://example.com/?url=7', 76000)
(u'http://example.com/?url=4', 76000)
(u'http://example.com/?url=5', 76000)
(u'http://example.com/?url=2', 76000)
(u'http://example.com/?url=3', 76000)
(u'http://example.com/?url=8', 76000)
(u'http://example.com/?url=0', 76000)
(u'http://example.com/?url=9', 76000)
(u'http://example.com/?url=1', 76000)

-------------------------------------------
Time: 2018-04-05 14:35:50
-------------------------------------------
(u'http://example.com/?url=6', 312000)
(u'http://example.com/?url=7', 312000)
(u'http://example.com/?url=4', 312000)
(u'http://example.com/?url=5', 312000)
(u'http://example.com/?url=2', 312000)
(u'http://example.com/?url=3', 312000)
(u'http://example.com/?url=8', 312000)
(u'http://example.com/?url=0', 312000)
(u'http://example.com/?url=9', 312000)
(u'http://example.com/?url=1', 312000)

-------------------------------------------
Time: 2018-04-05 14:35:55
-------------------------------------------
(u'http://example.com/?url=6', 40000)
(u'http://example.com/?url=7', 40000)
(u'http://example.com/?url=4', 40000)
(u'http://example.com/?url=5', 40000)
(u'http://example.com/?url=2', 40000)
(u'http://example.com/?url=3', 40000)
(u'http://example.com/?url=8', 40000)
(u'http://example.com/?url=0', 40000)
(u'http://example.com/?url=9', 40000)
(u'http://example.com/?url=1', 40000)

-------------------------------------------
Time: 2018-04-05 14:35:55
-------------------------------------------
(u'http://example.com/?url=6', 352000)
(u'http://example.com/?url=7', 352000)
(u'http://example.com/?url=4', 352000)
(u'http://example.com/?url=5', 352000)
(u'http://example.com/?url=2', 352000)
(u'http://example.com/?url=3', 352000)
(u'http://example.com/?url=8', 352000)
(u'http://example.com/?url=0', 352000)
(u'http://example.com/?url=9', 352000)
(u'http://example.com/?url=1', 352000)

-------------------------------------------
Time: 2018-04-05 14:36:00
-------------------------------------------
(u'http://example.com/?url=6', 48000)
(u'http://example.com/?url=7', 48000)
(u'http://example.com/?url=4', 48000)
(u'http://example.com/?url=5', 48000)
(u'http://example.com/?url=2', 48000)
(u'http://example.com/?url=3', 48000)
(u'http://example.com/?url=8', 48000)
(u'http://example.com/?url=0', 48000)
(u'http://example.com/?url=9', 48000)
(u'http://example.com/?url=1', 48000)

-------------------------------------------
Time: 2018-04-05 14:36:00
-------------------------------------------
(u'http://example.com/?url=6', 400000)
(u'http://example.com/?url=7', 400000)
(u'http://example.com/?url=4', 400000)
(u'http://example.com/?url=5', 400000)
(u'http://example.com/?url=2', 400000)
(u'http://example.com/?url=3', 400000)
(u'http://example.com/?url=8', 400000)
(u'http://example.com/?url=0', 400000)
(u'http://example.com/?url=9', 400000)
(u'http://example.com/?url=1', 400000)

-------------------------------------------
Time: 2018-04-05 14:36:05
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 14:36:05
-------------------------------------------
(u'http://example.com/?url=6', 400000)
(u'http://example.com/?url=7', 400000)
(u'http://example.com/?url=4', 400000)
(u'http://example.com/?url=5', 400000)
(u'http://example.com/?url=2', 400000)
(u'http://example.com/?url=3', 400000)
(u'http://example.com/?url=8', 400000)
(u'http://example.com/?url=0', 400000)
(u'http://example.com/?url=9', 400000)
(u'http://example.com/?url=1', 400000)
```


## Problem 3

Our job `p3_aggregation.py` will peform the count of distinct users in each window using the traditional Spark aggregation techniques. We will use a 30 second window and slide it by 30 seconds each so that the windows are non-overlapping.
```
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os

conf = SparkConf().setAppName('p3_aggregation').setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 1)

def updateFunction(newValues, runningCount):
    """
    Update the running count
    :param newValues: the number of records processed in the current window
    :param runningCount: the current running sum to increment
    """
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def extract_user(line):
    """
    Parse a line with UUID, timestqmap, URL, and user. Return the user.
    :param line: A string containing a record
    """
    (uuid, timestamp, url, user) = line.strip().split(' ')
    return user

print 'Unique users in window'
lines = ssc.textFileStream('data_input')
users = lines.map(extract_user).window(30, 30).transform(lambda rdd: rdd.distinct())
users.pprint(100)
users.count().pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()

```

We see that there are 40 unique users, so we will modify our `pprint()` to print at least 40 results for 30 second batch.
```
$ awk '{ print $4 }' data_stage/hw9_logs_*.txt | sort | uniq
User_0
User_1
User_10
User_11
User_12
User_13
User_14
User_15
User_16
User_17
User_18
User_19
User_2
User_20
User_21
User_22
User_23
User_24
User_25
User_26
User_27
User_28
User_29
User_3
User_30
User_31
User_32
User_33
User_34
User_35
User_36
User_37
User_38
User_39
User_4
User_5
User_6
User_7
User_8
User_9
$ awk '{ print $4 }' data_stage/hw9_logs_*.txt | sort | uniq | wc -l
      40
```


We launch the job
```
$ spark-submit p3_aggregation.py 
2018-04-05 15:39:43 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2018-04-05 15:39:43 INFO  SparkContext:54 - Running Spark version 2.3.0
2018-04-05 15:39:44 INFO  SparkContext:54 - Submitted application: p3_aggregation
2018-04-05 15:39:44 INFO  SecurityManager:54 - Changing view acls to: david.shaub
2018-04-05 15:39:44 INFO  SecurityManager:54 - Changing modify acls to: david.shaub
2018-04-05 15:39:44 INFO  SecurityManager:54 - Changing view acls groups to: 
2018-04-05 15:39:44 INFO  SecurityManager:54 - Changing modify acls groups to: 
2018-04-05 15:39:44 INFO  SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(david.shaub); groups with view permissions: Set(); users  with modify permissions: Set(david.shaub); groups with modify permissions: Set()

```

When the job runs, we see that all logs were processed during two 30-second windows. In each window, 40 distinct users were encountered. We see that no duplicate users appear _within_ a window, but duplicate users do appear _across_ windows--as we would expect.
```
2018-04-05 15:39:44 INFO  Executor:54 - Starting executor ID driver on host localhost
2018-04-05 15:39:44 INFO  Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 64559.
2018-04-05 15:39:44 INFO  NettyBlockTransferService:54 - Server created on usmac2752dshau.schq.secious.com:64559
2018-04-05 15:39:44 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
2018-04-05 15:39:44 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, usmac2752dshau.schq.secious.com, 64559, None)
2018-04-05 15:39:44 INFO  BlockManagerMasterEndpoint:54 - Registering block manager usmac2752dshau.schq.secious.com:64559 with 366.3 MB RAM, BlockManagerId(driver, usmac2752dshau.schq.secious.com, 64559, None)
2018-04-05 15:39:44 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, usmac2752dshau.schq.secious.com, 64559, None)
2018-04-05 15:39:44 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, usmac2752dshau.schq.secious.com, 64559, None)
2018-04-05 15:39:45 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@58c1dd51{/metrics/json,null,AVAILABLE,@Spark}
Unique users in window
-------------------------------------------
Time: 2018-04-05 15:40:15
-------------------------------------------
User_8
User_38
User_9
User_10
User_39
User_29
User_2
User_11
User_25
User_28
User_32
User_3
User_12
User_24
User_33
User_0
User_13
User_27
User_30
User_1
User_14
User_26
User_18
User_21
User_31
User_6
User_15
User_19
User_36
User_7
User_16
User_20
User_23
User_37
User_4
User_17
User_22
User_34
User_5
User_35

-------------------------------------------
Time: 2018-04-05 15:40:15
-------------------------------------------
40

-------------------------------------------
Time: 2018-04-05 15:40:45
-------------------------------------------
User_2
User_30
User_13
User_26
User_5
User_21
User_3
User_38
User_31
User_14
User_15
User_0
User_36
User_20
User_39
User_29
User_18
User_16
User_1
User_37
User_23
User_8
User_28
User_19
User_10
User_17
User_25
User_6
User_34
User_22
User_9
User_32
User_11
User_7
User_24
User_35
User_33
User_12
User_27
User_4

-------------------------------------------
Time: 2018-04-05 15:40:45
-------------------------------------------
40

-------------------------------------------
Time: 2018-04-05 15:41:15
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 15:41:15
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 15:41:45
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 15:41:45
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 15:42:15
-------------------------------------------

-------------------------------------------
Time: 2018-04-05 15:42:15
-------------------------------------------

```
