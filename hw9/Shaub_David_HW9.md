---
title: Homework 9
author: David Shaub
geometry: margin=2cm
date: 2018-04-07
---


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
