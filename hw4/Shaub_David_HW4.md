---
title: Homework 4
author: David Shaub
geometry: margin=2cm
date: 2018-02-24
---

All problems were completed, including problem 5.

## Problem 1

The python file `p1_avrowriter.py`:
```
"""Load the text logfiles and save them in a single avro file"""
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Read the schema file
schema = avro.schema.parse(open('input_logs.avsc', 'rb').read())

# Define the files to process
input_files = ['logs_{}.txt'.format(i) for i in range(4)]
with DataFileWriter(open('logs.avro', 'wb'), DatumWriter(), schema) as writer:
    # Process each input file
    for input_file in input_files:
        # Open each input file
        with open(input_file, 'r') as current_file:
            # Process each line in each input file
            for line in current_file:
                current_line = line.strip().split('\t')
                # Only parse and write if there is correct input
                if len(current_line) == 3:
                    timestamp, url, user = current_line
                    # Write to avro
                    writer.append({'timestamp': timestamp, 'url': url, 'user': user})
```

The avro schema was easy to create since all fields are `string` values. The only field that *could* have another type would be the timestamp field as a date/time object, but since avro does not have a primitive date/time object, we use a string for this too:
```
{"namespace": "logs.avro",
 "type": "record",
 "name": "visits",
 "fields": [
     {"name": "timestamp", "type": "string"},
     {"name": "url",  "type": "string"},
     {"name": "user", "type": "string"}
 ]
}
```
We launch the conversion process:
```
$ python p1_avrowriter.py
```
And examine the results in `logs.avro` that are partially human-readable:
```
$ head -c 2000 logs.avro 
Objavro.schema?{"type": "record", "namespace": "logs.avro", "name": "visits", "fields": [{"type": "string", "name": "timestamp"}, {"type": "string", "name": "url"}, {"type": "string", "name": "user"}]}avro.codenullg?W?Lܕ?N
?[??o????(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                        User_0(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                                                             User_1(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                                                                                                                  User_2(2018-02-13T00:00:00Z2http://example.com/?url=0
           User_3(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                User_402018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                         User_002018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                                                                                  User_102018-02-13T00:00:19.200Z2http://example.com/?url=0
                               User_202018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                        User_302018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                                                 User_402018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                                                                                                          User_002018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                       User_102018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                User_202018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                                                                         User_302018-02-13T00:00:38.400Z2http://example.com/?url=0
                      User_402018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                               User_002018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                                                        User_102018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                                                                                                                 User_202018-02-13T00:00:57.600Z2http://example.com/?url=0
                                              User_302018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                       User_402018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                                                                User_002018-02-13T00:01:16.800Z2http://example.com/?url=0
             User_102018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                      User_202018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                               User_302018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                                                                                        User_4(2018-02-13T00:01:36Z2http://example.com/?url=0
                                 User_0(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                      User_1(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                                                                           User_2(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                                                                                                                                User_3(2018-02-13T00:01:36Z2http://example.com/?url=0
                                         User_402018-02-13T00:01:55.200Z2http://example.com/?url=0
```

## Problem 2

Only very minor changes were required from our previous streaming MR jobs that used input data in text format. The reducers do not need to change at all since they receive identical output data from the mapper and are not inpacted by the input data format on disk. The mappers can structurally remain the same, but we make a few tiny modifications to handle the avro input: instead of manually splitting the input line on the table character and producing an array, we load the JSON line into a python dict and can then easily access the fields we are interested in.

Place the files in HDFS:
```
$ hadoop fs -mkdir /avro
$ hadoop fs -copyFromLocal logs.avro /avro/
$ hadoop fs -ls /avro
Found 1 items
-rw-r--r--   1 hadoop hadoop   14003587 2018-02-24 19:47 /avro/logs.avro
```

Launch Q1:

```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p2_q1" -files p2_q1_mapper.py,p2_q1_reducer.py -mapper p2_q1_mapper.py -reducer p2_q1_reducer.py -input /avro -output /p2_q1 -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob4046090679719735987.jar tmpDir=null
18/02/24 20:14:28 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:14:28 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:14:29 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:14:29 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:14:30 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/24 20:14:30 INFO mapreduce.JobSubmitter: number of splits:8
18/02/24 20:14:30 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0003
18/02/24 20:14:30 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0003
18/02/24 20:14:30 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0003/
18/02/24 20:14:30 INFO mapreduce.Job: Running job: job_1519500853873_0003
18/02/24 20:14:37 INFO mapreduce.Job: Job job_1519500853873_0003 running in uber mode : false
18/02/24 20:14:37 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 20:14:48 INFO mapreduce.Job:  map 25% reduce 0%
18/02/24 20:14:49 INFO mapreduce.Job:  map 38% reduce 0%
18/02/24 20:14:56 INFO mapreduce.Job:  map 50% reduce 0%
18/02/24 20:14:57 INFO mapreduce.Job:  map 75% reduce 0%
18/02/24 20:14:58 INFO mapreduce.Job:  map 88% reduce 0%
18/02/24 20:14:59 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 20:15:01 INFO mapreduce.Job:  map 100% reduce 33%
18/02/24 20:15:03 INFO mapreduce.Job:  map 100% reduce 67%
18/02/24 20:15:06 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 20:15:07 INFO mapreduce.Job: Job job_1519500853873_0003 completed successfully
18/02/24 20:15:08 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=354688
		FILE: Number of bytes written=2157533
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=14690096
		HDFS: Number of bytes written=4
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed map tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=3789984
		Total time spent by all reduces in occupied slots (ms)=1523232
		Total time spent by all map tasks (ms)=78958
		Total time spent by all reduce tasks (ms)=15867
		Total vcore-milliseconds taken by all map tasks=78958
		Total vcore-milliseconds taken by all reduce tasks=15867
		Total megabyte-milliseconds taken by all map tasks=121279488
		Total megabyte-milliseconds taken by all reduce tasks=48743424
	Map-Reduce Framework
		Map input records=243750
		Map output records=243750
		Map output bytes=6881250
		Map output materialized bytes=355180
		Input split bytes=976
		Combine input records=0
		Combine output records=0
		Reduce input groups=1
		Reduce shuffle bytes=355180
		Reduce input records=243750
		Reduce output records=1
		Spilled Records=487500
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=1821
		CPU time spent (ms)=20650
		Physical memory (bytes) snapshot=4143689728
		Virtual memory (bytes) snapshot=40069242880
		Total committed heap usage (bytes)=3654287360
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14689120
	File Output Format Counters 
		Bytes Written=4
18/02/24 20:15:08 INFO streaming.StreamJob: Output directory: /p2_q1
```

The output matches our earlier result:
```
$ hadoop fs -cat /p2_q1/*
13
``

Launch Q2:

```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p2_q2" -files p2_q2_mapper.py,p2_q2_reducer.py -mapper p2_q2_mapper.py -reducer p2_q2_reducer.py -input /avro -output /p2_q2 -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob731665940153735652.jar tmpDir=null
18/02/24 20:18:28 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:18:28 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:18:28 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:18:28 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:18:29 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/24 20:18:29 INFO mapreduce.JobSubmitter: number of splits:8
18/02/24 20:18:29 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0004
18/02/24 20:18:29 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0004
18/02/24 20:18:30 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0004/
18/02/24 20:18:30 INFO mapreduce.Job: Running job: job_1519500853873_0004
18/02/24 20:18:37 INFO mapreduce.Job: Job job_1519500853873_0004 running in uber mode : false
18/02/24 20:18:37 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 20:18:48 INFO mapreduce.Job:  map 25% reduce 0%
18/02/24 20:18:49 INFO mapreduce.Job:  map 38% reduce 0%
18/02/24 20:18:55 INFO mapreduce.Job:  map 50% reduce 0%
18/02/24 20:18:58 INFO mapreduce.Job:  map 88% reduce 0%
18/02/24 20:19:00 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 20:19:02 INFO mapreduce.Job:  map 100% reduce 33%
18/02/24 20:19:05 INFO mapreduce.Job:  map 100% reduce 67%
18/02/24 20:19:06 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 20:19:07 INFO mapreduce.Job: Job job_1519500853873_0004 completed successfully
18/02/24 20:19:07 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=419712
		FILE: Number of bytes written=2288928
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=14690096
		HDFS: Number of bytes written=367
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed map tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=3828672
		Total time spent by all reduces in occupied slots (ms)=1778304
		Total time spent by all map tasks (ms)=79764
		Total time spent by all reduce tasks (ms)=18524
		Total vcore-milliseconds taken by all map tasks=79764
		Total vcore-milliseconds taken by all reduce tasks=18524
		Total megabyte-milliseconds taken by all map tasks=122517504
		Total megabyte-milliseconds taken by all reduce tasks=56905728
	Map-Reduce Framework
		Map input records=243750
		Map output records=243750
		Map output bytes=8100000
		Map output materialized bytes=421562
		Input split bytes=976
		Combine input records=0
		Combine output records=0
		Reduce input groups=13
		Reduce shuffle bytes=421562
		Reduce input records=243750
		Reduce output records=13
		Spilled Records=487500
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=2041
		CPU time spent (ms)=24330
		Physical memory (bytes) snapshot=4409503744
		Virtual memory (bytes) snapshot=40134574080
		Total committed heap usage (bytes)=3739222016
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14689120
	File Output Format Counters 
		Bytes Written=367
18/02/24 20:19:07 INFO streaming.StreamJob: Output directory: /p2_q2
```

The output matches our earlier result:
```
$ hadoop fs -cat /p2_q2/*
http://example.com/?url=1	5
http://example.com/?url=11	5
http://example.com/?url=4	5
http://example.com/?url=7	5
http://example.com/?url=12	5
http://example.com/?url=2	5
http://example.com/?url=5	5
http://example.com/?url=8	5
http://example.com/?url=0	5
http://example.com/?url=10	5
http://example.com/?url=3	5
http://example.com/?url=6	5
http://example.com/?url=9	5
``

Launch Q3:

```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p2_q3" -files p2_q3_mapper.py,p2_q3_reducer.py -mapper p2_q3_mapper.py -reducer p2_q3_reducer.py -input /avro -output /p2_q3 -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob1245710039366343928.jar tmpDir=null
18/02/24 20:24:48 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:24:48 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:24:48 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 20:24:48 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 20:24:49 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/24 20:24:49 WARN hdfs.DFSClient: Caught exception 
java.lang.InterruptedException
	at java.lang.Object.wait(Native Method)
	at java.lang.Thread.join(Thread.java:1252)
	at java.lang.Thread.join(Thread.java:1326)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.closeResponder(DFSOutputStream.java:609)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.endBlock(DFSOutputStream.java:370)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:546)
18/02/24 20:24:49 INFO mapreduce.JobSubmitter: number of splits:8
18/02/24 20:24:49 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0005
18/02/24 20:24:49 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0005
18/02/24 20:24:50 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0005/
18/02/24 20:24:50 INFO mapreduce.Job: Running job: job_1519500853873_0005
18/02/24 20:24:58 INFO mapreduce.Job: Job job_1519500853873_0005 running in uber mode : false
18/02/24 20:24:58 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 20:25:08 INFO mapreduce.Job:  map 13% reduce 0%
18/02/24 20:25:09 INFO mapreduce.Job:  map 25% reduce 0%
18/02/24 20:25:10 INFO mapreduce.Job:  map 38% reduce 0%
18/02/24 20:25:16 INFO mapreduce.Job:  map 50% reduce 0%
18/02/24 20:25:18 INFO mapreduce.Job:  map 75% reduce 0%
18/02/24 20:25:19 INFO mapreduce.Job:  map 88% reduce 0%
18/02/24 20:25:20 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 20:25:23 INFO mapreduce.Job:  map 100% reduce 33%
18/02/24 20:25:24 INFO mapreduce.Job:  map 100% reduce 67%
18/02/24 20:25:27 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 20:25:27 INFO mapreduce.Job: Job job_1519500853873_0005 completed successfully
18/02/24 20:25:27 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=461590
		FILE: Number of bytes written=2374380
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=14690096
		HDFS: Number of bytes written=2615
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed map tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=3939072
		Total time spent by all reduces in occupied slots (ms)=1659936
		Total time spent by all map tasks (ms)=82064
		Total time spent by all reduce tasks (ms)=17291
		Total vcore-milliseconds taken by all map tasks=82064
		Total vcore-milliseconds taken by all reduce tasks=17291
		Total megabyte-milliseconds taken by all map tasks=126050304
		Total megabyte-milliseconds taken by all reduce tasks=53117952
	Map-Reduce Framework
		Map input records=243750
		Map output records=243750
		Map output bytes=9075000
		Map output materialized bytes=465125
		Input split bytes=976
		Combine input records=0
		Combine output records=0
		Reduce input groups=65
		Reduce shuffle bytes=465125
		Reduce input records=243750
		Reduce output records=65
		Spilled Records=487500
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=1827
		CPU time spent (ms)=24830
		Physical memory (bytes) snapshot=4426149888
		Virtual memory (bytes) snapshot=40113229824
		Total committed heap usage (bytes)=3764387840
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14689120
	File Output Format Counters 
		Bytes Written=2615
18/02/24 20:25:27 INFO streaming.StreamJob: Output directory: /p2_q3

```

We notice that an HDFS exception was caught while the job was running, but the output matches our earlier result:
```
$ hadoop fs -cat /p2_q3/*
http://example.com/?url=0 : User_2	3750
http://example.com/?url=1 : User_1	3750
http://example.com/?url=1 : User_4	3750
http://example.com/?url=10 : User_0	3750
http://example.com/?url=10 : User_3	3750
http://example.com/?url=11 : User_2	3750
http://example.com/?url=12 : User_1	3750
http://example.com/?url=12 : User_4	3750
http://example.com/?url=2 : User_0	3750
http://example.com/?url=2 : User_3	3750
http://example.com/?url=3 : User_0	3750
http://example.com/?url=3 : User_3	3750
http://example.com/?url=4 : User_2	3750
http://example.com/?url=5 : User_1	3750
http://example.com/?url=5 : User_4	3750
http://example.com/?url=6 : User_0	3750
http://example.com/?url=6 : User_3	3750
http://example.com/?url=7 : User_2	3750
http://example.com/?url=8 : User_1	3750
http://example.com/?url=8 : User_4	3750
http://example.com/?url=9 : User_0	3750
http://example.com/?url=9 : User_3	3750
http://example.com/?url=0 : User_0	3750
http://example.com/?url=0 : User_3	3750
http://example.com/?url=1 : User_2	3750
http://example.com/?url=10 : User_1	3750
http://example.com/?url=10 : User_4	3750
http://example.com/?url=11 : User_0	3750
http://example.com/?url=11 : User_3	3750
http://example.com/?url=12 : User_2	3750
http://example.com/?url=2 : User_1	3750
http://example.com/?url=2 : User_4	3750
http://example.com/?url=3 : User_1	3750
http://example.com/?url=3 : User_4	3750
http://example.com/?url=4 : User_0	3750
http://example.com/?url=4 : User_3	3750
http://example.com/?url=5 : User_2	3750
http://example.com/?url=6 : User_1	3750
http://example.com/?url=6 : User_4	3750
http://example.com/?url=7 : User_0	3750
http://example.com/?url=7 : User_3	3750
http://example.com/?url=8 : User_2	3750
http://example.com/?url=9 : User_1	3750
http://example.com/?url=9 : User_4	3750
http://example.com/?url=0 : User_1	3750
http://example.com/?url=0 : User_4	3750
http://example.com/?url=1 : User_0	3750
http://example.com/?url=1 : User_3	3750
http://example.com/?url=10 : User_2	3750
http://example.com/?url=11 : User_1	3750
http://example.com/?url=11 : User_4	3750
http://example.com/?url=12 : User_0	3750
http://example.com/?url=12 : User_3	3750
http://example.com/?url=2 : User_2	3750
http://example.com/?url=3 : User_2	3750
http://example.com/?url=4 : User_1	3750
http://example.com/?url=4 : User_4	3750
http://example.com/?url=5 : User_0	3750
http://example.com/?url=5 : User_3	3750
http://example.com/?url=6 : User_2	3750
http://example.com/?url=7 : User_1	3750
http://example.com/?url=7 : User_4	3750
http://example.com/?url=8 : User_0	3750
http://example.com/?url=8 : User_3	3750
http://example.com/?url=9 : User_2	3750
```

If we sort the results so that similar keys appear next to each other, the results are more clear:
```
$ hadoop fs -cat /p2_q3/* | sort
http://example.com/?url=0 : User_0	3750
http://example.com/?url=0 : User_1	3750
http://example.com/?url=0 : User_2	3750
http://example.com/?url=0 : User_3	3750
http://example.com/?url=0 : User_4	3750
http://example.com/?url=10 : User_0	3750
http://example.com/?url=10 : User_1	3750
http://example.com/?url=10 : User_2	3750
http://example.com/?url=10 : User_3	3750
http://example.com/?url=10 : User_4	3750
http://example.com/?url=11 : User_0	3750
http://example.com/?url=11 : User_1	3750
http://example.com/?url=11 : User_2	3750
http://example.com/?url=11 : User_3	3750
http://example.com/?url=11 : User_4	3750
http://example.com/?url=12 : User_0	3750
http://example.com/?url=12 : User_1	3750
http://example.com/?url=12 : User_2	3750
http://example.com/?url=12 : User_3	3750
http://example.com/?url=12 : User_4	3750
http://example.com/?url=1 : User_0	3750
http://example.com/?url=1 : User_1	3750
http://example.com/?url=1 : User_2	3750
http://example.com/?url=1 : User_3	3750
http://example.com/?url=1 : User_4	3750
http://example.com/?url=2 : User_0	3750
http://example.com/?url=2 : User_1	3750
http://example.com/?url=2 : User_2	3750
http://example.com/?url=2 : User_3	3750
http://example.com/?url=2 : User_4	3750
http://example.com/?url=3 : User_0	3750
http://example.com/?url=3 : User_1	3750
http://example.com/?url=3 : User_2	3750
http://example.com/?url=3 : User_3	3750
http://example.com/?url=3 : User_4	3750
http://example.com/?url=4 : User_0	3750
http://example.com/?url=4 : User_1	3750
http://example.com/?url=4 : User_2	3750
http://example.com/?url=4 : User_3	3750
http://example.com/?url=4 : User_4	3750
http://example.com/?url=5 : User_0	3750
http://example.com/?url=5 : User_1	3750
http://example.com/?url=5 : User_2	3750
http://example.com/?url=5 : User_3	3750
http://example.com/?url=5 : User_4	3750
http://example.com/?url=6 : User_0	3750
http://example.com/?url=6 : User_1	3750
http://example.com/?url=6 : User_2	3750
http://example.com/?url=6 : User_3	3750
http://example.com/?url=6 : User_4	3750
http://example.com/?url=7 : User_0	3750
http://example.com/?url=7 : User_1	3750
http://example.com/?url=7 : User_2	3750
http://example.com/?url=7 : User_3	3750
http://example.com/?url=7 : User_4	3750
http://example.com/?url=8 : User_0	3750
http://example.com/?url=8 : User_1	3750
http://example.com/?url=8 : User_2	3750
http://example.com/?url=8 : User_3	3750
http://example.com/?url=8 : User_4	3750
http://example.com/?url=9 : User_0	3750
http://example.com/?url=9 : User_1	3750
http://example.com/?url=9 : User_2	3750
http://example.com/?url=9 : User_3	3750
http://example.com/?url=9 : User_4	3750
``

## Problem 3

To convert our avro data to parquet, we will use Hive. First we must enter the Hive shell and create an external table that points to our avro data and defines a schema:
```
$ hadoop fs -copyFromLocal input_logs.avsc /
$ hive

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j2.properties Async: false

hive> CREATE EXTERNAL TABLE avro_table 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
LOCATION '/avro' 
TBLPROPERTIES ('avro.schema.url'='/input_logs.avsc');
```

We run a few queries to verify that all data was loaded correctly:
```
hive> select * from avro_table limit 10;
OK
2018-02-13T00:00:00Z	http://example.com/?url=0	User_0
2018-02-13T00:00:00Z	http://example.com/?url=0	User_1
2018-02-13T00:00:00Z	http://example.com/?url=0	User_2
2018-02-13T00:00:00Z	http://example.com/?url=0	User_3
2018-02-13T00:00:00Z	http://example.com/?url=0	User_4
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_0
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_1
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_2
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_3
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_4
Time taken: 0.191 seconds, Fetched: 10 row(s)

hive> select count(*) from avro_table;
Query ID = hadoop_20180224204404_7e741816-7443-4326-8609-e34707bfa571
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1519500853873_0010)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 7.43 s     
----------------------------------------------------------------------------------------------
OK
243750
Time taken: 12.893 seconds, Fetched: 1 row(s)

```

Now we create the external parquet table that we will insert data into:
```
hive> CREATE EXTERNAL TABLE parquet_table (ts STRING, url STRING, usr STRING) STORED AS parquet LOCATION '/parquet_data';
OK
Time taken: 0.189 seconds

hive> INSERT OVERWRITE TABLE parquet_table SELECT * FROM avro_table;
Query ID = hadoop_20180224210823_279d2080-c55b-4dca-97c3-d59ff2767754
Total jobs = 1
Launching Job 1 out of 1
Tez session was closed. Reopening...
Session re-established.
Status: Running (Executing on YARN cluster with App id application_1519500853873_0011)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 7.03 s     
----------------------------------------------------------------------------------------------
Loading data to table default.parquet_table
OK
Time taken: 17.308 seconds
``

And we verify that the parquet data appears correctly--both in the table and in HDFS:
```
hive> select count(*) from parquet_table;
OK
243750
Time taken: 0.212 seconds, Fetched: 1 row(s)
hive> select * from parquet_table limit 10;
OK
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
2018-02-13T00:00:00Z	http://example.com/?url=0	User_0
2018-02-13T00:00:00Z	http://example.com/?url=0	User_1
2018-02-13T00:00:00Z	http://example.com/?url=0	User_2
2018-02-13T00:00:00Z	http://example.com/?url=0	User_3
2018-02-13T00:00:00Z	http://example.com/?url=0	User_4
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_0
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_1
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_2
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_3
2018-02-13T00:00:19.200Z	http://example.com/?url=0	User_4
Time taken: 0.136 seconds, Fetched: 10 row(s)
hive> quit;
$ hadoop fs -ls /parquet_data
Found 1 items
-rwxr-xr-x   1 hadoop hadoop     543818 2018-02-24 21:08 /parquet_data/000000_0
```

We notice that not only did our Hive queries complete far quicker, but the data on disk is much smaller than on avro or txt files--due to the efficient columnar compression.


## Problem 4

The parquet format is efficient for job that do not require acces to all columns since only data from the columns that are required are read. In our problem here, we do not need any data from the user columns, so this is not read. Moreover, since compression is done on a per-column basis, we can expect excellent compression ratios and smaller data reads. In general this format would be very efficient for scenarios where our data has a large number of columns but we only need to read a few columns since we we only need to read a tiny fractional amount of the data compared to a row-store format.


Since we must supply a URL and date for filtering, we will use `http://example.com/?url=0` and `2018-02-13`, respectively. We launch the MR job including hadoop2-iow-lib-1.20.jar built from https://github.com/whale2/iow-hadoop-streaming and parquet-hadoop-bundle-1.8.1.jar from http://central.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.8.1/parquet-hadoop-bundle-1.8.1.jar and are sure to include to pas our filter arguments to the mapper:
```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars hadoop2-iow-lib-1.20.jar,parquet-hadoop-bundle-1.8.1.jar -D mapredduce.job.name="p4" -D parquet.read.support.class=net.iponweb.hadoop.streaming.parquet.GroupReadSupport -D stream.reduce.output=text -files p4_mapper.py,p4_reducer.py -inputformat net.iponweb.hadoop.streaming.parquet.ParquetAsJsonInputFormat -input /parquet_data -output /p4_output -mapper 'p4_mapper.py http://example.com/?url=0 2018-02-13' -reducer p4_reducer.py
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob6799359616360321424.jar tmpDir=null
18/02/24 23:05:27 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 23:05:28 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 23:05:28 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 23:05:28 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 23:05:29 INFO input.FileInputFormat: Total input paths to process : 1
18/02/24 23:05:29 INFO mapreduce.JobSubmitter: number of splits:1
18/02/24 23:05:29 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0022
18/02/24 23:05:29 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0022
18/02/24 23:05:29 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0022/
18/02/24 23:05:29 INFO mapreduce.Job: Running job: job_1519500853873_0022
18/02/24 23:05:36 INFO mapreduce.Job: Job job_1519500853873_0022 running in uber mode : false
18/02/24 23:05:36 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 23:05:41 INFO mapreduce.Job: Task Id : attempt_1519500853873_0022_m_000000_0, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
	at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:322)
	at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:535)
	at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
	at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
	at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
	at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:455)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:344)
	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:164)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1698)
	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)

18/02/24 23:05:48 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 23:05:48 INFO mapreduce.Job: Task Id : attempt_1519500853873_0022_m_000000_1, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
	at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:322)
	at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:535)
	at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
	at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
	at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
	at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:455)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:344)
	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:164)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1698)
	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)

Container killed by the ApplicationMaster.
Container killed on request. Exit code is 143
Container exited with a non-zero exit code 143

18/02/24 23:05:49 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 23:05:53 INFO mapreduce.Job: Task Id : attempt_1519500853873_0022_m_000000_2, Status : FAILED
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
	at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:322)
	at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:535)
	at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
	at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
	at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
	at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:455)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:344)
	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:164)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1698)
	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)

Container killed by the ApplicationMaster.
Container killed on request. Exit code is 143
Container exited with a non-zero exit code 143

18/02/24 23:05:59 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 23:05:59 INFO mapreduce.Job: Job job_1519500853873_0022 failed with state FAILED due to: Task failed task_1519500853873_0022_m_000000
Job failed as tasks failed. failedMaps:1 failedReduces:0

18/02/24 23:06:00 INFO mapreduce.Job: Counters: 13
	Job Counters 
		Failed map tasks=4
		Killed reduce tasks=3
		Launched map tasks=4
		Other local map tasks=3
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=801504
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=16698
		Total time spent by all reduce tasks (ms)=0
		Total vcore-milliseconds taken by all map tasks=16698
		Total vcore-milliseconds taken by all reduce tasks=0
		Total megabyte-milliseconds taken by all map tasks=25648128
		Total megabyte-milliseconds taken by all reduce tasks=0
18/02/24 23:06:00 ERROR streaming.StreamJob: Job not successful!
Streaming Command Failed!
Feb 24, 2018 11:05:29 PM INFO: org.apache.parquet.hadoop.ParquetInputFormat: Total input paths to process : 1
```

We get a YARN error that appears difficult to debug. So we launch it again using our avro input:
```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p4" -files p4_mapper.py,p4_reducer.py -mapper 'p4_mapper.py http://example.com/?url=0 2018-02-13' -reducer p4_reducer.py -input /avro -output /p4_from_avro -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob3939476190954594775.jar tmpDir=null
18/02/24 22:48:23 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 22:48:23 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 22:48:24 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 22:48:24 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 22:48:26 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/24 22:48:26 INFO mapreduce.JobSubmitter: number of splits:8
18/02/24 22:48:26 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0020
18/02/24 22:48:27 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0020
18/02/24 22:48:27 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0020/
18/02/24 22:48:27 INFO mapreduce.Job: Running job: job_1519500853873_0020
18/02/24 22:48:34 INFO mapreduce.Job: Job job_1519500853873_0020 running in uber mode : false
18/02/24 22:48:34 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 22:48:45 INFO mapreduce.Job:  map 13% reduce 0%
18/02/24 22:48:47 INFO mapreduce.Job:  map 25% reduce 0%
18/02/24 22:48:53 INFO mapreduce.Job:  map 88% reduce 0%
18/02/24 22:48:56 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 22:48:59 INFO mapreduce.Job:  map 100% reduce 33%
18/02/24 22:49:00 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 22:49:01 INFO mapreduce.Job: Job job_1519500853873_0020 completed successfully
18/02/24 22:49:01 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=6278
		FILE: Number of bytes written=1460940
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=14690096
		HDFS: Number of bytes written=40
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed map tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=4378368
		Total time spent by all reduces in occupied slots (ms)=1527840
		Total time spent by all map tasks (ms)=91216
		Total time spent by all reduce tasks (ms)=15915
		Total vcore-milliseconds taken by all map tasks=91216
		Total vcore-milliseconds taken by all reduce tasks=15915
		Total megabyte-milliseconds taken by all map tasks=140107776
		Total megabyte-milliseconds taken by all reduce tasks=48890880
	Map-Reduce Framework
		Map input records=243750
		Map output records=18750
		Map output bytes=93750
		Map output materialized bytes=6744
		Input split bytes=976
		Combine input records=0
		Combine output records=0
		Reduce input groups=5
		Reduce shuffle bytes=6744
		Reduce input records=18750
		Reduce output records=5
		Spilled Records=37500
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=1930
		CPU time spent (ms)=18880
		Physical memory (bytes) snapshot=4341911552
		Virtual memory (bytes) snapshot=40096989184
		Total committed heap usage (bytes)=3740794880
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14689120
	File Output Format Counters 
		Bytes Written=40
18/02/24 22:49:01 INFO streaming.StreamJob: Output directory: /p4_from_avro
$ hadoop fs -cat /p4_from_avro/*
02	3750
00	3750
03	3750
01	3750
04	3750
```

This appears to work. So we'll launch one more with a different url/date filter:
```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p4" -files p4_mapper.py,p4_reducer.py -mapper 'p4_mapper.py http://example.com/?url=11 2018-02-13' -reducer p4_reducer.py -input /avro -output /p4_from_avro2 -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob5311059961523202693.jar tmpDir=null
18/02/24 23:10:33 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 23:10:33 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 23:10:33 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/24 23:10:33 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/24 23:10:34 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/24 23:10:34 INFO mapreduce.JobSubmitter: number of splits:8
18/02/24 23:10:34 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0023
18/02/24 23:10:35 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0023
18/02/24 23:10:35 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0023/
18/02/24 23:10:35 INFO mapreduce.Job: Running job: job_1519500853873_0023
18/02/24 23:10:43 INFO mapreduce.Job: Job job_1519500853873_0023 running in uber mode : false
18/02/24 23:10:43 INFO mapreduce.Job:  map 0% reduce 0%
18/02/24 23:10:53 INFO mapreduce.Job:  map 13% reduce 0%
18/02/24 23:10:55 INFO mapreduce.Job:  map 25% reduce 0%
18/02/24 23:11:00 INFO mapreduce.Job:  map 38% reduce 0%
18/02/24 23:11:01 INFO mapreduce.Job:  map 75% reduce 0%
18/02/24 23:11:02 INFO mapreduce.Job:  map 88% reduce 0%
18/02/24 23:11:04 INFO mapreduce.Job:  map 100% reduce 0%
18/02/24 23:11:07 INFO mapreduce.Job:  map 100% reduce 33%
18/02/24 23:11:08 INFO mapreduce.Job:  map 100% reduce 100%
18/02/24 23:11:08 INFO mapreduce.Job: Job job_1519500853873_0023 completed successfully
18/02/24 23:11:08 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=6278
		FILE: Number of bytes written=1460952
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=14690096
		HDFS: Number of bytes written=40
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed reduce tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=4383504
		Total time spent by all reduces in occupied slots (ms)=1443264
		Total time spent by all map tasks (ms)=91323
		Total time spent by all reduce tasks (ms)=15034
		Total vcore-milliseconds taken by all map tasks=91323
		Total vcore-milliseconds taken by all reduce tasks=15034
		Total megabyte-milliseconds taken by all map tasks=140272128
		Total megabyte-milliseconds taken by all reduce tasks=46184448
	Map-Reduce Framework
		Map input records=243750
		Map output records=18750
		Map output bytes=93750
		Map output materialized bytes=6734
		Input split bytes=976
		Combine input records=0
		Combine output records=0
		Reduce input groups=5
		Reduce shuffle bytes=6734
		Reduce input records=18750
		Reduce output records=5
		Spilled Records=37500
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=2007
		CPU time spent (ms)=19760
		Physical memory (bytes) snapshot=4313460736
		Virtual memory (bytes) snapshot=40145256448
		Total committed heap usage (bytes)=3679453184
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=14689120
	File Output Format Counters 
		Bytes Written=40
18/02/24 23:11:08 INFO streaming.StreamJob: Output directory: /p4_from_avro2
$ hadoop fs -cat /p4_from_avro2/*
02	3750
00	3750
03	3750
01	3750
04	3750
```
## Problem 5

Our avro schema now includes a field for `uuid`. This is of type string and our program will generate this data by MD5 hashing the input line so that if we have two identical inputs we will generate a collision. To duplicate each record, we read in each file twice, and we see that our output file `logs_p5.avro` is more than twice as large as the avro file `logs.avro` from p1 since we additionally have included UUID data.
```
$ ls -lh *.avro
-rw-r--r--  1 david.shaub  1289279784    13M Feb 21 20:20 logs.avro
-rw-r--r--  1 david.shaub  1289279784    42M Feb 24 12:01 logs_p5.avro
```

We place our "duplicates" file in HDFS:
```
$ hadoop fs -mkdir /avro_duplicates
$ hadoop fs -copyFromLocal logs_p5.avro /avro_duplicates/
$ hadoop fs -ls /avro_duplicates
Found 1 items
-rw-r--r--   1 hadoop hadoop   44099746 2018-02-24 23:43 /avro_duplicates/logs_p5.avro
```

The MR job requires very few modifications: our mapper now outputs the UUID as a value, and our reducer only increments the count if this is a new UUID that hasn't been seen before for the URL/user combination.

We launch the job:
```
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -libjars avro-mapred-1.8.2.jar -D mapredduce.job.name="p5" -files p5_mapper.py,p5_reducer.py -mapper p5_mapper.py -reducer p5_reducer.py -input /avro_duplicates -output /p5 -inputformat org.apache.avro.mapred.AvroAsTextInputFormat
packageJobJar: [] [/usr/lib/hadoop/hadoop-streaming-2.7.3-amzn-6.jar] /tmp/streamjob5712300643655873598.jar tmpDir=null
18/02/25 00:00:25 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/25 00:00:25 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/25 00:00:26 INFO impl.TimelineClientImpl: Timeline service address: http://ip-172-31-13-111.us-east-2.compute.internal:8188/ws/v1/timeline/
18/02/25 00:00:26 INFO client.RMProxy: Connecting to ResourceManager at ip-172-31-13-111.us-east-2.compute.internal/172.31.13.111:8032
18/02/25 00:00:26 INFO mapred.FileInputFormat: Total input paths to process : 1
18/02/25 00:00:27 INFO mapreduce.JobSubmitter: number of splits:8
18/02/25 00:00:27 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1519500853873_0029
18/02/25 00:00:27 INFO impl.YarnClientImpl: Submitted application application_1519500853873_0029
18/02/25 00:00:27 INFO mapreduce.Job: The url to track the job: http://ip-172-31-13-111.us-east-2.compute.internal:20888/proxy/application_1519500853873_0029/
18/02/25 00:00:27 INFO mapreduce.Job: Running job: job_1519500853873_0029
18/02/25 00:00:34 INFO mapreduce.Job: Job job_1519500853873_0029 running in uber mode : false
18/02/25 00:00:34 INFO mapreduce.Job:  map 0% reduce 0%
18/02/25 00:00:47 INFO mapreduce.Job:  map 13% reduce 0%
18/02/25 00:00:50 INFO mapreduce.Job:  map 25% reduce 0%
18/02/25 00:00:53 INFO mapreduce.Job:  map 30% reduce 0%
18/02/25 00:00:54 INFO mapreduce.Job:  map 41% reduce 0%
18/02/25 00:00:56 INFO mapreduce.Job:  map 44% reduce 0%
18/02/25 00:00:57 INFO mapreduce.Job:  map 55% reduce 0%
18/02/25 00:00:58 INFO mapreduce.Job:  map 68% reduce 0%
18/02/25 00:00:59 INFO mapreduce.Job:  map 88% reduce 0%
18/02/25 00:01:00 INFO mapreduce.Job:  map 100% reduce 0%
18/02/25 00:01:06 INFO mapreduce.Job:  map 100% reduce 33%
18/02/25 00:01:07 INFO mapreduce.Job:  map 100% reduce 67%
18/02/25 00:01:08 INFO mapreduce.Job:  map 100% reduce 100%
18/02/25 00:01:08 INFO mapreduce.Job: Job job_1519500853873_0029 completed successfully
18/02/25 00:01:08 INFO mapreduce.Job: Counters: 51
	File System Counters
		FILE: Number of bytes read=16605564
		FILE: Number of bytes written=34742703
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=44967813
		HDFS: Number of bytes written=2615
		HDFS: Number of read operations=41
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=6
	Job Counters 
		Killed map tasks=1
		Launched map tasks=8
		Launched reduce tasks=3
		Data-local map tasks=4
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=5947200
		Total time spent by all reduces in occupied slots (ms)=2001504
		Total time spent by all map tasks (ms)=123900
		Total time spent by all reduce tasks (ms)=20849
		Total vcore-milliseconds taken by all map tasks=123900
		Total vcore-milliseconds taken by all reduce tasks=20849
		Total megabyte-milliseconds taken by all map tasks=190310400
		Total megabyte-milliseconds taken by all reduce tasks=64048128
	Map-Reduce Framework
		Map input records=487500
		Map output records=487500
		Map output bytes=33262500
		Map output materialized bytes=16689749
		Input split bytes=1088
		Combine input records=0
		Combine output records=0
		Reduce input groups=65
		Reduce shuffle bytes=16689749
		Reduce input records=487500
		Reduce output records=65
		Spilled Records=975000
		Shuffled Maps =24
		Failed Shuffles=0
		Merged Map outputs=24
		GC time elapsed (ms)=2308
		CPU time spent (ms)=41120
		Physical memory (bytes) snapshot=4483514368
		Virtual memory (bytes) snapshot=40154021888
		Total committed heap usage (bytes)=3818913792
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=44966725
	File Output Format Counters 
		Bytes Written=2615
18/02/25 00:01:08 INFO streaming.StreamJob: Output directory: /p5
```

From the results we see that we get the same resluts as before even though we had duplicates in our input data. This is because we use the UUID to protect us from duplicates.
```
$ hadoop fs -cat /p5/* | sort
http://example.com/?url=0 : User_0	3750
http://example.com/?url=0 : User_1	3750
http://example.com/?url=0 : User_2	3750
http://example.com/?url=0 : User_3	3750
http://example.com/?url=0 : User_4	3750
http://example.com/?url=10 : User_0	3750
http://example.com/?url=10 : User_1	3750
http://example.com/?url=10 : User_2	3750
http://example.com/?url=10 : User_3	3750
http://example.com/?url=10 : User_4	3750
http://example.com/?url=11 : User_0	3750
http://example.com/?url=11 : User_1	3750
http://example.com/?url=11 : User_2	3750
http://example.com/?url=11 : User_3	3750
http://example.com/?url=11 : User_4	3750
http://example.com/?url=12 : User_0	3750
http://example.com/?url=12 : User_1	3750
http://example.com/?url=12 : User_2	3750
http://example.com/?url=12 : User_3	3750
http://example.com/?url=12 : User_4	3750
http://example.com/?url=1 : User_0	3750
http://example.com/?url=1 : User_1	3750
http://example.com/?url=1 : User_2	3750
http://example.com/?url=1 : User_3	3750
http://example.com/?url=1 : User_4	3750
http://example.com/?url=2 : User_0	3750
http://example.com/?url=2 : User_1	3750
http://example.com/?url=2 : User_2	3750
http://example.com/?url=2 : User_3	3750
http://example.com/?url=2 : User_4	3750
http://example.com/?url=3 : User_0	3750
http://example.com/?url=3 : User_1	3750
http://example.com/?url=3 : User_2	3750
http://example.com/?url=3 : User_3	3750
http://example.com/?url=3 : User_4	3750
http://example.com/?url=4 : User_0	3750
http://example.com/?url=4 : User_1	3750
http://example.com/?url=4 : User_2	3750
http://example.com/?url=4 : User_3	3750
http://example.com/?url=4 : User_4	3750
http://example.com/?url=5 : User_0	3750
http://example.com/?url=5 : User_1	3750
http://example.com/?url=5 : User_2	3750
http://example.com/?url=5 : User_3	3750
http://example.com/?url=5 : User_4	3750
http://example.com/?url=6 : User_0	3750
http://example.com/?url=6 : User_1	3750
http://example.com/?url=6 : User_2	3750
http://example.com/?url=6 : User_3	3750
http://example.com/?url=6 : User_4	3750
http://example.com/?url=7 : User_0	3750
http://example.com/?url=7 : User_1	3750
http://example.com/?url=7 : User_2	3750
http://example.com/?url=7 : User_3	3750
http://example.com/?url=7 : User_4	3750
http://example.com/?url=8 : User_0	3750
http://example.com/?url=8 : User_1	3750
http://example.com/?url=8 : User_2	3750
http://example.com/?url=8 : User_3	3750
http://example.com/?url=8 : User_4	3750
http://example.com/?url=9 : User_0	3750
http://example.com/?url=9 : User_1	3750
http://example.com/?url=9 : User_2	3750
http://example.com/?url=9 : User_3	3750
http://example.com/?url=9 : User_4	3750
```
