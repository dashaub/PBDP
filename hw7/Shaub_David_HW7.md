---
title: Homework 7
author: David Shaub
geometry: margin=2cm
date: 2018-04-20
---

All problems were completed including Problem 5.
export PYSPARK_DRIVER_PYTHON=ipython

## Problem 1
We make place the log files in HDFS:
```
$ hadoop fs -mkdir /hw7
$ hadoop fs -put hw7_logs_*.txt /hw7
```


**Query 1**
The Spark program `p1_q1.py`:
```
```

We submit the job to Spark:
```
$ spark-submit p1_q1.py 
18/03/24 21:05:31 INFO SparkContext: Running Spark version 2.2.1
18/03/24 21:05:32 INFO SparkContext: Submitted application: p1_q1
18/03/24 21:05:32 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:05:32 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:05:32 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:05:32 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:05:32 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:05:32 INFO Utils: Successfully started service 'sparkDriver' on port 46223.
18/03/24 21:05:32 INFO SparkEnv: Registering MapOutputTracker
18/03/24 21:05:32 INFO SparkEnv: Registering BlockManagerMaster
18/03/24 21:05:32 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/24 21:05:32 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/24 21:05:32 INFO DiskBlockManager: Created local directory at /mnt/tmp/blockmgr-1fc009bd-80d8-4b35-b47e-61cbf500b585
18/03/24 21:05:32 INFO MemoryStore: MemoryStore started with capacity 3.1 GB
18/03/24 21:05:33 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/24 21:05:33 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/24 21:05:33 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:05:33 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:05:34 INFO RMProxy: Connecting to ResourceManager at ip-172-31-24-43.us-east-2.compute.internal/172.31.24.43:8032
18/03/24 21:05:35 INFO Client: Requesting a new application from cluster with 2 NodeManagers
18/03/24 21:05:35 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (6144 MB per container)
18/03/24 21:05:35 INFO Client: Will allocate AM container, with 896 MB memory including 384 MB overhead
18/03/24 21:05:35 INFO Client: Setting up container launch context for our AM
18/03/24 21:05:35 INFO Client: Setting up the launch environment for our AM container
18/03/24 21:05:35 INFO Client: Preparing resources for our AM container
18/03/24 21:05:36 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
18/03/24 21:05:38 INFO Client: Uploading resource file:/mnt/tmp/spark-151ececf-5380-4fa6-b578-4261a100e52f/__spark_libs__3726949574768902160.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0002/__spark_libs__3726949574768902160.zip
18/03/24 21:05:40 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/pyspark.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0002/pyspark.zip
18/03/24 21:05:41 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/py4j-0.10.4-src.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0002/py4j-0.10.4-src.zip
18/03/24 21:05:41 INFO Client: Uploading resource file:/mnt/tmp/spark-151ececf-5380-4fa6-b578-4261a100e52f/__spark_conf__5723821386720264130.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0002/__spark_conf__.zip
18/03/24 21:05:41 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:05:41 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:05:41 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:05:41 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:05:41 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:05:41 INFO Client: Submitting application application_1521923763184_0002 to ResourceManager
18/03/24 21:05:41 INFO YarnClientImpl: Submitted application application_1521923763184_0002
18/03/24 21:05:41 INFO SchedulerExtensionServices: Starting Yarn extension services with app application_1521923763184_0002 and attemptId None
18/03/24 21:05:42 INFO Client: Application report for application_1521923763184_0002 (state: ACCEPTED)
18/03/24 21:05:42 INFO Client: 
	 client token: N/A
	 diagnostics: AM container is launched, waiting for AM container to Register with RM
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: default
	 start time: 1521925541684
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0002/
	 user: hadoop
18/03/24 21:05:43 INFO Client: Application report for application_1521923763184_0002 (state: ACCEPTED)
18/03/24 21:05:44 INFO Client: Application report for application_1521923763184_0002 (state: ACCEPTED)
18/03/24 21:05:45 INFO Client: Application report for application_1521923763184_0002 (state: ACCEPTED)
18/03/24 21:05:46 INFO Client: Application report for application_1521923763184_0002 (state: ACCEPTED)
18/03/24 21:05:46 INFO YarnSchedulerBackend$YarnSchedulerEndpoint: ApplicationMaster registered as NettyRpcEndpointRef(spark-client://YarnAM)
18/03/24 21:05:46 INFO YarnClientSchedulerBackend: Add WebUI Filter. org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter, Map(PROXY_HOSTS -> ip-172-31-24-43.us-east-2.compute.internal, PROXY_URI_BASES -> http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0002), /proxy/application_1521923763184_0002
18/03/24 21:05:46 INFO JettyUtils: Adding filter: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
18/03/24 21:05:47 INFO Client: Application report for application_1521923763184_0002 (state: RUNNING)
18/03/24 21:05:47 INFO Client: 
	 client token: N/A
	 diagnostics: N/A
	 ApplicationMaster host: 172.31.26.62
	 ApplicationMaster RPC port: 0
	 queue: default
	 start time: 1521925541684
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0002/
	 user: hadoop
18/03/24 21:05:47 INFO YarnClientSchedulerBackend: Application application_1521923763184_0002 has started running.
18/03/24 21:05:47 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 44851.
18/03/24 21:05:47 INFO NettyBlockTransferService: Server created on 172.31.24.43:44851
18/03/24 21:05:47 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/24 21:05:47 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.31.24.43, 44851, None)
18/03/24 21:05:47 INFO BlockManagerMasterEndpoint: Registering block manager 172.31.24.43:44851 with 3.1 GB RAM, BlockManagerId(driver, 172.31.24.43, 44851, None)
18/03/24 21:05:47 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.31.24.43, 44851, None)
18/03/24 21:05:47 INFO BlockManager: external shuffle service port = 7337
18/03/24 21:05:47 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.31.24.43, 44851, None)
18/03/24 21:05:48 INFO EventLoggingListener: Logging events to hdfs:///var/log/spark/apps/application_1521923763184_0002
18/03/24 21:05:48 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:05:51 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.26.62:52738) with ID 2
18/03/24 21:05:51 INFO ExecutorAllocationManager: New executor 2 has registered (new total is 1)
18/03/24 21:05:51 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-26-62.us-east-2.compute.internal:45139 with 2.6 GB RAM, BlockManagerId(2, ip-172-31-26-62.us-east-2.compute.internal, 45139, None)
18/03/24 21:05:53 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.30.96:37684) with ID 1
18/03/24 21:05:53 INFO ExecutorAllocationManager: New executor 1 has registered (new total is 2)
18/03/24 21:05:53 INFO YarnClientSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.8
18/03/24 21:05:53 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-30-96.us-east-2.compute.internal:45223 with 2.6 GB RAM, BlockManagerId(1, ip-172-31-30-96.us-east-2.compute.internal, 45223, None)
18/03/24 21:05:53 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 273.9 KB, free 3.1 GB)
18/03/24 21:05:54 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.3 KB, free 3.1 GB)
18/03/24 21:05:54 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.31.24.43:44851 (size: 23.3 KB, free: 3.1 GB)
18/03/24 21:05:54 INFO SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
18/03/24 21:05:54 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
18/03/24 21:05:54 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
18/03/24 21:05:54 INFO SparkContext: Starting job: saveAsTextFile at NativeMethodAccessorImpl.java:0
18/03/24 21:05:54 INFO GPLNativeCodeLoader: Loaded native gpl library
18/03/24 21:05:54 INFO LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev cfe28705e7dfdec92539cc7b24fc97936c259a05]
18/03/24 21:05:54 INFO FileInputFormat: Total input files to process : 20
18/03/24 21:05:54 INFO DAGScheduler: Registering RDD 3 (distinct at /home/hadoop/hw7/p1_q1.py:23)
18/03/24 21:05:54 INFO DAGScheduler: Registering RDD 7 (reduceByKey at /home/hadoop/hw7/p1_q1.py:26)
18/03/24 21:05:54 INFO DAGScheduler: Got job 0 (saveAsTextFile at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/24 21:05:54 INFO DAGScheduler: Final stage: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0)
18/03/24 21:05:54 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
18/03/24 21:05:54 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
18/03/24 21:05:54 INFO DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q1.py:23), which has no missing parents
18/03/24 21:05:54 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 9.4 KB, free 3.1 GB)
18/03/24 21:05:54 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 5.9 KB, free 3.1 GB)
18/03/24 21:05:54 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.31.24.43:44851 (size: 5.9 KB, free: 3.1 GB)
18/03/24 21:05:54 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1047
18/03/24 21:05:54 INFO DAGScheduler: Submitting 20 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q1.py:23) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:05:54 INFO YarnScheduler: Adding task set 0.0 with 20 tasks
18/03/24 21:05:54 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 0, NODE_LOCAL, 4892 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 1, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 2, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 4, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 3, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 2, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 4, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 5, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 5, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 3, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 12.0 in stage 0.0 (TID 6, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 12, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:54 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 7, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 6, NODE_LOCAL, 4893 bytes)
18/03/24 21:05:55 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:45223 (size: 5.9 KB, free: 2.6 GB)
18/03/24 21:05:55 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:45139 (size: 5.9 KB, free: 2.6 GB)
18/03/24 21:05:55 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:45223 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:05:55 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:45139 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:05:55 INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged (new desired total will be 3)
18/03/24 21:05:56 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 5)
18/03/24 21:06:01 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 8, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 7, NODE_LOCAL, 4893 bytes)
18/03/24 21:06:01 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 5) in 6966 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (1/20)
18/03/24 21:06:02 INFO TaskSetManager: Starting task 13.0 in stage 0.0 (TID 9, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 13, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:02 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 2) in 7414 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (2/20)
18/03/24 21:06:02 INFO TaskSetManager: Starting task 15.0 in stage 0.0 (TID 10, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 15, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:02 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 4) in 7439 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (3/20)
18/03/24 21:06:02 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 11, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 8, NODE_LOCAL, 4893 bytes)
18/03/24 21:06:02 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 7900 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (4/20)
18/03/24 21:06:03 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 12, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 9, NODE_LOCAL, 4893 bytes)
18/03/24 21:06:03 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 8) in 1526 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (5/20)
18/03/24 21:06:03 INFO TaskSetManager: Starting task 19.0 in stage 0.0 (TID 13, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 19, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:03 INFO TaskSetManager: Finished task 12.0 in stage 0.0 (TID 6) in 8678 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (6/20)
18/03/24 21:06:05 INFO TaskSetManager: Finished task 13.0 in stage 0.0 (TID 9) in 3376 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (7/20)
18/03/24 21:06:06 INFO TaskSetManager: Starting task 10.0 in stage 0.0 (TID 14, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 10, NODE_LOCAL, 4893 bytes)
18/03/24 21:06:06 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 7) in 11523 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (8/20)
18/03/24 21:06:06 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 11568 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (9/20)
18/03/24 21:06:06 INFO TaskSetManager: Starting task 11.0 in stage 0.0 (TID 15, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 11, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:06 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 11) in 3783 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (10/20)
18/03/24 21:06:06 INFO TaskSetManager: Starting task 14.0 in stage 0.0 (TID 16, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 14, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:06 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 3) in 11873 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (11/20)
18/03/24 21:06:08 INFO TaskSetManager: Finished task 19.0 in stage 0.0 (TID 13) in 4662 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (12/20)
18/03/24 21:06:08 INFO TaskSetManager: Finished task 15.0 in stage 0.0 (TID 10) in 6064 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (13/20)
18/03/24 21:06:09 INFO TaskSetManager: Starting task 16.0 in stage 0.0 (TID 17, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 16, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:09 INFO TaskSetManager: Finished task 11.0 in stage 0.0 (TID 15) in 3002 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (14/20)
18/03/24 21:06:10 INFO TaskSetManager: Starting task 17.0 in stage 0.0 (TID 18, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 17, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:10 INFO TaskSetManager: Finished task 16.0 in stage 0.0 (TID 17) in 1001 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (15/20)
18/03/24 21:06:10 INFO TaskSetManager: Starting task 18.0 in stage 0.0 (TID 19, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 18, NODE_LOCAL, 4892 bytes)
18/03/24 21:06:10 INFO TaskSetManager: Finished task 14.0 in stage 0.0 (TID 16) in 3800 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (16/20)
18/03/24 21:06:10 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 12) in 7611 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (17/20)
18/03/24 21:06:12 INFO TaskSetManager: Finished task 10.0 in stage 0.0 (TID 14) in 6687 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (18/20)
18/03/24 21:06:13 INFO TaskSetManager: Finished task 17.0 in stage 0.0 (TID 18) in 2985 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (19/20)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 18.0 in stage 0.0 (TID 19) in 4040 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (20/20)
18/03/24 21:06:14 INFO YarnScheduler: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/24 21:06:14 INFO DAGScheduler: ShuffleMapStage 0 (distinct at /home/hadoop/hw7/p1_q1.py:23) finished in 19.746 s
18/03/24 21:06:14 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:06:14 INFO DAGScheduler: running: Set()
18/03/24 21:06:14 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
18/03/24 21:06:14 INFO DAGScheduler: failed: Set()
18/03/24 21:06:14 INFO DAGScheduler: Submitting ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q1.py:26), which has no missing parents
18/03/24 21:06:14 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 8.5 KB, free 3.1 GB)
18/03/24 21:06:14 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 5.4 KB, free 3.1 GB)
18/03/24 21:06:14 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.31.24.43:44851 (size: 5.4 KB, free: 3.1 GB)
18/03/24 21:06:14 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1047
18/03/24 21:06:14 INFO DAGScheduler: Submitting 16 missing tasks from ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q1.py:26) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:06:14 INFO YarnScheduler: Adding task set 1.0 with 16 tasks
18/03/24 21:06:14 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 20, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 0, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 21, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 1, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 22, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 2, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 23, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 3, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 24, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 4, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 25, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 5, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 26, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 6, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 27, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 7, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:45139 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:06:14 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:45223 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:06:14 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.26.62:52738
18/03/24 21:06:14 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 229 bytes
18/03/24 21:06:14 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.30.96:37684
18/03/24 21:06:14 INFO TaskSetManager: Starting task 8.0 in stage 1.0 (TID 28, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 8, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 7.0 in stage 1.0 (TID 27) in 305 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (1/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 9.0 in stage 1.0 (TID 29, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 9, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 21) in 326 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (2/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 10.0 in stage 1.0 (TID 30, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 10, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 23) in 331 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (3/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 11.0 in stage 1.0 (TID 31, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 11, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 25) in 336 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (4/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 12.0 in stage 1.0 (TID 32, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 12, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 22) in 357 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (5/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 13.0 in stage 1.0 (TID 33, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 13, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 20) in 359 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (6/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 14.0 in stage 1.0 (TID 34, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 14, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 24) in 365 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (7/16)
18/03/24 21:06:14 INFO TaskSetManager: Starting task 15.0 in stage 1.0 (TID 35, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 15, NODE_LOCAL, 4621 bytes)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 6.0 in stage 1.0 (TID 26) in 426 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (8/16)
18/03/24 21:06:14 INFO TaskSetManager: Finished task 12.0 in stage 1.0 (TID 32) in 162 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (9/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 30) in 208 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (10/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 9.0 in stage 1.0 (TID 29) in 219 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (11/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 8.0 in stage 1.0 (TID 28) in 240 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (12/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 11.0 in stage 1.0 (TID 31) in 213 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (13/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 14.0 in stage 1.0 (TID 34) in 192 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (14/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 13.0 in stage 1.0 (TID 33) in 203 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (15/16)
18/03/24 21:06:15 INFO TaskSetManager: Finished task 15.0 in stage 1.0 (TID 35) in 138 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (16/16)
18/03/24 21:06:15 INFO YarnScheduler: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/24 21:06:15 INFO DAGScheduler: ShuffleMapStage 1 (reduceByKey at /home/hadoop/hw7/p1_q1.py:26) finished in 0.562 s
18/03/24 21:06:15 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:06:15 INFO DAGScheduler: running: Set()
18/03/24 21:06:15 INFO DAGScheduler: waiting: Set(ResultStage 2)
18/03/24 21:06:15 INFO DAGScheduler: failed: Set()
18/03/24 21:06:15 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/24 21:06:15 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 75.7 KB, free 3.1 GB)
18/03/24 21:06:15 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 29.0 KB, free 3.1 GB)
18/03/24 21:06:15 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.31.24.43:44851 (size: 29.0 KB, free: 3.1 GB)
18/03/24 21:06:15 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1047
18/03/24 21:06:15 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/24 21:06:15 INFO YarnScheduler: Adding task set 2.0 with 1 tasks
18/03/24 21:06:15 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 36, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 0, NODE_LOCAL, 5178 bytes)
18/03/24 21:06:15 INFO BlockManagerInfo: Removed broadcast_1_piece0 on ip-172-31-30-96.us-east-2.compute.internal:45223 in memory (size: 5.9 KB, free: 2.6 GB)
18/03/24 21:06:15 INFO BlockManagerInfo: Removed broadcast_1_piece0 on ip-172-31-26-62.us-east-2.compute.internal:45139 in memory (size: 5.9 KB, free: 2.6 GB)
18/03/24 21:06:15 INFO BlockManagerInfo: Removed broadcast_1_piece0 on 172.31.24.43:44851 in memory (size: 5.9 KB, free: 3.1 GB)
18/03/24 21:06:15 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:45223 (size: 29.0 KB, free: 2.6 GB)
18/03/24 21:06:15 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.31.30.96:37684
18/03/24 21:06:15 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 312 bytes
18/03/24 21:06:16 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 36) in 1202 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (1/1)
18/03/24 21:06:16 INFO YarnScheduler: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/03/24 21:06:16 INFO DAGScheduler: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0) finished in 1.198 s
18/03/24 21:06:16 INFO DAGScheduler: Job 0 finished: saveAsTextFile at NativeMethodAccessorImpl.java:0, took 21.952929 s
18/03/24 21:06:16 INFO SparkContext: Invoking stop() from shutdown hook
18/03/24 21:06:16 INFO SparkUI: Stopped Spark web UI at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:06:16 INFO YarnClientSchedulerBackend: Interrupting monitor thread
18/03/24 21:06:16 INFO YarnClientSchedulerBackend: Shutting down all executors
18/03/24 21:06:16 INFO YarnSchedulerBackend$YarnDriverEndpoint: Asking each executor to shut down
18/03/24 21:06:16 INFO SchedulerExtensionServices: Stopping SchedulerExtensionServices
(serviceOption=None,
 services=List(),
 started=false)
18/03/24 21:06:16 INFO YarnClientSchedulerBackend: Stopped
18/03/24 21:06:16 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/24 21:06:16 INFO MemoryStore: MemoryStore cleared
18/03/24 21:06:16 INFO BlockManager: BlockManager stopped
18/03/24 21:06:16 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/24 21:06:16 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/24 21:06:16 INFO SparkContext: Successfully stopped SparkContext
18/03/24 21:06:16 INFO ShutdownHookManager: Shutdown hook called
18/03/24 21:06:16 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-151ececf-5380-4fa6-b578-4261a100e52f/pyspark-78b104dc-7775-4b62-ad8d-a596617de3fb
18/03/24 21:06:16 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-151ececf-5380-4fa6-b578-4261a100e52f
```

The job completed in 22 seconds and used three stages (a distinct operation, group by operation, and file saving operation).
![p1_q1 job](p1_q1.png)

The DAG for the job:
![p1_q1 DAG](p1_q1_dag.png)

Furthermore, by exmining *stage 1* in the DAG we see that the RDD had 16 partitions, and that median shuffle read and write size is 46.8 KB and 3.2 KB, respectively.

The full set of logs from the Spark job are in *p1_q1_sparklogs.zip*.

We look at the job results:
```
$ hadoop fs -cat /output_p1_q1/part* | head
('2018-02-22T19', 10)
('2018-03-01T00', 10)
('2018-03-04T03', 10)
('2018-03-02T01', 10)
('2018-02-28T08', 10)
('2018-02-21T10', 10)
('2018-03-03T20', 10)
('2018-02-23T12', 10)
('2018-02-22T20', 10)
('2018-02-26T15', 10)

$ hadoop fs -cat /output_p1_q1/part* | tail
('2018-02-28T10', 10)
('2018-03-04T13', 10)
('2018-03-03T16', 10)
('2018-02-24T03', 10)
('2018-02-22T16', 10)
('2018-02-25T13', 10)
('2018-03-01T18', 10)
('2018-02-27T02', 10)
('2018-02-23T20', 10)
('2018-02-21T08', 10)
```

**Query 2**
The Spark program `p1_q2.py`:
```
```

We submit the job to Spark:
```
$ spark-submit p1_q2.py 
18/03/24 21:30:05 INFO SparkContext: Running Spark version 2.2.1
18/03/24 21:30:06 INFO SparkContext: Submitted application: p1_q2
18/03/24 21:30:06 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:30:06 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:30:06 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:30:06 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:30:06 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:30:06 INFO Utils: Successfully started service 'sparkDriver' on port 36767.
18/03/24 21:30:06 INFO SparkEnv: Registering MapOutputTracker
18/03/24 21:30:06 INFO SparkEnv: Registering BlockManagerMaster
18/03/24 21:30:06 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/24 21:30:06 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/24 21:30:06 INFO DiskBlockManager: Created local directory at /mnt/tmp/blockmgr-c9581927-fd6d-417d-a245-8a41a9c3b922
18/03/24 21:30:06 INFO MemoryStore: MemoryStore started with capacity 3.1 GB
18/03/24 21:30:07 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/24 21:30:07 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/24 21:30:07 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:30:07 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:30:08 INFO RMProxy: Connecting to ResourceManager at ip-172-31-24-43.us-east-2.compute.internal/172.31.24.43:8032
18/03/24 21:30:09 INFO Client: Requesting a new application from cluster with 2 NodeManagers
18/03/24 21:30:09 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (6144 MB per container)
18/03/24 21:30:09 INFO Client: Will allocate AM container, with 896 MB memory including 384 MB overhead
18/03/24 21:30:09 INFO Client: Setting up container launch context for our AM
18/03/24 21:30:09 INFO Client: Setting up the launch environment for our AM container
18/03/24 21:30:09 INFO Client: Preparing resources for our AM container
18/03/24 21:30:10 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
18/03/24 21:30:12 INFO Client: Uploading resource file:/mnt/tmp/spark-efeef0e4-ce70-4997-bfa8-ce14280d92d0/__spark_libs__6950093246502793835.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0003/__spark_libs__6950093246502793835.zip
18/03/24 21:30:14 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/pyspark.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0003/pyspark.zip
18/03/24 21:30:14 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/py4j-0.10.4-src.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0003/py4j-0.10.4-src.zip
18/03/24 21:30:14 INFO Client: Uploading resource file:/mnt/tmp/spark-efeef0e4-ce70-4997-bfa8-ce14280d92d0/__spark_conf__5430417008070508570.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0003/__spark_conf__.zip
18/03/24 21:30:15 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:30:15 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:30:15 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:30:15 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:30:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:30:15 INFO Client: Submitting application application_1521923763184_0003 to ResourceManager
18/03/24 21:30:15 INFO YarnClientImpl: Submitted application application_1521923763184_0003
18/03/24 21:30:15 INFO SchedulerExtensionServices: Starting Yarn extension services with app application_1521923763184_0003 and attemptId None
18/03/24 21:30:16 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:16 INFO Client: 
	 client token: N/A
	 diagnostics: AM container is launched, waiting for AM container to Register with RM
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: default
	 start time: 1521927015027
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0003/
	 user: hadoop
18/03/24 21:30:17 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:18 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:19 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:20 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:21 INFO Client: Application report for application_1521923763184_0003 (state: ACCEPTED)
18/03/24 21:30:21 INFO YarnSchedulerBackend$YarnSchedulerEndpoint: ApplicationMaster registered as NettyRpcEndpointRef(spark-client://YarnAM)
18/03/24 21:30:21 INFO YarnClientSchedulerBackend: Add WebUI Filter. org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter, Map(PROXY_HOSTS -> ip-172-31-24-43.us-east-2.compute.internal, PROXY_URI_BASES -> http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0003), /proxy/application_1521923763184_0003
18/03/24 21:30:21 INFO JettyUtils: Adding filter: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
18/03/24 21:30:22 INFO Client: Application report for application_1521923763184_0003 (state: RUNNING)
18/03/24 21:30:22 INFO Client: 
	 client token: N/A
	 diagnostics: N/A
	 ApplicationMaster host: 172.31.30.96
	 ApplicationMaster RPC port: 0
	 queue: default
	 start time: 1521927015027
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0003/
	 user: hadoop
18/03/24 21:30:22 INFO YarnClientSchedulerBackend: Application application_1521923763184_0003 has started running.
18/03/24 21:30:22 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 35111.
18/03/24 21:30:22 INFO NettyBlockTransferService: Server created on 172.31.24.43:35111
18/03/24 21:30:22 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/24 21:30:22 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.31.24.43, 35111, None)
18/03/24 21:30:22 INFO BlockManagerMasterEndpoint: Registering block manager 172.31.24.43:35111 with 3.1 GB RAM, BlockManagerId(driver, 172.31.24.43, 35111, None)
18/03/24 21:30:22 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.31.24.43, 35111, None)
18/03/24 21:30:22 INFO BlockManager: external shuffle service port = 7337
18/03/24 21:30:22 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.31.24.43, 35111, None)
18/03/24 21:30:22 INFO EventLoggingListener: Logging events to hdfs:///var/log/spark/apps/application_1521923763184_0003
18/03/24 21:30:22 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:30:27 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.30.96:49818) with ID 1
18/03/24 21:30:27 INFO ExecutorAllocationManager: New executor 1 has registered (new total is 1)
18/03/24 21:30:27 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-30-96.us-east-2.compute.internal:35791 with 2.6 GB RAM, BlockManagerId(1, ip-172-31-30-96.us-east-2.compute.internal, 35791, None)
18/03/24 21:30:28 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.26.62:46376) with ID 2
18/03/24 21:30:28 INFO ExecutorAllocationManager: New executor 2 has registered (new total is 2)
18/03/24 21:30:28 INFO YarnClientSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.8
18/03/24 21:30:28 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-26-62.us-east-2.compute.internal:33185 with 2.6 GB RAM, BlockManagerId(2, ip-172-31-26-62.us-east-2.compute.internal, 33185, None)
18/03/24 21:30:29 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 273.9 KB, free 3.1 GB)
18/03/24 21:30:29 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.3 KB, free 3.1 GB)
18/03/24 21:30:29 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.31.24.43:35111 (size: 23.3 KB, free: 3.1 GB)
18/03/24 21:30:29 INFO SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
18/03/24 21:30:29 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
18/03/24 21:30:29 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
18/03/24 21:30:29 INFO SparkContext: Starting job: saveAsTextFile at NativeMethodAccessorImpl.java:0
18/03/24 21:30:29 INFO GPLNativeCodeLoader: Loaded native gpl library
18/03/24 21:30:29 INFO LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev cfe28705e7dfdec92539cc7b24fc97936c259a05]
18/03/24 21:30:29 INFO FileInputFormat: Total input files to process : 20
18/03/24 21:30:29 INFO DAGScheduler: Registering RDD 3 (distinct at /home/hadoop/hw7/p1_q2.py:23)
18/03/24 21:30:29 INFO DAGScheduler: Registering RDD 7 (reduceByKey at /home/hadoop/hw7/p1_q2.py:27)
18/03/24 21:30:29 INFO DAGScheduler: Got job 0 (saveAsTextFile at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/24 21:30:29 INFO DAGScheduler: Final stage: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0)
18/03/24 21:30:29 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
18/03/24 21:30:29 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
18/03/24 21:30:29 INFO DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q2.py:23), which has no missing parents
18/03/24 21:30:30 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 9.4 KB, free 3.1 GB)
18/03/24 21:30:30 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.0 KB, free 3.1 GB)
18/03/24 21:30:30 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.31.24.43:35111 (size: 6.0 KB, free: 3.1 GB)
18/03/24 21:30:30 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1047
18/03/24 21:30:30 INFO DAGScheduler: Submitting 20 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q2.py:23) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:30:30 INFO YarnScheduler: Adding task set 0.0 with 20 tasks
18/03/24 21:30:30 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 0, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 1, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 1, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 0, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 2, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 2, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 3, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 4, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 4, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 3, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 5, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 5, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 6, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 6, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO TaskSetManager: Starting task 12.0 in stage 0.0 (TID 7, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 12, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:30 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33185 (size: 6.0 KB, free: 2.6 GB)
18/03/24 21:30:30 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:35791 (size: 6.0 KB, free: 2.6 GB)
18/03/24 21:30:30 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:35791 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:30:30 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33185 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:30:31 INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged (new desired total will be 3)
18/03/24 21:30:32 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 5)
18/03/24 21:30:37 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 8, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 7, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:37 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 4) in 7607 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (1/20)
18/03/24 21:30:38 INFO TaskSetManager: Starting task 13.0 in stage 0.0 (TID 9, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 13, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:38 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 3) in 8051 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (2/20)
18/03/24 21:30:38 INFO TaskSetManager: Starting task 15.0 in stage 0.0 (TID 10, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 15, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:38 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 5) in 8585 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (3/20)
18/03/24 21:30:39 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 11, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 8, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:39 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 8) in 1692 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (4/20)
18/03/24 21:30:39 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 12, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 9, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:39 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 0) in 9587 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (5/20)
18/03/24 21:30:40 INFO TaskSetManager: Starting task 19.0 in stage 0.0 (TID 13, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 19, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:40 INFO TaskSetManager: Finished task 12.0 in stage 0.0 (TID 7) in 10039 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (6/20)
18/03/24 21:30:42 INFO TaskSetManager: Finished task 13.0 in stage 0.0 (TID 9) in 4281 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (7/20)
18/03/24 21:30:43 INFO TaskSetManager: Starting task 10.0 in stage 0.0 (TID 14, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 10, NODE_LOCAL, 4893 bytes)
18/03/24 21:30:43 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 6) in 13661 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (8/20)
18/03/24 21:30:43 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 1) in 13695 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (9/20)
18/03/24 21:30:44 INFO TaskSetManager: Starting task 11.0 in stage 0.0 (TID 15, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 11, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:44 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 13881 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (10/20)
18/03/24 21:30:44 INFO TaskSetManager: Starting task 14.0 in stage 0.0 (TID 16, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 14, NODE_LOCAL, 4892 bytes)
18/03/24 21:30:44 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 11) in 5347 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (11/20)
18/03/24 21:30:45 INFO TaskSetManager: Finished task 19.0 in stage 0.0 (TID 13) in 5584 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (12/20)
18/03/24 21:30:45 INFO TaskSetManager: Finished task 15.0 in stage 0.0 (TID 10) in 7155 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (13/20)
18/03/24 21:30:48 INFO TaskSetManager: Starting task 16.0 in stage 0.0 (TID 17, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 16, RACK_LOCAL, 4892 bytes)
18/03/24 21:30:48 INFO TaskSetManager: Starting task 17.0 in stage 0.0 (TID 18, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 17, RACK_LOCAL, 4892 bytes)
18/03/24 21:30:48 INFO TaskSetManager: Starting task 18.0 in stage 0.0 (TID 19, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 18, RACK_LOCAL, 4892 bytes)
18/03/24 21:30:48 INFO TaskSetManager: Finished task 11.0 in stage 0.0 (TID 15) in 4167 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (14/20)
18/03/24 21:30:49 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 12) in 9512 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (15/20)
18/03/24 21:30:49 INFO TaskSetManager: Finished task 14.0 in stage 0.0 (TID 16) in 4614 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (16/20)
18/03/24 21:30:49 INFO TaskSetManager: Finished task 16.0 in stage 0.0 (TID 17) in 1347 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (17/20)
18/03/24 21:30:50 INFO TaskSetManager: Finished task 10.0 in stage 0.0 (TID 14) in 6571 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (18/20)
18/03/24 21:30:51 INFO TaskSetManager: Finished task 17.0 in stage 0.0 (TID 18) in 3328 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (19/20)
18/03/24 21:30:52 INFO TaskSetManager: Finished task 18.0 in stage 0.0 (TID 19) in 4390 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (20/20)
18/03/24 21:30:52 INFO YarnScheduler: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/24 21:30:52 INFO DAGScheduler: ShuffleMapStage 0 (distinct at /home/hadoop/hw7/p1_q2.py:23) finished in 22.394 s
18/03/24 21:30:52 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:30:52 INFO DAGScheduler: running: Set()
18/03/24 21:30:52 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
18/03/24 21:30:52 INFO DAGScheduler: failed: Set()
18/03/24 21:30:52 INFO DAGScheduler: Submitting ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q2.py:27), which has no missing parents
18/03/24 21:30:52 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 8.5 KB, free 3.1 GB)
18/03/24 21:30:52 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 5.4 KB, free 3.1 GB)
18/03/24 21:30:52 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.31.24.43:35111 (size: 5.4 KB, free: 3.1 GB)
18/03/24 21:30:52 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1047
18/03/24 21:30:52 INFO DAGScheduler: Submitting 16 missing tasks from ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q2.py:27) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:30:52 INFO YarnScheduler: Adding task set 1.0 with 16 tasks
18/03/24 21:30:52 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 20, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 0, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 21, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 1, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 22, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 2, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 23, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 3, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 24, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 4, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 25, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 5, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 26, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 6, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 27, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 7, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:52 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:35791 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:30:52 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33185 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:30:52 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.26.62:46376
18/03/24 21:30:52 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.30.96:49818
18/03/24 21:30:52 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 264 bytes
18/03/24 21:30:53 INFO TaskSetManager: Starting task 8.0 in stage 1.0 (TID 28, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 8, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 7.0 in stage 1.0 (TID 27) in 865 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (1/16)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 9.0 in stage 1.0 (TID 29, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 9, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 10.0 in stage 1.0 (TID 30, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 10, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 22) in 942 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (2/16)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 6.0 in stage 1.0 (TID 26) in 941 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (3/16)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 11.0 in stage 1.0 (TID 31, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 11, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 24) in 982 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (4/16)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 12.0 in stage 1.0 (TID 32, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 12, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 23) in 1011 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (5/16)
18/03/24 21:30:53 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 3)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 13.0 in stage 1.0 (TID 33, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 13, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 25) in 1079 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (6/16)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 14.0 in stage 1.0 (TID 34, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 14, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 21) in 1127 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (7/16)
18/03/24 21:30:53 INFO TaskSetManager: Starting task 15.0 in stage 1.0 (TID 35, ip-172-31-30-96.us-east-2.compute.internal, executor 1, partition 15, NODE_LOCAL, 4621 bytes)
18/03/24 21:30:53 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 20) in 1140 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (8/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 8.0 in stage 1.0 (TID 28) in 715 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (9/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 14.0 in stage 1.0 (TID 34) in 625 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (10/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 12.0 in stage 1.0 (TID 32) in 742 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (11/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 11.0 in stage 1.0 (TID 31) in 780 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (12/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 9.0 in stage 1.0 (TID 29) in 822 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (13/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 30) in 828 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (14/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 13.0 in stage 1.0 (TID 33) in 705 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (15/16)
18/03/24 21:30:54 INFO TaskSetManager: Finished task 15.0 in stage 1.0 (TID 35) in 674 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 1) (16/16)
18/03/24 21:30:54 INFO YarnScheduler: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/24 21:30:54 INFO DAGScheduler: ShuffleMapStage 1 (reduceByKey at /home/hadoop/hw7/p1_q2.py:27) finished in 1.815 s
18/03/24 21:30:54 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:30:54 INFO DAGScheduler: running: Set()
18/03/24 21:30:54 INFO DAGScheduler: waiting: Set(ResultStage 2)
18/03/24 21:30:54 INFO DAGScheduler: failed: Set()
18/03/24 21:30:54 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/24 21:30:54 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 75.7 KB, free 3.1 GB)
18/03/24 21:30:54 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 29.0 KB, free 3.1 GB)
18/03/24 21:30:54 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.31.24.43:35111 (size: 29.0 KB, free: 3.1 GB)
18/03/24 21:30:54 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1047
18/03/24 21:30:54 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/24 21:30:54 INFO YarnScheduler: Adding task set 2.0 with 1 tasks
18/03/24 21:30:54 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 36, ip-172-31-26-62.us-east-2.compute.internal, executor 2, partition 0, NODE_LOCAL, 5178 bytes)
18/03/24 21:30:54 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33185 (size: 29.0 KB, free: 2.6 GB)
18/03/24 21:30:54 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.31.26.62:46376
18/03/24 21:30:54 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 235 bytes
18/03/24 21:30:55 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 36) in 955 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 2) (1/1)
18/03/24 21:30:55 INFO YarnScheduler: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/03/24 21:30:55 INFO DAGScheduler: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0) finished in 0.956 s
18/03/24 21:30:55 INFO DAGScheduler: Job 0 finished: saveAsTextFile at NativeMethodAccessorImpl.java:0, took 25.553541 s
18/03/24 21:30:55 INFO SparkContext: Invoking stop() from shutdown hook
18/03/24 21:30:55 INFO SparkUI: Stopped Spark web UI at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:30:55 INFO YarnClientSchedulerBackend: Interrupting monitor thread
18/03/24 21:30:55 INFO YarnClientSchedulerBackend: Shutting down all executors
18/03/24 21:30:55 INFO YarnSchedulerBackend$YarnDriverEndpoint: Asking each executor to shut down
18/03/24 21:30:55 INFO SchedulerExtensionServices: Stopping SchedulerExtensionServices
(serviceOption=None,
 services=List(),
 started=false)
18/03/24 21:30:55 INFO YarnClientSchedulerBackend: Stopped
18/03/24 21:30:55 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/24 21:30:55 INFO MemoryStore: MemoryStore cleared
18/03/24 21:30:55 INFO BlockManager: BlockManager stopped
18/03/24 21:30:55 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/24 21:30:55 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/24 21:30:55 INFO SparkContext: Successfully stopped SparkContext
18/03/24 21:30:55 INFO ShutdownHookManager: Shutdown hook called
18/03/24 21:30:55 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-efeef0e4-ce70-4997-bfa8-ce14280d92d0
18/03/24 21:30:55 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-efeef0e4-ce70-4997-bfa8-ce14280d92d0/pyspark-23fb0f91-be8e-4f34-a3bb-f5476eca5923
```

The job completed in 25 seconds and used three stages (a distinct operation, group by operation, and file saving operation).
![p1_q1 job](p1_q1.png)

The DAG for the job:
![p1_q2 DAG](p1_q2_dag.png)

Furthermore, by exmining *stage 1* in the DAG we see that the RDD had 16 partitions, and that median shuffle read and write size is 2.0 MB and 38.0 KB, respectively.

The full set of logs from the Spark job are in *p1_q2_sparklogs.zip*.

We look at the job results:
```
$ hadoop fs -cat /output_p1_q2/part* | head
('2018-03-01T06 http://example.com/?url=3', 40)
('2018-02-27T09 http://example.com/?url=1', 40)
('2018-02-27T02 http://example.com/?url=2', 40)
('2018-02-27T16 http://example.com/?url=5', 40)
('2018-02-26T23 http://example.com/?url=2', 40)
('2018-02-20T22 http://example.com/?url=5', 40)
('2018-02-20T20 http://example.com/?url=7', 40)
('2018-02-25T18 http://example.com/?url=1', 40)
('2018-02-24T19 http://example.com/?url=7', 40)
('2018-02-21T04 http://example.com/?url=6', 40)

$ hadoop fs -cat /output_p1_q2/part* | tail
('2018-02-22T00 http://example.com/?url=8', 40)
('2018-02-25T09 http://example.com/?url=8', 40)
('2018-02-20T20 http://example.com/?url=8', 40)
('2018-02-25T00 http://example.com/?url=9', 40)
('2018-02-21T10 http://example.com/?url=2', 40)
('2018-02-21T13 http://example.com/?url=9', 40)
('2018-02-22T06 http://example.com/?url=6', 40)
('2018-02-24T11 http://example.com/?url=0', 40)
('2018-02-21T06 http://example.com/?url=3', 40)
('2018-03-02T00 http://example.com/?url=3', 40)
```

**Query 3**
The Spark program `p1_q3.py`:
```
```

We submit the job to Spark:
```
$ spark-submit p1_q3.py 
18/03/24 21:38:15 INFO SparkContext: Running Spark version 2.2.1
18/03/24 21:38:16 INFO SparkContext: Submitted application: p1_q3
18/03/24 21:38:16 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:38:16 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:38:16 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:38:16 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:38:16 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:38:16 INFO Utils: Successfully started service 'sparkDriver' on port 37135.
18/03/24 21:38:16 INFO SparkEnv: Registering MapOutputTracker
18/03/24 21:38:16 INFO SparkEnv: Registering BlockManagerMaster
18/03/24 21:38:16 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/03/24 21:38:16 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/03/24 21:38:16 INFO DiskBlockManager: Created local directory at /mnt/tmp/blockmgr-12cc8eba-3061-4a16-9ae0-d7275c425c23
18/03/24 21:38:16 INFO MemoryStore: MemoryStore started with capacity 3.1 GB
18/03/24 21:38:16 INFO SparkEnv: Registering OutputCommitCoordinator
18/03/24 21:38:17 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/03/24 21:38:17 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:38:17 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:38:18 INFO RMProxy: Connecting to ResourceManager at ip-172-31-24-43.us-east-2.compute.internal/172.31.24.43:8032
18/03/24 21:38:18 INFO Client: Requesting a new application from cluster with 2 NodeManagers
18/03/24 21:38:18 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (6144 MB per container)
18/03/24 21:38:18 INFO Client: Will allocate AM container, with 896 MB memory including 384 MB overhead
18/03/24 21:38:18 INFO Client: Setting up container launch context for our AM
18/03/24 21:38:18 INFO Client: Setting up the launch environment for our AM container
18/03/24 21:38:18 INFO Client: Preparing resources for our AM container
18/03/24 21:38:20 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
18/03/24 21:38:22 INFO Client: Uploading resource file:/mnt/tmp/spark-e22fd739-735e-43b4-891e-9c004f561ff7/__spark_libs__802440489466338234.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0004/__spark_libs__802440489466338234.zip
18/03/24 21:38:24 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/pyspark.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0004/pyspark.zip
18/03/24 21:38:24 INFO Client: Uploading resource file:/usr/lib/spark/python/lib/py4j-0.10.4-src.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0004/py4j-0.10.4-src.zip
18/03/24 21:38:24 INFO Client: Uploading resource file:/mnt/tmp/spark-e22fd739-735e-43b4-891e-9c004f561ff7/__spark_conf__4702585636499410133.zip -> hdfs://ip-172-31-24-43.us-east-2.compute.internal:8020/user/hadoop/.sparkStaging/application_1521923763184_0004/__spark_conf__.zip
18/03/24 21:38:24 INFO SecurityManager: Changing view acls to: hadoop
18/03/24 21:38:24 INFO SecurityManager: Changing modify acls to: hadoop
18/03/24 21:38:24 INFO SecurityManager: Changing view acls groups to: 
18/03/24 21:38:24 INFO SecurityManager: Changing modify acls groups to: 
18/03/24 21:38:24 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hadoop); groups with view permissions: Set(); users  with modify permissions: Set(hadoop); groups with modify permissions: Set()
18/03/24 21:38:24 INFO Client: Submitting application application_1521923763184_0004 to ResourceManager
18/03/24 21:38:24 INFO YarnClientImpl: Submitted application application_1521923763184_0004
18/03/24 21:38:24 INFO SchedulerExtensionServices: Starting Yarn extension services with app application_1521923763184_0004 and attemptId None
18/03/24 21:38:25 INFO Client: Application report for application_1521923763184_0004 (state: ACCEPTED)
18/03/24 21:38:25 INFO Client: 
	 client token: N/A
	 diagnostics: AM container is launched, waiting for AM container to Register with RM
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: default
	 start time: 1521927504731
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0004/
	 user: hadoop
18/03/24 21:38:26 INFO Client: Application report for application_1521923763184_0004 (state: ACCEPTED)
18/03/24 21:38:27 INFO Client: Application report for application_1521923763184_0004 (state: ACCEPTED)
18/03/24 21:38:28 INFO Client: Application report for application_1521923763184_0004 (state: ACCEPTED)
18/03/24 21:38:29 INFO Client: Application report for application_1521923763184_0004 (state: ACCEPTED)
18/03/24 21:38:30 INFO YarnSchedulerBackend$YarnSchedulerEndpoint: ApplicationMaster registered as NettyRpcEndpointRef(spark-client://YarnAM)
18/03/24 21:38:30 INFO YarnClientSchedulerBackend: Add WebUI Filter. org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter, Map(PROXY_HOSTS -> ip-172-31-24-43.us-east-2.compute.internal, PROXY_URI_BASES -> http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0004), /proxy/application_1521923763184_0004
18/03/24 21:38:30 INFO JettyUtils: Adding filter: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
18/03/24 21:38:30 INFO Client: Application report for application_1521923763184_0004 (state: RUNNING)
18/03/24 21:38:30 INFO Client: 
	 client token: N/A
	 diagnostics: N/A
	 ApplicationMaster host: 172.31.26.62
	 ApplicationMaster RPC port: 0
	 queue: default
	 start time: 1521927504731
	 final status: UNDEFINED
	 tracking URL: http://ip-172-31-24-43.us-east-2.compute.internal:20888/proxy/application_1521923763184_0004/
	 user: hadoop
18/03/24 21:38:30 INFO YarnClientSchedulerBackend: Application application_1521923763184_0004 has started running.
18/03/24 21:38:30 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 39457.
18/03/24 21:38:30 INFO NettyBlockTransferService: Server created on 172.31.24.43:39457
18/03/24 21:38:30 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/03/24 21:38:30 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 172.31.24.43, 39457, None)
18/03/24 21:38:30 INFO BlockManagerMasterEndpoint: Registering block manager 172.31.24.43:39457 with 3.1 GB RAM, BlockManagerId(driver, 172.31.24.43, 39457, None)
18/03/24 21:38:30 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 172.31.24.43, 39457, None)
18/03/24 21:38:30 INFO BlockManager: external shuffle service port = 7337
18/03/24 21:38:30 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 172.31.24.43, 39457, None)
18/03/24 21:38:31 INFO EventLoggingListener: Logging events to hdfs:///var/log/spark/apps/application_1521923763184_0004
18/03/24 21:38:31 INFO Utils: Using initial executors = 2, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
18/03/24 21:38:36 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.26.62:51310) with ID 1
18/03/24 21:38:36 INFO ExecutorAllocationManager: New executor 1 has registered (new total is 1)
18/03/24 21:38:36 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-26-62.us-east-2.compute.internal:33073 with 2.6 GB RAM, BlockManagerId(1, ip-172-31-26-62.us-east-2.compute.internal, 33073, None)
18/03/24 21:38:37 INFO YarnSchedulerBackend$YarnDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.31.30.96:56588) with ID 2
18/03/24 21:38:37 INFO ExecutorAllocationManager: New executor 2 has registered (new total is 2)
18/03/24 21:38:37 INFO YarnClientSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.8
18/03/24 21:38:37 INFO BlockManagerMasterEndpoint: Registering block manager ip-172-31-30-96.us-east-2.compute.internal:37253 with 2.6 GB RAM, BlockManagerId(2, ip-172-31-30-96.us-east-2.compute.internal, 37253, None)
18/03/24 21:38:38 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 273.9 KB, free 3.1 GB)
18/03/24 21:38:38 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.3 KB, free 3.1 GB)
18/03/24 21:38:38 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.31.24.43:39457 (size: 23.3 KB, free: 3.1 GB)
18/03/24 21:38:38 INFO SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
18/03/24 21:38:38 INFO FileOutputCommitter: File Output Committer Algorithm version is 1
18/03/24 21:38:38 INFO FileOutputCommitter: FileOutputCommitter skip cleanup _temporary folders under output directory:false, ignore cleanup failures: false
18/03/24 21:38:38 INFO SparkContext: Starting job: saveAsTextFile at NativeMethodAccessorImpl.java:0
18/03/24 21:38:38 INFO GPLNativeCodeLoader: Loaded native gpl library
18/03/24 21:38:38 INFO LzoCodec: Successfully loaded & initialized native-lzo library [hadoop-lzo rev cfe28705e7dfdec92539cc7b24fc97936c259a05]
18/03/24 21:38:38 INFO FileInputFormat: Total input files to process : 20
18/03/24 21:38:38 INFO DAGScheduler: Registering RDD 3 (distinct at /home/hadoop/hw7/p1_q3.py:23)
18/03/24 21:38:38 INFO DAGScheduler: Registering RDD 7 (reduceByKey at /home/hadoop/hw7/p1_q3.py:27)
18/03/24 21:38:38 INFO DAGScheduler: Got job 0 (saveAsTextFile at NativeMethodAccessorImpl.java:0) with 1 output partitions
18/03/24 21:38:38 INFO DAGScheduler: Final stage: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0)
18/03/24 21:38:38 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 1)
18/03/24 21:38:38 INFO DAGScheduler: Missing parents: List(ShuffleMapStage 1)
18/03/24 21:38:38 INFO DAGScheduler: Submitting ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q3.py:23), which has no missing parents
18/03/24 21:38:38 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 9.4 KB, free 3.1 GB)
18/03/24 21:38:38 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.0 KB, free 3.1 GB)
18/03/24 21:38:38 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.31.24.43:39457 (size: 6.0 KB, free: 3.1 GB)
18/03/24 21:38:38 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1047
18/03/24 21:38:38 INFO DAGScheduler: Submitting 20 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /home/hadoop/hw7/p1_q3.py:23) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:38:38 INFO YarnScheduler: Adding task set 0.0 with 20 tasks
18/03/24 21:38:39 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 0, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 1, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 4.0 in stage 0.0 (TID 2, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 4, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 2.0 in stage 0.0 (TID 3, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 2, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 5.0 in stage 0.0 (TID 4, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 5, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 3.0 in stage 0.0 (TID 5, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 3, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 12.0 in stage 0.0 (TID 6, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 12, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO TaskSetManager: Starting task 6.0 in stage 0.0 (TID 7, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 6, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:39 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:37253 (size: 6.0 KB, free: 2.6 GB)
18/03/24 21:38:39 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33073 (size: 6.0 KB, free: 2.6 GB)
18/03/24 21:38:39 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33073 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:38:39 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:37253 (size: 23.3 KB, free: 2.6 GB)
18/03/24 21:38:40 INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged (new desired total will be 3)
18/03/24 21:38:41 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 5)
18/03/24 21:38:46 INFO TaskSetManager: Starting task 13.0 in stage 0.0 (TID 8, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 13, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:46 INFO TaskSetManager: Finished task 4.0 in stage 0.0 (TID 2) in 7587 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (1/20)
18/03/24 21:38:46 INFO TaskSetManager: Starting task 7.0 in stage 0.0 (TID 9, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 7, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:46 INFO TaskSetManager: Finished task 3.0 in stage 0.0 (TID 5) in 7712 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (2/20)
18/03/24 21:38:47 INFO TaskSetManager: Starting task 15.0 in stage 0.0 (TID 10, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 15, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:47 INFO TaskSetManager: Finished task 5.0 in stage 0.0 (TID 4) in 7975 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (3/20)
18/03/24 21:38:48 INFO TaskSetManager: Starting task 8.0 in stage 0.0 (TID 11, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 8, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:48 INFO TaskSetManager: Finished task 7.0 in stage 0.0 (TID 9) in 1681 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (4/20)
18/03/24 21:38:48 INFO TaskSetManager: Starting task 19.0 in stage 0.0 (TID 12, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 19, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:48 INFO TaskSetManager: Finished task 12.0 in stage 0.0 (TID 6) in 9685 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (5/20)
18/03/24 21:38:48 INFO TaskSetManager: Starting task 9.0 in stage 0.0 (TID 13, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 9, NODE_LOCAL, 4893 bytes)
18/03/24 21:38:48 INFO TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 9750 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (6/20)
18/03/24 21:38:50 INFO TaskSetManager: Finished task 13.0 in stage 0.0 (TID 8) in 4382 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (7/20)
18/03/24 21:38:51 INFO TaskSetManager: Starting task 10.0 in stage 0.0 (TID 14, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 10, RACK_LOCAL, 4893 bytes)
18/03/24 21:38:52 INFO TaskSetManager: Starting task 11.0 in stage 0.0 (TID 15, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 11, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:52 INFO TaskSetManager: Finished task 6.0 in stage 0.0 (TID 7) in 13553 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (8/20)
18/03/24 21:38:52 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 13640 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (9/20)
18/03/24 21:38:52 INFO TaskSetManager: Starting task 14.0 in stage 0.0 (TID 16, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 14, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:52 INFO TaskSetManager: Finished task 2.0 in stage 0.0 (TID 3) in 13913 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (10/20)
18/03/24 21:38:53 INFO TaskSetManager: Starting task 16.0 in stage 0.0 (TID 17, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 16, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:53 INFO TaskSetManager: Finished task 8.0 in stage 0.0 (TID 11) in 5450 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (11/20)
18/03/24 21:38:55 INFO TaskSetManager: Starting task 17.0 in stage 0.0 (TID 18, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 17, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:55 INFO TaskSetManager: Finished task 16.0 in stage 0.0 (TID 17) in 1309 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (12/20)
18/03/24 21:38:55 INFO TaskSetManager: Finished task 19.0 in stage 0.0 (TID 12) in 6752 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (13/20)
18/03/24 21:38:55 INFO TaskSetManager: Finished task 15.0 in stage 0.0 (TID 10) in 8737 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (14/20)
18/03/24 21:38:56 INFO TaskSetManager: Starting task 18.0 in stage 0.0 (TID 19, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 18, NODE_LOCAL, 4892 bytes)
18/03/24 21:38:56 INFO TaskSetManager: Finished task 11.0 in stage 0.0 (TID 15) in 3979 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (15/20)
18/03/24 21:38:56 INFO TaskSetManager: Finished task 10.0 in stage 0.0 (TID 14) in 4864 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (16/20)
18/03/24 21:38:58 INFO TaskSetManager: Finished task 9.0 in stage 0.0 (TID 13) in 9401 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (17/20)
18/03/24 21:38:58 INFO TaskSetManager: Finished task 14.0 in stage 0.0 (TID 16) in 5254 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (18/20)
18/03/24 21:38:59 INFO TaskSetManager: Finished task 17.0 in stage 0.0 (TID 18) in 4103 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (19/20)
18/03/24 21:39:00 INFO TaskSetManager: Finished task 18.0 in stage 0.0 (TID 19) in 4188 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (20/20)
18/03/24 21:39:00 INFO YarnScheduler: Removed TaskSet 0.0, whose tasks have all completed, from pool 
18/03/24 21:39:00 INFO DAGScheduler: ShuffleMapStage 0 (distinct at /home/hadoop/hw7/p1_q3.py:23) finished in 21.745 s
18/03/24 21:39:00 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:39:00 INFO DAGScheduler: running: Set()
18/03/24 21:39:00 INFO DAGScheduler: waiting: Set(ShuffleMapStage 1, ResultStage 2)
18/03/24 21:39:00 INFO DAGScheduler: failed: Set()
18/03/24 21:39:00 INFO DAGScheduler: Submitting ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q3.py:27), which has no missing parents
18/03/24 21:39:00 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 8.5 KB, free 3.1 GB)
18/03/24 21:39:00 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 5.4 KB, free 3.1 GB)
18/03/24 21:39:00 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on 172.31.24.43:39457 (size: 5.4 KB, free: 3.1 GB)
18/03/24 21:39:00 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1047
18/03/24 21:39:00 INFO DAGScheduler: Submitting 16 missing tasks from ShuffleMapStage 1 (PairwiseRDD[7] at reduceByKey at /home/hadoop/hw7/p1_q3.py:27) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
18/03/24 21:39:00 INFO YarnScheduler: Adding task set 1.0 with 16 tasks
18/03/24 21:39:00 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 20, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 0, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 21, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 1, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 22, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 2, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 23, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 3, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 24, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 4, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 25, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 5, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 26, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 6, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 27, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 7, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:00 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-26-62.us-east-2.compute.internal:33073 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:39:00 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:37253 (size: 5.4 KB, free: 2.6 GB)
18/03/24 21:39:00 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.30.96:56588
18/03/24 21:39:00 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 172.31.26.62:51310
18/03/24 21:39:00 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 1 is 255 bytes
18/03/24 21:39:01 INFO TaskSetManager: Starting task 8.0 in stage 1.0 (TID 28, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 8, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 5.0 in stage 1.0 (TID 25) in 854 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (1/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 9.0 in stage 1.0 (TID 29, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 9, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 6.0 in stage 1.0 (TID 26) in 863 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (2/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 10.0 in stage 1.0 (TID 30, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 10, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 2.0 in stage 1.0 (TID 22) in 907 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (3/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 11.0 in stage 1.0 (TID 31, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 11, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 23) in 937 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (4/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 12.0 in stage 1.0 (TID 32, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 12, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 7.0 in stage 1.0 (TID 27) in 943 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (5/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 13.0 in stage 1.0 (TID 33, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 13, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 4.0 in stage 1.0 (TID 24) in 972 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (6/16)
18/03/24 21:39:01 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 3)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 14.0 in stage 1.0 (TID 34, ip-172-31-26-62.us-east-2.compute.internal, executor 1, partition 14, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 1.0 in stage 1.0 (TID 21) in 1069 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (7/16)
18/03/24 21:39:01 INFO TaskSetManager: Starting task 15.0 in stage 1.0 (TID 35, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 15, NODE_LOCAL, 4621 bytes)
18/03/24 21:39:01 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 20) in 1116 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (8/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 10.0 in stage 1.0 (TID 30) in 624 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (9/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 8.0 in stage 1.0 (TID 28) in 679 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (10/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 12.0 in stage 1.0 (TID 32) in 604 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (11/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 11.0 in stage 1.0 (TID 31) in 644 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (12/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 14.0 in stage 1.0 (TID 34) in 519 ms on ip-172-31-26-62.us-east-2.compute.internal (executor 1) (13/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 9.0 in stage 1.0 (TID 29) in 765 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (14/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 13.0 in stage 1.0 (TID 33) in 675 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (15/16)
18/03/24 21:39:02 INFO TaskSetManager: Finished task 15.0 in stage 1.0 (TID 35) in 583 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (16/16)
18/03/24 21:39:02 INFO YarnScheduler: Removed TaskSet 1.0, whose tasks have all completed, from pool 
18/03/24 21:39:02 INFO DAGScheduler: ShuffleMapStage 1 (reduceByKey at /home/hadoop/hw7/p1_q3.py:27) finished in 1.699 s
18/03/24 21:39:02 INFO DAGScheduler: looking for newly runnable stages
18/03/24 21:39:02 INFO DAGScheduler: running: Set()
18/03/24 21:39:02 INFO DAGScheduler: waiting: Set(ResultStage 2)
18/03/24 21:39:02 INFO DAGScheduler: failed: Set()
18/03/24 21:39:02 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0), which has no missing parents
18/03/24 21:39:02 INFO MemoryStore: Block broadcast_3 stored as values in memory (estimated size 75.7 KB, free 3.1 GB)
18/03/24 21:39:02 INFO MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 29.1 KB, free 3.1 GB)
18/03/24 21:39:02 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on 172.31.24.43:39457 (size: 29.1 KB, free: 3.1 GB)
18/03/24 21:39:02 INFO SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1047
18/03/24 21:39:02 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[14] at saveAsTextFile at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
18/03/24 21:39:02 INFO YarnScheduler: Adding task set 2.0 with 1 tasks
18/03/24 21:39:02 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 36, ip-172-31-30-96.us-east-2.compute.internal, executor 2, partition 0, NODE_LOCAL, 5178 bytes)
18/03/24 21:39:02 INFO BlockManagerInfo: Added broadcast_3_piece0 in memory on ip-172-31-30-96.us-east-2.compute.internal:37253 (size: 29.1 KB, free: 2.6 GB)
18/03/24 21:39:02 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 0 to 172.31.30.96:56588
18/03/24 21:39:02 INFO MapOutputTrackerMaster: Size of output statuses for shuffle 0 is 268 bytes
18/03/24 21:39:04 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 36) in 1480 ms on ip-172-31-30-96.us-east-2.compute.internal (executor 2) (1/1)
18/03/24 21:39:04 INFO YarnScheduler: Removed TaskSet 2.0, whose tasks have all completed, from pool 
18/03/24 21:39:04 INFO DAGScheduler: ResultStage 2 (saveAsTextFile at NativeMethodAccessorImpl.java:0) finished in 1.474 s
18/03/24 21:39:04 INFO DAGScheduler: Job 0 finished: saveAsTextFile at NativeMethodAccessorImpl.java:0, took 25.342809 s
18/03/24 21:39:04 INFO SparkContext: Invoking stop() from shutdown hook
18/03/24 21:39:04 INFO SparkUI: Stopped Spark web UI at http://ip-172-31-24-43.us-east-2.compute.internal:4040
18/03/24 21:39:04 INFO YarnClientSchedulerBackend: Interrupting monitor thread
18/03/24 21:39:04 INFO YarnClientSchedulerBackend: Shutting down all executors
18/03/24 21:39:04 INFO YarnSchedulerBackend$YarnDriverEndpoint: Asking each executor to shut down
18/03/24 21:39:04 INFO SchedulerExtensionServices: Stopping SchedulerExtensionServices
(serviceOption=None,
 services=List(),
 started=false)
18/03/24 21:39:04 INFO YarnClientSchedulerBackend: Stopped
18/03/24 21:39:04 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
18/03/24 21:39:04 INFO MemoryStore: MemoryStore cleared
18/03/24 21:39:04 INFO BlockManager: BlockManager stopped
18/03/24 21:39:04 INFO BlockManagerMaster: BlockManagerMaster stopped
18/03/24 21:39:04 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
18/03/24 21:39:04 INFO SparkContext: Successfully stopped SparkContext
18/03/24 21:39:04 INFO ShutdownHookManager: Shutdown hook called
18/03/24 21:39:04 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-e22fd739-735e-43b4-891e-9c004f561ff7/pyspark-4a2d614b-1599-4adf-83bc-fcdbafb6a973
18/03/24 21:39:04 INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-e22fd739-735e-43b4-891e-9c004f561ff7
```

The job completed in 25 seconds and used three stages (a distinct operation, group by operation, and file saving operation). The DAG for the job:
![p1_q2 DAG](p1_q3_dag.png)

Furthermore, by exmining *stage 1* in the DAG we see that the RDD had 16 partitions, and that median shuffle read and write size is 2.0 MB and 134.8 KB, respectively.

The full set of logs from the Spark job are in *p1_q3_sparklogs.zip*.

We look at the job results:
```
$ hadoop fs -cat /output_p1_q3/part* | head
('2018-02-23T05 http://example.com/?url=4 User_0', 1)
('2018-03-03T04 http://example.com/?url=4 User_18', 1)
('2018-03-02T22 http://example.com/?url=3 User_29', 1)
('2018-03-02T04 http://example.com/?url=4 User_13', 1)
('2018-03-02T06 http://example.com/?url=0 User_20', 1)
('2018-02-25T22 http://example.com/?url=5 User_18', 1)
('2018-02-21T08 http://example.com/?url=1 User_6', 1)
('2018-03-03T16 http://example.com/?url=2 User_17', 1)
('2018-03-03T12 http://example.com/?url=6 User_26', 1)
('2018-02-21T13 http://example.com/?url=4 User_28', 1)

$ hadoop fs -cat /output_p1_q3/part* | tail
('2018-03-03T06 http://example.com/?url=2 User_1', 1)
('2018-02-20T17 http://example.com/?url=4 User_28', 1)
('2018-03-02T10 http://example.com/?url=1 User_30', 1)
('2018-03-03T10 http://example.com/?url=9 User_7', 1)
('2018-03-02T11 http://example.com/?url=7 User_3', 1)
('2018-02-22T02 http://example.com/?url=7 User_34', 1)
('2018-02-28T00 http://example.com/?url=7 User_34', 1)
('2018-03-03T22 http://example.com/?url=4 User_26', 1)
('2018-02-20T21 http://example.com/?url=9 User_22', 1)
('2018-02-22T09 http://example.com/?url=7 User_33', 1)
```


## Problem 2
The Spark program `p2_q1.py`:
```
```

## Problem 3
The Spark program `p3_groupbykey.py`:
```
```

The Spark program `p3_reducebykey.py`:
```
```

## Problem 4
The Spark program `p4.py`:
```
```

## Problem 5

**Task 1**
The Spark program `p5_t1.py`:
```
```

**Task 2**
The Spark program `p5_t2.py`:
```
```
