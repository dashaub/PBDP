---
title: Homework 6
author: David Shaub
geometry: margin=2cm
date: 2018-03-03
---

All problems were completed, including problem 5.

## Problem 1

Download Kafka:
```
$ wget http://apache.cs.utah.edu/kafka/1.0.0/kafka_2.12-1.0.0.tgz
$ tar xvf kafka_2.12-1.0.0.tgz
```

Launch Zookeper and Kafka:
```
$ ~/kafka_2.12-1.0.0/bin/zookeeper-server-start.sh ~/kafka_2.12-1.0.0/config/zookeeper.properties
[2018-03-02 18:59:27,513] INFO Reading configuration from: /home/vagrant/kafka_2.12-1.0.0/config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
[2018-03-02 18:59:27,515] INFO autopurge.snapRetainCount set to 3 (org.apache.zookeeper.server.DatadirCleanupManager)
[2018-03-02 18:59:27,515] INFO autopurge.purgeInterval set to 0 (org.apache.zookeeper.server.DatadirCleanupManager)
[2018-03-02 18:59:27,515] INFO Purge task is not scheduled. (org.apache.zookeeper.server.DatadirCleanupManager)
[2018-03-02 18:59:27,515] WARN Either no config or no quorum defined in config, running  in standalone mode (org.apache.zookeeper.server.quorum.QuorumPeerMain)
[2018-03-02 18:59:27,531] INFO Reading configuration from: /home/vagrant/kafka_2.12-1.0.0/config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
[2018-03-02 18:59:27,531] INFO Starting server (org.apache.zookeeper.server.ZooKeeperServerMain)
[2018-03-02 18:59:27,540] INFO Server environment:zookeeper.version=3.4.10-39d3a4f269333c922ed3db283be479f9deacaa0f, built on 03/23/2017 10:13 GMT (org.apache.zookeeper.server.ZooKeeperServer)
[2018-03-02 18:59:27,540] INFO Server environment:host.name=localhost (org.apache.zookeeper.server.ZooKeeperServer)
[2018-03-02 18:59:27,540] INFO Server environment:java.version=1.8.0_161 (org.apache.zookeeper.server.ZooKeeperServer)
[2018-03-02 18:59:27,540] INFO Server environment:java.vendor=Oracle Corporation (org.apache.zookeeper.server.ZooKeeperServer)
[2018-03-02 18:59:27,540] INFO Server environment:java.home=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.161-0.b14.el7_4.x86_64/jre (org.apache.zookeeper.server.ZooKeeperServer)

$ ~/kafka_2.12-1.0.0/bin/kafka-server-start.sh ~/kafka_2.12-1.0.0/config/server.properties
[2018-03-02 19:00:26,379] INFO KafkaConfig values: 
	advertised.host.name = null
	advertised.listeners = null
	advertised.port = null
	alter.config.policy.class.name = null
	authorizer.class.name = 
	auto.create.topics.enable = true
	auto.leader.rebalance.enable = true
	background.threads = 10
	broker.id = 0
	broker.id.generation.enable = true
	broker.rack = null
	compression.type = producer
	connections.max.idle.ms = 600000
	controlled.shutdown.enable = true
	controlled.shutdown.max.retries = 3
	controlled.shutdown.retry.backoff.ms = 5000
	controller.socket.timeout.ms = 30000
	create.topic.policy.class.name = null
	default.replication.factor = 1
	delete.records.purgatory.purge.interval.requests = 1
	delete.topic.enable = true
	fetch.purgatory.purge.interval.requests = 1000
	group.initial.rebalance.delay.ms = 0
	group.max.session.timeout.ms = 300000
	group.min.session.timeout.ms = 6000
	host.name = 
	inter.broker.listener.name = null
	inter.broker.protocol.version = 1.0-IV0
	leader.imbalance.check.interval.seconds = 300
	leader.imbalance.per.broker.percentage = 10
	listener.security.protocol.map = PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
	listeners = null
	log.cleaner.backoff.ms = 15000
	log.cleaner.dedupe.buffer.size = 134217728
	log.cleaner.delete.retention.ms = 86400000
	log.cleaner.enable = true
	log.cleaner.io.buffer.load.factor = 0.9
	log.cleaner.io.buffer.size = 524288
	log.cleaner.io.max.bytes.per.second = 1.7976931348623157E308
	log.cleaner.min.cleanable.ratio = 0.5
	log.cleaner.min.compaction.lag.ms = 0
	log.cleaner.threads = 1
	log.cleanup.policy = [delete]
	log.dir = /tmp/kafka-logs
	log.dirs = /tmp/kafka-logs
	log.flush.interval.messages = 9223372036854775807
	log.flush.interval.ms = null
	log.flush.offset.checkpoint.interval.ms = 60000
	log.flush.scheduler.interval.ms = 9223372036854775807
	log.flush.start.offset.checkpoint.interval.ms = 60000
	log.index.interval.bytes = 4096
	log.index.size.max.bytes = 10485760
	log.message.format.version = 1.0-IV0
	log.message.timestamp.difference.max.ms = 9223372036854775807
	log.message.timestamp.type = CreateTime
	log.preallocate = false
	log.retention.bytes = -1
	log.retention.check.interval.ms = 300000
	log.retention.hours = 168
	log.retention.minutes = null
	log.retention.ms = null
	log.roll.hours = 168
	log.roll.jitter.hours = 0
	log.roll.jitter.ms = null
	log.roll.ms = null
	log.segment.bytes = 1073741824
	log.segment.delete.delay.ms = 60000
	max.connections.per.ip = 2147483647
	max.connections.per.ip.overrides = 
	message.max.bytes = 1000012
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	min.insync.replicas = 1
	num.io.threads = 8
	num.network.threads = 3
	num.partitions = 1
	num.recovery.threads.per.data.dir = 1
	num.replica.fetchers = 1
	offset.metadata.max.bytes = 4096
	offsets.commit.required.acks = -1
	offsets.commit.timeout.ms = 5000
	offsets.load.buffer.size = 5242880
	offsets.retention.check.interval.ms = 600000
	offsets.retention.minutes = 1440
	offsets.topic.compression.codec = 0
	offsets.topic.num.partitions = 50
	offsets.topic.replication.factor = 1
	offsets.topic.segment.bytes = 104857600
	port = 9092
	principal.builder.class = null
	producer.purgatory.purge.interval.requests = 1000
	queued.max.request.bytes = -1
	queued.max.requests = 500
	quota.consumer.default = 9223372036854775807
	quota.producer.default = 9223372036854775807
	quota.window.num = 11
	quota.window.size.seconds = 1
	replica.fetch.backoff.ms = 1000
	replica.fetch.max.bytes = 1048576
	replica.fetch.min.bytes = 1
	replica.fetch.response.max.bytes = 10485760
	replica.fetch.wait.max.ms = 500
	replica.high.watermark.checkpoint.interval.ms = 5000
	replica.lag.time.max.ms = 10000
	replica.socket.receive.buffer.bytes = 65536
	replica.socket.timeout.ms = 30000
	replication.quota.window.num = 11
	replication.quota.window.size.seconds = 1
	request.timeout.ms = 30000
	reserved.broker.max.id = 1000
	sasl.enabled.mechanisms = [GSSAPI]
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.min.time.before.relogin = 60000
	sasl.kerberos.principal.to.local.rules = [DEFAULT]
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	sasl.kerberos.ticket.renew.window.factor = 0.8
	sasl.mechanism.inter.broker.protocol = GSSAPI
	security.inter.broker.protocol = PLAINTEXT
	socket.receive.buffer.bytes = 102400
	socket.request.max.bytes = 104857600
	socket.send.buffer.bytes = 102400
	ssl.cipher.suites = null
	ssl.client.auth = none
	ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	ssl.endpoint.identification.algorithm = null
	ssl.key.password = null
	ssl.keymanager.algorithm = SunX509
	ssl.keystore.location = null
	ssl.keystore.password = null
	ssl.keystore.type = JKS
	ssl.protocol = TLS
	ssl.provider = null
	ssl.secure.random.implementation = null
	ssl.trustmanager.algorithm = PKIX
	ssl.truststore.location = null
	ssl.truststore.password = null
	ssl.truststore.type = JKS
	transaction.abort.timed.out.transaction.cleanup.interval.ms = 60000
	transaction.max.timeout.ms = 900000
	transaction.remove.expired.transaction.cleanup.interval.ms = 3600000
	transaction.state.log.load.buffer.size = 5242880
	transaction.state.log.min.isr = 1
	transaction.state.log.num.partitions = 50
	transaction.state.log.replication.factor = 1
	transaction.state.log.segment.bytes = 104857600
	transactional.id.expiration.ms = 604800000
	unclean.leader.election.enable = false
	zookeeper.connect = localhost:2181
	zookeeper.connection.timeout.ms = 6000
	zookeeper.session.timeout.ms = 6000
	zookeeper.set.acl = false
	zookeeper.sync.time.ms = 2000
 (kafka.server.KafkaConfig)
[2018-03-02 19:00:26,449] INFO starting (kafka.server.KafkaServer)
[2018-03-02 19:00:26,453] INFO Connecting to zookeeper on localhost:2181 (kafka.server.KafkaServer)
[2018-03-02 19:00:26,473] INFO Starting ZkClient event thread. (org.I0Itec.zkclient.ZkEventThread)
[2018-03-02 19:00:26,480] INFO Client environment:zookeeper.version=3.4.10-39d3a4f269333c922ed3db283be479f9deacaa0f, built on 03/23/2017 10:13 GMT (org.apache.zookeeper.ZooKeeper)
[2018-03-02 19:00:26,480] INFO Client environment:host.name=localhost (org.apache.zookeeper.ZooKeeper)
[2018-03-02 19:00:26,480] INFO Client environment:java.version=1.8.0_161 (org.apache.zookeeper.ZooKeeper)
[2018-03-02 19:00:26,480] INFO Client environment:java.vendor=Oracle Corporation (org.apache.zookeeper.ZooKeeper)
[2018-03-02 19:00:26,480] INFO Client environment:java.home=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.161-0.b14.el7_4.x86_64/jre (org.apache.zookeeper.ZooKeeper)
```

Now create out topic and verify that it has been created:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic problem1
Created topic "problem1".
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --list --zookeeper localhost:2181
problem1
```

View the cluster and its state:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --describe --zookeeper localhost:2181
Topic:problem1	PartitionCount:2	ReplicationFactor:1	Configs:
	Topic: problem1	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
	Topic: problem1	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
```

We will launch two new terminal session for the producer and the consumer. For the producer we submit four messages:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic problem1
>hello
>world
>foo
>bar
```

In the consumer we view these messages
```
$ ~/kafka_2.12-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic problem1 --from-beginning
world
bar
hello
foo
```

We run `describe` again to look at the cluster's state:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --describe --zookeeper localhost:2181
Topic:__consumer_offsets	PartitionCount:50	ReplicationFactor:1	Configs:segment.bytes=104857600,cleanup.policy=compact,compression.type=producer
	Topic: __consumer_offsets	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 2	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 3	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 4	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 5	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 6	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 7	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 8	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 9	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 10	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 11	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 12	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 13	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 14	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 15	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 16	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 17	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 18	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 19	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 20	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 21	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 22	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 23	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 24	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 25	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 26	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 27	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 28	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 29	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 30	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 31	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 32	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 33	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 34	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 35	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 36	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 37	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 38	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 39	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 40	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 41	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 42	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 43	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 44	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 45	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 46	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 47	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 48	Leader: 0	Replicas: 0	Isr: 0
	Topic: __consumer_offsets	Partition: 49	Leader: 0	Replicas: 0	Isr: 0
Topic:problem1	PartitionCount:2	ReplicationFactor:1	Configs:
	Topic: problem1	Partition: 0	Leader: 0	Replicas: 0	Isr: 0
	Topic: problem1	Partition: 1	Leader: 0	Replicas: 0	Isr: 0
```
![Kafka cluster status](cluster_status.png)

Finally, we check the commit log for each of the two partitions:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-run-class.sh kafka.tools.DumpLogSegments  --transaction-log-decoder --files /tmp/kafka-logs/problem1-0/00000000000000000000.log
Dumping /tmp/kafka-logs/problem1-0/00000000000000000000.log
Starting offset: 0
baseOffset: 0 lastOffset: 0 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false position: 0 CreateTime: 1520017981884 isvalid: true size: 73 magic: 2 compresscodec: NONE crc: 1614140345
baseOffset: 1 lastOffset: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false position: 73 CreateTime: 1520017985299 isvalid: true size: 71 magic: 2 compresscodec: NONE crc: 3833554304

$ ~/kafka_2.12-1.0.0/bin/kafka-run-class.sh kafka.tools.DumpLogSegments  --transaction-log-decoder --files /tmp/kafka-logs/problem1-1/00000000000000000000.log
Dumping /tmp/kafka-logs/problem1-1/00000000000000000000.log
Starting offset: 0
baseOffset: 0 lastOffset: 0 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false position: 0 CreateTime: 1520017980068 isvalid: true size: 73 magic: 2 compresscodec: NONE crc: 1790533959
baseOffset: 1 lastOffset: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 0 isTransactional: false position: 73 CreateTime: 1520017983436 isvalid: true size: 71 magic: 2 compresscodec: NONE crc: 2860924450
```

## Problem 2

The Flume configuration in `p2.conf`:
```
a1.channels = ch-1
a1.channels.ch-1.type = memory
a1.channels.ch-1.capacity = 10000
a1.channels.ch-1.transactionCapacity = 100

a1.sources = src-1
a1.sources.src-1.type = exec
a1.sources.src-1.channels = ch-1
a1.sources.src-1.command = sudo tail -F /var/log/httpd/access_log

a1.sinks = k1
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.channel = ch-1
a1.sinks.k1.kafka.topic = problem2
a1.sinks.k1.kafka.bootstrap.servers = localhost:9092
```
We create our topic:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic problem2
Created topic "problem2".
```

Launch the Flume agent and our generator script to create a modest 10 mps:
```
$ ./generator.sh 10
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p2.conf --name a1 -Dflume.root.logger=INFO,console
``

We'll launch a console consumer to watch the messages passed from Flume:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic problem2 --from-beginning
```


We modify our configuration include an interceptor that adds a UUID in `p2_interceptor.conf`:
```
a1.channels = ch-1
a1.channels.ch-1.type = memory
a1.channels.ch-1.capacity = 10000
a1.channels.ch-1.transactionCapacity = 100

a1.sources = src-1
a1.sources.src-1.type = exec
a1.sources.src-1.channels = ch-1
a1.sources.src-1.command = sudo tail -F /var/log/httpd/access_log
a1.sources.src-1.interceptors = i1
a1.sources.src-1.interceptors.i1.type = org.apache.flume.sink.solr.morphline.UUIDInterceptor$Builder

a1.sinks = k1
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.channel = ch-1
a1.sinks.k1.kafka.topic = problem2
a1.sinks.k1.kafka.bootstrap.servers = localhost:9092
```

TODO: verify UUID appear in record headers

## Problem 3

## Problem 4

