---
title: Homework 6
author: David Shaub
geometry: margin=2cm
date: 2018-03-03
---

All problems were completed, including problems A and B.

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
$ ./generator.sh --rate 10
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p2.conf --name a1 -Dflume.root.logger=INFO,console
```

We'll launch a console consumer to watch the messages passed from Flume:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic problem2 --from-beginning
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [02/Mar/2018:15:16:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:01 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:02 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:03 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
```

The we compare the timestamp of the last events in our Kafka consumer with the last events that appear in our access log and see that they match:
```
$ sudo tail -f /var/log/httpd/access_log
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:04 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
::1 - - [03/Mar/2018:09:07:05 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"
```

Now to utilize a UUID with the kafka record headers we modify our Flume configuration to include an interceptor that adds a UUID in `p2_interceptor.conf`:
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
a1.sources.src-1.interceptors.i1.headerName = key

a1.sinks = k1
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.channel = ch-1
a1.sinks.k1.kafka.topic = problem2
a1.sinks.k1.kafka.bootstrap.servers = localhost:9092
```

Launch the Flume agent using this configuration:
```
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p2_interceptor.conf --name a1 -Dflume.root.logger=INFO,console
```

To view the messages and ensure the UUID is being passed, we'll create a simple consumer `p2_consumer.py`:
```
"""
A Kafka consumer that consumes from the problem2 topic.
"""

from kafka import KafkaConsumer


consumer = KafkaConsumer('problem2')

for msg in consumer:
    print(msg)
```

Launch the consumer and view the messages appearing:
```
$ python p2_consumer.py
ConsumerRecord(topic=u'problem2', partition=0, offset=927, timestamp=-1, timestamp_type=0, key='738d8202-dd5f-4d98-b54c-14741bd657dc', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=928, timestamp=-1, timestamp_type=0, key='dc04efe0-dcd7-4e24-afda-5a73679ed7e5', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=929, timestamp=-1, timestamp_type=0, key='63d23367-ee24-4de1-b279-9df89404c4d7', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=930, timestamp=-1, timestamp_type=0, key='c8c8a988-22d3-4bc6-8c51-027cf3606ddb', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=931, timestamp=-1, timestamp_type=0, key='1179b902-0f58-430f-95b9-b87e39591d2f', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=932, timestamp=-1, timestamp_type=0, key='935a838a-f6a3-4bf8-82e1-fbb1336acde0', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=933, timestamp=-1, timestamp_type=0, key='7d8aaeb6-ee15-460d-9b66-2b1fddca818d', value='::1 - - [04/Mar/2018:21:11:07 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
ConsumerRecord(topic=u'problem2', partition=0, offset=934, timestamp=-1, timestamp_type=0, key='0b00271c-ea88-472c-bb14-60ff888301ad', value='::1 - - [04/Mar/2018:21:11:08 +0000] "GET / HTTP/1.1" 200 85 "-" "curl/7.29.0"', checksum=None, serialized_key_size=36, serialized_value_size=78)
```
We see the UUID appearing in the `key` field.


## Problem 3

To produce messages, we will run `p3_producer.py`:
```
"""
A Kafka producer that generates events at a specified rate with a timestamp, user, and URL.
"""
import argparse
import datetime
import hashlib
import random
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--rate', type=float, default=1,
                    help='Number of events per second to generate')
parser.add_argument('--seed', type=int, default=12345,
                    help='See to use for reproducibility')
args = parser.parse_args()
sleep_time = 1 / args.rate
seed = args.seed

users = ['foo', 'bar', 'baz']
urls = ['https://en.wikipedia.org/wiki/Apache_Flume',
        'https://en.wikipedia.org/wiki/Main_Page',
        'https://en.wikipedia.org/wiki/Apache_Kafka']

producer = KafkaProducer(bootstrap_servers='localhost:9092')
random.seed(seed)
while True:
    # Select a random user and URL
    user = random.choice(users)
    url = random.choice(urls)
    # Get the current time in UTC
    current_time = str(datetime.datetime.utcnow())
    # Generate a UUID for the event
    identifier = str([current_time, url, user]).encode('utf-8')
    uuid = hashlib.md5(identifier).hexdigest()
    # Build final message and send to the Kafka cluster
    message = '{}\t{}\t{}\t{}'.format(uuid, current_time, url, user)
    producer.send('problem3', message)
    time.sleep(sleep_time)

```

Create out topic with 3 partitions and launch our Python producer:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic problem3
Created topic "problem3".
$ python p3_producer.py 
```

And then we see the messages appear in our Kafka console consumer:
```
$ ~/kafka_2.12-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic problem3 --from-beginning
af5c05a04b473e16ceffff0e8effc4bf	2018-03-03 19:11:13.831716	https://en.wikipedia.org/wiki/Apache_Flume	bar
cf3ed4ec135000c7e6a986f9d6b251a0	2018-03-03 19:11:14.834083	https://en.wikipedia.org/wiki/Main_Page	foo
82caef1d9d22a0af965d82c70d62c9d6	2018-03-03 19:11:15.834735	https://en.wikipedia.org/wiki/Apache_Flume	bar
b196992ef248e94de3fb8ff3af0bef3e	2018-03-03 19:11:16.837919	https://en.wikipedia.org/wiki/Main_Page	bar
8cf5636b706a85246febc367d5c667b6	2018-03-03 19:11:17.850812	https://en.wikipedia.org/wiki/Main_Page	bar
b500b7fea3f52a5b894bda13ed426466	2018-03-03 19:11:18.851872	https://en.wikipedia.org/wiki/Apache_Kafka	foo
823a0957e63b47a007f5793570219dce	2018-03-03 19:11:19.859302	https://en.wikipedia.org/wiki/Apache_Flume	bar
9405349f3b06faa9761d118cba1e533a	2018-03-03 19:11:20.864085	https://en.wikipedia.org/wiki/Main_Page	foo
3c3b647bac90ff5c39d4a1879f670d05	2018-03-03 19:11:21.869171	https://en.wikipedia.org/wiki/Apache_Kafka	bar
268a419180c043ad4f840789932fa4e1	2018-03-03 19:11:22.871213	https://en.wikipedia.org/wiki/Apache_Flume	baz
31ecde0365edc556ea7c3ff5403d3e76	2018-03-03 19:11:23.874269	https://en.wikipedia.org/wiki/Apache_Kafka	baz
c82c35a9471ecf07b9907d20fb7f7691	2018-03-03 19:11:24.879059	https://en.wikipedia.org/wiki/Main_Page	foo
b4022af71efbe643cf7fb494d9b16ec4	2018-03-03 19:11:25.880389	https://en.wikipedia.org/wiki/Apache_Kafka	bar
6cd9df7d0ebb504a244c828cb00353b8	2018-03-03 19:11:26.884465	https://en.wikipedia.org/wiki/Apache_Kafka	bar
bc12afae1a425def3f83904a1f84e5e7	2018-03-03 19:11:27.885213	https://en.wikipedia.org/wiki/Apache_Flume	baz
21af41fd5e08d3d8b19288f4e5ded1a8	2018-03-03 19:11:28.886150	https://en.wikipedia.org/wiki/Apache_Kafka	foo
```

Now we hook up a Python consumer `p3_consumer.py` to read the data:
```
"""
A Kafka consumer that consumes from the problem3 topic.
"""

from kafka import KafkaConsumer

def print_distribution(data):
    """
    Print a summary of the frequency of each partition encounter. This will show the balance of
    load across the partitions
    :param data: A list containing the partition numbers
    """
    unique_partitions = list(set(data))
    num_elements = len(data)
    for partition in unique_partitions:
        partition_count = data.count(partition)
        print('Partition {}: {}'.format(partition, partition_count))


consumer = KafkaConsumer('problem3')
count = 0
# Keep track of which partitions 
partitions = []
for msg in consumer:
    print(msg)
    count += 1
    partitions.append(msg.partition)
    # Print a distribution of the partitions every 1000 events
    if not count % 1000:
        print_distribution(partitions)
```


To demonstrate that the load distribution is split evenly across the partitions, after 1000 messages it will also print out a summary of the load across the partitions. So that we do not need to wait very long to reach 1000 messages, our producer was launcher with a faster `--rate 50` argument. The last few events before the partition statistics are included below:
```
$ python p3_consumer.py 
ConsumerRecord(topic=u'problem3', partition=1, offset=745, timestamp=1520114161551, timestamp_type=0, key=None, value='e4e0650557a9060e98a39525637b0733\t2018-03-03 21:56:01.550845\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=899, timestamp=1520114161756, timestamp_type=0, key=None, value='005057a2e5354080c860d4ccb9928a6a\t2018-03-03 21:56:01.755995\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=746, timestamp=1520114161573, timestamp_type=0, key=None, value='b42ce2e616ca368d762429caa8312e6c\t2018-03-03 21:56:01.572880\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=747, timestamp=1520114161602, timestamp_type=0, key=None, value='852adc4d33d5b51ecc2375979e8ce0a5\t2018-03-03 21:56:01.602628\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=748, timestamp=1520114161677, timestamp_type=0, key=None, value='a2dbf13ffc20f8d434696956a231d885\t2018-03-03 21:56:01.677720\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=749, timestamp=1520114161700, timestamp_type=0, key=None, value='55e35718f75671f8c65ab8db65293269\t2018-03-03 21:56:01.699985\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=757, timestamp=1520114161631, timestamp_type=0, key=None, value='9e19b07d922efa44ab7084eeb36181b7\t2018-03-03 21:56:01.630908\thttps://en.wikipedia.org/wiki/Main_Page\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=758, timestamp=1520114161654, timestamp_type=0, key=None, value='0f5d95cddf150fe213d5f0c705896718\t2018-03-03 21:56:01.654250\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=759, timestamp=1520114161729, timestamp_type=0, key=None, value='eb9b2497d7e2bcc2c3ffecbd2c747221\t2018-03-03 21:56:01.728838\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=900, timestamp=1520114161786, timestamp_type=0, key=None, value='06879b5ddda9cee3a35105968b758f61\t2018-03-03 21:56:01.786486\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=750, timestamp=1520114161815, timestamp_type=0, key=None, value='fb96cf64a3d95420e6e351402adaf6d3\t2018-03-03 21:56:01.815605\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=901, timestamp=1520114161915, timestamp_type=0, key=None, value='d81bb8b120598b0f555a5fd07d922bd4\t2018-03-03 21:56:01.915159\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
Partition 0: 365
Partition 1: 318
Partition 2: 317
```

We can see the offsets, partitions, the headers, and the body of the events. The last three lines show that each partition receives approximately one third of the messages.

## Problem 4

To place the consumers into a consumer group, we use a very simple consumer called `p4_consumer.py`:
```
"""
A Kafka consumer that consumes from the problem3 topic in test_consumer_group.
"""

from kafka import KafkaConsumer


consumer = KafkaConsumer('problem3', group_id='test_consumer_group')

for msg in consumer:
    print(msg)
```

We launch two instances that we will name **consumer_A** and **consumer_B**:
```
$ python p4_consumer.py 
```

And then launch our producer:
```
$ python p3_producer.py --rate 10
```

We see that consumer_A is only reading from partition 2:
```
ConsumerRecord(topic=u'problem3', partition=2, offset=2107, timestamp=1520387031129, timestamp_type=0, key=None, value='9a353c1365f53935e54a7e9951e873b1\t2018-03-07 01:43:51.128774\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2108, timestamp=1520387031231, timestamp_type=0, key=None, value='06621b5e7676c39ce50202bd0568aa56\t2018-03-07 01:43:51.230916\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2109, timestamp=1520387031432, timestamp_type=0, key=None, value='99ddee2d93d2c3fbef6f90835cf5d2ea\t2018-03-07 01:43:51.431906\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2110, timestamp=1520387031634, timestamp_type=0, key=None, value='d0d41131e8196094766fc3dc06a00472\t2018-03-07 01:43:51.633669\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2111, timestamp=1520387031734, timestamp_type=0, key=None, value='5a438ef0763244c3787d58bc35a8533c\t2018-03-07 01:43:51.734340\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2112, timestamp=1520387032340, timestamp_type=0, key=None, value='751c691254aacd087b1dbff9d17f23ff\t2018-03-07 01:43:52.340546\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2113, timestamp=1520387032441, timestamp_type=0, key=None, value='ff5c710ec5a0fe53b233097840a44058\t2018-03-07 01:43:52.441036\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2114, timestamp=1520387032853, timestamp_type=0, key=None, value='38be97299c0234c47b9fb19fa8d1e6d8\t2018-03-07 01:43:52.853156\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2115, timestamp=1520387032955, timestamp_type=0, key=None, value='49f6d3447d24d0a92cdfc94205302471\t2018-03-07 01:43:52.955405\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2116, timestamp=1520387033467, timestamp_type=0, key=None, value='3972f39a90b9ca3e74386ad22c88a7bd\t2018-03-07 01:43:53.467115\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
```

But consumer_B is reading from partitions 0 and 1:
```
ord(topic=u'problem3', partition=0, offset=2324, timestamp=1520387032746, timestamp_type=0, key=None, value='e03a7bfacd00d47074381700ce3f57c1\t2018-03-07 01:43:52.746566\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2325, timestamp=1520387033058, timestamp_type=0, key=None, value='fec044e2e819436342c8bd21a767a8ac\t2018-03-07 01:43:53.057810\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2061, timestamp=1520387033265, timestamp_type=0, key=None, value='85d6c89212de3f03604d4da9c76c5046\t2018-03-07 01:43:53.265377\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2326, timestamp=1520387033163, timestamp_type=0, key=None, value='7f294ec6d4ad0a051b1993af2691dc45\t2018-03-07 01:43:53.163190\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2062, timestamp=1520387033366, timestamp_type=0, key=None, value='6cbf8753841a6aacaf79be46edf438a4\t2018-03-07 01:43:53.366328\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2327, timestamp=1520387033770, timestamp_type=0, key=None, value='e6d859e9909dcbe0bcf04a32ab186da8\t2018-03-07 01:43:53.770242\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2063, timestamp=1520387033568, timestamp_type=0, key=None, value='7b83131872af7b7cde32d44427b76402\t2018-03-07 01:43:53.568293\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2064, timestamp=1520387033669, timestamp_type=0, key=None, value='0887fbbe55e58d722c0965aa30475263\t2018-03-07 01:43:53.669349\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2328, timestamp=1520387033871, timestamp_type=0, key=None, value='707fb12a1e381461b18a73f6d693b50c\t2018-03-07 01:43:53.871172\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
```

Now we launch two more consumers (**consumer_C** and **consumer_D**) and see how the partitions are balanced. Consumer_A only receives from partition 1 now:
```
ConsumerRecord(topic=u'problem3', partition=1, offset=2092, timestamp=1520387219154, timestamp_type=0, key=None, value='604e620d8b1f44446a79bf6c83f27321\t2018-03-07 01:46:59.154013\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2093, timestamp=1520387219860, timestamp_type=0, key=None, value='79d9bdc1b872049299920011926730c1\t2018-03-07 01:46:59.860554\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2094, timestamp=1520387220264, timestamp_type=0, key=None, value='f45a32aa1eb8fd990fc9b9a5081ab8f2\t2018-03-07 01:47:00.264602\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2095, timestamp=1520387220365, timestamp_type=0, key=None, value='0d043a85a6e1748f7239688b077a03e4\t2018-03-07 01:47:00.365390\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2096, timestamp=1520387220466, timestamp_type=0, key=None, value='ebac12c19d39878cebaa92300454021f\t2018-03-07 01:47:00.466186\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2097, timestamp=1520387220769, timestamp_type=0, key=None, value='28422e6723c9427f0dbd6269d135da87\t2018-03-07 01:47:00.769480\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2098, timestamp=1520387220973, timestamp_type=0, key=None, value='041ad9467959f3c1456518641e192be9\t2018-03-07 01:47:00.973109\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2099, timestamp=1520387221177, timestamp_type=0, key=None, value='1fbe0b65ff2e585ba7dae71ab1566f64\t2018-03-07 01:47:01.176975\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
```

Consumer_B receives from only partition 0:
```
ConsumerRecord(topic=u'problem3', partition=0, offset=2365, timestamp=1520387218339, timestamp_type=0, key=None, value='4eaaafff6dbaec701fbd4028f413e4d7\t2018-03-07 01:46:58.339178\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2366, timestamp=1520387218951, timestamp_type=0, key=None, value='77699c974812917fcddfda4157b509a1\t2018-03-07 01:46:58.950845\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2367, timestamp=1520387219052, timestamp_type=0, key=None, value='05ea3a845b7858349c8b573b62be032b\t2018-03-07 01:46:59.052192\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2368, timestamp=1520387219356, timestamp_type=0, key=None, value='2b44ffe0e6dacc24f2de6b3a0aca5cb9\t2018-03-07 01:46:59.355788\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2369, timestamp=1520387219457, timestamp_type=0, key=None, value='1bff58788ed860e7e6a113f2a373ec24\t2018-03-07 01:46:59.456731\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2370, timestamp=1520387220164, timestamp_type=0, key=None, value='d381a9f8b37adad354d0519a87fb80d0\t2018-03-07 01:47:00.164041\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2371, timestamp=1520387220567, timestamp_type=0, key=None, value='d2712e33121d406e9b99f5b8e6097bca\t2018-03-07 01:47:00.566861\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2372, timestamp=1520387220870, timestamp_type=0, key=None, value='b1ba3ce6c72cab81d75a53a1cd5d4fd2\t2018-03-07 01:47:00.870233\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2373, timestamp=1520387221278, timestamp_type=0, key=None, value='aa26ebf98c445c0b27b0152ad4e1c26f\t2018-03-07 01:47:01.278096\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2374, timestamp=1520387221479, timestamp_type=0, key=None, value='e8cea37a462fb7bb0c5fab7850c09f3b\t2018-03-07 01:47:01.479075\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
```

Consumer_C receives no data: this happens since we have more consumers in the consumer group than partitions in the Kafka topic). Consumer_D reads the final partition 2:
```
ConsumerRecord(topic=u'problem3', partition=2, offset=2148, timestamp=1520387219558, timestamp_type=0, key=None, value='970a050c6ec4e420ea76f458cad02bb1\t2018-03-07 01:46:59.558728\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2149, timestamp=1520387219659, timestamp_type=0, key=None, value='0b019ca6e85bc28ca6498a15de1461e8\t2018-03-07 01:46:59.659190\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2150, timestamp=1520387219759, timestamp_type=0, key=None, value='71921674c46da36cc0b8829e286b73d8\t2018-03-07 01:46:59.759763\thttps://en.wikipedia.org/wiki/Main_Page\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2151, timestamp=1520387219961, timestamp_type=0, key=None, value='63697f4d9e2f03bd43734a5e7e36f3c5\t2018-03-07 01:46:59.961145\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2152, timestamp=1520387220063, timestamp_type=0, key=None, value='db80ba3de28c6ea4c166cc2a21e6f6d0\t2018-03-07 01:47:00.063523\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2153, timestamp=1520387220669, timestamp_type=0, key=None, value='e4ff1a956cbcf90ced1bc1489c8ceb79\t2018-03-07 01:47:00.668844\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2154, timestamp=1520387221076, timestamp_type=0, key=None, value='e6f5b209a9b602b6d220099fc260b3ad\t2018-03-07 01:47:01.076201\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2155, timestamp=1520387221378, timestamp_type=0, key=None, value='3a2db664d8179f61da740a9ab6881b49\t2018-03-07 01:47:01.378682\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2156, timestamp=1520387221580, timestamp_type=0, key=None, value='118d07ef58a5c04ac637caa1a33a210d\t2018-03-07 01:47:01.580604\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
```

We bring down consumer_A and consumer_B. We expect consumer_C to now receive data, and the load should be shared with consumer_D. Indeed consumer_C is now reading from partition 2:
```
ConsumerRecord(topic=u'problem3', partition=2, offset=2224, timestamp=1520387539921, timestamp_type=0, key=None, value='0ac26e79464e35d3d44fe76b958cf9d7\t2018-03-07 01:52:19.920831\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2225, timestamp=1520387540533, timestamp_type=0, key=None, value='63da56104ecea4ca348dc10326f64057\t2018-03-07 01:52:20.532983\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2226, timestamp=1520387540633, timestamp_type=0, key=None, value='445f363fd24dbce3d68404fad5ffa58c\t2018-03-07 01:52:20.633561\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2227, timestamp=1520387541039, timestamp_type=0, key=None, value='5dca6c889c587dd860a04f111691fcb5\t2018-03-07 01:52:21.038865\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2228, timestamp=1520387541141, timestamp_type=0, key=None, value='e6a12dcfea0e41f57e866958d2562d3c\t2018-03-07 01:52:21.141058\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2229, timestamp=1520387541654, timestamp_type=0, key=None, value='b8f01b4e0727cf3851d0df130f4bfd53\t2018-03-07 01:52:21.654597\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2230, timestamp=1520387542675, timestamp_type=0, key=None, value='d1ed12dcfacc869cac30f1caf0077d74\t2018-03-07 01:52:22.675267\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2231, timestamp=1520387543084, timestamp_type=0, key=None, value='20396304e9871139a858f396a9f0cb88\t2018-03-07 01:52:23.083935\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2232, timestamp=1520387543795, timestamp_type=0, key=None, value='18f7ce54b4c8ee7461095f40bef40231\t2018-03-07 01:52:23.795428\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
```

And consumer_D is reading from partitions 0 and 1:
```
ConsumerRecord(topic=u'problem3', partition=1, offset=2207, timestamp=1520387613398, timestamp_type=0, key=None, value='d1a5e058fb38b5422194026b60321acf\t2018-03-07 01:53:33.397658\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2508, timestamp=1520387613499, timestamp_type=0, key=None, value='33925139e9d39bf4729b266152ad37b3\t2018-03-07 01:53:33.499178\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2509, timestamp=1520387613806, timestamp_type=0, key=None, value='dad870f344e1b8bc082aa11fc4c8ba3e\t2018-03-07 01:53:33.806423\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2510, timestamp=1520387613912, timestamp_type=0, key=None, value='50967b76529c7f579556bd2f374a0e7e\t2018-03-07 01:53:33.911778\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2208, timestamp=1520387614013, timestamp_type=0, key=None, value='9c64b918ba2986d72c48f49e4d07ba68\t2018-03-07 01:53:34.012966\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=1, offset=2209, timestamp=1520387614423, timestamp_type=0, key=None, value='a0e2b177a9928f25b5d3436adae1b62d\t2018-03-07 01:53:34.423220\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2511, timestamp=1520387614524, timestamp_type=0, key=None, value='74696bf2331f9be4e7f499d2dd5916af\t2018-03-07 01:53:34.523888\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=-1, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=1, offset=2210, timestamp=1520387614725, timestamp_type=0, key=None, value='b1a419e8840848d66a9366c7878781fa\t2018-03-07 01:53:34.725012\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=-1, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2512, timestamp=1520387614624, timestamp_type=0, key=None, value='b1d29dfb2b435cb0f745c5ae47005439\t2018-03-07 01:53:34.624302\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=-1, serialized_value_size=106)
```

## Problem A


To send each userID to the same partition, our strategy will be to produce produce a hash and send this to Kafka as the key. This producer is implemented in `pa_producer.py`:
```
"""
A Kafka producer that generates events at a specified rate with a timestamp, user, and URL.
"""
import argparse
import datetime
import hashlib
import random
import time
import uuid

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--rate', type=float, default=1,
                    help='Number of events per second to generate')
parser.add_argument('--seed', type=int, default=12345,
                    help='See to use for reproducibility')
args = parser.parse_args()
sleep_time = 1 / args.rate
seed = args.seed

users = ['foo', 'bar', 'baz', 'foobar']
urls = ['https://en.wikipedia.org/wiki/Apache_Flume',
        'https://en.wikipedia.org/wiki/Main_Page',
        'https://en.wikipedia.org/wiki/Apache_Kafka']

producer = KafkaProducer(bootstrap_servers='localhost:9092')
random.seed(seed)
num_partitions = 3
while True:
    # Select a random user and URL
    user = random.choice(users)
    # Determine the partition
    partition = hashlib.md5(user).hexdigest()
    url = random.choice(urls)
    # Get the current time in UTC
    current_time = str(datetime.datetime.utcnow())
    # Generate a UUID for the event
    identifier = str([current_time, url, user]).encode('utf-8')
    uuid = hashlib.md5(identifier).hexdigest()
    # Build final message and send to the Kafka cluster
    message = '{}\t{}\t{}\t{}'.format(uuid, current_time, url, user)
    producer.send('problem3', key=partition, value=message)
    time.sleep(sleep_time)
```

We run our consumer with `$ python p3_consumer.py` and see which partitions receive which users:
```
ConsumerRecord(topic=u'problem3', partition=2, offset=2290, timestamp=1520390855015, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='0af73f1bb20a12103c7c5b66e70de1e7\t2018-03-07 02:47:35.014976\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2291, timestamp=1520390855217, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='623e11b89f9610a55bc1f894a2ba02ed\t2018-03-07 02:47:35.217182\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2585, timestamp=1520390854912, timestamp_type=0, key='73feffa4b7f6bb68e44cf984c85f6e88', value='016909023ac1751cfd0e13358c9af124\t2018-03-07 02:47:34.912745\thttps://en.wikipedia.org/wiki/Main_Page\tbaz', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2586, timestamp=1520390855115, timestamp_type=0, key='3858f62230ac3c915f300c664312c63f', value='80021c2ec7d54a56a0a2f0e477eb9650\t2018-03-07 02:47:35.115625\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoobar', checksum=None, serialized_key_size=32, serialized_value_size=109)
ConsumerRecord(topic=u'problem3', partition=0, offset=2587, timestamp=1520390855321, timestamp_type=0, key='73feffa4b7f6bb68e44cf984c85f6e88', value='79fd3800275a22d95de0b1d15fbd654f\t2018-03-07 02:47:35.321011\thttps://en.wikipedia.org/wiki/Main_Page\tbaz', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2292, timestamp=1520390855422, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='5bb3de301ead45ec47b53888c72fc771\t2018-03-07 02:47:35.422713\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2588, timestamp=1520390855524, timestamp_type=0, key='37b51d194a7513e45b56f6524f2d51f2', value='afd4ffc46fc877aca67c0439cdbc585c\t2018-03-07 02:47:35.523887\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2293, timestamp=1520390855728, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='27c6f4e1d0dd20194c9def0b0cefd96c\t2018-03-07 02:47:35.728132\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2589, timestamp=1520390855624, timestamp_type=0, key='73feffa4b7f6bb68e44cf984c85f6e88', value='19450aff9b5f39f32e9236156f6e11a8\t2018-03-07 02:47:35.624531\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbaz', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2294, timestamp=1520390856038, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='6692cc3d514a40d902bfc5b0084b9d9d\t2018-03-07 02:47:36.038199\thttps://en.wikipedia.org/wiki/Apache_Kafka\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2590, timestamp=1520390855831, timestamp_type=0, key='37b51d194a7513e45b56f6524f2d51f2', value='de51fb8f78a79d5c7f214e9b04e443e2\t2018-03-07 02:47:35.830701\thttps://en.wikipedia.org/wiki/Apache_Kafka\tbar', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2591, timestamp=1520390855936, timestamp_type=0, key='3858f62230ac3c915f300c664312c63f', value='5ffdb3130f045582ca2628f4d90d7045\t2018-03-07 02:47:35.935715\thttps://en.wikipedia.org/wiki/Main_Page\tfoobar', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2295, timestamp=1520390856141, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='34f8735696928899a3ab4932bc71a24a\t2018-03-07 02:47:36.140657\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2592, timestamp=1520390856241, timestamp_type=0, key='37b51d194a7513e45b56f6524f2d51f2', value='14e8e30db970a1e016a3f7fdb2190ed3\t2018-03-07 02:47:36.241508\thttps://en.wikipedia.org/wiki/Apache_Flume\tbar', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2296, timestamp=1520390856446, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='08519ecda9ee499939cebedb9baba25c\t2018-03-07 02:47:36.445986\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2593, timestamp=1520390856345, timestamp_type=0, key='73feffa4b7f6bb68e44cf984c85f6e88', value='b66d0ab6a84e5b035f5affab118d2704\t2018-03-07 02:47:36.344821\thttps://en.wikipedia.org/wiki/Main_Page\tbaz', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=2, offset=2297, timestamp=1520390856649, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='b639ff18365e701082fec0db7fa8d9b1\t2018-03-07 02:47:36.649367\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2594, timestamp=1520390856547, timestamp_type=0, key='73feffa4b7f6bb68e44cf984c85f6e88', value='e186eb718db9ee2f8888388c42984000\t2018-03-07 02:47:36.547576\thttps://en.wikipedia.org/wiki/Apache_Flume\tbaz', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=2, offset=2298, timestamp=1520390856750, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='f3b7839b38ff4e23ae9dc2f65845d08f\t2018-03-07 02:47:36.750149\thttps://en.wikipedia.org/wiki/Main_Page\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=103)
ConsumerRecord(topic=u'problem3', partition=0, offset=2595, timestamp=1520390856957, timestamp_type=0, key='3858f62230ac3c915f300c664312c63f', value='97353476d60e5a144fa5b2b10ad43f0f\t2018-03-07 02:47:36.957015\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoobar', checksum=None, serialized_key_size=32, serialized_value_size=109)
ConsumerRecord(topic=u'problem3', partition=2, offset=2299, timestamp=1520390856850, timestamp_type=0, key='acbd18db4cc2f85cedef654fccc4a4d8', value='c466c5c7bde8db8871db5d83d02a1303\t2018-03-07 02:47:36.850581\thttps://en.wikipedia.org/wiki/Apache_Flume\tfoo', checksum=None, serialized_key_size=32, serialized_value_size=106)
ConsumerRecord(topic=u'problem3', partition=0, offset=2596, timestamp=1520390857059, timestamp_type=0, key='37b51d194a7513e45b56f6524f2d51f2', value='e4051891282b85d11f97fe6e7b23b91e\t2018-03-07 02:47:37.058829\thttps://en.wikipedia.org/wiki/Main_Page\tbar', checksum=None, serialized_key_size=32, serialized_value_size=103)
```

We see that `bar` always appears in partition 0, `foo` in partition 2, `foobar` in partition 0, and and `baz` in partition 0. If we were to have a large number of users, we'd expect the users to be fairly balanced between the three partitions.
