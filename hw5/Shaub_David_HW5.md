---
title: Homework 5
author: David Shaub
geometry: margin=2cm
date: 2018-02-24
---

All problems were completed, including problem 5.

## Problem 1

We download Flume and after verifying the signature extract to `~/apache-flume-1.8.0-bin`.

The flume configuration file `p1.conf`:
```
a1.channels = ch-1
a1.channels.ch-1.type = memory
a1.sources = src-1
a1.sinks = k1

a1.sources.src-1.type = spooldir
a1.sources.src-1.channels = ch-1
a1.sources.src-1.spoolDir = /home/vagrant/PBDP/hw5/hw5_p1_source


a1.sinks.k1.type = file_roll
a1.sinks.k1.channel = ch-1
a1.sinks.k1.sink.directory = /home/vagrant/PBDP/hw5/hw5_p1_sink
```

Initially both our source and sink directories are empty:
```
$ wc -l hw5_p1_source/*
wc: hw5_p1_source/*: No such file or directory
$ wc -l hw5_p1_sink/*
wc: hw5_p1_sink/*: No such file or directory
```

We start our flume agent with the log appearing in our console:
```
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p1.conf --name a1 -Dflume.root.logger=INFO,console
Info: Including Hive libraries found via () for Hive access
+ exec /usr/lib/jvm/jre-1.8.0-openjdk/bin/java -Xmx20m -Dflume.root.logger=INFO,console -cp '/home/vagrant/apache-flume-1.8.0-bin/conf:/home/vagrant/apache-flume-1.8.0-bin/lib/*:/lib/*' -Djava.library.path= org.apache.flume.node.Application --conf-file p1.conf --name a1
2018-02-27 03:42:14,802 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider.start(PollingPropertiesFileConfigurationProvider.java:62)] Configuration provider starting
2018-02-27 03:42:14,806 (conf-file-poller-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider$FileWatcherRunnable.run(PollingPropertiesFileConfigurationProvider.java:134)] Reloading configuration file:p1.conf
2018-02-27 03:42:14,812 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 03:42:14,812 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:930)] Added sinks: k1 Agent: a1
2018-02-27 03:42:14,813 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 03:42:14,813 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 03:42:14,820 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration.validateConfiguration(FlumeConfiguration.java:140)] Post-validation flume configuration contains configuration for agents: [a1]
2018-02-27 03:42:14,821 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:147)] Creating channels
2018-02-27 03:42:14,825 (conf-file-poller-0) [INFO - org.apache.flume.channel.DefaultChannelFactory.create(DefaultChannelFactory.java:42)] Creating instance of channel ch-1 type memory
2018-02-27 03:42:14,828 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:201)] Created channel ch-1
2018-02-27 03:42:14,832 (conf-file-poller-0) [INFO - org.apache.flume.source.DefaultSourceFactory.create(DefaultSourceFactory.java:41)] Creating instance of source src-1, type spooldir
2018-02-27 03:42:14,838 (conf-file-poller-0) [INFO - org.apache.flume.sink.DefaultSinkFactory.create(DefaultSinkFactory.java:42)] Creating instance of sink: k1, type: file_roll
2018-02-27 03:42:14,842 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.getConfiguration(AbstractConfigurationProvider.java:116)] Channel ch-1 connected to [src-1, k1]
2018-02-27 03:42:14,847 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:137)] Starting new configuration:{ sourceRunners:{src-1=EventDrivenSourceRunner: { source:Spool Directory source src-1: { spoolDir: /home/vagrant/PBDP/hw5/hw5_p1_source } }} sinkRunners:{k1=SinkRunner: { policy:org.apache.flume.sink.DefaultSinkProcessor@5d3cee04 counterGroup:{ name:null counters:{} } }} channels:{ch-1=org.apache.flume.channel.MemoryChannel{name: ch-1}} }
2018-02-27 03:42:14,853 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:144)] Starting Channel ch-1
2018-02-27 03:42:14,896 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: CHANNEL, name: ch-1: Successfully registered new MBean.
2018-02-27 03:42:14,897 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: CHANNEL, name: ch-1 started
2018-02-27 03:42:14,898 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:171)] Starting Sink k1
2018-02-27 03:42:14,898 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:110)] Starting org.apache.flume.sink.RollingFileSink{name:k1, channel:ch-1}...
2018-02-27 03:42:14,899 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SINK, name: k1: Successfully registered new MBean.
2018-02-27 03:42:14,899 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:182)] Starting Source src-1
2018-02-27 03:42:14,899 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SINK, name: k1 started
2018-02-27 03:42:14,899 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.source.SpoolDirectorySource.start(SpoolDirectorySource.java:83)] SpoolDirectorySource source starting with directory: /home/vagrant/PBDP/hw5/hw5_p1_source
2018-02-27 03:42:14,901 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:142)] RollingFileSink k1 started.
2018-02-27 03:42:14,922 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SOURCE, name: src-1: Successfully registered new MBean.
2018-02-27 03:42:14,922 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SOURCE, name: src-1 started
```

We copy the four log files into the source directory, and in the log notice that Flume is processing the data:
```
2018-02-27 03:43:26,433 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 03:43:26,434 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_01.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_01.txt.COMPLETED
2018-02-27 03:43:30,542 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 03:43:30,542 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_02.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_02.txt.COMPLETED
2018-02-27 03:43:30,611 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 03:43:30,611 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_03.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_03.txt.COMPLETED
2018-02-27 03:43:30,680 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 03:43:30,681 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_04.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_04.txt.COMPLETED
```

We can also validated that the data in the source have been processed and now appear in the sink:
```
$ wc -l hw5_p1_source/*
  25452 hw5_p1_source/log_file_01.txt.COMPLETED
  33800 hw5_p1_source/log_file_02.txt.COMPLETED
  25200 hw5_p1_source/log_file_03.txt.COMPLETED
  29250 hw5_p1_source/log_file_04.txt.COMPLETED
 113702 total

$ wc -l hw5_p1_sink/*
      0 hw5_p1_sink/1519702934840-1
      0 hw5_p1_sink/1519702934840-2
 113702 hw5_p1_sink/1519702934840-3
      0 hw5_p1_sink/1519702934840-4
      0 hw5_p1_sink/1519702934840-5
 113702 total
```

For experiment 1, we copy the same log files into the source directory. The Flume documentation explains that "If a file name is reused at a later time, Flume will print an error to its log file and stop processing". Indeed, in the log we see that Flume halts:
```
2018-02-27 03:46:00,829 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 03:46:00,829 (pool-4-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_01.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_01.txt.COMPLETED
2018-02-27 03:46:00,830 (pool-4-thread-1) [ERROR - org.apache.flume.source.SpoolDirectorySource$SpoolDirectoryRunnable.run(SpoolDirectorySource.java:280)] FATAL: Spool Directory source src-1: { spoolDir: /home/vagrant/PBDP/hw5/hw5_p1_source }: Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing.
java.lang.IllegalStateException: File name has been re-used with different files. Spooling assumptions violated for /home/vagrant/PBDP/hw5/hw5_p1_source/log_file_01.txt.COMPLETED
	at org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:463)
	at org.apache.flume.client.avro.ReliableSpoolingFileEventReader.retireCurrentFile(ReliableSpoolingFileEventReader.java:414)
	at org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:326)
	at org.apache.flume.source.SpoolDirectorySource$SpoolDirectoryRunnable.run(SpoolDirectorySource.java:250)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```

And a look into the source directory reveals no duplicate files marked as complated:
```
$ wc -l hw5_p1_source/*
   25452 hw5_p1_source/log_file_01.txt
   25452 hw5_p1_source/log_file_01.txt.COMPLETED
   33800 hw5_p1_source/log_file_02.txt
   33800 hw5_p1_source/log_file_02.txt.COMPLETED
   25200 hw5_p1_source/log_file_03.txt
   25200 hw5_p1_source/log_file_03.txt.COMPLETED
   29250 hw5_p1_source/log_file_04.txt
   29250 hw5_p1_source/log_file_04.txt.COMPLETED
  227404 total
```

But in the sink directory we do notice a new file processed, so it appears some duplicate data did in fact enter our sink:
```
$ wc -l hw5_p1_sink/*
      0 hw5_p1_sink/1519702934840-1
      0 hw5_p1_sink/1519702934840-10
      0 hw5_p1_sink/1519702934840-11
      0 hw5_p1_sink/1519702934840-2
 113702 hw5_p1_sink/1519702934840-3
      0 hw5_p1_sink/1519702934840-4
      0 hw5_p1_sink/1519702934840-5
      0 hw5_p1_sink/1519702934840-6
      0 hw5_p1_sink/1519702934840-7
  25452 hw5_p1_sink/1519702934840-8
      0 hw5_p1_sink/1519702934840-9
 139154 total
```
The documentation does explain that "Despite the reliability guarantees of this source, there are still cases in which events may be duplicated if certain downstream failures occur. This is consistent with the guarantees offered by other Flume components."

For the next experiment, we stop the agent and will copy some new text files into the source directory. We'll delete the unprocessed log files and add a few Shakespears books to the source directory while the Flume agent is not running:
```
$ wget https://www.gutenberg.org/cache/epub/1791/pg1791.txt
--2018-02-27 04:09:55--  https://www.gutenberg.org/cache/epub/1791/pg1791.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 160623 (157K) [text/plain]
Saving to: ‘pg1791.txt.1’

100%[==================================================================================================================================================================>] 160,623      590KB/s   in 0.3s   

2018-02-27 04:09:56 (590 KB/s) - ‘pg1791.txt.1’ saved [160623/160623]
$ wget https://www.gutenberg.org/cache/epub/1121/pg1121.txt
--2018-02-27 04:10:14--  https://www.gutenberg.org/cache/epub/1121/pg1121.txt
Resolving www.gutenberg.org (www.gutenberg.org)... 152.19.134.47, 2610:28:3090:3000:0:bad:cafe:47
Connecting to www.gutenberg.org (www.gutenberg.org)|152.19.134.47|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 162250 (158K) [text/plain]
Saving to: ‘pg1121.txt’

100%[==================================================================================================================================================================>] 162,250      627KB/s   in 0.3s   

2018-02-27 04:10:15 (627 KB/s) - ‘pg1121.txt’ saved [162250/162250]
$ rm hw5_p1_source/*.txt
$ cp pg1121.txt pg1791.txt hw5_p1_source/
$ wc -l hw5_p1_source/*
  25452 hw5_p1_source/log_file_01.txt.COMPLETED
  33800 hw5_p1_source/log_file_02.txt.COMPLETED
  25200 hw5_p1_source/log_file_03.txt.COMPLETED
  29250 hw5_p1_source/log_file_04.txt.COMPLETED
   4204 hw5_p1_source/pg1121.txt
   3969 hw5_p1_source/pg1791.txt
 121875 total
$ wc -l hw5_p1_sink/*
      0 hw5_p1_sink/1519702934840-1
      0 hw5_p1_sink/1519702934840-10
      0 hw5_p1_sink/1519702934840-11
      0 hw5_p1_sink/1519702934840-12
      0 hw5_p1_sink/1519702934840-13
      0 hw5_p1_sink/1519702934840-14
      0 hw5_p1_sink/1519702934840-15
      0 hw5_p1_sink/1519702934840-16
      0 hw5_p1_sink/1519702934840-17
      0 hw5_p1_sink/1519702934840-18
      0 hw5_p1_sink/1519702934840-19
      0 hw5_p1_sink/1519702934840-2
      0 hw5_p1_sink/1519702934840-20
      0 hw5_p1_sink/1519702934840-21
      0 hw5_p1_sink/1519702934840-22
      0 hw5_p1_sink/1519702934840-23
      0 hw5_p1_sink/1519702934840-24
      0 hw5_p1_sink/1519702934840-25
      0 hw5_p1_sink/1519702934840-26
      0 hw5_p1_sink/1519702934840-27
      0 hw5_p1_sink/1519702934840-28
      0 hw5_p1_sink/1519702934840-29
 113702 hw5_p1_sink/1519702934840-3
      0 hw5_p1_sink/1519702934840-30
      0 hw5_p1_sink/1519702934840-31
      0 hw5_p1_sink/1519702934840-32
      0 hw5_p1_sink/1519702934840-33
      0 hw5_p1_sink/1519702934840-34
      0 hw5_p1_sink/1519702934840-35
      0 hw5_p1_sink/1519702934840-36
      0 hw5_p1_sink/1519702934840-37
      0 hw5_p1_sink/1519702934840-38
      0 hw5_p1_sink/1519702934840-39
      0 hw5_p1_sink/1519702934840-4
      0 hw5_p1_sink/1519702934840-40
      0 hw5_p1_sink/1519702934840-41
      0 hw5_p1_sink/1519702934840-42
      0 hw5_p1_sink/1519702934840-43
      0 hw5_p1_sink/1519702934840-44
      0 hw5_p1_sink/1519702934840-45
      0 hw5_p1_sink/1519702934840-46
      0 hw5_p1_sink/1519702934840-47
      0 hw5_p1_sink/1519702934840-48
      0 hw5_p1_sink/1519702934840-49
      0 hw5_p1_sink/1519702934840-5
      0 hw5_p1_sink/1519702934840-50
      0 hw5_p1_sink/1519702934840-51
      0 hw5_p1_sink/1519702934840-52
      0 hw5_p1_sink/1519702934840-53
      0 hw5_p1_sink/1519702934840-54
      0 hw5_p1_sink/1519702934840-55
      0 hw5_p1_sink/1519702934840-56
      0 hw5_p1_sink/1519702934840-57
      0 hw5_p1_sink/1519702934840-6
      0 hw5_p1_sink/1519702934840-7
  25452 hw5_p1_sink/1519702934840-8
      0 hw5_p1_sink/1519702934840-9
      0 hw5_p1_sink/1519704795345-1
 139154 total
```

We restart the Flume agent and watch it pick up the files:
```
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p1.conf --name a1 -Dflume.root.logger=INFO,console
Info: Including Hive libraries found via () for Hive access
+ exec /usr/lib/jvm/jre-1.8.0-openjdk/bin/java -Xmx20m -Dflume.root.logger=INFO,console -cp '/home/vagrant/apache-flume-1.8.0-bin/conf:/home/vagrant/apache-flume-1.8.0-bin/lib/*:/lib/*' -Djava.library.path= org.apache.flume.node.Application --conf-file p1.conf --name a1
2018-02-27 04:15:05,352 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider.start(PollingPropertiesFileConfigurationProvider.java:62)] Configuration provider starting
2018-02-27 04:15:05,356 (conf-file-poller-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider$FileWatcherRunnable.run(PollingPropertiesFileConfigurationProvider.java:134)] Reloading configuration file:p1.conf
2018-02-27 04:15:05,361 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 04:15:05,361 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:930)] Added sinks: k1 Agent: a1
2018-02-27 04:15:05,362 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 04:15:05,362 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 04:15:05,369 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration.validateConfiguration(FlumeConfiguration.java:140)] Post-validation flume configuration contains configuration for agents: [a1]
2018-02-27 04:15:05,370 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:147)] Creating channels
2018-02-27 04:15:05,374 (conf-file-poller-0) [INFO - org.apache.flume.channel.DefaultChannelFactory.create(DefaultChannelFactory.java:42)] Creating instance of channel ch-1 type memory
2018-02-27 04:15:05,377 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:201)] Created channel ch-1
2018-02-27 04:15:05,380 (conf-file-poller-0) [INFO - org.apache.flume.source.DefaultSourceFactory.create(DefaultSourceFactory.java:41)] Creating instance of source src-1, type spooldir
2018-02-27 04:15:05,385 (conf-file-poller-0) [INFO - org.apache.flume.sink.DefaultSinkFactory.create(DefaultSinkFactory.java:42)] Creating instance of sink: k1, type: file_roll
2018-02-27 04:15:05,389 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.getConfiguration(AbstractConfigurationProvider.java:116)] Channel ch-1 connected to [src-1, k1]
2018-02-27 04:15:05,394 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:137)] Starting new configuration:{ sourceRunners:{src-1=EventDrivenSourceRunner: { source:Spool Directory source src-1: { spoolDir: /home/vagrant/PBDP/hw5/hw5_p1_source } }} sinkRunners:{k1=SinkRunner: { policy:org.apache.flume.sink.DefaultSinkProcessor@5d3cee04 counterGroup:{ name:null counters:{} } }} channels:{ch-1=org.apache.flume.channel.MemoryChannel{name: ch-1}} }
2018-02-27 04:15:05,400 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:144)] Starting Channel ch-1
2018-02-27 04:15:05,443 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: CHANNEL, name: ch-1: Successfully registered new MBean.
2018-02-27 04:15:05,444 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: CHANNEL, name: ch-1 started
2018-02-27 04:15:05,445 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:171)] Starting Sink k1
2018-02-27 04:15:05,445 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:110)] Starting org.apache.flume.sink.RollingFileSink{name:k1, channel:ch-1}...
2018-02-27 04:15:05,445 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:182)] Starting Source src-1
2018-02-27 04:15:05,446 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.source.SpoolDirectorySource.start(SpoolDirectorySource.java:83)] SpoolDirectorySource source starting with directory: /home/vagrant/PBDP/hw5/hw5_p1_source
2018-02-27 04:15:05,446 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SINK, name: k1: Successfully registered new MBean.
2018-02-27 04:15:05,446 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SINK, name: k1 started
2018-02-27 04:15:05,447 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:142)] RollingFileSink k1 started.
2018-02-27 04:15:05,467 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SOURCE, name: src-1: Successfully registered new MBean.
2018-02-27 04:15:05,467 (lifecycleSupervisor-1-1) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SOURCE, name: src-1 started
2018-02-27 04:15:05,671 (pool-3-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 04:15:05,672 (pool-3-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/pg1121.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/pg1121.txt.COMPLETED
2018-02-27 04:15:09,695 (pool-3-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.readEvents(ReliableSpoolingFileEventReader.java:324)] Last read took us just up to a file boundary. Rolling to the next file, if there is one.
2018-02-27 04:15:09,695 (pool-3-thread-1) [INFO - org.apache.flume.client.avro.ReliableSpoolingFileEventReader.rollCurrentFile(ReliableSpoolingFileEventReader.java:433)] Preparing to move file /home/vagrant/PBDP/hw5/hw5_p1_source/pg1791.txt to /home/vagrant/PBDP/hw5/hw5_p1_source/pg1791.txt.COMPLETED
```

The source text files are marked completed, and we now have additional data in the sink directory:
```
$ wc -l hw5_p1_source/*
  25452 hw5_p1_source/log_file_01.txt.COMPLETED
  33800 hw5_p1_source/log_file_02.txt.COMPLETED
  25200 hw5_p1_source/log_file_03.txt.COMPLETED
  29250 hw5_p1_source/log_file_04.txt.COMPLETED
   4204 hw5_p1_source/pg1121.txt.COMPLETED
   3969 hw5_p1_source/pg1791.txt.COMPLETED
 121875 total
$ wc -l hw5_p1_sink/*
      0 hw5_p1_sink/1519702934840-1
      0 hw5_p1_sink/1519702934840-10
      0 hw5_p1_sink/1519702934840-11
      0 hw5_p1_sink/1519702934840-12
      0 hw5_p1_sink/1519702934840-13
      0 hw5_p1_sink/1519702934840-14
      0 hw5_p1_sink/1519702934840-15
      0 hw5_p1_sink/1519702934840-16
      0 hw5_p1_sink/1519702934840-17
      0 hw5_p1_sink/1519702934840-18
      0 hw5_p1_sink/1519702934840-19
      0 hw5_p1_sink/1519702934840-2
      0 hw5_p1_sink/1519702934840-20
      0 hw5_p1_sink/1519702934840-21
      0 hw5_p1_sink/1519702934840-22
      0 hw5_p1_sink/1519702934840-23
      0 hw5_p1_sink/1519702934840-24
      0 hw5_p1_sink/1519702934840-25
      0 hw5_p1_sink/1519702934840-26
      0 hw5_p1_sink/1519702934840-27
      0 hw5_p1_sink/1519702934840-28
      0 hw5_p1_sink/1519702934840-29
 113702 hw5_p1_sink/1519702934840-3
      0 hw5_p1_sink/1519702934840-30
      0 hw5_p1_sink/1519702934840-31
      0 hw5_p1_sink/1519702934840-32
      0 hw5_p1_sink/1519702934840-33
      0 hw5_p1_sink/1519702934840-34
      0 hw5_p1_sink/1519702934840-35
      0 hw5_p1_sink/1519702934840-36
      0 hw5_p1_sink/1519702934840-37
      0 hw5_p1_sink/1519702934840-38
      0 hw5_p1_sink/1519702934840-39
      0 hw5_p1_sink/1519702934840-4
      0 hw5_p1_sink/1519702934840-40
      0 hw5_p1_sink/1519702934840-41
      0 hw5_p1_sink/1519702934840-42
      0 hw5_p1_sink/1519702934840-43
      0 hw5_p1_sink/1519702934840-44
      0 hw5_p1_sink/1519702934840-45
      0 hw5_p1_sink/1519702934840-46
      0 hw5_p1_sink/1519702934840-47
      0 hw5_p1_sink/1519702934840-48
      0 hw5_p1_sink/1519702934840-49
      0 hw5_p1_sink/1519702934840-5
      0 hw5_p1_sink/1519702934840-50
      0 hw5_p1_sink/1519702934840-51
      0 hw5_p1_sink/1519702934840-52
      0 hw5_p1_sink/1519702934840-53
      0 hw5_p1_sink/1519702934840-54
      0 hw5_p1_sink/1519702934840-55
      0 hw5_p1_sink/1519702934840-56
      0 hw5_p1_sink/1519702934840-57
      0 hw5_p1_sink/1519702934840-6
      0 hw5_p1_sink/1519702934840-7
  25452 hw5_p1_sink/1519702934840-8
      0 hw5_p1_sink/1519702934840-9
      0 hw5_p1_sink/1519704795345-1
   8173 hw5_p1_sink/1519704905388-1
      0 hw5_p1_sink/1519704905388-2
      0 hw5_p1_sink/1519704905388-3
 147327 total
```
As the Flume documentation explains, although our agent will die if duplicate logs enter the source, "this source is reliable and will not miss data, even if Flume is restarted or killed."


## Probem 2


Inside of a Vagrant machine (IP `192.168.183.2`) we create a hello world html file for our Apache server:
```
<html>
<header><title>Small page</title></header>
<body>
David Shaub
</body>
</html>
```

![hello world](apache.png)

We see that this access appears in `access_log`:
```
$ sudo tail /var/log/httpd/access_log
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/bootstrap.min.css HTTP/1.1" 200 19341 "http://192.168.183.2/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/open-sans.css HTTP/1.1" 200 5081 "http://192.168.183.2/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /images/apache_pb.gif HTTP/1.1" 200 2326 "http://192.168.183.2/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /images/poweredby.png HTTP/1.1" 200 3956 "http://192.168.183.2/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/fonts/Light/OpenSans-Light.woff HTTP/1.1" 404 241 "http://192.168.183.2/noindex/css/open-sans.css" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/fonts/Bold/OpenSans-Bold.woff HTTP/1.1" 404 239 "http://192.168.183.2/noindex/css/open-sans.css" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/fonts/Light/OpenSans-Light.ttf HTTP/1.1" 404 240 "http://192.168.183.2/noindex/css/open-sans.css" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /noindex/css/fonts/Bold/OpenSans-Bold.ttf HTTP/1.1" 404 238 "http://192.168.183.2/noindex/css/open-sans.css" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:53:59 +0000] "GET /favicon.ico HTTP/1.1" 404 209 "http://192.168.183.2/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
192.168.183.1 - - [28/Feb/2018:03:54:58 +0000] "GET / HTTP/1.1" 200 86 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36"
```

To generate access events to our http server we use the below shell script:
```
#!/usr/bin/env bash
# This script requires 'bc', 'curl', and GNU coreutils 'sleep'

if (( "$1" != 1 )); then
    rate=$(echo "1 / $1" | bc -l)
    while true; do
        curl 'localhost:80'
        sleep $rate
    done
fi
echo "Must supply a numeric rate argument"
exit 1
```

We setup our Flume agent with the following `p2.conf` config file so that a new file in the sink is created every 5 seconds:
```
a1.channels = ch-1
a1.channels.ch-1.type = memory
a1.channels.c1.capacity = 100
a1.channels.c1.transactionCapacity = 100
a1.sources = src-1
a1.sinks = k1

a1.sources.src-1.type = exec
a1.sources.src-1.channels = ch-1
a1.sources.r1.command = tail -F /var/log/secure

a1.sinks.k1.type = file_roll
a1.sinks.k1.channel = ch-1
a1.sinks.k1.sink.directory = /home/vagrant/PBDP/hw5/hw5_p2_sink
a1.sinks.k1.sink.directory.rollInterval = 5
```

Launch the Flume agent:
```
$ mkdir -p /home/vagrant/PBDP/hw5/hw5_p2_sink
$ ~/apache-flume-1.8.0-bin/bin/flume-ng agent --conf ~/apache-flume-1.8.0-bin/conf --conf-file p2.conf --name a1 -Dflume.root.logger=INFO,console
Warning: JAVA_HOME is not set!
Info: Including Hive libraries found via () for Hive access
+ exec /usr/bin/java -Xmx20m -Dflume.root.logger=INFO,console -cp '/home/vagrant/apache-flume-1.8.0-bin/conf:/home/vagrant/apache-flume-1.8.0-bin/lib/*:/lib/*' -Djava.library.path= org.apache.flume.node.Application --conf-file p2.conf --name a1
2018-02-27 15:21:29,123 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider.start(PollingPropertiesFileConfigurationProvider.java:62)] Configuration provider starting
2018-02-27 15:21:29,127 (conf-file-poller-0) [INFO - org.apache.flume.node.PollingPropertiesFileConfigurationProvider$FileWatcherRunnable.run(PollingPropertiesFileConfigurationProvider.java:134)] Reloading configuration file:p2.conf
2018-02-27 15:21:29,133 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:930)] Added sinks: k1 Agent: a1
2018-02-27 15:21:29,133 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 15:21:29,133 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 15:21:29,133 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 15:21:29,134 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration$AgentConfiguration.addProperty(FlumeConfiguration.java:1016)] Processing:k1
2018-02-27 15:21:29,142 (conf-file-poller-0) [INFO - org.apache.flume.conf.FlumeConfiguration.validateConfiguration(FlumeConfiguration.java:140)] Post-validation flume configuration contains configuration for agents: [a1]
2018-02-27 15:21:29,142 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:147)] Creating channels
2018-02-27 15:21:29,147 (conf-file-poller-0) [INFO - org.apache.flume.channel.DefaultChannelFactory.create(DefaultChannelFactory.java:42)] Creating instance of channel ch-1 type memory
2018-02-27 15:21:29,153 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.loadChannels(AbstractConfigurationProvider.java:201)] Created channel ch-1
2018-02-27 15:21:29,154 (conf-file-poller-0) [INFO - org.apache.flume.source.DefaultSourceFactory.create(DefaultSourceFactory.java:41)] Creating instance of source src-1, type exec
2018-02-27 15:21:29,159 (conf-file-poller-0) [INFO - org.apache.flume.sink.DefaultSinkFactory.create(DefaultSinkFactory.java:42)] Creating instance of sink: k1, type: file_roll
2018-02-27 15:21:29,163 (conf-file-poller-0) [INFO - org.apache.flume.node.AbstractConfigurationProvider.getConfiguration(AbstractConfigurationProvider.java:116)] Channel ch-1 connected to [src-1, k1]
2018-02-27 15:21:29,168 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:137)] Starting new configuration:{ sourceRunners:{src-1=EventDrivenSourceRunner: { source:org.apache.flume.source.ExecSource{name:src-1,state:IDLE} }} sinkRunners:{k1=SinkRunner: { policy:org.apache.flume.sink.DefaultSinkProcessor@18ca564c counterGroup:{ name:null counters:{} } }} channels:{ch-1=org.apache.flume.channel.MemoryChannel{name: ch-1}} }
2018-02-27 15:21:29,175 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:144)] Starting Channel ch-1
2018-02-27 15:21:29,177 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:159)] Waiting for channel: ch-1 to start. Sleeping for 500 ms
2018-02-27 15:21:29,220 (lifecycleSupervisor-1-2) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: CHANNEL, name: ch-1: Successfully registered new MBean.
2018-02-27 15:21:29,220 (lifecycleSupervisor-1-2) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: CHANNEL, name: ch-1 started
2018-02-27 15:21:29,682 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:171)] Starting Sink k1
2018-02-27 15:21:29,683 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:110)] Starting org.apache.flume.sink.RollingFileSink{name:k1, channel:ch-1}...
2018-02-27 15:21:29,683 (conf-file-poller-0) [INFO - org.apache.flume.node.Application.startAllComponents(Application.java:182)] Starting Source src-1
2018-02-27 15:21:29,684 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SINK, name: k1: Successfully registered new MBean.
2018-02-27 15:21:29,684 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SINK, name: k1 started
2018-02-27 15:21:29,684 (lifecycleSupervisor-1-2) [INFO - org.apache.flume.source.ExecSource.start(ExecSource.java:168)] Exec source starting with command: sudo tail -F /var/log/httpd/access_log
2018-02-27 15:21:29,684 (lifecycleSupervisor-1-2) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.register(MonitoredCounterGroup.java:119)] Monitored counter group for type: SOURCE, name: src-1: Successfully registered new MBean.
2018-02-27 15:21:29,685 (lifecycleSupervisor-1-2) [INFO - org.apache.flume.instrumentation.MonitoredCounterGroup.start(MonitoredCounterGroup.java:95)] Component type: SOURCE, name: src-1 started
2018-02-27 15:21:29,685 (lifecycleSupervisor-1-0) [INFO - org.apache.flume.sink.RollingFileSink.start(RollingFileSink.java:142)] RollingFileSink k1 started.
```

For our first experiment, we launch our script to generate access to the server at 20 events per second and then revoke permissions so that Flume can no longer create new sink files. We'll wait 5 seconds before revoking permissions and checking the files created in the sink. Then after 5 minutes we'll restore access and again check the sink file contents.
```
./p2_generator.sh 20 & sleep 5
chmod 444 /home/vagrant/PBDP/hw5/hw5_p2_sink
sleep 5
wc -l /home/vagrant/PBDP/hw5/hw5_p2_sink/*
sleep 300
chmod 775 /home/vagrant/PBDP/hw5/hw5_p2_sink
wc -l /home/vagrant/PBDP/hw5/hw5_p2_sink/*
sleep 5
wc -l /home/vagrant/PBDP/hw5/hw5_p2_sink/*
sudo wc -l /var/log/httpd/access_log
```
