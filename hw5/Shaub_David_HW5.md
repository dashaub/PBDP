---
title: Homework 5
author: David Shaub
geometry: margin=2cm
date: 2018-02-24
---

All problems were completed, including problem 5.

## Problem 1

The flume configuration file:
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
