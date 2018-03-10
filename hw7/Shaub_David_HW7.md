---
title: Homework 7
author: David Shaub
geometry: margin=2cm
date: 2018-04-20
---

All problems were completed.

## Problem 1

```
def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {}'.format(hourpart, url)

export PYSPARK_DRIVER_PYTHON=ipython


logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
hour_url = logs.map(extract_hourpart_url)


q1 = hour_url.distinct().map(lambda x: x.split(' ')[0]).countByValue()


q3 = hour_url.countByValue()




```
