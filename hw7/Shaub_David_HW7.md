---
title: Homework 7
author: David Shaub
geometry: margin=2cm
date: 2018-04-20
---

All problems were completed.

## Problem 1

```
export PYSPARK_DRIVER_PYTHON=ipython

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {}'.format(hourpart, url)

def extract_hourpart_url_user(dat):
    """
    Return timestamp (up to the hour), URL, and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13]
    return '{} {} {}'.format(hourpart, url, user)


logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
# Convert full timestamp to only hour and return only needed fields
hour_url = logs.map(extract_hourpart_url)
# Remove duplicates, select only the hour field, and get count
q1 = hour_url.distinct().map(lambda x: x.split(' ')[0]).countByValue()

# Extract hour, url, and user and remove duplicates
hour_url_user = logs.map(extract_hourpart_url_user).distinct()
# Return only the first two fields (hour and URL)
two_fields = hour_url_user.map(lambda x: ' '.join(x.split(' ')[0:2]))
# Get count
q2 = two_fields.countByValue()

#
q3 = hour_url.countByValue()
```


## Problem 2
```
deduped = logs.distinct()
hour_url = deduped.map(extract_hourpart_url)
p2_q1 = hour_url.distinct().map(lambda x: x.split(' ')[0]).countByValue()
```

## Problem 3
```
def create_pairs(dat):
    """
    Create key/value pairs
    hour, url, user = dat.split(' ')
    results = (hour + ' ' + url, user)
    return results
    """

# Transform into key/value pairs
pairs = hour_url_user = logs.map(extract_hourpart_url_user).distinct()
np = pairs.map(lambda x: (x.split(' ')[0:2], x))
np = pairs.map(create_pairs)
```

## Problem 4
```
# Load the community data and create tuples
community = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_community.txt')
community = community.map(lambda x: tuple(x.split('\t')))

# Load the log dat and create tuples
logs = sc.textFile('/Users/david.shaub/PBDP/hw7/hw7_logs*.txt')
user_url = logs.map(lambda x: tuple([x.split(' ')[3], x.split(' ')[2]]))

# Join the datasets
joined = community.join(user_url)
# Discard user
community_url = joined.map(lambda x: x[1])
# 

```