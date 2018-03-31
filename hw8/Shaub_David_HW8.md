---
title: Homework 8
author: David Shaub
geometry: margin=2cm
date: 2018-03-31
---

## Setup

We'll modify the Spark jobs we created in HW7 (`p1_q1.py` and `p1_q2.py`) to now output tuples with the hour and user for `unique_users.py` and the hour and url for `unique_url.py`. While we could collect the unique url and users for each hour into an array, this nested data structure isn't quite as nice to work with in MariaDB, so we will prefer flat records for the output of our batch job--even though there will be more of them.

`unique_url.py`:
```
"""
Get unique URLs by hour
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("unique_url")
sc = SparkContext(conf = conf)

def extract_hourpart_url(dat):
    """
    Return timestamp (up to the hour) and the URL
    :param dat: An RDD from our log data
    """
    _, timestamp, url, _ = dat.split(' ')
    hourpart = timestamp[:13] + ':00:00'
    return '{} {}'.format(hourpart, url)

logs = sc.textFile('hw7_logs*.txt')
# Convert full timestamp to only hour and return only needed fields and remove duplicate URLs
hour_url = logs.map(extract_hourpart_url).distinct()

hour_url.coalesce(1).saveAsTextFile("output_unique_url/")
```

`unique_users.py`:
```
"""
Get unique visitors by URLs by hour
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime


conf = SparkConf().setAppName("unique_users")
sc = SparkContext(conf = conf)

def extract_hourpart_user(dat):
    """
    Return timestamp (up to the hour) and user
    :param dat: An RDD from our log data
    """
    _, timestamp, url, user = dat.split(' ')
    hourpart = timestamp[:13] + ':00:00'
    return '{} {}'.format(hourpart, user)

logs = sc.textFile('hw7_logs*.txt')
# Extract hour, url, and user and remove duplicates
hour_user = logs.map(extract_hourpart_user).distinct()

# Form key/values and get grouped counts
#tuples = hour_url_user.map(lambda x: tuple([x.split(' ')[0] + ' ' + x.split(' ')[1], 1]))
#q2 = tuples.reduceByKey(lambda x, y: x + y)

hour_user.coalesce(1).saveAsTextFile("output_unique_users/")
```

We see that our Spark job of deduplciation has vasty reduced the size of our data: even though we decided to produce a "longer" set of results by using the flat records instead of a "wider" set with an array with the unique urls/users, we still have a manageable number of results:
```
$ wc -l output_unique_users/part-00000 
   11960 output_unique_users/part-00000
$ wc -l output_unique_url/part-00000 
    2990 output_unique_url/part-00000
```

And the output of the jobs:
```
$ head output_unique_users/part-00000 
2018-02-23T05:00:00 User_28
2018-03-01T10:00:00 User_30
2018-02-22T02:00:00 User_1
2018-02-23T12:00:00 User_19
2018-03-04T14:00:00 User_37
2018-02-24T10:00:00 User_16
2018-03-01T02:00:00 User_26
2018-02-23T04:00:00 User_14
2018-02-20T08:00:00 User_5
2018-02-21T20:00:00 User_32

$ head output_unique_url/part-00000 
2018-02-21T13:00:00 http://example.com/?url=8
2018-02-27T09:00:00 http://example.com/?url=7
2018-02-25T07:00:00 http://example.com/?url=7
2018-03-02T05:00:00 http://example.com/?url=7
2018-03-01T18:00:00 http://example.com/?url=4
2018-02-20T19:00:00 http://example.com/?url=5
2018-03-04T10:00:00 http://example.com/?url=7
2018-03-01T07:00:00 http://example.com/?url=8
2018-02-27T06:00:00 http://example.com/?url=0
2018-02-26T12:00:00 http://example.com/?url=4
```

![Database creation in MariaDB](show_databases.png)


Now we create the databases and tables in MariDB to hold these results:
```
MariaDB [(none)]> create database hw8;
Query OK, 1 row affected (0.00 sec)

MariaDB [(none)]> show databases;
+--------------------+
| Database           |
+--------------------+
| hw8                |
| information_schema |
| mysql              |
| performance_schema |
+--------------------+
4 rows in set (0.00 sec)

MariaDB [(none)]> use hw8;
Database changed
```

The schemas for the table we create will be very simple since we hold each url/user in its own record. For report 1, we will create a table with columns for the timestamp (always aligned to the hour) and the URL:
```
MariaDB [hw8]> create table hour_url (hour timestamp not null, url text not null);
Query OK, 0 rows affected (0.02 sec)

MariaDB [hw8]> show columns from hour_url;
+-------+-----------+------+-----+---------------------+-------------------------------+
| Field | Type      | Null | Key | Default             | Extra                         |
+-------+-----------+------+-----+---------------------+-------------------------------+
| hour  | timestamp | NO   |     | current_timestamp() | on update current_timestamp() |
| url   | text      | NO   |     | NULL                |                               |
+-------+-----------+------+-----+---------------------+-------------------------------+
2 rows in set (0.00 sec)
```

For report 2, we create a table with columns for the timestamp (always aligned to the hour) and the user:
```
MariaDB [hw8]> create table hour_user (hour timestamp not null, user text not null);
Query OK, 0 rows affected (0.02 sec)

MariaDB [hw8]> show columns from hour_user;
+-------+-----------+------+-----+---------------------+-------------------------------+
| Field | Type      | Null | Key | Default             | Extra                         |
+-------+-----------+------+-----+---------------------+-------------------------------+
| hour  | timestamp | NO   |     | current_timestamp() | on update current_timestamp() |
| user  | text      | NO   |     | NULL                |                               |
+-------+-----------+------+-----+---------------------+-------------------------------+
2 rows in set (0.00 sec)
```

Now we load the data into these tables:
```
MariaDB [hw8]> load data infile '~/PBDP/hw8/output_unique_url/part-00000' into table hour_url fields terminated by ' ';
Query OK, 2990 rows affected (0.01 sec)              
Records: 2990  Deleted: 0  Skipped: 0  Warnings: 0

MariaDB [hw8]> load data infile '~/PBDP/hw8/output_unique_users/part-00000' into table hour_user fields terminated by ' ';
Query OK, 11960 rows affected (0.03 sec)             
Records: 11960  Deleted: 0  Skipped: 0  Warnings: 0
```

The daily reports are now very easy to generate in SQL, and if we needed to generate reports based on some frequency other than daily, we all we need to do is modify our grouping condition.
**Report 1**
```
MariaDB [hw8]> select date(hour) as day, count(distinct(url)) as num_url from hour_url group by day order by day asc;
+------------+---------+
| day        | num_url |
+------------+---------+
| 2018-02-20 |      10 |
| 2018-02-21 |      10 |
| 2018-02-22 |      10 |
| 2018-02-23 |      10 |
| 2018-02-24 |      10 |
| 2018-02-25 |      10 |
| 2018-02-26 |      10 |
| 2018-02-27 |      10 |
| 2018-02-28 |      10 |
| 2018-03-01 |      10 |
| 2018-03-02 |      10 |
| 2018-03-03 |      10 |
| 2018-03-04 |      10 |
+------------+---------+
13 rows in set (0.02 sec)
```

**Report 2**
```
MariaDB [hw8]> select date(hour) as day, count(distinct(user)) as num_users from hour_user group by day order by day asc;
+------------+-----------+
| day        | num_users |
+------------+-----------+
| 2018-02-20 |        40 |
| 2018-02-21 |        40 |
| 2018-02-22 |        40 |
| 2018-02-23 |        40 |
| 2018-02-24 |        40 |
| 2018-02-25 |        40 |
| 2018-02-26 |        40 |
| 2018-02-27 |        40 |
| 2018-02-28 |        40 |
| 2018-03-01 |        40 |
| 2018-03-02 |        40 |
| 2018-03-03 |        40 |
| 2018-03-04 |        40 |
+------------+-----------+
13 rows in set (0.06 sec)
```

## Problem 1

## Problem 2

## Problem 3
