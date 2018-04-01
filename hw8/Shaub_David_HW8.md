---
title: Homework 8
author: David Shaub
geometry: margin=2cm
date: 2018-03-31
---

## Setup

We'll modify the Spark jobs we created in HW7 (`p1_q1.py` and `p1_q2.py`) to now output tuples with the hour and user for `unique_users.py` and the hour and url for `unique_url.py`. While we could collect the unique url and users for each hour into an array, this nested data structure isn't quite as nice to work with in MariaDB, so we will prefer flat records for the output of our batch job--even though there will be more records per hour instead of only one.

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



## Problem 1

```
$ mysql --version
mysql  Ver 15.1 Distrib 10.2.14-MariaDB, for osx10.11 (x86_64) using readline 5.1
$ mysql -uroot
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Your MariaDB connection id is 121
Server version: 10.2.14-MariaDB Homebrew

Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MariaDB [(none)]> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| mysql              |
| performance_schema |
+--------------------+
3 rows in set (0.00 sec)
```

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

![Database creation in MariaDB](show_databases.png)


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

The daily reports are now very easy to generate in SQL, and if we needed to generate reports based on some frequency other than daily, all we need to do is modify our grouping condition.

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

## Problem 2

We install mongodb from the EPEL repos and have version 2.6.12.
```
$ mongo --version
MongoDB shell version: 2.6.12
$ mongo
MongoDB shell version: 2.6.12
connecting to: test
Server has startup warnings: 
2018-03-31T16:36:59.363+0000 [initandlisten] 
2018-03-31T16:36:59.363+0000 [initandlisten] ** WARNING: Readahead for /var/lib/mongodb is set to 4096KB
2018-03-31T16:36:59.363+0000 [initandlisten] **          We suggest setting it to 256KB (512 sectors) or less
2018-03-31T16:36:59.363+0000 [initandlisten] **          http://dochub.mongodb.org/core/readahead
```

We load our data into MongoDB by changing the delimiter to a comma; the result is piped to `mongoimport` with the specified field names, database, and collection:
```
$ tr " " "," <  ~/PBDP/hw8/output_unique_url/part-00000 | mongoimport -d hw8 -c hour_url --type csv -f hour,url
connected to: 127.0.0.1
2018-03-31T18:12:55.836+0000 check 9 2990
2018-03-31T18:12:55.838+0000 imported 2990 objects

$ tr " " "," <  ~/PBDP/hw8/output_unique_users/part-00000 | mongoimport -d hw8 -c hour_user --type csv -f hour,user
connected to: 127.0.0.1
2018-03-31T18:13:15.341+0000 check 9 11960
2018-03-31T18:13:15.369+0000 imported 11960 objects
```

![Mongo database and collections from shell](mongo_databases.png)

We enter the Mongo shell and see that our dabase and collections have been populated. The schema we use for our two collections is essentially the same that we used for MariaDB: one field in the JSON contains the timestamp `hour` and the other contains the `user` or `url`. We can also see that our records have been inserted:
```
> db.hour_url.find()
{ "_id" : ObjectId("5abfe1535397feaeda6272e8"), "hour" : "2018-02-21T13:00:00", "url" : "http://example.com/?url=8" }
{ "_id" : ObjectId("5abfe1535397feaeda6272e9"), "hour" : "2018-02-27T09:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272ea"), "hour" : "2018-02-25T07:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272eb"), "hour" : "2018-03-02T05:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272ec"), "hour" : "2018-03-01T18:00:00", "url" : "http://example.com/?url=4" }
{ "_id" : ObjectId("5abfe1535397feaeda6272ed"), "hour" : "2018-02-20T19:00:00", "url" : "http://example.com/?url=5" }
{ "_id" : ObjectId("5abfe1535397feaeda6272ee"), "hour" : "2018-03-04T10:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272ef"), "hour" : "2018-03-01T07:00:00", "url" : "http://example.com/?url=8" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f0"), "hour" : "2018-02-27T06:00:00", "url" : "http://example.com/?url=0" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f1"), "hour" : "2018-02-26T12:00:00", "url" : "http://example.com/?url=4" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f2"), "hour" : "2018-03-02T04:00:00", "url" : "http://example.com/?url=6" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f3"), "hour" : "2018-03-01T14:00:00", "url" : "http://example.com/?url=0" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f4"), "hour" : "2018-02-21T01:00:00", "url" : "http://example.com/?url=1" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f5"), "hour" : "2018-02-21T03:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f6"), "hour" : "2018-02-27T10:00:00", "url" : "http://example.com/?url=5" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f7"), "hour" : "2018-03-03T20:00:00", "url" : "http://example.com/?url=7" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f8"), "hour" : "2018-02-28T07:00:00", "url" : "http://example.com/?url=4" }
{ "_id" : ObjectId("5abfe1535397feaeda6272f9"), "hour" : "2018-02-26T06:00:00", "url" : "http://example.com/?url=3" }
{ "_id" : ObjectId("5abfe1535397feaeda6272fa"), "hour" : "2018-02-26T13:00:00", "url" : "http://example.com/?url=9" }
{ "_id" : ObjectId("5abfe1535397feaeda6272fb"), "hour" : "2018-02-22T21:00:00", "url" : "http://example.com/?url=2" }
Type "it" for more

> db.hour_user.find()
{ "_id" : ObjectId("5abfe0045397feaeda624430"), "hour" : "2018-02-23T05:00:00", "user" : "User_28" }
{ "_id" : ObjectId("5abfe0045397feaeda624431"), "hour" : "2018-03-01T10:00:00", "user" : "User_30" }
{ "_id" : ObjectId("5abfe0045397feaeda624432"), "hour" : "2018-02-22T02:00:00", "user" : "User_1" }
{ "_id" : ObjectId("5abfe0045397feaeda624433"), "hour" : "2018-02-23T12:00:00", "user" : "User_19" }
{ "_id" : ObjectId("5abfe0045397feaeda624434"), "hour" : "2018-03-04T14:00:00", "user" : "User_37" }
{ "_id" : ObjectId("5abfe0045397feaeda624435"), "hour" : "2018-02-24T10:00:00", "user" : "User_16" }
{ "_id" : ObjectId("5abfe0045397feaeda624436"), "hour" : "2018-03-01T02:00:00", "user" : "User_26" }
{ "_id" : ObjectId("5abfe0045397feaeda624437"), "hour" : "2018-02-23T04:00:00", "user" : "User_14" }
{ "_id" : ObjectId("5abfe0045397feaeda624438"), "hour" : "2018-02-20T08:00:00", "user" : "User_5" }
{ "_id" : ObjectId("5abfe0045397feaeda624439"), "hour" : "2018-02-21T20:00:00", "user" : "User_32" }
{ "_id" : ObjectId("5abfe0045397feaeda62443a"), "hour" : "2018-03-02T06:00:00", "user" : "User_0" }
{ "_id" : ObjectId("5abfe0045397feaeda62443b"), "hour" : "2018-03-01T00:00:00", "user" : "User_19" }
{ "_id" : ObjectId("5abfe0045397feaeda62443c"), "hour" : "2018-02-21T14:00:00", "user" : "User_1" }
{ "_id" : ObjectId("5abfe0045397feaeda62443d"), "hour" : "2018-02-22T12:00:00", "user" : "User_0" }
{ "_id" : ObjectId("5abfe0045397feaeda62443e"), "hour" : "2018-03-02T14:00:00", "user" : "User_39" }
{ "_id" : ObjectId("5abfe0045397feaeda62443f"), "hour" : "2018-02-21T05:00:00", "user" : "User_31" }
{ "_id" : ObjectId("5abfe0045397feaeda624440"), "hour" : "2018-02-28T03:00:00", "user" : "User_27" }
{ "_id" : ObjectId("5abfe0045397feaeda624441"), "hour" : "2018-02-21T19:00:00", "user" : "User_36" }
{ "_id" : ObjectId("5abfe0045397feaeda624442"), "hour" : "2018-03-01T21:00:00", "user" : "User_27" }
{ "_id" : ObjectId("5abfe0045397feaeda624443"), "hour" : "2018-03-03T19:00:00", "user" : "User_15" }
Type "it" for more
```

Our Mongo queries will consist of the following steps inside an aggregation pipeline:

1. Transform the full timestamp into its day part and select this field and the url or user--depending on the report.
2. Group the results based on day and build a set without duplicates.
3. Count the number of elements in the grouped set from step 3.
4. Sort the results by the date.

**Report 1**
```
> db.hour_url.aggregate([ 
{$project : {day :  {$substr: ["$hour", 0, 10]}, url : "$url"}},
{$group : {_id: "$day", uniqueCount : {$addToSet : "$url"}}},
{$project : {day : 1, num_url : {$size : "$uniqueCount"}}},
{$sort : {_id : 1}}
])
{ "_id" : "2018-02-20", "num_url" : 10 }
{ "_id" : "2018-02-21", "num_url" : 10 }
{ "_id" : "2018-02-22", "num_url" : 10 }
{ "_id" : "2018-02-23", "num_url" : 10 }
{ "_id" : "2018-02-24", "num_url" : 10 }
{ "_id" : "2018-02-25", "num_url" : 10 }
{ "_id" : "2018-02-26", "num_url" : 10 }
{ "_id" : "2018-02-27", "num_url" : 10 }
{ "_id" : "2018-02-28", "num_url" : 10 }
{ "_id" : "2018-03-01", "num_url" : 10 }
{ "_id" : "2018-03-02", "num_url" : 10 }
{ "_id" : "2018-03-03", "num_url" : 10 }
{ "_id" : "2018-03-04", "num_url" : 10 }
```

**Report 2**
```
$ db.hour_user.aggregate([ 
{$project : {day :  {$substr: ["$hour", 0, 10]}, user : "$user"}},
{$group : {_id: "$day", uniqueCount : {$addToSet : "$user"}}},
{$project : {day : 1, num_user : {$size : "$uniqueCount"}}},
{$sort : {_id : 1}}
])
{ "_id" : "2018-02-20", "num_user" : 40 }
{ "_id" : "2018-02-21", "num_user" : 40 }
{ "_id" : "2018-02-22", "num_user" : 40 }
{ "_id" : "2018-02-23", "num_user" : 40 }
{ "_id" : "2018-02-24", "num_user" : 40 }
{ "_id" : "2018-02-25", "num_user" : 40 }
{ "_id" : "2018-02-26", "num_user" : 40 }
{ "_id" : "2018-02-27", "num_user" : 40 }
{ "_id" : "2018-02-28", "num_user" : 40 }
{ "_id" : "2018-03-01", "num_user" : 40 }
{ "_id" : "2018-03-02", "num_user" : 40 }
{ "_id" : "2018-03-03", "num_user" : 40 }
{ "_id" : "2018-03-04", "num_user" : 40 }
```

Our schema and strategies were very similar for both MariaDB and MongoDB, and both used essentially the same schema. An advantage of this approach is that we could very easily generate new monthly reports as well (assuming the datasizes do not grow too large). If we were using these daily views very frequently (with no need for reports with granularity less than a day) and wanted to also generate monthly reports, we might consider modifying the Spark job to instead generate all the unique URLs/users for each day. This would keep data sizes small and allow us to generate the monthly report similar to how we did here with very few code modifications.

## Problem 3

We'll create a new database to hold the results written from Spark:
```
MariaDB [(none)]> create database hw8_spark;
Query OK, 1 row affected (0.01 sec)
```

We modify our Spark jobs again. The major difference here is we will collect the final results in a dataframe to write to MariaDB with the JDBC connector.

`unique_url_mariadb.py`
```
"""
Get unique URLs by hour
"""

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
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

# Prepare the results in a dataframe
hour_url = hour_url.map(lambda x: Row(hour=x.split(' ')[0], url=x.split(' ')[1]))
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(hour_url)

# Save the results to MariaDB
properties={'user':'root', 'driver':'org.mariadb.jdbc.Driver'}
table='hour_url'
db_locatio='jdbc:mysql://localhost:3306/hw8_spark'
df.write.mode('overwrite').jdbc(url=db_locatio, table=table, mode='overwrite', properties=properties)

```

`unique_users_mariadb.py`
```
"""
Get unique visitors by URLs by hour
"""

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
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

# Prepare the results in a dataframe
hour_user = hour_user.map(lambda x: Row(hour=x.split(' ')[0], user=x.split(' ')[1]))
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(hour_user)

# Save the results to MariaDB
properties={'user':'root', 'driver':'org.mariadb.jdbc.Driver'}
table='hour_user'
db_locatio='jdbc:mysql://localhost:3306/hw8_spark'
df.write.mode('overwrite').jdbc(url=db_locatio, table=table, mode='overwrite', properties=properties)


```

Now we run the spark jobs and include the MariaDB ODBC connector jar downloaded from the MariaDB [**website**](https://downloads.mariadb.com/Connectors/java/connector-java-2.2.3/mariadb-java-client-2.2.3.jar).

We launch the first job:
```
$ spark-submit --jars mariadb-java-client-2.2.3.jar unique_url_mariadb.py 
2018-03-31 17:00:36 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2018-03-31 17:00:37 INFO  SparkContext:54 - Running Spark version 2.3.0
2018-03-31 17:00:37 INFO  SparkContext:54 - Submitted application: unique_url
2018-03-31 17:00:37 INFO  SecurityManager:54 - Changing view acls to: david.shaub
2018-03-31 17:00:37 INFO  SecurityManager:54 - Changing modify acls to: david.shaub
2018-03-31 17:00:37 INFO  SecurityManager:54 - Changing view acls groups to: 
2018-03-31 17:00:37 INFO  SecurityManager:54 - Changing modify acls groups to: 
2018-03-31 17:00:37 INFO  SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(david.shaub); groups with view permissions: Set(); users  with modify permissions: Set(david.shaub); groups with modify permissions: Set()
2018-03-31 17:00:37 INFO  Utils:54 - Successfully started service 'sparkDriver' on port 55135.
2018-03-31 17:00:37 INFO  SparkEnv:54 - Registering MapOutputTracker
2018-03-31 17:00:37 INFO  SparkEnv:54 - Registering BlockManagerMaster
2018-03-31 17:00:37 INFO  BlockManagerMasterEndpoint:54 - Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
2018-03-31 17:00:37 INFO  BlockManagerMasterEndpoint:54 - BlockManagerMasterEndpoint up
2018-03-31 17:00:37 INFO  DiskBlockManager:54 - Created local directory at /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/blockmgr-18fc2f90-a76b-49d5-9d27-dacd788d6b60
2018-03-31 17:00:37 INFO  MemoryStore:54 - MemoryStore started with capacity 366.3 MB
2018-03-31 17:00:37 INFO  SparkEnv:54 - Registering OutputCommitCoordinator
2018-03-31 17:00:37 INFO  log:192 - Logging initialized @1999ms
2018-03-31 17:00:37 INFO  Server:346 - jetty-9.3.z-SNAPSHOT
2018-03-31 17:00:37 INFO  Server:414 - Started @2070ms
2018-03-31 17:00:37 INFO  AbstractConnector:278 - Started ServerConnector@1b8eab92{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2018-03-31 17:00:37 INFO  Utils:54 - Successfully started service 'SparkUI' on port 4040.
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@384bfe8e{/jobs,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@365c9fee{/jobs/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2f486ae3{/jobs/job,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@345bb6d7{/jobs/job/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@662aec5d{/stages,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@a598d09{/stages/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7f41662d{/stages/stage,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@614b24a6{/stages/stage/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@15cf56ac{/stages/pool,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2f602b73{/stages/pool/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6eb99a2e{/storage,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@56efed4b{/storage/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@397aab0c{/storage/rdd,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@176f34e3{/storage/rdd/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@35a5a4ca{/environment,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@1c099280{/environment/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@704778c6{/executors,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@1e619201{/executors/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6faf6f3{/executors/threadDump,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@5637e93e{/executors/threadDump/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@1dfd3a76{/static,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@377fc49b{/,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@56f36e29{/api,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@50d730e9{/jobs/job/kill,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@7eeca67e{/stages/stage/kill,null,AVAILABLE,@Spark}
2018-03-31 17:00:37 INFO  SparkUI:54 - Bound SparkUI to 0.0.0.0, and started at http://usmac2752dshau.local:4040
2018-03-31 17:00:38 INFO  SparkContext:54 - Added JAR file:///Users/david.shaub/PBDP/hw8/mariadb-java-client-2.2.3.jar at spark://usmac2752dshau.local:55135/jars/mariadb-java-client-2.2.3.jar with timestamp 1522537238056
2018-03-31 17:00:38 INFO  SparkContext:54 - Added file file:/Users/david.shaub/PBDP/hw8/unique_url_mariadb.py at file:/Users/david.shaub/PBDP/hw8/unique_url_mariadb.py with timestamp 1522537238058
2018-03-31 17:00:38 INFO  Utils:54 - Copying /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf/userFiles-a16af93f-236b-4fb2-a155-83fdfc04c8c5/unique_url_mariadb.py
2018-03-31 17:00:38 INFO  Executor:54 - Starting executor ID driver on host localhost
2018-03-31 17:00:38 INFO  Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 55136.
2018-03-31 17:00:38 INFO  NettyBlockTransferService:54 - Server created on usmac2752dshau.local:55136
2018-03-31 17:00:38 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
2018-03-31 17:00:38 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, usmac2752dshau.local, 55136, None)
2018-03-31 17:00:38 INFO  BlockManagerMasterEndpoint:54 - Registering block manager usmac2752dshau.local:55136 with 366.3 MB RAM, BlockManagerId(driver, usmac2752dshau.local, 55136, None)
2018-03-31 17:00:38 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, usmac2752dshau.local, 55136, None)
2018-03-31 17:00:38 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, usmac2752dshau.local, 55136, None)
2018-03-31 17:00:38 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@49b6b9c9{/metrics/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:38 INFO  MemoryStore:54 - Block broadcast_0 stored as values in memory (estimated size 236.5 KB, free 366.1 MB)
2018-03-31 17:00:38 INFO  MemoryStore:54 - Block broadcast_0_piece0 stored as bytes in memory (estimated size 22.9 KB, free 366.0 MB)
2018-03-31 17:00:38 INFO  BlockManagerInfo:54 - Added broadcast_0_piece0 in memory on usmac2752dshau.local:55136 (size: 22.9 KB, free: 366.3 MB)
2018-03-31 17:00:38 INFO  SparkContext:54 - Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
2018-03-31 17:00:38 INFO  FileInputFormat:249 - Total input paths to process : 20
2018-03-31 17:00:39 INFO  SparkContext:54 - Starting job: runJob at PythonRDD.scala:141
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Registering RDD 3 (distinct at /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py:24)
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Got job 0 (runJob at PythonRDD.scala:141) with 1 output partitions
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Final stage: ResultStage 1 (runJob at PythonRDD.scala:141)
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Parents of final stage: List(ShuffleMapStage 0)
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Missing parents: List(ShuffleMapStage 0)
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Submitting ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py:24), which has no missing parents
2018-03-31 17:00:39 INFO  MemoryStore:54 - Block broadcast_1 stored as values in memory (estimated size 9.7 KB, free 366.0 MB)
2018-03-31 17:00:39 INFO  MemoryStore:54 - Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.0 KB, free 366.0 MB)
2018-03-31 17:00:39 INFO  BlockManagerInfo:54 - Added broadcast_1_piece0 in memory on usmac2752dshau.local:55136 (size: 6.0 KB, free: 366.3 MB)
2018-03-31 17:00:39 INFO  SparkContext:54 - Created broadcast 1 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:00:39 INFO  DAGScheduler:54 - Submitting 20 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py:24) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
2018-03-31 17:00:39 INFO  TaskSchedulerImpl:54 - Adding task set 0.0 with 20 tasks
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 2.0 in stage 0.0 (TID 2, localhost, executor driver, partition 2, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 3.0 in stage 0.0 (TID 3, localhost, executor driver, partition 3, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 4.0 in stage 0.0 (TID 4, localhost, executor driver, partition 4, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 5.0 in stage 0.0 (TID 5, localhost, executor driver, partition 5, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 6.0 in stage 0.0 (TID 6, localhost, executor driver, partition 6, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  TaskSetManager:54 - Starting task 7.0 in stage 0.0 (TID 7, localhost, executor driver, partition 7, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 4.0 in stage 0.0 (TID 4)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 2.0 in stage 0.0 (TID 2)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 7.0 in stage 0.0 (TID 7)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 6.0 in stage 0.0 (TID 6)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 5.0 in stage 0.0 (TID 5)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 0.0 in stage 0.0 (TID 0)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 3.0 in stage 0.0 (TID 3)
2018-03-31 17:00:39 INFO  Executor:54 - Running task 1.0 in stage 0.0 (TID 1)
2018-03-31 17:00:39 INFO  Executor:54 - Fetching file:/Users/david.shaub/PBDP/hw8/unique_url_mariadb.py with timestamp 1522537238058
2018-03-31 17:00:39 INFO  Utils:54 - /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py has been previously copied to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf/userFiles-a16af93f-236b-4fb2-a155-83fdfc04c8c5/unique_url_mariadb.py
2018-03-31 17:00:39 INFO  Executor:54 - Fetching spark://usmac2752dshau.local:55135/jars/mariadb-java-client-2.2.3.jar with timestamp 1522537238056
2018-03-31 17:00:39 INFO  TransportClientFactory:267 - Successfully created connection to USMAC2752DSHAU.local/192.168.10.238:55135 after 44 ms (0 ms spent in bootstraps)
2018-03-31 17:00:39 INFO  Utils:54 - Fetching spark://usmac2752dshau.local:55135/jars/mariadb-java-client-2.2.3.jar to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf/userFiles-a16af93f-236b-4fb2-a155-83fdfc04c8c5/fetchFileTemp2117785478173407575.tmp
2018-03-31 17:00:39 INFO  Executor:54 - Adding file:/private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf/userFiles-a16af93f-236b-4fb2-a155-83fdfc04c8c5/mariadb-java-client-2.2.3.jar to class loader
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_14.txt:0+15319508
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_11.txt:0+34467772
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_12.txt:0+11489720
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_13.txt:0+11489816
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_1.txt:0+34467772
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_15.txt:0+34468772
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_10.txt:0+19148972
2018-03-31 17:00:39 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_16.txt:0+3830000
2018-03-31 17:00:41 INFO  PythonRunner:54 - Times: total = 1077, boot = 293, init = 24, finish = 760
2018-03-31 17:00:41 INFO  Executor:54 - Finished task 7.0 in stage 0.0 (TID 7). 1637 bytes result sent to driver
2018-03-31 17:00:41 INFO  TaskSetManager:54 - Starting task 8.0 in stage 0.0 (TID 8, localhost, executor driver, partition 8, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:41 INFO  Executor:54 - Running task 8.0 in stage 0.0 (TID 8)
2018-03-31 17:00:41 INFO  TaskSetManager:54 - Finished task 7.0 in stage 0.0 (TID 7) in 1835 ms on localhost (executor driver) (1/20)
2018-03-31 17:00:41 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_17.txt:0+15318540
2018-03-31 17:00:42 INFO  PythonRunner:54 - Times: total = 2517, boot = 281, init = 48, finish = 2188
2018-03-31 17:00:42 INFO  PythonRunner:54 - Times: total = 2520, boot = 295, init = 25, finish = 2200
2018-03-31 17:00:42 INFO  Executor:54 - Finished task 3.0 in stage 0.0 (TID 3). 1594 bytes result sent to driver
2018-03-31 17:00:42 INFO  TaskSetManager:54 - Starting task 9.0 in stage 0.0 (TID 9, localhost, executor driver, partition 9, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:42 INFO  Executor:54 - Running task 9.0 in stage 0.0 (TID 9)
2018-03-31 17:00:42 INFO  TaskSetManager:54 - Finished task 3.0 in stage 0.0 (TID 3) in 2883 ms on localhost (executor driver) (2/20)
2018-03-31 17:00:42 INFO  Executor:54 - Finished task 4.0 in stage 0.0 (TID 4). 1594 bytes result sent to driver
2018-03-31 17:00:42 INFO  TaskSetManager:54 - Starting task 10.0 in stage 0.0 (TID 10, localhost, executor driver, partition 10, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:42 INFO  Executor:54 - Running task 10.0 in stage 0.0 (TID 10)
2018-03-31 17:00:42 INFO  TaskSetManager:54 - Finished task 4.0 in stage 0.0 (TID 4) in 2885 ms on localhost (executor driver) (3/20)
2018-03-31 17:00:42 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_18.txt:0+30638280
2018-03-31 17:00:42 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_19.txt:0+26809760
2018-03-31 17:00:43 INFO  PythonRunner:54 - Times: total = 3279, boot = 279, init = 49, finish = 2951
2018-03-31 17:00:43 INFO  Executor:54 - Finished task 5.0 in stage 0.0 (TID 5). 1637 bytes result sent to driver
2018-03-31 17:00:43 INFO  TaskSetManager:54 - Starting task 11.0 in stage 0.0 (TID 11, localhost, executor driver, partition 11, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:43 INFO  Executor:54 - Running task 11.0 in stage 0.0 (TID 11)
2018-03-31 17:00:43 INFO  TaskSetManager:54 - Finished task 5.0 in stage 0.0 (TID 5) in 3596 ms on localhost (executor driver) (4/20)
2018-03-31 17:00:43 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_2.txt:0+11489720
2018-03-31 17:00:43 INFO  PythonRunner:54 - Times: total = 4064, boot = 283, init = 55, finish = 3726
2018-03-31 17:00:43 INFO  Executor:54 - Finished task 1.0 in stage 0.0 (TID 1). 1594 bytes result sent to driver
2018-03-31 17:00:43 INFO  TaskSetManager:54 - Starting task 12.0 in stage 0.0 (TID 12, localhost, executor driver, partition 12, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:00:43 INFO  Executor:54 - Running task 12.0 in stage 0.0 (TID 12)
2018-03-31 17:00:43 INFO  TaskSetManager:54 - Finished task 1.0 in stage 0.0 (TID 1) in 4397 ms on localhost (executor driver) (5/20)
2018-03-31 17:00:43 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_20.txt:0+19148972
2018-03-31 17:00:44 INFO  PythonRunner:54 - Times: total = 2865, boot = -469, init = 474, finish = 2860
2018-03-31 17:00:44 INFO  Executor:54 - Finished task 8.0 in stage 0.0 (TID 8). 1594 bytes result sent to driver
2018-03-31 17:00:44 INFO  TaskSetManager:54 - Starting task 13.0 in stage 0.0 (TID 13, localhost, executor driver, partition 13, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:44 INFO  Executor:54 - Running task 13.0 in stage 0.0 (TID 13)
2018-03-31 17:00:44 INFO  TaskSetManager:54 - Finished task 8.0 in stage 0.0 (TID 8) in 2936 ms on localhost (executor driver) (6/20)
2018-03-31 17:00:44 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_3.txt:0+11489816
2018-03-31 17:00:45 INFO  PythonRunner:54 - Times: total = 2333, boot = 31, init = 1, finish = 2301
2018-03-31 17:00:45 INFO  Executor:54 - Finished task 11.0 in stage 0.0 (TID 11). 1594 bytes result sent to driver
2018-03-31 17:00:45 INFO  TaskSetManager:54 - Starting task 14.0 in stage 0.0 (TID 14, localhost, executor driver, partition 14, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:45 INFO  Executor:54 - Running task 14.0 in stage 0.0 (TID 14)
2018-03-31 17:00:45 INFO  TaskSetManager:54 - Finished task 11.0 in stage 0.0 (TID 11) in 2391 ms on localhost (executor driver) (7/20)
2018-03-31 17:00:45 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_4.txt:0+15319508
2018-03-31 17:00:46 INFO  PythonRunner:54 - Times: total = 2163, boot = -35, init = 37, finish = 2161
2018-03-31 17:00:46 INFO  Executor:54 - Finished task 13.0 in stage 0.0 (TID 13). 1594 bytes result sent to driver
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Starting task 15.0 in stage 0.0 (TID 15, localhost, executor driver, partition 15, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:46 INFO  Executor:54 - Running task 15.0 in stage 0.0 (TID 15)
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Finished task 13.0 in stage 0.0 (TID 13) in 2207 ms on localhost (executor driver) (8/20)
2018-03-31 17:00:46 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_5.txt:0+34468772
2018-03-31 17:00:46 INFO  PythonRunner:54 - Times: total = 7001, boot = 289, init = 23, finish = 6689
2018-03-31 17:00:46 INFO  Executor:54 - Finished task 6.0 in stage 0.0 (TID 6). 1594 bytes result sent to driver
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Starting task 16.0 in stage 0.0 (TID 16, localhost, executor driver, partition 16, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Finished task 6.0 in stage 0.0 (TID 6) in 7329 ms on localhost (executor driver) (9/20)
2018-03-31 17:00:46 INFO  Executor:54 - Running task 16.0 in stage 0.0 (TID 16)
2018-03-31 17:00:46 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_6.txt:0+3830000
2018-03-31 17:00:46 INFO  PythonRunner:54 - Times: total = 7045, boot = 286, init = 41, finish = 6718
2018-03-31 17:00:46 INFO  PythonRunner:54 - Times: total = 7052, boot = 276, init = 36, finish = 6740
2018-03-31 17:00:46 INFO  Executor:54 - Finished task 0.0 in stage 0.0 (TID 0). 1594 bytes result sent to driver
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Starting task 17.0 in stage 0.0 (TID 17, localhost, executor driver, partition 17, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:46 INFO  Executor:54 - Running task 17.0 in stage 0.0 (TID 17)
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Finished task 0.0 in stage 0.0 (TID 0) in 7395 ms on localhost (executor driver) (10/20)
2018-03-31 17:00:46 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_7.txt:0+15318540
2018-03-31 17:00:46 INFO  Executor:54 - Finished task 2.0 in stage 0.0 (TID 2). 1594 bytes result sent to driver
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Starting task 18.0 in stage 0.0 (TID 18, localhost, executor driver, partition 18, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:46 INFO  Executor:54 - Running task 18.0 in stage 0.0 (TID 18)
2018-03-31 17:00:46 INFO  TaskSetManager:54 - Finished task 2.0 in stage 0.0 (TID 2) in 7387 ms on localhost (executor driver) (11/20)
2018-03-31 17:00:46 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_8.txt:0+30638280
2018-03-31 17:00:47 INFO  PythonRunner:54 - Times: total = 729, boot = -29, init = 48, finish = 710
2018-03-31 17:00:47 INFO  Executor:54 - Finished task 16.0 in stage 0.0 (TID 16). 1594 bytes result sent to driver
2018-03-31 17:00:47 INFO  TaskSetManager:54 - Starting task 19.0 in stage 0.0 (TID 19, localhost, executor driver, partition 19, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:00:47 INFO  TaskSetManager:54 - Finished task 16.0 in stage 0.0 (TID 16) in 788 ms on localhost (executor driver) (12/20)
2018-03-31 17:00:47 INFO  Executor:54 - Running task 19.0 in stage 0.0 (TID 19)
2018-03-31 17:00:47 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_9.txt:0+26809760
2018-03-31 17:00:47 INFO  PythonRunner:54 - Times: total = 5284, boot = -31, init = 56, finish = 5259
2018-03-31 17:00:47 INFO  PythonRunner:54 - Times: total = 3756, boot = 34, init = 0, finish = 3722
2018-03-31 17:00:47 INFO  Executor:54 - Finished task 10.0 in stage 0.0 (TID 10). 1637 bytes result sent to driver
2018-03-31 17:00:47 INFO  Executor:54 - Finished task 12.0 in stage 0.0 (TID 12). 1594 bytes result sent to driver
2018-03-31 17:00:47 INFO  TaskSetManager:54 - Finished task 10.0 in stage 0.0 (TID 10) in 5311 ms on localhost (executor driver) (13/20)
2018-03-31 17:00:47 INFO  TaskSetManager:54 - Finished task 12.0 in stage 0.0 (TID 12) in 3802 ms on localhost (executor driver) (14/20)
2018-03-31 17:00:48 INFO  PythonRunner:54 - Times: total = 5864, boot = 16, init = 0, finish = 5848
2018-03-31 17:00:48 INFO  PythonRunner:54 - Times: total = 2765, boot = -48, init = 50, finish = 2763
2018-03-31 17:00:48 INFO  Executor:54 - Finished task 9.0 in stage 0.0 (TID 9). 1594 bytes result sent to driver
2018-03-31 17:00:48 INFO  Executor:54 - Finished task 14.0 in stage 0.0 (TID 14). 1594 bytes result sent to driver
2018-03-31 17:00:48 INFO  TaskSetManager:54 - Finished task 9.0 in stage 0.0 (TID 9) in 5904 ms on localhost (executor driver) (15/20)
2018-03-31 17:00:48 INFO  TaskSetManager:54 - Finished task 14.0 in stage 0.0 (TID 14) in 2800 ms on localhost (executor driver) (16/20)
2018-03-31 17:00:48 INFO  PythonRunner:54 - Times: total = 2093, boot = 20, init = 0, finish = 2073
2018-03-31 17:00:48 INFO  Executor:54 - Finished task 17.0 in stage 0.0 (TID 17). 1594 bytes result sent to driver
2018-03-31 17:00:48 INFO  TaskSetManager:54 - Finished task 17.0 in stage 0.0 (TID 17) in 2134 ms on localhost (executor driver) (17/20)
2018-03-31 17:00:50 INFO  PythonRunner:54 - Times: total = 2467, boot = -11, init = 17, finish = 2461
2018-03-31 17:00:50 INFO  PythonRunner:54 - Times: total = 3190, boot = 17, init = 1, finish = 3172
2018-03-31 17:00:50 INFO  Executor:54 - Finished task 19.0 in stage 0.0 (TID 19). 1594 bytes result sent to driver
2018-03-31 17:00:50 INFO  TaskSetManager:54 - Finished task 19.0 in stage 0.0 (TID 19) in 2502 ms on localhost (executor driver) (18/20)
2018-03-31 17:00:50 INFO  Executor:54 - Finished task 18.0 in stage 0.0 (TID 18). 1594 bytes result sent to driver
2018-03-31 17:00:50 INFO  TaskSetManager:54 - Finished task 18.0 in stage 0.0 (TID 18) in 3222 ms on localhost (executor driver) (19/20)
2018-03-31 17:00:50 INFO  PythonRunner:54 - Times: total = 3729, boot = -34, init = 66, finish = 3697
2018-03-31 17:00:50 INFO  Executor:54 - Finished task 15.0 in stage 0.0 (TID 15). 1594 bytes result sent to driver
2018-03-31 17:00:50 INFO  TaskSetManager:54 - Finished task 15.0 in stage 0.0 (TID 15) in 3762 ms on localhost (executor driver) (20/20)
2018-03-31 17:00:50 INFO  TaskSchedulerImpl:54 - Removed TaskSet 0.0, whose tasks have all completed, from pool 
2018-03-31 17:00:50 INFO  DAGScheduler:54 - ShuffleMapStage 0 (distinct at /Users/david.shaub/PBDP/hw8/unique_url_mariadb.py:24) finished in 10.849 s
2018-03-31 17:00:50 INFO  DAGScheduler:54 - looking for newly runnable stages
2018-03-31 17:00:50 INFO  DAGScheduler:54 - running: Set()
2018-03-31 17:00:50 INFO  DAGScheduler:54 - waiting: Set(ResultStage 1)
2018-03-31 17:00:50 INFO  DAGScheduler:54 - failed: Set()
2018-03-31 17:00:50 INFO  DAGScheduler:54 - Submitting ResultStage 1 (PythonRDD[6] at RDD at PythonRDD.scala:48), which has no missing parents
2018-03-31 17:00:50 INFO  MemoryStore:54 - Block broadcast_2 stored as values in memory (estimated size 7.6 KB, free 366.0 MB)
2018-03-31 17:00:50 INFO  MemoryStore:54 - Block broadcast_2_piece0 stored as bytes in memory (estimated size 4.6 KB, free 366.0 MB)
2018-03-31 17:00:50 INFO  BlockManagerInfo:54 - Added broadcast_2_piece0 in memory on usmac2752dshau.local:55136 (size: 4.6 KB, free: 366.3 MB)
2018-03-31 17:00:50 INFO  SparkContext:54 - Created broadcast 2 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:00:50 INFO  DAGScheduler:54 - Submitting 1 missing tasks from ResultStage 1 (PythonRDD[6] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
2018-03-31 17:00:50 INFO  TaskSchedulerImpl:54 - Adding task set 1.0 with 1 tasks
2018-03-31 17:00:50 INFO  TaskSetManager:54 - Starting task 0.0 in stage 1.0 (TID 20, localhost, executor driver, partition 0, ANY, 7649 bytes)
2018-03-31 17:00:50 INFO  Executor:54 - Running task 0.0 in stage 1.0 (TID 20)
2018-03-31 17:00:50 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:50 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 4 ms
2018-03-31 17:00:50 INFO  PythonRunner:54 - Times: total = 15, boot = -2587, init = 2596, finish = 6
2018-03-31 17:00:50 INFO  Executor:54 - Finished task 0.0 in stage 1.0 (TID 20). 1719 bytes result sent to driver
2018-03-31 17:00:50 INFO  TaskSetManager:54 - Finished task 0.0 in stage 1.0 (TID 20) in 44 ms on localhost (executor driver) (1/1)
2018-03-31 17:00:50 INFO  TaskSchedulerImpl:54 - Removed TaskSet 1.0, whose tasks have all completed, from pool 
2018-03-31 17:00:50 INFO  DAGScheduler:54 - ResultStage 1 (runJob at PythonRDD.scala:141) finished in 0.054 s
2018-03-31 17:00:50 INFO  DAGScheduler:54 - Job 0 finished: runJob at PythonRDD.scala:141, took 10.976376 s
2018-03-31 17:00:50 INFO  SharedState:54 - Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/Users/david.shaub/PBDP/hw8/spark-warehouse/').
2018-03-31 17:00:50 INFO  SharedState:54 - Warehouse path is 'file:/Users/david.shaub/PBDP/hw8/spark-warehouse/'.
2018-03-31 17:00:50 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@56777d06{/SQL,null,AVAILABLE,@Spark}
2018-03-31 17:00:50 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@af2613{/SQL/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:50 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@59966a19{/SQL/execution,null,AVAILABLE,@Spark}
2018-03-31 17:00:50 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@ace5b46{/SQL/execution/json,null,AVAILABLE,@Spark}
2018-03-31 17:00:50 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@bb1131d{/static/sql,null,AVAILABLE,@Spark}
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 32
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 37
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 29
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 44
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 45
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 48
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 47
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 39
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 50
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 42
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 36
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 26
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 31
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 43
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 34
2018-03-31 17:00:50 INFO  BlockManagerInfo:54 - Removed broadcast_2_piece0 on usmac2752dshau.local:55136 in memory (size: 4.6 KB, free: 366.3 MB)
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 38
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 27
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 33
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 40
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 35
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 41
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 46
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 30
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 49
2018-03-31 17:00:50 INFO  ContextCleaner:54 - Cleaned accumulator 28
2018-03-31 17:00:50 INFO  StateStoreCoordinatorRef:54 - Registered StateStoreCoordinator endpoint
2018-03-31 17:00:51 INFO  BlockManagerInfo:54 - Removed broadcast_1_piece0 on usmac2752dshau.local:55136 in memory (size: 6.0 KB, free: 366.3 MB)
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 17
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 22
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 1
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 7
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 3
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 9
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 16
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 14
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 20
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 19
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 12
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 6
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 10
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 5
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 18
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 8
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 21
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 15
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 24
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 13
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 23
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 25
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 11
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 2
2018-03-31 17:00:51 INFO  ContextCleaner:54 - Cleaned accumulator 4
2018-03-31 17:00:52 INFO  SparkContext:54 - Starting job: jdbc at NativeMethodAccessorImpl.java:0
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Got job 1 (jdbc at NativeMethodAccessorImpl.java:0) with 20 output partitions
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Final stage: ResultStage 3 (jdbc at NativeMethodAccessorImpl.java:0)
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Parents of final stage: List(ShuffleMapStage 2)
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Missing parents: List()
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Submitting ResultStage 3 (MapPartitionsRDD[13] at jdbc at NativeMethodAccessorImpl.java:0), which has no missing parents
2018-03-31 17:00:52 INFO  MemoryStore:54 - Block broadcast_3 stored as values in memory (estimated size 17.4 KB, free 366.0 MB)
2018-03-31 17:00:52 INFO  MemoryStore:54 - Block broadcast_3_piece0 stored as bytes in memory (estimated size 9.6 KB, free 366.0 MB)
2018-03-31 17:00:52 INFO  BlockManagerInfo:54 - Added broadcast_3_piece0 in memory on usmac2752dshau.local:55136 (size: 9.6 KB, free: 366.3 MB)
2018-03-31 17:00:52 INFO  SparkContext:54 - Created broadcast 3 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Submitting 20 missing tasks from ResultStage 3 (MapPartitionsRDD[13] at jdbc at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
2018-03-31 17:00:52 INFO  TaskSchedulerImpl:54 - Adding task set 3.0 with 20 tasks
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 0.0 in stage 3.0 (TID 21, localhost, executor driver, partition 0, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 1.0 in stage 3.0 (TID 22, localhost, executor driver, partition 1, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 2.0 in stage 3.0 (TID 23, localhost, executor driver, partition 2, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 3.0 in stage 3.0 (TID 24, localhost, executor driver, partition 3, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 4.0 in stage 3.0 (TID 25, localhost, executor driver, partition 4, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 5.0 in stage 3.0 (TID 26, localhost, executor driver, partition 5, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 6.0 in stage 3.0 (TID 27, localhost, executor driver, partition 6, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 7.0 in stage 3.0 (TID 28, localhost, executor driver, partition 7, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 0.0 in stage 3.0 (TID 21)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 2.0 in stage 3.0 (TID 23)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 5.0 in stage 3.0 (TID 26)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 1.0 in stage 3.0 (TID 22)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 7.0 in stage 3.0 (TID 28)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 6.0 in stage 3.0 (TID 27)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 4.0 in stage 3.0 (TID 25)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 3.0 in stage 3.0 (TID 24)
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  CodeGenerator:54 - Code generated in 193.624824 ms
2018-03-31 17:00:52 INFO  CodeGenerator:54 - Code generated in 25.330731 ms
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 27, boot = -1960, init = 1975, finish = 12
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 28, boot = -2047, init = 2064, finish = 11
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 35, boot = -3987, init = 4003, finish = 19
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 34, boot = -3270, init = 3279, finish = 25
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 33, boot = -3979, init = 3995, finish = 17
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 52, boot = -2174, init = 2184, finish = 42
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 52, boot = -2172, init = 2195, finish = 29
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 40, boot = -4559, init = 4573, finish = 26
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 3.0 in stage 3.0 (TID 24). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 8.0 in stage 3.0 (TID 29, localhost, executor driver, partition 8, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 8.0 in stage 3.0 (TID 29)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 3.0 in stage 3.0 (TID 24) in 513 ms on localhost (executor driver) (1/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 6.0 in stage 3.0 (TID 27). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 9.0 in stage 3.0 (TID 30, localhost, executor driver, partition 9, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 6.0 in stage 3.0 (TID 27) in 509 ms on localhost (executor driver) (2/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 9.0 in stage 3.0 (TID 30)
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 13, boot = -442, init = 447, finish = 8
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 12, boot = -447, init = 451, finish = 8
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 0.0 in stage 3.0 (TID 21). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 10.0 in stage 3.0 (TID 31, localhost, executor driver, partition 10, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 0.0 in stage 3.0 (TID 21) in 566 ms on localhost (executor driver) (3/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 10.0 in stage 3.0 (TID 31)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 1.0 in stage 3.0 (TID 22). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 11.0 in stage 3.0 (TID 32, localhost, executor driver, partition 11, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 11.0 in stage 3.0 (TID 32)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 1.0 in stage 3.0 (TID 22) in 572 ms on localhost (executor driver) (4/20)
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 12, boot = -503, init = 507, finish = 8
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 12, boot = -501, init = 505, finish = 8
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 4.0 in stage 3.0 (TID 25). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 12.0 in stage 3.0 (TID 33, localhost, executor driver, partition 12, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 4.0 in stage 3.0 (TID 25) in 611 ms on localhost (executor driver) (5/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 2.0 in stage 3.0 (TID 23). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 5.0 in stage 3.0 (TID 26). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  Executor:54 - Running task 12.0 in stage 3.0 (TID 33)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 13.0 in stage 3.0 (TID 34, localhost, executor driver, partition 13, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 8.0 in stage 3.0 (TID 29). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 14.0 in stage 3.0 (TID 35, localhost, executor driver, partition 14, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 13.0 in stage 3.0 (TID 34)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 7.0 in stage 3.0 (TID 28). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 11.0 in stage 3.0 (TID 32). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 15.0 in stage 3.0 (TID 36, localhost, executor driver, partition 15, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 8.0 in stage 3.0 (TID 29) in 109 ms on localhost (executor driver) (6/20)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 5.0 in stage 3.0 (TID 26) in 616 ms on localhost (executor driver) (7/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 14.0 in stage 3.0 (TID 35)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 2.0 in stage 3.0 (TID 23) in 622 ms on localhost (executor driver) (8/20)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 16.0 in stage 3.0 (TID 37, localhost, executor driver, partition 16, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 17.0 in stage 3.0 (TID 38, localhost, executor driver, partition 17, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 7.0 in stage 3.0 (TID 28) in 618 ms on localhost (executor driver) (9/20)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 11.0 in stage 3.0 (TID 32) in 55 ms on localhost (executor driver) (10/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 15.0 in stage 3.0 (TID 36)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 16.0 in stage 3.0 (TID 37)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 17.0 in stage 3.0 (TID 38)
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 9.0 in stage 3.0 (TID 30). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 10.0 in stage 3.0 (TID 31). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 18.0 in stage 3.0 (TID 39, localhost, executor driver, partition 18, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 9.0 in stage 3.0 (TID 30) in 123 ms on localhost (executor driver) (11/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 18.0 in stage 3.0 (TID 39)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Starting task 19.0 in stage 3.0 (TID 40, localhost, executor driver, partition 19, ANY, 7649 bytes)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 10.0 in stage 3.0 (TID 31) in 84 ms on localhost (executor driver) (12/20)
2018-03-31 17:00:52 INFO  Executor:54 - Running task 19.0 in stage 3.0 (TID 40)
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 19, boot = -548, init = 555, finish = 12
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 23, boot = -546, init = 554, finish = 15
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 26, boot = -559, init = 574, finish = 11
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 16.0 in stage 3.0 (TID 37). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 18, boot = -566, init = 575, finish = 9
2018-03-31 17:00:52 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 7 ms
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 13.0 in stage 3.0 (TID 34). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 16.0 in stage 3.0 (TID 37) in 79 ms on localhost (executor driver) (13/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 12.0 in stage 3.0 (TID 33). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 13.0 in stage 3.0 (TID 34) in 86 ms on localhost (executor driver) (14/20)
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 12.0 in stage 3.0 (TID 33) in 90 ms on localhost (executor driver) (15/20)
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 51, boot = -100, init = 140, finish = 11
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 17.0 in stage 3.0 (TID 38). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 17.0 in stage 3.0 (TID 38) in 84 ms on localhost (executor driver) (16/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 15.0 in stage 3.0 (TID 36). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 15.0 in stage 3.0 (TID 36) in 91 ms on localhost (executor driver) (17/20)
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 17, boot = -154, init = 163, finish = 8
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 16, boot = -104, init = 110, finish = 10
2018-03-31 17:00:52 INFO  PythonRunner:54 - Times: total = 14, boot = -111, init = 117, finish = 8
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 18.0 in stage 3.0 (TID 39). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 18.0 in stage 3.0 (TID 39) in 102 ms on localhost (executor driver) (18/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 19.0 in stage 3.0 (TID 40). 1798 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 19.0 in stage 3.0 (TID 40) in 104 ms on localhost (executor driver) (19/20)
2018-03-31 17:00:52 INFO  Executor:54 - Finished task 14.0 in stage 3.0 (TID 35). 1841 bytes result sent to driver
2018-03-31 17:00:52 INFO  TaskSetManager:54 - Finished task 14.0 in stage 3.0 (TID 35) in 129 ms on localhost (executor driver) (20/20)
2018-03-31 17:00:52 INFO  TaskSchedulerImpl:54 - Removed TaskSet 3.0, whose tasks have all completed, from pool 
2018-03-31 17:00:52 INFO  DAGScheduler:54 - ResultStage 3 (jdbc at NativeMethodAccessorImpl.java:0) finished in 0.774 s
2018-03-31 17:00:52 INFO  DAGScheduler:54 - Job 1 finished: jdbc at NativeMethodAccessorImpl.java:0, took 0.782014 s
2018-03-31 17:00:52 INFO  SparkContext:54 - Invoking stop() from shutdown hook
2018-03-31 17:00:52 INFO  AbstractConnector:318 - Stopped Spark@1b8eab92{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2018-03-31 17:00:52 INFO  SparkUI:54 - Stopped Spark web UI at http://usmac2752dshau.local:4040
2018-03-31 17:00:53 INFO  MapOutputTrackerMasterEndpoint:54 - MapOutputTrackerMasterEndpoint stopped!
2018-03-31 17:00:53 INFO  MemoryStore:54 - MemoryStore cleared
2018-03-31 17:00:53 INFO  BlockManager:54 - BlockManager stopped
2018-03-31 17:00:53 INFO  BlockManagerMaster:54 - BlockManagerMaster stopped
2018-03-31 17:00:53 INFO  OutputCommitCoordinator$OutputCommitCoordinatorEndpoint:54 - OutputCommitCoordinator stopped!
2018-03-31 17:00:53 INFO  SparkContext:54 - Successfully stopped SparkContext
2018-03-31 17:00:53 INFO  ShutdownHookManager:54 - Shutdown hook called
2018-03-31 17:00:53 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-083c652f-7fed-49f4-bf84-9627c493e17e
2018-03-31 17:00:53 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf/pyspark-f36f4a47-438f-4df7-bb91-e6147aadd1d7
2018-03-31 17:00:53 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54d9f413-00cd-4395-a8ce-a3e4cabd7bbf
```

We launch the second job:
```
$ spark-submit --jars mariadb-java-client-2.2.3.jar unique_users_mariadb.py 
2018-03-31 17:02:13 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2018-03-31 17:02:14 INFO  SparkContext:54 - Running Spark version 2.3.0
2018-03-31 17:02:14 INFO  SparkContext:54 - Submitted application: unique_users
2018-03-31 17:02:14 INFO  SecurityManager:54 - Changing view acls to: david.shaub
2018-03-31 17:02:14 INFO  SecurityManager:54 - Changing modify acls to: david.shaub
2018-03-31 17:02:14 INFO  SecurityManager:54 - Changing view acls groups to: 
2018-03-31 17:02:14 INFO  SecurityManager:54 - Changing modify acls groups to: 
2018-03-31 17:02:14 INFO  SecurityManager:54 - SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(david.shaub); groups with view permissions: Set(); users  with modify permissions: Set(david.shaub); groups with modify permissions: Set()
2018-03-31 17:02:14 INFO  Utils:54 - Successfully started service 'sparkDriver' on port 55202.
2018-03-31 17:02:14 INFO  SparkEnv:54 - Registering MapOutputTracker
2018-03-31 17:02:14 INFO  SparkEnv:54 - Registering BlockManagerMaster
2018-03-31 17:02:14 INFO  BlockManagerMasterEndpoint:54 - Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
2018-03-31 17:02:14 INFO  BlockManagerMasterEndpoint:54 - BlockManagerMasterEndpoint up
2018-03-31 17:02:14 INFO  DiskBlockManager:54 - Created local directory at /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/blockmgr-39bed832-8343-4f1b-8264-519343f0fd4d
2018-03-31 17:02:14 INFO  MemoryStore:54 - MemoryStore started with capacity 366.3 MB
2018-03-31 17:02:14 INFO  SparkEnv:54 - Registering OutputCommitCoordinator
2018-03-31 17:02:14 INFO  log:192 - Logging initialized @2392ms
2018-03-31 17:02:14 INFO  Server:346 - jetty-9.3.z-SNAPSHOT
2018-03-31 17:02:14 INFO  Server:414 - Started @2464ms
2018-03-31 17:02:14 INFO  AbstractConnector:278 - Started ServerConnector@5cb9e5e9{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2018-03-31 17:02:14 INFO  Utils:54 - Successfully started service 'SparkUI' on port 4040.
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6b1e5a2d{/jobs,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@72ff4ae1{/jobs/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@10e856b1{/jobs/job,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2e71cbce{/jobs/job/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@265124a3{/stages,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@74deb306{/stages/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@5cdc6a2{/stages/stage,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@bdeda9f{/stages/stage/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@604df590{/stages/pool,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@572b0ad5{/stages/pool/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@1502bdaf{/storage,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@68f587b2{/storage/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@671b1e49{/storage/rdd,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@68e9cddd{/storage/rdd/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6886faaa{/environment,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2c8a281f{/environment/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@13dc8291{/executors,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@35fd2212{/executors/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@6af1cdad{/executors/threadDump,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@17c6ca3c{/executors/threadDump/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@60f90a5f{/static,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@4e7d7ff2{/,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@4d55b295{/api,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@148a2773{/jobs/job/kill,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@5682ea5a{/stages/stage/kill,null,AVAILABLE,@Spark}
2018-03-31 17:02:14 INFO  SparkUI:54 - Bound SparkUI to 0.0.0.0, and started at http://192.168.99.1:4040
2018-03-31 17:02:14 INFO  SparkContext:54 - Added JAR file:///Users/david.shaub/PBDP/hw8/mariadb-java-client-2.2.3.jar at spark://192.168.99.1:55202/jars/mariadb-java-client-2.2.3.jar with timestamp 1522537334696
2018-03-31 17:02:14 INFO  SparkContext:54 - Added file file:/Users/david.shaub/PBDP/hw8/unique_users_mariadb.py at file:/Users/david.shaub/PBDP/hw8/unique_users_mariadb.py with timestamp 1522537334698
2018-03-31 17:02:14 INFO  Utils:54 - Copying /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7/userFiles-cd7a1a04-0303-438a-a46e-66d39b9f8e2a/unique_users_mariadb.py
2018-03-31 17:02:14 INFO  Executor:54 - Starting executor ID driver on host localhost
2018-03-31 17:02:14 INFO  Utils:54 - Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 55203.
2018-03-31 17:02:14 INFO  NettyBlockTransferService:54 - Server created on 192.168.99.1:55203
2018-03-31 17:02:14 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
2018-03-31 17:02:14 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, 192.168.99.1, 55203, None)
2018-03-31 17:02:14 INFO  BlockManagerMasterEndpoint:54 - Registering block manager 192.168.99.1:55203 with 366.3 MB RAM, BlockManagerId(driver, 192.168.99.1, 55203, None)
2018-03-31 17:02:14 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, 192.168.99.1, 55203, None)
2018-03-31 17:02:14 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, 192.168.99.1, 55203, None)
2018-03-31 17:02:15 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@2ac3dfb8{/metrics/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:15 INFO  MemoryStore:54 - Block broadcast_0 stored as values in memory (estimated size 236.5 KB, free 366.1 MB)
2018-03-31 17:02:15 INFO  MemoryStore:54 - Block broadcast_0_piece0 stored as bytes in memory (estimated size 22.9 KB, free 366.0 MB)
2018-03-31 17:02:15 INFO  BlockManagerInfo:54 - Added broadcast_0_piece0 in memory on 192.168.99.1:55203 (size: 22.9 KB, free: 366.3 MB)
2018-03-31 17:02:15 INFO  SparkContext:54 - Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:0
2018-03-31 17:02:15 INFO  FileInputFormat:249 - Total input paths to process : 20
2018-03-31 17:02:15 INFO  SparkContext:54 - Starting job: runJob at PythonRDD.scala:141
2018-03-31 17:02:15 INFO  DAGScheduler:54 - Registering RDD 3 (distinct at /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py:24)
2018-03-31 17:02:15 INFO  DAGScheduler:54 - Got job 0 (runJob at PythonRDD.scala:141) with 1 output partitions
2018-03-31 17:02:15 INFO  DAGScheduler:54 - Final stage: ResultStage 1 (runJob at PythonRDD.scala:141)
2018-03-31 17:02:15 INFO  DAGScheduler:54 - Parents of final stage: List(ShuffleMapStage 0)
2018-03-31 17:02:16 INFO  DAGScheduler:54 - Missing parents: List(ShuffleMapStage 0)
2018-03-31 17:02:16 INFO  DAGScheduler:54 - Submitting ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py:24), which has no missing parents
2018-03-31 17:02:16 INFO  MemoryStore:54 - Block broadcast_1 stored as values in memory (estimated size 9.7 KB, free 366.0 MB)
2018-03-31 17:02:16 INFO  MemoryStore:54 - Block broadcast_1_piece0 stored as bytes in memory (estimated size 6.0 KB, free 366.0 MB)
2018-03-31 17:02:16 INFO  BlockManagerInfo:54 - Added broadcast_1_piece0 in memory on 192.168.99.1:55203 (size: 6.0 KB, free: 366.3 MB)
2018-03-31 17:02:16 INFO  SparkContext:54 - Created broadcast 1 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:02:16 INFO  DAGScheduler:54 - Submitting 20 missing tasks from ShuffleMapStage 0 (PairwiseRDD[3] at distinct at /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py:24) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
2018-03-31 17:02:16 INFO  TaskSchedulerImpl:54 - Adding task set 0.0 with 20 tasks
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 2.0 in stage 0.0 (TID 2, localhost, executor driver, partition 2, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 3.0 in stage 0.0 (TID 3, localhost, executor driver, partition 3, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 4.0 in stage 0.0 (TID 4, localhost, executor driver, partition 4, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 5.0 in stage 0.0 (TID 5, localhost, executor driver, partition 5, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 6.0 in stage 0.0 (TID 6, localhost, executor driver, partition 6, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  TaskSetManager:54 - Starting task 7.0 in stage 0.0 (TID 7, localhost, executor driver, partition 7, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 4.0 in stage 0.0 (TID 4)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 7.0 in stage 0.0 (TID 7)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 1.0 in stage 0.0 (TID 1)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 3.0 in stage 0.0 (TID 3)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 2.0 in stage 0.0 (TID 2)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 5.0 in stage 0.0 (TID 5)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 0.0 in stage 0.0 (TID 0)
2018-03-31 17:02:16 INFO  Executor:54 - Running task 6.0 in stage 0.0 (TID 6)
2018-03-31 17:02:16 INFO  Executor:54 - Fetching file:/Users/david.shaub/PBDP/hw8/unique_users_mariadb.py with timestamp 1522537334698
2018-03-31 17:02:16 INFO  Utils:54 - /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py has been previously copied to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7/userFiles-cd7a1a04-0303-438a-a46e-66d39b9f8e2a/unique_users_mariadb.py
2018-03-31 17:02:16 INFO  Executor:54 - Fetching spark://192.168.99.1:55202/jars/mariadb-java-client-2.2.3.jar with timestamp 1522537334696
2018-03-31 17:02:16 INFO  TransportClientFactory:267 - Successfully created connection to /192.168.99.1:55202 after 39 ms (0 ms spent in bootstraps)
2018-03-31 17:02:16 INFO  Utils:54 - Fetching spark://192.168.99.1:55202/jars/mariadb-java-client-2.2.3.jar to /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7/userFiles-cd7a1a04-0303-438a-a46e-66d39b9f8e2a/fetchFileTemp124191828555372383.tmp
2018-03-31 17:02:16 INFO  Executor:54 - Adding file:/private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7/userFiles-cd7a1a04-0303-438a-a46e-66d39b9f8e2a/mariadb-java-client-2.2.3.jar to class loader
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_15.txt:0+34468772
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_13.txt:0+11489816
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_11.txt:0+34467772
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_12.txt:0+11489720
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_10.txt:0+19148972
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_16.txt:0+3830000
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_14.txt:0+15319508
2018-03-31 17:02:16 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_1.txt:0+34467772
2018-03-31 17:02:17 INFO  PythonRunner:54 - Times: total = 1206, boot = 268, init = 29, finish = 909
2018-03-31 17:02:18 INFO  Executor:54 - Finished task 7.0 in stage 0.0 (TID 7). 1629 bytes result sent to driver
2018-03-31 17:02:18 INFO  TaskSetManager:54 - Starting task 8.0 in stage 0.0 (TID 8, localhost, executor driver, partition 8, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:18 INFO  Executor:54 - Running task 8.0 in stage 0.0 (TID 8)
2018-03-31 17:02:18 INFO  TaskSetManager:54 - Finished task 7.0 in stage 0.0 (TID 7) in 1888 ms on localhost (executor driver) (1/20)
2018-03-31 17:02:18 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_17.txt:0+15318540
2018-03-31 17:02:19 INFO  PythonRunner:54 - Times: total = 2694, boot = 276, init = 32, finish = 2386
2018-03-31 17:02:19 INFO  Executor:54 - Finished task 3.0 in stage 0.0 (TID 3). 1586 bytes result sent to driver
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Starting task 9.0 in stage 0.0 (TID 9, localhost, executor driver, partition 9, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:19 INFO  Executor:54 - Running task 9.0 in stage 0.0 (TID 9)
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Finished task 3.0 in stage 0.0 (TID 3) in 2973 ms on localhost (executor driver) (2/20)
2018-03-31 17:02:19 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_18.txt:0+30638280
2018-03-31 17:02:19 INFO  PythonRunner:54 - Times: total = 2727, boot = 272, init = 22, finish = 2433
2018-03-31 17:02:19 INFO  Executor:54 - Finished task 4.0 in stage 0.0 (TID 4). 1586 bytes result sent to driver
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Starting task 10.0 in stage 0.0 (TID 10, localhost, executor driver, partition 10, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:19 INFO  Executor:54 - Running task 10.0 in stage 0.0 (TID 10)
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Finished task 4.0 in stage 0.0 (TID 4) in 3012 ms on localhost (executor driver) (3/20)
2018-03-31 17:02:19 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_19.txt:0+26809760
2018-03-31 17:02:19 INFO  PythonRunner:54 - Times: total = 3458, boot = 279, init = 27, finish = 3152
2018-03-31 17:02:19 INFO  Executor:54 - Finished task 5.0 in stage 0.0 (TID 5). 1586 bytes result sent to driver
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Starting task 11.0 in stage 0.0 (TID 11, localhost, executor driver, partition 11, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:19 INFO  Executor:54 - Running task 11.0 in stage 0.0 (TID 11)
2018-03-31 17:02:19 INFO  TaskSetManager:54 - Finished task 5.0 in stage 0.0 (TID 5) in 3736 ms on localhost (executor driver) (4/20)
2018-03-31 17:02:19 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_2.txt:0+11489720
2018-03-31 17:02:20 INFO  PythonRunner:54 - Times: total = 4335, boot = 274, init = 33, finish = 4028
2018-03-31 17:02:20 INFO  Executor:54 - Finished task 1.0 in stage 0.0 (TID 1). 1586 bytes result sent to driver
2018-03-31 17:02:20 INFO  TaskSetManager:54 - Starting task 12.0 in stage 0.0 (TID 12, localhost, executor driver, partition 12, PROCESS_LOCAL, 7884 bytes)
2018-03-31 17:02:20 INFO  Executor:54 - Running task 12.0 in stage 0.0 (TID 12)
2018-03-31 17:02:20 INFO  TaskSetManager:54 - Finished task 1.0 in stage 0.0 (TID 1) in 4613 ms on localhost (executor driver) (5/20)
2018-03-31 17:02:20 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_20.txt:0+19148972
2018-03-31 17:02:21 INFO  PythonRunner:54 - Times: total = 3112, boot = -385, init = 387, finish = 3110
2018-03-31 17:02:21 INFO  Executor:54 - Finished task 8.0 in stage 0.0 (TID 8). 1586 bytes result sent to driver
2018-03-31 17:02:21 INFO  TaskSetManager:54 - Starting task 13.0 in stage 0.0 (TID 13, localhost, executor driver, partition 13, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:21 INFO  Executor:54 - Running task 13.0 in stage 0.0 (TID 13)
2018-03-31 17:02:21 INFO  TaskSetManager:54 - Finished task 8.0 in stage 0.0 (TID 8) in 3141 ms on localhost (executor driver) (6/20)
2018-03-31 17:02:21 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_3.txt:0+11489816
2018-03-31 17:02:22 INFO  PythonRunner:54 - Times: total = 2412, boot = 73, init = 1, finish = 2338
2018-03-31 17:02:22 INFO  Executor:54 - Finished task 11.0 in stage 0.0 (TID 11). 1629 bytes result sent to driver
2018-03-31 17:02:22 INFO  TaskSetManager:54 - Starting task 14.0 in stage 0.0 (TID 14, localhost, executor driver, partition 14, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:22 INFO  Executor:54 - Running task 14.0 in stage 0.0 (TID 14)
2018-03-31 17:02:22 INFO  TaskSetManager:54 - Finished task 11.0 in stage 0.0 (TID 11) in 2434 ms on localhost (executor driver) (7/20)
2018-03-31 17:02:22 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_4.txt:0+15319508
2018-03-31 17:02:23 INFO  PythonRunner:54 - Times: total = 2171, boot = 1, init = 2, finish = 2168
2018-03-31 17:02:23 INFO  Executor:54 - Finished task 13.0 in stage 0.0 (TID 13). 1586 bytes result sent to driver
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Starting task 15.0 in stage 0.0 (TID 15, localhost, executor driver, partition 15, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:23 INFO  Executor:54 - Running task 15.0 in stage 0.0 (TID 15)
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Finished task 13.0 in stage 0.0 (TID 13) in 2190 ms on localhost (executor driver) (8/20)
2018-03-31 17:02:23 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_5.txt:0+34468772
2018-03-31 17:02:23 INFO  PythonRunner:54 - Times: total = 7158, boot = 270, init = 35, finish = 6853
2018-03-31 17:02:23 INFO  Executor:54 - Finished task 2.0 in stage 0.0 (TID 2). 1586 bytes result sent to driver
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Starting task 16.0 in stage 0.0 (TID 16, localhost, executor driver, partition 16, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:23 INFO  Executor:54 - Running task 16.0 in stage 0.0 (TID 16)
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Finished task 2.0 in stage 0.0 (TID 2) in 7444 ms on localhost (executor driver) (9/20)
2018-03-31 17:02:23 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_6.txt:0+3830000
2018-03-31 17:02:23 INFO  PythonRunner:54 - Times: total = 7260, boot = 283, init = 27, finish = 6950
2018-03-31 17:02:23 INFO  Executor:54 - Finished task 6.0 in stage 0.0 (TID 6). 1586 bytes result sent to driver
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Starting task 17.0 in stage 0.0 (TID 17, localhost, executor driver, partition 17, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Finished task 6.0 in stage 0.0 (TID 6) in 7540 ms on localhost (executor driver) (10/20)
2018-03-31 17:02:23 INFO  Executor:54 - Running task 17.0 in stage 0.0 (TID 17)
2018-03-31 17:02:23 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_7.txt:0+15318540
2018-03-31 17:02:23 INFO  PythonRunner:54 - Times: total = 7335, boot = 265, init = 32, finish = 7038
2018-03-31 17:02:23 INFO  Executor:54 - Finished task 0.0 in stage 0.0 (TID 0). 1629 bytes result sent to driver
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Starting task 18.0 in stage 0.0 (TID 18, localhost, executor driver, partition 18, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:23 INFO  Executor:54 - Running task 18.0 in stage 0.0 (TID 18)
2018-03-31 17:02:23 INFO  TaskSetManager:54 - Finished task 0.0 in stage 0.0 (TID 0) in 7631 ms on localhost (executor driver) (11/20)
2018-03-31 17:02:23 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_8.txt:0+30638280
2018-03-31 17:02:24 INFO  PythonRunner:54 - Times: total = 877, boot = 41, init = 2, finish = 834
2018-03-31 17:02:24 INFO  Executor:54 - Finished task 16.0 in stage 0.0 (TID 16). 1586 bytes result sent to driver
2018-03-31 17:02:24 INFO  TaskSetManager:54 - Starting task 19.0 in stage 0.0 (TID 19, localhost, executor driver, partition 19, PROCESS_LOCAL, 7883 bytes)
2018-03-31 17:02:24 INFO  Executor:54 - Running task 19.0 in stage 0.0 (TID 19)
2018-03-31 17:02:24 INFO  TaskSetManager:54 - Finished task 16.0 in stage 0.0 (TID 16) in 897 ms on localhost (executor driver) (12/20)
2018-03-31 17:02:24 INFO  HadoopRDD:54 - Input split: file:/Users/david.shaub/PBDP/hw8/hw7_logs_9.txt:0+26809760
2018-03-31 17:02:24 INFO  PythonRunner:54 - Times: total = 5432, boot = 35, init = 1, finish = 5396
2018-03-31 17:02:24 INFO  Executor:54 - Finished task 10.0 in stage 0.0 (TID 10). 1629 bytes result sent to driver
2018-03-31 17:02:24 INFO  TaskSetManager:54 - Finished task 10.0 in stage 0.0 (TID 10) in 5456 ms on localhost (executor driver) (13/20)
2018-03-31 17:02:24 INFO  PythonRunner:54 - Times: total = 3902, boot = 94, init = 0, finish = 3808
2018-03-31 17:02:24 INFO  Executor:54 - Finished task 12.0 in stage 0.0 (TID 12). 1586 bytes result sent to driver
2018-03-31 17:02:24 INFO  TaskSetManager:54 - Finished task 12.0 in stage 0.0 (TID 12) in 3922 ms on localhost (executor driver) (14/20)
2018-03-31 17:02:25 INFO  PythonRunner:54 - Times: total = 6008, boot = 10, init = 1, finish = 5997
2018-03-31 17:02:25 INFO  Executor:54 - Finished task 9.0 in stage 0.0 (TID 9). 1586 bytes result sent to driver
2018-03-31 17:02:25 INFO  TaskSetManager:54 - Finished task 9.0 in stage 0.0 (TID 9) in 6026 ms on localhost (executor driver) (15/20)
2018-03-31 17:02:25 INFO  PythonRunner:54 - Times: total = 2900, boot = 13, init = 0, finish = 2887
2018-03-31 17:02:25 INFO  Executor:54 - Finished task 14.0 in stage 0.0 (TID 14). 1586 bytes result sent to driver
2018-03-31 17:02:25 INFO  TaskSetManager:54 - Finished task 14.0 in stage 0.0 (TID 14) in 2922 ms on localhost (executor driver) (16/20)
2018-03-31 17:02:25 INFO  PythonRunner:54 - Times: total = 2219, boot = 33, init = 1, finish = 2185
2018-03-31 17:02:25 INFO  Executor:54 - Finished task 17.0 in stage 0.0 (TID 17). 1586 bytes result sent to driver
2018-03-31 17:02:25 INFO  TaskSetManager:54 - Finished task 17.0 in stage 0.0 (TID 17) in 2236 ms on localhost (executor driver) (17/20)
2018-03-31 17:02:27 INFO  PythonRunner:54 - Times: total = 2618, boot = -8, init = 11, finish = 2615
2018-03-31 17:02:27 INFO  Executor:54 - Finished task 19.0 in stage 0.0 (TID 19). 1586 bytes result sent to driver
2018-03-31 17:02:27 INFO  TaskSetManager:54 - Finished task 19.0 in stage 0.0 (TID 19) in 2636 ms on localhost (executor driver) (18/20)
2018-03-31 17:02:27 INFO  PythonRunner:54 - Times: total = 3368, boot = 35, init = 1, finish = 3332
2018-03-31 17:02:27 INFO  Executor:54 - Finished task 18.0 in stage 0.0 (TID 18). 1586 bytes result sent to driver
2018-03-31 17:02:27 INFO  TaskSetManager:54 - Finished task 18.0 in stage 0.0 (TID 18) in 3401 ms on localhost (executor driver) (19/20)
2018-03-31 17:02:27 INFO  PythonRunner:54 - Times: total = 3968, boot = 9, init = 0, finish = 3959
2018-03-31 17:02:27 INFO  Executor:54 - Finished task 15.0 in stage 0.0 (TID 15). 1586 bytes result sent to driver
2018-03-31 17:02:27 INFO  TaskSetManager:54 - Finished task 15.0 in stage 0.0 (TID 15) in 3987 ms on localhost (executor driver) (20/20)
2018-03-31 17:02:27 INFO  TaskSchedulerImpl:54 - Removed TaskSet 0.0, whose tasks have all completed, from pool 
2018-03-31 17:02:27 INFO  DAGScheduler:54 - ShuffleMapStage 0 (distinct at /Users/david.shaub/PBDP/hw8/unique_users_mariadb.py:24) finished in 11.310 s
2018-03-31 17:02:27 INFO  DAGScheduler:54 - looking for newly runnable stages
2018-03-31 17:02:27 INFO  DAGScheduler:54 - running: Set()
2018-03-31 17:02:27 INFO  DAGScheduler:54 - waiting: Set(ResultStage 1)
2018-03-31 17:02:27 INFO  DAGScheduler:54 - failed: Set()
2018-03-31 17:02:27 INFO  DAGScheduler:54 - Submitting ResultStage 1 (PythonRDD[6] at RDD at PythonRDD.scala:48), which has no missing parents
2018-03-31 17:02:27 INFO  MemoryStore:54 - Block broadcast_2 stored as values in memory (estimated size 7.6 KB, free 366.0 MB)
2018-03-31 17:02:27 INFO  MemoryStore:54 - Block broadcast_2_piece0 stored as bytes in memory (estimated size 4.6 KB, free 366.0 MB)
2018-03-31 17:02:27 INFO  BlockManagerInfo:54 - Added broadcast_2_piece0 in memory on 192.168.99.1:55203 (size: 4.6 KB, free: 366.3 MB)
2018-03-31 17:02:27 INFO  SparkContext:54 - Created broadcast 2 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:02:27 INFO  DAGScheduler:54 - Submitting 1 missing tasks from ResultStage 1 (PythonRDD[6] at RDD at PythonRDD.scala:48) (first 15 tasks are for partitions Vector(0))
2018-03-31 17:02:27 INFO  TaskSchedulerImpl:54 - Adding task set 1.0 with 1 tasks
2018-03-31 17:02:27 INFO  TaskSetManager:54 - Starting task 0.0 in stage 1.0 (TID 20, localhost, executor driver, partition 0, ANY, 7649 bytes)
2018-03-31 17:02:27 INFO  Executor:54 - Running task 0.0 in stage 1.0 (TID 20)
2018-03-31 17:02:27 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:27 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 4 ms
2018-03-31 17:02:27 INFO  PythonRunner:54 - Times: total = 29, boot = -2793, init = 2804, finish = 18
2018-03-31 17:02:27 INFO  Executor:54 - Finished task 0.0 in stage 1.0 (TID 20). 1659 bytes result sent to driver
2018-03-31 17:02:27 INFO  TaskSetManager:54 - Finished task 0.0 in stage 1.0 (TID 20) in 60 ms on localhost (executor driver) (1/1)
2018-03-31 17:02:27 INFO  TaskSchedulerImpl:54 - Removed TaskSet 1.0, whose tasks have all completed, from pool 
2018-03-31 17:02:27 INFO  DAGScheduler:54 - ResultStage 1 (runJob at PythonRDD.scala:141) finished in 0.072 s
2018-03-31 17:02:27 INFO  DAGScheduler:54 - Job 0 finished: runJob at PythonRDD.scala:141, took 11.469950 s
2018-03-31 17:02:27 INFO  SharedState:54 - Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir ('file:/Users/david.shaub/PBDP/hw8/spark-warehouse/').
2018-03-31 17:02:27 INFO  SharedState:54 - Warehouse path is 'file:/Users/david.shaub/PBDP/hw8/spark-warehouse/'.
2018-03-31 17:02:27 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@e30dba6{/SQL,null,AVAILABLE,@Spark}
2018-03-31 17:02:27 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@259f6887{/SQL/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:27 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@42fdb40d{/SQL/execution,null,AVAILABLE,@Spark}
2018-03-31 17:02:27 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@32fa948{/SQL/execution/json,null,AVAILABLE,@Spark}
2018-03-31 17:02:27 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@540abeea{/static/sql,null,AVAILABLE,@Spark}
2018-03-31 17:02:27 INFO  BlockManagerInfo:54 - Removed broadcast_2_piece0 on 192.168.99.1:55203 in memory (size: 4.6 KB, free: 366.3 MB)
2018-03-31 17:02:27 INFO  StateStoreCoordinatorRef:54 - Registered StateStoreCoordinator endpoint
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 11
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 26
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 17
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 34
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 23
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 46
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 19
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 33
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 24
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 31
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 3
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 37
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 21
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 22
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 41
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 48
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 12
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 47
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 2
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 49
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 32
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 18
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 44
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 30
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 43
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 45
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 42
2018-03-31 17:02:28 INFO  BlockManagerInfo:54 - Removed broadcast_1_piece0 on 192.168.99.1:55203 in memory (size: 6.0 KB, free: 366.3 MB)
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 5
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 16
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 38
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 29
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 6
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 27
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 4
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 50
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 20
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 13
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 14
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 40
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 36
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 15
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 39
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 8
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 28
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 35
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 1
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 25
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 9
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 7
2018-03-31 17:02:28 INFO  ContextCleaner:54 - Cleaned accumulator 10
2018-03-31 17:02:29 INFO  SparkContext:54 - Starting job: jdbc at NativeMethodAccessorImpl.java:0
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Got job 1 (jdbc at NativeMethodAccessorImpl.java:0) with 20 output partitions
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Final stage: ResultStage 3 (jdbc at NativeMethodAccessorImpl.java:0)
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Parents of final stage: List(ShuffleMapStage 2)
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Missing parents: List()
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Submitting ResultStage 3 (MapPartitionsRDD[13] at jdbc at NativeMethodAccessorImpl.java:0), which has no missing parents
2018-03-31 17:02:29 INFO  MemoryStore:54 - Block broadcast_3 stored as values in memory (estimated size 17.4 KB, free 366.0 MB)
2018-03-31 17:02:29 INFO  MemoryStore:54 - Block broadcast_3_piece0 stored as bytes in memory (estimated size 9.6 KB, free 366.0 MB)
2018-03-31 17:02:29 INFO  BlockManagerInfo:54 - Added broadcast_3_piece0 in memory on 192.168.99.1:55203 (size: 9.6 KB, free: 366.3 MB)
2018-03-31 17:02:29 INFO  SparkContext:54 - Created broadcast 3 from broadcast at DAGScheduler.scala:1039
2018-03-31 17:02:29 INFO  DAGScheduler:54 - Submitting 20 missing tasks from ResultStage 3 (MapPartitionsRDD[13] at jdbc at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
2018-03-31 17:02:29 INFO  TaskSchedulerImpl:54 - Adding task set 3.0 with 20 tasks
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 0.0 in stage 3.0 (TID 21, localhost, executor driver, partition 0, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 1.0 in stage 3.0 (TID 22, localhost, executor driver, partition 1, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 2.0 in stage 3.0 (TID 23, localhost, executor driver, partition 2, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 3.0 in stage 3.0 (TID 24, localhost, executor driver, partition 3, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 4.0 in stage 3.0 (TID 25, localhost, executor driver, partition 4, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 5.0 in stage 3.0 (TID 26, localhost, executor driver, partition 5, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 6.0 in stage 3.0 (TID 27, localhost, executor driver, partition 6, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  TaskSetManager:54 - Starting task 7.0 in stage 3.0 (TID 28, localhost, executor driver, partition 7, ANY, 7649 bytes)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 0.0 in stage 3.0 (TID 21)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 2.0 in stage 3.0 (TID 23)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 3.0 in stage 3.0 (TID 24)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 4.0 in stage 3.0 (TID 25)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 5.0 in stage 3.0 (TID 26)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 6.0 in stage 3.0 (TID 27)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 7.0 in stage 3.0 (TID 28)
2018-03-31 17:02:29 INFO  Executor:54 - Running task 1.0 in stage 3.0 (TID 22)
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 2 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 2 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:29 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  CodeGenerator:54 - Code generated in 156.998833 ms
2018-03-31 17:02:30 INFO  CodeGenerator:54 - Code generated in 13.921144 ms
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 90, boot = -2440, init = 2487, finish = 43
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 92, boot = -2662, init = 2667, finish = 87
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 76, boot = -2356, init = 2373, finish = 59
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 94, boot = -5103, init = 5110, finish = 87
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 93, boot = -4546, init = 4562, finish = 77
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 92, boot = -2626, init = 2639, finish = 79
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 103, boot = -4633, init = 4684, finish = 52
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 105, boot = -3859, init = 3886, finish = 78
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 7.0 in stage 3.0 (TID 28). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 1.0 in stage 3.0 (TID 22). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 3.0 in stage 3.0 (TID 24). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 8.0 in stage 3.0 (TID 29, localhost, executor driver, partition 8, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 9.0 in stage 3.0 (TID 30, localhost, executor driver, partition 9, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 8.0 in stage 3.0 (TID 29)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 7.0 in stage 3.0 (TID 28) in 508 ms on localhost (executor driver) (1/20)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 1.0 in stage 3.0 (TID 22) in 515 ms on localhost (executor driver) (2/20)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 10.0 in stage 3.0 (TID 31, localhost, executor driver, partition 10, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 3.0 in stage 3.0 (TID 24) in 516 ms on localhost (executor driver) (3/20)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 10.0 in stage 3.0 (TID 31)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 9.0 in stage 3.0 (TID 30)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 0.0 in stage 3.0 (TID 21). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 11.0 in stage 3.0 (TID 32, localhost, executor driver, partition 11, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 0.0 in stage 3.0 (TID 21) in 524 ms on localhost (executor driver) (4/20)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  Executor:54 - Running task 11.0 in stage 3.0 (TID 32)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 5.0 in stage 3.0 (TID 26). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 6.0 in stage 3.0 (TID 27). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 12.0 in stage 3.0 (TID 33, localhost, executor driver, partition 12, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 13.0 in stage 3.0 (TID 34, localhost, executor driver, partition 13, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 5.0 in stage 3.0 (TID 26) in 542 ms on localhost (executor driver) (5/20)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 10 ms
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 6.0 in stage 3.0 (TID 27) in 541 ms on localhost (executor driver) (6/20)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 13.0 in stage 3.0 (TID 34)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 12.0 in stage 3.0 (TID 33)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 6 ms
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 2.0 in stage 3.0 (TID 23). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 14.0 in stage 3.0 (TID 35, localhost, executor driver, partition 14, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 14.0 in stage 3.0 (TID 35)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 2.0 in stage 3.0 (TID 23) in 557 ms on localhost (executor driver) (7/20)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 2 ms
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 4.0 in stage 3.0 (TID 25). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 15.0 in stage 3.0 (TID 36, localhost, executor driver, partition 15, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 15.0 in stage 3.0 (TID 36)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 4.0 in stage 3.0 (TID 25) in 640 ms on localhost (executor driver) (8/20)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 85, boot = -401, init = 412, finish = 74
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 44, boot = -395, init = 397, finish = 42
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 51, boot = -403, init = 409, finish = 45
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 38, boot = -473, init = 478, finish = 33
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 11.0 in stage 3.0 (TID 32). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 16.0 in stage 3.0 (TID 37, localhost, executor driver, partition 16, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 16.0 in stage 3.0 (TID 37)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 11.0 in stage 3.0 (TID 32) in 162 ms on localhost (executor driver) (9/20)
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 49, boot = -460, init = 476, finish = 33
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 44, boot = -450, init = 455, finish = 39
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 51, boot = -467, init = 484, finish = 34
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 43, boot = -504, init = 508, finish = 39
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 10.0 in stage 3.0 (TID 31). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 17.0 in stage 3.0 (TID 38, localhost, executor driver, partition 17, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 17.0 in stage 3.0 (TID 38)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 10.0 in stage 3.0 (TID 31) in 188 ms on localhost (executor driver) (10/20)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 0 ms
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 44, boot = -25, init = 28, finish = 41
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 33, boot = -124, init = 128, finish = 29
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 9.0 in stage 3.0 (TID 30). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 18.0 in stage 3.0 (TID 39, localhost, executor driver, partition 18, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 12.0 in stage 3.0 (TID 33). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Running task 18.0 in stage 3.0 (TID 39)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 9.0 in stage 3.0 (TID 30) in 285 ms on localhost (executor driver) (11/20)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Starting task 19.0 in stage 3.0 (TID 40, localhost, executor driver, partition 19, ANY, 7649 bytes)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 12.0 in stage 3.0 (TID 33) in 272 ms on localhost (executor driver) (12/20)
2018-03-31 17:02:30 INFO  Executor:54 - Running task 19.0 in stage 3.0 (TID 40)
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 5 ms
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Getting 20 non-empty blocks out of 20 blocks
2018-03-31 17:02:30 INFO  ShuffleBlockFetcherIterator:54 - Started 0 remote fetches in 1 ms
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 17.0 in stage 3.0 (TID 38). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 17.0 in stage 3.0 (TID 38) in 115 ms on localhost (executor driver) (13/20)
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 8.0 in stage 3.0 (TID 29). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 8.0 in stage 3.0 (TID 29) in 326 ms on localhost (executor driver) (14/20)
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 14.0 in stage 3.0 (TID 35). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 15.0 in stage 3.0 (TID 36). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 14.0 in stage 3.0 (TID 35) in 296 ms on localhost (executor driver) (15/20)
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 15.0 in stage 3.0 (TID 36) in 214 ms on localhost (executor driver) (16/20)
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 16.0 in stage 3.0 (TID 37). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 13.0 in stage 3.0 (TID 34). 1841 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 16.0 in stage 3.0 (TID 37) in 174 ms on localhost (executor driver) (17/20)
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 34, boot = -233, init = 236, finish = 31
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 13.0 in stage 3.0 (TID 34) in 327 ms on localhost (executor driver) (18/20)
2018-03-31 17:02:30 INFO  PythonRunner:54 - Times: total = 40, boot = -160, init = 162, finish = 38
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 18.0 in stage 3.0 (TID 39). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 18.0 in stage 3.0 (TID 39) in 68 ms on localhost (executor driver) (19/20)
2018-03-31 17:02:30 INFO  Executor:54 - Finished task 19.0 in stage 3.0 (TID 40). 1798 bytes result sent to driver
2018-03-31 17:02:30 INFO  TaskSetManager:54 - Finished task 19.0 in stage 3.0 (TID 40) in 71 ms on localhost (executor driver) (20/20)
2018-03-31 17:02:30 INFO  TaskSchedulerImpl:54 - Removed TaskSet 3.0, whose tasks have all completed, from pool 
2018-03-31 17:02:30 INFO  DAGScheduler:54 - ResultStage 3 (jdbc at NativeMethodAccessorImpl.java:0) finished in 0.898 s
2018-03-31 17:02:30 INFO  DAGScheduler:54 - Job 1 finished: jdbc at NativeMethodAccessorImpl.java:0, took 0.907173 s
2018-03-31 17:02:30 INFO  SparkContext:54 - Invoking stop() from shutdown hook
2018-03-31 17:02:30 INFO  AbstractConnector:318 - Stopped Spark@5cb9e5e9{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
2018-03-31 17:02:30 INFO  SparkUI:54 - Stopped Spark web UI at http://192.168.99.1:4040
2018-03-31 17:02:30 INFO  MapOutputTrackerMasterEndpoint:54 - MapOutputTrackerMasterEndpoint stopped!
2018-03-31 17:02:30 INFO  MemoryStore:54 - MemoryStore cleared
2018-03-31 17:02:30 INFO  BlockManager:54 - BlockManager stopped
2018-03-31 17:02:30 INFO  BlockManagerMaster:54 - BlockManagerMaster stopped
2018-03-31 17:02:30 INFO  OutputCommitCoordinator$OutputCommitCoordinatorEndpoint:54 - OutputCommitCoordinator stopped!
2018-03-31 17:02:30 INFO  SparkContext:54 - Successfully stopped SparkContext
2018-03-31 17:02:30 INFO  ShutdownHookManager:54 - Shutdown hook called
2018-03-31 17:02:30 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7
2018-03-31 17:02:30 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-54bb0cf7-e3a3-4822-a2c5-4dd17eeb3dd8
2018-03-31 17:02:30 INFO  ShutdownHookManager:54 - Deleting directory /private/var/folders/_b/jy0l3rkx14j7rn4098kf2rpd9f7r2c/T/spark-06548f90-a336-47ee-96ee-3131a1a4e5d7/pyspark-f719c655-d6a5-49b2-8e45-e26f239846e3
```

We verify that the data has been written to the database:
```
MariaDB [hw8_spark]> select * from hw8_spark.hour_url limit 10;
+---------------------+---------------------------+
| hour                | url                       |
+---------------------+---------------------------+
| 2018-02-27T00:00:00 | http://example.com/?url=0 |
| 2018-02-22T18:00:00 | http://example.com/?url=8 |
| 2018-03-04T02:00:00 | http://example.com/?url=0 |
| 2018-02-27T09:00:00 | http://example.com/?url=5 |
| 2018-02-20T19:00:00 | http://example.com/?url=7 |
| 2018-02-23T00:00:00 | http://example.com/?url=0 |
| 2018-02-20T13:00:00 | http://example.com/?url=9 |
| 2018-03-04T10:00:00 | http://example.com/?url=5 |
| 2018-02-26T19:00:00 | http://example.com/?url=9 |
| 2018-02-22T06:00:00 | http://example.com/?url=9 |
+---------------------+---------------------------+
10 rows in set (0.00 sec)

MariaDB [hw8_spark]> select * from hw8_spark.hour_user limit 10;
+---------------------+---------+
| hour                | user    |
+---------------------+---------+
| 2018-02-21T08:00:00 | User_37 |
| 2018-03-04T09:00:00 | User_10 |
| 2018-02-24T12:00:00 | User_39 |
| 2018-02-24T19:00:00 | User_36 |
| 2018-03-01T10:00:00 | User_33 |
| 2018-02-28T06:00:00 | User_18 |
| 2018-02-23T23:00:00 | User_23 |
| 2018-02-24T10:00:00 | User_15 |
| 2018-02-23T04:00:00 | User_17 |
| 2018-02-27T15:00:00 | User_24 |
+---------------------+---------+
10 rows in set (0.00 sec)
```

When we manually created these tables earlier we chose the `timestamp` datatype for the `hour` fiel, but we notice that Spark has created both of these tables with all text fields:
```
MariaDB [(none)]> show columns from hw8_spark.hour_url;
+-------+------+------+-----+---------+-------+
| Field | Type | Null | Key | Default | Extra |
+-------+------+------+-----+---------+-------+
| hour  | text | YES  |     | NULL    |       |
| url   | text | YES  |     | NULL    |       |
+-------+------+------+-----+---------+-------+
2 rows in set (0.00 sec)

MariaDB [(none)]> show columns from hw8_spark.hour_user;
+-------+------+------+-----+---------+-------+
| Field | Type | Null | Key | Default | Extra |
+-------+------+------+-----+---------+-------+
| hour  | text | YES  |     | NULL    |       |
| user  | text | YES  |     | NULL    |       |
+-------+------+------+-----+---------+-------+
2 rows in set (0.00 sec)
```

We generate the two reports as before, but this time are sure to specify the tables in our new database instead of the old `hw8` database.

**Report 1**
```
MariaDB [hw8]> select date(hour) as day, count(distinct(url)) as num_url from hw8_spark.hour_url group by day order by day asc;
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
13 rows in set (0.01 sec)
```

**Report 2**
```
MariaDB [hw8]> select date(hour) as day, count(distinct(user)) as num_users from hw8_spark.hour_user group by day order by day asc;
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
