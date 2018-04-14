---
title: Homework 10
author: David Shaub
geometry: margin=2cm
date: 2018-04-14
---

## Problem 1

We'll use the Cassandra Docker images
```
$ docker run -it --name some-cassandra -d cassandra:latest
$ docker run -it --link some-cassandra:cassandra --rm cassandra cqlsh cassandra
```

Create the keyspace:
```
cqlsh> create keyspace if not exists hw10 with replication = {'class':'SimpleStrategy','replication_factor':1};
cqlsh> describe hw10;

CREATE KEYSPACE hw10 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

cqlsh> use hw10;

cqlsh:hw10> create table hw10_p1 (uuid text, record_time timestamp, url text, ua_country text, ttfb float, primary key((url, ua_country), record_time));
cqlsh:hw10> describe hw10_p1;

CREATE TABLE hw10.hw10_p1 (
    url text,
    ua_country text,
    record_time timestamp,
    ttfb float,
    uuid text,
    PRIMARY KEY ((url, ua_country), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

Now we insert some records.
```
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('foo_uuid', '2017-12-31T01:01:01', 'google.com', 'us', 5.0);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('bar_uuid', '2018-01-01T01:01:01', 'msn.com', 'us', 3.14);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('baz_uuid', '2018-01-01T01:02:01', 'yahoo.com', 'us', 2.71);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('spam_uuid', '2018-01-01T01:10:01', 'yahoo.com', 'us', 1.23);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('eggs_uuid', '2018-01-01T01:10:01', 'google.com', 'us', 1.23);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('foobar_uuid', '2018-01-01T03:01:01', 'google.com', 'us', 50.0);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('something_uuid', '2018-01-01T03:15:00', 'google.com', 'ru', 13.5);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('other_uuid', '2018-01-01T04:01:01', 'facebook.com', 'in', 4.5);
cqlsh:hw10> insert into hw10_p1 (uuid, record_time, url, ua_country, ttfb) values ('more_uuid', '2018-01-01T23:01:01', 'google.com', 'uk', 100.25);
```

The primary key here is composed of `url`, and `ua_country` with clustering on `record_time`. These were selected since grouping in all of the queries use `url` and `ua_country`, so we want these combinations to land in the same partitions and Canssandra node. Furthermore, since our queries will be filtering based on ranges of the `record_time`, performance will benefit if the records within a row are sorted according to this field. 

We see that all records were successfully inserted:
```
cqlsh:hw10> select * from hw10_p1;

 url          | ua_country | record_time                     | ttfb   | uuid
--------------+------------+---------------------------------+--------+----------------
    yahoo.com |         us | 2018-01-01 01:02:01.000000+0000 |   2.71 |       baz_uuid
    yahoo.com |         us | 2018-01-01 01:10:01.000000+0000 |   1.23 |      spam_uuid
   google.com |         us | 2017-12-31 01:01:01.000000+0000 |      5 |       foo_uuid
   google.com |         us | 2018-01-01 01:10:01.000000+0000 |   1.23 |      eggs_uuid
   google.com |         us | 2018-01-01 03:01:01.000000+0000 |     50 |    foobar_uuid
      msn.com |         us | 2018-01-01 01:01:01.000000+0000 |   3.14 |       bar_uuid
 facebook.com |         in | 2018-01-01 04:01:01.000000+0000 |    4.5 |     other_uuid
   google.com |         uk | 2018-01-01 23:01:01.000000+0000 | 100.25 |      more_uuid
   google.com |         ru | 2018-01-01 03:15:00.000000+0000 |   13.5 | something_uuid

(9 rows)
```

**Query 1**
```
cqlsh:hw10> select url, ua_country, count(uuid) as num_records from hw10_p1 where record_time >= '2018-01-01T00:00:00' and record_time < '2018-01-01T23:00:00' and url='google.com' and ua_country='us' group by url, ua_country;


 url        | ua_country | num_records
------------+------------+-------------
 google.com |         us |           2

(1 rows)
```

We run another example:
```
cqlsh:hw10> select url, ua_country, count(uuid) as num_records from hw10_p1 where record_time >= '2018-01-01T00:00:00' and record_time < '2018-01-01T23:00:00' and url='yahoo.com' and ua_country='us' group by url, ua_country;

 url       | ua_country | num_records
-----------+------------+-------------
 yahoo.com |         us |           2

(1 rows)
```

**Query 2**
```


cqlsh:hw10> select url, ua_country, avg(ttfb) as avg_ttfb from hw10_p1 where record_time >= '2018-01-01T00:00:00' and record_time < '2018-01-01T23:00:00' and url='google.com' and ua_country='us' group by url, ua_country;

 url        | ua_country | avg_ttfb
------------+------------+----------
 google.com |         us |   25.615

(1 rows)
```

And one more example:
```
cqlsh:hw10> select url, ua_country, avg(ttfb) as avg_ttfb from hw10_p1 where record_time >= '2018-01-01T00:00:00' and record_time < '2018-01-01T23:00:00' and url='yahoo.com' and ua_country='us' group by url, ua_country;

 url       | ua_country | avg_ttfb
-----------+------------+----------
 yahoo.com |         us |     1.97

(1 rows)
```

## Problem 2


Our schema will remain largely the same, but we will add a new field `hour`, and this will also be included inside our composite primary key. Since we will include the hour in our queries, this means Cassandra can still handle the larger data volume since each query will still go to a single partition and node. We create the new `hw10_p2` table in our `hw10` keyspace, so there are now two tables.
```
cqlsh:hw10> create table hw10_p2 (uuid text, record_time timestamp, hour tinyint, url text, ua_country text, ttfb float, primary key((url, ua_country, hour), record_time));
cqlsh:hw10> describe hw10_p2

CREATE TABLE hw10.hw10_p2 (
    url text,
    ua_country text,
    hour tinyint,
    record_time timestamp,
    ttfb float,
    uuid text,
    PRIMARY KEY ((url, ua_country, hour), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
cqlsh:hw10> describe hw10;

CREATE KEYSPACE hw10 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE hw10.hw10_p1 (
    url text,
    ua_country text,
    record_time timestamp,
    ttfb float,
    uuid text,
    PRIMARY KEY ((url, ua_country), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE hw10.hw10_p2 (
    url text,
    ua_country text,
    hour tinyint,
    record_time timestamp,
    ttfb float,
    uuid text,
    PRIMARY KEY ((url, ua_country, hour), record_time)
) WITH CLUSTERING ORDER BY (record_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

We insert a few events across a few hours:
```
cqlsh:hw10> insert into hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb) values ('bar_uuid', '2018-01-01T01:01:31', 1, 'msn.com', 'us', 2.71);
cqlsh:hw10> insert into hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb) values ('foo_uuid', '2018-01-01T01:01:01', 1, 'msn.com', 'us', 2.71);
cqlsh:hw10> insert into hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb) values ('baz_uuid', '2018-01-01T03:01:01', 3, 'google.com', 'us', 100.0);
cqlsh:hw10> insert into hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb) values ('spam_uuid', '2018-01-01T04:01:01', 4, 'msn.com', 'uk', 4);
cqlsh:hw10> insert into hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb) values ('eggs_uuid', '2018-01-01T05:01:01', 5, 'msn.com', 'us', 1000);
```
The all records appear in the table:
```
cqlsh:hw10> select * from hw10_p2;

 url        | ua_country | hour | record_time                     | ttfb | uuid
------------+------------+------+---------------------------------+------+-----------
 google.com |         us |    3 | 2018-01-01 03:01:01.000000+0000 |  100 |  baz_uuid
    msn.com |         us |    5 | 2018-01-01 05:01:01.000000+0000 | 1000 | eggs_uuid
    msn.com |         us |    1 | 2018-01-01 01:01:01.000000+0000 | 2.71 |  foo_uuid
    msn.com |         us |    1 | 2018-01-01 01:01:31.000000+0000 | 2.71 |  bar_uuid
    msn.com |         uk |    4 | 2018-01-01 04:01:01.000000+0000 |    4 | spam_uuid

(5 rows)
```


**Query 1**

```
cqlsh:hw10> select url, ua_country, count(uuid) as num_records from hw10_p2 where record_time >= '2018-01-01T01:00:00' and record_time < '2018-01-01T02:00:00' and hour=1 and url='msn.com' and ua_country='us'group by url, ua_country, hour;

 url     | ua_country | num_records
---------+------------+-------------
 msn.com |         us |           2

(1 rows)

cqlsh:hw10> select url, ua_country, count(uuid) as num_records from hw10_p2 where record_time >= '2018-01-01T04:00:00' and record_time < '2018-01-01T05:00:00' and hour=4 and url='msn.com' and ua_country='us'group by url, ua_country, hour;

 url | ua_country | num_records
-----+------------+-------------

(0 rows)
cqlsh:hw10> select url, ua_country, count(uuid) as num_records from hw10_p2 where record_time >= '2018-01-01T04:00:00' and record_time < '2018-01-01T05:00:00' and hour=4 and url='msn.com' and ua_country='uk'group by url, ua_country, hour;

 url     | ua_country | num_records
---------+------------+-------------
 msn.com |         uk |           1

(1 rows)
```

**Query 2**

```
cqlsh:hw10> select url, ua_country, avg(ttfb) as avg_ttfb from hw10_p2 where record_time >= '2018-01-01T01:00:00' and record_time < '2018-01-01T02:00:00' and hour=1 and url='google.com' and ua_country='us' group by url, ua_country, hour;

 url | ua_country | avg_ttfb
-----+------------+----------

(0 rows)
cqlsh:hw10> select url, ua_country, avg(ttfb) as avg_ttfb from hw10_p2 where record_time >= '2018-01-01T01:00:00' and record_time < '2018-01-01T02:00:00' and hour=1 and url='msn.com' and ua_country='us' group by url, ua_country, hour;

 url     | ua_country | avg_ttfb
---------+------------+----------
 msn.com |         us |     2.71

(1 rows)


cqlsh:hw10> select url, ua_country, avg(ttfb) as avg_ttfb from hw10_p2 where record_time >= '2018-01-01T04:00:00' and record_time < '2018-01-01T05:00:00' and hour=4 and url='msn.com' and ua_country='uk' group by url, ua_country, hour;

 url     | ua_country | avg_ttfb
---------+------------+----------
 msn.com |         uk |        4

(1 rows)
```
