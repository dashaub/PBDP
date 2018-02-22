---
title: Homework 4
author: David Shaub
geometry: margin=2cm
date: 2018-02-24
---

All problems were completed, including problem 5.

## Problem 1

The python file `p1_avrowriter.py`:
```
"""Load the text logfiles and save them in a single avro file"""
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Read the schema file
schema = avro.schema.parse(open('input_logs.avsc', 'rb').read())

# Define the files to process
input_files = ['logs_{}.txt'.format(i) for i in range(4)]
with DataFileWriter(open('logs.avro', 'wb'), DatumWriter(), schema) as writer:
    # Process each input file
    for input_file in input_files:
        # Open each input file
        with open(input_file, 'r') as current_file:
            # Process each line in each input file
            for line in current_file:
                current_line = line.strip().split('\t')
                # Only parse and write if there is correct input
                if len(current_line) == 3:
                    timestamp, url, user = current_line
                    # Write to avro
                    writer.append({'timestamp': timestamp, 'url': url, 'user': user})
```

The avro schema was easy to create since all fields are `string` values. The only field that *could* have another type would be the timestamp field as a date/time object, but since avro does not have a primitive date/time object, we use a string for this too:
```
{"namespace": "logs.avro",
 "type": "record",
 "name": "visits",
 "fields": [
     {"name": "timestamp", "type": "string"},
     {"name": "url",  "type": "string"},
     {"name": "user", "type": "string"}
 ]
}
```
We launch the conversion process:
```
$ python p1_avrowriter.py
```
And examine the results in `logs.avro` that are partially human-readable:
```
$ head -c 2000 logs.avro 
Objavro.schema?{"type": "record", "namespace": "logs.avro", "name": "visits", "fields": [{"type": "string", "name": "timestamp"}, {"type": "string", "name": "url"}, {"type": "string", "name": "user"}]}avro.codenullg?W?Lܕ?N
?[??o????(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                        User_0(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                                                             User_1(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                                                                                                                  User_2(2018-02-13T00:00:00Z2http://example.com/?url=0
           User_3(2018-02-13T00:00:00Z2http://example.com/?url=0
                                                                User_402018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                         User_002018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                                                                                  User_102018-02-13T00:00:19.200Z2http://example.com/?url=0
                               User_202018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                        User_302018-02-13T00:00:19.200Z2http://example.com/?url=0
                                                                                                                                                 User_402018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                                                                                                          User_002018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                       User_102018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                User_202018-02-13T00:00:38.400Z2http://example.com/?url=0
                                                                                                                                                                         User_302018-02-13T00:00:38.400Z2http://example.com/?url=0
                      User_402018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                               User_002018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                                                        User_102018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                                                                                                                 User_202018-02-13T00:00:57.600Z2http://example.com/?url=0
                                              User_302018-02-13T00:00:57.600Z2http://example.com/?url=0
                                                                                                       User_402018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                                                                User_002018-02-13T00:01:16.800Z2http://example.com/?url=0
             User_102018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                      User_202018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                               User_302018-02-13T00:01:16.800Z2http://example.com/?url=0
                                                                                                                                                                                        User_4(2018-02-13T00:01:36Z2http://example.com/?url=0
                                 User_0(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                      User_1(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                                                                           User_2(2018-02-13T00:01:36Z2http://example.com/?url=0
                                                                                                                                                                                                User_3(2018-02-13T00:01:36Z2http://example.com/?url=0
                                         User_402018-02-13T00:01:55.200Z2http://example.com/?url=0
```
