---
title: Homework 3
author: David Shaub
geometry: margin=2cm
date: 2018-02-16
---

## Problem 1

Our cluster has one master node and two slave nodes:
![cluster](cluster.png)

We connect to the master node:
```
$ ssh -i ~/ohio.pem hadoop@ec2-18-221-60-255.us-east-2.compute.amazonaws.com
The authenticity of host 'ec2-18-221-60-255.us-east-2.compute.amazonaws.com (18.221.60.255)' can't be established.
ECDSA key fingerprint is SHA256:753xHLDs0hHFD/hr6b3y0a9yyCDzULJHXKSYUvJQm/Y.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'ec2-18-221-60-255.us-east-2.compute.amazonaws.com,18.221.60.255' (ECDSA) to the list of known hosts.

       __|  __|_  )
       _|  (     /   Amazon Linux AMI
      ___|\___|___|

https://aws.amazon.com/amazon-linux-ami/2017.09-release-notes/
11 package(s) needed for security, out of 13 available
Run "sudo yum update" to apply all updates.
                                                                    
EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR    
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R   
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R 
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R 
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR   
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R  
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR
```

Using the `hdfs version` command we can see which version we have:

![hdfs version](hdfs_version.png)

We perform some manipulations on hdfs (i.e. creating a directory, copying our data files, verifying our files have been copied):
```

```

## Problem 2

## Problem 3

**Q1**

The mapper is `p3_q1_mapper.py`:
```
#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        # Send all URLs to the same reducer. Since our data is not too large, we can get away
        # with this. If we really had "Big Data" and wished to reduce the load better, we should
        # utilize a combiner here so far fewer duplicate rows of input must be processed by the reducer.
        print("1\t{}".format(url))
```

The reducer is `p3_q1_reducer.py`:
```
#!/usr/bin/python

import sys

current_user = None
count = 0

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        user = current_line[1]
        # We'll increment the counter when a new user is encountered
        if user != current_user:
            count += 1
            current_user = user

print(count)
```

We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p3_q1_mapper.py | sort | ./p3_q1_reducer.py
13

```
We can also validate that this is the correct result by comparing a shell one-liner:
```
$ cat logs_*.txt | gawk '{print $2}' | sort | uniq | wc -l
13

```

**Q2**

The mapper is `p3_q2_mapper.py`:
```
#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        user = current_line[2]
        # Build key of url and value of user
        print("{}\t{}".format(url, user))
```

The reducer is `p3_q2_reducer.py`:
```
#!/usr/bin/python

import sys

current_user = None
current_url = None
count = 0

for line in sys.stdin:
    try:
        url, user = line.strip().split('\t')
        # If same URL, we might increment count
        if url == current_url:
            # Only increment if this is a new user
            if user != current_user:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_url, count))
            count = 1
        current_user = user
        current_url = url
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_url, count))
```

We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p3_q2_mapper.py | sort | ./p3_q2_reducer.py 
http://example.com/?url=0	5
http://example.com/?url=1	5
http://example.com/?url=10	5
http://example.com/?url=11	5
http://example.com/?url=12	5
http://example.com/?url=2	5
http://example.com/?url=3	5
http://example.com/?url=4	5
http://example.com/?url=5	5
http://example.com/?url=6	5
http://example.com/?url=7	5
http://example.com/?url=8	5
http://example.com/?url=9	5
```

And a similar shell verification:
```
$ cat logs_*.txt | gawk '{print $2, $3}' | sort | uniq | gawk '{print $1}' | sort | uniq -c
   5 http://example.com/?url=0
   5 http://example.com/?url=1
   5 http://example.com/?url=10
   5 http://example.com/?url=11
   5 http://example.com/?url=12
   5 http://example.com/?url=2
   5 http://example.com/?url=3
   5 http://example.com/?url=4
   5 http://example.com/?url=5
   5 http://example.com/?url=6
   5 http://example.com/?url=7
   5 http://example.com/?url=8
   5 http://example.com/?url=9
```

**Q3**

The mapper is `p3_q3_mapper.py`:
```
#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        user = current_line[2]
        # Build key is url/user and the value is a count 1
        print("{} : {}\t{}".format(url, user, 1))
```

The reducer is `p3_q3_reducer.py`:
```
#!/usr/bin/python

import sys

current_user_url = None
count = 0

for line in sys.stdin:
    user_url, value = line.strip().split('\t')
    # If same URL/user, we might increment count
    if user_url == current_user_url:
        count += int(value)
    else:
        # Only emit results if there is data
        if count > 0:
            print("{}\t{}".format(current_user_url, count))
        current_user_url = user_url
        count = 1

if count > 0:
    print("{}\t{}".format(user_url, count))
```

We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p3_q3_mapper.py | sort | ./p3_q3_reducer.py 
http://example.com/?url=0 : User_0	3750
http://example.com/?url=0 : User_1	3750
http://example.com/?url=0 : User_2	3750
http://example.com/?url=0 : User_3	3750
http://example.com/?url=0 : User_4	3750
http://example.com/?url=1 : User_0	3750
http://example.com/?url=1 : User_1	3750
http://example.com/?url=1 : User_2	3750
http://example.com/?url=1 : User_3	3750
http://example.com/?url=1 : User_4	3750
http://example.com/?url=10 : User_0	3750
http://example.com/?url=10 : User_1	3750
http://example.com/?url=10 : User_2	3750
http://example.com/?url=10 : User_3	3750
http://example.com/?url=10 : User_4	3750
http://example.com/?url=11 : User_0	3750
http://example.com/?url=11 : User_1	3750
http://example.com/?url=11 : User_2	3750
http://example.com/?url=11 : User_3	3750
http://example.com/?url=11 : User_4	3750
http://example.com/?url=12 : User_0	3750
http://example.com/?url=12 : User_1	3750
http://example.com/?url=12 : User_2	3750
http://example.com/?url=12 : User_3	3750
http://example.com/?url=12 : User_4	3750
http://example.com/?url=2 : User_0	3750
http://example.com/?url=2 : User_1	3750
http://example.com/?url=2 : User_2	3750
http://example.com/?url=2 : User_3	3750
http://example.com/?url=2 : User_4	3750
http://example.com/?url=3 : User_0	3750
http://example.com/?url=3 : User_1	3750
http://example.com/?url=3 : User_2	3750
http://example.com/?url=3 : User_3	3750
http://example.com/?url=3 : User_4	3750
http://example.com/?url=4 : User_0	3750
http://example.com/?url=4 : User_1	3750
http://example.com/?url=4 : User_2	3750
http://example.com/?url=4 : User_3	3750
http://example.com/?url=4 : User_4	3750
http://example.com/?url=5 : User_0	3750
http://example.com/?url=5 : User_1	3750
http://example.com/?url=5 : User_2	3750
http://example.com/?url=5 : User_3	3750
http://example.com/?url=5 : User_4	3750
http://example.com/?url=6 : User_0	3750
http://example.com/?url=6 : User_1	3750
http://example.com/?url=6 : User_2	3750
http://example.com/?url=6 : User_3	3750
http://example.com/?url=6 : User_4	3750
http://example.com/?url=7 : User_0	3750
http://example.com/?url=7 : User_1	3750
http://example.com/?url=7 : User_2	3750
http://example.com/?url=7 : User_3	3750
http://example.com/?url=7 : User_4	3750
http://example.com/?url=8 : User_0	3750
http://example.com/?url=8 : User_1	3750
http://example.com/?url=8 : User_2	3750
http://example.com/?url=8 : User_3	3750
http://example.com/?url=8 : User_4	3750
http://example.com/?url=9 : User_0	3750
http://example.com/?url=9 : User_1	3750
http://example.com/?url=9 : User_2	3750
http://example.com/?url=9 : User_3	3750
http://example.com/?url=9 : User_4	3750
```

And a similar shell verification:
```
$ cat logs_*.txt | gawk '{print $2, $3}' | sort | uniq -c
3750 http://example.com/?url=0 User_0
3750 http://example.com/?url=0 User_1
3750 http://example.com/?url=0 User_2
3750 http://example.com/?url=0 User_3
3750 http://example.com/?url=0 User_4
3750 http://example.com/?url=1 User_0
3750 http://example.com/?url=1 User_1
3750 http://example.com/?url=1 User_2
3750 http://example.com/?url=1 User_3
3750 http://example.com/?url=1 User_4
3750 http://example.com/?url=10 User_0
3750 http://example.com/?url=10 User_1
3750 http://example.com/?url=10 User_2
3750 http://example.com/?url=10 User_3
3750 http://example.com/?url=10 User_4
3750 http://example.com/?url=11 User_0
3750 http://example.com/?url=11 User_1
3750 http://example.com/?url=11 User_2
3750 http://example.com/?url=11 User_3
3750 http://example.com/?url=11 User_4
3750 http://example.com/?url=12 User_0
3750 http://example.com/?url=12 User_1
3750 http://example.com/?url=12 User_2
3750 http://example.com/?url=12 User_3
3750 http://example.com/?url=12 User_4
3750 http://example.com/?url=2 User_0
3750 http://example.com/?url=2 User_1
3750 http://example.com/?url=2 User_2
3750 http://example.com/?url=2 User_3
3750 http://example.com/?url=2 User_4
3750 http://example.com/?url=3 User_0
3750 http://example.com/?url=3 User_1
3750 http://example.com/?url=3 User_2
3750 http://example.com/?url=3 User_3
3750 http://example.com/?url=3 User_4
3750 http://example.com/?url=4 User_0
3750 http://example.com/?url=4 User_1
3750 http://example.com/?url=4 User_2
3750 http://example.com/?url=4 User_3
3750 http://example.com/?url=4 User_4
3750 http://example.com/?url=5 User_0
3750 http://example.com/?url=5 User_1
3750 http://example.com/?url=5 User_2
3750 http://example.com/?url=5 User_3
3750 http://example.com/?url=5 User_4
3750 http://example.com/?url=6 User_0
3750 http://example.com/?url=6 User_1
3750 http://example.com/?url=6 User_2
3750 http://example.com/?url=6 User_3
3750 http://example.com/?url=6 User_4
3750 http://example.com/?url=7 User_0
3750 http://example.com/?url=7 User_1
3750 http://example.com/?url=7 User_2
3750 http://example.com/?url=7 User_3
3750 http://example.com/?url=7 User_4
3750 http://example.com/?url=8 User_0
3750 http://example.com/?url=8 User_1
3750 http://example.com/?url=8 User_2
3750 http://example.com/?url=8 User_3
3750 http://example.com/?url=8 User_4
3750 http://example.com/?url=9 User_0
3750 http://example.com/?url=9 User_1
3750 http://example.com/?url=9 User_2
3750 http://example.com/?url=9 User_3
3750 http://example.com/?url=9 User_4
```

### Problem 4

**Q1**

The mapper is `p4_q1_mapper.py`:
```
#!/usr/bin/python

import sys

def extract_hour(timestamp):
    """
    Return a timestamp with no more precision than the hour.
    :param timestamp: A string representation of a timestamp
    """
    return timestamp[:13]

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        timestamp = current_line[0]
        hour = extract_hour(timestamp)
        url = current_line[1]
        # Key is the hour, value is url
        print("{}\t{}".format(hour, url))
```

The reducer is `p4_q1_reducer.py`:
```
#!/usr/bin/python

import sys


current_hour = None
current_url = None
count = 0

for line in sys.stdin:
    try:
        hour, url = line.strip().split('\t')
        # If same hour, we might increment count
        if hour == current_hour:
            # Only increment if this is a new url
            if url != current_url:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_hour, count))
            count = 1
        current_url = url
        current_hour = hour
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_hour, count))
```

We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p4_q1_mapper.py | sort | ./p4_q1_reducer.py 
2018-02-13T00	13
2018-02-13T01	13
2018-02-13T02	13
2018-02-13T03	13
2018-02-13T04	13
```

**Q2**

The mapper is `p4_q2_mapper.py`:
```
#!/usr/bin/python
import sys

def extract_hour(timestamp):
    """
    Return a timestamp with no more precision than the hour.
    :param timestamp: A string representation of a timestamp
    """
    return timestamp[:13]

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        timestamp = current_line[0]
        hour = extract_hour(timestamp)
        url = current_line[1]
        user = current_line[2]
        url_hour = url + ' : ' + hour
        # Build key-value pairs url/hour as the key and user as the value
        print("{}\t{}".format(url_hour, user))
```

The reducer is `p4_q2_reducer.py`:
```
#!/usr/bin/python

import sys

current_user = None
current_url_hour = None
count = 0

for line in sys.stdin:
    try:
        url_hour, user = line.strip().split('\t')
        # If same URL/hour, we might increment count
        if url_hour == current_url_hour:
            # Only increment if this is a new user
            if user != current_user:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_url_hour, count))
            count = 1
        current_user = user
        current_url_hour = url_hour
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_url_hour, count))
```

We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p4_q2_mapper.py | sort | ./p4_q2_reducer.py 
http://example.com/?url=0 : 2018-02-13T00	5
http://example.com/?url=0 : 2018-02-13T01	5
http://example.com/?url=0 : 2018-02-13T02	5
http://example.com/?url=0 : 2018-02-13T03	5
http://example.com/?url=0 : 2018-02-13T04	5
http://example.com/?url=1 : 2018-02-13T00	5
http://example.com/?url=1 : 2018-02-13T01	5
http://example.com/?url=1 : 2018-02-13T02	5
http://example.com/?url=1 : 2018-02-13T03	5
http://example.com/?url=1 : 2018-02-13T04	5
http://example.com/?url=10 : 2018-02-13T00	5
http://example.com/?url=10 : 2018-02-13T01	5
http://example.com/?url=10 : 2018-02-13T02	5
http://example.com/?url=10 : 2018-02-13T03	5
http://example.com/?url=10 : 2018-02-13T04	5
http://example.com/?url=11 : 2018-02-13T00	5
http://example.com/?url=11 : 2018-02-13T01	5
http://example.com/?url=11 : 2018-02-13T02	5
http://example.com/?url=11 : 2018-02-13T03	5
http://example.com/?url=11 : 2018-02-13T04	5
http://example.com/?url=12 : 2018-02-13T00	5
http://example.com/?url=12 : 2018-02-13T01	5
http://example.com/?url=12 : 2018-02-13T02	5
http://example.com/?url=12 : 2018-02-13T03	5
http://example.com/?url=12 : 2018-02-13T04	5
http://example.com/?url=2 : 2018-02-13T00	5
http://example.com/?url=2 : 2018-02-13T01	5
http://example.com/?url=2 : 2018-02-13T02	5
http://example.com/?url=2 : 2018-02-13T03	5
http://example.com/?url=2 : 2018-02-13T04	5
http://example.com/?url=3 : 2018-02-13T00	5
http://example.com/?url=3 : 2018-02-13T01	5
http://example.com/?url=3 : 2018-02-13T02	5
http://example.com/?url=3 : 2018-02-13T03	5
http://example.com/?url=3 : 2018-02-13T04	5
http://example.com/?url=4 : 2018-02-13T00	5
http://example.com/?url=4 : 2018-02-13T01	5
http://example.com/?url=4 : 2018-02-13T02	5
http://example.com/?url=4 : 2018-02-13T03	5
http://example.com/?url=4 : 2018-02-13T04	5
http://example.com/?url=5 : 2018-02-13T00	5
http://example.com/?url=5 : 2018-02-13T01	5
http://example.com/?url=5 : 2018-02-13T02	5
http://example.com/?url=5 : 2018-02-13T03	5
http://example.com/?url=5 : 2018-02-13T04	5
http://example.com/?url=6 : 2018-02-13T00	5
http://example.com/?url=6 : 2018-02-13T01	5
http://example.com/?url=6 : 2018-02-13T02	5
http://example.com/?url=6 : 2018-02-13T03	5
http://example.com/?url=6 : 2018-02-13T04	5
http://example.com/?url=7 : 2018-02-13T00	5
http://example.com/?url=7 : 2018-02-13T01	5
http://example.com/?url=7 : 2018-02-13T02	5
http://example.com/?url=7 : 2018-02-13T03	5
http://example.com/?url=7 : 2018-02-13T04	5
http://example.com/?url=8 : 2018-02-13T00	5
http://example.com/?url=8 : 2018-02-13T01	5
http://example.com/?url=8 : 2018-02-13T02	5
http://example.com/?url=8 : 2018-02-13T03	5
http://example.com/?url=8 : 2018-02-13T04	5
http://example.com/?url=9 : 2018-02-13T00	5
http://example.com/?url=9 : 2018-02-13T01	5
http://example.com/?url=9 : 2018-02-13T02	5
http://example.com/?url=9 : 2018-02-13T03	5
http://example.com/?url=9 : 2018-02-13T04	5
```

**Q3**

The mapper is `p4_q3_mapper.py`:
```
#!/usr/bin/python

import sys

def extract_hour(timestamp):
    """
    Return a timestamp with no more precision than the hour.
    :param timestamp: A string representation of a timestamp
    """
    return timestamp[:13]


for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        timestamp = current_line[0]
        hour = extract_hour(timestamp)
        url = current_line[1]
        user = current_line[2]
        url_user_hour = url + ' : ' + user + ' : ' + hour
        # Build key with URL-user and value 1
        print("{}\t{}".format(url_user_hour, 1))
```

The reducer is `p4_q3_reducer.py`:
```
#!/usr/bin/python

import sys

current_url_user_hour = None
count = 0

for line in sys.stdin:
    url_user_hour, value = line.strip().split('\t')
    # If same URL/user/hour, increment the count
    if url_user_hour == current_url_user_hour:
        count += int(value)
    else:
        # Only emit results if there is data
        if count > 0:
            print("{}\t{}".format(current_url_user_hour, count))
        current_url_user_hour = url_user_hour
        count = 1

if count > 0:
    print("{}\t{}".format(url_user_hour, count))
```

We run our map-reduce program on a single node. This is very verbose output, so we'll only print 10 lines here:
```
$ cat logs_*.txt | ./p4_q3_mapper.py | sort | ./p4_q3_reducer.py | head
http://example.com/?url=0 : User_0 : 2018-02-13T00	750
http://example.com/?url=0 : User_0 : 2018-02-13T01	750
http://example.com/?url=0 : User_0 : 2018-02-13T02	750
http://example.com/?url=0 : User_0 : 2018-02-13T03	750
http://example.com/?url=0 : User_0 : 2018-02-13T04	750
http://example.com/?url=0 : User_1 : 2018-02-13T00	750
http://example.com/?url=0 : User_1 : 2018-02-13T01	750
http://example.com/?url=0 : User_1 : 2018-02-13T02	750
http://example.com/?url=0 : User_1 : 2018-02-13T03	750
http://example.com/?url=0 : User_1 : 2018-02-13T04	750
```

### Problem 5

The approach for this problem will be to run two map reduce jobs. The first job is essentially a "word count" job (a mapper that outputs tuples with URL and count 1 and a reducer that outputs the total number of times each URL is observed). The second job sends all URLs and their counts to the same reducer so we can emit only those URLs that are the five most common. Note that this implementation does not implement a stable sort algorithm, handle ties in a manner that might be desired, or print the final top 5 results in order of frequency since these requirements were not mentioned. Since they are properties that are commonly desired, however, comments in the program source code suggest how this could be done.

The first mapper is `p5_s1_mapper.py`:
```
#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        # Key is the url, value is count 1
        print("{}\t{}".format(url, 1))
```

The first reducer is `p5_s1_reducer.py`:
```
#!/usr/bin/python

import sys


current_url = None
count = 0

for line in sys.stdin:
    try:
        url, value = line.strip().split('\t')
        # If same url, we increment count
        if url == current_url:
            count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_url, count))
            count = 1
        current_url = url
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_url, count))

```

The second mapper is `p5_s2_mapper.py`:
```
#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[0]
        count = current_line[1]
        # Send all URLs/counts to the same reducer
        # Since we are dealing with aggregated counts, this should not be a lot of data, so we
        # won't overload one reducer with load
        print("1\t{}\t{}".format(url, count))

```

The second reducer is `p5_s2_reducer.py`:
```
#!/usr/bin/python

import sys

# Dictionary to hold the top items
count_dict = {}
# Number of top items we want to print
TOP_N = 5

def add_to_dict(current_dict, url, count):
    # If we don't yet have the TOP_N items, add the url and count to the dict
    if len(current_dict) < TOP_N:
        current_dict[url] = count
        return current_dict

    current_min = min(current_dict.values())
    # Add to the dict only if the new count is greater than the current min
    # We could modify this rule to >= if we wished different tie-breaking rules for replacements
    if count > current_min:
        # Remove one instance of the current min and add new key/value to the dict
        for k, v in current_dict.iteritems():
            if v == current_min:
                del(current_dict[k])
                current_dict[url] = count
                break
    return current_dict


for line in sys.stdin:
    try:
        _, url, value = line.strip().split('\t')
        # If same url, we increment count
        count_dict = add_to_dict(count_dict, url, value)
    except ValueError:
        continue

# This won't print the items in the order of decreasing count. If we did care about this, we should sort
# before printing
for k, v in count_dict.iteritems():
    print("{}\t{}".format(k, v))

```


We run two chained map reduce jobs on a single node:
```
$ cat logs_*.txt | ./p5_s1_mapper.py | sort | ./p5_s1_reducer.py | ./p5_s2_mapper.py | sort | ./p5_s2_reducer.py 
http://example.com/?url=10	18750
http://example.com/?url=11	18750
http://example.com/?url=12	18750
http://example.com/?url=0	18750
http://example.com/?url=1	18750
``
