

## Problem 3

**Q1**
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
We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p3_q2_mapper.py | sort | ./p3_q2_reducer.py 
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
We run our map-reduce program on a single node:
```
$ cat logs_*.txt | ./p3_q3_mapper.py | sort | ./p3_q3_reducer.py 
http://example.com/?url=0:User_1	3750
http://example.com/?url=0:User_2	3750
http://example.com/?url=0:User_3	3750
http://example.com/?url=0:User_4	3750
http://example.com/?url=10:User_0	3750
http://example.com/?url=10:User_1	3750
http://example.com/?url=10:User_2	3750
http://example.com/?url=10:User_3	3750
http://example.com/?url=10:User_4	3750
http://example.com/?url=11:User_0	3750
http://example.com/?url=11:User_1	3750
http://example.com/?url=11:User_2	3750
http://example.com/?url=11:User_3	3750
http://example.com/?url=11:User_4	3750
http://example.com/?url=12:User_0	3750
http://example.com/?url=12:User_1	3750
http://example.com/?url=12:User_2	3750
http://example.com/?url=12:User_3	3750
http://example.com/?url=12:User_4	3750
http://example.com/?url=1:User_0	3750
http://example.com/?url=1:User_1	3750
http://example.com/?url=1:User_2	3750
http://example.com/?url=1:User_3	3750
http://example.com/?url=1:User_4	3750
http://example.com/?url=2:User_0	3750
http://example.com/?url=2:User_1	3750
http://example.com/?url=2:User_2	3750
http://example.com/?url=2:User_3	3750
http://example.com/?url=2:User_4	3750
http://example.com/?url=3:User_0	3750
http://example.com/?url=3:User_1	3750
http://example.com/?url=3:User_2	3750
http://example.com/?url=3:User_3	3750
http://example.com/?url=3:User_4	3750
http://example.com/?url=4:User_0	3750
http://example.com/?url=4:User_1	3750
http://example.com/?url=4:User_2	3750
http://example.com/?url=4:User_3	3750
http://example.com/?url=4:User_4	3750
http://example.com/?url=5:User_0	3750
http://example.com/?url=5:User_1	3750
http://example.com/?url=5:User_2	3750
http://example.com/?url=5:User_3	3750
http://example.com/?url=5:User_4	3750
http://example.com/?url=6:User_0	3750
http://example.com/?url=6:User_1	3750
http://example.com/?url=6:User_2	3750
http://example.com/?url=6:User_3	3750
http://example.com/?url=6:User_4	3750
http://example.com/?url=7:User_0	3750
http://example.com/?url=7:User_1	3750
http://example.com/?url=7:User_2	3750
http://example.com/?url=7:User_3	3750
http://example.com/?url=7:User_4	3750
http://example.com/?url=8:User_0	3750
http://example.com/?url=8:User_1	3750
http://example.com/?url=8:User_2	3750
http://example.com/?url=8:User_3	3750
http://example.com/?url=8:User_4	3750
http://example.com/?url=9:User_0	3750
http://example.com/?url=9:User_1	3750
http://example.com/?url=9:User_2	3750
http://example.com/?url=9:User_3	3750
http://example.com/?url=9:User_4	3750
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
