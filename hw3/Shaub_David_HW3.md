

## Problem 3
We can validate the map reduce job by comparing with the results of a shell 
```
$ cat logs_*.txt | gawk '{print $2}' | sort | uniq -c
18750 http://example.com/?url=0
18750 http://example.com/?url=1
18750 http://example.com/?url=10
18750 http://example.com/?url=11
18750 http://example.com/?url=12
18750 http://example.com/?url=2
18750 http://example.com/?url=3
18750 http://example.com/?url=4
18750 http://example.com/?url=5
18750 http://example.com/?url=6
18750 http://example.com/?url=7
18750 http://example.com/?url=8
18750 http://example.com/?url=9
```

```
$ cat logs_*.txt | gawk '{print $2, $3}' | sort | gawk '{print $1}' | sort | uniq -c
18750 http://example.com/?url=0
18750 http://example.com/?url=1
18750 http://example.com/?url=10
18750 http://example.com/?url=11
18750 http://example.com/?url=12
18750 http://example.com/?url=2
18750 http://example.com/?url=3
18750 http://example.com/?url=4
18750 http://example.com/?url=5
18750 http://example.com/?url=6
18750 http://example.com/?url=7
18750 http://example.com/?url=8
18750 http://example.com/?url=9
```

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
