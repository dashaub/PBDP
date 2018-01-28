Homework 1
===========
David Shaub
-----------
2017-01-27
----------

### Problem 1
Run the program:
```
$ ./hw1_problem1.py --numFiles 13 --numLines 45
```
We see that the expected files are generated:
```
$ ls *.txt
david_shaub_0.txt   david_shaub_12.txt  david_shaub_5.txt  david_shaub_9.txt
david_shaub_1.txt   david_shaub_2.txt   david_shaub_6.txt
david_shaub_10.txt  david_shaub_3.txt   david_shaub_7.txt
david_shaub_11.txt  david_shaub_4.txt   david_shaub_8.txt

```
The files have the expected number of lines:
```
$ wc -l *.txt
  45 david_shaub_0.txt
  45 david_shaub_1.txt
  45 david_shaub_10.txt
  45 david_shaub_11.txt
  45 david_shaub_12.txt
  45 david_shaub_2.txt
  45 david_shaub_3.txt
  45 david_shaub_4.txt
  45 david_shaub_5.txt
  45 david_shaub_6.txt
  45 david_shaub_7.txt
  45 david_shaub_8.txt
  45 david_shaub_9.txt
 585 total
```
And there are three random numbers per line between 0 and 10, inclusive:
```
$ head david_shaub_0.txt
8 0 5
7 2 3
4 6 10
2 7 1
8 9 8
9 5 9
4 3 3
5 6 7
9 2 6
3 3 9

```

### Problem 2

### Problem 3
Inside the mariadb shell, we create our database and select it:
```
> create database shaub;
Query OK, 1 row affected (0.01 sec)

> use shaub;
Database changed

```
We create the table with the specified schema:
```
> create table testtable
(
ID int not null auto_increment primary key,
name varchar(30) not null,
creation_date date
);
Query OK, 0 rows affected (0.10 sec)

```
We insert two records:
```
> insert into testtable (name, creation_date) values ('Richard Stallman', '2000-01-01');
Query OK, 1 row affected (0.04 sec)

> insert into testtable (name, creation_date) values ('Linus Torvalds', '2018-01-026');
Query OK, 1 row affected (0.02 sec)


```
And query to see that the results appear:
```
> select * from testtable;
+----+------------------+---------------+
| ID | name             | creation_date |
+----+------------------+---------------+
|  1 | Richard Stallman | 2000-01-01    |
|  2 | Linus Torvalds   | 2018-01-26    |
+----+------------------+---------------+
2 rows in set (0.00 sec)

```

### Problem 4

