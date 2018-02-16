#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        user = current_line[2]
        # Build key is url/user and the value is a count 1
        print("{} : {}\t{}".format(url, user, 1))
