#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        user = current_line[2]
        # Build key of url and value of user
        print("{}\t{}".format(url, user))
