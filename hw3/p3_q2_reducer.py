#!/usr/bin/python

import sys


for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        url = current_line[0]
        user = current_line[1]
        results.add(user)

print("{}\t{}".format(url, len(results)))
