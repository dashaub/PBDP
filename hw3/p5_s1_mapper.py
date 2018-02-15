#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        # Key is the url, value is count 1
        print("{}\t{}".format(url, 1))
