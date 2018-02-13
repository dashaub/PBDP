#!/usr/bin/python

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        url = current_line[1]
        user = current_line[2]
        # Build key-value pairs with the URL-user
        print("{}\t{}".format(url))
