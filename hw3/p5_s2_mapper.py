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
