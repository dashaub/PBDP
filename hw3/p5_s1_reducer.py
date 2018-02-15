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
