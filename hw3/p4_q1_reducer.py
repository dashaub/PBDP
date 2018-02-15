#!/usr/bin/python

import sys


current_hour = None
current_url = None
count = 0

for line in sys.stdin:
    try:
        hour, url = line.strip().split('\t')
        # If same hour, we might increment count
        if hour == current_hour:
            # Only increment if this is a new url
            if url != current_url:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_hour, count))
            count = 1
        current_url = url
        current_hour = hour
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_hour, count))
