#!/usr/bin/python

import sys


current_hour = None
# For our data we do not have that many URLs for each hour. If the cardinality of this was very large,
# this set would not be safe for memory. The solution is to once again use a two stage MR job.
url_set = set()

for line in sys.stdin:
    try:
        hour, url = line.strip().split('\t')
        # If same hour, we might increment count
        if hour == current_hour:
            url_set.add(url)
        else:
            # Only emit results if there is data
            if len(url_set) > 0:
                print("{}\t{}".format(current_hour, len(url_set)))
            url_set = set()
        current_hour = hour
    except ValueError:
        continue

if len(url_set) > 0:
    print("{}\t{}".format(current_hour, len(url_set)))
