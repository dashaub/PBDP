#!/usr/bin/python

import sys

current_url_hour = None
# For our data we do not have that many users for each URL/hour combination. If the cardinality of this was very large,
# this set would not be safe for memory. The solution is to once again use a two stage MR job.
user_set = set()
count = 0

for line in sys.stdin:
    try:
        url_hour, user = line.strip().split('\t')
        # If same URL/hour, we might increment count
        if url_hour == current_url_hour:
            user_set.add(user)
        else:
            # Only emit results if there is data
            if len(user_set) > 0:
                print("{}\t{}".format(current_url_hour, len(user_set)))
            user_set = set()
        current_url_hour = url_hour
    except ValueError:
        continue

if len(user_set) > 0:
    print("{}\t{}".format(current_url_hour, len(user_set)))
