#!/usr/bin/python

import sys

current_url = None
count = 0
# For our data we do not have that many users for each URL. If the cardinality of this was very large,
# this set would not be safe for memory. The solution is to once again use a two stage MR job.
distinct_users = set()

for line in sys.stdin:
    try:
        url, user = line.strip().split('\t')
        # If same URL, we might increment count
        if url == current_url:
            distinct_users.add(user)
        else:
            # Only emit results if there is data
            if len(distinct_users) > 0:
                print("{}\t{}".format(current_url, len(distinct_users)))
            distinct_users = set()
        current_url = url
    except ValueError:
        continue

if len(distinct_users) > 0:
    print("{}\t{}".format(current_url, len(distinct_users)))
