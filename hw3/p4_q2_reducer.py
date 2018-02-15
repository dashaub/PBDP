#!/usr/bin/python

import sys

current_user = None
current_url_hour = None
count = 0

for line in sys.stdin:
    try:
        url_hour, user = line.strip().split('\t')
        # If same URL, we might increment count
        if url_hour == current_url_hour:
            # Only increment if this is a new user
            if user != current_user:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_url_hour, count))
            count = 1
        current_user = user
        current_url_hour = url_hour
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(current_url_hour, count))
