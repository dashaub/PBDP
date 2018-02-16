#!/usr/bin/python

import sys

current_url_user_hour = None
count = 0

for line in sys.stdin:
    url_user_hour, value = line.strip().split('\t')
    # If same URL/user/hour, increment the count
    if url_user_hour == current_url_user_hour:
        count += int(value)
    else:
        # Only emit results if there is data
        if count > 0:
            print("{}\t{}".format(current_url_user_hour, count))
        current_url_user_hour = url_user_hour
        count = 1

if count > 0:
    print("{}\t{}".format(url_user_hour, count))
