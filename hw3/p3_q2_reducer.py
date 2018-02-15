#!/usr/bin/python

import sys

current_user = None
current_url = None
count = 0

for line in sys.stdin:
    try:
        url, user = line.strip().split('\t')
        # If same URL, we might increment count
        if url == current_url:
            # Only increment if this is a new user
            if user != current_user:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(current_url, count))
            count = 1
        current_user = user
        current_url = url
    except ValueError:
        continue

if count > 0:
    print("{}\t{}".format(url, count))
