#!/usr/bin/python

import sys

current_user = None
current_url = None
count = 1
for line in sys.stdin:
    url, user = current_line = line.strip().split('\t')
    if current_url:
        # If same URL, we might increment count
        if url == current_url:
            # Only increment if this is a new user
            if user != current_user:
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(url, count))
            count = 1
    current_user = user
    current_url = url

if count > 0:
    print("{}\t{}".format(url, count))
