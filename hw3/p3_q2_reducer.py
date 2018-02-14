#!/usr/bin/python

import sys

current_user = None
current_url = None
count = 0
for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        url = current_line[0]
        user = current_line[1]

        # If same URL, we might increment count
        if url == current_url:
            # Only increment if this is a new user
            if user != current_user:
                current_user = user
                count += 1
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(url, count))
            current_url = url
            current_user = user
            count = 1

