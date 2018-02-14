#!/usr/bin/python

import sys

current_user_url = None
count = 0

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        user_url = current_line[0]
        value = current_line[1]

        # If same URL-user, we might increment count
        if user_url == current_user_url:
            count += int(value)
        else:
            # Only emit results if there is data
            if count > 0:
                print("{}\t{}".format(user_url, count))
            current_user_url = user_url
            count = 1
