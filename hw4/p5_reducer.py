#!/usr/bin/python

import sys

uuid_set = set()
current_user_url = None
count = 0

for line in sys.stdin:
    user_url, value = line.strip().split('\t')
    # If same URL/user, we might increment count
    if user_url == current_user_url:
        # Only increment the count if we haven't encountered this UUID yet
        if value not in uuid_set:
            uuid_set.add(value)
            count += 1
    else:
        # Only emit results if there is data
        if len(uuid_set) > 0:
            print("{}\t{}".format(current_user_url, count))
        current_user_url = user_url
        uuid_set = set(value)
        count = 0

if len(uuid_set) > 0:
    print("{}\t{}".format(user_url, count))
