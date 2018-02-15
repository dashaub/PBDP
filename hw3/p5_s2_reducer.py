#!/usr/bin/python

import sys

# Dictionary to hold the top items
count_dict = {}
# Number of top items we want to print
TOP_N = 5

def add_to_dict(current_dict, url, count):
    # If we don't yet have the TOP_N items, add the url and count to the dict
    if len(current_dict) < TOP_N:
        current_dict[url] = count
        return current_dict

    current_min = min(current_dict.values())
    # Add to the dict only if the new count is greater than the current min
    if count > current_min:
        # Remove one instance of the current min and add new key/value to the dict
        for k, v in current_dict.iteritems():
            if v == current_min:
                del(current_dict[k])
                current_dict[url] = count
                break
    return current_dict


for line in sys.stdin:
    try:
        _, url, value = line.strip().split('\t')
        # If same url, we increment count
        count_dict = add_to_dict(count_dict, url, value)
    except ValueError:
        continue

# This won't print the items in the order of count. If we did care about this, we should sort
# before printing
for k, v in count_dict.iteritems():
    print("{}\t{}".format(k, v))
