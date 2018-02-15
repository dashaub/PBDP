#!/usr/bin/python

import sys

def extract_hour(timestamp):
    """
    Return a timestamp with no more precision than the hour.
    :param timestamp: A string representation of a timestamp
    """
    return timestamp[:13]

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        timestamp = current_line[0]
        hour = extract_hour(timestamp)
        url = current_line[1]
        # Key is the hour, value is url
        print("{}\t{}".format(hour, url))
