#!/usr/bin/python

def extract_hour(timestamp):
    """
    Return a timestamp with no more precision than the hour.
    :param timestamp: A string representation of a timestamp
    """
    return timestamp[:13]

import sys

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None:
        timestamp = current_line[0]
        hour = extract_hour(timestamp)
        url = current_line[1]
        user = current_line[2]
        url_hour = url + ' : ' + hour
        # Build key-value pairs url/hour as the key and user as the value
        print("{}\t{}".format(url_hour, user))
