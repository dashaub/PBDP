#!/usr/bin/python
"""
Mapper that emits a tuple for only a matching URL and date. The date should be YYYY-MM-DD.
"""
import sys
from json import loads

filter_url = sys.argv[1]
filter_date = sys.argv[2]

for line in sys.stdin:
    current_line = loads(line)
    if current_line is not None:
        timestamp = current_line['timestamp']
        url = current_line['url']
        # We might emit if the URL matches our filter URL
        if filter_url == url:
            # Extract the day
            day = timestamp[:10]
            # Only emit if this matches the specified day
            if day == filter_date:
                # Emit a key with the hour and a value with count 1
                hour = timestamp[11:13]
                print('{}\t1'.format(hour))
