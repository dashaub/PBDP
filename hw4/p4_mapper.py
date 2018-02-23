#!/usr/bin/python
"""
Mapper that emits a tuple for only a matching URL and date. The date should be YYYY-MM-DD.
"""
import sys

filter_url = sys.argv[1]
filter_date = sys.argv[2]

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None and len(current_line) == 3:
        timestamp, url, _ = current_line
        # We might emit if the URL matches our filter URL
        if filter_url == url:
            # Extract the day
            day = timestamp[:10]
            # Only emit if this matches the specified day
            if day == filter_date:
                # Emit a key with the hour and a value with count 1
                hour = timestamp[11:13]
                print('{}\t1'.format(hour))
