#!/usr/bin/python

import sys

current_item = None
current_count = 0

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if current_line is not None and len(current_line) == 2:
        hour, count = current_line
        # If a new hour is encountered we will reset the count and possibly print a result
        if hour != current_item:
            # Print after first item
            if current_item is not None:
                print('{}\t{}'.format(current_item, current_count))
            current_item = hour
            current_count = 0
        current_count += int(count)

if count > 0:
    print('{}\t{}'.format(current_item, current_count))
