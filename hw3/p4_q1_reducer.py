#!/usr/bin/python

import sys

current_user = None
count = 0

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        user = current_line[1]
        if user != current_user:
            count += 1
            current_user = user

print(count)
