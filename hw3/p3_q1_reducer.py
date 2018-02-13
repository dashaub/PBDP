#!/usr/bin/python

import sys

results = set()
for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        user = current_line[1]
        results.add(user)

print(len(results))
