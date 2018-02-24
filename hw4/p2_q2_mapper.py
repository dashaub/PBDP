#!/usr/bin/python

import sys
from json import loads

for line in sys.stdin:
    current_line = loads(line)
    if current_line is not None:
        url = current_line['url']
        user = current_line['user']
        # Build key of url and value of user
        print("{}\t{}".format(url, user))
