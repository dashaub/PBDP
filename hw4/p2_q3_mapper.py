#!/usr/bin/python

import sys
from json import loads

for line in sys.stdin:
    current_line = loads(line)
    if current_line is not None:
        url = current_line['url']
        user = current_line['user']
        # Build key is url/user and the value is a count 1
        print("{} : {}\t{}".format(url, user, 1))
