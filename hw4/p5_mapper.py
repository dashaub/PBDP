#!/usr/bin/python

import sys
from json import loads

for line in sys.stdin:
    current_line = loads(line)
    if current_line is not None:
        uuid = current_line['uuid']
        url = current_line['url']
        user = current_line['user']
        # Build key is url/user and the value is the uuid
        print("{} : {}\t{}".format(url, user, uuid))
