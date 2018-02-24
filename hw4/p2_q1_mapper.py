#!/usr/bin/python

import sys
from json import loads

for line in sys.stdin:
    current_line = loads(line)
    if current_line is not None:
        url = current_line['url']
        # Send all URLs to the same reducer. Since our data is not too large, we can get away
        # with this. If we really had "Big Data" and wished to reduce the load better, we should
        # utilize a combiner here so far fewer duplicate rows of input must be processed by the reducer.
        # Or we could implement a two-stage MR job as mentioned in the reducer comments.
        print("1\t{}".format(url))
