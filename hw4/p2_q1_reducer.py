#!/usr/bin/python

import sys

current_user = None
count = 0

# For our data we do not have that many distinct users. If the cardinality of this was very large,
# this set would not be safe for memory. The solution is to use a two stage MR job.
all_users = set()

for line in sys.stdin:
    current_line = line.strip().split('\t')
    if len(current_line) == 2:
        user = current_line[1]
        # If we have very high cardinality for number of users, this implementation is not memory safe.
        # For our dataset this is not a problem, but if it were we could do a two-stage MR job
        # Where the first job is a "word count" type job and the second stage then returns
        # a count for the total number of records.
        all_users.add(user)

print(len(all_users))
