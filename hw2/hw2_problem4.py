"""
Load 4 logfiles--each with a thread--and answer 3 queries about the data
"""
logfiles = ['log_file_01.txt', 'log_file_02.txt',
            'log_file_03.txt', 'log_file_04.txt']

shared = {}
def process_data(timestamp, url user):
    try:
        # Test if the URL exists already
        _ = shared[url]
        try:
            # Now test if the subdict exists
            count = shared[url][user]
            # Increment the count
            shared[url][user] += 1
        except(KeyError) as ke2:
            # Create the subdict
        shared[url][user] = 1
    except(KeyError) as ke1:
        # URL does not exist, so create it and a subdict with the user
        shared[url] = {user: 1}

sep = '\t'
with open('log_file_01.txt', 'r') as f:
    # Read first line
    line = f.readline()
    # Stop reading on EOF
    while(len(line) > 0):
        # Split the string into a list with timestamp, URL, and user
        parsed = line.split(sep)
        timestamp = parsed[0]
        url = parsed[1]
        user = parsed[2]
        process_data(timestamp, url, user)
        # Read next line
        line = f.readline()
