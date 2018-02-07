"""
Load 4 logfiles--each with a thread--and answer 3 queries about the data
"""
logfiles = ['log_file_01.txt', 'log_file_02.txt',
            'log_file_03.txt', 'log_file_04.txt']

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
        # Read next line
        line = f.readline()
