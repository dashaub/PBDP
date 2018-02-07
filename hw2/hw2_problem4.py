"""
Load 4 logfiles--each with a thread--and answer 3 queries about the data
"""
import threading
from multiprocessing.dummy import Pool as ThreadPool

SEP = '\t'
class LogProcessor():
    def __init__(self, num_files=4):
        """
        Constructor
        :param num_files: The number of files (and threads)
        """
        # Lock for the shared state
        self.lock = threading.Lock()
        # The shared state, stored as nested dict
        self.shared = {}
        # The filenames to process
        self.files = ['log_file_0' + str(i + 1) + '.txt' for i in range(num_files)]

    def process_data(self, url, user):
        """
        Add a single observation to the shared state
        :param timestamp: The timestamp of the observation
        :param url: The URL of the observation
        :param user: The uesr of the observation
        """
        try:
            # Test if the URL exists already
            _ = self.shared[url]
            try:
                # Now test if the subdict exists
                count = self.shared[url][user]
                # Increment the count
                self.shared[url][user] = count + 1
            except(KeyError) as ke2:
                # User does not exist in subdict, so create it
                self.shared[url][user] = 1
        except(KeyError) as ke1:
            # URL does not exist, so create it and a subdict with the user
            self.shared[url] = {user: 1}

    def process_log(self, filename):
        """
        Process a single logfile
        :param filename: The filename of the log to process
        """
        with open(filename, 'r') as f:
            # Read first line
            line = f.readline()
            # Stop reading on EOF
            while(len(line) > 0):
                # Split the string into a list with timestamp, URL, and user
                parsed = line.split(SEP)
                # Timestamp isn't actually needed
                timestamp = parsed[0]
                url = parsed[1]
                # User has trailing newline
                user = parsed[2].split('\n')[0]
                self.lock.acquire()
                self.process_data(url, user)
                self.lock.release()
                # Read next line
                line = f.readline()

    def process_all(self):
        """
        Process all the log files.
        """
        for filename in self.files:
            self.process_log(filename)

lp = LogProcessor()
lp.process_all()
