"""
Load 4 logfiles--each with a thread--and answer 3 queries about the data
"""
import threading
from multiprocessing.dummy import Pool as ThreadPool

SEP = '\t'
class LogProcessor(object):
    """
    A class for processing log files and printing information about them
    """
    def __init__(self, num_files=4):
        """
        Constructor
        :param num_files: The number of files (and threads)
        """
        self.num_threads = num_files
        # Lock for the shared state
        self.lock = threading.Lock()
        # The shared state, stored as nested dict
        self.shared = {}
        # The filenames to process
        self.files = ['log_file_0' + str(i + 1) + '.txt' for i in range(num_files)]

    def process_data(self, url, user):
        """
        Add a single observation to the shared state
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
            except KeyError:
                # User does not exist in subdict, so create it
                self.shared[url][user] = 1
        except KeyError:
            # URL does not exist, so create it and a subdict with the user
            self.shared[url] = {user: 1}

    def process_log(self, filename):
        """
        Process a single logfile
        :param filename: The filename of the log to process
        """
        with open(filename, 'r') as file_con:
            # Read first line
            line = file_con.readline()
            # Stop reading on EOF
            while line:
                # Split the string into a list with timestamp, URL, and user
                parsed = line.split(SEP)
                # Timestamp isn't actually needed
                url = parsed[1]
                # User has trailing newline
                user = parsed[2].split('\n')[0]
                self.lock.acquire()
                self.process_data(url, user)
                self.lock.release()
                # Read next line
                line = file_con.readline()

    def process_all(self):
        """
        Process all the log files. Use one thread for each log file.
        """
        pool = ThreadPool(self.num_threads)
        _ = pool.map(lambda x: self.process_log(filename=x), self.files)

    def print_results(self):
        """
        Print all the results
        """
        self.print_q1()
        self.print_q2()
        self.print_q3()

    def print_q1(self):
        """
        Print the results for the first query
        Count of unique URLs
        """
        num_logs = len(self.shared)
        print('Number of distinct URLs: {}'.format(num_logs))

    def print_q2(self):
        """
        Print the results for the second query
        Count of unique visitors per URL
        """
        print('Number of distinct visitors for each URL')
        for url in self.shared:
            num_visitors = len(self.shared[url])
            print('{}: {}'.format(url, num_visitors))

    def print_q3(self):
        """
        Print the results for the third query
        Number of visits for each URL per user
        """
        print('Number of visits for each URL')
        for url in self.shared:
            num_visits = 0
            for user in self.shared[url]:
                num_visits += self.shared[url][user]
            print('{}: {}'.format(url, num_visits))

log_processor = LogProcessor()
log_processor.process_all()
log_processor.print_results()
