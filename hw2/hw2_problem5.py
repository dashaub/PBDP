"""
Load 4 logfiles--each with a thread--and answer 3 queries about the data
"""
import argparse
import hashlib
import MySQLdb
import _mysql_exceptions
import threading
from multiprocessing.dummy import Pool as ThreadPool

parser = argparse.ArgumentParser()
parser.add_argument('--startingFile', type=int,
                    help='The number of the first file to process.'
                         'One more file after this will also be processed')
args = parser.parse_args()
starting_file = args.startingFile

SEP = '\t'
class LogProcessor(object):
    """
    A class for processing log files and printing information about them
    """
    def __init__(self, starting_file):
        """
        Constructor
        :param starting_file: The first file to process
        """
        # Always use two threads
        self.num_threads = 2
        # Lock for the client's transactions
        self.lock = threading.Lock()
        # The two filenames to process
        self.files = ['log_file_0' + str(i + starting_file) + '.txt' for i in range(2)]
        # Database connection
        self.db = MySQLdb.connect(user='root', passwd='passwd', db='shaub')

    def insert_data(self, timestamp, url, user):
        """
        Insert a single observation into MariaDB
        :param timestamp: The timestamp of the observation
        :param url: The URL of the observation
        :param user: The uesr of the observation
        """
        # Generate a uid for the record
        uid = hashlib.md5(timestamp + url + user).hexdigest()
        statement = 'insert into logs values ("{}", "{}", "{}", "{}");'.format(uid,timestamp,
                                                                      url, user)
        print statement
        self.lock.acquire()
        try:
            self.db.cursor().execute(statement)
            self.db.commit()
        except _mysql_exceptions.IntegrityError:
            # Duplicate key
            next
        self.lock.release()
        pass

    def process_log(self, filename):
        """
        Process a single logfile
        :param filename: The filename of the log to process
        """
        print 'processing file ' + filename
        with open(filename, 'r') as file_con:
            # Read first line
            line = file_con.readline()
            # Stop reading on EOF
            while line:
                # Split the string into a list with timestamp, URL, and user
                parsed = line.split(SEP)
                # Timestamp is needed here
                timestamp = parsed[0]
                url = parsed[1]
                # User has trailing newline
                user = parsed[2].split('\n')[0]
                self.insert_data(timestamp, url, user)
                # Read next line
                line = file_con.readline()

    def process_all(self):
        """
        Process all the log files. Use one thread for each log file.
        """
        pool = ThreadPool(self.num_threads)
        _ = pool.map(lambda x: self.process_log(filename=x), self.files)

log_processor = LogProcessor(starting_file=starting_file)
log_processor.process_all()
