"""
Launch several threads, each performing an IO-intensive task
"""
import argparse
from multiprocessing.dummy import Pool as ThreadPool
import time

parser = argparse.ArgumentParser()
parser.add_argument('--numThreads', type=int,
                    help='Number of threads to launch')
args = parser.parse_args()
num_threads = args.numThreads


def load_data():
    """
    A naive implementation to generate the specified Fibonacci number
    """
    source = 'data.dat'
    with open(source, 'rb') as f:
        _ = f.read()


def work_io(thread_num):
    """
    Generate IO load. This function will never terminate.
    :param thread_num: A number to identify this process.
    """
    while True:
        time.sleep(1)
        print('Launching worker ' + str(thread_num))
        _ = load_data()


workers = [i for i in range(num_threads)]
pool = ThreadPool(num_threads)
_ = pool.map(lambda x: work_io(thread_num=x), workers)
