"""
Launch several threads, each performing a CPU-intensive task
"""
import argparse
from multiprocessing.dummy import Pool as ThreadPool
import time

parser = argparse.ArgumentParser()
parser.add_argument('--numThreads', type=int,
                    help='Number of threads to launch')
args = parser.parse_args()
num_threads = args.numThreads


def calc_fib(number):
    """
    A naive implementation to generate the specified Fibonacci number
    :param number: The Fibonacci number to generate
    """
    current_num = 0
    next_num = 1
    for num in range(number):
        tmp = current_num
        current_num = next_num
        next_num += tmp
    return current_num


def work_cpu(thread_num):
    """
    Generate a CPU load. This function will never terminate.
    :param thread_num: A number to identify this process.
    """
    while True:
        time.sleep(1)
        print('Launching worker ' + str(thread_num))
        _ = calc_fib(10**6)


workers = [i for i in range(num_files)]
pool = ThreadPool(num_files)
_ = pool.map(lambda x: work_cpu(thread_num=x), workers)
