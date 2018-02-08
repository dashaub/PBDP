"""
Launch several threads, each performing a CPU-intensive task
"""
import argparse
import multiprocessing
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
    for _ in range(number):
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
        calc_fib(10**7)

for thread_num in range(num_threads):
    job = multiprocessing.Process(target=work_cpu, args=(thread_num, ))
    job.start()
