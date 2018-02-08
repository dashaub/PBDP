"""
Launch several threads, each performing an IO-intensive task
"""
import argparse
import multiprocessing
import random
import time

parser = argparse.ArgumentParser()
parser.add_argument('--numThreads', type=int,
                    help='Number of threads to launch')
args = parser.parse_args()
num_threads = args.numThreads


def write_data(thread_num):
    """
    Write junk data to a data_*.dat file
    :param thread_num: A suffix that is added to each file.
    """
    num = 1000000
    source = 'data_' + str(thread_num) + '.dat'
    with open(source, 'w') as file_con:
        for _ in range(num):
            file_con.write(str(random.random()))


def work_io(thread_num):
    """
    Generate IO load. This function will never terminate.
    :param thread_num: A number to identify this process.
    """
    while True:
        time.sleep(1)
        print('Launching worker ' + str(thread_num))
        write_data(thread_num)

for thread_num in range(num_threads):
    job = multiprocessing.Process(target=work_io, args=(thread_num, ))
    job.start()
