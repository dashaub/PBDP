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


def write_data(thread_num):
    """
    Write junk data to a data_*.dat file
    :param thread_num: A suffix that 
    """
    num_lines = 100*2**20
    source = 'data_' + str(thread_num) + '.dat'
    with open(source, 'w') as file_con:
        for _ in range(num_lines)
            file_con.write('foobar')


def work_io(thread_num):
    """
    Generate IO load. This function will never terminate.
    :param thread_num: A number to identify this process.
    """
    while True:
        time.sleep(1)
        print('Launching worker ' + str(thread_num))
        write_data(thread_num)


workers = [i for i in range(num_threads)]
pool = ThreadPool(num_threads)
_ = pool.map(lambda x: work_io(thread_num=x), workers)
