#!/usr/bin/python3
"""
Generates files with random numbers
"""
import argparse
from multiprocessing.dummy import Pool as ThreadPool
from random import randint

parser = argparse.ArgumentParser()
parser.add_argument('--num_files', type=int,
                    help='Number of files to write')
parser.add_argument('--num_lines', type=int,
                    help='Number of lines to write in each file')
args = parser.parse_args()
num_files = args.num_files
num_lines = args.num_lines

def generate_random():
    """
    Generate a single string containing three random numbers between
    0 and 10 (inclusive) and separated by a space.
    """
    result = str(randint(0, 10)) + ' ' + str(randint(0, 10)) + ' '
    result += str(randint(0, 10)) + '\n'
    return result


def write_file(file_number, num_lines):
    """
    Write a file with random numbers in the specified format
    a given number of lines
    :param file_number: The number of the file--will appear in filename
    :param num_lines: The number of lines to generate in the file
    """
    file_name = 'david_shaub' + str(file_number) + '.txt'
    with open(file_name, 'w') as outfile:
        for _ in range(num_lines):
            result = generate_random()
            outfile.write(result)


workers = [i for i in range(num_files)]
pool = ThreadPool(num_files)
_ = pool.map(lambda x: write_file(file_number=x, num_lines=num_lines),
             workers)
