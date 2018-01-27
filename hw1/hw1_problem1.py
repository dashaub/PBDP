#!/usr/bin/python3
from multiprocessing.dummy import Pool as ThreadPool
from random import randint

def generate_random():
	result = str(randint(0, 10)) + ' ' + str(randint(0, 10)) + ' '
	result += str(randint(0, 10)) + '\n'
	return(result)

def write_file(file_number, num_lines):
	file_name = 'david_shaub' + str(file_number) + '.txt'
	with open(file_name, 'w') as f:
		for line in range(num_lines):
			result = generate_random()
			f.write(result)

numFiles = 3
numLines = 100


worker = [i for i in range(numFiles)]
pool = ThreadPool(4) 
results = pool.map(my_function, )
