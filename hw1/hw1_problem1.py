#!/usr/bin/python3
from multiprocessing.dummy import Pool as ThreadPool

def format_line(arr):
	result = ''
	for num in arr:
		result +=

def write_file(file_number, num_lines):
	file_name = 'david_shaub' + str(file_number) + '.txt'
	with open(file_name, 'w') as f:
		for line in range(num_lines):
			arr = (np.random.rand(3)*11).astype(int)
			result = str(arr)[1:-1] + '\n'
			f.write(result)

numFiles = 3
numLines = 100


worker = [i for i in range(numFiles)]
pool = ThreadPool(4) 
results = pool.map(my_function, )
