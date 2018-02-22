"""Load the text logfiles and save them in a single avro file"""
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

# Read the schema file
schema = avro.schema.parse(open('input_logs.avsc', 'rb').read())

# Define the files to process
input_files = ['logs_{}.txt'.format(i) for i in range(4)]
with DataFileWriter(open('logs.avro', 'wb'), DatumWriter(), schema) as writer:
    # Process each input file
    for input_file in input_files:
        # Open each input file
        with open(input_file, 'r') as current_file:
            # Process each line in each input file
            for line in current_file:
                current_line = line.strip().split('\t')
                # Only parse and write if there is correct input
                if len(current_line) == 3:
                    timestamp, url, user = current_line
                    # Write to avro
                    writer.append({'timestamp': timestamp, 'url': url, 'user': user})
