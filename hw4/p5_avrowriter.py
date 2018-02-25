"""Load the text logfiles and save them in a single avro file"""
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import hashlib

# Read the schema file
schema = avro.schema.parse(open('input_logs_p5.avsc', 'rb').read())

# Define the files to process
# Intentionallly read each file twice so we have duplicates
num_files = 4
input_files = ['logs_{}.txt'.format(i % num_files) for i in range(num_files * 2)]
with DataFileWriter(open('logs_p5.avro', 'wb'), DatumWriter(), schema) as writer:
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
                    # Generate uuid for the line
                    uuid = hashlib.md5(str(current_line)).hexdigest()
                    # Write to avro
                    writer.append({'uuid': uuid, 'timestamp': timestamp, 'url': url, 'user': user})
