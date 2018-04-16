"""
Fetch blocks at the desired height
"""
from blockchain import blockexplorer
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--block_height', help='Get the block at the specified height',
                    type=int, required=True)
args = parser.parse_args()

# Get the block at the specified height
block = blockexplorer.get_block_height(args.block_height)[0]
transactions = [transaction.hash for transaction in block.transactions]


# Write output to file
output_file = uuid.uuid4().hex
with open(output_file + '.txt', 'w') as output:
    for line in transactions:
        output.write(line + '\n')
