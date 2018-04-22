"""
Fetch blocks at the desired height
"""
from blockchain import blockexplorer
import uuid
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--block_height', help='Get the block at the specified height',
                    type=int, required=True)
args = parser.parse_args()

# Get the block at the specified height
block = blockexplorer.get_block_height(args.block_height)[0]
# Form tuples with the block height and transaction hash
transactions = ['{} {}'.format(block.height, tx.hash) for tx in block.transactions]


def write_block(transactions):
    """
    Write a list of transactions to file
    """
    output_file = 'blocks/' + uuid.uuid4().hex + '.txt'
    with open(output_file, 'w') as output:
        for line in transactions:
            output.write(line + '\n')
