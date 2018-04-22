"""
Fetch the current mempool
"""

from blockchain import blockexplorer
import uuid
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--block_height', help='Get the block at the specified height',
                    type=int, required=True)
args = parser.parse_args()

unconfirmed = blockexplorer.get_unconfirmed_tx()
transactions = ['{}'.format(tx.hash) for tx in unconfirmed]

# Write output to file
output_file = 'unconfirmed/' + uuid.uuid4().hex + '.txt'
with open(output_file, 'w') as output:
    for line in transactions:
        output.write(line + '\n')
