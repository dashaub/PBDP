"""
Follow the chain, download blocks, and extract transactions
"""
from blockchain import blockexplorer

import time
import uuid


def read_api_key():
    """
    Read the api key from the file. An API key is not absolutely required for the application
    to run, but it can help prevent request throttling and support higher query frequency.
    """
    try:
        with open('api.key') as api:
            return api.readline().strip()
    except:
        return None

api_key = read_api_key()
processed_heights = set()
best_height = blockexplorer.get_latest_block(api_code=api_key).height - 1


def process_block(block_height):
    """
    Fetch a block and write the results
    :param block_height: The chain height
    """
    print 'Processing new block at height {}'.format(block_height)
    transactions = fetch_block(block_height)
    write_block(transactions, block_height)
    processed_heights.add(block_height)

def fetch_block(block_height):
    """
    Fetches a block at a specified chain height and returns a list of its transactions
    :param block_height: The chain height
    """
    block = blockexplorer.get_block_height(height=block_height, api_code=api_key)[0]
    transactions = ['{} {}'.format(tx.hash, block.height) for tx in block.transactions]
    return transactions

def write_block(transactions, height):
    """
    Write a list of transactions to file
    :param transactions: A list of transaction hashes
    :param height: The block height
    """
    output_file = 'blocks/{}_{}.txt'.format(height, uuid.uuid4().hex)
    with open(output_file, 'w') as output:
        for line in transactions:
            output.write(line + '\n')

print 'Using api key: ' + api_key
while True:
    # Determine current best block
    current_height = blockexplorer.get_latest_block(api_code=api_key).height
    # Only process a block if it is better than current best block
    if current_height > best_height:
        process_block(current_height)
        # Process any other blocks we may have missed
        # more than 10 missed blocks in 1 minute occurs with probability 2.285845e-19
        for candidate_height in range(current_height - 10, current_height):
            if candidate_height not in processed_heights:
                process_block(candidate_height)
        best_height = current_height


    # Wait 5 minutes before checking for a new block
    time.sleep(60 * 5)
