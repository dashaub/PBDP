"""
Fetch the current mempool
"""
SLEEP_TIME=2

from blockchain import blockexplorer
import time
import uuid
import argparse

def read_api_key():
    """
    Read the api key from the file
    """
    try:
        with open('api.key') as api:
            return api.readline().strip()
    except:
        return None

api_key = read_api_key()
parser = argparse.ArgumentParser()
parser.add_argument('--block_height', help='Get the block at the specified height',
                    type=int, required=False)
args = parser.parse_args()


def write_transactions(transactions):
    """
    Write transactions to file
    """
    output_file = 'unconfirmed/{}.txt'.format(uuid.uuid4().hex)
    with open(output_file, 'w') as output:
        for line in transactions:
            output.write(line + '\n')

def get_mempool():
    """
    Return a list of transaction hashes currently in the mempool
    """
    # Protect against API timeout with very crude try-catch
    try:
        unconfirmed = blockexplorer.get_unconfirmed_tx()
        transactions = ['{} unconfirmed'.format(tx.hash) for tx in unconfirmed]
    except:
        return None
    return transactions

print 'Using api key: ' + api_key
while True:
    transactions = set()
    # Collect batches together before writing results
    for _ in range(60 / SLEEP_TIME):
        current_transactions = get_mempool()
        # Prevent duplicates within one batch
        for transaction in current_transactions:
            transactions.add(transaction)
        time.sleep(SLEEP_TIME)
    transactions = list(transactions)
    print 'Writing {} transactions'.format(len(transactions))
    write_transactions(transactions)
