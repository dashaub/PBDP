"""
A Kafka producer that generates events at a specified rate with a timestamp, user, and URL
"""
import argparse
import datetime
import hashlib
import random
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--rate', type=float, default=1,
                    help='Number of events per second to generate')
parser.add_argument('--seed', type=int, default=12345,
                    help='See to use for reproducibility')
args = parser.parse_args()
sleep_time = 1 / args.rate
seed = args.seed

users = ['foo', 'bar', 'baz']
urls = ['https://en.wikipedia.org/wiki/Apache_Flume',
        'https://en.wikipedia.org/wiki/Main_Page',
        'https://en.wikipedia.org/wiki/Apache_Kafka']

producer = KafkaProducer(bootstrap_servers='localhost:9092')
random.seed(seed)
while True:
    # Select a random user and URL
    user = random.choice(users)
    url = random.choice(urls)
    # Get the current time in UTC
    current_time = str(datetime.datetime.utcnow())
    # Generate a UUID for the event
    identifier = str([current_time, url, user]).encode('utf-8')
    uuid = hashlib.md5(identifier).hexdigest()
    # Build final message and send to the Kafka cluster
    message = '{}\t{}\t{}\t{}'.format(uuid, current_time, url, user)
    producer.add(message)
    time.sleep(sleep_time)
