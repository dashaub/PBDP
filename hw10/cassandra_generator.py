"""
Generate random events and insert them into a Cassandra table
"""

import hashlib
import random

from cassandra.cluster import Cluster

# Initialize the connection and session with Cassandra on localhost
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('hw10')

# Possible URLs, dates, and countries to sample from.
# Include some duplicates so these are included more frequently for a non-uniform distribution
urls = ['https://en.wikipedia.org/wiki/Apache_Cassandra',
        'https://en.wikipedia.org/wiki/Apache_Cassandra',
        'https://en.wikipedia.org/wiki/Richard_Stallman',
        'https://stallman.org/biographies.html#serious',
        'https://www.gnu.org/software/software.html',
        'https://www.gnu.org/gnu/gnu.html']
# Generate seconds and minutes uniformly in [0, 60)
seconds = ['0' + str(i) for i in range(10)] + [str(i) for i in range(10, 60)]
minutes = ['0' + str(i) for i in range(10)] + [str(i) for i in range(10, 60)]
# Sample the hours [10, 24) twice as frequently
hours = ['0' + str(i) for i in range(10)] + [str(i) for i in range(10, 24) for _ in range(2)]
# Generate more events on certain days
days = ['01', '01', '02', '03', '03', '03', '03', '04', '05']
months = ['01']
years = ['2018']
# Generate more events in certain countries
countries = ['us', 'us', 'us', 'us', 'ru', 'de', 'jp', 'ca', 'in', 'uk', 'uk']
ttfbs = [i for i in range(100)]

def generate_event():
    """
    Generate a single random event to insert into Cassandra
    """
    url = random.choice(urls)
    ua_country = random.choice(countries)
    ttfb = random.choice(ttfbs)

    # Build the timestamp
    year = random.choice(years)
    month = random.choice(months)
    day = random.choice(days)
    hour = random.choice(hours)
    minute = random.choice(minutes)
    second = random.choice(seconds)
    record_time = '{}-{}-{}T{}:{}:{}'.format(year, month, day, hour, minute, second)

    # Combine the fieds and generate a UUID hash
    line = record_time + hour + url + ua_country + str(ttfb)
    uuid = hashlib.md5(line).hexdigest()
    return([uuid, record_time, int(hour), url, ua_country, ttfb])


def insert_cassandra(event):
    """
    Insert an event into the hw10.hw10_p2 table
    :param event: A single event ot insert
    """
    session.execute(
    """
    INSERT INTO hw10_p2 (uuid, record_time, hour, url, ua_country, ttfb)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, event)


# Insert 1000 events into Cassandra
for count in range(1, 1001):
    event = generate_event()
    insert_cassandra(event)
    if count % 100 == 0:
        print('Inserted {} total events'.format(count))
