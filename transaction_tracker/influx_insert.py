import argparse
import glob
from influxdb import InfluxDBClient

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--timestamp', type=str, help = 'Current run timestamp',
                    required = True)
args = parser.parse_args()
timestamp = args.timestamp


client = InfluxDBClient(database='blockchain')
#timestamp = '2018-04-23T193341Z'
def insert_influx(timestamp, count):
     """
     Insert the count into InfluxDB
     :param filename: The file to process
     """
     json_body = [{"measurement": "unconfirmed_transactions",
                   "time": timestamp,
                   "fields": {"count": count}
                   }]
     client.write_points(json_body)


def count_lines(filename):
     """
     Count the number of transactions in a file
     :param filename: The file to process
     """
     return sum(1 for line in open(filename))

# Safety if multiple output files
filenames = glob.glob('beam/{}*'.format(timestamp))

count = 0
# Insert total number of unconfirmed transactions into InfluxDB
for filename in filenames:
     count += count_lines(filename)
insert_influx(timestamp, count)
