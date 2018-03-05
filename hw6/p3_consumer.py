"""
A Kafka consumer that consumes from the problem3 topic.
"""

from kafka import KafkaConsumer

def print_distribution(data):
    """
    Print a summary of the frequency of each partition encounter. This will show the balance of
    load across the partitions
    :param data: A list containing the partition numbers
    """
    unique_partitions = list(set(data))
    num_elements = len(data)
    for partition in unique_partitions:
        partition_count = data.count(partition)
        print('Partition {}: {}'.format(partition, partition_count))


consumer = KafkaConsumer('problem3')
count = 0
# Keep track of which partitions 
partitions = []
for msg in consumer:
    print(msg)
    count += 1
    partitions.append(msg.partition)
    # Print a distribution of the partitions every 1000 events
    if not count % 1000:
        print_distribution(partitions)
