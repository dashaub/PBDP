"""
A Kafka consumer that consumes from the problem3 topic in test_consumer_group.
"""

from kafka import KafkaConsumer


consumer = KafkaConsumer('problem3', group_id='test_consumer_group')

for msg in consumer:
    print(msg)
