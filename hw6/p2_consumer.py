"""
A Kafka consumer that consumes from the problem2 topic.
"""

from kafka import KafkaConsumer


consumer = KafkaConsumer('problem2')

for msg in consumer:
    print(msg)
