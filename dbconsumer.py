import threading
import time
from confluent_kafka import Consumer, KafkaError

# Kafka broker settings
bootstrap_servers = 'localhost:9094'
topic = 'clothes_purchases'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_db',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([topic])


# Define a function for the threads to execute
def consume_messages():
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError.PARTITION_EOF:
                print('Reached end of partition, exiting consumer loop')
            else:
                print(f"Error reading message: {message.error()}")
        else:
            # Simulate writing to a database
            print(f"Writing to database: {message.value().decode('utf-8')}")

            # Sleep for 3 seconds between events
            time.sleep(3)


# Create 4 threads and start them
for i in range(4):
    thread = threading.Thread(target=consume_messages)
    thread.start()