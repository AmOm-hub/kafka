import threading
import time
import random
from confluent_kafka import Consumer, KafkaError, Producer

# Kafka broker settings
bootstrap_servers = 'localhost:9094'
topic = 'clothes_purchases'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_store_gw',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Create a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers
}

# Create a Kafka producer instance
producer = Producer(producer_config)

# Subscribe to the Kafka input topic
consumer.subscribe([topic])


# Define a function for the threads to execute
def process_messages():
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            print("Error")
        else:
            # Simulate charging a card
            purchase_details = message.value().decode('utf-8')
            purchase_price = int(purchase_details.split('paid ')[1].split(' ')[0])
            card_type = random.choice(['Visa', 'MasterCard', 'American Express', 'Discover'])
            card_number = random.randint(1000000000000000, 9999999999999999)
            transaction_id = random.randint(1000000000000000, 9999999999999999)

            print(f"Charging {card_type} card number {card_number} for ${purchase_price}...")

            # Create the payment transaction message to send to the Kafka output topic
            transaction_details = f'Transaction #{transaction_id}: charged {card_type} card {card_number} for ${purchase_price}'

            # Send the payment transaction message to the Kafka output topic
            producer.produce('payment_transactions', value=transaction_details.encode('utf-8'))

            # Flush the producer buffer to ensure messages are sent immediately
            producer.flush()

            # Sleep for 1 second before processing the next message
            time.sleep(1)


# Create 2 threads and start them
for i in range(2):
    thread = threading.Thread(target=process_messages)
    thread.start()