import time
import random
from confluent_kafka import Producer

# Kafka broker settings
bootstrap_servers = 'localhost:9094'
topic = 'clothes_purchases'

# List of product types
product_types = ['shirt', 'pants', 'skirt', 'dress', 'jacket']

# List of card types
card_types = ['Visa', 'MasterCard', 'American Express', 'Discover']

# Create a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers
}

# Create a Kafka producer instance
producer = Producer(producer_config)

# Send events simulating clothes purchases
purchase_number = 1

while True:
    # Generate random purchase details
    random_type = random.choice(product_types)
    random_card_type = random.choice(card_types)
    random_price = random.randint(10, 300)

    # Create the message to send
    message = f'Purchase #{purchase_number}: {random_type}, paid {random_price} with {random_card_type}'

    # Send the message to Kafka
    producer.produce(topic, value=message)

    # Flush the producer buffer to ensure messages are sent immediately
    producer.flush()

    # Increment the purchase number
    purchase_number += 1

    # Sleep for 1 second before sending the next message
    time.sleep(1)

    # print the order as is
    print(f'Purchase #{purchase_number}: {random_type}, paid {random_price} with {random_card_type}')
