import random
from confluent_kafka import Consumer, KafkaError

# Kafka broker settings
bootstrap_servers = 'localhost:9094'
topic = 'clothes_purchases'

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'my_warehouse',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([topic])

# Read messages from the Kafka topic and simulate sending packing details to a warehouse API
while True:
    message = consumer.poll(1.0)

    if message is None:
        continue
    if message.error():
        print("Error")
    else:
        # Simulate sending packing details to a warehouse API
        purchase_details = message.value().decode('utf-8')
        purchase_number = int(purchase_details.split('#')[1].split(':')[0])
        product_type = purchase_details.split(':')[1].split(',')[0].strip()
        quantity = random.randint(1, 5)
        packing_details = f"Packing details for Purchase #{purchase_number}: {quantity} x {product_type}"

        # Print the packing details to the screen
        print(packing_details)
