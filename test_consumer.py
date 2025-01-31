from confluent_kafka import Consumer, KafkaError

# Configuration
conf = {
    'bootstrap.servers': 'localhost:29092',  # Adjust if necessary
    'group.id': 'consumer-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'PLAINTEXT',  # Ensure this is PLAINTEXT if you're not using SSL
    'api.version.request': False
}

# Create consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['source_db.public.roles'])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

        if msg is None:
            continue  # No message available, continue polling
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.partition(), msg.offset()))
            else:
                print(msg.error())
        else:
            print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer interrupted")
finally:
    consumer.close()
