from confluent_kafka import Producer
from faker import Faker
import json
import time

# Kafka Producer Configuration
producer_config = {
    "bootstrap.servers": "localhost:29092",  # External listener for Kafka
}

# Create Kafka producer
producer = Producer(producer_config)

# Initialize Faker for generating random data
faker = Faker()

# Function to generate a fake Debezium-style message
def generate_fake_message():
    user_id = faker.random_int(min=1, max=1000)
    
    return {
        "schema": {
            "type": "struct",
            "fields": [],
            "optional": False,
            "name": "source_server.public.fake_table.Envelope"
        },
        "payload": {
            "before": None,  # No old data (new insert)
            "after": {
                "id": user_id,
                "name": faker.name(),
                "email": faker.email(),
                "phone_number": faker.phone_number(),
                "created_at": int(time.time() * 1000000),  # Microtimestamp
                "updated_at": int(time.time() * 1000000)
            },
            "source": {
                "version": "1.9.7.Final",
                "connector": "postgresql",
                "name": "source_server",
                "ts_ms": int(time.time() * 1000),
                "snapshot": "false",
                "db": "source_db",
                "schema": "public",
                "table": "fake_table",
            },
            "op": "c",  # 'c' means INSERT (Debezium format)
            "ts_ms": int(time.time() * 1000),
            "transaction": None
        }
    }

# Function to send messages to Kafka
def produce_messages(num_messages=10, topic="fake_topic"):
    for _ in range(num_messages):
        message = generate_fake_message()
        message_str = json.dumps(message)

        producer.produce(topic, key=str(message["payload"]["after"]["id"]), value=message_str)
        print(f"Sent message: {message_str}")

        time.sleep(1)  # Simulate real-time event streaming

    producer.flush()  # Ensure all messages are sent

# Run producer
if __name__ == "__main__":
    print("Producing fake Kafka messages...")
    produce_messages(num_messages=10)
    print("Finished producing messages.")
