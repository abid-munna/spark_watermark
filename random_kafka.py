import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["172.18.0.4:9092"],
    value_serializer=lambda message: json.dumps(message).encode('utf-8')
)



while True:
    # Generate random values
    messages = {
        "id": int(time.time()),  # Convert timestamp to integer for Kafka
        "eventTimestamp": datetime.now().isoformat(),  # Use ISO format for JSON serialization
        "temperature": float("{0:.5f}".format(random.uniform(253.0, 373.0))),
        "pressure": float("{0:.5f}".format(random.uniform(200.0, 374.3))),
    }


    # Print and send the message
    print(f"Producing message {datetime.now()} Message :\n {str(messages)}")
    producer.send("rosmsgs", messages)

    # Increment the message id

    # Wait for 2 seconds before sending the next message
    time.sleep(20)