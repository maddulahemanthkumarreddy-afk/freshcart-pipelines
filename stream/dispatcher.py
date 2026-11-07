
import os
import json
import time
import random
import uuid
from datetime import datetime, timezone

import pika
from dotenv import load_dotenv

# Load RabbitMQ URL from the project root .env file
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
EXCHANGE = "rapido-dispatch"

if not RABBITMQ_URL:
    raise ValueError("RABBITMQ_URL not found in .env file")

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare topic exchange
channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

cities = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "velachery", "adyar"]
}

request_types = ["ride", "food"]

message_count = 0

print("Dispatcher started. Sending requests every 2 seconds...")

try:
    while True:
        city = random.choice(list(cities.keys()))
        zone = random.choice(cities[city])
        request_type = random.choice(request_types)

        routing_key = f"{city}.{zone}.{request_type}"

        message = {
            "id": str(uuid.uuid4()),
            "city": city,
            "zone": zone,
            "type": request_type,
            "ts": datetime.now(timezone.utc).isoformat()
        }

        message_count += 1

        # Send one bad message every 10 messages
        if message_count % 10 == 0:
           message.pop("zone")
           print("Creating BAD message: zone field removed")
         
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2
            )
        )

        print(f"Published: {routing_key} | {message['id']}")

        time.sleep(2)

except KeyboardInterrupt:
    print("\nDispatcher stopped.")

finally:
    connection.close()
