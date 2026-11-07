
import os
import sys
import json

import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
EXCHANGE = "rapido-dispatch"

if not RABBITMQ_URL:
    raise ValueError("RABBITMQ_URL not found in .env file")

if len(sys.argv) != 3:
    print("Usage: python stream/driver.py NAME PATTERN")
    sys.exit(1)

driver_name = sys.argv[1]
binding_pattern = sys.argv[2]

params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

# Create a private queue for this driver
result = channel.queue_declare(queue="", exclusive=True)
queue_name = result.method.queue

channel.queue_bind(
    exchange=EXCHANGE,
    queue=queue_name,
    routing_key=binding_pattern
)

print(f"{driver_name} is listening...")
print(f"Binding pattern: {binding_pattern}")
print(f"Private queue: {queue_name}")


# def callback(ch, method, properties, body):
#     message = json.loads(body)

#     print("\nRequest received!")
#     print(f"Driver: {driver_name}")
#     print(f"Routing key: {method.routing_key}")
#     print(f"Message: {message}")

#     # Acknowledge successfully processed message
#     ch.basic_ack(delivery_tag=method.delivery_tag)

def callback(ch, method, properties, body):
    message = json.loads(body)

    print("\nRequest received!")
    print(f"Driver: {driver_name}")
    print(f"Routing key: {method.routing_key}")
    print(f"Message: {message}")

    if "zone" not in message:
        print("BAD MESSAGE: zone field missing")
        print("Sending NACK - message will be discarded")

        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )
        return

    print("GOOD MESSAGE: sending ACK")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=False
)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print(f"\n{driver_name} stopped.")
    channel.stop_consuming()
finally:
    connection.close()
