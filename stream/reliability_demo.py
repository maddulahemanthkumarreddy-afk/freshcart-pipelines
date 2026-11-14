
import os
import json
import pika
from dotenv import load_dotenv

# Load RabbitMQ connection URL from project-root .env
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")

if not RABBITMQ_URL:
    raise ValueError("RABBITMQ_URL not found in .env file")

EXCHANGE = "rapido-dispatch"

CRASH_QUEUE = "partd.crash.demo"
POISON_QUEUE = "partd.poison.demo"


def get_connection():
    parameters = pika.URLParameters(RABBITMQ_URL)
    return pika.BlockingConnection(parameters)


# -------------------------------------------------
# DEMO 1: Consumer crashes before acknowledging
# -------------------------------------------------

def demonstrate_crash_before_ack():

    print("\n========== CRASH BEFORE ACK DEMO ==========")

    # First connection: publish a test message.
    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(
        queue=CRASH_QUEUE,
        durable=True
    )

    channel.queue_purge(queue=CRASH_QUEUE)

    message = {
        "id": "CRASH-TEST-001",
        "city": "bangalore",
        "zone": "koramangala",
        "type": "food"
    }

    channel.basic_publish(
        exchange="",
        routing_key=CRASH_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    print("Test message published.")
    connection.close()

    # Second connection: receive message but DO NOT ACK.
    connection = get_connection()
    channel = connection.channel()

    method, properties, body = channel.basic_get(
        queue=CRASH_QUEUE,
        auto_ack=False
    )

    if method:
        print("Consumer 1 received:", body.decode())
        print("Consumer 1 crashes before ACK.")
        print("Closing connection without acknowledging...")

    # Closing the connection requeues the unacknowledged message.
    connection.close()

    # Third connection: another consumer receives it again.
    connection = get_connection()
    channel = connection.channel()

    method, properties, body = channel.basic_get(
        queue=CRASH_QUEUE,
        auto_ack=False
    )

    if method:
        print("\nConsumer 2 received:", body.decode())
        print("Redelivered:", method.redelivered)

        channel.basic_ack(
            delivery_tag=method.delivery_tag
        )

        print("Message acknowledged successfully.")

    connection.close()


# -------------------------------------------------
# DEMO 2: Poison message and requeue=True
# -------------------------------------------------

def demonstrate_poison_message():

    print("\n========== POISON MESSAGE DEMO ==========")

    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(
        queue=POISON_QUEUE,
        durable=True
    )

    channel.queue_purge(queue=POISON_QUEUE)

    # Publish a malformed message without the zone field.
    malformed_message = {
        "id": "POISON-001",
        "city": "bangalore",
        "type": "food"
    }

    channel.basic_publish(
        exchange="",
        routing_key=POISON_QUEUE,
        body=json.dumps(malformed_message),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    print("Malformed message published.")
    print("Missing required field: zone")

    # Demonstrate repeated requeue, but stop after 3 deliveries.
    for attempt in range(1, 4):

        method, properties, body = channel.basic_get(
            queue=POISON_QUEUE,
            auto_ack=False
        )

        if method is None:
            print("No message available.")
            break

        print(f"\nDelivery attempt: {attempt}")
        print("Message:", body.decode())
        print("Redelivered:", method.redelivered)

        message = json.loads(body)

        if "zone" not in message:

            print("BAD MESSAGE: zone field missing.")

            if attempt < 3:
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True
                )

                print("NACK sent with requeue=True.")
                print("Message returned to the queue.")

            else:
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )

                print("NACK sent with requeue=False.")
                print("Poison message removed from the queue.")

    connection.close()


# -------------------------------------------------
# MAIN
# -------------------------------------------------

if __name__ == "__main__":

    demonstrate_crash_before_ack()
    demonstrate_poison_message()

    print("\nAll reliability demonstrations completed.")
