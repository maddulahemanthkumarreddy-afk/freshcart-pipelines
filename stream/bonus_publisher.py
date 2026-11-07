import json
import os
import time
from dotenv import load_dotenv
import pika

load_dotenv()

AMQP_URL = (
    os.getenv("CLOUDAMQP_URL")
    or os.getenv("RABBITMQ_URL")
    or os.getenv("AMQP_URL")
)

if not AMQP_URL:
    raise RuntimeError(
        "RabbitMQ URL not found. Check the variable name in .env"
    )

EXCHANGE = "rapido-dispatch"
ROUTING_KEY = "freshcart.orders.created"

orders = [
    {
        "order_id": f"ORD-STREAM-{i:04d}",
        "cust_email": "meera@outlook.com",
        "cust_name": "Meera S",
        "order_date": "2026-09-03",
        "amount": "500.00",
        "currency": "INR",
        "pincode": "400050",
        "updated_at": f"2026-09-03 18:{i:02d}:00"
    }
    for i in range(1, 7)
]

connection = pika.BlockingConnection(
    pika.URLParameters(AMQP_URL)
)

channel = connection.channel()

channel.exchange_declare(
    exchange=EXCHANGE,
    exchange_type="topic",
    durable=True
)

try:
    for i, order in enumerate(orders, start=1):
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(order).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=order["order_id"]
            )
        )

        print(f"Published {i}/6: {order['order_id']}")

        if i < 6:
            time.sleep(10)

finally:
    connection.close()

print("Publisher finished successfully.")
