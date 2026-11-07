"""Consume FreshCart-style RabbitMQ order events and land them as NDJSON.GZ.

Run from the repository root:
    python stream/bonus_consumer.py --duration 60

Required .env setting: RABBITMQ_URL (or CLOUDAMQP_URL / AMQP_URL).
Optional Snowflake loading is enabled with --load-bronze and these .env keys:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
Optional: SNOWFLAKE_ROLE (default ACCOUNTADMIN).

The script ACKs messages only after the complete batch is safely written to the
local landing file. If landing fails, it NACKs/requeues the collected messages.
It can optionally PUT/COPY that file into bronze.orders_raw. Silver/Gold SQL
transformations are run using the existing FreshCart notebook SQL cells.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pika
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
LANDING_DIR = ROOT / "freshcart_landing"
EXCHANGE = "rapido-dispatch"
QUEUE = "freshcart-lakehouse-bonus"
BINDING_KEY = "freshcart.orders.*"


def get_broker_url() -> str:
    load_dotenv(ROOT / ".env")
    url = os.getenv("RABBITMQ_URL") or os.getenv("CLOUDAMQP_URL") or os.getenv("AMQP_URL")
    if not url:
        raise RuntimeError("Set RABBITMQ_URL in the repository-root .env file.")
    return url


def write_landing(records: list[dict[str, Any]], run_id: str) -> Path:
    """Write one gzip-compressed JSON Lines file using the FreshCart envelope."""
    folder = LANDING_DIR / "web"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"web__{run_id}.json.gz"
    extracted_at = datetime.now(timezone.utc).isoformat()
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for record in records:
            envelope = {
                "source": "web",
                "run_id": run_id,
                "extracted_at": extracted_at,
                "data": record,
            }
            handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
    return path


def consume_and_land(duration: int, max_messages: int | None) -> tuple[Path, str, int]:
    connection = pika.BlockingConnection(pika.URLParameters(get_broker_url()))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=100)
    records: list[dict] = []
    tags: list[int] = []
    deadline = time.monotonic() + duration
    print(f"Consuming from {QUEUE} for up to {duration} seconds...")
    try:
        while time.monotonic() < deadline and (max_messages is None or len(records) < max_messages):
            connection.process_data_events(time_limit=min(1.0, max(0.0, deadline - time.monotonic())))
            method, _, body = channel.basic_get(queue=QUEUE, auto_ack=False)
            if method is None:
                continue
            try:
                record = json.loads(body.decode("utf-8"))
                required = {"order_id", "cust_email", "cust_name", "order_date", "amount", "currency", "pincode", "updated_at"}
                missing = required.difference(record)
                if missing:
                    raise ValueError(f"Missing fields: {', '.join(sorted(missing))}")
                records.append(record)
                tags.append(method.delivery_tag)
                print(f"Received {record['order_id']}")
            except Exception as exc:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"NACK malformed message (discard): {exc}")

        if not records:
            raise RuntimeError("No valid order messages received; no landing file was written.")

        run_id = "BONUS_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        landing_file = write_landing(records, run_id)
        # ACK only after durable local landing write succeeds.
        for tag in tags:
            channel.basic_ack(delivery_tag=tag)
        print(f"Landed {len(records)} records: {landing_file}")
        print(f"ACKed {len(tags)} messages. Run ID: {run_id}")
        return landing_file, run_id, len(records)
    except Exception:
        # Closing an open connection returns unacked messages to the queue.
        raise
    finally:
        connection.close()


def load_to_bronze(local_path: Path, run_id: str) -> None:
    """PUT the landing file and COPY it to bronze.orders_raw in Snowflake."""
    load_dotenv(ROOT / ".env")
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing Snowflake .env values: " + ", ".join(missing))

    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )
    stage_dir = f"orders_web/{run_id}"
    try:
        with conn.cursor() as cur:
            file_uri = "file://" + local_path.resolve().as_posix()
            cur.execute(f"PUT '{file_uri}' @bronze.landing/{stage_dir}/ AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
            cur.execute(f"""
                COPY INTO bronze.orders_raw (source, raw, run_id, extracted_at, src_file, src_row)
                FROM (
                  SELECT $1:source::STRING, $1:data, $1:run_id::STRING,
                         $1:extracted_at::TIMESTAMP_TZ, METADATA$FILENAME,
                         METADATA$FILE_ROW_NUMBER
                  FROM @bronze.landing/{stage_dir}/
                )
                FILE_FORMAT = (FORMAT_NAME = 'bronze.ff_ndjson')
                ON_ERROR = 'ABORT_STATEMENT'
            """)
            print(f"Snowflake Bronze COPY completed for run {run_id}.")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=60, help="collection window in seconds (default: 60)")
    parser.add_argument("--max-messages", type=int, default=None, help="stop after this many valid messages")
    parser.add_argument("--load-bronze", action="store_true", help="also PUT/COPY the landing file into Snowflake Bronze")
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error("--duration must be greater than zero")

    landing_file, run_id, count = consume_and_land(args.duration, args.max_messages)
    print(f"Summary: {count} orders landed; file exists={landing_file.is_file()}")
    if args.load_bronze:
        load_to_bronze(landing_file, run_id)
        print("Next: run SQL_10_ORDERS_CLEAN, SQL_20_ORDERS_DEDUP, SQL_30/31/32 enrichment, and SQL_40_GOLD from freshcart_elt.ipynb.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted; unacknowledged deliveries will be requeued when the connection closes.", file=sys.stderr)
        raise SystemExit(130)
