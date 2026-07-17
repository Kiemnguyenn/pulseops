import argparse
import json
import logging
import os

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("BronzeWriter")

KAFKA_TOPIC = "txn_raw"
KAFKA_CONF = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": "bronze-writer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

DB_CONF = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "dbname": "pulseops",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

INSERT_SQL = """
    INSERT INTO bronze_transactions (
        transaction_id, transaction_time, amount,
        sender_account_id, receiver_account_id,
        transaction_type, status, channel
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING;
"""


def parse_message(raw_value: bytes) -> dict:
    return json.loads(raw_value.decode("utf-8"))


def insert_transaction(cursor, txn: dict):
    cursor.execute(
        INSERT_SQL,
        (
            txn["transaction_id"],
            txn["transaction_time"],
            txn["amount"],
            txn["sender_account_id"],
            txn["receiver_account_id"],
            txn["transaction_type"],
            txn["status"],
            txn["channel"],
        ),
    )


def start_consuming(once: bool = False, max_idle_polls: int = 5):
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([KAFKA_TOPIC])
    conn = psycopg2.connect(**DB_CONF)
    conn.autocommit = False
    cursor = conn.cursor()

    logger.info(f"Starting Bronze writer, consuming topic '{KAFKA_TOPIC}'...")
    processed_count = 0
    idle_polls = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle_polls += 1
                if once and idle_polls >= max_idle_polls:
                    logger.info(
                        f"No new messages after {max_idle_polls} polls, "
                        "stopping (--once mode)."
                    )
                    break
                continue
            idle_polls = 0

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                txn = parse_message(msg.value())
                insert_transaction(cursor, txn)
                conn.commit()
                consumer.commit(msg)
                processed_count += 1

                if processed_count % 100 == 0:
                    logger.info(f"Successfully processed {processed_count} records.")

            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info("Gracefully shutting down Bronze writer...")
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        logger.info(f"Shutdown complete. Total processed this run: {processed_count}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PulseOps Bronze writer")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one bounded batch: exit after no new messages for a few seconds.",
    )
    args = parser.parse_args()
    start_consuming(once=args.once)
