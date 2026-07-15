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
    "bootstrap.servers": "localhost:9092",
    "group.id": "bronze-writer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

DB_CONF = {
    "host": "localhost",
    "port": "5433",
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


def start_consuming():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([KAFKA_TOPIC])
    conn = psycopg2.connect(**DB_CONF)
    conn.autocommit = False
    cursor = conn.cursor()

    logger.info(f"Starting Bronze writer, consuming topic '{KAFKA_TOPIC}'...")
    processed_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                # txn = json.loads(msg.value().decode("utf-8"))
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
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    start_consuming()
