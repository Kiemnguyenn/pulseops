import json
import time
import uuid
import random
import logging
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("KafkaProducer")

# kafka configuration
KAFKA_TOPIC = "txn_raw"
conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "pulseops-realtime-producer",
    "enable.idempotence": "true",
    "acks": "all",
    "retries": 5,
}
producer = Producer(conf)
fake = Faker()

# Mock data pools
USER_IDS = [f"U{random.randint(100000, 999999)}" for _ in range(500)]
MERCHANT_IDS = [f"M{random.randint(10000, 99999)}" for _ in range(100)]
CHANNELS = ["APP", "WEB", "ATM", "POS"]
TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT"]


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")


def generate_transaction_payload(user_id=None, status="COMPLETED"):
    return {
        "transaction_id": str(uuid.uuid4()),
        "transaction_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "sender_account_id": user_id or random.choice(USER_IDS),
        "receiver_account_id": random.choice(MERCHANT_IDS),
        "transaction_type": random.choice(TXN_TYPES),
        "status": status,
        "channel": random.choice(CHANNELS),
        "ip_address": fake.ipv4(),
    }


def start_streaming():
    logger.info(f"Starting real-time streaming to topic '{KAFKA_TOPIC}'...")
    # Bỏ qua check độ dài cho dòng log này bằng thẻ noqa: E501
    logger.info(
        "Idempotence logic is ENABLED. Duplicate prevention is active."
    )  # noqa: E501

    msg_count = 0
    try:
        while True:
            is_fraud_burst = random.random() < 0.2

            if is_fraud_burst:
                target_fraud_user = random.choice(USER_IDS)
                # Tách chuỗi dài thành 2 dòng ngắn để thỏa mãn flake8
                logger.warning(
                    f"[FRAUD PATTERN DETECTED] Generating 5 consecutive "
                    f"FAILED transactions for User: {target_fraud_user}"
                )

                for _ in range(5):
                    txn = generate_transaction_payload(
                        user_id=target_fraud_user, status="FAILED"
                    )
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=json.dumps(txn).encode("utf-8"),
                        callback=delivery_report,
                    )
                    msg_count += 1
            else:
                txn = generate_transaction_payload(status="COMPLETED")
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(txn).encode("utf-8"),
                    callback=delivery_report,
                )
                msg_count += 1

            producer.poll(0)

            if msg_count % 1000 == 0:
                producer.flush()
                logger.info(f"Successfully ingested {msg_count} msgs.")

            time.sleep(0.005)

    except KeyboardInterrupt:
        # Bỏ qua check độ dài
        logger.info(
            "Gracefully shutting down Producer. Flushing pending messages..."
        )  # noqa: E501
        producer.flush()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    start_streaming()
