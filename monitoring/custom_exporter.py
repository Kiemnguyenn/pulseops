import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import Consumer, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CustomExporter")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "txn_raw"
PORT = 9100
DBT_RUN_RESULTS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "dbt", "target", "run_results.json"
)


def get_topic_message_count(topic: str) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "custom-exporter-probe",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic, timeout=10)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        consumer.close()
        raise Exception(f"Topic '{topic}' not found")

    total = 0
    for partition_id in metadata.topics[topic].partitions.keys():
        tp = TopicPartition(topic, partition_id)
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        total += high - low

    consumer.close()
    return total


def get_dbt_test_pass_rate() -> float:
    if not os.path.exists(DBT_RUN_RESULTS_PATH):
        logger.warning(f"run_results.json not found at {DBT_RUN_RESULTS_PATH}")
        return 1.0

    with open(DBT_RUN_RESULTS_PATH, "r") as f:
        results = json.load(f)

    test_results = [
        r for r in results.get("results", []) if r.get("unique_id", "").startswith("test.")
    ]
    if not test_results:
        return 1.0

    passed = sum(1 for r in test_results if r.get("status") == "pass")
    return passed / len(test_results)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        try:
            kafka_count = get_topic_message_count(TOPIC)
            pass_rate = get_dbt_test_pass_rate()
            body = (
                f'kafka_topic_message_count{{topic="{TOPIC}"}} {kafka_count}\n'
                f"dbt_test_pass_rate {pass_rate}\n"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))
            logger.info(
                f"Served metrics: kafka_count={kafka_count}, dbt_pass_rate={pass_rate}"
            )
        except Exception as e:
            logger.error(f"Failed to fetch metrics: {e}")
            self.send_response(500)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def main():
    server = HTTPServer(("0.0.0.0", PORT), MetricsHandler)
    logger.info(f"Custom exporter listening on :{PORT}/metrics")
    server.serve_forever()


if __name__ == "__main__":
    main()
