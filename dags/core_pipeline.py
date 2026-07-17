import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def check_kafka_topic_exists(**context):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = admin_client.list_topics(timeout=10).topics
    if "txn_raw" not in topics:
        raise Exception(f"Kafka topic 'txn_raw' not found. Available: {list(topics.keys())}")
    logging.info(f"Kafka topic 'txn_raw' found. All topics: {list(topics.keys())}")


def notify_slack(**context):
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification.")
        return
    message = {"text": "✅ PulseOps core_pipeline DAG completed successfully!"}
    response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
    if response.status_code != 200:
        logging.warning(f"Slack notify failed: {response.status_code} {response.text}")


default_args = {
    "owner": "pulseops",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(minutes=10),
}

with DAG(
    dag_id="core_pipeline",
    default_args=default_args,
    description="PulseOps core DataOps pipeline: ingest -> transform -> test -> notify",
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["pulseops", "core"],
) as dag:

    check_kafka_topic = PythonOperator(
        task_id="check_kafka_topic_exists",
        python_callable=check_kafka_topic_exists,
    )

    run_bronze_ingest = BashOperator(
        task_id="run_bronze_ingest",
        bash_command="python /opt/airflow/src/bronze/bronze_writer.py --once",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    )

    notify = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack,
    )

    check_kafka_topic >> run_bronze_ingest >> dbt_run >> dbt_test >> notify