import os
import json
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path="../../.env")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
if not SCHEMA_REGISTRY_URL:
    raise ValueError("SCHEMA_REGISTRY_URL is not defined in the .env file!")

SUBJECT_NAME = "txn_raw-value"
SCHEMA_FILE_PATH = "../schemas/transaction_v1.avsc"


def register_schema():
    print(f"Connecting to Schema Registry at: {SCHEMA_REGISTRY_URL}...")

    check_response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT_NAME}/versions/latest"
    )
    if check_response.status_code == 200:
        print(f"Schema '{SUBJECT_NAME}' exists. Skipping.")
        return

    with open(SCHEMA_FILE_PATH, "r") as f:
        schema_str = f.read()

    payload = {"schema": schema_str}
    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT_NAME}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
    )

    if response.status_code == 200:
        print(f"Schema registered successfully! ID: {response.json()['id']}")
    else:
        print(f"Error: {response.text}")


if __name__ == "__main__":
    register_schema()
