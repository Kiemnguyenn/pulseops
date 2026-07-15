import json
from src.bronze.bronze_writer import parse_message


def test_parse_message_valid_json():
    payload = {
        "transaction_id": "abc-123-def",
        "transaction_time": "2026-07-15 10:00:00",
        "amount": 100.5,
        "sender_account_id": "U123456",
        "receiver_account_id": "M12345",
        "transaction_type": "PAYMENT",
        "status": "COMPLETED",
        "channel": "APP",
        "ip_address": "1.2.3.4",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    result = parse_message(raw_bytes)

    assert result["transaction_id"] == "abc-123-def"
    assert result["amount"] == 100.5
    assert result["status"] == "COMPLETED"
    assert isinstance(result, dict)