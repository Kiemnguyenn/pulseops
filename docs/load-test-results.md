# Load Test Results

## Test Conditions
Machine: Windows 11, Docker Desktop, 16 CPUs available. Measured while the full core stack (Kafka, Postgres, Debezium, Schema Registry) was running concurrently, plus Bronze writer consuming in parallel.

## Producer Throughput (Task 7.5)
- Ran `transaction_producer.py` for exactly 60 seconds (`timeout 60` via Git Bash).
- Cross-verified with a direct database count:
  - `bronze_transactions` row count before: 178,735
  - `bronze_transactions` row count after: 198,291
  - Net rows written during the test window: **19,556**
- **Throughput: ~326 events/s** (19,556 / 60), fully accounted for — no meaningful data loss between Kafka and Postgres, thanks to idempotent writes (`ON CONFLICT DO NOTHING`, see ADR-003).

## Alert Response Time (Task 6.7)
- First attempt (before the `evaluation_interval` fix): ~7-8 minutes
- Clean re-test (after adding `evaluation_interval: 15s` to `prometheus.yml`): **6 minutes 19 seconds** (kill at 19:41:14, Slack received at 19:47:33)
- Exceeds the original <5-minute target. Root cause: cAdvisor appears to retain metrics for a stopped-but-not-removed container for several minutes before `container_last_seen` truly disappears — an inherent cAdvisor limitation, not a Prometheus/Alertmanager misconfiguration. See Runbook-003 for details.

## CI Duration
1 minute 8 seconds (Core CI Pipeline, run #13, triggered via PR #4, status Success).

## CD Duration
32 seconds (Core CD Pipeline, run #6, triggered via merge of PR #4 into main, status Success).
