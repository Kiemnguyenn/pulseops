# Runbook-003: Container Down Alert (Kafka Broker)

## Symptoms
Slack `#data-alerts` receives the message: **"Kafka broker container is down"**. The Prometheus rule `ContainerDown` shows state Firing (see `http://localhost:9090/alerts`).

## Immediate Actions
```bash
docker ps -a --filter "name=kafka_broker"      # confirm the container actually stopped
docker logs kafka_broker --tail 50             # check why it stopped (crash vs. manual stop)
docker[]() start kafka_broker                      # restart it
docker ps --filter "name=kafka_broker"         # verify it's Up again
```

## Root Cause Checklist
- Was the container stopped manually and unintentionally (`docker stop`)?
- Did it crash due to a configuration error (`docker logs` will show a traceback)?
- Did the host run out of resources (RAM/disk), causing Docker Desktop to kill the container?

## Escalation
If `docker start` doesn't keep the container alive (it crash-loops again), inspect `docker logs` for the specific error — see the `InconsistentClusterIdException` incident from Day 1 (caused by Kafka/Zookeeper Cluster ID mismatch after repeated unsynchronized restarts), fixed by `docker-compose down -v` to reset the volumes and rebuild from scratch.

## Known Limitation — Real-World Detection Latency
Measured twice on Day 6 (Task 6.7): the first attempt took ~7-8 minutes; after adding `evaluation_interval: 15s` to `prometheus.yml` (missing this caused Prometheus to default to a 1-minute rule evaluation interval instead of matching the 15s scrape interval), a clean re-test still measured **6 minutes 19 seconds** (kill at 19:41:14, Slack received at 19:47:33) — exceeding the original <5-minute target. The remaining root cause is most likely that **cAdvisor keeps exposing stats for a stopped-but-not-removed container for several minutes** before the `container_last_seen` metric truly disappears — an inherent cAdvisor limitation outside the scope of Prometheus/Alertmanager configuration. Documented as a known limitation rather than an unresolved bug.
