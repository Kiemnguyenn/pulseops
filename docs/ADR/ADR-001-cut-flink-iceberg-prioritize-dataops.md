# ADR-001: Cut Flink, Iceberg, OpenMetadata — Prioritize DataOps Depth

## Context
The original BRD (FintechFlow) had a broad Data Engineering scope (real-time Flink, full Iceberg medallion architecture, OpenMetadata lineage). With a 9-day deadline and a target role of pure DataOps, there wasn't enough time to go deep on both fronts.

## Decision
Cut Flink, Iceberg, OpenMetadata, and Metabase from scope. Replace the Bronze writer with a plain Python script (no Spark); replace Iceberg with a regular Postgres table.

## Reasons
DataOps hiring evaluates operational reliability (CI/CD, IaC, Observability, Incident Response) rather than transform-logic complexity. A simple pipeline with proper CI/CD, full monitoring, and clear runbooks carries more interview weight than a complex Flink pipeline lacking observability.

## Trade-offs
Loses the "I built sub-5-second real-time fraud detection" story — but gains the story of "I personally debugged and fixed many real CI/CD and infrastructure incidents, with log evidence" (this happened more than 10 times for real across the first 6 days: Black formatting x3, network config x3, Python version mismatch, missing profiles.yml, evaluation_interval bug...), which maps much more closely to day-to-day DataOps work.
