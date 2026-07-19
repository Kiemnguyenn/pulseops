# ADR-003: Use ON CONFLICT DO NOTHING Instead of Complex Application-Level Dedup

## Context
The Bronze writer (Kafka consumer) must avoid duplicate `transaction_id` rows when the consumer crashes/restarts mid-stream and re-reads previously processed messages (at-least-once delivery).

## Decision
Use a PostgreSQL PRIMARY KEY constraint on `transaction_id` combined with `INSERT ... ON CONFLICT (transaction_id) DO NOTHING`, instead of writing custom duplicate-checking logic in Python (e.g., a SELECT-before-INSERT check or an in-memory cache).

## Reasons
Simpler, and follows the principle of pushing data-integrity guarantees down to the database layer, where they're best optimized — Postgres handles constraint checks at the index level, faster and less error-prone than hand-rolled, race-condition-prone application logic. Verified in practice at Task 3.4 (killed the script mid-run, restarted, 0 duplicate rows) and Gap 2/UC-5 (restarted the Kafka broker while both Producer and Bronze writer were running concurrently, still 0 duplicates thanks to `enable.idempotence=true` on the Producer combined with this constraint).

## Trade-offs
Only works correctly if `transaction_id` generation is genuinely unique (UUIDs here, collision probability effectively zero) — a less trustworthy ID source would need an additional composite business key.
