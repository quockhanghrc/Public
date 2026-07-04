# Debezium CDC Demo — PostgreSQL → Kafka (Confluent Cloud)

This folder contains a **Change Data Capture (CDC)** simulation project using **Debezium** to stream PostgreSQL database changes to **Confluent Cloud (Kafka)**.

## Files

| File | Purpose |
|---|---|
| `table_schema.sql` | PostgreSQL schema with `users` and `orders` tables, configured for CDC (`REPLICA IDENTITY FULL`, `wal_level = logical`). |
| `generator.py` | Continuous data generator — randomly inserts, updates, and deletes rows to simulate live transactional traffic. |
| `test_connection.py` | Validates connectivity to the PostgreSQL instance and checks that `wal_level` is set to `logical`. |

## How it works

1. **Setup** — Run `table_schema.sql` on a PostgreSQL instance (currently pointed at a GCP Cloud SQL host).
2. **Validate** — Run `test_connection.py` to confirm the DB is reachable and CDC-ready.
3. **Generate** — Run `generator.py` to produce INSERT/UPDATE/DELETE events.
4. **Capture** — Debezium connector (deployed separately) reads the WAL and streams changes to Confluent Cloud.

