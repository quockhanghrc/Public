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

---

## ⚠️ Credentials & Secrets Found

This folder contains **hardcoded credentials** in multiple places. **Do not commit this folder to a public repository.**

### 1. `config/api-key-T3WBHQ2KS3WY6R7A.txt`
- **Confluent Cloud API key + secret** for cluster `lkc-jxjnop`.
- Used to authenticate the Kafka producer/connector.

### 2. `config/teak-store-392915-a1eeec999969.json`
- **GCP Service Account JSON key** for project `teak-store-392915`.
- Grants full access to the GCP resources (Cloud SQL, GCS, etc.).

### 3. Hardcoded in `generator.py` & `test_connection.py`
- **PostgreSQL password** (`u-rJmo:e9a4p/pq2`) for the `postgres` user.
- Host: `34.50.88.3` (public IP of a GCP Cloud SQL instance).

### Recommended actions
- Move all secrets to environment variables or a secrets manager (e.g., GCP Secret Manager, HashiCorp Vault).
- Add `config/` to `.gitignore`.
- Rotate the exposed credentials (Confluent API key, GCP service account key, PostgreSQL password).
