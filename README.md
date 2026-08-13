# Real-Time Crypto Data Pipeline

Crypto API → Kafka → Airflow → Snowflake → Power BI

A production-shaped (not production-scale) data engineering pipeline, tuned
to run entirely on a single 8GB-RAM laptop.

## 1. Why this stack fits an 8GB / i3 machine

The stock "Kafka + Zookeeper + Airflow + Celery + Redis + Flower" tutorial
stack easily wants 6-8GB on its own, before your OS or browser. Two changes
bring it down to a comfortable footprint:

| Change | Saves | Why it's safe here |
|---|---|---|
| Kafka in **KRaft mode** (no Zookeeper), official `apache/kafka` image | ~500MB-1GB | KRaft has been production-ready since Kafka 3.3; Zookeeper is legacy for new deployments. The official ASF image also avoids third-party registries that can change their free-tier tagging policy without notice (this happened to `bitnami/kafka` in 2025). |
| Airflow **LocalExecutor** (no Redis/Celery/Flower) | ~1GB+ | You have one worker (your laptop) anyway — a distributed task queue buys nothing here |
| Explicit `mem_limit` per container + capped Kafka JVM heap | prevents OOM freezes | Docker's default (no limit) lets one container's heap grab RAM until Windows starts swapping hard |

Approximate footprint with this compose file:

```
postgres            256MB
kafka                768MB
airflow-webserver   1024MB
airflow-scheduler   1024MB
producer              128MB
kafka-init (transient) 128MB
---------------------------------
~3.3GB total, leaving ~4.5GB for Windows + your browser
```

**Practical tips for your machine specifically:**
- Close Chrome/Edge tabs before `docker compose up` — browsers are usually
  the biggest RAM competitor on a dev laptop.
- In Docker Desktop → Settings → Resources, cap the WSL2 VM at ~5-6GB so it
  can't starve Windows itself.
- Run one thing at a time while developing: bring up `postgres` + `kafka`
  first, confirm healthy, *then* bring up Airflow.
- If Airflow feels sluggish, it's normal on a dual-core CPU — the
  scheduler + webserver + gunicorn workers are competing for 4 threads.
  This is fine for a portfolio demo; it is not how you'd run this at scale
  (see "Scaling beyond this laptop" below).

## 2. Project structure

```
airflow-docker/
├── docker-compose.yml       # Kafka (KRaft) + Postgres + Airflow (LocalExecutor)
├── Dockerfile                # Airflow image + Snowflake/Kafka providers
├── Dockerfile.producer       # Slim standalone image for the producer
├── requirements.txt          # Airflow-side Python deps
├── .env.example               # Copy to .env and fill in Snowflake creds
├── dags/
│   └── kafka_consumer_dag.py # Kafka -> Bronze -> Silver, idempotent MERGE
├── scripts/
│   └── producer.py           # Polls CoinGecko, publishes to Kafka
├── sql/
│   └── snowflake_schema.sql  # Warehouse/DB/schema/tables/roles DDL
├── logs/                     # Airflow logs (gitignored)
└── plugins/                  # Airflow plugins (empty, optional)
```

## 3. One-time setup

### 3.1 Snowflake
1. Log into Snowflake (a free trial account is enough for this project).
2. Run `sql/snowflake_schema.sql` in a Snowsight worksheet as an admin role.
   It creates an XS warehouse (auto-suspends after 60s idle, so cost stays
   near zero), the `CRYPTO_DB` database, `BRONZE`/`SILVER` schemas, tables,
   and two roles (`CRYPTO_LOADER_ROLE` for Airflow, `CRYPTO_BI_ROLE` for
   Power BI).
3. Create a service user for Airflow and grant it `CRYPTO_LOADER_ROLE`.
   For a portfolio project a password is fine; for anything closer to real
   production, switch to **key-pair authentication** (Snowflake supports
   RSA key auth so no password sits in an env var at all).

### 3.2 Environment variables
```bash
cp .env.example .env
# edit .env with your real Snowflake account/user/password
```

### 3.3 Bring the stack up
```bash
docker compose up -d --build postgres kafka
# wait ~20s, then confirm kafka is healthy:
docker compose ps

docker compose up -d --build airflow-init
docker compose up -d --build airflow-webserver airflow-scheduler producer
```

Airflow UI: http://localhost:8080 (user/pass from `.env`, default `admin`/`admin`).

### 3.4 Add the Snowflake connection in Airflow
UI → Admin → Connections → `+`:
- Conn Id: `snowflake_default`
- Conn Type: `Snowflake`
- Login / Password / Account / Warehouse / Database / Role: from your `.env`

(Or set it via env var instead of the UI: `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`
as a Snowflake-format URI — see the provider docs linked in section 7.)

### 3.5 Unpause the DAG
In the UI, toggle `kafka_crypto_to_snowflake` on. It runs every 5 minutes,
draining whatever the producer has published to the `crypto_prices` topic
since the last run.

## 4. How the reliability pieces fit together

**Idempotency, end to end:**
- The producer derives `event_id` from `(symbol, minute-bucket)`, not a
  random UUID — so re-publishing the same reading twice (e.g. after a
  producer restart) produces the same key.
- The Bronze load is a `MERGE ... WHEN NOT MATCHED THEN INSERT` keyed on
  `event_id`, so replaying Kafka messages (which is expected/normal with
  at-least-once delivery) never creates duplicate rows.
- The Silver merge is likewise keyed on `event_id`.
- Kafka consumer offsets are committed manually, only after a successful
  read — if a downstream Snowflake write fails, the DAG retries and the
  same messages get re-read, but the MERGE means that's harmless.

**Retries:**
- Each Airflow task has `retries=3` with exponential backoff
  (`retry_exponential_backoff=True`) at the DAG level.
- The producer's HTTP calls use `tenacity` with exponential backoff; its
  Kafka connection retries up to 10 times on startup (handles Kafka not
  being ready yet on `docker compose up`).
- `max_active_runs=1` on the DAG prevents overlapping runs from stacking
  up if Snowflake is briefly slow — important on constrained hardware.

## 5. Data model: Bronze vs Silver

- **Bronze (`CRYPTO_DB.BRONZE.CRYPTO_RAW`)**: append-only landing zone.
  Keeps the full raw JSON in a `VARIANT` column alongside a few extracted
  fields, so you can always replay/reprocess if the Silver logic changes
  later, and so schema drift in the source API doesn't break ingestion.
- **Silver (`CRYPTO_DB.SILVER.CRYPTO_PRICES`)**: typed, deduplicated,
  clustered by `(symbol, ingested_at)` for fast "latest price" / time-range
  queries. This is the layer Power BI (and any analyst) should query —
  never Bronze directly.
- A convenience view, `SILVER.V_LATEST_PRICES`, gives the current price per
  symbol without a window function in every BI query.

## 6. Power BI

1. Power BI Desktop → Get Data → **Snowflake**.
2. Server: `<account>.snowflakecomputing.com`, Warehouse: `CRYPTO_WH`.
3. Sign in with the `CRYPTO_BI_ROLE` service account (read-only — Power BI
   should never use the loader role).
4. Import `SILVER.CRYPTO_PRICES` (for trend charts over time) and/or
   `SILVER.V_LATEST_PRICES` (for a current-price tile).
5. Suggested visuals: line chart of `price_usd` over `ingested_at` per
   `symbol`; card visuals showing `change_24h_pct`; a table of latest
   prices from the view.
6. For a live-feeling dashboard without Premium, set the dataset refresh
   to Power BI's shortest allowed interval (or use DirectQuery against
   Snowflake if query volume/cost is acceptable).

## 7. Monitoring & logging

- Airflow UI's **Graph**/**Grid** views + task logs are your primary
  monitoring surface for this project size — no need for a separate
  metrics stack.
- `docker compose logs -f producer` shows live producer activity
  (published events, retry warnings).
- `docker compose logs -f kafka` for broker-level issues.
- For something closer to production, the natural next step is shipping
  Airflow task logs + a StatsD/Prometheus exporter to Grafana — out of
  scope for a laptop demo but worth mentioning in a portfolio write-up as
  "next steps."

## 8. Scaling beyond this laptop (for your portfolio write-up)

Worth documenting even if you don't build it:
- Swap `LocalExecutor` → `CeleryExecutor` (or `KubernetesExecutor`) once
  you have more than one worker node.
- Multi-broker Kafka cluster with replication factor > 1 (this repo uses
  RF=1, single broker — fine for a demo, not for durability guarantees).
- Snowflake Snowpipe / Snowpipe Streaming instead of micro-batched Airflow
  MERGEs, for genuinely low-latency ingestion at higher volume.
- Move DAG-level Python transforms into dbt models for the Bronze→Silver
  (and a future Gold) layer, so transformations are version-controlled,
  tested, and documented independently of orchestration.

## 9. Bonus ideas (not implemented here, but natural extensions)

- **S3 Bronze landing** before Snowflake, so raw events are durable even
  if Snowflake is briefly unavailable.
- **Data quality checks**: add a Great Expectations or dbt-test step after
  `merge_silver` (e.g. `price_usd IS NOT NULL AND price_usd > 0`).
- **CI/CD**: GitHub Actions workflow that lints the DAG (`airflow dags
  list-import-errors`), runs `sqlfluff` on the SQL, and on merge to `main`
  deploys DAGs to a remote Airflow instance (e.g. via `rsync`/`git-sync`
  sidecar or MWAA/Composer's deploy mechanism).
- **Spark Structured Streaming** in place of the Airflow micro-batch
  consumer, if you outgrow 5-minute batch latency.
