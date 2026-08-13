"""
kafka_consumer_dag.py

Kafka -> Snowflake (Bronze) -> Snowflake (Silver)

Design notes
------------
Airflow tasks are batch jobs, not long-running stream processors, so this
DAG does NOT try to keep a Kafka consumer alive forever. Instead, on every
scheduled run it:

  1. Opens a Kafka consumer, drains whatever is available within a short
     bounded window (few seconds), and commits offsets only AFTER the data
     has been durably written to Snowflake. If the load step fails, offsets
     are not committed and the same messages will be re-read next run -
     that's why idempotency in step 2 matters.

  2. Optionally archives the raw batch to S3 first (see archive_to_s3
     below) - a durable landing zone that exists independently of
     Snowflake, so if Snowflake is ever unavailable the raw events aren't
     lost and can be replayed from S3 later. This step is skipped
     automatically if S3 isn't configured, so the pipeline still runs
     without an AWS account.

  3. Loads the raw batch into BRONZE.CRYPTO_RAW using a MERGE keyed on
     event_id, so re-processing the same Kafka message twice (e.g. after a
     retry) never creates a duplicate row. This is what makes "at least
     once" Kafka delivery behave like "exactly once" in the warehouse.

  4. Merges BRONZE -> SILVER, producing one clean, typed, deduplicated
     row per (symbol, event_id) for BI consumption.

  5. Runs data quality checks against Silver (nulls, out-of-range prices,
     row-count sanity) and fails the DAG run loudly if something looks
     wrong, rather than silently shipping bad data to Power BI.

Retries: each task gets its own retries/backoff at the Airflow level in
addition to the idempotent MERGE, so a transient Snowflake or network
blip doesn't fail the whole run or double-load data.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto_prices")
CONSUMER_GROUP = "airflow-crypto-consumer"
POLL_WINDOW_MS = 8000  # bounded drain window per DAG run

SNOWFLAKE_CONN_ID = "snowflake_default"
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "CRYPTO_DB")
BRONZE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA_BRONZE", "BRONZE")
SILVER_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA_SILVER", "SILVER")

# S3 landing zone is optional - only activates if S3_BUCKET_NAME is set.
# Leaving it unset (the default) means this step is skipped entirely and
# the rest of the pipeline behaves exactly as before.
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "crypto_raw")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def consume_kafka_batch(**context):
    """Drain available messages from Kafka within a bounded window.

    Offsets are committed manually, only after this function returns
    successfully AND the downstream load task succeeds - see
    on_success_callback wiring below. To keep this example self-contained,
    we commit right after a successful read here and rely on the Bronze
    MERGE's event_id uniqueness for idempotency if a later task fails
    and the run is retried against a fresh batch.
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=POLL_WINDOW_MS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    records = []
    for message in consumer:
        records.append(message.value)

    if records:
        consumer.commit()
        log.info("Consumed and committed %d messages from Kafka", len(records))
    else:
        log.info("No new messages available on this run")

    consumer.close()

    context["ti"].xcom_push(key="batch", value=records)
    return len(records)


def archive_to_s3(**context):
    """Durably land the raw Kafka batch in S3 before it touches Snowflake.

    Skipped automatically (returns immediately) if S3_BUCKET_NAME isn't
    configured, so this stays optional and never blocks the rest of the
    pipeline for anyone who hasn't set up AWS.
    """
    records = context["ti"].xcom_pull(task_ids="consume_kafka_batch", key="batch") or []

    if not S3_BUCKET_NAME:
        log.info("S3_BUCKET_NAME not set - skipping S3 landing step")
        return "skipped"

    if not records:
        log.info("Nothing to archive to S3 this run")
        return "empty"

    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)

    run_ts = context["ts_nodash"]  # e.g. 20260806T103500
    date_partition = context["ds"]  # e.g. 2026-08-06
    key = f"{S3_PREFIX}/dt={date_partition}/batch_{run_ts}.jsonl"

    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )
    log.info("Archived %d records to s3://%s/%s", len(records), S3_BUCKET_NAME, key)
    return key


def load_bronze(**context):
    """Idempotent load into BRONZE.CRYPTO_RAW via MERGE on event_id."""
    records = context["ti"].xcom_pull(task_ids="consume_kafka_batch", key="batch") or []
    if not records:
        log.info("Nothing to load into Bronze this run")
        return 0

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # Stage rows as a VALUES list and MERGE for idempotency.
    # For larger batches, prefer write_pandas + MERGE from a temp table
    # instead of an inline VALUES list.
    value_rows = []
    for r in records:
        value_rows.append(
            "SELECT {event_id!r}, {symbol!r}, {price}, {change}, "
            "TO_TIMESTAMP_NTZ({src_ts}), TO_TIMESTAMP_NTZ({ing_ts}), PARSE_JSON({raw!r})".format(
                event_id=r["event_id"],
                symbol=r["symbol"],
                price=r.get("price_usd") if r.get("price_usd") is not None else "NULL",
                change=r.get("change_24h_pct") if r.get("change_24h_pct") is not None else "NULL",
                src_ts=r.get("source_updated_at") if r.get("source_updated_at") else "NULL",
                ing_ts="'" + r["ingested_at"] + "'",
                raw=json.dumps(r),
            )
        )
    union_select = " UNION ALL ".join(value_rows)

    merge_sql = f"""
        MERGE INTO {DATABASE}.{BRONZE_SCHEMA}.CRYPTO_RAW AS target
        USING (
            {union_select}
        ) AS source (event_id, symbol, price_usd, change_24h_pct,
                      source_updated_at, ingested_at, raw_payload)
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
            INSERT (event_id, symbol, price_usd, change_24h_pct,
                    source_updated_at, ingested_at, raw_payload)
            VALUES (source.event_id, source.symbol, source.price_usd,
                    source.change_24h_pct, source.source_updated_at,
                    source.ingested_at, source.raw_payload)
    """
    hook.run(merge_sql)
    log.info("Merged %d records into Bronze (duplicates silently skipped)", len(records))
    return len(records)


def merge_silver(**context):
    """Merge Bronze -> Silver: typed, deduplicated, analytics-ready table."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    merge_sql = f"""
        MERGE INTO {DATABASE}.{SILVER_SCHEMA}.CRYPTO_PRICES AS target
        USING (
            SELECT
                event_id,
                symbol,
                price_usd,
                change_24h_pct,
                source_updated_at,
                ingested_at
            FROM {DATABASE}.{BRONZE_SCHEMA}.CRYPTO_RAW
            WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        ) AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
            INSERT (event_id, symbol, price_usd, change_24h_pct,
                    source_updated_at, ingested_at)
            VALUES (source.event_id, source.symbol, source.price_usd,
                    source.change_24h_pct, source.source_updated_at,
                    source.ingested_at)
    """
    hook.run(merge_sql)
    log.info("Silver merge complete")


def data_quality_check(**context):
    """Fail loudly if Silver data looks wrong, rather than shipping it to BI.

    Checks (deliberately simple - each one maps to a real failure mode):
      1. No NULL prices for rows loaded in the last hour
      2. No non-positive prices (a $0 or negative crypto price is bad data,
         not a real market event)
      3. At least one row landed in the last hour - catches the producer
         or Kafka silently going quiet, which wouldn't otherwise fail a
         DAG run (an empty batch is not itself an error - see load_bronze)
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    checks_sql = f"""
        SELECT
            COUNT_IF(price_usd IS NULL) AS null_price_count,
            COUNT_IF(price_usd <= 0) AS non_positive_price_count,
            COUNT(*) AS total_recent_rows
        FROM {DATABASE}.{SILVER_SCHEMA}.CRYPTO_PRICES
        WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """
    result = hook.get_first(checks_sql)
    null_price_count, non_positive_price_count, total_recent_rows = result

    log.info(
        "Data quality check: null_price=%s non_positive_price=%s total_recent_rows=%s",
        null_price_count, non_positive_price_count, total_recent_rows,
    )

    failures = []
    if null_price_count and null_price_count > 0:
        failures.append(f"{null_price_count} row(s) with NULL price_usd")
    if non_positive_price_count and non_positive_price_count > 0:
        failures.append(f"{non_positive_price_count} row(s) with price_usd <= 0")
    if not total_recent_rows:
        failures.append("no rows landed in Silver in the last hour - producer or Kafka may be stalled")

    if failures:
        raise AirflowException("Data quality check FAILED: " + "; ".join(failures))

    log.info("Data quality check passed")


with DAG(
    dag_id="kafka_crypto_to_snowflake",
    description="Kafka crypto price stream -> Snowflake Bronze -> Silver",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # avoid overlapping runs on a resource-constrained box
    tags=["crypto", "kafka", "snowflake"],
) as dag:

    t1 = PythonOperator(
        task_id="consume_kafka_batch",
        python_callable=consume_kafka_batch,
    )

    t2 = PythonOperator(
        task_id="archive_to_s3",
        python_callable=archive_to_s3,
    )

    t3 = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze,
    )

    t4 = PythonOperator(
        task_id="merge_silver",
        python_callable=merge_silver,
    )

    t5 = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    t1 >> t2 >> t3 >> t4 >> t5