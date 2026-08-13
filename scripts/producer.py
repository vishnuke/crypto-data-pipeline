"""
Crypto price producer.

Polls a public crypto price API (CoinGecko by default - no API key required)
on a fixed interval and publishes one JSON event per symbol to Kafka.

Idempotency: each event carries a stable `event_id` derived from
(symbol, source_timestamp_minute) rather than a random UUID. If this
producer restarts and re-polls the same minute, the consumer/Snowflake
MERGE step can de-dupe on that key instead of creating duplicate rows.

Reliability:
  - HTTP calls wrapped in tenacity retry with exponential backoff
  - Kafka producer configured with acks='all' and retries so messages
    aren't silently dropped if a broker hiccups
  - Producer flush()'d after every batch so we don't lose buffered
    messages on a crash
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("crypto-producer")

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "crypto_prices")
API_URL = os.environ.get("CRYPTO_API_URL", "https://api.coingecko.com/api/v3/simple/price")
SYMBOLS = os.environ.get("CRYPTO_SYMBOLS", "bitcoin,ethereum,solana,cardano").split(",")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))


def make_event_id(symbol: str, minute_bucket: str) -> str:
    raw = f"{symbol}:{minute_bucket}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=20))
def fetch_prices() -> dict:
    params = {
        "ids": ",".join(SYMBOLS),
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def build_producer() -> KafkaProducer:
    for attempt in range(1, 11):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=200,
            )
        except KafkaError as e:
            log.warning("Kafka not ready yet (attempt %s/10): %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def main():
    producer = build_producer()
    log.info("Connected to Kafka at %s, publishing to topic '%s'", BOOTSTRAP_SERVERS, TOPIC)

    while True:
        try:
            data = fetch_prices()
            now = datetime.now(timezone.utc)
            minute_bucket = now.strftime("%Y-%m-%dT%H:%M")

            for symbol, payload in data.items():
                event = {
                    "event_id": make_event_id(symbol, minute_bucket),
                    "symbol": symbol,
                    "price_usd": payload.get("usd"),
                    "change_24h_pct": payload.get("usd_24h_change"),
                    "source_updated_at": payload.get("last_updated_at"),
                    "ingested_at": now.isoformat(),
                }
                producer.send(TOPIC, key=symbol, value=event)
                log.info("Published: %s", event)

            producer.flush()

        except Exception as exc:  # noqa: BLE001 - producer loop must never die silently
            log.error("Error during poll/publish cycle: %s", exc, exc_info=True)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
