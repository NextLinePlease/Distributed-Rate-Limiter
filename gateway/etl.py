import csv
from datetime import datetime, timezone
import json
import logging
import os
import time

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("violation-etl")

logger.info("Booting up Extract-Transform-Load Pipeline")

redis_client = redis.Redis(host="redis_store", port=6379, decode_responses=True)
csv_file_path = "/app/logs/violations.csv"
os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
        csv.writer(file).writerow(["Client_IP", "Violation_Timestamp", "Status"])


def _parse_event(raw_data):
    try:
        event = json.loads(raw_data)
        client_id = event["client_id"]
        timestamp = float(event["timestamp"])
        request_id = event.get("request_id")
    except json.JSONDecodeError:
        client_id, raw_timestamp = raw_data.split("|", 1)
        timestamp = float(raw_timestamp)
        request_id = None

    if not isinstance(client_id, str) or not client_id:
        raise ValueError("Violation event has no client identifier")

    return client_id, timestamp, request_id


while True:
    try:
        item = redis_client.blpop("violation_queue", timeout=0)
        if not item:
            continue

        try:
            client_id, unix_time, request_id = _parse_event(item[1])
            clean_time = datetime.fromtimestamp(unix_time, timezone.utc).isoformat()

            with open(csv_file_path, mode="a", newline="", encoding="utf-8") as file:
                csv.writer(file).writerow([client_id, clean_time, "BLOCKED"])

            audit_event = json.dumps({
                "client_id": client_id,
                "timestamp": clean_time,
                "request_id": request_id,
                "status": "BLOCKED",
            })
            pipeline = redis_client.pipeline(transaction=False)
            pipeline.hincrby("etl:metrics", "events_processed", 1)
            pipeline.hset("etl:metrics", "last_processed_at", clean_time)
            pipeline.expire("etl:metrics", 86400)
            pipeline.set("etl:audit:last_processed", audit_event, ex=86400)
            pipeline.execute()
            logger.info("Processed violation for %s at %s", client_id, clean_time)
        except (KeyError, ValueError, TypeError, OverflowError, OSError, redis.RedisError):
            try:
                pipeline = redis_client.pipeline(transaction=False)
                pipeline.hincrby("etl:metrics", "events_failed", 1)
                pipeline.expire("etl:metrics", 86400)
                pipeline.execute()
            except redis.RedisError:
                logger.exception("Failed to record ETL failure telemetry")
            logger.exception("Failed to process violation event")
    except redis.RedisError:
        logger.exception("Redis unavailable to ETL worker")
        time.sleep(5)
