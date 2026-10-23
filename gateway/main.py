from collections import defaultdict
import hashlib
import json
import logging
import time
import uuid
from threading import Lock

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
import redis

app = FastAPI(title="Rate Limiter API Gateway")
logger = logging.getLogger(__name__)

CPP_ENGINE_URL = "http://rate_limiter_app:8080/api/limit"
# Connect to the internal Redis container
redis_client = redis.Redis(host='redis_store', port=6379, decode_responses=True)

LOCAL_FALLBACK_LIMIT = 5
LOCAL_FALLBACK_WINDOW_SECONDS = 10
TELEMETRY_TTL_SECONDS = 86400
MAX_CLIENTS_IN_METRICS_RESPONSE = 100
LOCAL_FALLBACK_COUNTS = defaultdict(int)
LOCAL_FALLBACK_WINDOW_START = time.monotonic()
LOCAL_METRICS = defaultdict(int)
STATE_LOCK = Lock()


def _record_metrics(client_id: str, outcome: str, elapsed_ms: float):
    client_hash = hashlib.sha256(client_id.encode("utf-8")).hexdigest()[:16]
    client_key = f"gateway:metrics:client:{client_hash}"
    now = time.time()

    with STATE_LOCK:
        LOCAL_METRICS["requests_total"] += 1
        LOCAL_METRICS[outcome] += 1
        LOCAL_METRICS["response_time_ms_total"] += int(elapsed_ms)

    try:
        pipeline = redis_client.pipeline(transaction=False)
        pipeline.hincrby("gateway:metrics", "requests_total", 1)
        pipeline.hincrby("gateway:metrics", outcome, 1)
        pipeline.hincrby("gateway:metrics", "response_time_ms_total", int(elapsed_ms))
        pipeline.expire("gateway:metrics", TELEMETRY_TTL_SECONDS)
        pipeline.hincrby(client_key, "requests_total", 1)
        pipeline.hincrby(client_key, outcome, 1)
        pipeline.hincrby(client_key, "response_time_ms_total", int(elapsed_ms))
        pipeline.expire(client_key, TELEMETRY_TTL_SECONDS)
        pipeline.zadd("gateway:metrics:client_index", {client_hash: now})
        pipeline.zremrangebyscore(
            "gateway:metrics:client_index",
            "-inf",
            now - TELEMETRY_TTL_SECONDS,
        )
        pipeline.expire("gateway:metrics:client_index", TELEMETRY_TTL_SECONDS)
        pipeline.execute()
    except redis.RedisError:
        with STATE_LOCK:
            LOCAL_METRICS["telemetry_write_errors"] += 1
        logger.exception("Failed to persist gateway telemetry to Redis")


def _metric_counts(hash_fields):
    counts = {
        "requests_total": 0,
        "allowed": 0,
        "blocked": 0,
        "fallback_allowed": 0,
        "fallback_blocked": 0,
        "response_time_ms_total": 0,
    }
    for field, value in hash_fields.items():
        if field in counts:
            counts[field] = int(value)
    return counts


def _enqueue_violation(client_id: str, request_id: str):
    try:
        redis_client.rpush(
            "violation_queue",
            json.dumps({
                "client_id": client_id,
                "timestamp": time.time(),
                "request_id": request_id,
            }),
        )
    except redis.RedisError:
        logger.exception("Failed to enqueue rate-limit violation for ETL")


def _reset_local_fallback_if_needed():
    global LOCAL_FALLBACK_WINDOW_START
    current_time = time.monotonic()
    with STATE_LOCK:
        if current_time - LOCAL_FALLBACK_WINDOW_START > LOCAL_FALLBACK_WINDOW_SECONDS:
            LOCAL_FALLBACK_COUNTS.clear()
            LOCAL_FALLBACK_WINDOW_START = current_time


def _local_fallback_decision(client_id: str):
    _reset_local_fallback_if_needed()
    with STATE_LOCK:
        LOCAL_FALLBACK_COUNTS[client_id] += 1
        return LOCAL_FALLBACK_COUNTS[client_id] <= LOCAL_FALLBACK_LIMIT


@app.get("/health")
def health_check():
    try:
        redis_status = redis_client.ping()
    except redis.RedisError:
        redis_status = False

    return {
        "status": "ok",
        "redis": "up" if redis_status else "down",
        "engine": CPP_ENGINE_URL,
        "local_fallback_limit": LOCAL_FALLBACK_LIMIT,
    }


@app.get("/metrics")
def metrics():
    _reset_local_fallback_if_needed()
    try:
        gateway_fields = redis_client.hgetall("gateway:metrics")
        client_hashes = redis_client.zrevrange(
            "gateway:metrics:client_index",
            0,
            MAX_CLIENTS_IN_METRICS_RESPONSE - 1,
        )
        client_count = redis_client.zcard("gateway:metrics:client_index")
        etl_fields = redis_client.hgetall("etl:metrics")
        latest_event = redis_client.get("etl:audit:last_processed")
        client_pipeline = redis_client.pipeline(transaction=False)
        for client_hash in client_hashes:
            client_pipeline.hgetall(f"gateway:metrics:client:{client_hash}")
        client_hash_fields = client_pipeline.execute()
        redis_status = True
    except redis.RedisError:
        logger.exception("Failed to read live telemetry from Redis")
        gateway_fields = {}
        client_hashes = []
        client_count = 0
        etl_fields = {}
        latest_event = None
        client_hash_fields = []
        redis_status = False

    client_metrics = {
        client_hash: _metric_counts(fields)
        for client_hash, fields in zip(client_hashes, client_hash_fields)
    }
    with STATE_LOCK:
        local_metrics = dict(LOCAL_METRICS)
        fallback_clients = dict(LOCAL_FALLBACK_COUNTS)

    return {
        "redis": "up" if redis_status else "down",
        "window_seconds": LOCAL_FALLBACK_WINDOW_SECONDS,
        "local_fallback_limit": LOCAL_FALLBACK_LIMIT,
        "gateway": {
            "counters": _metric_counts(gateway_fields),
            "clients": client_metrics,
            "clients_reported": len(client_metrics),
            "clients_truncated": client_count > MAX_CLIENTS_IN_METRICS_RESPONSE,
            "local_process_counters": local_metrics,
            "local_fallback_clients_in_window": fallback_clients,
        },
        "etl": {
            "events_processed": int(etl_fields.get("events_processed", 0)),
            "events_failed": int(etl_fields.get("events_failed", 0)),
            "last_processed_at": etl_fields.get("last_processed_at"),
            "last_event": json.loads(latest_event) if latest_event else None,
        },
    }


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path in {"/health", "/metrics"}:
        return await call_next(request)

    client_id = request.client.host if request.client else "unknown"
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start_time = time.perf_counter()

    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            res = await client.get(f"{CPP_ENGINE_URL}?client_id={client_id}")

            if res.status_code == 429:
                upstream_fallback = res.headers.get("X-RateLimit-Fallback", "").lower() == "true"
                _enqueue_violation(client_id, request_id)
                _record_metrics(
                    client_id,
                    "fallback_blocked" if upstream_fallback else "blocked",
                    (time.perf_counter() - start_time) * 1000,
                )
                response = JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests. Blocked by C++ Engine.", "request_id": request_id},
                    headers={
                        "X-RateLimit-Remaining": res.headers.get("X-RateLimit-Remaining", "0"),
                        "Retry-After": res.headers.get("Retry-After", "1"),
                        "X-RateLimit-Fallback": "true" if upstream_fallback else "false",
                        "X-Request-ID": request_id,
                    },
                )
                return response

            response = await call_next(request)
            upstream_fallback = res.headers.get("X-RateLimit-Fallback", "").lower() == "true"
            _record_metrics(
                client_id,
                "fallback_allowed" if upstream_fallback else "allowed",
                (time.perf_counter() - start_time) * 1000,
            )
            response.headers["X-Request-ID"] = request_id
            response.headers["X-RateLimit-Remaining"] = res.headers.get("X-RateLimit-Remaining", "unknown")
            response.headers["X-RateLimit-Limit"] = res.headers.get("X-RateLimit-Limit", "unknown")
            response.headers["X-RateLimit-Fallback"] = "true" if upstream_fallback else "false"
            response.headers["X-Response-Time"] = f"{(time.perf_counter() - start_time) * 1000:.2f}ms"
            return response
    except httpx.RequestError:
        fallback_allowed = _local_fallback_decision(client_id)
        outcome = "fallback_allowed" if fallback_allowed else "fallback_blocked"
        _record_metrics(
            client_id,
            outcome,
            (time.perf_counter() - start_time) * 1000,
        )
        with STATE_LOCK:
            fallback_count = LOCAL_FALLBACK_COUNTS[client_id]

        if fallback_allowed:
            response = JSONResponse(
                status_code=200,
                content={
                    "status": "degraded_mode",
                    "message": "Using local fallback while Redis/C++ engine is unavailable.",
                    "request_id": request_id,
                },
                headers={"X-Request-ID": request_id, "X-RateLimit-Remaining": str(max(0, LOCAL_FALLBACK_LIMIT - fallback_count))},
            )
            return response

        _enqueue_violation(client_id, request_id)
        response = JSONResponse(
            status_code=429,
            content={"error": "Too Many Requests. Local fallback limit exceeded.", "request_id": request_id},
            headers={"X-Request-ID": request_id, "Retry-After": str(LOCAL_FALLBACK_WINDOW_SECONDS), "X-RateLimit-Remaining": "0"},
        )
        return response


@app.get("/api/data")
def get_protected_data():
    return {"data": "This is highly sensitive backend data. You passed the rate limit!"}