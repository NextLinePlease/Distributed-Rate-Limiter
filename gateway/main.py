from collections import defaultdict
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
import redis
import time
import uuid

app = FastAPI(title="Rate Limiter API Gateway")

CPP_ENGINE_URL = "http://rate_limiter_app:8080/api/limit"
# Connect to the internal Redis container
redis_client = redis.Redis(host='redis_store', port=6379, decode_responses=True)

LOCAL_FALLBACK_LIMIT = 5
LOCAL_FALLBACK_WINDOW_SECONDS = 10
LOCAL_FALLBACK_COUNTS = defaultdict(int)
LOCAL_FALLBACK_WINDOW_START = time.monotonic()


def _reset_local_fallback_if_needed():
    global LOCAL_FALLBACK_WINDOW_START
    current_time = time.monotonic()
    if current_time - LOCAL_FALLBACK_WINDOW_START > LOCAL_FALLBACK_WINDOW_SECONDS:
        LOCAL_FALLBACK_COUNTS.clear()
        LOCAL_FALLBACK_WINDOW_START = current_time


def _local_fallback_decision(client_id: str):
    _reset_local_fallback_if_needed()
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
    return {
        "window_seconds": LOCAL_FALLBACK_WINDOW_SECONDS,
        "local_fallback_limit": LOCAL_FALLBACK_LIMIT,
        "clients_in_window": dict(LOCAL_FALLBACK_COUNTS),
    }


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_id = request.client.host if request.client else "unknown"
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start_time = time.perf_counter()

    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            res = await client.get(f"{CPP_ENGINE_URL}?client_id={client_id}")

            if res.status_code == 429:
                redis_client.rpush("violation_queue", f"{client_id}|{time.time()}")
                response = JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests. Blocked by C++ Engine.", "request_id": request_id},
                    headers={
                        "X-RateLimit-Remaining": res.headers.get("X-RateLimit-Remaining", "0"),
                        "Retry-After": res.headers.get("Retry-After", "1"),
                        "X-Request-ID": request_id,
                    },
                )
                return response

            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            response.headers["X-RateLimit-Remaining"] = res.headers.get("X-RateLimit-Remaining", "unknown")
            response.headers["X-RateLimit-Limit"] = res.headers.get("X-RateLimit-Limit", "unknown")
            response.headers["X-Response-Time"] = f"{(time.perf_counter() - start_time) * 1000:.2f}ms"
            return response
    except httpx.RequestError:
        if _local_fallback_decision(client_id):
            response = JSONResponse(
                status_code=200,
                content={
                    "status": "degraded_mode",
                    "message": "Using local fallback while Redis/C++ engine is unavailable.",
                    "request_id": request_id,
                },
                headers={"X-Request-ID": request_id, "X-RateLimit-Remaining": str(max(0, LOCAL_FALLBACK_LIMIT - LOCAL_FALLBACK_COUNTS[client_id]))},
            )
            return response

        response = JSONResponse(
            status_code=429,
            content={"error": "Too Many Requests. Local fallback limit exceeded.", "request_id": request_id},
            headers={"X-Request-ID": request_id, "Retry-After": str(LOCAL_FALLBACK_WINDOW_SECONDS), "X-RateLimit-Remaining": "0"},
        )
        return response


@app.get("/api/data")
def get_protected_data():
    return {"data": "This is highly sensitive backend data. You passed the rate limit!"}