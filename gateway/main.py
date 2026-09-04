from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
import redis
import time

app = FastAPI(title="Rate Limiter API Gateway")

CPP_ENGINE_URL = "http://rate_limiter_app:8080/api/limit"
# Connect to the internal Redis container
redis_client = redis.Redis(host='redis_store', port=6379, decode_responses=True)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_id = request.client.host
    
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(f"{CPP_ENGINE_URL}?client_id={client_id}")
            if res.status_code == 429:
                # Push raw event to Redis for the ETL pipeline to process
                redis_client.rpush("violation_queue", f"{client_id}|{time.time()}")
                return JSONResponse(status_code=429, content={"error": "Too Many Requests. Blocked by C++ Engine."})
        except httpx.RequestError:
            return JSONResponse(status_code=503, content={"error": "Rate Limiter Service Unavailable"})
            
    response = await call_next(request)
    return response

@app.get("/api/data")
def get_protected_data():
    return {"data": "This is highly sensitive backend data. You passed the rate limit!"}