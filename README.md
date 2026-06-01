# High-Performance Distributed Rate Limiting Engine (C++11 & Redis)

A production-grade, highly concurrent, distributed rate-limiting infrastructure framework built from scratch in C++. This engine implements low-latency traffic policing across decoupled server nodes using custom network layer protocols and atomic scripting.

## 🛠️ Key Architectural Features
* **Zero Distributed Race Conditions:** Decision engine offloaded entirely to Redis memory boundaries via atomic optimized **Lua scripts**.
* **Zero Script Network Bloat:** Bootstrapped compile-time fingerprinting using **EVALSHA**, dropping network bandwidth payloads from 1KB down to 40 bytes per request.
* **Resilient Three-State Circuit Breaker:** Implements an automated `CLOSED`, `OPEN`, and `HALF-OPEN` Finite State Machine (FSM) that catches database partitions, failing open to a local **in-memory backup cache** to preserve 100% platform availability.
* **Hardware-Sympathetic Optimization:** Data layout structures utilize **C++11 cache-line alignment (`alignas(64)`)** to prevent false sharing, and **bit-field packing** to compress client validation flags by 87.5% (from 8 bytes down to 1 byte).
* **Asynchronous Telemetry Stream:** Decoupled metrics pipeline that queues log payloads into Redis via microsecond `LPUSH` rings, keeping analytics entirely out of the critical performance request path.

## 📊 Algorithmic Paradigms Implemented
1. **Token Bucket:** Optimized for smooth steady-state traffic profile enforcement.
2. **Sliding Window Log:** Maximum security iteration providing absolute zero burst tolerance via Redis Sorted Sets ($O(N)$ memory).
3. **Sliding Window Counter:** High-scale compromise utilizing proportional time-slice weight calculations ($O(1)$ memory).

## 🚀 Quickstart Build (Windows Native)
Prerequisites: Docker (Running local Redis instance on port 6379), GCC compiler.

```powershell
# Clone the repository
git clone [https://github.com/NextLinePlease/Distributed-Rate-Limiter.git](https://github.com/NextLinePlease/Distributed-Rate-Limiter.git)
cd Distributed-Rate-Limiter

# Compile and execute the live simulation harness
g++ -std=c++11 main.cpp -o production_engine -lws2_32
.\production_engine.exe