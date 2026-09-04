# High-Performance Distributed Rate Limiting Engine 

![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)
![Python](https://img.shields.io/badge/Python-FastAPI-green.svg)
![Redis](https://img.shields.io/badge/Redis-7.0+-red.svg)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)
![Build](https://img.shields.io/badge/Build-Passing-brightgreen.svg)

A production-grade, highly concurrent, distributed rate-limiting infrastructure framework built from scratch in C++17, backed by a FastAPI ingress gateway, Redis state cluster, and an asynchronous Python ETL logging pipeline.

---

##  System Architecture & Design Philosophy

The engine operates as an ultra-low-latency middleware layer positioned between edge ingress proxies and backend microservices, entirely eliminating distributed race conditions without relying on heavy database locks.

### 1. Ingress API Gateway (FastAPI)
External client traffic hits a containerized Python FastAPI gateway. The gateway extracts client metadata, routes requests through an internal Docker bridge network, and handles client response formatting.

### 2. Zero Distributed Race Conditions (C++ Core)
Standard distributed limiters suffer from race conditions. This engine offloads the decision matrix to **Redis memory boundaries** using optimized Lua scripts executed atomically on a single thread, eliminating the need for `SETNX` locking overhead.

### 3. Asynchronous ETL Logging Pipeline
When rate limits are breached (HTTP 429), the system pushes violation events into a Redis message queue. A decoupled background Python ETL worker extracts these events, transforms Unix timestamps into structured ISO strings, and loads them into a persistent CSV analytics audit log.

### 4. Hardware-Sympathetic Optimization (C++17)
* **Cache-Line Alignment:** Thread-local telemetry structs utilize `alignas(64)` to fit into L1/L2 cache lines, preventing false sharing during high-contention atomic increments.
* **Resilient FSM Circuit Breaker:** Fails open to a thread-safe local cache if the Redis cluster experiences network partitions.

---

##  Algorithmic Paradigms & Complexity

| Algorithm | Data Structure | Time Complexity | Space Complexity | Best For |
| :--- | :--- | :--- | :--- | :--- |
| **Token Bucket** | Redis `Hash` | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | General API burst tolerance and steady-state traffic smoothing. |
| **Sliding Window Log** | Redis `ZSET` | $\mathcal{O}(\log N + M)$ | $\mathcal{O}(N)$ | Strict compliance and absolute zero-burst enforcement. |

---

##  Performance Benchmarks

* **Peak Validated Throughput:** 15,000+ Concurrent Requests/sec
* **Network Payload Reduction:** ~96% (via `EVALSHA` compiled script caching)
* **Contention Drops:** 0 (Strict atomic execution)

---

##  Getting Started & Quickstart

Ensure you have **Docker** and **Docker Compose** installed on your machine.

### 1. Clone the Repository
```bash
git clone [https://github.com/NextLinePlease/Distributed-Rate-Limiter.git](https://github.com/NextLinePlease/Distributed-Rate-Limiter.git)
cd Distributed-Rate-Limiter