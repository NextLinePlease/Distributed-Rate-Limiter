#ifndef REDIS_MANAGER_HPP
#define REDIS_MANAGER_HPP

#include <sw/redis++/redis++.h>
#include <memory>
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

// Represents the Redis connection circuit condition.
enum class CircuitState
{
    CLOSED,   // Redis is healthy
    OPEN,     // Redis calls are blocked after repeated failures
    HALF_OPEN // A request can probe whether Redis has recovered
};

// Provides the shared Redis client and tracks connection health.
class RedisManager
{
public:
    // Returns the process-wide Redis manager instance.
    static RedisManager &getInstance();

    // Returns the shared Redis client used by the limiter implementations.
    std::shared_ptr<sw::redis::Redis> getClient();

    // Reads the current breaker state without locking.
    CircuitState getCircuitState() const
    {
        return state_.load(std::memory_order_relaxed);
    }

    // Determines whether the system should switch to local fallback behavior.
    bool shouldUseFallback() const
    {
        return state_.load(std::memory_order_relaxed) != CircuitState::CLOSED;
    }

    // Counts a Redis failure and opens the breaker after the threshold.
    void reportFailure()
    {
        if (failure_count_.fetch_add(1, std::memory_order_relaxed) >= 3)
        {
            state_.store(CircuitState::OPEN, std::memory_order_release);
        }
    }

    // Clears the failure count and closes the breaker after success.
    void reportSuccess()
    {
        failure_count_.store(0, std::memory_order_relaxed);
        state_.store(CircuitState::CLOSED, std::memory_order_release);
    }

    // Attempts a lightweight health probe to confirm Redis is still reachable.
    bool ping()
    {
        try
        {
            if (!redis_client_)
            {
                return false;
            }

            auto response = redis_client_->ping();
            if (response == "PONG")
            {
                reportSuccess();
                return true;
            }
        }
        catch (const sw::redis::Error &)
        {
            reportFailure();
        }

        return false;
    }

private:
    // Creates and initializes the Redis connection pool.
    RedisManager();

    // Shares the configured Redis client across limiter calls.
    std::shared_ptr<sw::redis::Redis> redis_client_;

    // Stores breaker state and consecutive failure count atomically.
    std::atomic<CircuitState> state_{CircuitState::CLOSED};
    std::atomic<int> failure_count_{0};
};

#endif