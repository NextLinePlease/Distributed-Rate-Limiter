#ifndef REDIS_MANAGER_HPP
#define REDIS_MANAGER_HPP

#include <sw/redis++/redis++.h>
#include <memory>
#include <atomic>

// CIRCUIT BREAKER FSM STATES
enum class CircuitState
{
    CLOSED,   // Redis is healthy
    OPEN,     // Redis is dead, failing open to local cache
    HALF_OPEN // Testing Redis recovery
};

class RedisManager
{
public:
    static RedisManager &getInstance();

    // Restored the exact signature expected by the .cpp files
    std::shared_ptr<sw::redis::Redis> getClient();

    // Inline Circuit Breaker implementations to prevent linker errors
    CircuitState getCircuitState() const
    {
        return state_.load(std::memory_order_relaxed);
    }

    void reportFailure()
    {
        if (failure_count_.fetch_add(1, std::memory_order_relaxed) > 3)
        {
            state_.store(CircuitState::OPEN, std::memory_order_release);
        }
    }

    void reportSuccess()
    {
        failure_count_.store(0, std::memory_order_relaxed);
        state_.store(CircuitState::CLOSED, std::memory_order_release);
    }

private:
    RedisManager();

    // Restored the exact variable name expected by RedisManager.cpp
    std::shared_ptr<sw::redis::Redis> redis_client_;

    // Thread-safe FSM tracking
    std::atomic<CircuitState> state_{CircuitState::CLOSED};
    std::atomic<int> failure_count_{0};
};

#endif