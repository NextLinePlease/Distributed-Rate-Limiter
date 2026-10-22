#ifndef SLIDING_WINDOW_LIMITER_HPP
#define SLIDING_WINDOW_LIMITER_HPP

#include "RateLimiter.hpp"
#include <string>
#include <cstdint>

// Enforces a request limit over a rolling time window.
class SlidingWindowLimiter : public RateLimiter
{
public:
    // Sets the rolling window duration and maximum request count.
    SlidingWindowLimiter(int64_t window_size_ms, int64_t max_requests);

    // Evaluates a client's request against the configured window.
    RateLimitResult allow(const std::string &client_id, int64_t cost = 1) override;

private:
    // Stores the rolling window length in milliseconds.
    int64_t window_size_ms_;

    // Stores the maximum number of requests allowed in the window.
    int64_t max_requests_;

    // Caches the SHA returned when the Lua script is loaded into Redis.
    std::string script_sha_;

    // Loads the atomic sliding-window script into Redis.
    void loadLuaScript();
};

#endif