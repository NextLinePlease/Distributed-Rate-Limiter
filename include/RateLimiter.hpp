#ifndef RATE_LIMITER_HPP
#define RATE_LIMITER_HPP

#include <string>
#include <cstdint>

// Returns the decision and quota details for one rate-limit check.
struct alignas(64) RateLimitResult
{
    // Packs the allow/fallback flags and tier value into one bit-field group.
    bool allowed : 1;
    bool is_fallback : 1;
    uint8_t tier_level : 6;

    // Reports the remaining quota and wait time before retrying.
    int64_t remaining_quota;
    int64_t retry_after_ms;
};

// Defines the common interface implemented by each limiter algorithm.
class RateLimiter
{
public:
    // Allows cleanup through a pointer to the base interface.
    virtual ~RateLimiter() = default;

    // Checks whether a client may consume the requested quota cost.
    virtual RateLimitResult allow(const std::string &client_id, int64_t cost = 1) = 0;
};

#endif