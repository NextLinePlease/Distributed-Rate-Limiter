#ifndef RATE_LIMITER_HPP
#define RATE_LIMITER_HPP

#include <string>
#include <cstdint>

// HARDWARE OPTIMIZATION: 64-byte cache-line alignment prevents false sharing
// across multi-core CPU threads during high-concurrency loops.
struct alignas(64) RateLimitResult
{
    // BIT-PACKING: Compressing 3 distinct boolean/state flags into a single byte
    bool allowed : 1;
    bool is_fallback : 1;
    uint8_t tier_level : 6;

    int64_t remaining_quota;
    int64_t retry_after_ms;
};

class RateLimiter
{
public:
    virtual ~RateLimiter() = default;
    virtual RateLimitResult allow(const std::string &client_id, int64_t cost = 1) = 0;
};

#endif