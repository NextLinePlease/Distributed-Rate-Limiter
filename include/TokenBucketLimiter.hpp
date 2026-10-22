#ifndef TOKEN_BUCKET_LIMITER_HPP
#define TOKEN_BUCKET_LIMITER_HPP

#include "RateLimiter.hpp"
#include <string>
#include <cstdint>

// Enforces a burst limit with tokens replenished at a steady rate.
class TokenBucketLimiter : public RateLimiter
{
public:
    // Sets the bucket capacity and token refill rate.
    TokenBucketLimiter(int64_t capacity, double refill_rate_per_sec);

    // Refills and consumes tokens atomically for the given client.
    RateLimitResult allow(const std::string &client_id, int64_t cost = 1) override;

private:
    // Stores the maximum number of tokens the bucket can hold.
    int64_t capacity_;

    // Stores the number of tokens added per second.
    double refill_rate_per_sec_;

    // Caches the SHA returned when the Lua script is loaded into Redis.
    std::string script_sha_;

    // Loads the atomic token-bucket script into Redis.
    void loadLuaScript();
};

#endif