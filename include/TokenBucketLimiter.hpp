#ifndef TOKEN_BUCKET_LIMITER_HPP
#define TOKEN_BUCKET_LIMITER_HPP

#include "RateLimiter.hpp"
#include <string>
#include <cstdint>

class TokenBucketLimiter : public RateLimiter
{
public:
    TokenBucketLimiter(int64_t capacity, double refill_rate_per_sec);

    RateLimitResult allow(const std::string &client_id, int64_t cost = 1) override;

private:
    int64_t capacity_;
    double refill_rate_per_sec_;
    std::string script_sha_;

    void loadLuaScript();
};

#endif