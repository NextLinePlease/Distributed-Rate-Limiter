#ifndef SLIDING_WINDOW_LIMITER_HPP
#define SLIDING_WINDOW_LIMITER_HPP

#include "RateLimiter.hpp"
#include <string>
#include <cstdint>

class SlidingWindowLimiter : public RateLimiter
{
public:
    SlidingWindowLimiter(int64_t window_size_ms, int64_t max_requests);

    RateLimitResult allow(const std::string &client_id, int64_t cost = 1) override;

private:
    int64_t window_size_ms_;
    int64_t max_requests_;
    std::string script_sha_;

    void loadLuaScript();
};

#endif