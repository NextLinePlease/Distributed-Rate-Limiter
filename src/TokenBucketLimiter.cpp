#include "TokenBucketLimiter.hpp"
#include "RedisManager.hpp"
#include <chrono>
#include <vector>
#include <cmath>

const std::string TOKEN_BUCKET_LUA = R"(
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_rate = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])
    local requested = tonumber(ARGV[4])

    local data = redis.call("HMGET", key, "tokens", "last_refreshed")
    local tokens = tonumber(data[1])
    local last_refreshed = tonumber(data[2])

    if tokens == nil then
        tokens = capacity
        last_refreshed = now
    else
        local delta = math.max(0, now - last_refreshed)
        local refilled_tokens = delta * refill_rate
        tokens = math.min(capacity, tokens + refilled_tokens)
        last_refreshed = now
    end

    local allowed = 0
    if tokens >= requested then
        tokens = tokens - requested
        allowed = 1
    end

    redis.call("HMSET", key, "tokens", tokens, "last_refreshed", last_refreshed)
    
    local ttl = math.ceil(capacity / refill_rate) * 2
    redis.call("EXPIRE", key, ttl)

    return { allowed, math.floor(tokens) }
)";

TokenBucketLimiter::TokenBucketLimiter(int64_t capacity, double refill_rate_per_sec)
    : capacity_(capacity), refill_rate_per_sec_(refill_rate_per_sec)
{
    loadLuaScript();
}

void TokenBucketLimiter::loadLuaScript()
{
    auto redis = RedisManager::getInstance().getClient();
    script_sha_ = redis->script_load(TOKEN_BUCKET_LUA);
}

RateLimitResult TokenBucketLimiter::allow(const std::string &client_id, int64_t cost)
{
    RateLimitResult res;
    auto &redis_manager = RedisManager::getInstance();

    if (redis_manager.shouldUseFallback() || !redis_manager.getClient())
    {
        res.is_fallback = true;
        res.allowed = true;
        res.remaining_quota = capacity_;
        res.retry_after_ms = 0;
        return res;
    }

    try
    {
        auto redis = redis_manager.getClient();

        auto now_chrono = std::chrono::system_clock::now();
        double now = std::chrono::duration<double>(now_chrono.time_since_epoch()).count();

        std::string key = "rl:tb:" + client_id;
        std::vector<std::string> keys = {key};
        std::vector<std::string> args = {
            std::to_string(capacity_),
            std::to_string(refill_rate_per_sec_),
            std::to_string(now),
            std::to_string(cost)};

        std::vector<long long> result;
        redis->evalsha(script_sha_, keys.begin(), keys.end(), args.begin(), args.end(), std::back_inserter(result));

        res.allowed = (result[0] == 1);
        res.remaining_quota = result[1];
        res.retry_after_ms = res.allowed ? 0 : static_cast<int64_t>(std::max(1.0, 1000.0 / refill_rate_per_sec_));
        redis_manager.reportSuccess();
        return res;
    }
    catch (const sw::redis::Error &)
    {
        redis_manager.reportFailure();
        res.is_fallback = true;
        res.allowed = true;
        res.remaining_quota = capacity_;
        res.retry_after_ms = 0;
        return res;
    }
}