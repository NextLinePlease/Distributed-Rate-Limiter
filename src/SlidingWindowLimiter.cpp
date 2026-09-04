#include "SlidingWindowLimiter.hpp"
#include "RedisManager.hpp"
#include <chrono>
#include <vector>

const std::string SLIDING_WINDOW_LUA = R"(
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    local member = ARGV[4]

    local clear_before = now - window

    redis.call("ZREMRANGEBYSCORE", key, "-inf", clear_before)
    local current_requests = redis.call("ZCARD", key)

    if current_requests < limit then
        redis.call("ZADD", key, now, member)
        redis.call("PEXPIRE", key, window)
        return { 1, limit - current_requests - 1 }
    else
        return { 0, 0 }
    end
)";

SlidingWindowLimiter::SlidingWindowLimiter(int64_t window_size_ms, int64_t max_requests)
    : window_size_ms_(window_size_ms), max_requests_(max_requests)
{
    loadLuaScript();
}

void SlidingWindowLimiter::loadLuaScript()
{
    auto redis = RedisManager::getInstance().getClient();
    script_sha_ = redis->script_load(SLIDING_WINDOW_LUA);
}

RateLimitResult SlidingWindowLimiter::allow(const std::string &client_id, int64_t cost)
{
    (void)cost;
    auto redis = RedisManager::getInstance().getClient();

    auto now_chrono = std::chrono::system_clock::now();
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         now_chrono.time_since_epoch())
                         .count();

    std::string key = "rl:swl:" + client_id;
    std::string member = std::to_string(now_ms) + "-" + client_id;

    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {
        std::to_string(now_ms),
        std::to_string(window_size_ms_),
        std::to_string(max_requests_),
        member};

    std::vector<long long> result;
    redis->evalsha(script_sha_, keys.begin(), keys.end(), args.begin(), args.end(), std::back_inserter(result));

    RateLimitResult res;
    res.allowed = (result[0] == 1);
    res.remaining_quota = result[1];
    res.retry_after_ms = res.allowed ? 0 : window_size_ms_;
    return res;
}