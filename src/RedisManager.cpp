#include "RedisManager.hpp"
#include <iostream>
#include <cstdlib>
#include <stdexcept>

RedisManager::RedisManager()
{
    sw::redis::ConnectionOptions connection_options;
    const char *env_host = std::getenv("REDIS_HOST");
    connection_options.host = env_host ? env_host : "redis";
    connection_options.port = std::getenv("REDIS_PORT") ? std::stoi(std::getenv("REDIS_PORT")) : 6379;
    connection_options.socket_timeout = std::chrono::milliseconds(250);
    connection_options.connect_timeout = std::chrono::milliseconds(250);

    sw::redis::ConnectionPoolOptions pool_options;
    pool_options.size = 64;
    pool_options.wait_timeout = std::chrono::milliseconds(100);

    try
    {
        redis_client_ = std::make_shared<sw::redis::Redis>(connection_options, pool_options);
        if (redis_client_->ping() != "PONG")
        {
            throw std::runtime_error("Redis ping failed");
        }
        std::cout << "[INFO] Successfully connected to Redis pool at " << connection_options.host << ":" << connection_options.port << ".\n";
        reportSuccess();
    }
    catch (const std::exception &e)
    {
        std::cerr << "[WARN] Redis unavailable at startup; entering degraded fallback mode. " << e.what() << "\n";
        state_.store(CircuitState::OPEN, std::memory_order_release);
        redis_client_ = nullptr;
    }
}

RedisManager &RedisManager::getInstance()
{
    static RedisManager instance;
    return instance;
}

std::shared_ptr<sw::redis::Redis> RedisManager::getClient()
{
    return redis_client_;
}