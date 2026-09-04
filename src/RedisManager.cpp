#include "RedisManager.hpp"
#include <iostream>
#include <cstdlib>
#include <stdexcept>

RedisManager::RedisManager()
{
    sw::redis::ConnectionOptions connection_options;
    const char *env_host = std::getenv("REDIS_HOST");
    connection_options.host = env_host ? env_host : "redis";
    connection_options.port = 6379;
    connection_options.socket_timeout = std::chrono::milliseconds(100);

    sw::redis::ConnectionPoolOptions pool_options;
    pool_options.size = 64;
    pool_options.wait_timeout = std::chrono::milliseconds(50);

    try
    {
        redis_client_ = std::make_shared<sw::redis::Redis>(connection_options, pool_options);
        if (redis_client_->ping() != "PONG")
        {
            throw std::runtime_error("Redis ping failed");
        }
        std::cout << "[INFO] Successfully connected to Redis pool.\n";
    }
    catch (const sw::redis::Error &e)
    {
        std::cerr << "[FATAL] Failed to connect to Redis: " << e.what() << "\n";
        throw;
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