#include <iostream>
#include <string>
#include <thread>
#include "httplib.h"
#include "TokenBucketLimiter.hpp"
#include "SlidingWindowLimiter.hpp"

int main()
{
    try
    {
        std::cout << "[SYSTEM] Booting C++ Distributed Rate Limiter API...\n";

        // Initialize limiters
        TokenBucketLimiter tb_limiter(100, 20.0); // 100 max capacity, 20 tokens/sec refill

        httplib::Server svr;

        // Health check endpoint
        svr.Get("/health", [](const httplib::Request &, httplib::Response &res)
                { res.set_content("{\"status\": \"healthy\"}", "application/json"); });

        // Core Rate Limiting API Endpoint
        svr.Get("/api/limit", [&](const httplib::Request &req, httplib::Response &res)
                {
            // Extract client ID from query params (e.g., ?client_id=192.168.1.5)
            std::string client_id = req.has_param("client_id") ? req.get_param_value("client_id") : "anonymous";
            
            // Evaluate the request atomically in Redis
            RateLimitResult result = tb_limiter.allow(client_id, 1);
            
            // Attach telemetry headers
            res.set_header("X-RateLimit-Remaining", std::to_string(result.remaining_quota));
            res.set_header("X-RateLimit-Fallback", result.is_fallback ? "true" : "false");
            
            if (result.allowed) {
                res.status = 200;
                res.set_content("{\"status\": \"Allowed\", \"client\": \"" + client_id + "\"}", "application/json");
            } else {
                res.status = 429;
                res.set_header("Retry-After", std::to_string(result.retry_after_ms / 1000));
                res.set_content("{\"error\": \"Too Many Requests\", \"client\": \"" + client_id + "\"}", "application/json");
            } });

        std::cout << "[SYSTEM] Engine listening for HTTP traffic on 0.0.0.0:8080\n";
        // Block and listen on all network interfaces
        svr.listen("0.0.0.0", 8080);
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FATAL ERROR] " << e.what() << "\n";
        return 1;
    }

    return 0;
}