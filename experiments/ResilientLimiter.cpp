#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class ResilientRateLimiter
{
private:
    SOCKET connect_socket;
    std::string script_sha;
    bool is_redis_healthy;

    // Local fallback state tracker (Day 1-5 style memory fallback)
    std::unordered_map<std::string, int> local_fallback_cache;
    std::chrono::steady_clock::time_point last_fallback_reset;

    std::string build_resp_bulk_string(const std::string &str)
    {
        return "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
    }

    std::string serialize_to_resp_array(const std::vector<std::string> &args)
    {
        std::string payload = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto &arg : args)
        {
            payload += build_resp_bulk_string(arg);
        }
        return payload;
    }

    std::string parse_resp_bulk_string(const std::string &resp)
    {
        if (resp.empty() || resp[0] != '$')
            return "";
        size_t first_nl = resp.find("\r\n");
        if (first_nl == std::string::npos)
            return "";
        size_t second_nl = resp.find("\r\n", first_nl + 2);
        if (second_nl == std::string::npos)
            return "";
        return resp.substr(first_nl + 2, second_nl - (first_nl + 2));
    }

    // Local basic fixed-window validation logic used during catastrophic outages
    bool handle_local_fallback(const std::string &client_id, int local_limit)
    {
        auto now = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed = now - last_fallback_reset;

        // Wipe local memory window tracking every 10 seconds
        if (elapsed.count() > 10.0)
        {
            local_fallback_cache.clear();
            last_fallback_reset = now;
        }

        local_fallback_cache[client_id]++;
        if (local_fallback_cache[client_id] <= local_limit)
        {
            std::cout << " [CIRCUIT BREAKER DEGRADATION] Local Cache -> ALLOWED ("
                      << local_fallback_cache[client_id] << "/" << local_limit << ")" << std::endl;
            return true;
        }
        std::cout << " [CIRCUIT BREAKER DEGRADATION] Local Cache -> REJECTED (429)" << std::endl;
        return false;
    }

public:
    ResilientRateLimiter() : connect_socket(INVALID_SOCKET), script_sha(""), is_redis_healthy(false)
    {
        last_fallback_reset = std::chrono::steady_clock::now();
    }

    ~ResilientRateLimiter()
    {
        if (connect_socket != INVALID_SOCKET)
            closesocket(connect_socket);
    }

    bool connect_to_redis(const std::string &ip, int port)
    {
        connect_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connect_socket == INVALID_SOCKET)
            return false;

        sockaddr_in target;
        target.sin_family = AF_INET;
        target.sin_port = htons(port);
        target.sin_addr.s_addr = inet_addr(ip.c_str());

        if (connect(connect_socket, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
        {
            closesocket(connect_socket);
            connect_socket = INVALID_SOCKET;
            is_redis_healthy = false;
            return false;
        }
        is_redis_healthy = true;
        return true;
    }

    bool bootstrap_script()
    {
        if (!is_redis_healthy)
            return false;

        std::string lua_script =
            "local key = KEYS[1] "
            "local now = tonumber(ARGV[1]) "
            "local max_capacity = tonumber(ARGV[2]) "
            "local refill_rate = tonumber(ARGV[3]) "
            "local data = redis.call('HMGET', key, 'tokens', 'last_updated') "
            "local tokens = tonumber(data[1]) "
            "local last_updated = tonumber(data[2]) "
            "if not tokens then "
            "    tokens = max_capacity "
            "    last_updated = now "
            "else "
            "    local elapsed = math.max(0, now - last_updated) "
            "    tokens = math.min(max_capacity, tokens + (elapsed * refill_rate)) "
            "end "
            "if tokens >= 1.0 then "
            "    redis.call('HMSET', key, 'tokens', tokens - 1.0, 'last_updated', now) "
            "    redis.call('EXPIRE', key, 60) "
            "    return 1 "
            "else "
            "    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', now) "
            "    return 0 "
            "end";

        std::vector<std::string> load_command = {"SCRIPT", "LOAD", lua_script};
        std::string payload = serialize_to_resp_array(load_command);

        if (send(connect_socket, payload.c_str(), payload.length(), 0) == SOCKET_ERROR)
        {
            is_redis_healthy = false;
            return false;
        }

        char recv_buffer[512] = {0};
        int bytes = recv(connect_socket, recv_buffer, 511, 0);
        if (bytes > 0)
        {
            script_sha = parse_resp_bulk_string(std::string(recv_buffer, bytes));
            return !script_sha.empty();
        }
        is_redis_healthy = false;
        return false;
    }

    bool allow_request(const std::string &client_id, double capacity, double refill_rate, int local_fallback_limit)
    {
        // STEP 1: Route immediately to fallback tracker if we already know Redis is dead
        if (!is_redis_healthy)
        {
            return handle_local_fallback(client_id, local_fallback_limit);
        }

        auto now = std::chrono::steady_clock::now();
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "1", "rate:res:" + client_id,
            std::to_string(capacity), std::to_string(refill_rate), std::to_string(current_time_seconds)};
        std::string resp_payload = serialize_to_resp_array(command_args);

        // STEP 2: Wrap network writes/reads in proactive connection safety boundaries
        int send_status = send(connect_socket, resp_payload.c_str(), resp_payload.length(), 0);
        if (send_status == SOCKET_ERROR)
        {
            std::cout << "[CRITICAL WARNING] Redis Write Pipeline Broken. Tripping Circuit Breaker..." << std::endl;
            is_redis_healthy = false;
            return handle_local_fallback(client_id, local_fallback_limit);
        }

        char recv_buffer[256] = {0};
        int bytes = recv(connect_socket, recv_buffer, 255, 0);
        if (bytes <= 0)
        {
            std::cout << "[CRITICAL WARNING] Redis Read Pipeline Timeout/Dropped. Tripping Circuit Breaker..." << std::endl;
            is_redis_healthy = false;
            return handle_local_fallback(client_id, local_fallback_limit);
        }

        std::string response(recv_buffer, bytes);
        return (response.find(":1") != std::string::npos);
    }

    // Diagnostic tool to forcefully break the network pipe mid-execution
    void force_network_outage()
    {
        if (connect_socket != INVALID_SOCKET)
        {
            closesocket(connect_socket);
            connect_socket = INVALID_SOCKET;
            // We intentionally leave is_redis_healthy true here so the next call
            // tries to hit the socket, fails, catches it dynamically, and degrades.
            std::cout << "\n--- !!! SIMULATING CATASTROPHIC NETWORK INFRASTRUCTURE OUTAGE !!! ---" << std::endl;
        }
    }
};

int main()
{
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    ResilientRateLimiter limiter;

    std::cout << "--- Initializing System Rate Limiter Pipeline ---" << std::endl;
    if (!limiter.connect_to_redis("127.0.0.1", 6379) || !limiter.bootstrap_script())
    {
        std::cout << "[ALERT] Central database unreachable at startup. Booting straight into degraded mode." << std::endl;
    }
    else
    {
        std::cout << "[SUCCESS] Primary Redis connections fully established and active.\n"
                  << std::endl;
    }

    // Normal operation run path
    std::cout << "Call 1 (Primary Redis): " << (limiter.allow_request("marcus", 2.0, 0.1, 2) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 2 (Primary Redis): " << (limiter.allow_request("marcus", 2.0, 0.1, 2) ? "ALLOWED" : "REJECTED") << std::endl;

    // Trigger simulation outage mid-stream
    limiter.force_network_outage();

    // The system catches the error dynamically, skips crashing, and falls back to safe local constraints
    std::cout << "Call 3 (Outage Occurred): ";
    limiter.allow_request("marcus", 2.0, 0.1, 2);
    std::cout << "Call 4 (Outage Ongoing):  ";
    limiter.allow_request("marcus", 2.0, 0.1, 2);
    std::cout << "Call 5 (Outage Ongoing):  ";
    limiter.allow_request("marcus", 2.0, 0.1, 2); // Exceeds loose local limits

    WSACleanup();
    return 0;
}