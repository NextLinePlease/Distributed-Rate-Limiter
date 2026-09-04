#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class RedisSlidingWindowLimiter
{
private:
    SOCKET connect_socket;
    std::string script_sha;

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

public:
    RedisSlidingWindowLimiter() : connect_socket(INVALID_SOCKET), script_sha("") {}
    ~RedisSlidingWindowLimiter()
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
            return false;
        }
        return true;
    }

    bool bootstrap_script()
    {
        // ATOMIC LUA SCRIPT: Manages the Sliding Window Log using a Redis ZSET
        std::string lua_script =
            "local key = KEYS[1] "
            "local now = tonumber(ARGV[1]) "
            "local window_ms = tonumber(ARGV[2]) "
            "local limit = tonumber(ARGV[3]) "
            "local clear_before = now - window_ms "

            // Step 1: Evict stale logs outside the current moving window
            "redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before) "

            // Step 2: Fetch the active log volume within the window
            "local current_requests = redis.call('ZCARD', key) "

            // Step 3: Enforce boundary limit checks
            "if current_requests < limit then "
            "    local unique_member = now .. '_' .. tostring(math.random()) "
            "    redis.call('ZADD', key, now, unique_member) "
            "    redis.call('EXPIRE', key, math.ceil(window_ms / 1000)) "
            "    return 1 " // ALLOWED
            "else "
            "    return 0 " // REJECTED
            "end";

        std::vector<std::string> load_command = {"SCRIPT", "LOAD", lua_script};
        std::string payload = serialize_to_resp_array(load_command);
        send(connect_socket, payload.c_str(), payload.length(), 0);

        char recv_buffer[512] = {0};
        int bytes = recv(connect_socket, recv_buffer, 511, 0);
        if (bytes > 0)
        {
            script_sha = parse_resp_bulk_string(std::string(recv_buffer, bytes));
            return !script_sha.empty();
        }
        return false;
    }

    bool allow_request(const std::string &client_id, long long window_ms, int limit)
    {
        auto now = std::chrono::system_clock::now();
        long long current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        now.time_since_epoch())
                                        .count();

        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "1", "rate:sliding:" + client_id,
            std::to_string(current_time_ms), std::to_string(window_ms), std::to_string(limit)};
        std::string resp_payload = serialize_to_resp_array(command_args);

        send(connect_socket, resp_payload.c_str(), resp_payload.length(), 0);

        char recv_buffer[256] = {0};
        int bytes = recv(connect_socket, recv_buffer, 255, 0);
        if (bytes > 0)
        {
            std::string response(recv_buffer, bytes);
            return (response.find(":1") != std::string::npos);
        }
        return false;
    }
};

int main()
{
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    RedisSlidingWindowLimiter limiter;
    if (!limiter.connect_to_redis("127.0.0.1", 6379) || !limiter.bootstrap_script())
    {
        std::cerr << "Failed to initialize Sliding Window Log script inside Redis." << std::endl;
        return 1;
    }

    std::cout << "--- Testing Sliding Window Log (Limit: 3 reqs per 4 seconds) ---" << std::endl;
    std::string user_id = "elena";
    long long window_ms = 4000; // 4-second sliding window
    int limit = 3;

    // Fire 3 immediate sequential requests
    std::cout << "Call 1: " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 2: " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 3: " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;

    // Call 4 breaks the limit inside this window immediately
    std::cout << "Call 4 (Immediate Burst Check): " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;

    std::cout << "\nSleeping for 2.5 seconds..." << std::endl;
    Sleep(2500);

    // After 2.5 seconds, we are still within the 4-second window of the first 3 calls. Call 5 should STILL be rejected.
    std::cout << "Call 5 (Mid-window Check): " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;

    std::cout << "\nSleeping for another 2 seconds (Total > 4 seconds elapsed since start)..." << std::endl;
    Sleep(2000);

    // Now, the initial 3 calls have slid completely out of the active window. Capacity has naturally restored log space.
    std::cout << "Call 6 (Post-window Check): " << (limiter.allow_request(user_id, window_ms, limit) ? "ALLOWED" : "REJECTED") << std::endl;

    WSACleanup();
    return 0;
}