#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class RedisConnection
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
    RedisConnection() : connect_socket(INVALID_SOCKET), script_sha("") {}
    ~RedisConnection()
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

    // Helper to seed dynamic configurations into Redis hashes directly from the app
    void seed_tier_config(const std::string &tier, const std::string &cap, const std::string &rate)
    {
        std::vector<std::string> cmd = {"HMSET", "cfg:" + tier, "capacity", cap, "refill_rate", rate};
        std::string payload = serialize_to_resp_array(cmd);
        send(connect_socket, payload.c_str(), payload.length(), 0);
        char buffer[128] = {0};
        recv(connect_socket, buffer, 127, 0); // Clear network buffer response
    }

    bool bootstrap_script()
    {
        // UPGRADED LUA SCRIPT: Fetches rules dynamically using KEYS[2]
        std::string lua_script =
            "local user_key = KEYS[1] "
            "local config_key = KEYS[2] "

            // Fetch rule criteria directly from Redis configuration hash
            "local cfg = redis.call('HMGET', config_key, 'capacity', 'refill_rate') "
            "local max_capacity = tonumber(cfg[1]) "
            "local refill_rate = tonumber(cfg[2]) "

            // Hardcoded safety fallbacks if config key missing
            "if not max_capacity then max_capacity = 2.0 end "
            "if not refill_rate then refill_rate = 0.1 end "

            "local now = tonumber(ARGV[1]) "
            "local data = redis.call('HMGET', user_key, 'tokens', 'last_updated') "
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
            "    redis.call('HMSET', user_key, 'tokens', tokens - 1.0, 'last_updated', now) "
            "    redis.call('EXPIRE', user_key, 60) "
            "    return 1 "
            "else "
            "    redis.call('HMSET', user_key, 'tokens', tokens, 'last_updated', now) "
            "    return 0 "
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
    bool allow_request(const std::string &client_id, const std::string &tier)
    {
        auto now = std::chrono::steady_clock::now();
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        // Pass 2 Keys: [1] The user state key, [2] The centralized configuration rule key
        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "2", "rate:" + client_id, "cfg:" + tier,
            std::to_string(current_time_seconds)};
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

    RedisConnection conn;
    if (!conn.connect_to_redis("127.0.0.1", 6379) || !conn.bootstrap_script())
    {
        std::cerr << "Initialization Error." << std::endl;
        return 1;
    }

    std::cout << "--- Initializing Centralized Rules inside Redis ---" << std::endl;
    // Free tier: capacity 2, refill 0.1/s
    conn.seed_tier_config("free", "2.0", "0.1");
    std::cout << "[SYSTEM] Config rules injected into Redis server cluster." << std::endl;

    std::cout << "\n--- Evaluating Traffic via Centralized Policies ---" << std::endl;
    std::string user_id = "user_4910";

    std::cout << "Request 1 (Free Tier): " << (conn.allow_request(user_id, "free") ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Request 2 (Free Tier): " << (conn.allow_request(user_id, "free") ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Request 3 (Free Tier): " << (conn.allow_request(user_id, "free") ? "ALLOWED" : "REJECTED") << std::endl; // Blocked

    // LIVE SIMULATION: Change rule directly in database without changing or compiling a single line of app code
    std::cout << "\n--- Admin Dynamically Updates Rules Globally inside Redis ---" << std::endl;
    conn.seed_tier_config("free", "5.0", "1.0"); // Upgrade free tier capacity to 5 instantly

    std::cout << "Request 4 (Free Tier - Post Change): " << (conn.allow_request(user_id, "free") ? "ALLOWED" : "REJECTED") << std::endl; // ALLOWED!

    WSACleanup();
    return 0;
}