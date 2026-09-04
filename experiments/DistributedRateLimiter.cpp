#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <winsock2.h>

#pragma comment(lib, "ws2_32.lib")

class OptimizedDistributedLimiter
{
private:
    SOCKET connect_socket;
    std::string script_sha; // Cached SHA-1 fingerprint

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

    // Helper to extract raw data out of a RESP Bulk String response ($40\r\n<sha>\r\n)
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
    OptimizedDistributedLimiter() : connect_socket(INVALID_SOCKET), script_sha("") {}

    bool connect_to_redis(const std::string &ip, int port)
    {
        WSADATA wsa_data;
        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0)
            return false;

        connect_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connect_socket == INVALID_SOCKET)
        {
            WSACleanup();
            return false;
        }

        sockaddr_in target;
        target.sin_family = AF_INET;
        target.sin_port = htons(port);
        target.sin_addr.s_addr = inet_addr(ip.c_str());

        if (connect(connect_socket, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
        {
            closesocket(connect_socket);
            WSACleanup();
            return false;
        }
        return true;
    }

    // STEP 1: Pre-load the script into Redis storage memory at system initialization
    bool bootstrap_script()
    {
        std::string lua_script =
            "local key = KEYS[1] "
            "local max_capacity = tonumber(ARGV[1]) "
            "local refill_rate = tonumber(ARGV[2]) "
            "local now = tonumber(ARGV[3]) "
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

        send(connect_socket, payload.c_str(), payload.length(), 0);

        char recv_buffer[512] = {0};
        int bytes = recv(connect_socket, recv_buffer, 511, 0);

        if (bytes > 0)
        {
            script_sha = parse_resp_bulk_string(std::string(recv_buffer, bytes));
            if (!script_sha.empty())
            {
                std::cout << "[BOOTSTRAP] Script cached. SHA-1 Registration: " << script_sha << std::endl;
                return true;
            }
        }
        return false;
    }

    // STEP 2: Execute traffic validation calls ultra-fast using EVALSHA
    bool allow_request(const std::string &client_id, double capacity, double refill_rate)
    {
        if (script_sha.empty())
            return false;

        auto now = std::chrono::steady_clock::now();
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                             now.time_since_epoch())
                                             .count();

        // Pass the 40-character hash identifier instead of the raw code text block
        std::vector<std::string> command_args = {
            "EVALSHA",
            script_sha,
            "1",
            "rate:" + client_id,
            std::to_string(capacity),
            std::to_string(refill_rate),
            std::to_string(current_time_seconds)};

        std::string resp_payload = serialize_to_resp_array(command_args);
        send(connect_socket, resp_payload.c_str(), resp_payload.length(), 0);

        char recv_buffer[256] = {0};
        int bytes = recv(connect_socket, recv_buffer, 255, 0);

        if (bytes > 0)
        {
            std::string response(recv_buffer, bytes);
            if (response.find(":1") != std::string::npos)
                return true;
        }
        return false;
    }

    ~OptimizedDistributedLimiter()
    {
        if (connect_socket != INVALID_SOCKET)
        {
            closesocket(connect_socket);
            WSACleanup();
        }
    }
};

int main()
{
    OptimizedDistributedLimiter limiter;

    if (!limiter.connect_to_redis("127.0.0.1", 6379))
    {
        std::cerr << "Database link failure." << std::endl;
        return 1;
    }

    // Register script fingerprint prior to entering active traffic run loop
    if (!limiter.bootstrap_script())
    {
        std::cerr << "Failed to register script cache fingerprint inside Redis." << std::endl;
        return 1;
    }

    std::cout << "\n--- Driving High-Performance EVALSHA Validation Pipes ---" << std::endl;
    std::cout << "Req 1 (Bob): " << (limiter.allow_request("bob", 2.0, 0.5) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Req 2 (Bob): " << (limiter.allow_request("bob", 2.0, 0.5) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Req 3 (Bob): " << (limiter.allow_request("bob", 2.0, 0.5) ? "ALLOWED" : "REJECTED") << std::endl;

    return 0;
}