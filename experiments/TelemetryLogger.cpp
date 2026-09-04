#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class RedisTelemetryLimiter
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
    RedisTelemetryLimiter() : connect_socket(INVALID_SOCKET), script_sha("") {}
    ~RedisTelemetryLimiter()
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
        // TELEMETRY LUA SCRIPT: Core token bucket evaluation combined with an LPUSH message stream
        std::string lua_script =
            "local rate_key = KEYS[1] "
            "local queue_key = KEYS[2] "

            "local client_id = ARGV[1] "
            "local now = tonumber(ARGV[2]) "
            "local max_capacity = tonumber(ARGV[3]) "
            "local refill_rate = tonumber(ARGV[4]) "

            "local data = redis.call('HMGET', rate_key, 'tokens', 'last_updated') "
            "local tokens = tonumber(data[1]) "
            "local last_updated = tonumber(data[2]) "

            "if not tokens then "
            "    tokens = max_capacity "
            "    last_updated = now "
            "else "
            "    local elapsed = math.max(0, now - last_updated) "
            "    tokens = math.min(max_capacity, tokens + (elapsed * refill_rate)) "
            "end "

            "local status = 'REJECTED' "
            "local return_code = 0 "

            "if tokens >= 1.0 then "
            "    tokens = tokens - 1.0 "
            "    status = 'ALLOWED' "
            "    return_code = 1 "
            "end "

            "redis.call('HMSET', rate_key, 'tokens', tokens, 'last_updated', now) "
            "redis.call('EXPIRE', rate_key, 60) "

            "local log_entry = now .. ',' .. client_id .. ',' .. status "
            "redis.call('LPUSH', queue_key, log_entry) "
            "redis.call('LTRIM', queue_key, 0, 999) " // Hard limit log length to 1000 items to avoid RAM leaks

            "return return_code";

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

    bool allow_request(const std::string &client_id, double capacity, double refill_rate)
    {
        auto now = std::chrono::steady_clock::now();
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        // Pass 2 Keys: [1] User profile metrics, [2] Centralized metric queue pipe
        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "2", "rate:tel:" + client_id, "telemetry:queue",
            client_id, std::to_string(current_time_seconds), std::to_string(capacity), std::to_string(refill_rate)};
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

    // Diagnostic tool simulating our out-of-band metrics processing pipeline
    void print_latest_telemetry_stream()
    {
        std::vector<std::string> cmd = {"RPOP", "telemetry:queue"};
        std::string payload = serialize_to_resp_array(cmd);
        send(connect_socket, payload.c_str(), payload.length(), 0);

        char buffer[256] = {0};
        int bytes = recv(connect_socket, buffer, 255, 0);
        if (bytes > 0)
        {
            std::string resp(buffer, bytes);
            std::string parsed_log = parse_resp_bulk_string(resp);
            if (!parsed_log.empty() && parsed_log.find("ERROR") == std::string::npos)
            {
                std::cout << " [ASYNC CONSUMER POOL] Log Parsed: " << parsed_log << std::endl;
            }
            else
            {
                std::cout << " [ASYNC CONSUMER POOL] Log Queue Empty." << std::endl;
            }
        }
    }
};

int main()
{
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    RedisTelemetryLimiter limiter;
    if (!limiter.connect_to_redis("127.0.0.1", 6379) || !limiter.bootstrap_script())
    {
        std::cerr << "Initialization error." << std::endl;
        return 1;
    }

    std::cout << "--- Initializing Operational API Traffic Generation Pipeline ---" << std::endl;

    // Simulate runtime activity spikes
    limiter.allow_request("user_grace", 2.0, 0.1);
    limiter.allow_request("user_grace", 2.0, 0.1);
    limiter.allow_request("user_grace", 2.0, 0.1); // Throttled hit
    limiter.allow_request("user_kevin", 5.0, 1.0);

    std::cout << "API requests handled. Core runtime execution logic unblocked instantly." << std::endl;

    std::cout << "\n--- Activating Isolated Background Consumer Processing Daemon ---" << std::endl;
    // Drain the metric broker collection lines back-to-front
    limiter.print_latest_telemetry_stream();
    limiter.print_latest_telemetry_stream();
    limiter.print_latest_telemetry_stream();
    limiter.print_latest_telemetry_stream();
    limiter.print_latest_telemetry_stream();

    WSACleanup();
    return 0;
}