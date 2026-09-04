#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class RedisSlidingWindowCounter
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
    RedisSlidingWindowCounter() : connect_socket(INVALID_SOCKET), script_sha("") {}
    ~RedisSlidingWindowCounter()
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
        // ATOMIC LUA SCRIPT: Combines and weights fixed window values dynamically inside Redis
        std::string lua_script =
            "local current_window_key = KEYS[1] "
            "local previous_window_key = KEYS[2] "
            "local weight = tonumber(ARGV[1]) "
            "local limit = tonumber(ARGV[2]) "

            "local current_count = tonumber(redis.call('GET', current_window_key) or '0') "
            "local previous_count = tonumber(redis.call('GET', previous_window_key) or '0') "

            // Fetch sliding approximation estimation safely
            "local estimated_count = (previous_count * weight) + current_count "

            "if estimated_count < limit then "
            "    redis.call('INCR', current_window_key) "
            "    redis.call('EXPIRE', current_window_key, 120) "
            "    return 1 "
            "else "
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

    bool allow_request(const std::string &client_id, int window_size_seconds, int limit)
    {
        auto now = std::chrono::system_clock::now();
        long long now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                    now.time_since_epoch())
                                    .count();

        // Calculate segment IDs based on current Unix epoch time blocks
        long long current_window_id = now_seconds / window_size_seconds;
        long long previous_window_id = current_window_id - 1;

        std::string current_key = "rate:swc:" + client_id + ":" + std::to_string(current_window_id);
        std::string previous_key = "rate:swc:" + client_id + ":" + std::to_string(previous_window_id);

        // Determine proportional weight of the previous window slice
        double time_elapsed_in_current_window = now_seconds % window_size_seconds;
        double previous_window_weight = (window_size_seconds - time_elapsed_in_current_window) / (double)window_size_seconds;

        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "2", current_key, previous_key,
            std::to_string(previous_window_weight), std::to_string(limit)};
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

    RedisSlidingWindowCounter limiter;
    if (!limiter.connect_to_redis("127.0.0.1", 6379) || !limiter.bootstrap_script())
    {
        std::cerr << "Initialization failed." << std::endl;
        return 1;
    }

    std::cout << "--- Testing Sliding Window Counter (Limit: 3 reqs per 10 seconds) ---" << std::endl;
    std::string user_id = "frank";
    int window_size = 10; // 10-second window blocks
    int limit = 3;

    // Fill current window to capacity limit
    std::cout << "Call 1: " << (limiter.allow_request(user_id, window_size, limit) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 2: " << (limiter.allow_request(user_id, window_size, limit) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 3: " << (limiter.allow_request(user_id, window_size, limit) ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Call 4 (Burst Check): " << (limiter.allow_request(user_id, window_size, limit) ? "ALLOWED" : "REJECTED") << std::endl; // Fails

    std::cout << "\nWaiting 5 seconds for partial window rotation..." << std::endl;
    Sleep(5000);

    // Call 5 relies on the weighted math: since time passed, part of the older capacity load scales down smoothly.
    std::cout << "Call 5 (Mid-Window Check): " << (limiter.allow_request(user_id, window_size, limit) ? "ALLOWED" : "REJECTED") << std::endl;

    WSACleanup();
    return 0;
}