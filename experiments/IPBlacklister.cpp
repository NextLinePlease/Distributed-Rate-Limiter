#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

class RedisSecurityLimiter
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
    RedisSecurityLimiter() : connect_socket(INVALID_SOCKET), script_sha("") {}
    ~RedisSecurityLimiter()
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
        // CLEAN LUA SCRIPT: All inner comments stripped out to avoid C++ syntax conflicts
        std::string lua_script =
            "local rate_key = KEYS[1] "
            "local ban_key = KEYS[2] "
            "local violation_key = KEYS[3] "

            "local now = tonumber(ARGV[1]) "
            "local max_capacity = tonumber(ARGV[2]) "
            "local refill_rate = tonumber(ARGV[3]) "
            "local ban_threshold = tonumber(ARGV[4]) "
            "local ban_duration_sec = tonumber(ARGV[5]) "

            "if redis.call('EXISTS', ban_key) == 1 then "
            "    return -1 "
            "end "

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

            "if tokens >= 1.0 then "
            "    redis.call('HMSET', rate_key, 'tokens', tokens - 1.0, 'last_updated', now) "
            "    redis.call('EXPIRE', rate_key, 60) "
            "    return 1 "
            "else "
            "    redis.call('HMSET', rate_key, 'tokens', tokens, 'last_updated', now) "
            "    local current_violations = redis.call('INCR', violation_key) "
            "    redis.call('EXPIRE', violation_key, 20) "
            "    if current_violations >= ban_threshold then "
            "        redis.call('SET', ban_key, '1') "
            "        redis.call('EXPIRE', ban_key, ban_duration_sec) "
            "        redis.call('DEL', violation_key) "
            "        return -1 "
            "    end "
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

    int evaluate_traffic(const std::string &client_id, double capacity, double refill_rate, int ban_threshold, int ban_sec)
    {
        auto now = std::chrono::steady_clock::now();
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "3",
            "rate:sec:" + client_id, "ban:sec:" + client_id, "violation:sec:" + client_id,
            std::to_string(current_time_seconds), std::to_string(capacity), std::to_string(refill_rate),
            std::to_string(ban_threshold), std::to_string(ban_sec)};
        std::string resp_payload = serialize_to_resp_array(command_args);

        send(connect_socket, resp_payload.c_str(), resp_payload.length(), 0);

        char recv_buffer[256] = {0};
        int bytes = recv(connect_socket, recv_buffer, 255, 0);
        if (bytes > 0)
        {
            std::string response(recv_buffer, bytes);
            if (response.find(":-1") != std::string::npos)
                return -1;
            if (response.find(":1") != std::string::npos)
                return 1;
        }
        return 0;
    }
};

void print_status(int response_code)
{
    if (response_code == 1)
        std::cout << "STATUS: ALLOWED" << std::endl;
    else if (response_code == 0)
        std::cout << "STATUS: REJECTED (429)" << std::endl;
    else if (response_code == -1)
        std::cout << "STATUS: !!! HARD BANNED !!!" << std::endl;
}

int main()
{
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    RedisSecurityLimiter limiter;
    if (!limiter.connect_to_redis("127.0.0.1", 6379) || !limiter.bootstrap_script())
    {
        std::cerr << "Initialization error." << std::endl;
        return 1;
    }

    std::cout << "--- Testing Automated Blacklisting (Capacity: 1, Ban Threshold: 3 Strikes) ---" << std::endl;
    std::string attacker_ip = "192.168.1.50";

    double capacity = 1.0;
    double refill_rate = 0.01;
    int ban_threshold = 3;
    int ban_seconds = 6;

    std::cout << "\n[Hit 1] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));
    std::cout << "[Hit 2] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));
    std::cout << "[Hit 3] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));
    std::cout << "[Hit 4] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));

    std::cout << "[Hit 5] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));

    std::cout << "\nSleeping 7 seconds for ban lifecycle cooldown..." << std::endl;
    Sleep(7000);

    std::cout << "[Hit 6] ";
    print_status(limiter.evaluate_traffic(attacker_ip, capacity, refill_rate, ban_threshold, ban_seconds));

    WSACleanup();
    return 0;
}