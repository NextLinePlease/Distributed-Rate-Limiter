#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

enum CircuitState
{
    CLOSED,
    OPEN,
    HALF_OPEN
};

class SelfHealingRateLimiter
{
private:
    SOCKET connect_socket;
    std::string script_sha;
    std::string redis_ip;
    int redis_port;

    // State Machine Controls
    CircuitState current_state;
    std::chrono::steady_clock::time_point state_changed_time;
    const double cooldown_duration_sec = 4.0; // Wait 4 seconds before testing recovery

    // Local Fallback Storage
    std::unordered_map<std::string, int> local_cache;

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

    void shift_state(CircuitState new_state, const std::string &reason)
    {
        current_state = new_state;
        state_changed_time = std::chrono::steady_clock::now();
        std::cout << "[STATE TRANSITION] Circuit shifted to ";
        if (new_state == CLOSED)
            std::cout << "CLOSED (Redis Active)";
        else if (new_state == OPEN)
            std::cout << "OPEN (Degraded Local Map)";
        else if (new_state == HALF_OPEN)
            std::cout << "HALF-OPEN (Canary Test)";
        std::cout << " | Reason: " << reason << std::endl;
    }

    bool execute_network_handshake()
    {
        if (connect_socket != INVALID_SOCKET)
        {
            closesocket(connect_socket);
            connect_socket = INVALID_SOCKET;
        }

        connect_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connect_socket == INVALID_SOCKET)
            return false;

        sockaddr_in target;
        target.sin_family = AF_INET;
        target.sin_port = htons(redis_port);
        target.sin_addr.s_addr = inet_addr(redis_ip.c_str());

        // Set a strict 500ms timeout on network interactions so we don't hang application threads
        int timeout_ms = 500;
        setsockopt(connect_socket, SOL_SOCKET, SO_SNDTIMEO, (const char *)&timeout_ms, sizeof(timeout_ms));
        setsockopt(connect_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout_ms, sizeof(timeout_ms));

        if (connect(connect_socket, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
        {
            closesocket(connect_socket);
            connect_socket = INVALID_SOCKET;
            return false;
        }
        return true;
    }

public:
    SelfHealingRateLimiter(std::string ip, int port)
        : connect_socket(INVALID_SOCKET), redis_ip(ip), redis_port(port), current_state(OPEN)
    {
        state_changed_time = std::chrono::steady_clock::now();
    }

    ~SelfHealingRateLimiter()
    {
        if (connect_socket != INVALID_SOCKET)
            closesocket(connect_socket);
    }

    bool bootstrap_script()
    {
        if (connect_socket == INVALID_SOCKET && !execute_network_handshake())
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
            return false;

        char recv_buffer[512] = {0};
        int bytes = recv(connect_socket, recv_buffer, 511, 0);
        if (bytes > 0)
        {
            script_sha = parse_resp_bulk_string(std::string(recv_buffer, bytes));
            if (!script_sha.empty())
            {
                shift_state(CLOSED, "System Initialization Successful.");
                return true;
            }
        }
        return false;
    }

    bool allow_request(const std::string &client_id, double capacity, double refill_rate)
    {
        auto now = std::chrono::steady_clock::now();

        // Evaluates State Transitions
        if (current_state == OPEN)
        {
            std::chrono::duration<double> elapsed_in_outage = now - state_changed_time;
            if (elapsed_in_outage.count() > cooldown_duration_sec)
            {
                shift_state(HALF_OPEN, "Cooldown expired. Initiating system network probe.");
            }
        }

        // Execution Branches
        if (current_state == OPEN)
        {
            // Hard bypass network logic, serve directly from emergency tracking limits
            local_cache[client_id]++;
            std::cout << "[TRAFFIC ROUTE: FALLBACK] Serving request via local cache context." << std::endl;
            return (local_cache[client_id] <= 5);
        }

        // Handle Live Database Testing or Regular Mode Actions
        long long current_time_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        std::vector<std::string> command_args = {
            "EVALSHA", script_sha, "1", "rate:chaos:" + client_id,
            std::to_string(capacity), std::to_string(refill_rate), std::to_string(current_time_seconds)};
        std::string resp_payload = serialize_to_resp_array(command_args);

        // Attempt Network Operation
        bool network_success = false;
        bool request_allowed = false;

        if (connect_socket != INVALID_SOCKET || execute_network_handshake())
        {
            int send_status = send(connect_socket, resp_payload.c_str(), resp_payload.length(), 0);
            if (send_status != SOCKET_ERROR)
            {
                char recv_buffer[256] = {0};
                int bytes = recv(connect_socket, recv_buffer, 255, 0);
                if (bytes > 0)
                {
                    network_success = true;
                    std::string response(recv_buffer, bytes);
                    request_allowed = (response.find(":1") != std::string::npos);
                }
            }
        }

        if (network_success)
        {
            if (current_state == HALF_OPEN)
            {
                shift_state(CLOSED, "Canary check passed cleanly. Core database verified healthy.");
            }
            std::cout << "[TRAFFIC ROUTE: REDIS] Request evaluated on centralized cluster cluster." << std::endl;
            return request_allowed;
        }
        else
        {
            // Network Call Blew Up!
            if (current_state == CLOSED)
            {
                shift_state(OPEN, "Detected abrupt network failure or transaction drop.");
            }
            else if (current_state == HALF_OPEN)
            {
                shift_state(OPEN, "Canary probe transaction crashed. Database remains offline.");
            }

            // Immediately execute fallback logic on this failed request to preserve user availability
            local_cache[client_id]++;
            std::cout << "[TRAFFIC ROUTE: FALLBACK EMERGENCE] Network transaction dropped; recovered into fallback." << std::endl;
            return (local_cache[client_id] <= 5);
        }
    }

    void simulate_manual_network_sever()
    {
        if (connect_socket != INVALID_SOCKET)
        {
            closesocket(connect_socket);
            connect_socket = INVALID_SOCKET;
        }
        redis_port = 9999; // Intentionally scramble the targeting configuration parameters to force connect failures
        std::cout << "\n>>> CHAOS ENGINE INJECTED OUTAGE: Severing Redis Node Paths... <<<" << std::endl;
    }

    void simulate_manual_network_healing()
    {
        redis_port = 6379; // Re-align parameters back to accurate values
        std::cout << "\n>>> CHAOS ENGINE INJECTED HEALING: Restoring Cluster Connectivity Links... <<<" << std::endl;
    }
};

int main()
{
    WSADATA wsa_data;
    WSAStartup(MAKEWORD(2, 2), &wsa_data);

    SelfHealingRateLimiter limiter("127.0.0.1", 6379);

    std::cout << "=== Running Chaos Engineering Integration Harness ===" << std::endl;
    if (!limiter.bootstrap_script())
    {
        std::cerr << "Fatal error setting initial context parameters." << std::endl;
        return 1;
    }

    std::cout << "\n--- Phase 1: Normal System Execution Mode ---" << std::endl;
    limiter.allow_request("victor", 5.0, 1.0);

    // Inject Intermittent Network Outage
    limiter.simulate_manual_network_sever();

    std::cout << "\n--- Phase 2: Evaluating Traffic During Outage Condition ---" << std::endl;
    limiter.allow_request("victor", 5.0, 1.0); // Transmits here, drops, trips circuit breaker instantly
    limiter.allow_request("victor", 5.0, 1.0); // Bypasses network completely, targets fallback directly

    std::cout << "\nSleeping 4.5 seconds to cross the probe cooldown window span..." << std::endl;
    Sleep(4500);

    std::cout << "\n--- Phase 3: Evaluating Traffic in Cooldown Expiration Block ---" << std::endl;
    limiter.allow_request("victor", 5.0, 1.0); // Circuit enters HALF-OPEN, runs test, fails connection, resets to OPEN

    // Restore Infrastructure Environment Back to Operational Status
    limiter.simulate_manual_network_healing();

    std::cout << "\nSleeping another 4.5 seconds for next probe phase rotation..." << std::endl;
    Sleep(4500);

    std::cout << "\n--- Phase 4: Self-Healing Automation Check ---" << std::endl;
    limiter.allow_request("victor", 5.0, 1.0); // Enters HALF-OPEN, passes canary check, shifts to CLOSED, processes request!
    limiter.allow_request("victor", 5.0, 1.0); // Continues flowing along healthy paths smoothly

    WSACleanup();
    return 0;
}