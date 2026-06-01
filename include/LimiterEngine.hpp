#ifndef LIMITER_ENGINE_HPP
#define LIMITER_ENGINE_HPP

#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <winsock2.h>
#include "Common.hpp"

#pragma comment(lib, "ws2_32.lib")

class RedisConnection
{
public:
    SOCKET sock;
    std::string token_bucket_sha;
    std::string security_sha;
    std::string telemetry_sha;

    RedisConnection() : sock(INVALID_SOCKET), token_bucket_sha(""), security_sha(""), telemetry_sha("") {}

    ~RedisConnection()
    {
        if (sock != INVALID_SOCKET)
        {
            closesocket(sock);
        }
    }

    bool connect_to_node(const std::string &ip, int port)
    {
        sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sock == INVALID_SOCKET)
            return false;
        sockaddr_in target;
        target.sin_family = AF_INET;
        target.sin_port = htons(port);
        target.sin_addr.s_addr = inet_addr(ip.c_str());
        return (connect(sock, (SOCKADDR *)&target, sizeof(target)) != SOCKET_ERROR);
    }

    void send_resp(const std::vector<std::string> &args)
    {
        std::string payload = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto &arg : args)
        {
            payload += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
        }
        send(sock, payload.c_str(), payload.length(), 0);
    }

    std::string read_resp()
    {
        char buf[1024] = {0};
        int bytes = recv(sock, buf, 1023, 0);
        return (bytes > 0) ? std::string(buf, bytes) : "";
    }

    std::string parse_bulk(const std::string &resp)
    {
        if (resp.empty() || resp[0] != '$')
            return "";
        size_t f = resp.find("\r\n");
        if (f == std::string::npos)
            return "";
        size_t s = resp.find("\r\n", f + 2);
        if (s == std::string::npos)
            return "";
        return resp.substr(f + 2, s - (f + 2));
    }
};

class LimiterEngine
{
private:
    std::string node_ip;
    int node_port;
    int max_pool_size;
    std::vector<RedisConnection *> connection_storage_pool;
    CRITICAL_SECTION pool_lock;

    CircuitState circuit_state;
    std::chrono::steady_clock::time_point state_changed_time;
    std::unordered_map<std::string, ClientProfile> local_cache;

    bool load_scripts(RedisConnection *conn)
    {
        if (!conn)
            return false;

        std::string script_bucket =
            "local user_key = KEYS[1] local config_key = KEYS[2] "
            "local cfg = redis.call('HMGET', config_key, 'capacity', 'refill_rate') "
            "local max_capacity = tonumber(cfg[1]) or 2.0 "
            "local refill_rate = tonumber(cfg[2]) or 0.1 "
            "local now = tonumber(ARGV[1]) "
            "local data = redis.call('HMGET', user_key, 'tokens', 'last_updated') "
            "local tokens = tonumber(data[1]) "
            "local last_updated = tonumber(data[2]) "
            "if not tokens then tokens = max_capacity last_updated = now "
            "else local elapsed = math.max(0, now - last_updated) "
            "tokens = math.min(max_capacity, tokens + (elapsed * refill_rate)) end "
            "if tokens >= 1.0 then "
            "redis.call('HMSET', user_key, 'tokens', tokens - 1.0, 'last_updated', now) "
            "redis.call('EXPIRE', user_key, 60) return 1 "
            "else redis.call('HMSET', user_key, 'tokens', tokens, 'last_updated', now) return 0 end";

        std::string script_security =
            "local r_key = KEYS[1] local b_key = KEYS[2] local v_key = KEYS[3] "
            "local now, cap, rate, thresh, dur = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5]) "
            "if redis.call('EXISTS', b_key) == 1 then return -1 end "
            "local data = redis.call('HMGET', r_key, 'tokens', 'last_updated') "
            "local tokens = tonumber(data[1]) or cap "
            "local last_updated = tonumber(data[2]) or now "
            "local elapsed = math.max(0, now - last_updated) "
            "tokens = math.min(cap, tokens + (elapsed * rate)) "
            "if tokens >= 1.0 then "
            "redis.call('HMSET', r_key, 'tokens', tokens - 1.0, 'last_updated', now) return 1 "
            "else "
            "redis.call('HMSET', r_key, 'tokens', tokens, 'last_updated', now) "
            "local viols = redis.call('INCR', v_key) redis.call('EXPIRE', v_key, 20) "
            "if viols >= thresh then redis.call('SET', b_key, '1') redis.call('EXPIRE', b_key, dur) redis.call('DEL', v_key) return -1 end "
            "return 0 end";

        std::string script_telemetry =
            "local r_key = KEYS[1] local q_key = KEYS[2] "
            "local cid, now, cap, rate = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]) "
            "local data = redis.call('HMGET', r_key, 'tokens', 'last_updated') "
            "local tokens = tonumber(data[1]) or cap "
            "local last_updated = tonumber(data[2]) or now "
            "local elapsed = math.max(0, now - last_updated) "
            "tokens = math.min(cap, tokens + (elapsed * rate)) "
            "local status = 'REJECTED' local ret = 0 "
            "if tokens >= 1.0 then tokens = tokens - 1.0 status = 'ALLOWED' ret = 1 end "
            "redis.call('HMSET', r_key, 'tokens', tokens, 'last_updated', now) "
            "redis.call('LPUSH', q_key, now .. ',' .. cid .. ',' .. status) "
            "redis.call('LTRIM', q_key, 0, 999) return ret";

        conn->send_resp({"SCRIPT", "LOAD", script_bucket});
        conn->token_bucket_sha = conn->parse_bulk(conn->read_resp());

        conn->send_resp({"SCRIPT", "LOAD", script_security});
        conn->security_sha = conn->parse_bulk(conn->read_resp());

        conn->send_resp({"SCRIPT", "LOAD", script_telemetry});
        conn->telemetry_sha = conn->parse_bulk(conn->read_resp());

        return (!conn->token_bucket_sha.empty() && !conn->security_sha.empty() && !conn->telemetry_sha.empty());
    }

    RedisConnection *acquire()
    {
        EnterCriticalSection(&pool_lock);
        if (connection_storage_pool.empty() || circuit_state == OPEN)
        {
            LeaveCriticalSection(&pool_lock);
            return nullptr;
        }
        RedisConnection *conn = connection_storage_pool.back();
        connection_storage_pool.pop_back();
        LeaveCriticalSection(&pool_lock);
        return conn;
    }

    void release(RedisConnection *conn)
    {
        if (!conn)
            return;
        EnterCriticalSection(&pool_lock);
        connection_storage_pool.push_back(conn);
        LeaveCriticalSection(&pool_lock);
    }

public:
    LimiterEngine(std::string redis_ip, int redis_port, int size)
        : node_ip(redis_ip), node_port(redis_port), max_pool_size(size), circuit_state(CLOSED)
    {
        InitializeCriticalSection(&pool_lock);
        state_changed_time = std::chrono::steady_clock::now();

        for (int i = 0; i < max_pool_size; ++i)
        {
            RedisConnection *conn = new RedisConnection();
            if (conn->connect_to_node(node_ip, node_port) && load_scripts(conn))
            {
                connection_storage_pool.push_back(conn);
            }
            else
            {
                delete conn;
            }
        }

        RedisConnection *seed_conn = acquire();
        if (seed_conn)
        {
            seed_conn->send_resp({"HMSET", "cfg:free", "capacity", "2.0", "refill_rate", "0.1"});
            seed_conn->read_resp();
            seed_conn->send_resp({"HMSET", "cfg:vip", "capacity", "10.0", "refill_rate", "5.0"});
            seed_conn->read_resp();
            release(seed_conn);
        }
    }

    ~LimiterEngine()
    {
        EnterCriticalSection(&pool_lock);
        for (size_t i = 0; i < connection_storage_pool.size(); ++i)
        {
            delete connection_storage_pool[i];
        }
        connection_storage_pool.clear();
        LeaveCriticalSection(&pool_lock);
        DeleteCriticalSection(&pool_lock);
    }

    bool allow(const std::string &client_id, const std::string &tier)
    {
        auto now_clock = std::chrono::steady_clock::now();

        if (circuit_state == OPEN)
        {
            std::chrono::duration<double> elapsed = now_clock - state_changed_time;
            if (elapsed.count() > 4.0)
            {
                circuit_state = HALF_OPEN;
                std::cout << "[STATE TRANSITION] Entering HALF-OPEN Canary Probing State..." << std::endl;
            }
            else
            {
                local_cache[client_id].current_tokens++;
                std::cout << "  [ROUTE: LOCAL FALLBACK MAP] Redis offline. Checking transient bounds..." << std::endl;
                return (local_cache[client_id].current_tokens <= 5);
            }
        }

        RedisConnection *conn = acquire();
        if (!conn)
        {
            circuit_state = OPEN;
            state_changed_time = std::chrono::steady_clock::now();
            std::cout << "[CRITICAL FAILURE] Lost contact with cluster nodes! Tripping Circuit Breaker to OPEN..." << std::endl;
            return allow(client_id, tier);
        }

        long long seconds = std::chrono::duration_cast<std::chrono::seconds>(now_clock.time_since_epoch()).count();
        conn->send_resp({"EVALSHA", conn->token_bucket_sha, "2", "rate:" + client_id, "cfg:" + tier, std::to_string(seconds)});

        std::string res = conn->read_resp();
        release(conn);

        if (res.empty())
        {
            circuit_state = OPEN;
            state_changed_time = std::chrono::steady_clock::now();
            std::cout << "[NETWORK EXCEPTION] Read timeout on transaction pipeline!" << std::endl;
            return allow(client_id, tier);
        }

        if (circuit_state == HALF_OPEN)
        {
            circuit_state = CLOSED;
            std::cout << "[STATE TRANSITION] Canary passed! Circuit self-healed back to CLOSED." << std::endl;
        }

        std::cout << "  [ROUTE: CENTRAL DISTRIBUTED REDIS] Evaluating metrics via node scripts..." << std::endl;
        return (res.find(":1") != std::string::npos);
    }

    int evaluate_security(const std::string &ip_addr)
    {
        RedisConnection *conn = acquire();
        if (!conn)
            return 0;

        auto now = std::chrono::steady_clock::now();
        long long secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        conn->send_resp({"EVALSHA", conn->security_sha, "3", "sec:rate:" + ip_addr, "sec:ban:" + ip_addr, "sec:viol:" + ip_addr,
                         std::to_string(secs), "1.0", "0.01", "3", "10"});

        std::string res = conn->read_resp();
        release(conn);

        if (res.find(":-1") != std::string::npos)
            return -1;
        if (res.find(":1") != std::string::npos)
            return 1;
        return 0;
    }

    void drain_telemetry_queue()
    {
        RedisConnection *conn = acquire();
        if (!conn)
            return;

        while (true)
        {
            conn->send_resp({"RPOP", "telemetry:queue"});
            std::string res = conn->read_resp();
            std::string data = conn->parse_bulk(res);
            if (data.empty() || data.find("ERROR") != std::string::npos)
                break;
            std::cout << "   -> [ASYNC TELEM CONSUMER] Stream captured event: " << data << std::endl;
        }
        release(conn);
    }

    void simulate_network_drop()
    {
        EnterCriticalSection(&pool_lock);
        for (size_t i = 0; i < connection_storage_pool.size(); ++i)
        {
            if (connection_storage_pool[i]->sock != INVALID_SOCKET)
            {
                closesocket(connection_storage_pool[i]->sock);
                connection_storage_pool[i]->sock = INVALID_SOCKET;
            }
        }
        std::cout << "\n>>> INJECTING CHAOS: Cutting network cables to Redis cluster... <<<" << std::endl;
        LeaveCriticalSection(&pool_lock);
    }

    void simulate_network_heal()
    {
        std::cout << "\n>>> INJECTING HEALING: Restoring hardware cluster nodes... <<<" << std::endl;
        EnterCriticalSection(&pool_lock);
        for (size_t i = 0; i < connection_storage_pool.size(); ++i)
        {
            delete connection_storage_pool[i];
        }
        connection_storage_pool.clear();

        for (int i = 0; i < max_pool_size; ++i)
        {
            RedisConnection *conn = new RedisConnection();
            if (conn->connect_to_node(node_ip, node_port) && load_scripts(conn))
            {
                connection_storage_pool.push_back(conn);
            }
            else
            {
                delete conn;
            }
        }
        LeaveCriticalSection(&pool_lock);
    }
};

#endif