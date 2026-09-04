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
    std::string bucket_sha;
    std::string sec_sha;
    std::string telem_sha;

    RedisConnection() : sock(INVALID_SOCKET), bucket_sha(""), sec_sha(""), telem_sha("") {}

    ~RedisConnection()
    {
        if (sock != INVALID_SOCKET)
            closesocket(sock);
    }

    bool connect_to_node(const std::string &ip, int port)
    {
        sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sock == INVALID_SOCKET)
            return false;
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(ip.c_str());
        return (connect(sock, (SOCKADDR *)&addr, sizeof(addr)) != SOCKET_ERROR);
    }

    void send_resp(const std::vector<std::string> &args)
    {
        std::string req = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto &arg : args)
        {
            req += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
        }
        send(sock, req.c_str(), req.length(), 0);
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
    std::string ip;
    int port;
    int pool_size;
    std::vector<RedisConnection *> conns;
    CRITICAL_SECTION lock;

    CircuitState state;
    std::chrono::steady_clock::time_point last_state_change;
    std::unordered_map<std::string, ClientProfile> fallback_cache;

    bool load_scripts(RedisConnection *conn)
    {
        if (!conn)
            return false;

        std::string lua_bucket =
            "local k,c=KEYS[1],KEYS[2] local cfg=redis.call('HMGET',c,'capacity','refill_rate') "
            "local cap,rate=tonumber(cfg[1]) or 2.0,tonumber(cfg[2]) or 0.1 local now=tonumber(ARGV[1]) "
            "local d=redis.call('HMGET',k,'tokens','last_updated') local t,lu=tonumber(d[1]),tonumber(d[2]) "
            "if not t then t=cap lu=now else local el=math.max(0,now-lu) t=math.min(cap,t+(el*rate)) end "
            "if t>=1.0 then redis.call('HMSET',k,'tokens',t-1.0,'last_updated',now) redis.call('EXPIRE',k,60) return 1 "
            "else redis.call('HMSET',k,'tokens',t,'last_updated',now) return 0 end";

        std::string lua_sec =
            "local r,b,v=KEYS[1],KEYS[2],KEYS[3] local now,cap,rate,th,dur=tonumber(ARGV[1]),tonumber(ARGV[2]),tonumber(ARGV[3]),tonumber(ARGV[4]),tonumber(ARGV[5]) "
            "if redis.call('EXISTS',b)==1 then return -1 end local d=redis.call('HMGET',r,'tokens','last_updated') "
            "local t,lu=tonumber(d[1]) or cap,tonumber(d[2]) or now local el=math.max(0,now-lu) t=math.min(cap,t+(el*rate)) "
            "if t>=1.0 then redis.call('HMSET',r,'tokens',t-1.0,'last_updated',now) return 1 "
            "else redis.call('HMSET',r,'tokens',t,'last_updated',now) local viols=redis.call('INCR',v) redis.call('EXPIRE',v,20) "
            "if viols>=th then redis.call('SET',b,'1') redis.call('EXPIRE',b,dur) redis.call('DEL',v) return -1 end return 0 end";

        std::string lua_telem =
            "local r,q=KEYS[1],KEYS[2] local cid,now,cap,rate=ARGV[1],tonumber(ARGV[2]),tonumber(ARGV[3]),tonumber(ARGV[4]) "
            "local d=redis.call('HMGET',r,'tokens','last_updated') local t,lu=tonumber(d[1]) or cap,tonumber(d[2]) or now "
            "local el=math.max(0,now-lu) t=math.min(cap,t+(el*rate)) local status,ret='REJECTED',0 "
            "if t>=1.0 then t=t-1.0 status='ALLOWED' ret=1 end redis.call('HMSET',r,'tokens',t,'last_updated',now) "
            "redis.call('LPUSH',q,now..','..cid..','..status) redis.call('LTRIM',q,0,999) return ret";

        conn->send_resp({"SCRIPT", "LOAD", lua_bucket});
        conn->bucket_sha = conn->parse_bulk(conn->read_resp());

        conn->send_resp({"SCRIPT", "LOAD", lua_sec});
        conn->sec_sha = conn->parse_bulk(conn->read_resp());

        conn->send_resp({"SCRIPT", "LOAD", lua_telem});
        conn->telem_sha = conn->parse_bulk(conn->read_resp());

        return (!conn->bucket_sha.empty() && !conn->sec_sha.empty() && !conn->telem_sha.empty());
    }

    RedisConnection *pop_conn()
    {
        EnterCriticalSection(&lock);
        if (conns.empty() || state == OPEN)
        {
            LeaveCriticalSection(&lock);
            return nullptr;
        }
        RedisConnection *conn = conns.back();
        conns.pop_back();
        LeaveCriticalSection(&lock);
        return conn;
    }

    void push_conn(RedisConnection *conn)
    {
        if (!conn)
            return;
        EnterCriticalSection(&lock);
        conns.push_back(conn);
        LeaveCriticalSection(&lock);
    }

public:
    LimiterEngine(std::string redis_ip, int redis_port, int size)
        : ip(redis_ip), port(redis_port), pool_size(size), state(CLOSED)
    {
        InitializeCriticalSection(&lock);
        last_state_change = std::chrono::steady_clock::now();

        for (int i = 0; i < pool_size; ++i)
        {
            RedisConnection *conn = new RedisConnection();
            if (conn->connect_to_node(ip, port) && load_scripts(conn))
            {
                conns.push_back(conn);
            }
            else
            {
                delete conn;
            }
        }

        RedisConnection *seed = pop_conn();
        if (seed)
        {
            seed->send_resp({"HMSET", "cfg:free", "capacity", "2.0", "refill_rate", "0.1"});
            seed->read_resp();
            seed->send_resp({"HMSET", "cfg:vip", "capacity", "10.0", "refill_rate", "5.0"});
            seed->read_resp();
            push_conn(seed);
        }
    }

    ~LimiterEngine()
    {
        EnterCriticalSection(&lock);
        for (size_t i = 0; i < conns.size(); ++i)
            delete conns[i];
        conns.clear();
        LeaveCriticalSection(&lock);
        DeleteCriticalSection(&lock);
    }

    bool allow(const std::string &client_id, const std::string &tier)
    {
        auto now = std::chrono::steady_clock::now();

        // FSM Circuit Breaker Check
        if (state == OPEN)
        {
            std::chrono::duration<double> elapsed = now - last_state_change;
            if (elapsed.count() > 4.0)
            {
                state = HALF_OPEN;
                std::cout << "[CB] HALF-OPEN: Testing cluster health via canary request..." << std::endl;
            }
            else
            {
                fallback_cache[client_id].current_tokens++;
                std::cout << "[CB-FALLBACK] Serving from transient local cache map..." << std::endl;
                return (fallback_cache[client_id].current_tokens <= 5);
            }
        }

        RedisConnection *conn = pop_conn();
        if (!conn)
        {
            state = OPEN;
            last_state_change = std::chrono::steady_clock::now();
            std::cout << "[CB-ALERT] Node connection failed. Breaking circuit to OPEN..." << std::endl;
            return allow(client_id, tier);
        }

        long long secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        conn->send_resp({"EVALSHA", conn->bucket_sha, "2", "rate:" + client_id, "cfg:" + tier, std::to_string(secs)});

        std::string res = conn->read_resp();
        push_conn(conn);

        if (res.empty())
        {
            state = OPEN;
            last_state_change = std::chrono::steady_clock::now();
            std::cout << "[CB-ALERT] Socket read timeout. Circuit tripped to OPEN." << std::endl;
            return allow(client_id, tier);
        }

        if (state == HALF_OPEN)
        {
            state = CLOSED;
            std::cout << "[CB] Canary passed. Restored circuit to CLOSED." << std::endl;
        }

        std::cout << "[ENGINE] Evaluated via remote Redis script logic." << std::endl;
        return (res.find(":1") != std::string::npos);
    }

    int evaluate_security(const std::string &ip_addr)
    {
        RedisConnection *conn = pop_conn();
        if (!conn)
            return 0;

        auto now = std::chrono::steady_clock::now();
        long long secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

        conn->send_resp({"EVALSHA", conn->sec_sha, "3", "sec:rate:" + ip_addr, "sec:ban:" + ip_addr, "sec:viol:" + ip_addr,
                         std::to_string(secs), "1.0", "0.01", "3", "10"});

        std::string res = conn->read_resp();
        push_conn(conn);

        if (res.find(":-1") != std::string::npos)
            return -1;
        if (res.find(":1") != std::string::npos)
            return 1;
        return 0;
    }

    void drain_telemetry_queue()
    {
        RedisConnection *conn = pop_conn();
        if (!conn)
            return;

        while (true)
        {
            conn->send_resp({"RPOP", "telemetry:queue"});
            std::string res = conn->read_resp();
            std::string data = conn->parse_bulk(res);
            if (data.empty() || data.find("ERROR") != std::string::npos)
                break;
            std::cout << "   -> [TELEMETRY CONSUMER] Dequeued log event: " << data << std::endl;
        }
        push_conn(conn);
    }

    void simulate_network_drop()
    {
        EnterCriticalSection(&lock);
        for (size_t i = 0; i < conns.size(); ++i)
        {
            if (conns[i]->sock != INVALID_SOCKET)
            {
                closesocket(conns[i]->sock);
                conns[i]->sock = INVALID_SOCKET;
            }
        }
        std::cout << "\n>>> [CHAOS] Dropping network handles to Redis cluster... <<<" << std::endl;
        LeaveCriticalSection(&lock);
    }

    void simulate_network_heal()
    {
        std::cout << "\n>>> [CHAOS] Re-establishing sockets to cluster... <<<" << std::endl;
        EnterCriticalSection(&lock);
        for (size_t i = 0; i < conns.size(); ++i)
            delete conns[i];
        conns.clear();

        for (int i = 0; i < pool_size; ++i)
        {
            RedisConnection *conn = new RedisConnection();
            if (conn->connect_to_node(ip, port) && load_scripts(conn))
            {
                conns.push_back(conn);
            }
            else
            {
                delete conn;
            }
        }
        LeaveCriticalSection(&lock);
    }
};

#endif