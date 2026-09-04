#include <iostream>
#include <unordered_map>
#include <string>
#include <memory> // For std::shared_ptr
#include <chrono>
#include <algorithm>

// Step 1: The Abstract Interface
class RateLimiterStrategy
{
public:
    virtual ~RateLimiterStrategy() = default;
    virtual bool allow_request() = 0; // Pure virtual function
};

// Step 2: Concrete Strategy A (Token Bucket)
class TokenBucket : public RateLimiterStrategy
{
private:
    double max_capacity = 3.0;
    double tokens = 3.0;
    double refill_rate = 1.0;
    std::chrono::time_point<std::chrono::steady_clock> last_refill;

    void refill()
    {
        auto now = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed = now - last_refill;
        tokens = std::min(max_capacity, tokens + (elapsed.count() * refill_rate));
        last_refill = now;
    }

public:
    TokenBucket() : last_refill(std::chrono::steady_clock::now()) {}

    bool allow_request() override
    {
        refill();
        if (tokens >= 1.0)
        {
            tokens -= 1.0;
            return true;
        }
        return false;
    }
};

// Step 3: Concrete Strategy B (Fixed Window Counter)
class FixedWindow : public RateLimiterStrategy
{
private:
    int limit = 2;
    long long window_size_ms = 2000;
    int counter = 0;
    long long current_window_id = 0;

public:
    bool allow_request() override
    {
        auto now = std::chrono::steady_clock::now();
        long long current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        long long window_id = current_time_ms / window_size_ms;

        if (window_id != current_window_id)
        {
            current_window_id = window_id;
            counter = 0;
        }

        if (counter < limit)
        {
            counter++;
            return true;
        }
        return false;
    }
};

// Step 4: The Extensible Manager (Clean Architecture)
class RateLimiterManager
{
private:
    // The Map stores pointers to the Interface, not specific concrete classes
    std::unordered_map<std::string, std::shared_ptr<RateLimiterStrategy>> user_buckets;
    std::string algorithm_type;

public:
    RateLimiterManager(std::string type) : algorithm_type(type) {}

    bool handle_request(const std::string &client_id)
    {
        // Dynamic Allocation based on system setting
        if (user_buckets.find(client_id) == user_buckets.end())
        {
            if (algorithm_type == "token_bucket")
            {
                user_buckets[client_id] = std::make_shared<TokenBucket>();
            }
            else if (algorithm_type == "fixed_window")
            {
                user_buckets[client_id] = std::make_shared<FixedWindow>();
            }
        }
        // Executes polymorphically without knowing the target algorithm class
        return user_buckets[client_id]->allow_request();
    }
};

int main()
{
    // We can swap the entire backend logic simply by changing this configuration string
    RateLimiterManager manager("fixed_window");

    std::cout << "--- Executing Extensible Architecture Pattern ---" << std::endl;
    std::cout << "Req 1: " << (manager.handle_request("alice") ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Req 2: " << (manager.handle_request("alice") ? "ALLOWED" : "REJECTED") << std::endl;
    std::cout << "Req 3: " << (manager.handle_request("alice") ? "ALLOWED" : "REJECTED") << std::endl;

    return 0;
}