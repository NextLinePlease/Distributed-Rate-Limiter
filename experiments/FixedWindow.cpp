#include <iostream>
#include <chrono>
#include <windows.h> // For Sleep()

class FixedWindowCounter
{
private:
    int limit;
    long long window_size_ms;
    int counter;
    long long current_window_id;

public:
    FixedWindowCounter(int max_requests, long long window_ms)
    {
        limit = max_requests;
        window_size_ms = window_ms;
        counter = 0;
        current_window_id = 0;
    }

    bool allow_request()
    {
        // Get absolute current system time in milliseconds
        auto now = std::chrono::steady_clock::now();
        long long current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        now.time_since_epoch())
                                        .count();

        // Step 1: Calculate which specific window slot we are currently in
        long long window_id = current_time_ms / window_size_ms;

        // Step 2: If the window ID has changed, reset the counter
        if (window_id != current_window_id)
        {
            current_window_id = window_id;
            counter = 0; // Clear usage record for the new window block
        }

        // Step 3: Evaluate limit
        if (counter < limit)
        {
            counter++;
            return true;
        }

        return false;
    }
};

int main()
{
    // Limit = 3 requests per 2-second window (2000 milliseconds)
    FixedWindowCounter limiter(3, 2000);

    std::cout << "--- Firing 4 rapid requests inside the same window ---" << std::endl;
    for (int i = 1; i <= 4; ++i)
    {
        std::cout << "Request " << i << ": " << (limiter.allow_request() ? "ALLOWED" : "REJECTED") << std::endl;
    }

    std::cout << "\n--- Sleeping 2 seconds to force a window rollover ---" << std::endl;
    Sleep(2000);

    std::cout << "\n--- Firing request after window reset ---" << std::endl;
    std::cout << "Request 5: " << (limiter.allow_request() ? "ALLOWED" : "REJECTED") << std::endl;

    return 0;
}