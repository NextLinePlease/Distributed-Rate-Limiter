#include <iostream>
#include <chrono>
#include <algorithm>
#include <windows.h> // For Sleep()

class LeakyBucket
{
private:
    double capacity;
    double leak_rate; // Water leaked per second
    double current_water;
    std::chrono::time_point<std::chrono::steady_clock> last_leak_timestamp;

    void leak()
    {
        auto now = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed = now - last_leak_timestamp;

        // Calculate how much water has leaked out over time
        double water_leaked = elapsed.count() * leak_rate;
        current_water = std::max(0.0, current_water - water_leaked);

        last_leak_timestamp = now;
    }

public:
    LeakyBucket(double cap, double rate)
        : capacity(cap), leak_rate(rate), current_water(0.0),
          last_leak_timestamp(std::chrono::steady_clock::now()) {}

    bool allow_request()
    {
        leak(); // Update current water level based on time passed

        // Check if adding 1 unit of water overflows the bucket
        if (current_water + 1.0 <= capacity)
        {
            current_water += 1.0;
            return true; // Request accepted into the queue
        }

        return false; // Bucket overflowed! Rate limit triggered
    }
};

int main()
{
    // Capacity = 3 requests, leaks at 1 request per second
    LeakyBucket limiter(3.0, 1.0);

    std::cout << "--- Firing 5 rapid requests ---" << std::endl;
    for (int i = 1; i <= 5; ++i)
    {
        std::cout << "Request " << i << ": " << (limiter.allow_request() ? "ALLOWED" : "REJECTED") << std::endl;
    }

    std::cout << "\n--- Waiting for 1.5 seconds ---" << std::endl;
    Sleep(1500);

    std::cout << "Request 6: " << (limiter.allow_request() ? "ALLOWED" : "REJECTED") << std::endl;

    return 0;
}