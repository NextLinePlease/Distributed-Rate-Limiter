#include <iostream>
#include "include/LimiterEngine.hpp"
#include <windows.h>

int main()
{
    std::cout << "--- Starting Limiter Engine Runtime Harness ---\n"
              << std::endl;

    LimiterEngine engine("127.0.0.1", 6379, 5);

    std::cout << "\n[TEST] Verifying standard tier operations..." << std::endl;
    std::string free_user = "user_free_tier";
    std::string vip_user = "user_vip_tier";

    for (int i = 1; i <= 3; i++)
    {
        std::cout << "  Req " << i << " (Free): " << (engine.allow(free_user, "free") ? "200 OK" : "429 TOO MANY REQUESTS") << std::endl;
    }
    std::cout << "  Req 1 (VIP): " << (engine.allow(vip_user, "vip") ? "200 OK" : "429 TOO MANY REQUESTS") << std::endl;

    std::cout << "\n[TEST] Verifying abusive traffic isolation..." << std::endl;
    std::string attacker = "192.168.1.99";
    for (int i = 1; i <= 6; i++)
    {
        int status = engine.evaluate_security(attacker);
        std::cout << "  Hit " << i << " -> ";
        if (status == 1)
            std::cout << "ALLOWED" << std::endl;
        else if (status == 0)
            std::cout << "REJECTED (429)" << std::endl;
        else if (status == -1)
            std::cout << "!!! HARD BANNED !!!" << std::endl;
    }

    std::cout << "\n[TEST] Injecting network fault..." << std::endl;
    engine.simulate_network_drop();

    engine.allow("normal_user", "free");
    engine.allow("normal_user", "free");

    std::cout << "\n[TEST] Recovering network..." << std::endl;
    engine.simulate_network_heal();
    Sleep(4500);

    engine.allow("normal_user", "free");

    std::cout << "\n[TEST] Draining asynchronous log buffers..." << std::endl;
    engine.drain_telemetry_queue();

    std::cout << "\n--- Harness Simulation Finished Cleanly ---" << std::endl;
    return 0;
}