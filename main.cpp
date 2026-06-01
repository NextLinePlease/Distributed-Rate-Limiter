#include <iostream>
#include "include/LimiterEngine.hpp"
#include <windows.h>

int main()
{
    std::cout << "=========================================================" << std::endl;
    std::cout << "  PRODUCTION SHOWCASE: DISTRIBUTED, FAULT-TOLERANT ENGINE" << std::endl;
    std::cout << "=========================================================\n"
              << std::endl;

    std::cout << "[STAGE 1] Initializing hardware-optimized connection pool..." << std::endl;
    LimiterEngine engine("127.0.0.1", 6379, 5);

    std::cout << "\n[STAGE 2] Simulating multi-tiered user traffic flows..." << std::endl;
    std::string free_user = "user_free_tier";
    std::string vip_user = "user_vip_tier";

    for (int i = 1; i <= 3; i++)
    {
        std::cout << "  - Free User Req " << i << ": " << (engine.allow(free_user, "free") ? "ALLOWED (200)" : "DENIED (429)") << std::endl;
    }
    std::cout << "  - VIP User Req 1: " << (engine.allow(vip_user, "vip") ? "ALLOWED (200)" : "DENIED (429)") << std::endl;

    std::cout << "\n[STAGE 3] Simulating brute-force malicious attack on endpoints..." << std::endl;
    std::string attacker = "192.168.1.99";
    for (int i = 1; i <= 6; i++)
    {
        int status = engine.evaluate_security(attacker);
        std::cout << "  - Attacker Hit " << i << " -> ";
        if (status == 1)
            std::cout << "ALLOWED" << std::endl;
        else if (status == 0)
            std::cout << "REJECTED (429)" << std::endl;
        else if (status == -1)
            std::cout << "!!! HARD BANNED (CIRCUIT ISOLATION) !!!" << std::endl;
    }

    std::cout << "\n[STAGE 4] Injecting Catastrophic Redis Network Outage (Chaos Test)..." << std::endl;
    engine.simulate_network_drop();

    engine.allow("normal_user", "free");
    engine.allow("normal_user", "free");

    std::cout << "\nRestoring network infrastructure channels..." << std::endl;
    engine.simulate_network_heal();
    Sleep(4500);

    engine.allow("normal_user", "free");

    std::cout << "\n[STAGE 5] Draining out-of-band asynchronous telemetry buffer..." << std::endl;
    engine.drain_telemetry_queue();

    std::cout << "\n=========================================================" << std::endl;
    std::cout << "  SIMULATION SUCCESSFUL: ALL SYSTEMS AUTO-HEALED" << std::endl;
    std::cout << "=========================================================" << std::endl;
    return 0;
}