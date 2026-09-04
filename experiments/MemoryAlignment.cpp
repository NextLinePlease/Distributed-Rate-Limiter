#include <iostream>
#include <chrono>
#include <windows.h>

// 24 Bytes - Disorganized architecture layout wasting RAM on structure holes
struct WastefulProfile
{
    bool is_premium;        // 1 byte + 7 bytes padding
    long long last_updated; // 8 bytes
    int current_tokens;     // 4 bytes + 4 bytes padding
};

// 16 Bytes - Optimized layout. Variables sorted descending by memory footprints
struct CleanProfile
{
    long long last_updated; // 8 bytes
    int current_tokens;     // 4 bytes
    bool is_premium;        // 1 byte + 3 bytes padding (for 8-byte grouping alignment)
};

// 64 Bytes - High-Performance Layout aligned directly to the CPU Cache Line
// This prevents multiple threads from dirtying adjacent elements in the same cache block
struct alignas(64) HardwareOptimizedProfile
{
    long long last_updated; // 8 bytes
    int current_tokens;     // 4 bytes
    bool is_premium;        // 1 byte
    char padding[51];       // Pad out precisely to 64 bytes to own the entire CPU Cache Line
};

int main()
{
    std::cout << "=== Hardcore C++ Memory Alignment Diagnostics ===" << std::endl;

    std::cout << "\n[1] WastefulProfile Details:" << std::endl;
    std::cout << "  - Real data needed: 13 bytes (1 + 8 + 4)" << std::endl;
    std::cout << "  - Actual allocated struct size: " << sizeof(WastefulProfile) << " bytes" << std::endl;
    std::cout << "  - Memory efficiency loss: " << (sizeof(WastefulProfile) - 13) << " bytes wasted per instance." << std::endl;

    std::cout << "\n[2] CleanProfile Details (Sorted Descending):" << std::endl;
    std::cout << "  - Actual allocated struct size: " << sizeof(CleanProfile) << " bytes" << std::endl;
    std::cout << "  - Saved space: " << (sizeof(WastefulProfile) - sizeof(CleanProfile)) << " bytes chopped instantly." << std::endl;

    std::cout << "\n[3] HardwareOptimizedProfile Details (alignas(64)):" << std::endl;
    std::cout << "  - Allocated struct size: " << sizeof(HardwareOptimizedProfile) << " bytes" << std::endl;
    std::cout << "  - Cache Line Alignment Boundary: " << alignof(HardwareOptimizedProfile) << " bytes" << std::endl;

    // Direct performance speed benchmark test
    const int iterations = 50000000;

    CleanProfile unaligned_array[2];
    auto start_unaligned = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        unaligned_array[0].last_updated = i;
        unaligned_array[1].current_tokens = i;
    }
    auto end_unaligned = std::chrono::high_resolution_clock::now();

    HardwareOptimizedProfile aligned_array[2];
    auto start_aligned = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        aligned_array[0].last_updated = i;
        aligned_array[1].current_tokens = i;
    }
    auto end_aligned = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> unaligned_ms = end_unaligned - start_unaligned;
    std::chrono::duration<double, std::milli> aligned_ms = end_aligned - start_aligned;

    std::cout << "\n=== Speed Metrics (50 Million Field Mutations) ===" << std::endl;
    std::cout << "  - Packed Unaligned Array Write Time: " << unaligned_ms.count() << " ms" << std::endl;
    std::cout << "  - Cache Line Aligned Array Write Time: " << aligned_ms.count() << " ms" << std::endl;

    return 0;
}