#include <iostream>

// 8 Bytes - Wasting massive amounts of space due to byte boundaries and structural alignment
struct WastefulFlags
{
    bool is_active;       // 1 byte
    bool is_premium;      // 1 byte
    bool is_banned;       // 1 byte
    bool is_admin;        // 1 byte
    bool track_telemetry; // 1 byte
    // 3 bytes of invisible padding are added automatically by the compiler here
};

// 1 Byte - Using C++ bit-fields to package all flags into 5 explicit bits
struct PackedBitFields
{
    unsigned char active : 1;    // Allocates exactly 1 bit
    unsigned char premium : 1;   // Allocates exactly 1 bit
    unsigned char banned : 1;    // Allocates exactly 1 bit
    unsigned char admin : 1;     // Allocates exactly 1 bit
    unsigned char telemetry : 1; // Allocates exactly 1 bit
    unsigned char reserved : 3;  // Remaining 3 bits left over for future expansion
};

// 1 Byte - Direct Manual Bit-Masking approach (Industry standard for performance-critical systems)
class ManualBitMask
{
private:
    unsigned char flags = 0; // 8 blank bits: 00000000

public:
    // Define exact bit positions using hexadecimal literals (Powers of 2)
    static const unsigned char MASK_ACTIVE = 0x01;    // 00000001
    static const unsigned char MASK_PREMIUM = 0x02;   // 00000010
    static const unsigned char MASK_BANNED = 0x04;    // 00000100
    static const unsigned char MASK_ADMIN = 0x08;     // 00001000
    static const unsigned char MASK_TELEMETRY = 0x10; // 00010000

    void set_flag(unsigned char mask) { flags |= mask; }                      // Bitwise OR to toggle bit ON
    void clear_flag(unsigned char mask) { flags &= ~mask; }                   // Bitwise AND with NOT to toggle bit OFF
    bool check_flag(unsigned char mask) const { return (flags & mask) != 0; } // Bitwise AND to check state
};

int main()
{
    std::cout << "=== Memory Footprint Structural Breakdown ===" << std::endl;
    std::cout << "Size of WastefulFlags struct:  " << sizeof(WastefulFlags) << " bytes" << std::endl;
    std::cout << "Size of PackedBitFields struct: " << sizeof(PackedBitFields) << " byte" << std::endl;
    std::cout << "Size of ManualBitMask class:   " << sizeof(ManualBitMask) << " byte" << std::endl;

    std::cout << "\n=== Testing Native Bit Fields Configuration ===" << std::endl;
    PackedBitFields user1{1, 0, 0, 1, 1, 0}; // Active, Not Premium, Not Banned, Admin, Telemetry
    std::cout << " User 1 Admin Status: " << (user1.admin ? "TRUE" : "FALSE") << std::endl;
    std::cout << " User 1 Premium Status: " << (user1.premium ? "TRUE" : "FALSE") << std::endl;

    std::cout << "\n=== Testing High-Performance Manual Bit-Masking ===" << std::endl;
    ManualBitMask user2;

    // Configure user permissions
    user2.set_flag(ManualBitMask::MASK_ACTIVE);
    user2.set_flag(ManualBitMask::MASK_BANNED);

    std::cout << " User 2 Active:  " << (user2.check_flag(ManualBitMask::MASK_ACTIVE) ? "YES" : "NO") << std::endl;
    std::cout << " User 2 Banned:  " << (user2.check_flag(ManualBitMask::MASK_BANNED) ? "YES" : "NO") << std::endl;
    std::cout << " User 2 Premium: " << (user2.check_flag(ManualBitMask::MASK_PREMIUM) ? "YES" : "NO") << std::endl;

    std::cout << "\nModifying User 2 flags on the fly..." << std::endl;
    user2.clear_flag(ManualBitMask::MASK_BANNED);
    user2.set_flag(ManualBitMask::MASK_PREMIUM);

    std::cout << " User 2 Banned:  " << (user2.check_flag(ManualBitMask::MASK_BANNED) ? "YES" : "NO") << std::endl;
    std::cout << " User 2 Premium: " << (user2.check_flag(ManualBitMask::MASK_PREMIUM) ? "YES" : "NO") << std::endl;

    return 0;
}