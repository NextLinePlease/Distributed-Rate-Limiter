#ifndef COMMON_HPP
#define COMMON_HPP

#include <windows.h>

enum CircuitState
{
    CLOSED,
    OPEN,
    HALF_OPEN
};

// 64-byte alignment avoids false sharing on hot CPU cache lines
class alignas(64) ClientProfile
{
private:
    unsigned char flags;

public:
    long long last_updated;
    int current_tokens;

    // Bit positions for tracking client state
    static const unsigned char MASK_ACTIVE = 0x01;
    static const unsigned char MASK_PREMIUM = 0x02;
    static const unsigned char MASK_BANNED = 0x04;
    static const unsigned char MASK_ADMIN = 0x08;
    static const unsigned char MASK_TELEMETRY = 0x10;

    ClientProfile() : flags(0), last_updated(0), current_tokens(0) {}

    void set_flag(unsigned char mask) { flags |= mask; }
    void clear_flag(unsigned char mask) { flags &= ~mask; }
    bool check_flag(unsigned char mask) const { return (flags & mask) != 0; }
};

#endif