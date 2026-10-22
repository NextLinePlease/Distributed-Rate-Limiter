#ifndef COMMON_HPP
#define COMMON_HPP

#include <windows.h>

// Tracks whether the Redis circuit breaker is healthy, open, or probing recovery.
enum CircuitState
{
    CLOSED,
    OPEN,
    HALF_OPEN
};

// Holds one client's local limiter state and compact feature flags.
class alignas(64) ClientProfile
{
private:
    // Stores the enabled client flags as individual bits.
    unsigned char flags;

public:
    // Records the last update time and the client's current local token count.
    long long last_updated;
    int current_tokens;

    // Defines the bit used to represent each client feature.
    static const unsigned char MASK_ACTIVE = 0x01;
    static const unsigned char MASK_PREMIUM = 0x02;
    static const unsigned char MASK_BANNED = 0x04;
    static const unsigned char MASK_ADMIN = 0x08;
    static const unsigned char MASK_TELEMETRY = 0x10;

    // Starts with no flags, no update time, and no tokens.
    ClientProfile() : flags(0), last_updated(0), current_tokens(0) {}

    // Turns on every flag bit included in the mask.
    void set_flag(unsigned char mask) { flags |= mask; }

    // Turns off every flag bit included in the mask.
    void clear_flag(unsigned char mask) { flags &= ~mask; }

    // Checks whether at least one masked flag bit is enabled.
    bool check_flag(unsigned char mask) const { return (flags & mask) != 0; }
};

#endif