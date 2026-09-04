cmake_minimum_required(VERSION 3.16)
project(DistributedRateLimiter LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra")

find_package(Threads REQUIRED)
find_library(REDIS_PLUS_PLUS_LIB redis++ PATHS /usr/local/lib REQUIRED)
find_library(HIREDIS_LIB hiredis REQUIRED)

include_directories(include /usr/local/include)

add_executable(rate_limiter_engine
    src/main.cpp
    src/RedisManager.cpp
    src/TokenBucketLimiter.cpp
    src/SlidingWindowLimiter.cpp
)

target_link_libraries(rate_limiter_engine
    PRIVATE
    ${REDIS_PLUS_PLUS_LIB}
    ${HIREDIS_LIB}
    Threads::Threads
)