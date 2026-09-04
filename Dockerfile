FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libhiredis-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN git clone https://github.com/sewenew/redis-plus-plus.git && \
    cd redis-plus-plus && \
    mkdir build && cd build && \
    cmake -DREDIS_PLUS_PLUS_BUILD_TEST=OFF .. && \
    make -j$(nproc) && \
    make install && \
    ldconfig

WORKDIR /app
COPY CMakeLists.txt ./
COPY include/ ./include/
COPY src/ ./src/

RUN mkdir build && cd build && \
    cmake .. && \
    make -j$(nproc)

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y libhiredis-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/libredis++* /usr/local/lib/
RUN ldconfig

WORKDIR /app
COPY --from=builder /app/build/rate_limiter_engine ./rate_limiter_engine

CMD ["./rate_limiter_engine"]