FROM rust:1.90-bookworm AS builder

WORKDIR /src

ENV CARGO_BUILD_JOBS=4

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    libprotobuf-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release -p smg --features vendored-openssl --bin smg

FROM 10.29.252.145:5000/nvidia/ai-dynamo/vllm-runtime:1.1.1-cuda13-mooncake-20260522T231348Z

COPY --from=builder /src/target/release/smg /usr/local/bin/smg

ENTRYPOINT ["/usr/local/bin/smg"]
