FROM rust:1.100.0-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY gaslightmq/ gaslightmq/
COPY gaslightmq-kernel/ gaslightmq-kernel/
COPY gaslightmq-scenario/ gaslightmq-scenario/
COPY gaslightmq-testing/ gaslightmq-testing/
COPY gaslightmq-testcontainers/ gaslightmq-testcontainers/
COPY gaslightmq-logging/ gaslightmq-logging/
COPY gaslightmq-kafka-client/ gaslightmq-kafka-client/
COPY bins/ bins/

RUN cargo build --release --bin gmq

FROM scratch

COPY --from=builder /build/target/release/gmq /usr/local/bin/gmq
COPY --from=builder --chown=65534:65534 /tmp /tmp

USER 65534:65534

EXPOSE 9092

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
    CMD ["/usr/local/bin/gmq", "health"]

ENTRYPOINT ["/usr/local/bin/gmq"]
