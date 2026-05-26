FROM rust:1.94.1-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY gaslightmq/ gaslightmq/
COPY gaslightmq-kernel/ gaslightmq-kernel/
COPY gaslightmq-testing/ gaslightmq-testing/
COPY gaslightmq-kafka-client/ gaslightmq-kafka-client/
COPY bins/ bins/

RUN cargo build --release --bin gmq

FROM scratch

COPY --from=builder /build/target/release/gmq /usr/local/bin/gmq

USER 65534:65534

EXPOSE 9092

ENTRYPOINT ["/usr/local/bin/gmq"]
