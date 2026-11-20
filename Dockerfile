# Proxy web UI: a Dioxus wasm app, built with `dx`.
FROM rust:1.100.0-trixie AS ui

ARG DX_VERSION=0.7.10

RUN rustup target add wasm32-unknown-unknown \
    && curl -fsSL "https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-$(uname -m)-unknown-linux-musl.tgz" \
        | tar -xz -C /usr/local/cargo/bin \
    && cargo binstall --no-confirm "dioxus-cli@${DX_VERSION}"

WORKDIR /build
COPY . .
RUN dx build --release --platform web -p gaslightmq-proxy-ui

FROM rust:1.100.0-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY . .
COPY --from=ui /build/target/dx/gaslightmq-proxy-ui/release/web/public crates/gaslightmq-proxy/ui-dist
RUN cargo build --release -p gmq --features proxy-ui

FROM scratch

COPY --from=builder /build/target/release/gmq /usr/local/bin/gmq
COPY --from=builder --chown=65534:65534 /tmp /tmp

USER 65534:65534

# 9092: fake broker (`gmq`). 9000: proxy control UI (`gmq proxy`).
EXPOSE 9092 9000

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
    CMD ["/usr/local/bin/gmq", "health"]

ENTRYPOINT ["/usr/local/bin/gmq"]
