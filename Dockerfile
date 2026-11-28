# The image of `gmq`, with the UI built in. `just image` builds it, and so does the release workflow.

# The UI: the Dioxus application, built for the browser by `just ui`, with the dx of `just dx-version`.
FROM rust:1.100.0-trixie AS ui

RUN rustup target add wasm32-unknown-unknown \
    && curl -fsSL "https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-$(uname -m)-unknown-linux-musl.tgz" \
        | tar -xz -C /usr/local/cargo/bin \
    && cargo binstall --no-confirm just

WORKDIR /build
COPY justfile .
RUN cargo binstall --no-confirm "dioxus-cli@$(just dx-version)"
COPY . .
RUN just ui

# The binary: static, so it runs in an image that holds nothing else. A release build holds the files of the UI.
FROM rust:1.100.0-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /build
COPY . .
COPY --from=ui /build/crates/gaslightmq_control_server/ui crates/gaslightmq_control_server/ui
RUN cargo build --release -p gmq

FROM scratch

COPY --from=builder /build/target/release/gmq /usr/local/bin/gmq
COPY --from=builder --chown=65534:65534 /tmp /tmp

# The process runs as nobody. A container of `gaslightmq_testcontainers` runs it as root instead: the files of the
# workspace that the crate copies into the container belong to root, and the process writes its definitions and its
# recordings there.
USER 65534:65534

# The control plane and the UI listen on every address of the container, so a published port reaches them.
ENV GMQ_CONTROL_LISTEN=0.0.0.0:7092

# 9092: where the clients of `gmq proxy` connect first. 19092-19191: its brokers. 7092: the control plane and the UI.
EXPOSE 9092 19092-19191 7092

# Healthy while every environment that the process was started with runs. `gmq health` asks the control plane over
# localhost, and fails when nothing answers.
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/gmq", "health"]

ENTRYPOINT ["/usr/local/bin/gmq"]
