# The checks are recipes here, and `.github/workflows/ci.yml` runs these recipes,
# so a check that passes locally passes in CI.
#
#   just              list every recipe
#   just pre-commit   the fast checks, before every commit
#   just ci           everything CI runs

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Show every recipe.
default:
    @just --list --unsorted

# Everything CI runs, cheapest first, so a formatting slip fails in seconds.
ci: fmt-check checks wasm clippy test doc
    @echo "✓ ci passed"

# The fast checks.
pre-commit: fmt-check checks clippy
    @echo "✓ pre-commit passed"

# Run `just pre-commit` before every commit.
install-hooks:
    printf '#!/usr/bin/env bash\nexec just pre-commit\n' > "$(git rev-parse --git-path hooks)/pre-commit"
    chmod +x "$(git rev-parse --git-path hooks)/pre-commit"

# Formatting, without changing a file.
fmt-check:
    cargo fmt --all -- --check

# Format every file.
fmt:
    cargo fmt --all

# The rules of crates.md and CLAUDE.md that the compiler cannot see. Name checks to run only those.
checks *names:
    cargo xtask {{names}}

# The list is read first, so a list that cannot be read stops the recipe, and cargo does not build every crate.
# Build what the UI shares for the browser, with every feature. The WebAssembly table of crates.md lists the crates.
wasm: (checks "wasm-boundary")
    packages="$(cargo xtask --wasm-packages)" \
        && cargo build --target wasm32-unknown-unknown --all-features $packages

# Lint with and without the optional features, warnings denied.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Every test, with every feature. The tests against real Kafka brokers need Docker, and say so when it is missing.
test *args:
    cargo test --workspace --all-features {{args}}

# Only the tests against real Kafka brokers, which `just test` runs too. They need Docker.
test-kafka *args:
    cargo test -p gaslightmq_test_kafka --test cluster {{args}}
    cargo test -p gaslightmq_kafka_client_native --test kafka {{args}}

# The API docs, warnings and broken links denied.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

image:
    docker build -t ghcr.io/cruxy-eu/gaslightmq:latest .

container:
    docker run --rm -p 9092:9092 ghcr.io/cruxy-eu/gaslightmq:latest

# Build the proxy web UI into crates/gaslightmq-proxy/ui-dist/ (needs `dx`).
ui:
    dx build --release --platform web -p gaslightmq-proxy-ui
    rm -rf crates/gaslightmq-proxy/ui-dist
    cp -R target/dx/gaslightmq-proxy-ui/release/web/public crates/gaslightmq-proxy/ui-dist

# Build gmq with the web UI embedded. Run `just ui` first.
proxy-ui:
    cargo build -p gmq --features proxy-ui
