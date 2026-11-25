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
    cargo test -p gaslightmq_backend_upstream --test real_cluster {{args}}
    cargo test -p gaslightmq_engine --test real_cluster {{args}}
    cargo test -p gaslightmq_host --test real_cluster {{args}}
    cargo test -p gaslightmq_control_server --test real_cluster {{args}}

# The API docs, warnings and broken links denied.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

image:
    docker build -t ghcr.io/cruxy-eu/gaslightmq:latest .

container:
    docker run --rm -p 9092:9092 ghcr.io/cruxy-eu/gaslightmq:latest

# The version of dx that builds the UI. It pins the Tailwind that builds the stylesheet: 4.1.5 for dx 0.7.10.
dx-version:
    @echo 0.7.10

# Build the web UI into crates/gaslightmq_control_server/ui/, which the control server serves.
# Needs `dx` at the version of `just dx-version` (`cargo binstall dioxus-cli@0.7.10`), which downloads its Tailwind on
# its first run, and the wasm32-unknown-unknown target of rust-toolchain.toml.
# A debug build of the server reads the folder while it runs, and a release build holds the files.
ui:
    dx --version | grep -q "dioxus $(just dx-version)" || { echo "just ui needs dx $(just dx-version)" >&2; exit 1; }
    rm -rf target/dx/gaslightmq_ui/release/web/public
    dx build --release --platform web -p gaslightmq_ui
    test -s crates/gaslightmq_ui/public/ui.css || { echo "Tailwind wrote no stylesheet" >&2; exit 1; }
    cp crates/gaslightmq_ui/public/ui.css target/dx/gaslightmq_ui/release/web/public/ui.css
    rm -rf crates/gaslightmq_control_server/ui
    cp -R target/dx/gaslightmq_ui/release/web/public crates/gaslightmq_control_server/ui

# The UI in front of live traffic on http://127.0.0.1:7092, or on the address given: a proxy, a real cluster and busy
# clients. Needs Docker.
demo *address: ui
    cargo run -p gaslightmq_control_server --example demo -- {{address}}
