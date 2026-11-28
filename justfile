# The checks are recipes here, and `.github/workflows/ci.yml` runs these recipes,
# so a check that passes locally passes in CI.
#
#   just              list every recipe
#   just pre-commit   the fast checks, before every commit
#   just ci           everything CI runs

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# The image of `gmq` that `just image` builds, `just container` runs and `just test-image` tests.
gmq_image := "ghcr.io/cruxy-eu/gaslightmq:latest"

# The examples whose tests run: the projects of a user, which test an application with GaslightMQ.
# Each is a workspace of its own, so a recipe names its manifest. It builds into the target directory of this
# workspace, which already holds the crates that the example depends on.
example_in_process := "examples/12-test-in-process/Cargo.toml"
example_with_a_container := "examples/13-test-with-a-container/Cargo.toml"

# Show every recipe.
default:
    @just --list --unsorted

# Everything CI runs, cheapest first, so a formatting slip fails in seconds.
ci: fmt-check checks wasm clippy test test-image doc
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
    cargo fmt --all --manifest-path {{example_in_process}} -- --check
    cargo fmt --all --manifest-path {{example_with_a_container}} -- --check

# Format every file.
fmt:
    cargo fmt --all
    cargo fmt --all --manifest-path {{example_in_process}}
    cargo fmt --all --manifest-path {{example_with_a_container}}

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
    cargo clippy --manifest-path {{example_in_process}} --target-dir target --all-targets -- -D warnings
    cargo clippy --manifest-path {{example_with_a_container}} --target-dir target --all-targets -- -D warnings

# The tests against real Kafka brokers need Docker, and say so when it is missing.
# Every test, with every feature, but those of the image, which `just test-image` runs.
# Then the tests of the example with GaslightMQ in process.
test *args:
    cargo test --workspace --all-features --exclude gaslightmq_testcontainers {{args}}
    cargo test --manifest-path {{example_in_process}} --target-dir target {{args}}

# It builds the image first, and the tests run that one. They need Docker.
# The tests of the image of `gmq`: GaslightMQ in a container next to a test, in front of real Kafka brokers.
# Then the tests of the example with GaslightMQ in a container.
test-image *args: image
    GASLIGHTMQ_IMAGE={{gmq_image}} cargo test -p gaslightmq_testcontainers --all-features {{args}}
    GASLIGHTMQ_IMAGE={{gmq_image}} cargo test --manifest-path {{example_with_a_container}} --target-dir target {{args}}

# Only the tests against real Kafka brokers, which `just test` runs too. They need Docker.
test-kafka *args:
    cargo test -p gaslightmq_test_kafka --test cluster {{args}}
    cargo test -p gaslightmq_kafka_client_native --test kafka {{args}}
    cargo test -p gaslightmq_backend_upstream --test real_cluster {{args}}
    cargo test -p gaslightmq_engine --test real_cluster {{args}}
    cargo test -p gaslightmq_host --test real_cluster {{args}}
    cargo test -p gaslightmq_control_server --test real_cluster {{args}}
    cargo test -p gaslightmq_test --test real_cluster {{args}}
    cargo test -p gmq --test real_cluster {{args}}

# The API docs, warnings and broken links denied.
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# The image of `gmq`, with the UI built in. Needs Docker. The image builds the UI itself, as `just ui` does.
image:
    docker build -t {{gmq_image}} .

# Run the image with the arguments of `gmq`: `just container proxy --upstream host.docker.internal:9092`.
# It publishes the bootstrap listener, the brokers and the control plane with the UI on the ports of `gmq proxy`.
container *args:
    docker run --rm -p 9092:9092 -p 19092-19191:19092-19191 -p 7092:7092 {{gmq_image}} {{args}}

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
