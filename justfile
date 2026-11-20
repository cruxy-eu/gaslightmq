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
