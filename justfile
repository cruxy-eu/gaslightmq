image:
    docker build -t ghcr.io/cruxy-eu/gaslightmq:latest .

container:
    docker run --rm -p 9092:9092 ghcr.io/cruxy-eu/gaslightmq:latest
