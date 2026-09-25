FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/relayfs /usr/local/bin/relayfs
RUN chmod +x /usr/local/bin/relayfs

EXPOSE 8788

CMD ["/usr/local/bin/relayfs", "--mode", "server", "--listen", "0.0.0.0:8788"]
