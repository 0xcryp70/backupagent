FROM debian:stable-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates curl jq openssl zstd gzip coreutils \
      postgresql-client awscli && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pg_backup_agent.sh /usr/local/bin/pg_backup_agent
RUN chmod +x /usr/local/bin/pg_backup_agent

VOLUME ["/backups"]
ENTRYPOINT ["/usr/local/bin/pg_backup_agent"]
