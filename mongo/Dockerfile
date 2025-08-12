# backupcenter/mongo/Dockerfile
FROM debian:stable-slim

ARG TOOLS_VER=100.12.2      # match current release from MongoDB
ARG TOOLS_PLATFORM=debian12 # pick one: debian12 | ubuntu2204 | ubuntu2404 | amazon2 | rhel8 | ...
ARG TARGETARCH=amd64        # docker sets this automatically in multi-arch builds

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates curl openssl zstd gzip awscli coreutils && \
    rm -rf /var/lib/apt/lists/*

# Map Docker arch to MongoDB's archive naming (amd64->x86_64, arm64->aarch64)
RUN set -eux; \
    arch="$(case "${TARGETARCH}" in \
              amd64) echo x86_64 ;; \
              arm64) echo aarch64 ;; \
              *)     echo "${TARGETARCH}";; \
            esac)"; \
    url="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-${TOOLS_PLATFORM}-${arch}-${TOOLS_VER}.tgz"; \
    echo "Fetching ${url}"; \
    curl -fsSL "${url}" -o /tmp/tools.tgz; \
    tar -xzf /tmp/tools.tgz -C /usr/local --strip-components=1; \
    rm -f /tmp/tools.tgz

WORKDIR /app
COPY mongo_backup_agent.sh /usr/local/bin/mongo_backup_agent
RUN chmod +x /usr/local/bin/mongo_backup_agent

VOLUME ["/backups"]
ENTRYPOINT ["/usr/local/bin/mongo_backup_agent"]
