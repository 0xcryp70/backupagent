# backupcenter/cassandra/Dockerfile
FROM cassandra:4.1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates openssl zstd gzip awscli coreutils findutils python3 && \
    rm -rf /var/lib/apt/lists/*

# Tools are already in PATH: nodetool, cqlsh, java, python3.
WORKDIR /app
COPY cassandra_backup_agent.sh /usr/local/bin/cassandra_backup_agent
RUN chmod +x /usr/local/bin/cassandra_backup_agent

VOLUME ["/backups"]
ENTRYPOINT ["/usr/local/bin/cassandra_backup_agent"]
