# backupcenter/oracle/Dockerfile
# Use the DB image you actually run (XE or Enterprise). Examples:
# FROM gvenzl/oracle-xe:21-slim
# FROM container-registry.oracle.com/database/enterprise:19.3.0
FROM gvenzl/oracle-xe:21-slim

USER root
RUN microdnf install -y ca-certificates openssl zstd gzip tar python3-pip jq \
 && pip3 install --no-cache-dir awscli \
 && microdnf clean all

# Add the agent
COPY oracle_backup_agent.sh /usr/local/bin/oracle_backup_agent.sh
RUN chmod +x /usr/local/bin/oracle_backup_agent.sh

# Optional: create backups dir inside the container (you will still bind-mount)
RUN mkdir -p /backups && chown -R oracle:oinstall /backups

# Drop to oracle user (matches tools environment in XE/DB images)
USER oracle
ENTRYPOINT ["/usr/local/bin/oracle_backup_agent.sh"]
