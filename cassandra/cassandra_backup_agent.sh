#!/usr/bin/env bash
set -euo pipefail

# ========= Config via ENV =========
# Target node (CQL + JMX must be reachable)
CASSANDRA_HOST="${CASSANDRA_HOST:-127.0.0.1}"   # for cqlsh
CQL_PORT="${CQL_PORT:-9042}"
CQL_USER="${CQL_USER:-}"
CQL_PASS="${CQL_PASS:-}"

JMX_HOST="${JMX_HOST:-127.0.0.1}"               # for nodetool
JMX_PORT="${JMX_PORT:-7199}"
JMX_USER="${JMX_USER:-}"
JMX_PASS="${JMX_PASS:-}"

# Data directories (comma-separated)
DATA_DIRS="${DATA_DIRS:-/var/lib/cassandra/data}"

# Mode: snapshot or incremental
BACKUP_MODE="${BACKUP_MODE:-snapshot}"          # "snapshot" | "incremental"
FLUSH_BEFORE_SNAPSHOT="${FLUSH_BEFORE_SNAPSHOT:-true}"  # flush memtables first
KEYSPACES="${KEYSPACES:-all}"                   # "all" or comma list (exclude system*)
SNAPSHOT_SCHEMA="${SNAPSHOT_SCHEMA:-true}"      # include schema.cql in the archive
INCR_CLEAR="${INCR_CLEAR:-true}"                # delete collected incremental hardlinks after packaging

# Output & retention
OUTPUT_DIR="${OUTPUT_DIR:-/backups}"
RETENTION_COUNT="${RETENTION_COUNT:-7}"

# Compression & encryption
COMPRESS="${COMPRESS:-zstd}"                    # zstd | gzip | none
ZSTD_LEVEL="${ZSTD_LEVEL:-6}"
GZIP_LEVEL="${GZIP_LEVEL:-6}"
ENCRYPT="${ENCRYPT:-false}"                     # true | false
ENCRYPT_PASSWORD="${ENCRYPT_PASSWORD:-}"

# S3/MinIO upload
S3_UPLOAD="${S3_UPLOAD:-false}"                 # true | false
S3_BUCKET="${S3_BUCKET:-}"                      # s3://bucket/prefix
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-}"                  # e.g. http://minio:9000 (API port)
S3_SSL_VERIFY="${S3_SSL_VERIFY:-true}"          # true | false
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-true}"

TZ=UTC; export TZ

# ========= Helpers =========
ts() { date -u +"%Y-%m-%dT%H-%M-%SZ"; }
log() { printf "[%s] %s\n" "$(ts)" "$*"; }
fail() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"; }

check_tools() {
  need tar
  need find
  need sha256sum
  need awk
  need sed
  need xargs
  need awk
  [[ "$COMPRESS" == "zstd" ]] && need zstd
  [[ "$COMPRESS" == "gzip" ]] && need gzip
  [[ "$ENCRYPT" == "true" ]] && need openssl
  [[ "$S3_UPLOAD" == "true" ]] && need aws
  need bash
  need python3
  need java
  # Cassandra tools (from the image): nodetool & cqlsh
  command -v nodetool >/dev/null 2>&1 || need "$CASSANDRA_HOME/bin/nodetool"
  command -v cqlsh >/dev/null 2>&1 || need "$CASSANDRA_HOME/bin/cqlsh"
}

compress_cmd() {
  case "$COMPRESS" in
    zstd) echo "zstd -T0 -$ZSTD_LEVEL" ;;
    gzip) echo "gzip -$GZIP_LEVEL" ;;
    none) echo "cat" ;;
    *) fail "Unknown COMPRESS=$COMPRESS" ;;
  esac
}

ext_for_compress() {
  case "$COMPRESS" in
    zstd) echo "zst" ;;
    gzip) echo "gz" ;;
    none) echo "bin" ;;
  esac
}

maybe_encrypt() {
  if [[ "$ENCRYPT" == "true" ]]; then
    [[ -n "$ENCRYPT_PASSWORD" ]] || fail "ENCRYPT=true but ENCRYPT_PASSWORD is empty"
    openssl enc -aes-256-cbc -salt -pbkdf2 -pass "pass:$ENCRYPT_PASSWORD"
  else
    cat
  fi
}

# Global for safe cleanup in traps
TMP_LIST=""
TMP_DIR=""
trap_cleanup() {
  set +u
  [[ -n "${TMP_LIST:-}" && -f "${TMP_LIST:-}" ]] && rm -f -- "$TMP_LIST" || true
  [[ -n "${TMP_DIR:-}"  && -d "${TMP_DIR:-}"  ]] && rm -rf -- "$TMP_DIR" || true
}

s3_put() {
  local file="$1" rel; rel="$(basename "$file")"
  if [[ "$S3_UPLOAD" == "true" ]]; then
    [[ -n "$S3_BUCKET" ]] || fail "S3_UPLOAD=true but S3_BUCKET is empty"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_S3_FORCE_PATH_STYLE
    local extra=()
    [[ -n "$S3_ENDPOINT" ]] && extra+=( --endpoint-url "$S3_ENDPOINT" )
    [[ "$S3_SSL_VERIFY" == "false" ]] && extra+=( --no-verify-ssl )
    aws s3 cp "$file" "${S3_BUCKET%/}/$rel" "${extra[@]}"
  fi
}

retention_prune() {
  local dir="$1" pattern="$2"
  [[ -d "$dir" ]] || return 0
  mapfile -t files < <(ls -1t "$dir"/$pattern 2>/dev/null || true)
  local keep="$RETENTION_COUNT" i=0
  for f in "${files[@]}"; do
    ((i++))
    if (( i > keep )); then rm -f -- "$f" "$f.sha256"; fi
  done
}

# ========= Cassandra helpers =========
NTOOL() {
  # Wrap nodetool with JMX params
  local args=()
  [[ -n "$JMX_HOST" ]] && args+=( -h "$JMX_HOST" )
  [[ -n "$JMX_PORT" ]] && args+=( -p "$JMX_PORT" )
  [[ -n "$JMX_USER" ]] && args+=( -u "$JMX_USER" )
  [[ -n "$JMX_PASS" ]] && args+=( -pw "$JMX_PASS" )
  nodetool "${args[@]}" "$@"
}

schema_dump() {
  local outfile="$1"
  local args=( "$CASSANDRA_HOST" "$CQL_PORT" )
  [[ -n "$CQL_USER" ]] && args+=( -u "$CQL_USER" )
  [[ -n "$CQL_PASS" ]] && args+=( -p "$CQL_PASS" )
  args+=( -e "DESCRIBE FULL SCHEMA" )
  cqlsh "${args[@]}" > "$outfile"
}

is_system_ks() {
  case "$1" in
    system|system_schema|system_traces|system_distributed|system_auth) return 0 ;;
    *) return 1 ;;
  esac
}

split_csv() {
  local IFS=','; read -r -a arr <<< "$1"; printf "%s\n" "${arr[@]}"
}

# ========= Snapshot mode =========
do_snapshot() {
  local tag="snap_$(ts)"
  local base="${OUTPUT_DIR%/}/snapshot"
  mkdir -p "$base"
  local extc; extc="$(ext_for_compress)"
  local out="${base}/cassandra_${tag}.snapshot.tar.${extc}"
  log "Snapshot tag: $tag"

  # Optional flush
  if [[ "$FLUSH_BEFORE_SNAPSHOT" == "true" ]]; then
    if [[ "$KEYSPACES" == "all" ]]; then
      log "Flushing all keyspaces..."
      NTOOL flush || fail "flush failed"
    else
      for ks in $(split_csv "$KEYSPACES"); do
        is_system_ks "$ks" && continue
        log "Flushing keyspace: $ks"
        NTOOL flush "$ks" || fail "flush $ks failed"
      done
    fi
  fi

  # Take snapshots (loop per ks for clarity)
  if [[ "$KEYSPACES" == "all" ]]; then
    log "Taking snapshot (all keyspaces) tag=$tag"
    NTOOL snapshot -t "$tag"
  else
    for ks in $(split_csv "$KEYSPACES"); do
      is_system_ks "$ks" && continue
      log "Taking snapshot for keyspace=$ks tag=$tag"
      NTOOL snapshot -t "$tag" "$ks"
    done
  fi

  # Build file list from DATA_DIRS
  TMP_LIST="$(mktemp)"; trap trap_cleanup EXIT
  while IFS= read -r d; do
    d="$(echo "$d" | xargs)"; [[ -d "$d" ]] || continue
    if [[ "$KEYSPACES" == "all" ]]; then
      # everything except system keyspaces
      find "$d" -type f -path "*/snapshots/$tag/*" \
        | grep -Ev '/(system|system_schema|system_traces|system_distributed|system_auth)/' \
        >> "$TMP_LIST"
    else
      for ks in $(split_csv "$KEYSPACES"); do
        is_system_ks "$ks" && continue
        find "$d/$ks" -type f -path "*/snapshots/$tag/*" >> "$TMP_LIST" 2>/dev/null || true
      done
    fi
  done < <(printf "%s\n" ${DATA_DIRS//,/ })

  if [[ ! -s "$TMP_LIST" ]]; then
    fail "No snapshot files found for tag=$tag. Check DATA_DIRS and keyspaces."
  fi

  # Optional schema dump
  TMP_DIR="$(mktemp -d)"
  if [[ "$SNAPSHOT_SCHEMA" == "true" ]]; then
    log "Dumping schema"
    schema_dump "$TMP_DIR/schema.cql" || fail "schema dump failed"
  fi

  log "Packaging snapshot -> $out"
  # Strip leading / in stored paths to keep archive portable
  if [[ "$SNAPSHOT_SCHEMA" == "true" ]]; then
    tar --transform='s,^/,,' -cf - -T "$TMP_LIST" -C "$TMP_DIR" schema.cql \
      | eval "$(compress_cmd)" \
      | maybe_encrypt > "$out"
  else
    tar --transform='s,^/,,' -cf - -T "$TMP_LIST" \
      | eval "$(compress_cmd)" \
      | maybe_encrypt > "$out"
  fi

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"; s3_put "${out}.sha256"
  retention_prune "$base" "cassandra_snap_*.snapshot.tar.*"

  log "Clearing snapshot tag=$tag"
  NTOOL clearsnapshot -t "$tag" || log "Warning: clearsnapshot failed"

  # cleanup
  rm -f -- "$TMP_LIST"; TMP_LIST=""
  rm -rf -- "$TMP_DIR"; TMP_DIR=""
  trap - EXIT
  log "Snapshot backup finished: $out"
}

# ========= Incremental mode =========
do_incremental() {
  local tag="incr_$(ts)"
  local base="${OUTPUT_DIR%/}/incremental"
  mkdir -p "$base"
  local extc; extc="$(ext_for_compress)"
  local out="${base}/cassandra_${tag}.incremental.tar.${extc}"

  log "Ensuring incremental hardlinks are enabled (idempotent)"
  NTOOL enablebackup || log "enablebackup returned non-zero (maybe already enabled)"

  TMP_LIST="$(mktemp)"; trap trap_cleanup EXIT

  while IFS= read -r d; do
    d="$(echo "$d" | xargs)"; [[ -d "$d" ]] || continue
    if [[ "$KEYSPACES" == "all" ]]; then
      find "$d" -type f -path "*/backups/*" \
        | grep -Ev '/(system|system_schema|system_traces|system_distributed|system_auth)/' \
        >> "$TMP_LIST"
    else
      for ks in $(split_csv "$KEYSPACES"); do
        is_system_ks "$ks" && continue
        find "$d/$ks" -type f -path "*/backups/*" >> "$TMP_LIST" 2>/dev/null || true
      done
    fi
  done < <(printf "%s\n" ${DATA_DIRS//,/ })

  if [[ ! -s "$TMP_LIST" ]]; then
    log "No incremental files found. Nothing to do."
    return 0
  fi

  TMP_DIR="$(mktemp -d)"
  if [[ "$SNAPSHOT_SCHEMA" == "true" ]]; then
    log "Dumping schema"
    schema_dump "$TMP_DIR/schema.cql" || log "schema dump failed (continuing)"
  fi

  log "Packaging incremental -> $out"
  if [[ "$SNAPSHOT_SCHEMA" == "true" && -s "$TMP_DIR/schema.cql" ]]; then
    tar --transform='s,^/,,' -cf - -T "$TMP_LIST" -C "$TMP_DIR" schema.cql \
      | eval "$(compress_cmd)" \
      | maybe_encrypt > "$out"
  else
    tar --transform='s,^/,,' -cf - -T "$TMP_LIST" \
      | eval "$(compress_cmd)" \
      | maybe_encrypt > "$out"
  fi

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"; s3_put "${out}.sha256"
  retention_prune "$base" "cassandra_incr_*.incremental.tar.*"

  if [[ "$INCR_CLEAR" == "true" ]]; then
    log "Clearing incremental hardlinks that were archived"
    # Delete exactly the files listed
    xargs -r -d '\n' rm -f -- < "$TMP_LIST" || log "Warning: failed to clear some incremental files"
  fi

  rm -f -- "$TMP_LIST"; TMP_LIST=""
  rm -rf -- "$TMP_DIR"; TMP_DIR=""
  trap - EXIT
  log "Incremental backup finished: $out"
}

main() {
  check_tools
  log "Starting Cassandra backup: mode=$BACKUP_MODE"
  case "$BACKUP_MODE" in
    snapshot)    do_snapshot ;;
    incremental) do_incremental ;;
    *) fail "Unknown BACKUP_MODE=$BACKUP_MODE" ;;
  esac
  log "Backup finished."
}

main "$@"
