#!/usr/bin/env bash
set -euo pipefail

# -------- Config via ENV --------
: "${MONGODB_URI:?MONGODB_URI is required}"   # e.g. mongodb://user:pass@127.0.0.1:27017/?replicaSet=rs0
MONGO_DB="${MONGO_DB:-all}"                   # "all" or comma-separated list (db1,db2)
READ_PREFERENCE="${READ_PREFERENCE:-secondaryPreferred}"
DUMP_OPLOG="${DUMP_OPLOG:-true}"              # true to include --oplog (replica sets)

OUTPUT_DIR="${OUTPUT_DIR:-/backups}"
RETENTION_COUNT="${RETENTION_COUNT:-7}"

COMPRESS="${COMPRESS:-zstd}"                  # zstd | gzip | none
ZSTD_LEVEL="${ZSTD_LEVEL:-6}"
GZIP_LEVEL="${GZIP_LEVEL:-6}"

ENCRYPT="${ENCRYPT:-false}"                   # true | false
ENCRYPT_PASSWORD="${ENCRYPT_PASSWORD:-}"

S3_UPLOAD="${S3_UPLOAD:-false}"               # true | false
S3_BUCKET="${S3_BUCKET:-}"                    # s3://bucket/prefix
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-}"                # http://minio:9000  (API port)
S3_SSL_VERIFY="${S3_SSL_VERIFY:-true}"        # true | false
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-true}"

NUM_PARALLEL_COLLECTIONS="${NUM_PARALLEL_COLLECTIONS:-$(nproc || echo 2)}"

TZ=UTC
export TZ

# -------- Helpers --------
ts() { date -u +"%Y-%m-%dT%H-%M-%SZ"; }
log() { printf "[%s] %s\n" "$(ts)" "$*"; }
fail() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"; }

check_tools() {
  need mongodump
  need sha256sum
  [[ "$COMPRESS" == "zstd" ]] && need zstd
  [[ "$COMPRESS" == "gzip" ]] && need gzip
  [[ "$ENCRYPT" == "true" ]] && need openssl
  [[ "$S3_UPLOAD" == "true" ]] && need aws
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

s3_put() {
  local file="$1" rel
  rel="$(basename "$file")"
  if [[ "$S3_UPLOAD" == "true" ]]; then
    [[ -n "$S3_BUCKET" ]] || fail "S3_UPLOAD=true but S3_BUCKET is empty"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_S3_FORCE_PATH_STYLE
    local extra=()
    [[ -n "$S3_ENDPOINT" ]] && extra+=( "--endpoint-url" "$S3_ENDPOINT" )
    [[ "$S3_SSL_VERIFY" == "false" ]] && extra+=( "--no-verify-ssl" )
    aws s3 cp "${file}" "${S3_BUCKET%/}/$rel" "${extra[@]}"
  fi
}

retention_prune() {
  local dir="$1" pattern="$2"
  [[ -d "$dir" ]] || return 0
  mapfile -t files < <(ls -1t "$dir"/$pattern 2>/dev/null || true)
  local keep="$RETENTION_COUNT" i=0
  for f in "${files[@]}"; do
    ((i++))
    if (( i > keep )); then
      rm -f -- "$f" "$f.sha256"
    fi
  done
}

# Safe trap cleanup (works with set -u)
TMPDIR_TO_CLEANUP=""
trap_cleanup() {
  set +u
  if [[ -n "${TMPDIR_TO_CLEANUP:-}" ]]; then
    rm -rf -- "${TMPDIR_TO_CLEANUP}" || true
  fi
}

dump_scope_name() {
  # Use host:port (sanitized) as scope when MONGO_DB=all, else the DB name
  local scope hostport
  hostport="$(printf "%s" "$MONGODB_URI" | sed -E 's|^mongodb(\+srv)?:\/\/([^@/]+)@?([^/]+).*|\3|')"
  scope="${hostport//[:.,]/_}"
  if [[ "$MONGO_DB" == "all" ]]; then
    printf "cluster_%s" "${scope:-mongo}"
  else
    printf "%s" "$1"
  fi
}

do_dump_all() {
  local base="${OUTPUT_DIR%/}/dump/all"
  mkdir -p "$base"
  local extc; extc="$(ext_for_compress)"
  local stamp; stamp="$(ts)"
  local scope; scope="$(dump_scope_name)"
  local out="${base}/${scope}_${stamp}.mongodump.tar.${extc}"

  log "MongoDB dump (ALL dbs): -> $out"

  local tmpdir; tmpdir="$(mktemp -d)"
  TMPDIR_TO_CLEANUP="$tmpdir"
  trap trap_cleanup EXIT

  # Directory dump; capture oplog if requested (replica set)
  local args=( --uri "$MONGODB_URI" --readPreference "$READ_PREFERENCE" --numParallelCollections "$NUM_PARALLEL_COLLECTIONS" --out "$tmpdir" )
  [[ "$DUMP_OPLOG" == "true" ]] && args+=( --oplog )
  mongodump "${args[@]}"

  tar -cf - -C "$tmpdir" . \
    | eval "$(compress_cmd)" \
    | maybe_encrypt > "$out"

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"
  s3_put "${out}.sha256"

  rm -rf -- "$tmpdir" || true
  TMPDIR_TO_CLEANUP=""
  trap - EXIT

  retention_prune "$base" "cluster_*.mongodump.tar.*"
}

do_dump_dbs() {
  IFS=',' read -r -a dbs <<< "$MONGO_DB"
  for db in "${dbs[@]}"; do
    db="$(echo "$db" | xargs)"  # trim
    [[ -n "$db" ]] || continue
    local base="${OUTPUT_DIR%/}/dump/${db}"
    mkdir -p "$base"
    local extc; extc="$(ext_for_compress)"
    local stamp; stamp="$(ts)"
    local out="${base}/${db}_${stamp}.mongodump.tar.${extc}"

    log "MongoDB dump (db=${db}): -> $out"

    local tmpdir; tmpdir="$(mktemp -d)"
    TMPDIR_TO_CLEANUP="$tmpdir"
    trap trap_cleanup EXIT

    local args=( --uri "$MONGODB_URI" --readPreference "$READ_PREFERENCE" --numParallelCollections "$NUM_PARALLEL_COLLECTIONS" --db "$db" --out "$tmpdir" )
    [[ "$DUMP_OPLOG" == "true" ]] && args+=( --oplog )
    mongodump "${args[@]}"

    tar -cf - -C "$tmpdir" . \
      | eval "$(compress_cmd)" \
      | maybe_encrypt > "$out"

    sha256sum "$out" > "${out}.sha256"
    s3_put "$out"
    s3_put "${out}.sha256"

    rm -rf -- "$tmpdir" || true
    TMPDIR_TO_CLEANUP=""
    trap - EXIT

    retention_prune "$base" "${db}_*.mongodump.tar.*"
  done
}

main() {
  check_tools
  log "Starting MongoDB backup"
  if [[ "$MONGO_DB" == "all" ]]; then
    do_dump_all
  else
    do_dump_dbs
  fi
  log "Backup finished."
}

main "$@"
