#!/usr/bin/env bash
set -euo pipefail

# -------- Config via ENV --------
: "${PGHOST:?PGHOST is required}"
: "${PGUSER:?PGUSER is required}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-all}"           # "all" or a single DB name
PGPASSWORD="${PGPASSWORD:-}"              # or use PGPASSFILE / .pgpass

BACKUP_TYPE="${BACKUP_TYPE:-dump}"        # "dump" | "basebackup"
OUTPUT_DIR="${OUTPUT_DIR:-/backups}"
RETENTION_COUNT="${RETENTION_COUNT:-7}"   # keep last N backups
JOBS="${JOBS:-$(nproc || echo 2)}"        # parallel jobs for pg_dump

COMPRESS="${COMPRESS:-zstd}"              # "zstd" | "gzip" | "none"
ZSTD_LEVEL="${ZSTD_LEVEL:-6}"
GZIP_LEVEL="${GZIP_LEVEL:-6}"

ENCRYPT="${ENCRYPT:-false}"               # "true" | "false"
ENCRYPT_PASSWORD="${ENCRYPT_PASSWORD:-}"  # used if ENCRYPT=true

S3_UPLOAD="${S3_UPLOAD:-false}"           # "true" | "false"
S3_BUCKET="${S3_BUCKET:-}"                # e.g. s3://my-bucket/backups
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-}"            # e.g. http://minio:9000 (API port)
S3_SSL_VERIFY="${S3_SSL_VERIFY:-true}"    # "true" | "false"
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-true}"

TZ=UTC
export TZ

# -------- Helpers --------
ts() { date -u +"%Y-%m-%dT%H-%M-%SZ"; }
log() { printf "[%s] %s\n" "$(ts)" "$*"; }
fail() { log "ERROR: $*"; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"; }

check_tools() {
  need psql
  need pg_dump
  need pg_basebackup
  need sha256sum
  [[ "$COMPRESS" == "zstd" ]] && need zstd
  [[ "$COMPRESS" == "gzip" ]] && need gzip
  [[ "$ENCRYPT" == "true" ]] && need openssl
  [[ "$S3_UPLOAD" == "true" ]] && need aws
}

psql_ping() {
  PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -U "$PGUSER" -p "$PGPORT" -d postgres -Atc "SELECT 1" >/dev/null
}

list_dbs() {
  PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -U "$PGUSER" -p "$PGPORT" -d postgres -Atc \
    "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY 1;"
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

# Global temp dir pointer used only by trap (safe with set -u)
TMPDIR_TO_CLEANUP=""

trap_cleanup() {
  set +u
  if [[ -n "${TMPDIR_TO_CLEANUP:-}" ]]; then
    rm -rf -- "${TMPDIR_TO_CLEANUP}" || true
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

do_dump_one() {
  local db="$1"
  local base="${OUTPUT_DIR%/}/dump/${db}"
  mkdir -p "$base"
  local extc; extc="$(ext_for_compress)"
  local stamp; stamp="$(ts)"
  local out="${base}/${db}_${stamp}.dump.tar.${extc}"

  log "Logical dump (directory format, jobs=${JOBS}): db=$db -> $out"

  # Create temp dir and enable safe cleanup trap
  local tmpdir
  tmpdir="$(mktemp -d)"
  TMPDIR_TO_CLEANUP="$tmpdir"
  trap trap_cleanup EXIT

  # Directory format enables parallel jobs (-j)
  PGPASSWORD="$PGPASSWORD" pg_dump -h "$PGHOST" -U "$PGUSER" -p "$PGPORT" \
    -d "$db" -Fd -j "$JOBS" -f "$tmpdir"

  # Package and (optionally) compress/encrypt
  tar -cf - -C "$tmpdir" . \
    | eval "$(compress_cmd)" \
    | maybe_encrypt > "$out"

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"
  s3_put "${out}.sha256"

  # Cleanup for this DB and disable trap for next iteration
  rm -rf -- "$tmpdir" || true
  TMPDIR_TO_CLEANUP=""
  trap - EXIT

  retention_prune "$base" "${db}_*.dump.tar.*"
}

do_basebackup() {
  local base="${OUTPUT_DIR%/}/basebackup/${PGHOST}_${PGPORT}"
  mkdir -p "$base"
  local file="${base}/basebackup_$(ts).tar.gz"
  log "Physical base backup -> $file"
  PGPASSWORD="$PGPASSWORD" pg_basebackup -h "$PGHOST" -U "$PGUSER" -p "$PGPORT" \
    -D - -Ft -z -Z "${GZIP_LEVEL}" --wal-method=stream --progress \
    | maybe_encrypt > "$file"

  sha256sum "$file" > "${file}.sha256"
  s3_put "$file"
  s3_put "${file}.sha256"
  retention_prune "$base" "basebackup_*.tar.gz"
}

main() {
  check_tools
  log "Starting PostgreSQL backup: type=$BACKUP_TYPE host=$PGHOST port=$PGPORT db=$PGDATABASE"
  psql_ping || fail "Cannot connect to PostgreSQL on ${PGHOST}:${PGPORT}"

  case "$BACKUP_TYPE" in
    dump)
      if [[ "$PGDATABASE" == "all" ]]; then
        while IFS= read -r db; do
          do_dump_one "$db"
        done < <(list_dbs)
      else
        do_dump_one "$PGDATABASE"
      fi
      ;;
    basebackup)
      do_basebackup
      ;;
    *)
      fail "Unknown BACKUP_TYPE=$BACKUP_TYPE"
      ;;
  esac

  log "Backup finished."
}

main "$@"
