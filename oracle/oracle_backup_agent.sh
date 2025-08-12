#!/usr/bin/env bash
set -euo pipefail

# ---------- Required connection ----------
# Use ONE of these:
# 1) OS auth (run as oracle user on the DB host): ORACLE_CONNECT="/ as sysdba"
# 2) Net service string: "system/password@//127.0.0.1:1521/ORCLPDB1"
: "${ORACLE_CONNECT:?Set ORACLE_CONNECT, e.g. \"/ as sysdba\" or \"user/pass@//host:1521/SERVICE\"}"

# ---------- Mode ----------
BACKUP_TYPE="${BACKUP_TYPE:-datapump}"   # datapump | rman

# ---------- Common ----------
OUTPUT_DIR="${OUTPUT_DIR:-/backups}"     # host/container path
RETENTION_COUNT="${RETENTION_COUNT:-7}"
COMPRESS="${COMPRESS:-zstd}"             # zstd | gzip | none
ZSTD_LEVEL="${ZSTD_LEVEL:-6}"
GZIP_LEVEL="${GZIP_LEVEL:-6}"
ENCRYPT="${ENCRYPT:-false}"              # true | false
ENCRYPT_PASSWORD="${ENCRYPT_PASSWORD:-}"

# S3/MinIO
S3_UPLOAD="${S3_UPLOAD:-false}"
S3_BUCKET="${S3_BUCKET:-}"               # s3://bucket/prefix
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
S3_ENDPOINT="${S3_ENDPOINT:-}"           # http://minio:9000
S3_SSL_VERIFY="${S3_SSL_VERIFY:-true}"
AWS_S3_FORCE_PATH_STYLE="${AWS_S3_FORCE_PATH_STYLE:-true}"

TZ=UTC; export TZ

ts() { date -u +"%Y-%m-%dT%H-%M-%SZ"; }
log(){ printf "[%s] %s\n" "$(ts)" "$*"; }
fail(){ log "ERROR: $*"; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1 || fail "Missing command: $1"; }

compress_cmd(){
  case "$COMPRESS" in
    zstd) echo "zstd -T0 -$ZSTD_LEVEL" ;;
    gzip) echo "gzip -$GZIP_LEVEL" ;;
    none) echo "cat" ;;
    *) fail "Unknown COMPRESS=$COMPRESS" ;;
  esac
}
ext_for_compress(){ case "$COMPRESS" in zstd) echo zst;; gzip) echo gz;; none) echo bin;; esac; }

maybe_encrypt(){
  if [[ "$ENCRYPT" == "true" ]]; then
    [[ -n "$ENCRYPT_PASSWORD" ]] || fail "ENCRYPT=true but ENCRYPT_PASSWORD empty"
    openssl enc -aes-256-cbc -salt -pbkdf2 -pass "pass:$ENCRYPT_PASSWORD"
  else cat; fi
}

s3_put(){
  local f="$1" rel; rel="$(basename "$f")"
  if [[ "$S3_UPLOAD" == "true" ]]; then
    [[ -n "$S3_BUCKET" ]] || fail "S3_BUCKET empty"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_S3_FORCE_PATH_STYLE
    local extra=()
    [[ -n "$S3_ENDPOINT" ]] && extra+=( --endpoint-url "$S3_ENDPOINT" )
    [[ "$S3_SSL_VERIFY" == "false" ]] && extra+=( --no-verify-ssl )
    aws s3 cp "$f" "${S3_BUCKET%/}/$rel" "${extra[@]}"
  fi
}

prune(){
  local dir="$1" pattern="$2"
  [[ -d "$dir" ]] || return 0
  mapfile -t files < <(ls -1t "$dir"/$pattern 2>/dev/null || true)
  local i=0
  for f in "${files[@]}"; do
    ((i++))
    (( i>RETENTION_COUNT )) && rm -f -- "$f" "$f.sha256"
  done
}

TMPDIR_TO_CLEANUP=""
cleanup(){ set +u; [[ -n "${TMPDIR_TO_CLEANUP:-}" ]] && rm -rf -- "$TMPDIR_TO_CLEANUP" || true; }
trap cleanup EXIT

check_common_tools(){ need tar; need sha256sum; [[ "$COMPRESS" == "zstd" ]] && need zstd; [[ "$COMPRESS" == "gzip" ]] && need gzip; [[ "$ENCRYPT" == "true" ]] && need openssl; [[ "$S3_UPLOAD" == "true" ]] && need aws; }

# ---------- Data Pump (expdp) ----------
# Scope controls: full DB or specific objects
DP_SCOPE="${DP_SCOPE:-full}"             # full | schemas | tablespaces
DP_SCHEMAS="${DP_SCHEMAS:-}"             # comma list if DP_SCOPE=schemas
DP_TABLESPACES="${DP_TABLESPACES:-}"     # comma list if DP_SCOPE=tablespaces
DP_PARALLEL="${DP_PARALLEL:-$(nproc || echo 2)}"
DP_DIR_NAME="${DP_DIR_NAME:-BACKUP_DIR}" # Oracle DIRECTORY object name
DP_DIR_PATH="${DP_DIR_PATH:-$OUTPUT_DIR/oracle/dpump}" # filesystem path on DB host
CREATE_DIR_IF_MISSING="${CREATE_DIR_IF_MISSING:-true}" # auto-create DIRECTORY

expdp_exists(){ need expdp; need sqlplus; }

ensure_directory_object(){
  mkdir -p "$DP_DIR_PATH"
  # Create DIRECTORY object if missing and point it to DP_DIR_PATH
  if [[ "$CREATE_DIR_IF_MISSING" == "true" ]]; then
    sqlplus -s "$ORACLE_CONNECT" <<SQL || fail "Failed to ensure DIRECTORY"
WHENEVER SQLERROR EXIT 1
DECLARE cnt NUMBER; BEGIN
  SELECT COUNT(*) INTO cnt FROM all_directories WHERE directory_name=UPPER('$DP_DIR_NAME');
  IF cnt=0 THEN
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY $DP_DIR_NAME AS ''''$DP_DIR_PATH'''''';
  ELSE
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY $DP_DIR_NAME AS ''''$DP_DIR_PATH'''''';
  END IF;
END;
/
GRANT READ, WRITE ON DIRECTORY $DP_DIR_NAME TO PUBLIC;
EXIT
SQL
  fi
}

do_datapump(){
  expdp_exists; check_common_tools
  ensure_directory_object

  local stamp outdir dumpbase extc
  stamp="$(ts)"
  outdir="${OUTPUT_DIR%/}/datapump"
  mkdir -p "$outdir"
  extc="$(ext_for_compress)"
  dumpbase="dp_${stamp}"
  TMPDIR_TO_CLEANUP="$(mktemp -d)"

  log "Starting Data Pump export: scope=$DP_SCOPE parallel=$DP_PARALLEL dir=$DP_DIR_NAME -> $outdir"
  local dp_args=( directory="${DP_DIR_NAME}" logfile="${dumpbase}.log" parallel="${DP_PARALLEL}" )
  case "$DP_SCOPE" in
    full)        dp_args+=( full=y dumpfile="${dumpbase}_%U.dmp" ) ;;
    schemas)     [[ -n "$DP_SCHEMAS" ]] || fail "DP_SCHEMAS empty"
                 dp_args+=( schemas="${DP_SCHEMAS}" dumpfile="${dumpbase}_%U.dmp" ) ;;
    tablespaces) [[ -n "$DP_TABLESPACES" ]] || fail "DP_TABLESPACES empty"
                 dp_args+=( tablespaces="${DP_TABLESPACES}" dumpfile="${dumpbase}_%U.dmp" ) ;;
    *) fail "Unknown DP_SCOPE=$DP_SCOPE" ;;
  esac

  # Kick off export (server-side writes into DP_DIR_PATH)
  expdp "$ORACLE_CONNECT" "${dp_args[@]}"

  # Collect generated files
  shopt -s nullglob
  mapfile -t files < <(ls -1 "${DP_DIR_PATH}/${dumpbase}"*.dmp 2>/dev/null || true)
  [[ "${#files[@]}" -gt 0 ]] || fail "No .dmp files produced in ${DP_DIR_PATH}"
  # package dumps + log
  local out="${outdir}/oracle_${stamp}.datapump.tar.${extc}"
  log "Packaging Data Pump -> $out"
  tar -cf - -C "${DP_DIR_PATH}" "${dumpbase}.log" $(printf "%q " "${files[@]##${DP_DIR_PATH}/}") \
    | eval "$(compress_cmd)" | maybe_encrypt > "$out"

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"; s3_put "${out}.sha256"
  prune "$outdir" "oracle_*.datapump.tar.*"

  # Optional: leave dump pieces behind; uncomment to remove raw .dmp after packaging
  # rm -f -- "${files[@]}" "${DP_DIR_PATH}/${dumpbase}.log" || true

  log "Data Pump export finished."
}

# ---------- RMAN ----------
RMAN_PARALLEL="${RMAN_PARALLEL:-2}"
RMAN_INCLUDE_ARCHIVELOG="${RMAN_INCLUDE_ARCHIVELOG:-true}" # include archivelogs

rman_exists(){ need rman; need sqlplus; check_common_tools; }

do_rman(){
  rman_exists
  local stamp outdir workdir extc
  stamp="$(ts)"
  outdir="${OUTPUT_DIR%/}/rman"
  workdir="$(mktemp -d)"; TMPDIR_TO_CLEANUP="$workdir"
  mkdir -p "$outdir"
  extc="$(ext_for_compress)"
  local tag="rman_${stamp}"

  log "Starting RMAN backupset -> $workdir (tag=$tag parallel=$RMAN_PARALLEL)"
  # RMAN script writes backup pieces into $workdir
  rman target "$ORACLE_CONNECT" <<RMAN || fail "RMAN failed"
CONFIGURE DEVICE TYPE DISK PARALLELISM ${RMAN_PARALLEL};
RUN {
  ALLOCATE CHANNEL c1 DEVICE TYPE DISK FORMAT '${workdir}/bk_%d_%T_%U.bkp';
  BACKUP AS COMPRESSED BACKUPSET DATABASE TAG '${tag}';
  ${RMAN_INCLUDE_ARCHIVELOG,,} == "true"
  BACKUP AS COMPRESSED BACKUPSET ARCHIVELOG ALL NOT BACKED UP TAG '${tag}';
  DELETE NOPROMPT OBSOLETE;
  RELEASE CHANNEL c1;
}
EXIT
RMAN

  shopt -s nullglob
  mapfile -t pieces < <(ls -1 "${workdir}/"*.bkp 2>/dev/null || true)
  [[ "${#pieces[@]}" -gt 0 ]] || fail "No RMAN pieces produced"
  local out="${outdir}/oracle_${stamp}.rman.tar.${extc}"
  log "Packaging RMAN -> $out"
  tar -cf - -C "$workdir" $(printf "%q " "${pieces[@]##${workdir}/}") \
    | eval "$(compress_cmd)" | maybe_encrypt > "$out"

  sha256sum "$out" > "${out}.sha256"
  s3_put "$out"; s3_put "${out}.sha256"
  prune "$outdir" "oracle_*.rman.tar.*"
  log "RMAN backup finished."
}

main(){
  log "Starting Oracle backup: type=$BACKUP_TYPE"
  case "$BACKUP_TYPE" in
    datapump) do_datapump ;;
    rman)     do_rman ;;
    *)        fail "Unknown BACKUP_TYPE=$BACKUP_TYPE" ;;
  esac
  log "Backup finished."
}

main "$@"
