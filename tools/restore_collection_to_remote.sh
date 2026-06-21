#!/usr/bin/env bash
# restore_collection_to_remote.sh
# Dump one local MongoDB collection and restore it to the same namespace on a remote server.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ -f "$ENV_FILE" ]]; then
    echo "[info] Loading environment from $ENV_FILE"
    set -o allexport
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +o allexport
fi

LOCAL_MONGO_URI="${LOCAL_MONGO_URI:-${MONGO_URI:-}}"
REMOTE_MONGO_URI="${REMOTE_MONGO_URI:-}"
REMOTE_SSH="${REMOTE_SSH:-}"
DB_NAME="${DB_NAME:-}"
COLLECTION_NAME="${COLLECTION_NAME:-}"
REMOTE_WORKDIR="${REMOTE_WORKDIR:-/tmp/quant-data-engine-mongo-restore}"
REMOTE_ARCHIVE="${REMOTE_ARCHIVE:-}"
DROP_COLLECTION=false
DRY_RUN=false
KEEP_LOCAL_DUMP=false
DELETE_REMOTE_ARCHIVE=false
ACTION="all"

usage() {
    cat <<'EOF'
Usage:
  # End-to-end: dump, upload, then restore. This is the default action.
  ./restore_collection_to_remote.sh [all] \
    --db <database> \
    --collection <collection> \
    --ssh <user@host> \
    --remote-uri <mongodb-uri> \
    [--local-uri <mongodb-uri>] \
    [--remote-dir <path>] \
    [--drop] \
    [--dry-run] \
    [--keep-local-dump] \
    [--delete-remote-archive]

  # Split step 1: dump locally and upload the archive to the remote server.
  ./restore_collection_to_remote.sh upload \
    --db <database> \
    --collection <collection> \
    --ssh <user@host> \
    [--local-uri <mongodb-uri>] \
    [--remote-dir <path>] \
    [--remote-archive <path>] \
    [--keep-local-dump]

  # Split step 2: restore a remote archive into the remote database.
  ./restore_collection_to_remote.sh restore \
    --db <database> \
    --collection <collection> \
    --ssh <user@host> \
    --remote-uri <mongodb-uri> \
    [--remote-dir <path>] \
    [--remote-archive <path>] \
    [--drop] \
    [--dry-run] \
    [--delete-remote-archive]

Environment variables:
  LOCAL_MONGO_URI   Source MongoDB URI. Falls back to MONGO_URI.
  REMOTE_MONGO_URI  Target MongoDB URI used on the remote server.
  REMOTE_SSH        Remote SSH target, for example ubuntu@1.2.3.4.
  DB_NAME           Database name.
  COLLECTION_NAME   Collection name.
  REMOTE_WORKDIR    Remote temporary directory.
  REMOTE_ARCHIVE    Remote archive path to restore.

Notes:
  Actions are all, upload, or restore. If omitted, action defaults to all.
  --remote-dir defaults to /tmp/quant-data-engine-mongo-restore.
  upload creates a timestamped .archive.gz when --remote-archive is omitted.
  restore uses --remote-archive when provided, otherwise it restores the newest
  matching archive in --remote-dir for the selected database and collection.
  Remote archives are kept by default. Use --delete-remote-archive to remove one
  after a successful non-dry-run restore.
  --drop is required if you want to replace the remote collection.
  Without --drop, mongorestore inserts documents into the existing collection.
  --dry-run validates dump, ssh, scp, and remote mongorestore without writing data.
EOF
}

redact_uri() {
    printf '%s' "$1" | sed -E 's#(mongodb(\+srv)?://[^:/@]+):[^@]+@#\1:***@#'
}

require_value() {
    local flag="$1"
    local value="${2:-}"
    if [[ -z "$value" || "$value" == --* ]]; then
        echo "[error] $flag requires a value." >&2
        exit 1
    fi
}

if [[ $# -gt 0 && "$1" != --* ]]; then
    ACTION="$1"
    shift
fi

case "$ACTION" in
    all|upload|restore) ;;
    *)
        echo "[error] Unknown action: $ACTION" >&2
        usage
        exit 1
        ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local-uri)
            require_value "$1" "${2:-}"
            LOCAL_MONGO_URI="$2"
            shift 2
            ;;
        --remote-uri)
            require_value "$1" "${2:-}"
            REMOTE_MONGO_URI="$2"
            shift 2
            ;;
        --db)
            require_value "$1" "${2:-}"
            DB_NAME="$2"
            shift 2
            ;;
        --collection)
            require_value "$1" "${2:-}"
            COLLECTION_NAME="$2"
            shift 2
            ;;
        --ssh)
            require_value "$1" "${2:-}"
            REMOTE_SSH="$2"
            shift 2
            ;;
        --remote-dir)
            require_value "$1" "${2:-}"
            REMOTE_WORKDIR="$2"
            shift 2
            ;;
        --remote-archive)
            require_value "$1" "${2:-}"
            REMOTE_ARCHIVE="$2"
            shift 2
            ;;
        --drop)
            DROP_COLLECTION=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --delete-remote-archive)
            DELETE_REMOTE_ARCHIVE=true
            shift
            ;;
        --keep-local-dump)
            KEEP_LOCAL_DUMP=true
            shift
            ;;
        --keep-dump)
            KEEP_LOCAL_DUMP=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "[error] Unknown argument: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ "$ACTION" != "restore" && -z "$LOCAL_MONGO_URI" ]]; then
    echo "[error] LOCAL_MONGO_URI is not set. Pass --local-uri or export LOCAL_MONGO_URI/MONGO_URI." >&2
    exit 1
fi

if [[ "$ACTION" != "upload" && -z "$REMOTE_MONGO_URI" ]]; then
    echo "[error] REMOTE_MONGO_URI is not set. Pass --remote-uri or export REMOTE_MONGO_URI." >&2
    exit 1
fi

if [[ -z "$REMOTE_SSH" ]]; then
    echo "[error] REMOTE_SSH is not set. Pass --ssh or export REMOTE_SSH." >&2
    exit 1
fi

if [[ -z "$DB_NAME" || -z "$COLLECTION_NAME" ]]; then
    echo "[error] Both --db and --collection are required." >&2
    exit 1
fi

if [[ "$REMOTE_WORKDIR" =~ [[:space:]] ]]; then
    echo "[error] REMOTE_WORKDIR must not contain whitespace because it is used by scp." >&2
    exit 1
fi

if [[ -n "$REMOTE_ARCHIVE" && "$REMOTE_ARCHIVE" =~ [[:space:]] ]]; then
    echo "[error] REMOTE_ARCHIVE must not contain whitespace because it may be used by scp." >&2
    exit 1
fi

REQUIRED_CMDS=(ssh sed date)
if [[ "$ACTION" != "restore" ]]; then
    REQUIRED_CMDS+=(mongodump scp mktemp)
fi

for cmd in "${REQUIRED_CMDS[@]}"; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "[error] '$cmd' not found in PATH." >&2
        exit 1
    fi
done

SAFE_DB="${DB_NAME//[^[:alnum:]_.-]/_}"
SAFE_COLLECTION="${COLLECTION_NAME//[^[:alnum:]_.-]/_}"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
ARCHIVE_NAME="${SAFE_DB}_${SAFE_COLLECTION}_${TIMESTAMP}.archive.gz"
TEMP_DIR=""
ARCHIVE_FILE=""
if [[ "$ACTION" != "restore" ]]; then
    TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mongo_collection_restore.XXXXXX")"
    ARCHIVE_FILE="$TEMP_DIR/$ARCHIVE_NAME"
fi
GENERATED_REMOTE_ARCHIVE="$REMOTE_WORKDIR/$ARCHIVE_NAME"

if [[ -z "$REMOTE_ARCHIVE" && "$ACTION" != "restore" ]]; then
    REMOTE_ARCHIVE="$GENERATED_REMOTE_ARCHIVE"
fi

cleanup() {
    if [[ "$ACTION" != "restore" && "$KEEP_LOCAL_DUMP" == true ]]; then
        echo "[info] Keeping local dump: $ARCHIVE_FILE"
    elif [[ -n "$TEMP_DIR" ]]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

echo ""
echo "Mongo collection remote restore"
echo "  Action     : $ACTION"
if [[ "$ACTION" != "restore" ]]; then
    echo "  Source URI : $(redact_uri "$LOCAL_MONGO_URI")"
fi
echo "  Target SSH : $REMOTE_SSH"
if [[ "$ACTION" != "upload" ]]; then
    echo "  Target URI : $(redact_uri "$REMOTE_MONGO_URI")"
fi
echo "  Namespace  : $DB_NAME.$COLLECTION_NAME"
echo "  Drop first : $DROP_COLLECTION"
echo "  Dry run    : $DRY_RUN"
echo "  Workdir    : $REMOTE_WORKDIR"
if [[ -n "$REMOTE_ARCHIVE" ]]; then
    echo "  Archive    : $REMOTE_ARCHIVE"
else
    echo "  Archive    : latest matching archive in remote workdir"
fi
echo ""

if [[ "$DRY_RUN" == true ]]; then
    echo "[dry-run] Validation mode is enabled."
    if [[ "$DROP_COLLECTION" == true ]]; then
        echo "[dry-run] --drop was provided but will not be used during validation."
    fi
    echo ""
fi

if [[ "$ACTION" != "restore" ]]; then
    echo "[upload 1/3] Dumping local collection..."
    mongodump \
        --uri="$LOCAL_MONGO_URI" \
        --db="$DB_NAME" \
        --collection="$COLLECTION_NAME" \
        --archive="$ARCHIVE_FILE" \
        --gzip

    echo "[upload 2/3] Preparing remote directory..."
    ssh "$REMOTE_SSH" bash -s -- "$REMOTE_WORKDIR" "$DRY_RUN" "$ACTION" <<'REMOTE_SCRIPT'
set -euo pipefail

REMOTE_WORKDIR="$1"
DRY_RUN="$2"
ACTION="$3"

mkdir -p "$REMOTE_WORKDIR"

if [[ "$ACTION" != "upload" ]]; then
    command -v mongorestore >/dev/null
fi

if [[ "$DRY_RUN" == true && "$ACTION" != "upload" ]]; then
    if [[ "$(mongorestore --help 2>&1)" != *"--dryRun"* ]]; then
        echo "[error] Remote mongorestore does not support --dryRun. Please upgrade MongoDB Database Tools." >&2
        exit 1
    fi
fi
REMOTE_SCRIPT

    echo "[upload 3/3] Uploading archive..."
    scp "$ARCHIVE_FILE" "$REMOTE_SSH:$REMOTE_ARCHIVE"

    echo ""
    echo "[upload done] Uploaded archive:"
    echo "  $REMOTE_ARCHIVE"

    if [[ "$ACTION" == "upload" ]]; then
        echo ""
        echo "Next restore command:"
        echo "  ./restore_collection_to_remote.sh restore \\"
        echo "    --db $DB_NAME \\"
        echo "    --collection $COLLECTION_NAME \\"
        echo "    --ssh $REMOTE_SSH \\"
        echo "    --remote-uri \"${REMOTE_MONGO_URI:-mongodb://127.0.0.1:27017}\" \\"
        echo "    --remote-archive \"$REMOTE_ARCHIVE\""
        exit 0
    fi
fi

echo "[restore 1/2] Resolving remote archive..."
REMOTE_ARCHIVE_ARG="${REMOTE_ARCHIVE:-__AUTO__}"
RESOLVED_REMOTE_ARCHIVE="$(
    ssh "$REMOTE_SSH" bash -s -- \
        "$REMOTE_WORKDIR" \
        "$REMOTE_ARCHIVE_ARG" \
        "$SAFE_DB" \
        "$SAFE_COLLECTION" \
        "$DRY_RUN" <<'REMOTE_SCRIPT'
set -euo pipefail

REMOTE_WORKDIR="$1"
REMOTE_ARCHIVE="$2"
SAFE_DB="$3"
SAFE_COLLECTION="$4"
DRY_RUN="$5"

command -v mongorestore >/dev/null

if [[ "$DRY_RUN" == true ]]; then
    if [[ "$(mongorestore --help 2>&1)" != *"--dryRun"* ]]; then
        echo "[error] Remote mongorestore does not support --dryRun. Please upgrade MongoDB Database Tools." >&2
        exit 1
    fi
fi

if [[ "$REMOTE_ARCHIVE" != "__AUTO__" ]]; then
    if [[ ! -f "$REMOTE_ARCHIVE" ]]; then
        echo "[error] Remote archive not found: $REMOTE_ARCHIVE" >&2
        exit 1
    fi
    printf '%s\n' "$REMOTE_ARCHIVE"
    exit 0
fi

shopt -s nullglob
matches=( "$REMOTE_WORKDIR/${SAFE_DB}_${SAFE_COLLECTION}_"*.archive.gz )
if [[ ${#matches[@]} -eq 0 ]]; then
    echo "[error] No matching remote archive found in $REMOTE_WORKDIR." >&2
    echo "[error] Expected pattern: ${SAFE_DB}_${SAFE_COLLECTION}_*.archive.gz" >&2
    exit 1
fi

latest=""
for archive in "${matches[@]}"; do
    latest="$archive"
done
printf '%s\n' "$latest"
REMOTE_SCRIPT
)"

echo "  $RESOLVED_REMOTE_ARCHIVE"

if [[ "$DRY_RUN" == true ]]; then
    echo "[restore 2/2] Validating restore on remote server..."
else
    echo "[restore 2/2] Restoring on remote server..."
fi

ssh "$REMOTE_SSH" bash -s -- \
    "$RESOLVED_REMOTE_ARCHIVE" \
    "$REMOTE_MONGO_URI" \
    "$DB_NAME" \
    "$COLLECTION_NAME" \
    "$DROP_COLLECTION" \
    "$DRY_RUN" \
    "$DELETE_REMOTE_ARCHIVE" <<'REMOTE_SCRIPT'
set -euo pipefail

REMOTE_ARCHIVE="$1"
REMOTE_MONGO_URI="$2"
DB_NAME="$3"
COLLECTION_NAME="$4"
DROP_COLLECTION="$5"
DRY_RUN="$6"
DELETE_REMOTE_ARCHIVE="$7"

RESTORE_ARGS=(
    --uri="$REMOTE_MONGO_URI"
    --archive="$REMOTE_ARCHIVE"
    --gzip
    --nsInclude="$DB_NAME.$COLLECTION_NAME"
)

if [[ "$DRY_RUN" == true ]]; then
    RESTORE_ARGS+=(--dryRun)
elif [[ "$DROP_COLLECTION" == true ]]; then
    RESTORE_ARGS+=(--drop)
fi

mongorestore "${RESTORE_ARGS[@]}"

if [[ "$DRY_RUN" != true && "$DELETE_REMOTE_ARCHIVE" == true ]]; then
    rm -f "$REMOTE_ARCHIVE"
    echo "[info] Deleted remote archive: $REMOTE_ARCHIVE"
else
    echo "[info] Kept remote archive: $REMOTE_ARCHIVE"
fi
REMOTE_SCRIPT

echo ""
if [[ "$DRY_RUN" == true ]]; then
    echo "[done] Dry run passed for $DB_NAME.$COLLECTION_NAME to $REMOTE_SSH"
else
    echo "[done] Restored $DB_NAME.$COLLECTION_NAME to $REMOTE_SSH"
fi
