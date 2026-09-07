#!/usr/bin/env bash
# migrate_mongodump.sh
# Migrates collections from finance -> quant_data using mongodump/mongorestore.
# Usage: ./migrate_mongodump.sh [--dry-run]

set -euo pipefail

# ── Load .env if present ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [[ -f "$ENV_FILE" ]]; then
    echo "[info] Loading environment from $ENV_FILE"
    set -o allexport
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +o allexport
fi

# ── Validate MONGO_URI ───────────────────────────────────────────────────────
if [[ -z "${MONGO_URI:-}" ]]; then
    echo "[error] MONGO_URI is not set. Export it or add it to .env." >&2
    exit 1
fi

# ── Parse flags ─────────────────────────────────────────────────────────────
DRY_RUN=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        *) echo "[warn] Unknown argument: $arg" ;;
    esac
done

# ── Collections: ordered smallest → largest ──────────────────────────────────
# Format: "name:expected_count"
COLLECTIONS=(
    "limit_down_ladder:3"
    "daily_limit_down:21"
    "market_etf_quotes_daily:260"
    "limit_up_ladder:302"
    "index_prices:1335"
    "index_constituents:0"
    "fund_holdings:0"
    "etf_prices:0"
    "etf_basic:1942"
    "market_spectrum:5172"
    "stock_info:5435"
    "industry_limitup_stats:5462"
    "sector_flow:10115"
    "etf_shares:11771"
    "etf_share_size:0"
    "daily_limit_up:20010"
    "etf_adj_factor:34516"
    "etf_daily:34516"
    "financial_indicator:72447"
    "financial_balance:90116"
    "financial_cashflow:94951"
    "financial_income:102119"
    "minute_bars:337989"
    "money_flow:1296582"
    "financial_daily_basic:1338989"
    "volume_price:16440045"
)

TEMP_DIR="/tmp/mongo_migration"
SRC_DB="finance"
DST_DB="quant_data"

# ── Helpers ──────────────────────────────────────────────────────────────────
pad_right() {
    local str="$1" width="$2"
    printf "%-${width}s" "$str"
}

format_count() {
    # Insert thousands separators
    printf "%d" "$1" | sed ':a;s/\B[0-9]\{3\}\b/,&/;ta'
}

elapsed() {
    local start="$1" end="$2"
    echo $(( end - start ))
}

# ── Dry-run banner ───────────────────────────────────────────────────────────
if $DRY_RUN; then
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║                      DRY-RUN MODE                               ║"
    echo "║  No commands will be executed.                                   ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Would run the following migrations (${#COLLECTIONS[@]} collections):"
    echo ""
    printf "  %-35s %15s\n" "COLLECTION" "EXPECTED DOCS"
    printf "  %-35s %15s\n" "─────────────────────────────────" "─────────────"
    for entry in "${COLLECTIONS[@]}"; do
        name="${entry%%:*}"
        count="${entry##*:}"
        printf "  %-35s %15s\n" "$name" "$(format_count "$count")"
    done
    echo ""
    echo "Commands that would be run for each collection (example: volume_price):"
    echo "  mongodump  --uri=\"\$MONGO_URI\" --db=$SRC_DB --collection=<name> --out=$TEMP_DIR"
    echo "  mongorestore --uri=\"\$MONGO_URI\" --db=$DST_DB --collection=<name> --drop $TEMP_DIR/$SRC_DB/<name>.bson"
    echo ""
    echo "Temp directory: $TEMP_DIR  (cleaned up afterwards)"
    exit 0
fi

# ── Pre-flight checks ────────────────────────────────────────────────────────
for cmd in mongodump mongorestore; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "[error] '$cmd' not found in PATH." >&2
        exit 1
    fi
done

# ── Setup temp dir ───────────────────────────────────────────────────────────
mkdir -p "$TEMP_DIR"
trap 'echo ""; echo "[info] Cleaning up $TEMP_DIR ..."; rm -rf "$TEMP_DIR"; echo "[info] Done."' EXIT

# ── Migration loop ───────────────────────────────────────────────────────────
TOTAL_START=$(date +%s)
PASS=0
FAIL=0
FAIL_LIST=()

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  Migration: $SRC_DB  →  $DST_DB"
echo "  Collections: ${#COLLECTIONS[@]}"
echo "  Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════════════════════════════"
echo ""

for entry in "${COLLECTIONS[@]}"; do
    name="${entry%%:*}"
    count="${entry##*:}"
    formatted_count="$(format_count "$count")"

    echo "──────────────────────────────────────────────────────────────────"
    echo "  Collection : $name"
    echo "  Expected   : $formatted_count documents"
    echo ""

    COL_START=$(date +%s)
    COL_TEMP="$TEMP_DIR/$SRC_DB"
    BSON_FILE="$COL_TEMP/$name.bson"

    # ── Step 1: dump ──────────────────────────────────────────────────────
    echo "  [1/2] mongodump  $SRC_DB.$name  →  $BSON_FILE"
    if mongodump \
        --uri="$MONGO_URI" \
        --db="$SRC_DB" \
        --collection="$name" \
        --out="$TEMP_DIR" \
        2>&1 | sed 's/^/        /'; then
        echo "        dump OK"
    else
        echo "  [FAIL] mongodump failed for $name" >&2
        (( FAIL++ )) || true
        FAIL_LIST+=("$name")
        rm -rf "$COL_TEMP"
        continue
    fi

    # ── Step 2: restore ───────────────────────────────────────────────────
    echo "  [2/2] mongorestore  $BSON_FILE  →  $DST_DB.$name  (--drop)"
    if mongorestore \
        --uri="$MONGO_URI" \
        --db="$DST_DB" \
        --collection="$name" \
        --drop \
        "$BSON_FILE" \
        2>&1 | sed 's/^/        /'; then
        echo "        restore OK"
    else
        echo "  [FAIL] mongorestore failed for $name" >&2
        (( FAIL++ )) || true
        FAIL_LIST+=("$name")
        rm -rf "$COL_TEMP"
        continue
    fi

    COL_END=$(date +%s)
    echo ""
    echo "  [OK] $name  completed in $(elapsed "$COL_START" "$COL_END")s"
    (( PASS++ )) || true

    # Clean up this collection's dump to save disk space
    rm -rf "$COL_TEMP"
done

# ── Summary ──────────────────────────────────────────────────────────────────
TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$(elapsed "$TOTAL_START" "$TOTAL_END")

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  MIGRATION SUMMARY"
echo "  Finished : $(date '+%Y-%m-%d %H:%M:%S')"
echo "  Duration : ${TOTAL_ELAPSED}s  ($(( TOTAL_ELAPSED / 60 ))m $(( TOTAL_ELAPSED % 60 ))s)"
echo "  Passed   : $PASS"
echo "  Failed   : $FAIL"
if [[ ${#FAIL_LIST[@]} -gt 0 ]]; then
    echo ""
    echo "  Failed collections:"
    for fc in "${FAIL_LIST[@]}"; do
        echo "    - $fc"
    done
fi
echo "════════════════════════════════════════════════════════════════════"
echo ""

[[ $FAIL -eq 0 ]] && exit 0 || exit 1
