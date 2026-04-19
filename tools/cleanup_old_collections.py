"""Batch-drop migrated collections from the old `finance` database.

Safety: Before dropping, verifies each collection EXISTS in `quant_data` with a
non-zero document count.  Dry-run is the default — pass --confirm to actually drop.

Usage:
    python tools/cleanup_old_collections.py              # dry-run (safe)
    python tools/cleanup_old_collections.py --confirm    # actually drop
"""

import argparse
import os
import sys
import pathlib

# ---------------------------------------------------------------------------
# Load .env from quantFinance/.env (same pattern as other tools)
# ---------------------------------------------------------------------------
from dotenv import load_dotenv

_env_path = pathlib.Path(__file__).parent.parent.parent / "quantFinance" / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)
else:
    load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Collections to clean up (mirrors migration script)
# ---------------------------------------------------------------------------
MIGRATED_COLLECTIONS = [
    "volume_price",
    "financial_daily_basic",
    "financial_income",
    "financial_balance",
    "financial_cashflow",
    "financial_indicator",
    "money_flow",
    "minute_bars",
    "daily_limit_up",
    "daily_limit_down",
    "limit_up_ladder",
    "limit_down_ladder",
    "sector_flow",
    "panic_sector_flow",
    "etf_basic",
    "etf_daily",
    "etf_adj_factor",
    "etf_shares",
    "etf_share_size",
    "earnings_forecast",
    "earnings_express",
    "earnings_disclosure",
    "earnings_report_rc",
    "market_industry_quotes_daily",
    "market_etf_quotes_daily",
    "industry_limitup_stats",
    "market_spectrum",
    "market_spectrum_minute",
    "stock_info",
    "stock_basic",
    "index_prices",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Drop migrated collections from the old `finance` DB after verifying "
            "they exist and are non-empty in `quant_data`."
        )
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually drop the collections. Without this flag the script is a dry-run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.confirm

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    source_db_name = os.getenv("MONGO_DB", "finance")
    target_db_name = os.getenv("DATA_DB_NAME", "quant_data")

    # Import pymongo only after env is loaded
    try:
        from pymongo import MongoClient
    except ImportError:
        print("[ERROR] pymongo is not installed. Run: pip install pymongo")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    source_db = client[source_db_name]
    target_db = client[target_db_name]

    mode_label = "[DRY-RUN]" if dry_run else "[LIVE]"
    print(f"\n{'=' * 60}")
    print(f" Cleanup old collections  {mode_label}")
    print(f"  Source DB : {source_db_name}")
    print(f"  Target DB : {target_db_name}")
    print(f"  URI       : {mongo_uri}")
    print(f"{'=' * 60}\n")

    if dry_run:
        print("Dry-run mode — no collections will be dropped.\n")

    existing_source = set(source_db.list_collection_names())
    existing_target = set(target_db.list_collection_names())

    dropped = 0
    skipped = 0
    not_found = 0

    for col in MIGRATED_COLLECTIONS:
        # ---- source side ----
        if col not in existing_source:
            print(f"  [NOT FOUND]  {col!r:45s} not in source DB '{source_db_name}' — skipping")
            not_found += 1
            continue

        source_count = source_db[col].count_documents({})

        # ---- target side safety check ----
        if col not in existing_target:
            print(
                f"  [SKIP]  {col!r:45s} "
                f"source={source_count:>10,}  target=MISSING in '{target_db_name}'"
            )
            skipped += 1
            continue

        target_count = target_db[col].count_documents({})

        if target_count == 0:
            print(
                f"  [SKIP]  {col!r:45s} "
                f"source={source_count:>10,}  target={target_count:>10,}  "
                f"(target empty — refusing to drop)"
            )
            skipped += 1
            continue

        # ---- safe to drop ----
        status = "WOULD DROP" if dry_run else "DROPPED"
        print(
            f"  [{status}]  {col!r:40s} "
            f"source={source_count:>10,}  target={target_count:>10,}"
        )

        if not dry_run:
            source_db.drop_collection(col)

        dropped += 1

    # ---- summary ----
    print(f"\n{'=' * 60}")
    if dry_run:
        print(f" Summary (dry-run — nothing was dropped)")
    else:
        print(f" Summary")
    print(f"  Would drop / Dropped : {dropped}")
    print(f"  Skipped (unsafe)     : {skipped}")
    print(f"  Not found in source  : {not_found}")
    print(f"{'=' * 60}\n")

    if dry_run and dropped > 0:
        print("Re-run with --confirm to actually drop the collections.\n")

    client.close()


if __name__ == "__main__":
    main()
