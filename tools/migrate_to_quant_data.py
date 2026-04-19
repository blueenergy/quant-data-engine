"""One-time data migration script: copies documents from finance.{collection}
to quant_data.{collection} for all specified collections.

Uses a read-then-bulk-write pattern (find + bulk_write with UpdateOne upsert)
to avoid cross-database $merge authorization requirements.

Usage:
    python tools/migrate_to_quant_data.py
    python tools/migrate_to_quant_data.py --dry-run
    python tools/migrate_to_quant_data.py --collections volume_price,money_flow
    python -m quant_data_engine.tools.migrate_to_quant_data --dry-run
"""

import argparse
import os
import sys
import pathlib

# ---------------------------------------------------------------------------
# Load .env from quantFinance/.env (same pattern as settings.py)
# ---------------------------------------------------------------------------
from dotenv import load_dotenv

_env_path = pathlib.Path(__file__).parent.parent.parent / "quantFinance" / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=True)
else:
    load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Collections to migrate
# ---------------------------------------------------------------------------
COLLECTIONS_TO_MIGRATE = [
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
    "fund_holdings",
    "etf_prices",
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
    "index_constituents",
]


BATCH_SIZE = 5000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate collections from finance DB to quant_data DB using bulk_write."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print source collection names and document counts; no copying.",
    )
    parser.add_argument(
        "--collections",
        type=str,
        default=None,
        help="Comma-separated list of collections to migrate. Default: all.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Determine which collections to process
    if args.collections:
        target_collections = [c.strip() for c in args.collections.split(",") if c.strip()]
        invalid = [c for c in target_collections if c not in COLLECTIONS_TO_MIGRATE]
        if invalid:
            print(f"[WARNING] Unknown collections (not in COLLECTIONS_TO_MIGRATE): {invalid}")
    else:
        target_collections = list(COLLECTIONS_TO_MIGRATE)

    # Read config from env
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    source_db_name = os.getenv("MONGO_DB", "finance")
    target_db_name = os.getenv("DATA_DB_NAME", "quant_data")

    print("=" * 60)
    print("MongoDB Data Migration")
    print("=" * 60)
    print(f"  Source DB : {source_db_name}")
    print(f"  Target DB : {target_db_name}")
    print(f"  Mode      : {'DRY RUN (no data will be copied)' if args.dry_run else 'LIVE'}")
    print(f"  Collections to process: {len(target_collections)}")
    print("=" * 60)

    try:
        from pymongo import MongoClient
        from pymongo import UpdateOne
    except ImportError:
        print("[ERROR] pymongo is not installed. Run: pip install pymongo")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    finance_db = client[source_db_name]
    quant_data_db = client[target_db_name]

    # Get existing collections in source DB
    existing_source = set(finance_db.list_collection_names())

    total_migrated = 0
    total_docs = 0

    for name in target_collections:
        if name not in existing_source:
            print(f"  [SKIP] {name!r} — not found in source DB '{source_db_name}'")
            continue

        source_col = finance_db[name]
        source_count = source_col.count_documents({})

        if args.dry_run:
            print(f"  [DRY RUN] {name!r}: {source_count:,} documents in source")
            total_migrated += 1
            total_docs += source_count
            continue

        # Perform migration via read-then-bulk-write (avoids $merge cross-DB auth)
        print(f"  [MIGRATING] {name!r}: {source_count:,} source documents ...")
        try:
            target_col = quant_data_db[name]
            batch = []
            processed = 0

            for doc in source_col.find({}):
                batch.append(
                    UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
                )
                if len(batch) >= BATCH_SIZE:
                    target_col.bulk_write(batch, ordered=False)
                    processed += len(batch)
                    print(f"    Migrated {processed:,}/{source_count:,} documents...")
                    batch = []

            # flush remaining
            if batch:
                target_col.bulk_write(batch, ordered=False)
                processed += len(batch)
                batch = []

            target_count = target_col.count_documents({})
            print(f"  [OK] {name!r} — {target_count:,} documents in target")
            total_migrated += 1
            total_docs += source_count
        except Exception as exc:
            print(f"  [FAILED] {name!r} — {exc}")

    client.close()

    print("=" * 60)
    print("Migration Summary")
    print("=" * 60)
    if args.dry_run:
        print(f"  [DRY RUN] Would migrate {total_migrated} collection(s), {total_docs:,} total documents")
    else:
        print(f"  Migrated {total_migrated} collection(s), {total_docs:,} total source documents")
    print("=" * 60)


if __name__ == "__main__":
    main()
