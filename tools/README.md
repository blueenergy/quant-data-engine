# quant_data_engine Tools

This directory contains utility tools for data management and maintenance.

## Available Tools

### update_symbol_with_suffix_generic.py

A generic script to standardize symbol fields in MongoDB collections by adding exchange suffixes.

**Purpose:**
- Standardize all symbol fields to include exchange suffixes (e.g., '000001' -> '000001.SZ', '600519' -> '600519.SH')
- Support both single symbol fields and array fields containing multiple symbols
- Works with any collection in the database

**Usage:**
```bash
# Run interactively
python update_symbol_with_suffix_generic.py

# The script will:
# 1. List all available collections
# 2. Prompt for collection name
# 3. Prompt for symbol field name (default: 'symbol')
# 4. Ask if the field is an array
# 5. Show preview and confirm before updating
```

**Features:**
- Interactive mode for safe operation
- Dry-run preview before actual updates
- Progress tracking for large collections
- Verification after updates
- Support for both single symbol fields and array fields

**Example:**
```
Collection: stock_info
Field: symbol
Type: single field
Result: Updates symbols like '000001' to '000001.SZ'

Collection: watchlist_strategies
Field: symbols
Type: array
Result: Updates symbol arrays to include exchange suffixes
```

### restore_collection_to_remote.sh

Dump one local MongoDB collection and restore it to the same database and collection name on a remote server through `ssh`/`scp`.

**End-to-end usage:**

The default action is `all`, so omitting the action is equivalent to running `./restore_collection_to_remote.sh all`.

```bash
./restore_collection_to_remote.sh \
  --db quant_data \
  --collection stock_info \
  --ssh ubuntu@1.2.3.4 \
  --local-uri "mongodb://localhost:27017" \
  --remote-uri "mongodb://127.0.0.1:27017" \
  --drop
```

Split a large migration into upload and restore:
```bash
# Step 1: dump locally and upload the .archive.gz file to the remote server.
./restore_collection_to_remote.sh upload \
  --db quant_data \
  --collection stock_info \
  --ssh ubuntu@1.2.3.4 \
  --local-uri "mongodb://localhost:27017"

# Step 2: restore the newest matching archive from the default remote directory.
./restore_collection_to_remote.sh restore \
  --db quant_data \
  --collection stock_info \
  --ssh ubuntu@1.2.3.4 \
  --remote-uri "mongodb://127.0.0.1:27017" \
  --drop
```

Validate the end-to-end path without writing remote data:
```bash
./restore_collection_to_remote.sh \
  --db quant_data \
  --collection stock_info \
  --ssh ubuntu@1.2.3.4 \
  --local-uri "mongodb://localhost:27017" \
  --remote-uri "mongodb://127.0.0.1:27017" \
  --dry-run
```

**Features:**
- Supports `.env` defaults via `LOCAL_MONGO_URI`, `REMOTE_MONGO_URI`, and `REMOTE_SSH`
- Uses `mongodump --archive --gzip` and remote `mongorestore`
- Restores to the same namespace, for example `quant_data.stock_info`
- Supports `upload` and `restore` actions for large collection migrations
- Uses `/tmp/quant-data-engine-mongo-restore` as the default remote archive directory
- Lets `restore` find the newest matching remote archive when `--remote-archive` is omitted
- Keeps remote archives by default; add `--delete-remote-archive` to remove one after restore
- Requires explicit `--drop` before replacing the remote collection
- Supports `--dry-run` to validate local dump, SSH, SCP, and remote `mongorestore --dryRun` without writing data

## Future Tools

Additional data management tools may be added here, such as:
- Data quality validators
- Duplicate cleaners
- Migration helpers
- Backup utilities
