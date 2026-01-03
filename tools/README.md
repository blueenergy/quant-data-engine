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

## Future Tools

Additional data management tools may be added here, such as:
- Data quality validators
- Duplicate cleaners
- Migration helpers
- Backup utilities
