#!/usr/bin/env bash
# Backfill report_type=2 (单季合并) income + cashflow for multiple report periods.
# Prereq: load MONGO_URI, DATA_DB_NAME, TUSHARE_TOKEN (e.g. from quantFinance/.env).
# Run migration once before first dual sync if you have legacy rows without report_type:
#   python -m quant_data_engine.tools.migrate_financial_report_type_key
#
# Usage:
#   cd /path/to/quant-data-engine
#   source ../quantFinance/.env 2>/dev/null || true
#   export MONGO_URI DATA_DB_NAME TUSHARE_TOKEN
#   ./scripts/backfill_report_type2.sh
#
# Or pass periods as args:
#   ./scripts/backfill_report_type2.sh 20240331 20240630 20240930 20241231
#
# Or a single sync for all quarter-ends in [START,END] (see run_financial_sync):
#   python -m quant_data_engine.tasks.run_financial_sync --all --period-range 20200630:20251231 --data-types income,cashflow
#
# If you have int vs string "1" duplicates, run first:
#   python -m quant_data_engine.tools.migrate_financial_report_type_key --full

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="${ROOT}/src:${PYTHONPATH:-}"

PERIODS=("$@")
if [[ ${#PERIODS[@]} -eq 0 ]]; then
  PERIODS=(20240331 20240630 20240930 20241231 20250331)
fi

echo "==> One-time migration (safe to re-run; only touches docs missing report_type)"
python -m quant_data_engine.tools.migrate_financial_report_type_key

for p in "${PERIODS[@]}"; do
  echo ""
  echo "==> Full-market sync period $p (income + cashflow, report_type 1+2)"
  python -m quant_data_engine.tasks.run_financial_sync --all --period "$p" --data-types income,cashflow
done

echo "Done. Verify: db.financial_income.find({report_type:2}).limit(1)"
