"""Declarative data contracts + reconciler for quant-data-engine.

A *contract* describes the desired state of a dataset (source, storage,
window, SLA). The *reconciler* compares desired vs actual coverage, optionally
backfills missing data via the dataset's repair task, persists a health
snapshot to ``finance.data_contract_state`` (consumed by the data-pulse UI),
and raises alerts on violation.

Phase 1 scope: a single contract (``stock_daily``) end-to-end.
"""
from quant_data_engine.contracts.models import (
    AlertSpec,
    Completeness,
    Contract,
    OnViolation,
    SLA,
    StorageSpec,
    UniverseSpec,
    Window,
    parse_contract,
)
from quant_data_engine.contracts.reconciler import (
    ContractReconciler,
    CoverageEval,
    ReconcileResult,
    chunk_missing_days,
    compress_missing_ranges,
    evaluate_coverage,
    judge_violations,
    mature_quarter_ends,
    period_disclosure_deadline,
    quarter_ends_in_range,
)

__all__ = [
    "AlertSpec",
    "Completeness",
    "Contract",
    "OnViolation",
    "SLA",
    "StorageSpec",
    "UniverseSpec",
    "Window",
    "parse_contract",
    "ContractReconciler",
    "CoverageEval",
    "ReconcileResult",
    "chunk_missing_days",
    "compress_missing_ranges",
    "evaluate_coverage",
    "judge_violations",
    "mature_quarter_ends",
    "period_disclosure_deadline",
    "quarter_ends_in_range",
]
