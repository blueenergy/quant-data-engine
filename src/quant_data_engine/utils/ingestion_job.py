"""Helpers to record ingestion job_status rows from CLI tasks."""
from __future__ import annotations

from typing import Any, Dict, Optional

from quant_data_engine.config.settings import Settings
from quant_data_engine.repositories.job_status_repository import JobStatusRepository


def record_ingestion_run(
    settings: Settings,
    job_type: str,
    date_str: str,
    *,
    status: str = "complete",
    error: Optional[str] = None,
    records_changed: Optional[int] = None,
    **extra: Any,
) -> bool:
    """Write one job_status document in finance DB (primary), optionally mirror to sync URI."""
    repo = JobStatusRepository(settings.mongo_uri, settings.mongo_db)
    payload: Dict[str, Any] = dict(extra)
    if records_changed is not None:
        payload["records_changed"] = records_changed
    sync_uri = getattr(settings, "job_status_sync_uri", None)
    return repo.record(
        job_type,
        date_str,
        payload,
        status=status,
        error=error,
        sync_uri=sync_uri,
    )


__all__ = ["record_ingestion_run"]
