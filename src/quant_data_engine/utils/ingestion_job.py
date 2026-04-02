"""Helpers to record ingestion job_status rows from CLI tasks."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from quant_data_engine.config.settings import Settings

_log = logging.getLogger(__name__)
from quant_data_engine.repositories.job_status_repository import JobStatusRepository
from quant_data_engine.repositories.pipeline_tasks_repository import PipelineTasksRepository


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
    """Write job_status and pipeline_tasks (data_collection) in finance DB; optional mirror URI."""
    payload: Dict[str, Any] = dict(extra)
    if records_changed is not None:
        payload["records_changed"] = records_changed
    sync_uri = getattr(settings, "job_status_sync_uri", None)
    base = dict(payload)
    js = JobStatusRepository(settings.mongo_uri, settings.mongo_db)
    ok_js = js.record(
        job_type,
        date_str,
        base,
        status=status,
        error=error,
        sync_uri=sync_uri,
    )
    try:
        pt = PipelineTasksRepository(settings.mongo_uri, settings.mongo_db)
        ok_pt = pt.record_data_collection(
            task_type=job_type,
            date=date_str,
            payload=payload,
            status=status,
            error=error,
            sync_uri=sync_uri,
        )
        if not ok_pt:
            _log.warning(
                "pipeline_tasks upsert returned false for %s date=%s",
                job_type,
                date_str,
            )
    except Exception as e:
        _log.warning(
            "pipeline_tasks write failed (job_status ok): %s",
            e,
            exc_info=_log.isEnabledFor(logging.DEBUG),
        )
    return bool(ok_js)


__all__ = ["record_ingestion_run"]
