"""ShareholderPipeline — orchestrate shareholder / chips ingestion from Tushare.

Several of these interfaces cap a single call (e.g. hk_hold ~4000 rows sorted by
ts_code, stk_holdernumber ~5000), so every full-market fetch is **paginated**
via offset/limit until the page is short. Three cadences share one pipeline:

  - run_holdernumber(start, end) : 股东户数, by disclosure-date range (paged)
  - run_events(dates)           : 增减持 / 解禁 / 回购, by announcement ann_date (paged)
  - run_hk_hold(dates)          : 沪深港股通持股, by trade_date snapshot (paged)

Each method returns a stats dict so the CLI tasks can record ingestion runs.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 5000
_MAX_PAGES = 400  # safety ceiling: 400 * 5000 = 2M rows before bailing out


class ShareholderPipeline:
    def __init__(self, adapter, repository, page_limit: int = _PAGE_LIMIT, max_pages: int = _MAX_PAGES):
        self._adapter = adapter
        self._repo = repository
        self._page_limit = page_limit
        self._max_pages = max_pages

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._adapter, "enabled", False))

    def _fetch_all(self, fetch_fn: Callable, **params) -> Optional[pd.DataFrame]:
        """Page through a Tushare interface until an empty page is returned.

        We cannot rely on ``len(page) < page_limit`` to detect the end: several
        interfaces hard-cap a single call below the requested limit (e.g. hk_hold
        caps at ~4200), so a "short" first page is not the last page. We therefore
        advance by the rows actually returned and stop only on an empty page, with
        a safety cap to avoid an infinite loop if an endpoint ignores ``offset``.
        """
        frames: List[pd.DataFrame] = []
        offset = 0
        for _ in range(self._max_pages):
            df = fetch_fn(offset=offset, limit=self._page_limit, **params)
            if df is None or getattr(df, "empty", True):
                break
            frames.append(df)
            offset += len(df)
        else:
            logger.warning("%s hit max_pages=%d; result may be truncated", getattr(fetch_fn, "__name__", "fetch"), self._max_pages)
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    # ── 股东户数（disclosure-date range, paged）──────────────────────────────
    def run_holdernumber(self, start_date: str, end_date: str) -> Dict[str, Any]:
        stats = {"records": 0, "status": "success", "start": start_date, "end": end_date}
        if not self.enabled:
            stats["status"] = "skipped"
            return stats
        try:
            df = self._fetch_all(self._adapter.fetch_stk_holdernumber, start_date=start_date, end_date=end_date)
            stats["records"] = self._repo.upsert_holdernumber(df)
            logger.info("[holdernumber %s..%s] upserted %s rows", start_date, end_date, stats["records"])
        except Exception as e:
            logger.error("holdernumber %s..%s failed: %s", start_date, end_date, e)
            stats["status"] = "partial"
        return stats

    # ── 股东事件（announcement, paged per day）──────────────────────────────
    def run_events(self, dates: Sequence[str]) -> Dict[str, Any]:
        stats = {
            "dates": 0, "holdertrade_records": 0, "share_float_records": 0,
            "repurchase_records": 0, "status": "success",
        }
        if not self.enabled:
            stats["status"] = "skipped"
            return stats
        sources = (
            ("holdertrade_records", self._adapter.fetch_stk_holdertrade, self._repo.upsert_holdertrade),
            ("share_float_records", self._adapter.fetch_share_float, self._repo.upsert_share_float),
            ("repurchase_records", self._adapter.fetch_repurchase, self._repo.upsert_repurchase),
        )
        for day in dates:
            for stat_key, fetch_fn, upsert_fn in sources:
                try:
                    df = self._fetch_all(fetch_fn, ann_date=day)
                    stats[stat_key] += upsert_fn(df)
                except Exception as e:
                    logger.error("%s %s failed: %s", stat_key, day, e)
                    stats["status"] = "partial"
            stats["dates"] += 1
        stats["records"] = (
            stats["holdertrade_records"] + stats["share_float_records"] + stats["repurchase_records"]
        )
        return stats

    # ── 港股通持股（trade_date snapshot, paged per date）─────────────────────
    def run_hk_hold(self, dates: Sequence[str]) -> Dict[str, Any]:
        stats = {"dates": 0, "records": 0, "status": "success"}
        if not self.enabled:
            stats["status"] = "skipped"
            return stats
        for day in dates:
            try:
                df = self._fetch_all(self._adapter.fetch_hk_hold, trade_date=day)
                n = self._repo.upsert_hk_hold(df)
                stats["records"] += n
                stats["dates"] += 1
                logger.info("[hk_hold %s] upserted %s rows", day, n)
            except Exception as e:
                logger.error("hk_hold %s failed: %s", day, e)
                stats["status"] = "partial"
        return stats


__all__ = ["ShareholderPipeline"]
