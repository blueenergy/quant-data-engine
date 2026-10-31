"""按报告期分页拉取 fund_portfolio，避免单次行数上限截断。"""
from __future__ import annotations

import logging
from typing import Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 5000
_MAX_PAGES = 400


def iter_fund_portfolio_pages(
    adapter,
    *,
    period: str = "",
    ann_date: str = "",
    ts_code: str = "",
    page_limit: int = _PAGE_LIMIT,
    max_pages: int = _MAX_PAGES,
) -> Iterator[pd.DataFrame]:
    offset = 0
    last_fp = None
    for _ in range(max_pages):
        df = adapter.fetch_fund_portfolio(
            ts_code=ts_code,
            ann_date=ann_date,
            period=period,
            offset=offset,
            limit=page_limit,
        )
        if df is None or getattr(df, "empty", True):
            return
        fp = tuple(str(x) for x in df.iloc[0].tolist()[:8])
        if last_fp is not None and fp == last_fp:
            raise RuntimeError(
                "fund_portfolio offset appears ignored; refusing partial result "
                f"for {period or ann_date or ts_code}"
            )
            break
        last_fp = fp
        yield df
        n = len(df)
        offset += n
    else:
        raise RuntimeError(
            f"fund_portfolio page cap hit period={period} ann_date={ann_date}"
        )


def fetch_fund_portfolio_paged(
    adapter,
    *,
    period: str = "",
    ann_date: str = "",
    ts_code: str = "",
    page_limit: int = _PAGE_LIMIT,
    max_pages: int = _MAX_PAGES,
) -> Optional[pd.DataFrame]:
    frames = list(
        iter_fund_portfolio_pages(
            adapter,
            period=period,
            ann_date=ann_date,
            ts_code=ts_code,
            page_limit=page_limit,
            max_pages=max_pages,
        )
    )
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


_FUND_BASIC_PAGE_LIMIT = 5000
_FUND_BASIC_MAX_PAGES = 20


def iter_fund_basic_pages(
    adapter,
    *,
    market: str,
    status: str,
    page_limit: int = _FUND_BASIC_PAGE_LIMIT,
    max_pages: int = _FUND_BASIC_MAX_PAGES,
) -> Iterator[pd.DataFrame]:
    """按 offset 分页拉取 fund_basic，避免单次 15,000 行上限截断。"""
    offset = 0
    last_fp = None
    for _ in range(max_pages):
        df = adapter.fetch_fund_basic(
            market=market,
            status=status,
            offset=offset,
            limit=page_limit,
        )
        if df is None or getattr(df, "empty", True):
            return
        fp = str(df.iloc[0].get("ts_code", ""))
        if last_fp is not None and fp == last_fp:
            raise RuntimeError(
                "fund_basic offset appears ignored; refusing partial result "
                f"for {market}/{status}"
            )
        last_fp = fp
        yield df
        n = len(df)
        offset += n
    else:
        raise RuntimeError(
            f"fund_basic page cap hit market={market} status={status}"
        )
