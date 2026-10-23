"""Tushare adapter with simple rate limiting & retry logic (synchronous minimal version)."""
from __future__ import annotations
import time
import threading
import logging
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover - optional dependency for tests
    ts = None
    logger.warning("Tushare not installed or import failed")

# Inline wait for 频率超限(1次/分钟). Hourly limits must not block a cron job.
TUSHARE_PER_MINUTE_WAIT_SEC = 65.0
TUSHARE_PER_HOUR_WAIT_SEC = 3605.0
TUSHARE_MAX_INLINE_RATE_WAIT_SEC = 90.0

# ``report_rc`` caps a single call at 5,000 rows and drops the tail silently
# (2023-10-31 held 5,589). Advance by the rows actually returned and stop only on
# an empty page — a short page proves nothing when the cap is the server's, not
# ours. The page bound is a guard against an endpoint that ignores ``offset``.
REPORT_RC_PAGE_SIZE = 5000
REPORT_RC_MAX_PAGES = 20


def tushare_rate_limit_sleep_sec(exc: BaseException) -> Optional[float]:
    """Seconds to wait after a Tushare 频率超限 error, or None if it is not one."""
    text = str(exc)
    if "频率超限" not in text:
        return None
    if "次/小时" in text:
        return TUSHARE_PER_HOUR_WAIT_SEC
    if "次/分钟" in text:
        return TUSHARE_PER_MINUTE_WAIT_SEC
    return TUSHARE_PER_MINUTE_WAIT_SEC


class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.interval = 1.0 / max(rate_per_sec, 0.1)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            delta = now - self._last
            if delta < self.interval:
                time.sleep(self.interval - delta)
            self._last = time.time()

class TushareAdapter:
    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)
            self._pro = ts.pro_api()
        else:
            self._pro = None

    def fetch_index_daily(self, ts_code: str, start_date: str, end_date: str):
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover - network dependent
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_index_daily failed after retries: {last_err}")

    def fetch_index_dailybasic(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        fields: Optional[str] = None,
    ):
        """Fetch Tushare index_dailybasic rows for index valuation metrics."""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if fields:
            kw["fields"] = fields
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.index_dailybasic(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover - network dependent
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_index_dailybasic failed after retries: {last_err}")

    def fetch_etf_daily(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ):
        """fund_daily：全市场须 ``trade_date`` 单日（服务端要求 ts_code 与 trade_date 至少其一）。

        不可仅用 ``start_date``/``end_date`` 不传 ``ts_code`` 拉全市场日线。单次最多 5000 行（无官方 offset 字段）。
        """
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_daily(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_etf_daily failed after retries: {last_err}")

    def fetch_fund_basic(self, market: str = 'E', status: str = 'L'):
        """Fetch ETF basic info (market='E' for exchange traded funds, status='L' for listed)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_basic(market=market, status=status)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_basic failed: {last_err}")

    def fetch_fund_adj(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        """Fetch ETF adjustment factors.不传 ts_code 时可按 trade_date 拉全市场（注意 2000 行/次，需分页）。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if offset is not None:
            kw["offset"] = str(offset)
        if limit is not None:
            kw["limit"] = str(limit)
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_adj(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_adj failed: {last_err}")

    def fetch_fund_share(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        """Fetch ETF share scale.不传 ts_code 时可按 trade_date 拉全市场（注意 2000 行/次，需分页）。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if offset is not None:
            kw["offset"] = str(offset)
        if limit is not None:
            kw["limit"] = str(limit)
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_share(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_share failed: {last_err}")

    def fetch_etf_share_size(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange: str = "",
    ):
        """Tushare etf_share_size：ETF 日份额、规模、净值、收盘等（需较高积分，见官方文档）。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if exchange:
            kw["exchange"] = exchange
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.etf_share_size(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_etf_share_size failed: {last_err}")

    def fetch_fund_portfolio(
        self,
        ts_code: str = "",
        symbol: str = "",
        ann_date: str = "",
        period: str = "",
        start_date: str = "",
        end_date: str = "",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        """公募基金持仓（fund_portfolio）。ts_code / ann_date / period 至少填其一。

        按 ``period`` 拉全市场时单次有行数上限，须传 ``offset``/``limit`` 分页。
        """
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if symbol:
            kw["symbol"] = symbol
        if ann_date:
            kw["ann_date"] = ann_date
        if period:
            kw["period"] = period
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if offset is not None:
            kw["offset"] = int(offset)
        if limit is not None:
            kw["limit"] = int(limit)
        if not kw:
            raise ValueError("fetch_fund_portfolio: 至少传入 ts_code、ann_date、period 之一")

        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fund_portfolio(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fund_portfolio failed: {last_err}")

    def fetch_forecast(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '', period: str = ''):
        """Fetch earnings forecast (业绩预告)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.forecast(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date, period=period)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_forecast failed: {last_err}")

    def fetch_express(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '', period: str = ''):
        """Fetch earnings express (业绩快报)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.express(ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date, period=period)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_express failed: {last_err}")

    def fetch_disclosure_date(self, ts_code: str = '', end_date: str = '', pre_date: str = '', actual_date: str = ''):
        """Fetch earnings disclosure date schedule (财报披露计划)."""
        if not self.enabled: return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.disclosure_date(ts_code=ts_code, end_date=end_date, pre_date=pre_date, actual_date=actual_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_disclosure_date failed: {last_err}")

    def fetch_report_rc(self, ts_code: str = '', report_date: str = '', start_date: str = '', end_date: str = ''):
        """Fetch broker earnings forecasts/ratings (券商盈利预测与评级).

        Pages internally: the interface caps one call at ``REPORT_RC_PAGE_SIZE``
        rows and truncates the remainder without raising, so on disclosure-peak
        days (2023-10-31 ran to 5,589) a one-shot fetch silently drops every
        report past the cap. Callers get the whole result.
        """
        frames: List[pd.DataFrame] = []
        offset = 0
        for _ in range(REPORT_RC_MAX_PAGES):
            df = self._query(
                "report_rc", ts_code=ts_code, report_date=report_date,
                start_date=start_date, end_date=end_date,
                offset=offset, limit=REPORT_RC_PAGE_SIZE,
            )
            if df is None or df.empty:
                break
            frames.append(df)
            offset += len(df)
        else:
            logger.warning(
                "report_rc stopped at max_pages=%d (offset=%d); result may be truncated",
                REPORT_RC_MAX_PAGES, offset,
            )
        if not frames:
            return None
        return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------ #
    #  Shareholder / chips data                                            #
    # ------------------------------------------------------------------ #

    def _query(self, name: str, *, offset: Optional[int] = None, limit: Optional[int] = None, **params: Any) -> Optional[pd.DataFrame]:
        """Generic rate-limited Tushare pro.query with retry (drops empty params).

        ``offset``/``limit`` enable pagination — several of these interfaces cap a
        single call (e.g. hk_hold ~4000 rows sorted by ts_code), so the caller must
        page to retrieve a complete full-market snapshot.

        ``频率超限(1次/分钟)`` waits ~65s and retries once. Hourly limits raise
        immediately — short retries burn the next hour's slot.
        """
        if not self.enabled:
            return None
        kw = {k: v for k, v in params.items() if not (v is None or (isinstance(v, str) and not v.strip()))}
        if offset is not None:
            kw["offset"] = int(offset)
        if limit is not None:
            kw["limit"] = int(limit)
        last_err = None
        extra_rate_retry = True
        attempt = 0
        while True:
            attempt += 1
            try:
                self._rl.wait()
                return self._pro.query(name, **kw)  # type: ignore[attr-defined]
            except Exception as e:  # pragma: no cover - network dependent
                last_err = e
                wait = tushare_rate_limit_sleep_sec(e)
                if wait is not None:
                    if extra_rate_retry and wait <= TUSHARE_MAX_INLINE_RATE_WAIT_SEC:
                        extra_rate_retry = False
                        logger.warning(
                            "Tushare %s rate-limited; sleeping %.0fs then retrying once",
                            name, wait,
                        )
                        time.sleep(wait)
                        continue
                    logger.error(
                        "Tushare %s rate-limited (%s); not retrying",
                        name, e,
                    )
                    break
                if attempt > self._max_retries:
                    break
                time.sleep(0.5)
        raise RuntimeError(f"Tushare query {name} failed after retries: {last_err}")

    def fetch_stk_holdernumber(self, ts_code: str = '', end_date: str = '', start_date: str = '',
                               offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """股东户数（stk_holdernumber）。start_date/end_date 为披露日范围；全市场须分页。

        字段：ts_code/ann_date/end_date/holder_num。
        """
        return self._query("stk_holdernumber", ts_code=ts_code, end_date=end_date, start_date=start_date,
                           offset=offset, limit=limit)

    def fetch_stk_holdertrade(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '',
                              offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """股东增减持（stk_holdertrade）。按 ann_date 可拉全市场（公告驱动）。"""
        return self._query("stk_holdertrade", ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_share_float(self, ts_code: str = '', ann_date: str = '', float_date: str = '', start_date: str = '', end_date: str = '',
                          offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """限售股解禁（share_float）。按 ann_date 可拉全市场；float_date 为未来解禁日。"""
        return self._query("share_float", ts_code=ts_code, ann_date=ann_date, float_date=float_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_repurchase(self, ts_code: str = '', ann_date: str = '', start_date: str = '', end_date: str = '',
                         offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """股票回购（repurchase）。按 ann_date 可拉全市场；proc 为回购阶段（预案/实施/完成等）。"""
        return self._query("repurchase", ts_code=ts_code, ann_date=ann_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_hk_hold(self, trade_date: str = '', ts_code: str = '', start_date: str = '', end_date: str = '', exchange: str = '',
                      offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """沪深港股通持股明细（hk_hold）。按 trade_date 可拉全市场（含北向 A 股与南向港股）；须分页。

        注意：北向持股明细在 2024-08 之后由日度披露改为季度披露（采样点为季末）。
        """
        return self._query("hk_hold", trade_date=trade_date, ts_code=ts_code, start_date=start_date, end_date=end_date, exchange=exchange,
                           offset=offset, limit=limit)

    # ------------------------------------------------------------------ #
    #  HK / US equity (own universes — do not mix into A-share tables)     #
    # ------------------------------------------------------------------ #

    def fetch_hk_basic(self, ts_code: str = "", list_status: str = "L",
                       offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """港股列表（hk_basic）。单次可拉全部在交易标的；list_status=L/D/P。"""
        return self._query("hk_basic", ts_code=ts_code, list_status=list_status, offset=offset, limit=limit)

    def fetch_hk_tradecal(self, start_date: str = "", end_date: str = "", is_open: str = "",
                          offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """港股交易日历（hk_tradecal）。单次最大 2000 行，按日期分段。"""
        return self._query("hk_tradecal", start_date=start_date, end_date=end_date, is_open=is_open,
                           offset=offset, limit=limit)

    def fetch_hk_daily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "",
                       offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """港股未复权日线（hk_daily）。单次最大 5000 行；本 token 实测频次极低（约 1 次/小时）。"""
        return self._query("hk_daily", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_hk_daily_adj(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "",
                           offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """港股复权行情（hk_daily_adj）。单次最大 6000 行；需单独权限。"""
        return self._query("hk_daily_adj", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_us_basic(self, ts_code: str = "", classify: str = "", list_status: str = "",
                       offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """美股列表（us_basic）。单次最大 6000，必须分页。classify=EQ/ADR/GDR/PF。"""
        kw: Dict[str, Any] = {"ts_code": ts_code, "classify": classify, "offset": offset, "limit": limit}
        # Tushare docs typo list_stauts; send both so either server spelling works.
        if list_status:
            kw["list_status"] = list_status
            kw["list_stauts"] = list_status
        return self._query("us_basic", **kw)

    def fetch_us_tradecal(self, start_date: str = "", end_date: str = "", is_open: str = "",
                          offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """美股交易日历（us_tradecal）。单次最大 6000 行。"""
        return self._query("us_tradecal", start_date=start_date, end_date=end_date, is_open=is_open,
                           offset=offset, limit=limit)

    def fetch_us_daily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "",
                       offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """美股未复权日线（us_daily）。单次最大 6000 行；需单独开通美股日线权限。"""
        return self._query("us_daily", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                           offset=offset, limit=limit)

    def fetch_us_daily_adj(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "",
                           exchange: str = "", offset: Optional[int] = None, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """美股复权行情（us_daily_adj）。单次最大 8000 行；需单独权限。"""
        return self._query("us_daily_adj", ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date,
                           exchange=exchange, offset=offset, limit=limit)

    def fetch_ths_hot(self, trade_date: str = '') -> Optional[pd.DataFrame]:
        """Fetch THS (同花顺) hot stock ranking.
        
        Args:
            trade_date: Trade date in YYYYMMDD format. Empty string for latest.
        """
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.ths_hot(trade_date=trade_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_ths_hot failed after retries: {last_err}")

    def fetch_dc_hot(self, trade_date: str = '') -> Optional[pd.DataFrame]:
        """Fetch DC (东方财富) hot stock ranking.
        
        Args:
            trade_date: Trade date in YYYYMMDD format. Empty string for latest.
        """
        if not self.enabled:
            return None
        last_err = None
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.dc_hot(trade_date=trade_date)
                return df
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_dc_hot failed after retries: {last_err}")

    # ------------------------------------------------------------------ #
    #  Macro economic data — China                                         #
    # ------------------------------------------------------------------ #

    def fetch_shibor(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """上海银行间同业拆放利率（Shibor），日频。字段：date/on/1w/2w/1m/3m/6m/9m/1y。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.shibor(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_shibor failed: {last_err}")

    def fetch_shibor_lpr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """贷款市场报价利率（LPR），月频报价日。字段：date/1y/5y（旧格式含 m6 等）。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.shibor_lpr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_shibor_lpr failed: {last_err}")

    def fetch_cn_pmi(self, start_m: str = '', end_m: str = '') -> Optional[pd.DataFrame]:
        """中国采购经理人指数（PMI），月频。字段：month/pmi010000(制造业)/pmi020100(非制造业商务活动)等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_m:
            kw['start_m'] = start_m
        if end_m:
            kw['end_m'] = end_m
        # Tushare cn_pmi 现在要求显式传小写 fields；不传会报
        # "指定了返回列，但是列名不匹配，[]"。pmi020000 当前不返回，
        # 但保留在请求里以便接口恢复后自动带上。
        kw['fields'] = 'month,pmi010000,pmi020000,pmi020100'
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.cn_pmi(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_cn_pmi failed: {last_err}")

    def fetch_cn_cpi(self, start_m: str = '', end_m: str = '') -> Optional[pd.DataFrame]:
        """居民消费价格指数（CPI），月频。字段：month/nt_yoy/nt_mom/town_yoy 等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_m:
            kw['start_m'] = start_m
        if end_m:
            kw['end_m'] = end_m
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.cn_cpi(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_cn_cpi failed: {last_err}")

    def fetch_cn_ppi(self, start_m: str = '', end_m: str = '') -> Optional[pd.DataFrame]:
        """工业生产者出厂价格指数（PPI），月频。字段：month/ppi_yoy/ppi_mp 等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_m:
            kw['start_m'] = start_m
        if end_m:
            kw['end_m'] = end_m
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.cn_ppi(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_cn_ppi failed: {last_err}")

    def fetch_cn_m(self, start_m: str = '', end_m: str = '') -> Optional[pd.DataFrame]:
        """货币供应量（M0/M1/M2），月频。字段：month/m0/m1/m2/m0_yoy/m1_yoy/m2_yoy 等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_m:
            kw['start_m'] = start_m
        if end_m:
            kw['end_m'] = end_m
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.cn_m(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_cn_m failed: {last_err}")

    def fetch_sf_month(self, start_m: str = '', end_m: str = '') -> Optional[pd.DataFrame]:
        """社会融资规模月度数据，月频。字段：month/rzrz(社融规模)/rzrz_yoy 等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_m:
            kw['start_m'] = start_m
        if end_m:
            kw['end_m'] = end_m
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.sf_month(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_sf_month failed: {last_err}")

    def fetch_cn_gdp(self, start_q: str = '', end_q: str = '') -> Optional[pd.DataFrame]:
        """国内生产总值（GDP），季频。字段：quarter/gdp_yoy/pi_yoy/si_yoy/ti_yoy 等。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_q:
            kw['start_q'] = start_q
        if end_q:
            kw['end_q'] = end_q
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.cn_gdp(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_cn_gdp failed: {last_err}")

    # ------------------------------------------------------------------ #
    #  Macro economic data — US rates                                      #
    # ------------------------------------------------------------------ #

    def fetch_us_tycr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """美国国债收益率曲线（日频）。字段：date/y1/y2/y3/y5/y7/y10/y20/y30。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.us_tycr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_us_tycr failed: {last_err}")

    def fetch_us_trycr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """美国国债实际收益率曲线（TIPS，日频）。字段：date/y5/y7/y10/y20/y30。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.us_trycr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_us_trycr failed: {last_err}")

    def fetch_us_tbr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """美国短期国债利率（T-Bill，日频）。字段：date/w4/w13/w26/w52。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.us_tbr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_us_tbr failed: {last_err}")

    def fetch_us_tltr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """美国国债长期利率平均值（日频）。字段：date/ltc。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.us_tltr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_us_tltr failed: {last_err}")

    def fetch_us_trltr(self, start_date: str = '', end_date: str = '') -> Optional[pd.DataFrame]:
        """美国国债实际长期利率平均值（日频）。字段：date/ltc。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw['start_date'] = start_date
        if end_date:
            kw['end_date'] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.us_trltr(**kw)
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_us_trltr failed: {last_err}")

    def fetch_margin(
        self,
        start_date: str = "",
        end_date: str = "",
        trade_date: str = "",
        exchange_id: str = "",
    ) -> Optional[pd.DataFrame]:
        """沪深融资融券交易汇总。字段含 rzye/rqye/rzrqye 等，金额单位元。"""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if trade_date:
            kw["trade_date"] = trade_date
        if exchange_id:
            kw["exchange_id"] = exchange_id
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                return self._pro.margin(**kw)  # type: ignore[attr-defined]
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_margin failed: {last_err}")

    def fetch_fut_basic(
        self,
        exchange: str = "",
        fut_type: str = "",
        fields: Optional[str] = None,
    ):
        """Fetch futures contract list (``fut_basic``)."""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if exchange:
            kw["exchange"] = exchange
        if fut_type:
            kw["fut_type"] = fut_type
        if fields:
            kw["fields"] = fields
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fut_basic(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fut_basic failed after retries: {last_err}")

    def fetch_fut_mapping(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ):
        """Fetch main/continuous contract mapping (``fut_mapping``)."""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fut_mapping(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fut_mapping failed after retries: {last_err}")

    def fetch_fut_daily(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
        exchange: str = "",
        fields: Optional[str] = None,
    ):
        """Fetch futures daily OHLCV (``fut_daily``). Main codes like IF.CFX return continuous bars."""
        if not self.enabled:
            return None
        last_err = None
        kw: Dict[str, Any] = {}
        if ts_code:
            kw["ts_code"] = ts_code
        if trade_date:
            kw["trade_date"] = trade_date
        if start_date:
            kw["start_date"] = start_date
        if end_date:
            kw["end_date"] = end_date
        if exchange:
            kw["exchange"] = exchange
        if fields:
            kw["fields"] = fields
        for _ in range(self._max_retries + 1):
            try:
                self._rl.wait()
                df = self._pro.fut_daily(**kw)  # type: ignore[attr-defined]
                return df
            except Exception as e:  # pragma: no cover
                last_err = e
                time.sleep(0.5)
        raise RuntimeError(f"Tushare fetch_fut_daily failed after retries: {last_err}")

class TushareLadderAdapter:
    """
    Adapter for fetching limit-up ladder data from Tushare.
    Handles rate limiting and data formatting.
    """
    
    def __init__(self, token: str):
        if not token:
            raise ValueError("Tushare token is required")
        
        if ts is None:
            raise ImportError("Tushare package is not installed")
            
        try:
            ts.set_token(token)
            self.pro = ts.pro_api()
            self._last_request_time = 0
            logger.info("Tushare adapter initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Tushare adapter: {e}")
            raise

    def _rate_limit(self):
        """
        Enforce rate limiting (approx 1 request per 1.1 seconds).
        Tushare standard limit is often around 100-200 requests/minute depending on score.
        1.1s is a safe conservative default.
        """
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < 1.1:
            sleep_time = 1.1 - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def get_limit_up_stocks(self, date: str) -> List[Dict]:
        """
        Fetch daily limit-up stocks list using `limit_list_d`.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with stock data
        """
        self._rate_limit()
        try:
            # limit_type='U' for limit up (D for limit down)
            df = self.pro.limit_list_d(trade_date=date, limit_type='U')
            if df.empty:
                logger.warning(f"No limit-up data found for date {date} from Tushare")
                return []
            
            # Convert to list of dicts
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d for {date}: {e}")
            return []
    def get_limit_down_stocks(self, date: str) -> List[Dict]:
        """
        Fetch daily limit-down stocks list using `limit_list_d`.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with stock data
        """
        self._rate_limit()
        try:
            # limit_type='D' for limit down
            df = self.pro.limit_list_d(trade_date=date, limit_type='D')
            if df.empty:
                logger.warning(f"No limit-down data found for date {date} from Tushare")
                return []
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d (down) for {date}: {e}")
            return []

    def get_limit_up_stocks_range(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch limit-up stocks for a date range.
        
        Args:
            start_date: YYYYMMDD
            end_date: YYYYMMDD
            
        Returns:
            List of dicts (flat list of all stocks across all dates)
        """
        self._rate_limit()
        try:
            df = self.pro.limit_list_d(start_date=start_date, end_date=end_date, limit_type='U')
            if df.empty:
                logger.warning(f"No limit-up data found for period {start_date}-{end_date}")
                return []
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_list_d range {start_date}-{end_date}: {e}")
            return []

    def get_limit_step(self, date: str) -> List[Dict]:
        """
        Fetch limit-up ladder streak data using `limit_step`.
        Contains consecutive limit-up counts.
        
        Args:
            date: YYYYMMDD format
            
        Returns:
            List of dictionaries with streak info
        """
        self._rate_limit()
        try:
            df = self.pro.limit_step(trade_date=date)
            if df.empty:
                logger.warning(f"No limit_step data found for date {date} from Tushare")
                return []
            
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_step for {date}: {e}")
            return []

    def get_sector_ranking(self, date: str) -> List[Dict]:
        """
        Fetch strongest concept sectors using `limit_cpt_list`.
        
        Args:
            date: YYYYMMDD format
        
        Returns:
            List of dictionaries with sector ranking
        """
        self._rate_limit()
        try:
            df = self.pro.limit_cpt_list(trade_date=date)
            if df.empty:
                return []
            
            # Rename columns to match our internal schema if needed, or keep as is
            # Tushare returns: ts_code, name, trade_date, days, up_stat, cons_nums, up_nums, pct_chg, rank
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching limit_cpt_list for {date}: {e}")
            return []

    # ========== East Money Concept APIs ==========
    
    _concept_cache: Dict[str, str] = {}  # BK代码 -> 概念名称
    _concept_cache_time: float = 0
    
    def get_concept_mapping(self, force_refresh: bool = False) -> Dict[str, str]:
        """
        Get mapping of concept code (BKxxxx.DC) to concept name.
        Uses in-memory cache with 24-hour TTL (concept list rarely changes).
        
        Returns:
            Dict mapping ts_code -> name (e.g., {'BK0428.DC': '电力'})
        """
        # Check cache (24 hour TTL - concept list rarely changes)
        current_time = time.time()
        if not force_refresh and self._concept_cache and (current_time - self._concept_cache_time) < 86400:
            return self._concept_cache
        
        self._rate_limit()
        try:
            df = self.pro.dc_index()
            if df.empty:
                logger.warning("No concept data from dc_index")
                return self._concept_cache or {}
            
            # Build mapping: BKxxxx.DC -> 概念名称
            self._concept_cache = dict(zip(df['ts_code'], df['name']))
            self._concept_cache_time = current_time
            logger.info(f"Cached {len(self._concept_cache)} concept mappings")
            return self._concept_cache
        except Exception as e:
            logger.error(f"Error fetching dc_index: {e}")
            return self._concept_cache or {}
    
    def get_stock_concepts(self, ts_code: str, trade_date: str) -> List[str]:
        """
        Get list of concept names for a stock using dc_member API.
        
        Args:
            ts_code: Stock code (e.g., '600900.SH')
            trade_date: Trade date in YYYYMMDD format
            
        Returns:
            List of concept names (e.g., ['电力', '新能源', '央企改革'])
        """
        self._rate_limit()
        try:
            df = self.pro.dc_member(con_code=ts_code, trade_date=trade_date)
            if df.empty:
                logger.debug(f"No concept membership found for {ts_code}")
                return []
            
            # Get concept codes from result
            concept_codes = df['ts_code'].tolist()
            
            # Get mapping and resolve names
            concept_map = self.get_concept_mapping()
            concept_names = []
            for code in concept_codes:
                name = concept_map.get(code)
                if name:
                    concept_names.append(name)
                else:
                    # Fallback: use code if name not found
                    logger.debug(f"Concept name not found for {code}")
                    concept_names.append(code)
            
            return concept_names
        except Exception as e:
            logger.error(f"Error fetching dc_member for {ts_code}: {e}")
            return []

__all__ = ["TushareAdapter", "TushareLadderAdapter"]
