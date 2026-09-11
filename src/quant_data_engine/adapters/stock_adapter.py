"""Stock data adapter for Tushare API with rate limiting & retry logic.

Provides methods for:
  - daily: Stock daily OHLCV data
  - daily_basic: Stock daily basic indicators (PE, PB, etc.)
  - moneyflow: Money flow data
  - Financial statements via Tushare ``*_vip`` (income_vip, balancesheet_vip, cashflow_vip, fina_indicator_vip)
"""
from __future__ import annotations
import time
import threading
from typing import Any, Callable, Dict, Optional, List
import pandas as pd

try:
    import tushare as ts  # type: ignore
except Exception:  # pragma: no cover
    ts = None


# fina_indicator_vip field groups (comma-joined in fetch_fina_indicator).
# Probe: tools/probe_fina_indicator_fields.py
FINA_INDICATOR_BASE_FIELDS = (
    "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,"
    "surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,"
    "quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,"
    "assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,"
    "current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,"
    "networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,"
    "cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,"
    "expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,"
    "impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,"
    "roe_yearly,roa2_yearly,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,"
    "ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,"
    "currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,"
    "eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,"
    "ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,"
    "ebitda_to_debt,turn_days,roa_yearly,interest_coverage,netprofit_yoy,"
    "dt_netprofit_yoy,grossprofit_yoy,op_yoy,ebt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,"
    "q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,"
    "q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
)
FINA_INDICATOR_PROFIT_QUALITY_FIELDS = (
    "roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,"
    "dtprofit_to_profit,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,profit_to_op"
)
FINA_INDICATOR_CASH_FLOW_QUALITY_FIELDS = (
    "salescash_to_or,ocf_to_or,ocf_to_opincome,ocf_to_profit,ocf_yoy,cfps_yoy"
)
FINA_INDICATOR_ASSET_EFFICIENCY_FIELDS = (
    "capitalized_to_da,roa_dp,fixed_assets,op_to_debt,cash_to_liqdebt,"
    "cash_to_liqdebt_withinterest,op_to_liqdebt,roic_yearly,total_fa_trun"
)
FINA_INDICATOR_QUARTERLY_FIELDS = (
    "q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,"
    "q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,"
    "q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or"
)


def _fina_indicator_core_fields() -> str:
    return ",".join(
        (
            FINA_INDICATOR_BASE_FIELDS,
            FINA_INDICATOR_PROFIT_QUALITY_FIELDS,
            FINA_INDICATOR_CASH_FLOW_QUALITY_FIELDS,
            FINA_INDICATOR_ASSET_EFFICIENCY_FIELDS,
            FINA_INDICATOR_QUARTERLY_FIELDS,
        )
    )


class RateLimiter:
    """Simple thread-safe rate limiter."""
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


class StockAdapter:
    """Adapter for stock-related Tushare API calls."""
    
    def __init__(self, token: Optional[str], rate_limit_per_sec: float = 4.0, max_retries: int = 2):
        self.enabled = bool(token and ts)
        self._max_retries = max_retries
        self._rl = RateLimiter(rate_limit_per_sec)
        if self.enabled:
            ts.set_token(token)
            self._pro = ts.pro_api()
        else:
            self._pro = None

    @staticmethod
    def _drop_empty_str_params(**kwargs: Any) -> dict[str, Any]:
        """Omit None and blank strings so Tushare does not receive empty filters."""
        out: dict[str, Any] = {}
        for k, v in kwargs.items():
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            out[k] = v
        return out

    def _retry_call(self, func, *args, **kwargs):
        """Execute API call with retry logic."""
        if not self.enabled or self._pro is None:
            return None
        last_err = None
        for attempt in range(self._max_retries + 1):
            try:
                self._rl.wait()
                result = func(*args, **kwargs)
                return result
            except Exception as e:  # pragma: no cover
                last_err = e
                if attempt < self._max_retries:
                    time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"Tushare API call failed after {self._max_retries + 1} attempts: {last_err}")

    # VIP 财报接口单次行数上限（分页）
    _FINANCIAL_VIP_PAGE_SIZE = 3000

    def _financial_vip_params(self, **kwargs: Any) -> dict[str, Any]:
        """Drop None / empty string so Tushare does not receive empty filters."""
        out: dict[str, Any] = {}
        for k, v in kwargs.items():
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            out[k] = v
        return out

    def _fetch_financial_vip_paged(self, api_name: str, **params: Any) -> pd.DataFrame:
        """Call ``income_vip`` / ``balancesheet_vip`` / … with offset/limit until drained."""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        func: Callable[..., pd.DataFrame] = getattr(self._pro, api_name)  # type: ignore[attr-defined]
        base = self._financial_vip_params(**params)
        chunks: List[pd.DataFrame] = []
        offset = 0
        page = self._FINANCIAL_VIP_PAGE_SIZE
        while True:
            kw = dict(base)
            kw["offset"] = str(offset)
            kw["limit"] = str(page)
            df = self._retry_call(func, **kw)
            if df is None or df.empty:
                break
            chunks.append(df)
            if len(df) < page:
                break
            offset += page
        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    # ==================== Daily OHLCV ====================
    
    def fetch_daily(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None,
                   start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch stock daily OHLCV data.
        
        Args:
            ts_code: Stock code (e.g., '000001.SZ'). If None, fetches all stocks.
            trade_date: Specific trade date (YYYYMMDD). If provided, start/end are ignored.
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with columns: ts_code, trade_date, open, high, low, close, pre_close, 
                                   change, pct_chg, vol, amount
        """
        return self._retry_call(
            self._pro.daily,  # type: ignore[attr-defined]
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date
        )

    def fetch_adj_factor(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch Tushare stock ``adj_factor`` records.

        The endpoint supports both all-market snapshots by ``trade_date`` and
        per-symbol ranges. ``offset``/``limit`` are passed through for large
        all-market snapshots and long ranges.
        """
        kwargs = self._drop_empty_str_params(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,adj_factor",
        )
        if offset is not None:
            kwargs["offset"] = str(offset)
        if limit is not None:
            kwargs["limit"] = str(limit)
        return self._retry_call(self._pro.adj_factor, **kwargs)  # type: ignore[attr-defined]

    def fetch_rt_k(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch Tushare realtime daily K snapshots via ``rt_k``.

        The rt_k endpoint is optional across Tushare deployments, so this method
        resolves it dynamically and returns an empty DataFrame when unavailable.
        """
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        func = getattr(self._pro, "rt_k", None) or getattr(ts, "rt_k", None)
        if func is None:
            return pd.DataFrame()
        params = self._drop_empty_str_params(
            ts_code=ts_code,
            trade_date=trade_date,
            fields=fields,
        )
        try:
            result = self._retry_call(func, **params)
        except RuntimeError as exc:
            if not ts_code or set(params.keys()) <= {"ts_code"}:
                raise
            # Some Tushare rt_k variants require ts_code but reject extra filters.
            try:
                result = self._retry_call(func, ts_code=ts_code)
            except RuntimeError:
                raise exc
        if result is None:
            return pd.DataFrame()
        return result

    def fetch_weekly(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Tushare ``weekly``：A 股周线。可按 ``trade_date`` 一次拉全市场（单次最多 6000 行，需 offset 分页）。"""
        kw: dict = {}
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
        return self._retry_call(self._pro.weekly, **kw)  # type: ignore[attr-defined]

    def fetch_monthly(
        self,
        ts_code: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Tushare ``monthly``：A 股月线。可按 ``trade_date`` 一次拉全市场（单次最多 6000 行，需 offset 分页）。"""
        kw: dict = {}
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
        return self._retry_call(self._pro.monthly, **kw)  # type: ignore[attr-defined]

    # ==================== Daily Basic Indicators ====================
    
    def fetch_daily_basic(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None,
                         start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch stock daily basic indicators (PE, PB, turnover rate, etc.).
        
        Args:
            ts_code: Stock code. If None, fetches all stocks for the given date.
            trade_date: Specific trade date (YYYYMMDD)
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with columns: ts_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm,
                                   total_mv, circ_mv, turnover_rate, turnover_rate_f,
                                   volume_ratio, dv_ratio, dv_ttm
        """
        return self._retry_call(
            self._pro.daily_basic,  # type: ignore[attr-defined]
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,pe,pe_ttm,pb,ps,ps_ttm,total_mv,circ_mv,turnover_rate,turnover_rate_f,volume_ratio,dv_ratio,dv_ttm'
        )

    # ==================== Money Flow ====================
    
    def fetch_moneyflow(self, ts_code: Optional[str] = None, trade_date: Optional[str] = None,
                       start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch money flow data.
        
        Args:
            ts_code: Stock code. If None, fetches all stocks for the given date.
            trade_date: Specific trade date (YYYYMMDD)
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with money flow indicators
        """
        return self._retry_call(
            self._pro.moneyflow,  # type: ignore[attr-defined]
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date
        )

    # ==================== Nine Turn (TD Sequential) ====================

    def fetch_nineturn(
        self,
        ts_code: Optional[str] = None,
        freq: str = "daily",
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch Tushare ``stk_nineturn`` records.

        The current verified production path is ``freq="daily"``. Tushare
        accepts minute frequencies such as ``60min`` but returned no rows in
        the probe on 2026-06-13, so callers should treat non-daily frequencies
        as best-effort until re-verified.
        """
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = self._drop_empty_str_params(
            ts_code=ts_code,
            freq=freq,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        return self._retry_call(self._pro.stk_nineturn, **kw)  # type: ignore[attr-defined]

    # ==================== Financial Statements ====================
    
    def fetch_income(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
        report_type: int = 1,
    ) -> pd.DataFrame:
        """Fetch income statement (Tushare ``income_vip``; 参数与原先 ``income`` 一致)."""
        return self._fetch_financial_vip_paged(
            "income_vip",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            report_type=report_type,
        )

    def fetch_balancesheet(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
        report_type: int = 1,
    ) -> pd.DataFrame:
        """Fetch balance sheet (Tushare ``balancesheet_vip``; 参数与原先 ``balancesheet`` 一致)."""
        return self._fetch_financial_vip_paged(
            "balancesheet_vip",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            report_type=report_type,
        )

    def fetch_cashflow(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
        report_type: int = 1,
    ) -> pd.DataFrame:
        """Fetch cash flow (Tushare ``cashflow_vip``; 参数与原先 ``cashflow`` 一致)."""
        return self._fetch_financial_vip_paged(
            "cashflow_vip",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            report_type=report_type,
        )

    def fetch_fina_indicator(
        self,
        ts_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch financial indicators (Tushare ``fina_indicator_vip``).

        Explicitly requests core extended fields beyond the Tushare default set,
        including growth-relevant metrics (rd_exp, ebit, ebitda, fcff, fcfe)
        and liquidity/solvency ratios (current_ratio, quick_ratio, debt_to_assets).
        The repository stores all returned columns as-is (no field whitelist).
        """
        return self._fetch_financial_vip_paged(
            "fina_indicator_vip",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            period=period,
            fields=_fina_indicator_core_fields(),
        )

    def fetch_dividend(
        self,
        ts_code: str = "",
        ann_date: Optional[str] = None,
        record_date: Optional[str] = None,
        ex_date: Optional[str] = None,
        imp_ann_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch dividend and bonus share records from Tushare ``dividend``."""
        params = self._drop_empty_str_params(
            ts_code=ts_code,
            ann_date=ann_date,
            record_date=record_date,
            ex_date=ex_date,
            imp_ann_date=imp_ann_date,
            fields=(
                "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
                "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
                "imp_ann_date,base_date,base_share"
            ),
        )
        return self._retry_call(self._pro.dividend, **params)  # type: ignore[attr-defined]

    def fetch_fina_mainbz(
        self,
        ts_code: str = "",
        period: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        bz_type: str = "P",
    ) -> pd.DataFrame:
        """Fetch main business composition from Tushare ``fina_mainbz``.

        ``bz_type`` maps to Tushare's ``type`` param: P=按产品, D=按地区, I=按行业.
        Tushare returns ``type`` as a query filter only (not a result column), so we
        attach ``bz_type`` to each row to keep the breakdown dimension queryable.
        """
        params = self._drop_empty_str_params(
            ts_code=ts_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            type=bz_type,
            fields="ts_code,end_date,bz_item,bz_sales,bz_profit,bz_cost,curr_type,update_flag",
        )
        df = self._retry_call(self._pro.fina_mainbz, **params)  # type: ignore[attr-defined]
        if df is not None and not df.empty:
            df = df.copy()
            df["bz_type"] = bz_type
        return df

    # ==================== Stock List ====================
    
    def fetch_stock_basic(
        self,
        list_status: str = 'L',
        exchange: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch stock basic info.
        
        Args:
            list_status: Listing status (L:上市, D:退市, P:暂停上市)
            exchange: Exchange (SSE:上交所, SZSE:深交所)
            fields: Optional comma-separated field list. The Tushare default
                omits list_status and delist_date, which callers tracking
                delisted names need.
            
        Returns:
            DataFrame with stock basic info
        """
        kwargs: Dict[str, Any] = {"list_status": list_status, "exchange": exchange}
        if fields:
            kwargs["fields"] = fields
        return self._retry_call(
            self._pro.stock_basic,  # type: ignore[attr-defined]
            **kwargs,
        )

    def fetch_bak_basic(self, trade_date: str, fields: Optional[str] = None) -> pd.DataFrame:
        """Fetch bak_basic snapshot for a specific trade date.

        Args:
            trade_date: Trade date in YYYYMMDD.
            fields: Optional comma-separated field list.

        Returns:
            DataFrame with bak_basic fields.
        """
        kwargs = {"trade_date": trade_date}
        if fields:
            kwargs["fields"] = fields
        return self._retry_call(
            self._pro.bak_basic,  # type: ignore[attr-defined]
            **kwargs,
        )

    def fetch_trade_cal(self, exchange: str = 'SSE', start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetch trading calendar.
        
        Args:
            exchange: Exchange code (SSE, SZSE, etc.)
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with trading calendar (cal_date, is_open)
        """
        return self._retry_call(
            self._pro.trade_cal,  # type: ignore[attr-defined]
            exchange=exchange,
            start_date=start_date,
            end_date=end_date
        )

    # ==================== Top10 holders & float / share_float / block_trade (深度分析) ====================

    def fetch_top10_holders(
        self,
        ts_code: str,
        period: str = "",
        start_date: str = "",
        end_date: str = "",
        ann_date: str = "",
    ) -> pd.DataFrame:
        """Tushare ``top10_holders``：十大股东。``period`` 为报告期 YYYYMMDD（如年报 20241231）。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = self._drop_empty_str_params(
            ts_code=ts_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            ann_date=ann_date,
        )
        return self._retry_call(self._pro.top10_holders, **kw)  # type: ignore[attr-defined]

    def fetch_top10_floatholders(
        self,
        ts_code: str,
        period: str = "",
        start_date: str = "",
        end_date: str = "",
        ann_date: str = "",
    ) -> pd.DataFrame:
        """Tushare ``top10_floatholders``：十大流通股东。参数同 ``top10_holders``。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = self._drop_empty_str_params(
            ts_code=ts_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            ann_date=ann_date,
        )
        return self._retry_call(self._pro.top10_floatholders, **kw)  # type: ignore[attr-defined]

    def fetch_share_float(
        self,
        ts_code: str = "",
        ann_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """Tushare ``share_float``：限售股解禁。可按 ``ts_code`` 与 ``ann_date`` / 公告区间拉取。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = self._drop_empty_str_params(
            ts_code=ts_code,
            ann_date=ann_date,
            start_date=start_date,
            end_date=end_date,
        )
        return self._retry_call(self._pro.share_float, **kw)  # type: ignore[attr-defined]

    def fetch_block_trade(
        self,
        ts_code: str = "",
        trade_date: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """Tushare ``block_trade``：大宗交易。宜传 ``ts_code`` + ``start_date``/``end_date`` 区间。"""
        if not self.enabled or self._pro is None:
            return pd.DataFrame()
        kw = self._drop_empty_str_params(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        return self._retry_call(self._pro.block_trade, **kw)  # type: ignore[attr-defined]


__all__ = ["StockAdapter"]
