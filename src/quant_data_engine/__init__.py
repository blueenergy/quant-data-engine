"""quant_data_engine package entry after Plan B restructure.

Expose high-level factories for adapter, repository and pipeline.
Minimal surface to keep responsibilities explicit.
"""
import platform
from .config.settings import get_settings
from .adapters.tushare_adapter import TushareAdapter
from .adapters.tdx_minute_adapter import TdxMinuteAdapter
from .adapters.realtime_adapter_base import RealtimeAdapter
from .adapters.stock_adapter import StockAdapter

# Conditional imports for Windows-only miniQMT adapters
IS_WINDOWS = platform.system() == 'Windows'
if IS_WINDOWS:
    from .adapters.mini_qmt_adapter import MiniQMTAdapter
    from .adapters.mini_qmt_realtime_adapter import MiniQMTRealtimeAdapter
else:
    MiniQMTAdapter = None
    MiniQMTRealtimeAdapter = None

from .repositories.index_repository import IndexRepository
from .repositories.etf_repository import EtfRepository
from .repositories.minute_repository import MinuteRepository
from .repositories.stock_repository import StockRepository
from .repositories.financial_repository import FinancialRepository
from .repositories.moneyflow_repository import MoneyFlowRepository
from .repositories.institutional_repository import InstitutionalRepository
from .repositories.sw_industry_repository import SwIndustryRepository
from .repositories.risk_report_repository import RiskReportRepository
from .adapters.global_market_adapter import GlobalMarketAdapter


from .pipelines.etf_daily_pipeline import EtfDailyPipeline
from .pipelines.etf_share_size_pipeline import EtfShareSizePipeline
from .pipelines.index_daily_pipeline import IndexDailyPipeline
from .pipelines.minute_pipeline import MinutePipeline
from .pipelines.stock_daily_pipeline import StockDailyPipeline
from .pipelines.financial_pipeline import FinancialPipeline
from .pipelines.moneyflow_pipeline import MoneyFlowPipeline
from .pipelines.institutional_pipeline import InstitutionalPipeline

# Conditional imports for Windows-only miniQMT pipelines
if IS_WINDOWS:
    from .pipelines.mini_qmt_pipeline import MiniQMTPipeline
    from .pipelines.realtime_pipeline import RealtimePipeline
else:
    MiniQMTPipeline = None
    RealtimePipeline = None

from .models.bar import Bar
from .utils.trading_scheduler import TradingScheduler
from .quality.validators import IndexPriceValidator

def create_index_pipeline() -> IndexDailyPipeline:
    """Factory returning a ready-to-run index daily pipeline.
    Adapter and repository are wired using env-driven settings.
    """
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = IndexRepository(settings.mongo_uri, settings.data_db)
    validator = IndexPriceValidator()
    return IndexDailyPipeline(adapter=adapter, repository=repo, validator=validator)


def create_etf_pipeline() -> EtfDailyPipeline:
    """Factory for ETF daily ingestion pipeline."""
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = EtfRepository(settings.mongo_uri, settings.data_db)
    validator = IndexPriceValidator()  # Reuse same validation rules (OHLC)
    return EtfDailyPipeline(adapter=adapter, repository=repo, validator=validator)


def create_etf_share_size_pipeline() -> EtfShareSizePipeline:
    """Factory for Tushare etf_share_size -> ``etf_share_size`` collection."""
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = EtfRepository(settings.mongo_uri, settings.data_db)
    return EtfShareSizePipeline(adapter=adapter, repository=repo)


def create_minute_pipeline() -> MinutePipeline:
    """Factory returning a ready-to-run minute data pipeline.
    Adapter and repository are wired using env-driven settings.
    """
    settings = get_settings()
    adapter = TdxMinuteAdapter()
    repo = MinuteRepository(settings.mongo_uri, settings.data_db)
    return MinutePipeline(adapter=adapter, repository=repo)

def create_mini_qmt_pipeline() -> MiniQMTPipeline:
    """Factory returning a ready-to-run minute data pipeline using miniQMT.
    
    Stores data to minute_bars_external collection to separate external data sources.
    
    Note: Only available on Windows systems.
    """
    if not IS_WINDOWS:
        raise RuntimeError("MiniQMT adapters are only available on Windows systems")
    
    settings = get_settings()
    adapter = MiniQMTAdapter()
    repo = MinuteRepository(settings.mongo_uri, settings.mongo_db, collection='minute_bars_external')
    return MiniQMTPipeline(adapter=adapter, repository=repo)

def create_realtime_pipeline(
    symbols: list,
    interval_sec: float = 1.0,
    enable_schedule: bool = True,
    debug: bool = False
) -> RealtimePipeline:
    """Factory returning a ready-to-run realtime data pipeline (Windows Bridge).
    
    Note: Only available on Windows systems.
    
    Args:
        symbols: List of stock symbols with exchange suffix (e.g., ['000001.SZ'])
        interval_sec: Tick polling interval in seconds (default: 1.0)
        enable_schedule: Enable trading time scheduling (default: True)
        debug: Enable debug logging (default: False)
        
    Returns:
        RealtimePipeline: Configured realtime pipeline
    """
    if not IS_WINDOWS:
        raise RuntimeError("MiniQMT realtime adapter is only available on Windows systems")
    
    settings = get_settings()
    adapter = MiniQMTRealtimeAdapter(
        symbols=symbols,
        interval_sec=interval_sec,
        enable_schedule=enable_schedule,
        debug=debug
    )
    repo = MinuteRepository(settings.mongo_uri, settings.data_db)
    return RealtimePipeline(adapter=adapter, repository=repo)

def create_stock_daily_pipeline() -> StockDailyPipeline:
    """Factory for stock daily data pipeline (OHLCV + basic indicators)."""
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = StockRepository(settings.mongo_uri, settings.data_db)
    validator = IndexPriceValidator()  # Reuse OHLC validation
    return StockDailyPipeline(adapter=adapter, repository=repo, validator=validator)

def create_financial_pipeline() -> FinancialPipeline:
    """Factory for financial data pipeline (statements + indicators)."""
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = FinancialRepository(settings.mongo_uri, settings.data_db)
    return FinancialPipeline(adapter=adapter, repository=repo)

def create_moneyflow_pipeline(ts_code_map=None) -> MoneyFlowPipeline:
    """Factory for money flow data pipeline.
    
    Args:
        ts_code_map: Optional mapping from ts_code to symbol
        
    Returns:
        MoneyFlowPipeline: Configured pipeline
    """
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = MoneyFlowRepository(settings.mongo_uri, settings.data_db)
    return MoneyFlowPipeline(adapter=adapter, repository=repo, ts_code_map=ts_code_map)


def create_institutional_pipeline() -> InstitutionalPipeline:
    """Factory：机构 top10/解禁/大宗 深度拉取，写入 ``quant_data`` 同名集合 / ``institutional_deep_snapshot``。
    若已回补申万数据，会附带 L1 60 日行业超额（``SwIndustryRepository``）。"""
    settings = get_settings()
    adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = InstitutionalRepository(settings.mongo_uri, settings.data_db)
    sw = SwIndustryRepository(settings.mongo_uri, settings.data_db)
    return InstitutionalPipeline(
        adapter=adapter, repository=repo, sw_industry_repository=sw
    )


def create_risk_scanner():
    """Factory for MarketRiskScanner wired to all adapters and RiskReportRepository.

    Returns a tuple ``(scanner, repo)`` so callers can persist results::

        scanner, repo = create_risk_scanner()
        report = scanner.scan("20260502", sector="semicon")
        repo.upsert_report(report)
    """
    from .tools.risk_scanner import MarketRiskScanner  # local import avoids circular
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    stock_adapter = StockAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    scanner = MarketRiskScanner(adapter=adapter, stock_adapter=stock_adapter)
    repo = RiskReportRepository(settings.mongo_uri, settings.data_db)
    return scanner, repo


__all__ = [
    "get_settings",
    "TushareAdapter",
    "TdxMinuteAdapter",
    "RealtimeAdapter",
    "StockAdapter",
    "IndexRepository",
    "EtfRepository",
    "MinuteRepository",
    "StockRepository",
    "FinancialRepository",
    "MoneyFlowRepository",
    "InstitutionalRepository",
    "SwIndustryRepository",
    "RiskReportRepository",
    "IndexDailyPipeline",
    "EtfDailyPipeline",
    "EtfShareSizePipeline",
    "MinutePipeline",
    "StockDailyPipeline",
    "FinancialPipeline",
    "MoneyFlowPipeline",
    "InstitutionalPipeline",
    "Bar",
    "TradingScheduler",
    "create_index_pipeline",
    "create_etf_pipeline",
    "create_etf_share_size_pipeline",
    "create_minute_pipeline",
    "create_stock_daily_pipeline",
    "create_financial_pipeline",
    "create_moneyflow_pipeline",
    "create_institutional_pipeline",
    "create_risk_scanner",
    "IS_WINDOWS",  # Export for checking platform availability
]

# Conditionally add Windows-only exports
if IS_WINDOWS:
    __all__.extend([
        "MiniQMTAdapter",
        "MiniQMTRealtimeAdapter",
        "MiniQMTPipeline",
        "RealtimePipeline",
        "create_mini_qmt_pipeline",
        "create_realtime_pipeline",
    ])