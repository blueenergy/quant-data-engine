"""quant_data_engine package entry after Plan B restructure.

Expose high-level factories for adapter, repository and pipeline.
Minimal surface to keep responsibilities explicit.
"""
from .config.settings import get_settings
from .adapters.tushare_adapter import TushareAdapter
from .adapters.tdx_minute_adapter import TdxMinuteAdapter
from .adapters.baostock_minute_adapter import BaostockMinuteAdapter
from .adapters.mini_qmt_adapter import MiniQMTAdapter
from .adapters.mini_qmt_realtime_adapter import MiniQMTRealtimeAdapter
from .adapters.realtime_adapter_base import RealtimeAdapter

from .repositories.index_repository import IndexRepository
from .repositories.etf_repository import EtfRepository
from .repositories.minute_repository import MinuteRepository



from .pipelines.etf_daily_pipeline import EtfDailyPipeline
from .pipelines.index_daily_pipeline import IndexDailyPipeline
from .pipelines.minute_pipeline import MinutePipeline
from .pipelines.mini_qmt_pipeline import MiniQMTPipeline
from .pipelines.realtime_pipeline import RealtimePipeline
from .models.bar import Bar
from .utils.trading_scheduler import TradingScheduler
from .quality.validators import IndexPriceValidator

def create_index_pipeline() -> IndexDailyPipeline:
    """Factory returning a ready-to-run index daily pipeline.
    Adapter and repository are wired using env-driven settings.
    """
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = IndexRepository(settings.mongo_uri, settings.mongo_db)
    validator = IndexPriceValidator()
    return IndexDailyPipeline(adapter=adapter, repository=repo, validator=validator)


def create_etf_pipeline() -> EtfDailyPipeline:
    """Factory for ETF daily ingestion pipeline."""
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = EtfRepository(settings.mongo_uri, settings.mongo_db)
    validator = IndexPriceValidator()  # Reuse same validation rules (OHLC)
    return EtfDailyPipeline(adapter=adapter, repository=repo, validator=validator)
def create_minute_pipeline() -> MinutePipeline:
    """Factory returning a ready-to-run minute data pipeline.
    Adapter and repository are wired using env-driven settings.
    """
    settings = get_settings()
    adapter = TdxMinuteAdapter()
    repo = MinuteRepository(settings.mongo_uri, settings.mongo_db)
    return MinutePipeline(adapter=adapter, repository=repo)

def create_baostock_minute_pipeline() -> MinutePipeline:
    """Factory returning a ready-to-run minute data pipeline using Baostock."""
    settings = get_settings()
    adapter = BaostockMinuteAdapter()
    repo = MinuteRepository(settings.mongo_uri, settings.mongo_db)
    return MinutePipeline(adapter=adapter, repository=repo)

def create_mini_qmt_pipeline() -> MiniQMTPipeline:
    """Factory returning a ready-to-run minute data pipeline using miniQMT.
    
    Stores data to minute_bars_external collection to separate external data sources.
    """
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
    
    Args:
        symbols: List of stock symbols with exchange suffix (e.g., ['000001.SZ'])
        interval_sec: Tick polling interval in seconds (default: 1.0)
        enable_schedule: Enable trading time scheduling (default: True)
        debug: Enable debug logging (default: False)
        
    Returns:
        RealtimePipeline: Configured realtime pipeline
    """
    settings = get_settings()
    adapter = MiniQMTRealtimeAdapter(
        symbols=symbols,
        interval_sec=interval_sec,
        enable_schedule=enable_schedule,
        debug=debug
    )
    repo = MinuteRepository(settings.mongo_uri, settings.mongo_db)
    return RealtimePipeline(adapter=adapter, repository=repo)

__all__ = [
    "get_settings",
    "TushareAdapter",
    "TdxMinuteAdapter",
    "BaostockMinuteAdapter",
    "MiniQMTAdapter",
    "MiniQMTRealtimeAdapter",
    "RealtimeAdapter",
    "IndexRepository",
    "MinuteRepository",
    "IndexDailyPipeline",
    "MinutePipeline",
    "MiniQMTPipeline",
    "RealtimePipeline",
    "Bar",
    "TradingScheduler",
    "create_index_pipeline",
    "EtfRepository",
    "EtfDailyPipeline",
    "create_etf_pipeline",
    "create_minute_pipeline",
    "create_baostock_minute_pipeline",
    "create_mini_qmt_pipeline",
    "create_realtime_pipeline",
]