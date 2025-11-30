"""quant_data_engine package entry after Plan B restructure.

Expose high-level factories for adapter, repository and pipeline.
Minimal surface to keep responsibilities explicit.
"""
from config.settings import get_settings
from adapters.tushare_adapter import TushareAdapter
from adapters.tdx_minute_adapter import TdxMinuteAdapter
from adapters.baostock_minute_adapter import BaostockMinuteAdapter
from repositories.index_repository import IndexRepository
from repositories.minute_repository import MinuteRepository
from pipelines.index_daily_pipeline import IndexDailyPipeline
from pipelines.minute_pipeline import MinutePipeline
from quality.validators import IndexPriceValidator

def create_index_pipeline() -> IndexDailyPipeline:
    """Factory returning a ready-to-run index daily pipeline.
    Adapter and repository are wired using env-driven settings.
    """
    settings = get_settings()
    adapter = TushareAdapter(settings.tushare_token, rate_limit_per_sec=settings.rate_limit_per_sec)
    repo = IndexRepository(settings.mongo_uri, settings.mongo_db)
    validator = IndexPriceValidator()
    return IndexDailyPipeline(adapter=adapter, repository=repo, validator=validator)

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

__all__ = [
    "get_settings",
    "TushareAdapter",
    "TdxMinuteAdapter",
    "BaostockMinuteAdapter",
    "IndexRepository",
    "MinuteRepository",
    "IndexDailyPipeline",
    "MinutePipeline",
    "create_index_pipeline",
    "create_minute_pipeline",
    "create_baostock_minute_pipeline",
]