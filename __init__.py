"""quant_data_engine package entry after Plan B restructure.

Expose high-level factories for adapter, repository and pipeline.
Minimal surface to keep responsibilities explicit.
"""
from config.settings import get_settings
from adapters.tushare_adapter import TushareAdapter
from repositories.index_repository import IndexRepository
from pipelines.index_daily_pipeline import IndexDailyPipeline
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

__all__ = [
    "get_settings",
    "TushareAdapter",
    "IndexRepository",
    "IndexDailyPipeline",
    "create_index_pipeline",
]