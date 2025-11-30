"""Pipeline for minute-level data ETL."""
from adapters.tdx_minute_adapter import TdxMinuteAdapter
from repositories.minute_repository import MinuteRepository

class MinutePipeline:
    def __init__(self, adapter: TdxMinuteAdapter, repository: MinuteRepository):
        self.adapter = adapter
        self.repository = repository

    def run(self, code: str, market: int, start: int = 0, count: int = 240):
        df = self.adapter.fetch_minute_bars(code, market, start, count)
        self.repository.upsert_minute_bars(code, df)
