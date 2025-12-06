"""Pipeline for miniQMT data ETL."""

from ..adapters.mini_qmt_adapter import MiniQMTAdapter
from ..repositories.minute_repository import MinuteRepository


class MiniQMTPipeline:
    def __init__(self, adapter: MiniQMTAdapter, repository: MinuteRepository):
        self.adapter = adapter
        self.repository = repository

    def run(self, code: str, start_date: str, end_date: str, period: str = '1m', max_wait: int = 60):
        """Run the pipeline to fetch and store miniQMT data.
        
        Args:
            code: Stock symbol (e.g., '300722.SZ')
            start_date: Start date in format 'YYYYMMDD'
            end_date: End date in format 'YYYYMMDD'
            period: Data frequency ('1m', '5m', '15m', '30m', '1h', '1d')
            max_wait: Max seconds to wait for data (default 60)
        """
        df = self.adapter.fetch_minute_bars(code, start_date, end_date, period, max_wait=max_wait)
        if not df.empty:
            # Store symbol without exchange suffix for compatibility with existing code
            normalized_code = code.split('.')[0] if '.' in code else code
            self.repository.upsert_minute_bars(normalized_code, df)
            print(f"✅ Processed {len(df)} records for {normalized_code}")
            
            # Check total count in database
            total_count = self.repository.collection.count_documents({'symbol': normalized_code})
            print(f"   Total in database: {total_count} records")
        else:
            print(f"⚠️ No data fetched for {code}")

    def run_batch(self, codes: list, start_date: str, end_date: str, period: str = '1m', max_wait: int = 60):
        """Run the pipeline for multiple symbols.
        
        Args:
            codes: List of stock symbols
            start_date: Start date in format 'YYYYMMDD'
            end_date: End date in format 'YYYYMMDD'
            period: Data frequency
            max_wait: Max seconds to wait for data (default 60)
        """
        results = self.adapter.download_batch_data(codes, start_date, end_date, period)
        for code, success in results.items():
            if success:
                print(f"✅ Download initiated for {code}")
            else:
                print(f"❌ Failed to initiate download for {code}")