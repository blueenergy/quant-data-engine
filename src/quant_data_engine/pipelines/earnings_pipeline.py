import logging
from typing import Optional, Dict, Any
from quant_data_engine.adapters.tushare_adapter import TushareAdapter
from quant_data_engine.repositories.earnings_repository import EarningsRepository

logger = logging.getLogger(__name__)

class EarningsPipeline:
    """Pipeline to sync earnings forecast and express reports from Tushare."""

    def __init__(self, tushare_adapter: TushareAdapter, repository: EarningsRepository):
        self._adapter = tushare_adapter
        self._repo = repository

    def run(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Synchronize earnings forecast and express reports for a given date range.
        Uses ann_date (announcement date).
        """
        if not self._adapter.enabled:
            logger.warning("Tushare adapter not enabled. Skipping Earnings Pipeline.")
            return {"forecast_records": 0, "express_records": 0, "disclosure_records": 0, "report_rc_records": 0, "status": "skipped"}

        logger.info(f"Running Earnings Pipeline for period: {start_date} to {end_date}")
        stats = {"forecast_records": 0, "express_records": 0, "disclosure_records": 0, "report_rc_records": 0, "status": "failed"}

        try:
            # 1. Fetch & Upsert Earnings Forecast (业绩预告)
            logger.info("Fetching earnings forecast...")
            # forecast API doesn't support ann_date range larger than a few months directly perfectly sometimes,
            # but usually start_date and end_date (period) are used. 
            # However we want recently *announced* ones. So we use ann_date range. Tushare's forecast API has ann_date.
            # But wait, Tushare's `forecast` API might only accept exact `ann_date`, or a `start_date/end_date` which refers to the *report period end date*.
            # Actually, `ann_date` usually takes a single date.
            # Let's check Tushare docs. Usually we fetch by a specific date. 
            # We will iterate over trading days.
            
            # Use date_utils to get all dates in the range, and iterate.
            # (Note for express/forecast, they can be announced on non-trading days too! 
            # But let's just generate all dates via simple iteration or rely on Tushare if it accepts ranges).
            
            # Tushare forecast: ann_date can be used. Let's do daily loop to be safe and avoid limits.
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            
            current_dt = start_dt
            
            while current_dt <= end_dt:
                curr_date_str = current_dt.strftime("%Y%m%d")
                
                # ------ Forecast ------
                try:
                    df_forecast = self._adapter.fetch_forecast(ann_date=curr_date_str)
                    if df_forecast is not None and not df_forecast.empty:
                        records = df_forecast.to_dict("records")
                        upserted = self._repo.upsert_forecasts(records)
                        stats["forecast_records"] += upserted
                        logger.info(f"[{curr_date_str}] Upserted {upserted} forecast records.")
                except Exception as e:
                    logger.error(f"Error fetching forecast for {curr_date_str}: {e}")
                    
                # ------ Express ------
                try:
                    df_express = self._adapter.fetch_express(ann_date=curr_date_str)
                    if df_express is not None and not df_express.empty:
                        records = df_express.to_dict("records")
                        upserted = self._repo.upsert_express(records)
                        stats["express_records"] += upserted
                        logger.info(f"[{curr_date_str}] Upserted {upserted} express records.")
                except Exception as e:
                    logger.error(f"Error fetching express for {curr_date_str}: {e}")
                    
                # ------ Disclosure Schedule ------
                # Note: disclosure_date API is usually queried by `pre_date` or `actual_date`. 
                # Since we iterate over a date window, let's use `pre_date` (预计披露日期) to catch 
                # upcoming schedules that were announced.
                try:
                    df_disc = self._adapter.fetch_disclosure_date(pre_date=curr_date_str)
                    if df_disc is not None and not df_disc.empty:
                        # Sometimes Tushare returns NaN for certain dates, fill with empty strings or None
                        df_disc = df_disc.fillna("")
                        records = df_disc.to_dict("records")
                        upserted = self._repo.upsert_disclosure(records)
                        stats["disclosure_records"] += upserted
                        logger.info(f"[{curr_date_str}] Upserted {upserted} disclosure schedule records.")
                except Exception as e:
                    logger.error(f"Error fetching disclosure schedule for {curr_date_str}: {e}")
                    
                # ------ Broker Reports (report_rc) ------
                try:
                    df_report_rc = self._adapter.fetch_report_rc(report_date=curr_date_str)
                    if df_report_rc is not None and not df_report_rc.empty:
                        df_report_rc = df_report_rc.fillna("")
                        records = df_report_rc.to_dict("records")
                        upserted = self._repo.upsert_report_rc(records)
                        stats["report_rc_records"] += upserted
                        logger.info(f"[{curr_date_str}] Upserted {upserted} broker report records (report_rc).")
                except Exception as e:
                    logger.error(f"Error fetching broker reports for {curr_date_str}: {e}")
                
                current_dt += timedelta(days=1)
                
            stats["status"] = "success"
            logger.info(f"Earnings Pipeline completed: {stats}")
            
        except Exception as e:
            logger.error(f"Earnings Pipeline failed: {e}")
            stats["error"] = str(e)
            
        return stats
