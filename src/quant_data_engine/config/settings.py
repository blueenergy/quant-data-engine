"""Global settings loader for quant-data-engine.

Configuration Pattern:
  .env (secrets layer)
    ↓ (read by)
  settings.py (abstraction layer)
    ↓ (imported by)
  All modules via: from quant_data_engine import get_settings()

This design:
  - Keeps secrets out of version control (.env is gitignored)
  - Provides type-safe configuration interface (Settings dataclass)
  - Centralizes defaults and env var reading
  - Caches settings to avoid repeated env reads
  - Allows easy testing via dependency injection

Usage:
  settings = get_settings()
  print(settings.mongo_uri)  # e.g., 'mongodb://localhost:27017'
"""

from dataclasses import dataclass
from typing import Optional
import os
from functools import lru_cache
from dotenv import load_dotenv
import pathlib

# Load this engine's own .env. Inside Docker the file is dockerignored, so
# settings fall back to the env vars injected by docker-compose (env_file +
# environment).
_engine_env = pathlib.Path(__file__).resolve().parent.parent.parent.parent / ".env"
if _engine_env.exists():
    load_dotenv(_engine_env, override=True)
else:
    load_dotenv(override=True)

@dataclass
class Settings:
    mongo_uri: str
    mongo_db: str
    trading_db: str
    data_db: str          # quant_data database for raw external data
    realtime_db: str      # quant_realtime database for hot latest snapshots
    tushare_token: Optional[str]
    rate_limit_per_sec: float
    # Optional: also upsert job_status on this URI (e.g. cloud finance while primary is local).
    # JOB_STATUS_SYNC_URI overrides; if unset, falls back to SECONDARY_MONGO_URI when set.
    job_status_sync_uri: Optional[str]
    # Controls which data pipelines are active (mirrors start.sh DEPLOY_MODE).
    # Values: "domestic" (A-share only) | "overseas" (global market only) | "full" (default)
    deploy_mode: str = "full"
    # Realtime daily K (Tushare rt_k) feature flags and quality gates.
    enable_realtime_daily_k: bool = False
    realtime_daily_k_shadow_mode: bool = True
    realtime_daily_k_skip_stock_daily: bool = False
    realtime_daily_k_min_coverage: float = 0.98
    realtime_daily_k_min_field_coverage: float = 0.98
    realtime_daily_k_equivalence_days: int = 3
    realtime_daily_k_price_tolerance_pct: float = 0.2
    realtime_daily_k_volume_tolerance_pct: float = 5.0
    realtime_daily_k_collection: str = "market_spectrum_realtime_daily"
    realtime_daily_k_source_mode: str = "tushare_rt_k_daily"
    realtime_daily_k_allow_outside_session: bool = False
    realtime_daily_k_batch_size: int = 500
    realtime_daily_k_ts_code_patterns: str = "3*.SZ,6*.SH,0*.SZ,9*.BJ"
    realtime_daily_k_fields: str = (
        "ts_code,name,pre_close,high,open,low,close,vol,amount,num,"
        "ask_price1,ask_volume1,bid_price1,bid_volume1,trade_time"
    )
    intraday_t0_trigger_url: Optional[str] = None
    intraday_t0_trigger_key: Optional[str] = None
    intraday_t0_trigger_timeout_sec: float = 5.0
    market_insight_trigger_url: Optional[str] = None
    market_insight_trigger_key: Optional[str] = None
    market_insight_trigger_timeout_sec: float = 5.0


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get settings from environment variables.
    
    Cached to avoid recreating Settings object on every call.
    """
    _sync = os.getenv("JOB_STATUS_SYNC_URI", "").strip() or os.getenv("SECONDARY_MONGO_URI", "").strip() or None
    def _bool_env(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in ("1", "true", "yes", "on")

    return Settings(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "finance"),
        trading_db=os.getenv("TRADING_DB_NAME", "quant_trading"),
        data_db=os.getenv("DATA_DB_NAME", "quant_data"),
        realtime_db=os.getenv("REALTIME_DB_NAME", "quant_realtime"),
        tushare_token=os.getenv("TUSHARE_TOKEN"),
        rate_limit_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "4")),  # basic soft limit
        job_status_sync_uri=_sync,
        deploy_mode=os.getenv("DEPLOY_MODE", "full"),
        enable_realtime_daily_k=_bool_env("ENABLE_REALTIME_DAILY_K", False),
        realtime_daily_k_shadow_mode=_bool_env("REALTIME_DAILY_K_SHADOW_MODE", True),
        realtime_daily_k_skip_stock_daily=_bool_env("REALTIME_DAILY_K_SKIP_STOCK_DAILY", False),
        realtime_daily_k_min_coverage=float(os.getenv("REALTIME_DAILY_K_MIN_COVERAGE", "0.98")),
        realtime_daily_k_min_field_coverage=float(os.getenv("REALTIME_DAILY_K_MIN_FIELD_COVERAGE", "0.98")),
        realtime_daily_k_equivalence_days=int(os.getenv("REALTIME_DAILY_K_EQUIVALENCE_DAYS", "3")),
        realtime_daily_k_price_tolerance_pct=float(os.getenv("REALTIME_DAILY_K_PRICE_TOLERANCE_PCT", "0.2")),
        realtime_daily_k_volume_tolerance_pct=float(os.getenv("REALTIME_DAILY_K_VOLUME_TOLERANCE_PCT", "5.0")),
        realtime_daily_k_collection=os.getenv("REALTIME_DAILY_K_COLLECTION", "market_spectrum_realtime_daily"),
        realtime_daily_k_source_mode=os.getenv("REALTIME_DAILY_K_SOURCE_MODE", "tushare_rt_k_daily"),
        realtime_daily_k_allow_outside_session=_bool_env("REALTIME_DAILY_K_ALLOW_OUTSIDE_SESSION", False),
        realtime_daily_k_batch_size=int(os.getenv("REALTIME_DAILY_K_BATCH_SIZE", "500")),
        realtime_daily_k_ts_code_patterns=os.getenv("REALTIME_DAILY_K_TS_CODE_PATTERNS", "3*.SZ,6*.SH,0*.SZ,9*.BJ"),
        realtime_daily_k_fields=os.getenv(
            "REALTIME_DAILY_K_FIELDS",
            "ts_code,name,pre_close,high,open,low,close,vol,amount,num,"
            "ask_price1,ask_volume1,bid_price1,bid_volume1,trade_time",
        ),
        intraday_t0_trigger_url=os.getenv("INTRADAY_T0_TRIGGER_URL", "").strip() or None,
        intraday_t0_trigger_key=(
            os.getenv("INTRADAY_T0_TRIGGER_KEY", "").strip()
            or os.getenv("HERMES_QUANT_INTERNAL_KEY", "").strip()
            or None
        ),
        intraday_t0_trigger_timeout_sec=float(os.getenv("INTRADAY_T0_TRIGGER_TIMEOUT_SEC", "5")),
        market_insight_trigger_url=os.getenv("MARKET_INSIGHT_TRIGGER_URL", "").strip() or None,
        market_insight_trigger_key=(
            os.getenv("MARKET_INSIGHT_TRIGGER_KEY", "").strip()
            or os.getenv("HERMES_QUANT_INTERNAL_KEY", "").strip()
            or None
        ),
        market_insight_trigger_timeout_sec=float(os.getenv("MARKET_INSIGHT_TRIGGER_TIMEOUT_SEC", "5")),
    )

__all__ = ["Settings", "get_settings"]
