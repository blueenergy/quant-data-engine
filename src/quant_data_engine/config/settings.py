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

# Load .env file from quantFinance or current directory, with override=True
env_path = pathlib.Path(__file__).parent.parent.parent.parent.parent / "quantFinance" / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    load_dotenv(override=True)

@dataclass
class Settings:
    mongo_uri: str
    mongo_db: str
    tushare_token: Optional[str]
    rate_limit_per_sec: float


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Get settings from environment variables.
    
    Cached to avoid recreating Settings object on every call.
    """
    return Settings(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "finance"),
        tushare_token=os.getenv("TUSHARE_TOKEN"),
        rate_limit_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "4")),  # basic soft limit
    )

__all__ = ["Settings", "get_settings"]
