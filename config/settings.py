"""Global settings loader for quant-data-engine.
Reads environment variables with sane defaults.
"""

from dataclasses import dataclass
from typing import Optional
import os
from functools import lru_cache
from dotenv import load_dotenv

# Load .env file once at module import time
load_dotenv()

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
        mongo_uri=os.getenv("MONGO_URL", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "finance"),
        tushare_token=os.getenv("TUSHARE_TOKEN"),
        rate_limit_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "4")),  # basic soft limit
    )

__all__ = ["Settings", "get_settings"]
