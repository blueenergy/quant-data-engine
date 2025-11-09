"""Global settings loader for quant-data-engine.
Reads environment variables with sane defaults.
"""
from dataclasses import dataclass
import os

@dataclass
class Settings:
    mongo_uri: str
    mongo_db: str
    tushare_token: str | None
    rate_limit_per_sec: float


def get_settings() -> Settings:
    return Settings(
        mongo_uri=os.getenv("MONGO_URL", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "finance"),
        tushare_token=os.getenv("TUSHARE_TOKEN"),
        rate_limit_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "4")),  # basic soft limit
    )

__all__ = ["Settings", "get_settings"]
