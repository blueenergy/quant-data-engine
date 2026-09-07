"""Configuration model for the standalone miniQMT bridge."""
from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass, field, replace
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from .constants import (
    DEFAULT_PLAN_STATUSES,
    DEFAULT_QUOTE_RPC_TIMEOUT_SEC,
    FULL_MARKET_DAILY_K_INTERVAL,
    FULL_MARKET_POLL_BATCH_SIZE,
    FULL_MARKET_POLL_INTERVAL,
    FULL_MARKET_REFRESH,
    FULL_MARKET_WARMUP_SECONDS,
    RESERVED_SINKS,
    SUPPORTED_SINKS,
    TARGETED_DAILY_K_INTERVAL,
    TARGETED_POLL_BATCH_SIZE,
    TARGETED_POLL_INTERVAL,
    TARGETED_REFRESH,
    TARGETED_WARMUP_SECONDS,
    TICK_SOURCE_BIGQMT_RPC,
    TICK_SOURCE_MINIQMT,
    UNIVERSE_MODE_FULL_MARKET,
    UNIVERSE_MODE_TARGETED,
)


def _clean_optional(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _split_csv(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, Iterable):
        return [str(part).strip() for part in value if str(part).strip()]
    return [str(value).strip()]


def _universe_mode(value: Any, default: str = UNIVERSE_MODE_TARGETED) -> str:
    text = str(value or default).strip().lower()
    if text in ("all", "listed", "full", "full-market"):
        return UNIVERSE_MODE_FULL_MARKET
    if text in ("watchlist", "auto"):
        return UNIVERSE_MODE_TARGETED
    return text or default


@dataclass(frozen=True)
class UniverseConfig:
    """Symbol universe settings for the miniQMT bridge."""

    auto: bool = True
    mode: str = UNIVERSE_MODE_TARGETED
    symbols: tuple[str, ...] = ()
    include_watchlists: bool = True
    include_watchlist_strategies: bool = True
    include_portfolio_plans: bool = True
    include_paper_positions: bool = True
    plan_statuses: tuple[str, ...] = DEFAULT_PLAN_STATUSES

    @property
    def is_full_market(self) -> bool:
        return self.mode == UNIVERSE_MODE_FULL_MARKET

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "UniverseConfig":
        if not raw:
            return cls()
        cfg = cls()
        return replace(
            cfg,
            auto=bool(raw.get("auto", cfg.auto)),
            mode=_universe_mode(raw.get("mode", cfg.mode), cfg.mode),
            symbols=tuple(_split_csv(raw.get("symbols", cfg.symbols))),
            include_watchlists=bool(raw.get("include_watchlists", cfg.include_watchlists)),
            include_watchlist_strategies=bool(
                raw.get("include_watchlist_strategies", cfg.include_watchlist_strategies)
            ),
            include_portfolio_plans=bool(raw.get("include_portfolio_plans", cfg.include_portfolio_plans)),
            include_paper_positions=bool(raw.get("include_paper_positions", cfg.include_paper_positions)),
            plan_statuses=tuple(_split_csv(raw.get("plan_statuses", cfg.plan_statuses))) or cfg.plan_statuses,
        )

    def with_cli(self, args: Namespace) -> "UniverseConfig":
        cfg = self
        updates: dict[str, Any] = {}
        if hasattr(args, "universe_mode") and args.universe_mode is not None:
            updates["mode"] = _universe_mode(args.universe_mode)
        if hasattr(args, "auto") and args.auto is not None:
            updates["auto"] = bool(args.auto)
        if hasattr(args, "symbols") and args.symbols is not None:
            updates["symbols"] = tuple(_split_csv(args.symbols))
            updates["auto"] = False
            if not (hasattr(args, "universe_mode") and args.universe_mode is not None):
                updates["mode"] = UNIVERSE_MODE_TARGETED
        for cli_name, field_name in (
            ("include_watchlists", "include_watchlists"),
            ("include_watchlist_strategies", "include_watchlist_strategies"),
            ("include_portfolio_plans", "include_portfolio_plans"),
            ("include_paper_positions", "include_paper_positions"),
        ):
            if hasattr(args, cli_name):
                value = getattr(args, cli_name)
                if value is not None:
                    updates[field_name] = bool(value)
        if hasattr(args, "plan_statuses") and args.plan_statuses is not None:
            updates["plan_statuses"] = tuple(_split_csv(args.plan_statuses)) or DEFAULT_PLAN_STATUSES
        return replace(cfg, **updates)


@dataclass(frozen=True)
class BridgeConfig:
    """Runtime configuration for the standalone miniQMT bridge."""

    mongo_uri: Optional[str] = None
    sync_uri: Optional[str] = None
    sinks: tuple[str, ...] = ("realtime-daily-k",)
    mode: str = "loop"
    poll_interval: float = TARGETED_POLL_INTERVAL
    daily_k_interval: float = TARGETED_DAILY_K_INTERVAL
    refresh: int = TARGETED_REFRESH
    ignore_session: bool = False
    warmup_seconds: float = TARGETED_WARMUP_SECONDS
    poll_batch_size: int = TARGETED_POLL_BATCH_SIZE
    market_insight_trigger_url: Optional[str] = None
    market_insight_trigger_key: Optional[str] = None
    market_insight_trigger_timeout_sec: Optional[float] = None
    intraday_t0_trigger_url: Optional[str] = None
    intraday_t0_trigger_key: Optional[str] = None
    intraday_t0_trigger_timeout_sec: Optional[float] = None
    tick_source: str = TICK_SOURCE_MINIQMT
    quote_rpc_address: Optional[str] = None
    quote_account_id: Optional[str] = None
    quote_rpc_timeout_sec: float = DEFAULT_QUOTE_RPC_TIMEOUT_SEC
    shadow_mode: bool = False
    require_listed_from_db: bool = False
    universe: UniverseConfig = field(default_factory=UniverseConfig)

    @classmethod
    def from_json(cls, path: str | Path) -> "BridgeConfig":
        with Path(path).open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, Mapping):
            raise ValueError("miniQMT bridge config must be a JSON object")
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "BridgeConfig":
        if not raw:
            return cls().validate()
        universe = UniverseConfig.from_mapping(raw.get("universe"))
        is_fm = universe.is_full_market
        cfg = cls()
        sinks = raw.get("sinks", cfg.sinks)
        mode = raw.get("mode", cfg.mode)
        return replace(
            cfg,
            mongo_uri=_clean_optional(raw.get("mongo_uri", cfg.mongo_uri)),
            sync_uri=_clean_optional(raw.get("sync_uri", cfg.sync_uri)),
            sinks=tuple(_split_csv(sinks)) or cfg.sinks,
            mode=str(mode).strip().lower(),
            poll_interval=float(
                raw.get("poll_interval", FULL_MARKET_POLL_INTERVAL if is_fm else cfg.poll_interval)
            ),
            daily_k_interval=float(
                raw.get(
                    "daily_k_interval",
                    FULL_MARKET_DAILY_K_INTERVAL if is_fm else cfg.daily_k_interval,
                )
            ),
            refresh=int(raw.get("refresh", FULL_MARKET_REFRESH if is_fm else cfg.refresh)),
            ignore_session=bool(raw.get("ignore_session", cfg.ignore_session)),
            warmup_seconds=float(
                raw.get("warmup_seconds", FULL_MARKET_WARMUP_SECONDS if is_fm else cfg.warmup_seconds)
            ),
            poll_batch_size=int(
                raw.get(
                    "poll_batch_size",
                    FULL_MARKET_POLL_BATCH_SIZE if is_fm else cfg.poll_batch_size,
                )
            ),
            market_insight_trigger_url=_clean_optional(
                raw.get("market_insight_trigger_url", cfg.market_insight_trigger_url)
            ),
            market_insight_trigger_key=_clean_optional(
                raw.get("market_insight_trigger_key", cfg.market_insight_trigger_key)
            ),
            market_insight_trigger_timeout_sec=(
                float(raw["market_insight_trigger_timeout_sec"])
                if raw.get("market_insight_trigger_timeout_sec") not in (None, "")
                else cfg.market_insight_trigger_timeout_sec
            ),
            intraday_t0_trigger_url=_clean_optional(
                raw.get("intraday_t0_trigger_url", cfg.intraday_t0_trigger_url)
            ),
            intraday_t0_trigger_key=_clean_optional(
                raw.get("intraday_t0_trigger_key", cfg.intraday_t0_trigger_key)
            ),
            intraday_t0_trigger_timeout_sec=(
                float(raw["intraday_t0_trigger_timeout_sec"])
                if raw.get("intraday_t0_trigger_timeout_sec") not in (None, "")
                else cfg.intraday_t0_trigger_timeout_sec
            ),
            tick_source=str(raw.get("tick_source", cfg.tick_source)).strip().lower() or cfg.tick_source,
            quote_rpc_address=_clean_optional(raw.get("quote_rpc_address", cfg.quote_rpc_address)),
            quote_account_id=_clean_optional(raw.get("quote_account_id", cfg.quote_account_id)),
            quote_rpc_timeout_sec=float(raw.get("quote_rpc_timeout_sec", cfg.quote_rpc_timeout_sec)),
            shadow_mode=bool(raw.get("shadow_mode", cfg.shadow_mode)),
            require_listed_from_db=bool(raw.get("require_listed_from_db", cfg.require_listed_from_db)),
            universe=universe,
        ).validate()

    def merge_cli(self, args: Namespace) -> "BridgeConfig":
        updates: dict[str, Any] = {}
        overridden: set[str] = set()
        if hasattr(args, "mongo_uri") and args.mongo_uri is not None:
            updates["mongo_uri"] = _clean_optional(args.mongo_uri)
        if hasattr(args, "sync_uri") and args.sync_uri is not None:
            updates["sync_uri"] = _clean_optional(args.sync_uri)
        if hasattr(args, "sinks") and args.sinks is not None:
            updates["sinks"] = tuple(_split_csv(args.sinks))
        if hasattr(args, "once") and args.once:
            updates["mode"] = "once"
        if hasattr(args, "loop") and args.loop:
            updates["mode"] = "loop"
        if hasattr(args, "poll_interval") and args.poll_interval is not None:
            updates["poll_interval"] = float(args.poll_interval)
            overridden.add("poll_interval")
        if hasattr(args, "daily_k_interval") and args.daily_k_interval is not None:
            updates["daily_k_interval"] = float(args.daily_k_interval)
            overridden.add("daily_k_interval")
        if hasattr(args, "refresh") and args.refresh is not None:
            updates["refresh"] = int(args.refresh)
            overridden.add("refresh")
        if hasattr(args, "ignore_session") and args.ignore_session is not None:
            updates["ignore_session"] = bool(args.ignore_session)
        if hasattr(args, "warmup_seconds") and args.warmup_seconds is not None:
            updates["warmup_seconds"] = float(args.warmup_seconds)
            overridden.add("warmup_seconds")
        if hasattr(args, "poll_batch_size") and args.poll_batch_size is not None:
            updates["poll_batch_size"] = int(args.poll_batch_size)
            overridden.add("poll_batch_size")
        for cli_name, field_name in (
            ("market_insight_trigger_url", "market_insight_trigger_url"),
            ("market_insight_trigger_key", "market_insight_trigger_key"),
            ("intraday_t0_trigger_url", "intraday_t0_trigger_url"),
            ("intraday_t0_trigger_key", "intraday_t0_trigger_key"),
        ):
            if hasattr(args, cli_name) and getattr(args, cli_name) is not None:
                updates[field_name] = _clean_optional(getattr(args, cli_name))
        if hasattr(args, "market_insight_trigger_timeout_sec") and args.market_insight_trigger_timeout_sec is not None:
            updates["market_insight_trigger_timeout_sec"] = float(args.market_insight_trigger_timeout_sec)
        if hasattr(args, "intraday_t0_trigger_timeout_sec") and args.intraday_t0_trigger_timeout_sec is not None:
            updates["intraday_t0_trigger_timeout_sec"] = float(args.intraday_t0_trigger_timeout_sec)
        if hasattr(args, "tick_source") and args.tick_source is not None:
            updates["tick_source"] = str(args.tick_source).strip().lower()
        if hasattr(args, "quote_rpc_address") and args.quote_rpc_address is not None:
            updates["quote_rpc_address"] = _clean_optional(args.quote_rpc_address)
        if hasattr(args, "quote_account_id") and args.quote_account_id is not None:
            updates["quote_account_id"] = _clean_optional(args.quote_account_id)
        if hasattr(args, "quote_rpc_timeout") and args.quote_rpc_timeout is not None:
            updates["quote_rpc_timeout_sec"] = float(args.quote_rpc_timeout)
        if hasattr(args, "shadow_mode") and args.shadow_mode:
            updates["shadow_mode"] = True
        if hasattr(args, "require_listed_from_db") and args.require_listed_from_db:
            updates["require_listed_from_db"] = True
        cfg = replace(self, universe=self.universe.with_cli(args), **updates)
        if cfg.universe.is_full_market:
            cfg = cfg.with_full_market_defaults(overridden=overridden)
        return cfg.validate()

    def with_full_market_defaults(self, *, overridden: set[str] | None = None) -> "BridgeConfig":
        """Fill Tushare-like 1-minute snapshot defaults when switching to full_market."""
        overridden = overridden or set()
        updates: dict[str, Any] = {}
        if "poll_interval" not in overridden and self.poll_interval == TARGETED_POLL_INTERVAL:
            updates["poll_interval"] = FULL_MARKET_POLL_INTERVAL
        if "daily_k_interval" not in overridden and self.daily_k_interval == TARGETED_DAILY_K_INTERVAL:
            updates["daily_k_interval"] = FULL_MARKET_DAILY_K_INTERVAL
        if "refresh" not in overridden and self.refresh == TARGETED_REFRESH:
            updates["refresh"] = FULL_MARKET_REFRESH
        if "poll_batch_size" not in overridden and self.poll_batch_size == TARGETED_POLL_BATCH_SIZE:
            updates["poll_batch_size"] = FULL_MARKET_POLL_BATCH_SIZE
        if "warmup_seconds" not in overridden and self.warmup_seconds == TARGETED_WARMUP_SECONDS:
            updates["warmup_seconds"] = FULL_MARKET_WARMUP_SECONDS
        return replace(self, **updates) if updates else self

    def validate(self) -> "BridgeConfig":
        mode = self.mode.strip().lower()
        if mode not in ("once", "loop"):
            raise ValueError("mode must be one of: once, loop")
        universe_mode = str(self.universe.mode).strip().lower()
        if universe_mode not in (UNIVERSE_MODE_TARGETED, UNIVERSE_MODE_FULL_MARKET):
            raise ValueError("universe.mode must be one of: targeted, full_market")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self.daily_k_interval <= 0:
            raise ValueError("daily_k_interval must be positive")
        if self.refresh < 0:
            raise ValueError("refresh must be >= 0")
        if self.poll_batch_size < 0:
            raise ValueError("poll_batch_size must be >= 0")
        unknown = [sink for sink in self.sinks if sink not in SUPPORTED_SINKS and sink not in RESERVED_SINKS]
        if unknown:
            raise ValueError(f"unsupported sink(s): {', '.join(unknown)}")
        reserved = [sink for sink in self.sinks if sink in RESERVED_SINKS]
        if reserved:
            raise NotImplementedError(f"{', '.join(reserved)} sink is not implemented yet")
        if not self.sinks:
            raise ValueError("at least one sink must be configured")
        if "minute-bars" in self.sinks and universe_mode == UNIVERSE_MODE_FULL_MARKET:
            raise ValueError("minute-bars sink requires universe.mode=targeted, not full_market")
        if (
            universe_mode == UNIVERSE_MODE_TARGETED
            and self.universe.auto is False
            and not self.universe.symbols
        ):
            raise ValueError("manual mode requires at least one symbol")
        if not self.universe.plan_statuses:
            raise ValueError("plan_statuses must not be empty")
        for timeout_name, timeout_value in (
            ("market_insight_trigger_timeout_sec", self.market_insight_trigger_timeout_sec),
            ("intraday_t0_trigger_timeout_sec", self.intraday_t0_trigger_timeout_sec),
            ("quote_rpc_timeout_sec", self.quote_rpc_timeout_sec),
        ):
            if timeout_value is not None and float(timeout_value) <= 0:
                raise ValueError(f"{timeout_name} must be positive")
        tick_source = str(self.tick_source).strip().lower()
        if tick_source not in (TICK_SOURCE_MINIQMT, TICK_SOURCE_BIGQMT_RPC):
            raise ValueError("tick_source must be one of: miniqmt, bigqmt_rpc")
        if tick_source == TICK_SOURCE_BIGQMT_RPC and not self.quote_rpc_address and not self.quote_account_id:
            raise ValueError("bigqmt_rpc requires quote_rpc_address or quote_account_id")
        return replace(self, mode=mode, tick_source=tick_source, universe=replace(self.universe, mode=universe_mode))

    def overlay_settings(self, settings: Any) -> Any:
        """Apply JSON/CLI overrides onto process settings (env remains fallback)."""
        updates: dict[str, Any] = {
            "mongo_uri": self.mongo_uri or settings.mongo_uri,
            "job_status_sync_uri": self.sync_uri or settings.job_status_sync_uri,
            "market_insight_trigger_url": self.market_insight_trigger_url or settings.market_insight_trigger_url,
            "market_insight_trigger_key": self.market_insight_trigger_key or settings.market_insight_trigger_key,
            "intraday_t0_trigger_url": self.intraday_t0_trigger_url or settings.intraday_t0_trigger_url,
            "intraday_t0_trigger_key": self.intraday_t0_trigger_key or settings.intraday_t0_trigger_key,
        }
        if self.market_insight_trigger_timeout_sec is not None:
            updates["market_insight_trigger_timeout_sec"] = float(self.market_insight_trigger_timeout_sec)
        if self.intraday_t0_trigger_timeout_sec is not None:
            updates["intraday_t0_trigger_timeout_sec"] = float(self.intraday_t0_trigger_timeout_sec)
        return replace(settings, **updates)
