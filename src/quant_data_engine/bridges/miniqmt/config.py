"""Configuration model for the standalone miniQMT bridge."""
from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass, field, replace
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from .constants import DEFAULT_PLAN_STATUSES, RESERVED_SINKS, SUPPORTED_SINKS


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


@dataclass(frozen=True)
class UniverseConfig:
    """Symbol universe settings for the miniQMT bridge."""

    auto: bool = True
    symbols: tuple[str, ...] = ()
    include_watchlists: bool = True
    include_watchlist_strategies: bool = True
    include_portfolio_plans: bool = True
    plan_statuses: tuple[str, ...] = DEFAULT_PLAN_STATUSES

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "UniverseConfig":
        if not raw:
            return cls()
        cfg = cls()
        return replace(
            cfg,
            auto=bool(raw.get("auto", cfg.auto)),
            symbols=tuple(_split_csv(raw.get("symbols", cfg.symbols))),
            include_watchlists=bool(raw.get("include_watchlists", cfg.include_watchlists)),
            include_watchlist_strategies=bool(
                raw.get("include_watchlist_strategies", cfg.include_watchlist_strategies)
            ),
            include_portfolio_plans=bool(raw.get("include_portfolio_plans", cfg.include_portfolio_plans)),
            plan_statuses=tuple(_split_csv(raw.get("plan_statuses", cfg.plan_statuses))) or cfg.plan_statuses,
        )

    def with_cli(self, args: Namespace) -> "UniverseConfig":
        cfg = self
        updates: dict[str, Any] = {}
        if hasattr(args, "auto") and args.auto is not None:
            updates["auto"] = bool(args.auto)
        if hasattr(args, "symbols") and args.symbols is not None:
            updates["symbols"] = tuple(_split_csv(args.symbols))
            updates["auto"] = False
        for cli_name, field_name in (
            ("include_watchlists", "include_watchlists"),
            ("include_watchlist_strategies", "include_watchlist_strategies"),
            ("include_portfolio_plans", "include_portfolio_plans"),
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
    poll_interval: float = 0.5
    daily_k_interval: float = 5.0
    refresh: int = 60
    ignore_session: bool = False
    warmup_seconds: float = 0.5
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
        cfg = cls()
        sinks = raw.get("sinks", cfg.sinks)
        mode = raw.get("mode", cfg.mode)
        return replace(
            cfg,
            mongo_uri=_clean_optional(raw.get("mongo_uri", cfg.mongo_uri)),
            sync_uri=_clean_optional(raw.get("sync_uri", cfg.sync_uri)),
            sinks=tuple(_split_csv(sinks)) or cfg.sinks,
            mode=str(mode).strip().lower(),
            poll_interval=float(raw.get("poll_interval", cfg.poll_interval)),
            daily_k_interval=float(raw.get("daily_k_interval", cfg.daily_k_interval)),
            refresh=int(raw.get("refresh", cfg.refresh)),
            ignore_session=bool(raw.get("ignore_session", cfg.ignore_session)),
            warmup_seconds=float(raw.get("warmup_seconds", cfg.warmup_seconds)),
            universe=UniverseConfig.from_mapping(raw.get("universe")),
        ).validate()

    def merge_cli(self, args: Namespace) -> "BridgeConfig":
        updates: dict[str, Any] = {}
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
        if hasattr(args, "daily_k_interval") and args.daily_k_interval is not None:
            updates["daily_k_interval"] = float(args.daily_k_interval)
        if hasattr(args, "refresh") and args.refresh is not None:
            updates["refresh"] = int(args.refresh)
        if hasattr(args, "ignore_session") and args.ignore_session is not None:
            updates["ignore_session"] = bool(args.ignore_session)
        if hasattr(args, "warmup_seconds") and args.warmup_seconds is not None:
            updates["warmup_seconds"] = float(args.warmup_seconds)
        return replace(self, universe=self.universe.with_cli(args), **updates).validate()

    def validate(self) -> "BridgeConfig":
        mode = self.mode.strip().lower()
        if mode not in ("once", "loop"):
            raise ValueError("mode must be one of: once, loop")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if self.daily_k_interval <= 0:
            raise ValueError("daily_k_interval must be positive")
        if self.refresh < 0:
            raise ValueError("refresh must be >= 0")
        unknown = [sink for sink in self.sinks if sink not in SUPPORTED_SINKS and sink not in RESERVED_SINKS]
        if unknown:
            raise ValueError(f"unsupported sink(s): {', '.join(unknown)}")
        reserved = [sink for sink in self.sinks if sink in RESERVED_SINKS]
        if reserved:
            raise NotImplementedError(f"{', '.join(reserved)} sink is not implemented yet")
        if not self.sinks:
            raise ValueError("at least one sink must be configured")
        if self.universe.auto is False and not self.universe.symbols:
            raise ValueError("manual mode requires at least one symbol")
        if not self.universe.plan_statuses:
            raise ValueError("plan_statuses must not be empty")
        return replace(self, mode=mode)

