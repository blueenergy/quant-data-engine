"""Filter configuration for A-share event volume control."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Set

from quant_data_engine.market_events.identity import load_yaml_config, normalize_symbol


@dataclass
class MarketEventsFilterConfig:
    min_importance: int = 3
    lookahead_days: int = 14
    retention_days: int = 14
    symbol_whitelist: Set[str] = field(default_factory=set)
    # When True (default), empty whitelist means NO individual A-share events —
    # only aggregates / unlock TopN. Prevents calendar flood.
    individual_require_whitelist: bool = True
    unlock_top_n_per_day: int = 10
    unlock_min_mktcap_yuan: float = 5_000_000_000
    gsrl_important_types: Set[str] = field(default_factory=set)
    report_aggregate_outside_whitelist: bool = True
    us_earnings_boost_symbols: Set[str] = field(default_factory=set)

    @classmethod
    def from_yaml(cls) -> "MarketEventsFilterConfig":
        raw = load_yaml_config("market_events_filter.yaml")
        wl = {normalize_symbol(s) for s in (raw.get("symbol_whitelist") or []) if s}
        gsrl_types = set(raw.get("gsrl", {}).get("important_event_types") or [])
        us_boost = {str(s).upper() for s in (raw.get("us_earnings_boost_symbols") or [])}
        unlock = raw.get("unlock") or {}
        return cls(
            min_importance=int(raw.get("min_importance", 3)),
            lookahead_days=int(raw.get("lookahead_days", 14)),
            retention_days=int(raw.get("retention_days", 14)),
            symbol_whitelist=wl,
            individual_require_whitelist=bool(raw.get("individual_require_whitelist", True)),
            unlock_top_n_per_day=int(unlock.get("top_n_per_day", 10)),
            unlock_min_mktcap_yuan=float(unlock.get("min_mktcap_yuan", 5_000_000_000)),
            gsrl_important_types=gsrl_types,
            report_aggregate_outside_whitelist=bool(
                raw.get("report", {}).get("aggregate_outside_whitelist", True)
            ),
            us_earnings_boost_symbols=us_boost,
        )

    def symbol_allowed(self, symbol: str) -> bool:
        """Return True only when symbol is on the whitelist (or whitelist disabled)."""
        if not self.symbol_whitelist:
            return not self.individual_require_whitelist
        return normalize_symbol(symbol) in self.symbol_whitelist
