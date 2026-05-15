# Realtime Daily K Mining Ideas

This note records short-horizon research ideas enabled by Tushare `rt_k` realtime daily K snapshots.

## Data Fields

The realtime daily K sync stores standard daily OHLCV fields in `quant_data.volume_price` and keeps realtime-only fields under both:

- top-level `realtime_*` fields for quick filtering
- nested `realtime_daily_snapshot` for source-specific audit/comparison

Important realtime-only fields:

- `realtime_ask_price1`, `realtime_ask_volume1`
- `realtime_bid_price1`, `realtime_bid_volume1`
- `realtime_trade_time`
- `realtime_num`
- `realtime_amount_yuan`

Note: `rt_k.amount` is yuan. `volume_price.amount` follows Tushare daily's thousand-yuan convention, so realtime sync converts `amount / 1000` for the standard field and preserves original yuan value in `realtime_amount_yuan`.

## Candidate Signals

### Intraday Breadth Inflection

Use `market_spectrum_realtime_daily` to track whether the share of stocks above MA5/MA10 improves during the day.

Potential features:

- `yang_spectrum` 30/60 minute slope
- `yang_spectrum` low-to-high reversal
- index flat/up while breadth improves

### Order Book Pressure Spectrum

Use best bid/ask volume as a simple microstructure pressure proxy:

```text
imbalance = (bid_volume1 - ask_volume1) / (bid_volume1 + ask_volume1)
```

Potential aggregate signals:

- share of stocks with `imbalance > 0`
- sector-level imbalance minus market-level imbalance
- `close > MA5` and `imbalance > 0`

### Spread and Liquidity Spectrum

Use best bid/ask prices to filter fragile signals:

```text
spread_pct = (ask_price1 - bid_price1) / close
```

Potential aggregate signals:

- narrow-spread stock ratio
- spread expansion as a market stress signal
- breadth improvement with narrow spread as a higher-quality confirmation

### Trade Activity Quality

Use `num`, `vol`, and `amount` to approximate activity structure:

```text
avg_trade_amount = amount_yuan / num
avg_trade_volume = vol / num
```

Potential uses:

- distinguish broad retail activity from larger-ticket accumulation
- filter low-quality breakouts with weak transaction count
- compare theme-level activity acceleration

### Data Freshness and Staleness

Use `trade_time` to detect stale quotes and lower signal confidence.

Potential uses:

- exclude symbols whose `trade_time` is stale
- monitor market-wide quote freshness distribution
- add freshness into realtime daily K `quality_passed`

## First Experiments

1. Intraday realtime daily K breadth inflection: `yang_spectrum` slope and reversal.
2. Sector relative breadth: sector `yang_spectrum` minus market `yang_spectrum`.
3. Order book pressure confirmation: `close > MA5` and `imbalance > 0`.
4. Liquidity filter: avoid candidates with wide `spread_pct`.
5. Tail-session candidate pool: after 14:30, screen for MA reclaim + volume expansion + positive order-book pressure.
