# CeFi Instrument Pipeline

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**Post-2026-05-06 additions** — CeFi options/futures bundle shards now subject to mandatory cluster validation at `record_captured` (`expected_root_clusters` + `cluster_extractor` kwargs). v6 columns `quote_asset` + `margin_type` for DERIBIT inverse vs linear; `combo_type` + `leg_weights` for spreads/butterflies/iron condors. Three-category empty-output decision (A/B/C) at every adapter; `_create_empty_output()` placeholder method banned.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Overview

CeFi instruments are fetched from **Tardis** (9 exchanges), **Deribit** (direct API), **Hyperliquid** (direct API), and **Aster** (direct API). The URDI Tardis adapter is the primary path — it handles Binance, Bybit, OKX, Coinbase, and Upbit.

## Venues (11 active)

| Venue           | Source      | Instrument Types                         |
| --------------- | ----------- | ---------------------------------------- |
| BINANCE-SPOT    | Tardis      | Spot pairs                               |
| BINANCE-FUTURES | Tardis      | Perpetuals, quarterly futures            |
| OKX             | Tardis      | Spot + derivatives                       |
| BYBIT           | Tardis      | Spot + derivatives                       |
| COINBASE        | Tardis      | Spot pairs                               |
| UPBIT           | Tardis      | KRW/BTC/USDT pairs (QUOTE-BASE format)   |
| DERIBIT         | Tardis      | BTC/ETH options + futures + perps + spot |
| HYPERLIQUID     | Direct REST | Perpetuals                               |
| ASTER           | Direct REST | Perpetuals                               |

**Removed:** GEMINI-SPOT, PHEMEX-SPOT (low volume, removed from all registries).

## Filtering

### Base Asset Universe (UAC `cefi_instrument_universe.py`)

Only instruments with `base_asset` in the curated universe pass through:

- **Top 20 by market cap:** BTC, ETH, SOL, XRP, BNB, ADA, DOGE, AVAX, DOT, LINK, MATIC, UNI, ATOM, LTC, FIL, NEAR, APE, ARB, OP, TRX
- **Stablecoins (as base):** USDT, USDC, DAI, TUSD, BUSD
- **Delisting test assets:** FTT, LUNA (verify system handles delistings)

### Quote Asset Filter

Only accepted quotes: `USDT`, `USDC`, `USD`, `BTC` (BTC pairs for cross-exchange arb).

### Deribit Options Filter

Options are restricted to **BTC and ETH underlyings only** (`CEFI_OPTIONS_UNDERLYINGS`). This reduces Deribit from 300K+ historical options to ~2,000 active ones.

### UPBIT Symbol Inversion

UPBIT uses QUOTE-BASE format (`KRW-BTC` = buy BTC with KRW). The Tardis adapter detects this and inverts base/quote before filtering.

## Expiry Parsing

Deribit symbols encode expiry: `BTC-27MAR26-190000-C` -> `2026-03-27`. The Tardis adapter parses DDMMMYY from the second segment when the Tardis API doesn't populate the `expiry` field (common for active options).

## Instrument Counts (2026-03-23)

| Venue           | Count                                        |
| --------------- | -------------------------------------------- |
| DERIBIT         | 2,117 (1,916 options + 12 futures + 2 perps) |
| OKX             | 115                                          |
| COINBASE        | 53                                           |
| BINANCE-SPOT    | 48                                           |
| BINANCE-FUTURES | 33                                           |
| BYBIT           | 32                                           |
| HYPERLIQUID     | 21                                           |
| ASTER           | 19                                           |
| UPBIT           | 12                                           |
| **Total**       | **2,450**                                    |

## Caching Strategy (Implemented)

Three layers:

1. **Adapter TTL cache** — `get_instruments_cached()` stores results for 1hr. First call hits API, subsequent calls return instantly.
2. **Factory adapter pool** — all Tardis venues (Binance, Bybit, OKX, etc.) share one adapter instance with one shared cache.
3. **Concurrency cap** — `asyncio.Semaphore(4)` limits concurrent API calls.

Tardis returns ALL instruments ever listed with `availableSince`/`availableTo` timestamps. The cache means this call happens once per run, not per-venue.

## Schema (22 fields)

`asset_group` is always `crypto` for CeFi instruments. `margin_type` is set by the adapter when relevant (LINEAR for USDT-margined, INVERSE for coin-margined). Session metadata (`is_trading_day`, `regular_open_utc`, etc.) is always None — crypto markets are 24/7.
