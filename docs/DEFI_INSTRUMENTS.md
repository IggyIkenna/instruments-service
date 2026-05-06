# DeFi Instrument Pipeline

<!-- POST_PLAN_SECTION_2026_05_06 -->

## Post-2026-05-06 additions

**Post-2026-05-06 additions** — DeFi `chain` is a first-class shard axis (independent RPC/subgraph endpoints + failure isolation). instrument_type is display-only (NOT a shard axis — bulk subgraph fetch). Three-category empty-output decision applies; pre-genesis dates per chain → `record_empty(row_key)` honest absence.

**Workspace SSOTs**: [POST_PLAN_REALITY](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) (10 cross-cutting principles + active plans), [availability-manifest-and-data-status](../../unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md), [deployment-clusters-live-vs-batch](../../unified-trading-pm/codex/05-infrastructure/deployment-clusters-live-vs-batch.md), [shard-level-failure-isolation](../../unified-trading-pm/codex/04-architecture/shard-level-failure-isolation.md), [error-handling](../../unified-trading-pm/codex/06-coding-standards/error-handling.md), [validation-patterns](../../unified-trading-pm/codex/06-coding-standards/validation-patterns.md).

## Overview

DeFi instruments are fetched from **The Graph** (DEX pools via subgraph queries) and **direct on-chain APIs** (lending protocols, LSTs, yield). All adapters query Ethereum mainnet. Each venue is a `PROTOCOL-CHAIN` pair (e.g. `UNISWAPV3-ETHEREUM`).

## Venues (12 active)

| Venue              | Source     | Instrument Type | What it represents                 |
| ------------------ | ---------- | --------------- | ---------------------------------- |
| UNISWAPV3-ETHEREUM | The Graph  | Pool            | Liquidity pools (concentrated)     |
| UNISWAPV4-ETHEREUM | The Graph  | Pool            | Liquidity pools (hooks-enabled)    |
| UNISWAPV2-ETHEREUM | The Graph  | Pool            | Liquidity pools (constant product) |
| BALANCER-ETHEREUM  | The Graph  | Pool            | Weighted/stable pools              |
| CURVE-ETHEREUM     | The Graph  | Pool            | Stableswap + tricrypto pools       |
| AAVEV3-ETHEREUM    | The Graph  | Lending Market  | Supply/borrow markets              |
| MORPHO-ETHEREUM    | The Graph  | Lending Market  | Morpho vaults (curated)            |
| EULER-ETHEREUM     | Direct API | Lending Market  | Lending vaults                     |
| FLUID-ETHEREUM     | Direct API | Lending Market  | Lending pairs                      |
| LIDO-ETHEREUM      | Direct API | LST             | stETH/wstETH staking               |
| ETHERFI-ETHEREUM   | Direct API | LST             | eETH staking                       |
| ETHENA-ETHEREUM    | Direct API | Yield           | USDe/sUSDe yield                   |

## Filtering

### Major Assets Whitelist (UAC `defi_major_assets.py`)

55 curated symbols representing the major DeFi ecosystem:

**Core:** ETH, WETH, BTC, WBTC, USDT, USDC, DAI
**LSTs:** stETH, wstETH, rETH, cbETH, swETH, oSETH, mETH, ankrETH, frxETH, eETH, weETH, ezETH, rsETH, ETHX
**Wrapped BTC:** tBTC, cbBTC, LBTC
**Stablecoins:** FRAX, LUSD, GHO, PYUSD, crvUSD, USDE, sUSDE, USDP, sUSD, EURC
**Governance/DeFi blue chips:** UNI, AAVE, MKR, COMP, CRV, LDO, RPL, BAL, SNX, FXS, SUSHI, 1INCH, LINK
**Other:** SOL, MATIC

### DEX Pool Filter (both sides required)

For DEX venues (Uniswap, Balancer, Curve), **both** `base_asset` AND `quote_asset` must be in the major assets whitelist. This removes long-tail meme pairs (PEPE/WETH, FAITH/MILAREPA, etc.) while keeping core DeFi pairs (WETH/USDC, WBTC/WETH, wstETH/WETH).

**Identified by:** `DEX_VENUE_KEYWORDS = ["UNISWAP", "CURVE", "BALANCER"]`

### Lending Market Filter (base side + quote side)

For lending protocols (Aave, Morpho, Euler, Fluid), both `base_asset` (collateral) AND `quote_asset` (borrow asset) must be in the major assets whitelist. This was tightened from base-only filtering after discovering Morpho had exotic quote assets leaking through (JPYC, WARS, EURCV, etc.).

### Venue Launch Date Filtering

Instruments from venues that didn't exist on the target date are excluded. Launch dates from UAC `VenueMapping.venue_start_dates`:

| Venue     | Launch Date |
| --------- | ----------- |
| Curve     | 2020-01-20  |
| UniswapV2 | 2020-05-18  |
| Lido      | 2020-12-18  |
| Balancer  | 2020-03-31  |
| UniswapV3 | 2021-05-05  |
| AaveV3    | 2023-01-27  |
| EtherFi   | 2023-11-01  |
| Euler     | 2023-12-18  |
| Morpho    | 2024-01-08  |
| Ethena    | 2024-02-19  |
| Fluid     | 2024-03-01  |
| UniswapV4 | 2025-01-31  |

### `createdAtTimestamp` Date Filter

DeFi pools have `createdAtTimestamp` from The Graph. Only pools created on or before the target date are included. This gives accurate historical snapshots.

## The Graph Query: Top-500-by-TVL

DEX adapters fetch the top 500 pools ordered by `totalValueLockedUSD DESC` from the subgraph. The major-asset filter then reduces this to the relevant pools. Since the highest-TVL pools are predominantly major-asset pairs, the filter captures nearly all of them.

**Future improvement (planned):** Filter by token address directly in the GraphQL query. The subgraph supports `where: { token0_in: [...] }`. This would fetch only relevant pools instead of 500 random-TVL ones.

## Instrument Counts (2026-03-23)

| Venue     | Raw       | After filter | Notes                        |
| --------- | --------- | ------------ | ---------------------------- |
| UniswapV3 | 500       | 89           | Concentrated liquidity pools |
| UniswapV4 | 500       | 64           | Hooks-enabled pools          |
| Balancer  | 500       | 60           | Weighted + stable pools      |
| AaveV3    | 86        | 51           | Supply/borrow markets        |
| Morpho    | 354       | 44           | Curated vaults               |
| UniswapV2 | 500       | 11           | Legacy AMM                   |
| Curve     | 49        | 11           | Stableswap pools             |
| Fluid     | 6         | 6            | All major                    |
| Euler     | 2         | 2            | All major                    |
| Lido      | 2         | 2            | stETH/wstETH                 |
| EtherFi   | 1         | 1            | eETH                         |
| Ethena    | 1         | 1            | sUSDe                        |
| **Total** | **2,502** | **331**      | 87% filtered out             |

### Historical Counts

| Date       | Venues | Instruments | Notes                   |
| ---------- | ------ | ----------- | ----------------------- |
| 2022-06-15 | 5      | 123         | Pre-AaveV3, pre-UniV4   |
| 2023-09-20 | 6      | 191         | AaveV3 live, pre-Morpho |
| 2026-03-23 | 11     | 331         | Full ecosystem          |

## Caching Strategy (Implemented)

Adapter TTL cache (1hr) + factory adapter pool. Each DeFi adapter (UniswapV3, Balancer, Aave, etc.) caches its pool/market list after the first fetch. DeFi pools never disappear from The Graph, so the cached set is valid for the full batch run. `createdAtTimestamp` filtering happens after cache retrieval.

## Schema (22 fields)

`asset_group` is always `crypto`. `raw_symbol` is the pool/vault address (e.g. `0x88e6a0c2...`). `tick_size`, `lot_size`, `contract_size` are all None (DeFi uses continuous liquidity). Session metadata is all None (24/7 on-chain). `available_since` is populated from `createdAtTimestamp`.

## Downstream: MTDS Integration

DeFi instruments flow to market-tick-data-service via `BaseDefiAdapter.download_batch()`. MTDS loads the instrument parquet from GCS, then calls `download_market_data()` per instrument. Currently wired for Aave (rate_indices, oracle_prices, utilization, risk_params). Other protocols need their `download_market_data()` implemented.
