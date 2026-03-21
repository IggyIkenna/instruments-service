# DeFi Guide

> **Related Documentation**:
>
> - [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter pattern and implementation
> - [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Instrument ID specification
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture

---

## Overview

This guide covers DeFi protocol integration, data sources, and instrument discovery for the instruments-service. The service fetches **instrument definitions** (metadata about what instruments exist), not market data (rates, prices, OHLCV).

**Key Distinction**:

- **Instrument Definitions**: Pool addresses, token addresses, fee tiers, contract addresses
- **Market Data**: Supply/borrow rates, oracle prices, OHLCV prices, staking yields (fetched by other services)

## Multi-Chain Support

**Supported Chains**:

- **Ethereum** (`@ETHEREUM`) - Primary chain for MVP
- **Plasma** (`@PLASMA`) - L1 for stablecoins (Euler, Fluid, AAVE Plasma markets)
- **Hyperliquid** (`@HYPERLIQUID`) - HyperEVM chain (perpetual futures DEX)
- **Aster** (`@ASTER`) - Perpetual futures exchange

**Instrument Key Format**: `VENUE:INSTRUMENT_TYPE:SYMBOL@CHAIN`

**Examples**:

- `AAVE_V3_ETH:A_TOKEN:aUSDC@ETHEREUM` - Ethereum AAVE supply token
- `EULER-PLASMA:SUPPLY_TOKEN:eUSDC@PLASMA` - Plasma Euler supply token
- `HYPERLIQUID:PERP:BTCUSDT@HYPERLIQUID` - Hyperliquid perpetual futures
- `ASTER:PERP:BTCUSDT@ASTER` - Aster perpetual futures

## Supported DeFi Protocols

### DEX Protocols

#### Uniswap V2/V3/V4

**Status**:

- ✅ **V3**: Implemented (`UniswapV3Adapter`)
- ✅ **V2**: Implemented (`UniswapV2Adapter`)
- ✅ **V4**: Implemented (`UniswapV4Adapter`) - Uses Envio fallback

**Data Sources**:

- **The Graph**: Primary source for V2/V3 (subgraph queries)
- **Envio**: Fallback for V4 (HyperSync API)
- **RPC**: Future option (requires event tracking)

**Uniswap V4 Fallback Order**:

1. The Graph Network gateway (if subgraph ID available)
2. Envio indexer (primary fallback)
3. RPC queries (skipped for MVP)

**Instrument Format**:

- **Pools**: `UNISWAPV3-ETH:POOL:ETH-USDC:3000@ETHEREUM`
- **Spot Pairs**: `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`

**Fee Tiers**:

- V2: 3000 bps (0.3%) implied
- V3: 100, 500, 3000, 10000 bps (0.01%, 0.05%, 0.3%, 1%)

**Verified Subgraph IDs**:

- V3: `5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV`
- V2: `A3Np3RQbaBA6oKJgiwDJeo5T3zrYfGHPWFYayMwtNDum`
- V4: `DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G`

#### Curve

**Status**: ⚠️ **Partially Implemented** - Subgraph deprecated, using RPC fallback

**Data Sources**:

- **The Graph**: Primary source (if subgraph available)
- **RPC**: Fallback via Curve Registry contract (`0x90E00ACe148ca3b23Ac1bC8C240C2a7Dd9c2d9f5`)

**Curve Fallback Order**:

1. The Graph Network gateway (if subgraph ID available)
2. RPC direct contract queries (primary fallback)

**Instrument Format**:

- **Pools**: `CURVE-ETH:POOL:ETH-USDT@ETHEREUM`
- **Spot Pairs**: `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM`

**Note**: Curve subgraph is deprecated. RPC adapter uses Curve Registry contract for pool discovery.

#### Balancer

**Status**: ✅ **Implemented** - Using Balancer API v3

**Data Source**: Balancer API v3

**Instrument Format**:

- **Pools**: `BALANCER-ETH:POOL:ETH-USDC@ETHEREUM`

### Lending Protocols

#### AAVE V3

**Status**: ✅ **Implemented** (`AaveV3Adapter`)

**Data Sources**:

- **The Graph**: AAVE V3 subgraph (primary)
- **AaveScan API**: Fallback

**Instrument Format**:

- **aTokens**: `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
- **Debt Tokens**: `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

**Metadata Includes**:

- Risk parameters (LTV, liquidation threshold, liquidation bonus)
- Interest rate model parameters (optimal utilization, slopes, base rate)
- Reserve factor
- eMode information (category, underlying, eMode LTV/thresholds)

**Required Metadata** (for rate impact analysis):

- ✅ Reserve symbol, underlying asset address, aToken/debtToken addresses
- ⚠️ eMode category, eMode underlying, eMode LTV/thresholds (available from subgraph)
- ⚠️ Risk parameters (LTV, liquidation threshold, liquidation bonus) (available from subgraph)
- ⚠️ Interest rate model parameters (optimal utilization, slopes, base rate, reserve factor) (available from subgraph)

**Note**: Utilization, total supply, and total borrows are **market data** (time-series), not instrument definitions. They should come from market data service.

#### Morpho

**Status**: ✅ **Implemented** (`MorphoAdapter`)

**Data Sources**:

- Morpho API: `https://api.morpho.org/graphql`
- Morpho Subgraph (fallback)

**Instrument Format**:

- **Supply tokens**: `MORPHO-ETHEREUM:SUPPLY_TOKEN:SUPPLYUSDC@ETHEREUM`
- **Debt tokens**: `MORPHO-ETHEREUM:DEBT_TOKEN:DEBTUSDC@ETHEREUM`

### Staking Protocols

#### EtherFi

**Status**: ✅ **Implemented** (`EtherFiAdapter`)

**Data Sources**:

- Alchemy SDK: Token metadata
- On-chain calls: Contract addresses and exchange rates

**Instrument Format**:

- **EtherFi**: `ETHERFI:LST:WEETH@ETHEREUM`

**Contract Address**: `0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee`

#### Lido

**Status**: ✅ **Implemented** (`LidoAdapter`)

**Data Sources**:

- Alchemy SDK: Token metadata
- On-chain calls: Contract addresses and exchange rates

**Instrument Format**:

- **Lido**: `LIDO:LST:STETH@ETHEREUM`, `LIDO:LST:WSTETH@ETHEREUM`

**Contract Addresses**:

- stETH: `0xae7ab96520de3a18e5e111b5eaab095312d7fe84`
- wstETH: `0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0`

### Pending Implementation

#### Euler (Plasma)

**Status**: ⏳ **Pending**

**Required Instruments**:

- `EULER-PLASMA:SUPPLY_TOKEN:eUSDC@PLASMA`
- `EULER-PLASMA:DEBT_TOKEN:dUSDC@PLASMA`

#### Fluid (Plasma)

**Status**: ⏳ **Pending**

**Required Instruments**:

- `FLUID-PLASMA:SUPPLY_TOKEN:fUSDC@PLASMA`
- `FLUID-PLASMA:DEBT_TOKEN:dUSDC@PLASMA`

#### Hyperliquid

**Status**: ⏳ **Pending**

**Required Instruments**:

- `HYPERLIQUID:PERP:BTCUSDT@HYPERLIQUID`

**Data Source**: `https://api.hyperliquid.xyz/info` (REST API)

#### Aster

**Status**: ⏳ **Pending**

**Required Instruments**:

- `ASTER:PERP:BTCUSDT@ASTER`

**Data Source**: `https://fapi.asterdex.com` (REST API, Binance-style)

## Data Sources

### The Graph Protocol

**What**: Decentralized indexing protocol for blockchain data
**API**: GraphQL
**Authentication**: API key required (free tier available)
**Documentation**: https://thegraph.com/docs/

**Subgraphs Required**:

- **Uniswap V3 Ethereum**: `https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3`
- **Uniswap V3 Arbitrum**: `https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-arbitrum`
- **Uniswap V3 Base**: `https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3-base`
- **Curve Ethereum**: `https://api.thegraph.com/subgraphs/name/curvefi/curve-ethereum`
- **Balancer V2 Ethereum**: `https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2`
- **AAVE V3 Ethereum**: `https://api.thegraph.com/subgraphs/name/aave/aave-v3-ethereum`

**API Endpoint Format**:

```
https://gateway.thegraph.com/api/{api-key}/subgraphs/id/{subgraph-id}
```

**GraphQL Query Example** (Uniswap V3):

```graphql
query GetPools($first: Int!, $skip: Int!) {
  pools(
    first: $first
    skip: $skip
    orderBy: totalValueLockedUSD
    orderDirection: desc
  ) {
    id # Pool address
    token0 {
      id # Token address
      symbol
      decimals
      name
    }
    token1 {
      id # Token address
      symbol
      decimals
      name
    }
    feeTier # Fee tier (e.g., 500 = 0.05%, 3000 = 0.3%)
    liquidity
    totalValueLockedUSD
    volumeUSD
  }
}
```

**API Key Setup**:

- Secret Name: `thegraph-api-key`
- Get API key from: https://thegraph.com/studio/
- Free tier: 100,000 queries/month
- Paid tier: **$2 per 100k queries** (billed in GRT on Arbitrum)
- Billing portal: https://thegraph.com/studio/billing/

### Alchemy SDK

**What**: Comprehensive Web3 development platform
**API**: REST API + SDK
**Authentication**: API key required
**Documentation**: https://docs.alchemy.com/

**Key Methods**:

- `getTokenMetadata(contract_address)` - Get token info by address
- `getTokensForOwner(wallet_address)` - Get tokens with balances
- `getTokenBalances(address, tokens)` - Get token balances

**API Key Setup**:

- Secret Name: `alchemy-api-key`
- Get API key from: https://dashboard.alchemy.com/
- Free tier: 300M compute units/month

**Usage**: Token metadata, contract address resolution, wallet position discovery

### Envio

**What**: Alternative blockchain indexer (HyperIndex/HyperSync)
**API**: GraphQL
**Authentication**: API token required
**Documentation**: https://docs.envio.dev/

**Key Concepts**:

- **HyperIndex**: The indexing framework (similar to The Graph subgraphs)
- **HyperSync**: The API service for accessing deployed indexers

**API Tokens**:

- Required from November 3, 2025
- Get Token: https://envio.dev/app/api-tokens
- Store: In GCP Secret Manager as `envio-api-key`

**Usage**: Uniswap V4 pool enumeration (fallback when The Graph unavailable)

**Local Development**:

- Clone: `https://github.com/enviodev/uniswap-v4-indexer.git`
- Run: `pnpm envio dev`
- GraphQL endpoint: `http://localhost:8080/v1/graphql`

**GraphQL Query Format** (Envio):

```graphql
query TopPools {
  Pool(
    order_by: { totalValueLockedUSD: desc }
    limit: 10
    where: { chainId: { _eq: "1" } }
  ) {
    id
    name
    token0
    token1
    totalValueLockedUSD
    volumeUSD
    feesUSD
  }
}
```

### Protocol SDKs

#### AAVE SDK

**Library**: `@aave/aave-sdk` (TypeScript/JavaScript)
**Documentation**: https://github.com/aave/aave-sdk

**Usage**: Market data, borrow/supply rates, user positions

**Alternative**: AAVE Subgraph (The Graph) - preferred for instrument definitions

#### Lido SDK

**Library**: `@lidofinance/lido-ethereum-sdk` (TypeScript/JavaScript)
**Documentation**: https://github.com/lidofinance/lido-ethereum-sdk

**Usage**: Stake/unstake operations, share rates, validator info

**Alternative**: Direct contract calls via Alchemy RPC - preferred for instrument definitions

## Instrument Discovery Process

### What We Fetch (Metadata Only)

**From The Graph Subgraphs**:

- Pool metadata (`id`, `token0`, `token1`, `feeTier`)
- Current liquidity (for filtering)
- TVL (`totalValueLockedUSD` for filtering by minimum liquidity)
- Creation timestamp (for availability dates)

**What We DON'T Fetch**:

- ❌ Transactions
- ❌ Swaps
- ❌ Trades
- ❌ Price data
- ❌ Historical prices
- ❌ Volume data (except for filtering)
- ❌ Order book data

### Example: Uniswap V3 Query

```graphql
{
  pools(
    first: 100
    where: { totalValueLockedUSD_gte: "10000" }
    orderBy: totalValueLockedUSD
    orderDirection: desc
  ) {
    id # Pool address (for routing)
    token0 {
      id # Token contract address
      symbol # Token symbol (ETH, USDT, etc.)
      decimals # Token decimals
    }
    token1 {
      id
      symbol
      decimals
    }
    feeTier # Fee tier (500, 3000, 10000)
    liquidity # Current liquidity
    totalValueLockedUSD # TVL for filtering
    createdAtTimestamp # Pool creation time
  }
}
```

### Output: Instrument Definitions

Each pool becomes an instrument definition:

```python
{
    'instrument_key': 'UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM',
    'venue': 'UNISWAPV3-ETH',
    'instrument_type': 'POOL',
    'symbol': 'ETH-USDT:3000',
    'base_asset': 'ETH',
    'quote_asset': 'USDT',
    'pool_address': '0x...',  # For routing
    'pool_fee_tier': 3000,    # For order handler fees
    'base_asset_contract_address': '0x...',
    'quote_asset_contract_address': '0x...',
    # ... other metadata fields
}
```

## Metadata Clarifications

### TVL Filtering

We filter by **TVL (Total Value Locked)**, NOT transaction volume:

- **TVL** = Total Value Locked (current liquidity in the pool)
- **NOT** transaction volume
- **Snapshot value** from The Graph subgraph
- Represents the **current** amount of capital locked in the pool

**Filtering Purpose**:

- Filter out pools with low liquidity (e.g., < $10k TVL)
- Prioritize pools with sufficient liquidity for trading
- Avoid illiquid pools that would have high slippage

### Data Types Field

The `data_types` field indicates **what market data types are available for download** from the market data service, NOT what the instrument type is.

**For Uniswap V3 Pools**:

- ✅ `trades` - Swap transactions (correct)
- ❌ `book_snapshot` - NOT available (Uniswap V3 uses concentrated liquidity, not order books)

**For Other Venues**:

- **CEX (Binance, etc.)**: `trades,book_snapshot_5` ✅
- **Databento**: `ohlcv-1m` ✅ (1-minute candles)
- **Deribit**: `options_chain` ✅ (options-specific)

### Exchange Raw Symbol

For DeFi protocols, `exchange_raw_symbol` contains the **native exchange identifier**:

- **Uniswap V3**: Pool contract address (e.g., `0x1234...`)
- **Curve**: Pool contract address (e.g., `0x5678...`)
- **Balancer**: Pool contract address (e.g., `0x9abc...`)
- **AAVE**: aToken/debtToken contract address (e.g., `0xdef0...`)
- **Lido/EtherFi**: LST token contract address (e.g., `0xfedc...`)

This is the identifier used directly by the protocol for execution, not a human-readable symbol.

## API Key Setup

### Required Secrets

Add these secrets to GCP Secret Manager:

```bash
# The Graph API key
gcloud secrets create thegraph-api-key --data-file=-

# Alchemy API key
gcloud secrets create alchemy-api-key --data-file=-

# Envio API key
gcloud secrets create envio-api-key --data-file=-

# AaveScan API key (optional, for AAVE V3 adapter fallback)
gcloud secrets create aavescan-api-key --data-file=-
```

### Environment Variables

Configure secret names in `.env`:

```bash
# Secret Manager secret names (keys stored in GCP Secret Manager)
THEGRAPH_SECRET_NAME=thegraph-api-key
ALCHEMY_SECRET_NAME=alchemy-api-key
ENVIO_SECRET_NAME=envio-api-key
AAVESCAN_SECRET_NAME=aavescan-api-key
```

**Never commit actual API keys to `.env` files!**

### API Key Status

| API Key       | Status                  | Purpose                            | Secret Name        |
| ------------- | ----------------------- | ---------------------------------- | ------------------ |
| **Alchemy**   | ✅ Available            | Token metadata, contract addresses | `alchemy-api-key`  |
| **The Graph** | ✅ Available            | DEX pool enumeration               | `thegraph-api-key` |
| **Envio**     | ✅ Available            | Uniswap V4 fallback                | `envio-api-key`    |
| **AaveScan**  | ✅ Available (optional) | AAVE fallback                      | `aavescan-api-key` |

**Note**: AAVE, EtherFi, and Lido use The Graph and Alchemy - no separate API keys needed.

## Testing Approaches

### 1. Mainnet Forking (Most Realistic) ⭐ Recommended

**Tools**: Tenderly, Hardhat, Foundry, Anvil

**How it works**:

- Fork Ethereum mainnet at a specific block number
- Get exact state (prices, liquidity, positions) from that block
- Execute transactions against the forked state
- No real money spent, but uses real contract code and state

**Pros**:

- ✅ Most realistic - uses actual contract code and state
- ✅ Can test against historical or current mainnet state
- ✅ Supports complex interactions (flash loans, multi-hop swaps)
- ✅ Can simulate MEV, slippage, and gas costs accurately

**Cons**:

- ⚠️ Requires RPC access (Alchemy, Tenderly, or local node)
- ⚠️ Can be slow for large state snapshots

**Example with Tenderly**:

```python
from web3 import Web3
from eth_account import Account

# Fork mainnet at specific block
tenderly_rpc = "https://rpc.tenderly.co/fork/YOUR_FORK_ID"
w3 = Web3(Web3.HTTPProvider(tenderly_rpc))

# Use your private key (safe - it's a fork)
account = Account.from_key("YOUR_PRIVATE_KEY")
w3.eth.default_account = account.address

# Execute swap on Uniswap V3 (uses real pool state)
uniswap_router = w3.eth.contract(address=UNISWAP_ROUTER, abi=ROUTER_ABI)
tx = uniswap_router.functions.exactInputSingle({
    'tokenIn': WETH_ADDRESS,
    'tokenOut': USDC_ADDRESS,
    'fee': 3000,
    'recipient': account.address,
    'deadline': int(time.time()) + 1800,
    'amountIn': Web3.toWei(1, 'ether'),
    'amountOutMinimum': 0,
    'sqrtPriceLimitX96': 0
}).buildTransaction({'from': account.address, 'gas': 300000})

# Sign and send (no real money!)
signed_tx = w3.eth.account.sign_transaction(tx, account.key)
tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
```

### 2. Backtesting with Historical Data

**How it works**:

- Use historical swap data from The Graph or DEX subgraphs
- Simulate trades using historical prices and liquidity
- Apply slippage models based on historical patterns
- Calculate PnL and positions over time

**Pros**:

- ✅ Fast - no blockchain interaction needed
- ✅ Can test long time periods efficiently
- ✅ Good for strategy development and optimization

**Cons**:

- ⚠️ May not capture all edge cases (reentrancy, MEV, etc.)
- ⚠️ Slippage models are approximations
- ⚠️ Doesn't test actual contract interactions

### 3. Testnets (Limited Usefulness)

**When to use**:

- Initial contract deployment testing
- Basic functionality verification
- Not recommended for strategy backtesting

**Cons**:

- ⚠️ Limited liquidity - hard to test realistic scenarios
- ⚠️ Testnet tokens don't reflect real market conditions
- ⚠️ Can be unreliable (testnets reset, faucets run dry)

## Coverage Analysis

### What We Have

- ✅ Basic instrument identifiers: Contract addresses, symbols, token metadata
- ✅ DEX pool metadata: Uniswap V2/V3/V4, Curve, Balancer (addresses, fee tiers, TVL)
- ✅ LST metadata: Lido, EtherFi (contract addresses, underlying assets)
- ✅ AAVE basic metadata: Reserve symbols, aToken/debtToken addresses, underlying addresses
- ✅ Morpho metadata: Market addresses, underlying assets

### What We're Missing (for Rate Impact Analysis)

- ⚠️ AAVE eMode information: Which assets are in eMode, which underlying they're attached to
- ⚠️ AAVE risk parameters: LTV, liquidation thresholds, liquidation bonus (available from subgraph)
- ⚠️ AAVE interest rate model parameters: Optimal utilization, slopes, base rate, reserve factor (available from subgraph)

**Note**: These are available from the AAVE subgraph but not currently fetched. They should be added to the AAVE adapter.

### What We DON'T Fetch (Market Data)

These should come from market data services, not instruments-service:

- ❌ Supply/borrow rates (APY)
- ❌ Oracle prices (weETH/ETH ratios)
- ❌ DEX pool OHLCV prices
- ❌ Staking yields
- ❌ Gas costs
- ❌ Execution costs

**This Is Intentional**: Instruments-service is for metadata, not market data.

## Data Availability Summary

### Currently Fetching

| Protocol       | What We Fetch                                      |
| -------------- | -------------------------------------------------- |
| **Uniswap V3** | Pool metadata, TVL, fee tiers, creation timestamps |
| **Curve**      | Pool metadata, TVL                                 |
| **Balancer**   | Pool metadata, liquidity                           |
| **Lido**       | Token metadata, contract addresses                 |
| **EtherFi**    | Token metadata, contract addresses                 |
| **AAVE**       | Reserve metadata, aToken/debtToken addresses       |

### Available but Missing (for Future Market Data Service)

| Protocol       | Available Data                                                 |
| -------------- | -------------------------------------------------------------- |
| **Uniswap V3** | Volume data, historical OHLC, swap transactions, current price |
| **Curve**      | Volume data, fees, APR, virtual price                          |
| **Balancer**   | Volume data, fees, token weights, swap count                   |
| **Lido**       | Stake/unstake rates, validator info, TVL history               |
| **EtherFi**    | Stake/unstake rates, validator info, TVL history               |
| **AAVE**       | Supply/borrow rates, liquidity, historical rates               |

**Recommendation**: Keep instrument definitions focused on metadata. Create separate market data service for rates, volume, and historical data.

## Implementation Status

### ✅ Complete

- **Uniswap V2/V3/V4**: All versions implemented
- **Curve**: RPC fallback implemented (subgraph deprecated)
- **Balancer**: Using Balancer API v3
- **AAVE V3**: The Graph + AaveScan fallback
- **Morpho**: Morpho API integration
- **EtherFi**: Alchemy integration
- **Lido**: Alchemy integration

### ⏳ Pending Implementation

- **Euler**: Plasma chain lending
- **Fluid**: Plasma chain lending
- **Hyperliquid**: Perpetual futures DEX
- **Aster**: Perpetual futures exchange

## Troubleshooting

### Secret Manager Errors

If you see "Failed to retrieve API key from Secret Manager":

1. Check secret exists: `gcloud secrets list`
2. Verify secret name matches `.env` config
3. Check GCP credentials: `GOOGLE_APPLICATION_CREDENTIALS`
4. Verify project ID: `GCP_PROJECT_ID`

### API Rate Limits

- **The Graph**: Free tier has rate limits (100,000 queries/month), consider upgrading
- **Alchemy**: Free tier (300M compute units/month), then paid
- **Envio**: Development plan is FREE (30-day limit), then paid

### Import Errors

If adapters fail to import:

1. Install dependencies: `pip install gql requests`
2. Check Python path includes `instruments_service`
3. Verify `unified-trading-services` is installed

## Related Documentation

- [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter pattern and implementation details
- [`INSTRUMENT_SPECIFICATION.md`](./INSTRUMENT_SPECIFICATION.md) - Complete instrument ID specification
- [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture and design decisions
