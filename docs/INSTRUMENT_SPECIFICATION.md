# Instrument Specification

> **Related Documentation**:
>
> - [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture
> - [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter pattern
> - [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples

---

## Purpose

This document provides the complete specification for canonical instrument IDs used throughout the instruments-service. Instrument IDs are stable, canonical identifiers that enable consistent instrument identification across all services in the trading system.

**Note**: "Instrument ID" and "instrument key" are used interchangeably. Both refer to the canonical instrument identifier format: `VENUE:INSTRUMENT_TYPE:PAYLOAD[@CHAIN]`.

## Design Principles

- **Single global canonical**: Instrument IDs are stable across all services
- **Routing-agnostic strategy layer**: Positions keyed by canonical instrument IDs; strategy never bakes in venue routing for spot
- **Execution-dynamic routing**: Execution service selects venues/pools at runtime based on expected slippage, fees, gas, and venue health
- **Perps are venue-bound**: Perpetuals are non-fungible across venues; position instrument ID = execution instrument ID for CEX perps
- **Deterministic normalization**: Assets and venues normalized (aliases → canonical)

## Canonical Format

**Grammar (BNF-style)**:

```
<instrument-id> ::= [<asset-class> ":"] <venue> ":" <type> ":" <payload> ["@" <chain>]

<asset-class>  ::= CEFI | DEFI | COMMODITIES | EQUITY-INDEX | EQUITY | BOND | FX
                  # Optional prefix to categorize instrument by asset class
                  # If omitted, defaults based on venue/type (backward compatible)

<venue>        ::= UPPER_ALNUM_DASH
<type>         ::= SPOT_ASSET | SPOT_PAIR | PERPETUAL | FUTURE | OPTION | POOL | LST | A_TOKEN | DEBT_TOKEN | EQUITY | INDEX

# SPOT_ASSET payloads (actual positions held)
<payload-SPOT_ASSET> ::= <asset>

# SPOT_PAIR payloads (trading routes)
<payload-SPOT_PAIR> ::= <base> "-" <quote>

# PERPETUAL payloads (linear/inverse encoded in suffix)
<payload-PERP> ::= <base> "-" <quote> ["@" <perp-flavour>]
<perp-flavour> ::= LIN | INV | QUANTO

# FUTURE payloads
<payload-FUT>  ::= <base> "-" <quote> "-" <yyyymmdd> ["@" <perp-flavour>]

# OPTION payloads
<payload-OPT>  ::= <base> "-" <quote> "-" <yyyymmdd> "-" <strike> "-" <cp> ["@" <perp-flavour>]
<cp>           ::= CALL | PUT

# POOL payloads (AMM pools)
<payload-POOL> ::= <tokenA> "-" <tokenB> [":" <fee-bps>] ["@" <chain>]

<base> | <quote> | <tokenA> | <tokenB> ::= CANONICAL_ASSET_CODE
<yyyymmdd>     ::= 6 DIGITS (YYMMDD)
<strike>       ::= DECIMAL
```

## Instrument Types

### SPOT_ASSET

**Purpose**: Represents actual asset positions held on a specific venue

**Format**: `VENUE:SPOT_ASSET:ASSET`

**Examples**:

- `CEFI:BINANCE-SPOT:SPOT_ASSET:BTC` (actual BTC position on Binance, with asset class prefix)
- `BINANCE-SPOT:SPOT_ASSET:BTC` (backward compatible, no prefix)
- `WALLET:SPOT_ASSET:ETH` (ETH held in wallet)
- `WALLET:SPOT_ASSET:USDT` (USDT held in wallet)

**Key Principle**: SPOT_ASSET represents actual holdings, not trading routes.

### SPOT_PAIR

**Purpose**: Represents trading routes for execution routing

**Format**: `VENUE:SPOT_PAIR:BASE-QUOTE`

**Examples**:

- `CEFI:BINANCE-SPOT:SPOT_PAIR:BTC-USDT` (trading route, never stored as position, with asset class prefix)
- `BINANCE-SPOT:SPOT_PAIR:BTC-USDT` (backward compatible, no prefix)
- `DEFI:UNISWAPV3-ETH:SPOT_PAIR:USDC-ETH@ETHEREUM` (DEX trading route, with asset class prefix)

**Key Principle**: SPOT_PAIR is routing-only, never stored as a position. Trades result in SPOT_ASSET deltas.

**Trading Flow**:

1. Execute trade: Use `SPOT_PAIR` to find best route (e.g., `BINANCE-SPOT:SPOT_PAIR:BTC-USDT`)
2. Update positions: After trade, track `SPOT_ASSET` positions (e.g., `BINANCE-SPOT:SPOT_ASSET:BTC`, `BINANCE-SPOT:SPOT_ASSET:USDT`)

### PERPETUAL

**Purpose**: Perpetual futures contracts (no expiry)

**Format**: `VENUE:PERPETUAL:BASE-QUOTE[@LIN|@INV]`

**Linear (`@LIN`)**: Quote asset == margin currency (settle_asset)

- Example: `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN` (USDT margin)

**Inverse (`@INV`)**: Margin currency == base asset

- Example: `DERIBIT:PERPETUAL:BTC-USD@INV` (BTC margin)

**Examples**:

- `BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN` (linear - USDT margin)
- `DERIBIT:PERPETUAL:BTC-USD@INV` (inverse - BTC margin)
- `HYPERLIQUID:PERPETUAL:BTC-USDC@LIN@HYPERLIQUID` (linear - USDC margin, Hyperliquid chain)

### FUTURE

**Purpose**: Dated futures contracts (with expiry)

**Format**: `VENUE:FUTURE:BASE-QUOTE-YYMMDD[@LIN|@INV]`

**Required Attributes**: `expiry` (datetime), `contract_size` (float)

**Examples**:

- `DERIBIT:FUTURE:BTC-USD-241225@INV` (inverse - BTC margin, expires Dec 25, 2024)
- `BINANCE-FUTURES:FUTURE:BTC-USDT-241225@LIN` (linear - USDT margin)
- `CME:FUTURE:ES-USD-241225` (TradFi futures)

### OPTION

**Purpose**: Options contracts (with expiry and strike)

**Format**: `VENUE:OPTION:BASE-QUOTE-YYMMDD-STRIKE-CALL|PUT[@LIN|@INV]`

**Required Attributes**: `expiry` (datetime), `strike` (string), `option_type` ("CALL" or "PUT")

**Examples**:

- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@INV` (inverse - BTC margin)
- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@LIN` (linear - USD margin)
- `CME:OPTION:ES-USD-241225-4500-CALL` (TradFi options)

### POOL

**Purpose**: DeFi DEX liquidity pools

**Format**: `VENUE:POOL:BASE-QUOTE[:FEE_TIER][@CHAIN]`

**Fee Tier**: In basis points (100 = 0.01%, 500 = 0.05%, 3000 = 0.3%, 10000 = 1%)

**Examples**:

- `UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM` (Uniswap V3 pool, 0.3% fee)
- `UNISWAPV3-ETH:POOL:ETH-USDT:500@ETHEREUM` (Uniswap V3 pool, 0.05% fee)
- `CURVE-ETH:POOL:ETH-USDT@ETHEREUM` (Curve pool)

**Note**: Fee tier is specified after colon (`:`) in symbol, chain is specified after `@` symbol.

### LST

**Purpose**: Liquid Staking Tokens

**Format**: `VENUE:LST:ASSET[@CHAIN]`

**Examples**:

- `ETHERFI:LST:WEETH@ETHEREUM` (EtherFi wrapped eETH)
- `LIDO:LST:STETH@ETHEREUM` (Lido staked ETH)
- `LIDO:LST:WSTETH@ETHEREUM` (Lido wrapped stETH)

### A_TOKEN

**Purpose**: AAVE lending positions (supply tokens)

**Format**: `VENUE:A_TOKEN:TOKEN[@CHAIN]`

**Examples**:

- `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM` (AAVE USDT supply position)
- `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM` (AAVE WETH supply position)

### DEBT_TOKEN

**Purpose**: AAVE borrowing positions (debt tokens)

**Format**: `VENUE:DEBT_TOKEN:TOKEN[@CHAIN]`

**Examples**:

- `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM` (AAVE WETH borrow position)

### EQUITY

**Purpose**: Traditional finance equities

**Format**: `VENUE:EQUITY:SYMBOL`

**Examples**:

- `NASDAQ:EQUITY:AAPL` (Apple stock)
- `NYSE:EQUITY:SPY` (S&P 500 ETF)

### INDEX

**Purpose**: Index instruments

**Format**: `VENUE:INDEX:SYMBOL`

**Examples**:

- `CME:INDEX:ES` (S&P 500 index)

## Chain Attribute

All instrument definitions include a `chain` attribute that clarifies which blockchain (if any) the instrument operates on:

- **CeFi (Centralized Finance)**: `chain="off-chain"` - Traditional exchanges like Binance, Bybit, OKX, Deribit
- **TradFi (Traditional Finance)**: `chain="off-chain"` - Traditional exchanges like CME, NASDAQ, NYSE
- **DeFi (Decentralized Finance)**: Chain name indicating the blockchain:
  - `chain="ETHEREUM"` - Ethereum mainnet (most DeFi protocols)
  - `chain="ARBITRUM"` - Arbitrum L2
  - `chain="BASE"` - Base L2
  - `chain="POLKADOT"` - Polkadot chain (Aster exchange)
  - `chain="HYPERLIQUID"` - Hyperliquid chain
  - `chain="PLASMA"` - Plasma chain (Plasma lending protocols)

### Chain Suffix in Instrument Keys

For DeFi instruments on specific chains, the chain is included in the instrument key as a suffix:

- **Ethereum-based**: `UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM`
- **Hyperliquid**: `HYPERLIQUID:PERPETUAL:BTC-USDC@LIN@HYPERLIQUID`
- **Aster (Polkadot)**: `ASTER:PERPETUAL:BTC-USDT@LIN@POLKADOT`
- **Plasma**: `EULER-PLASMA:A_TOKEN:AUSDT@PLASMA`

**Note**: The `@CHAIN` suffix is optional for Ethereum-based protocols (can be inferred from venue), but **required** for exchanges on different chains (Hyperliquid, Aster) to avoid ambiguity.

## Symbol Formats Summary

| Instrument Type | Symbol Format                                  | Example                         |
| --------------- | ---------------------------------------------- | ------------------------------- |
| **SPOT_ASSET**  | `ASSET`                                        | `BTC`, `ETH`, `USDT`, `WEETH`   |
| **SPOT_PAIR**   | `BASE-QUOTE`                                   | `BTC-USDT`, `ETH-USDT`          |
| **PERPETUAL**   | `BASE-QUOTE@LIN\|@INV`                         | `ETH-USDT@LIN`, `BTC-USD@INV`   |
| **FUTURE**      | `BASE-QUOTE-YYMMDD@LIN\|@INV`                  | `BTC-USD-241225@LIN`            |
| **OPTION**      | `BASE-QUOTE-YYMMDD-STRIKE-CALL\|PUT@LIN\|@INV` | `BTC-USD-241225-50000-CALL@LIN` |
| **POOL**        | `BASE-QUOTE[:FEE_TIER]`                        | `ETH-USDT:3000`, `BTC-USDC:500` |
| **LST**         | `ASSET`                                        | `WEETH`, `STETH`, `WSTETH`      |
| **A_TOKEN**     | `TOKEN`                                        | `AUSDT`, `AWETH`                |
| **DEBT_TOKEN**  | `TOKEN`                                        | `DEBTWETH`                      |

## Venue Names

Venue names follow the pattern `VENUE` or `VENUE-CHAIN`:

**CEX Venues**:

- `BINANCE-SPOT`, `BINANCE-FUTURES`
- `BYBIT`, `OKX`, `DERIBIT`

**TradFi Venues**:

- `CME`, `NASDAQ`, `NYSE`, `ICE`

**DeFi Venues**:

- `UNISWAPV2-ETH`, `UNISWAPV3-ETH`, `UNISWAPV4-ETH`
- `CURVE-ETH`, `BALANCER-ETH`
- `AAVE_V3_ETH`, `ETHERFI`, `LIDO`
- `MORPHO-ETHEREUM`

**Special Venues**:

- `WALLET` - For on-chain wallet positions

## Attributes Schema

Instrument definitions include extended attributes:

### Standard Attributes

- `underlying`: Optional[str] - Canonical reference (e.g., `BTC-USDT`) or another instrument_key
- `base_asset`: Optional[str] - Base asset code
- `quote_asset`: Optional[str] - Quote asset code
- `settle_asset`: Optional[str] - Settlement asset
- `chain`: str - Chain identifier: `"off-chain"` for CeFi/TradFi, chain name for DeFi
- `expiry`: Optional[datetime] - Precise UTC datetime
- `strike`: Optional[str] - Strike price (as string to support formats like "5K")
- `option_type`: Optional["CALL","PUT"] - Option type (uppercase)
- `contract_size`: Optional[float] - Contract size
- `tick_size`: Optional[float] - Tick size
- `min_size`: Optional[float] - Minimum order size
- `asset_class`: ["crypto","traditional"] - Asset class
- `venue_type`: ["exchange","protocol","wallet"] - Venue type
- `exchange_raw_symbol`: Optional[str] - Raw exchange symbol (native identifier)
- `ccxt_symbol`: Optional[str] - CCXT symbol
- `ccxt_exchange`: Optional[str] - CCXT exchange name
- `inverse`: Optional[bool] - Whether instrument is inverse
- `tardis_symbol`: Optional[str] - Tardis API symbol
- `tardis_exchange`: Optional[str] - Tardis API exchange name
- `data_provider`: Optional["tardis","databento"] - Data provider
- `data_types`: Optional[list[str]] - Available data types (e.g., `["trades","book_snapshot_5","liquidations"]`)

### DeFi-Specific Attributes

For DeFi instruments, additional attributes provide contract addresses and pool information:

- `base_asset_contract_address`: Optional[str] - ERC-20 contract address for base asset
- `quote_asset_contract_address`: Optional[str] - ERC-20 contract address for quote asset
- `pool_address`: Optional[str] - Pool contract address (for DEX pairs)
- `pool_fee_tier`: Optional[int] - Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%)
- `pool_version`: Optional[str] - Pool version (e.g., "v3", "v4")
- `factory_address`: Optional[str] - Factory contract address for pool computation
- `chain_id`: Optional[int] - Chain ID (1 = Ethereum mainnet, 137 = Polygon, etc.)
- `token_decimals`: Optional[Dict[str, int]] - Token decimals (e.g., `{"base": 18, "quote": 6}`)
- `pool_type`: Optional[str] - Pool type (e.g., "uniswap_v3", "curve_stable")

## Validation Rules

- `VENUE` must be in Venue enum
- `INSTRUMENT_TYPE` must be in InstrumentType enum
- **SPOT_ASSET**: Symbol is asset code (BTC, ETH, USDT, ...) - represents actual holdings
- **SPOT_PAIR**: Symbol is `BASE-QUOTE`; QUOTE ∈ {USD, USDT, USDC, ...}; routing only, never stored as position
- **PERPETUAL**: Symbol is `BASE-QUOTE@LIN` or `BASE-QUOTE@INV`; QUOTE ∈ {USD, USDT, USDC, ...}
- **FUTURE**: Symbol is `BASE-QUOTE-YYMMDD@LIN` or `BASE-QUOTE-YYMMDD@INV`; `attrs.expiry` required; `attrs.contract_size` required
- **OPTION**: Symbol is `BASE-QUOTE-YYMMDD-STRIKE-CALL|PUT@LIN` or `@INV`; `attrs.expiry`, `attrs.strike`, `attrs.option_type` required

## Raw Exchange Type → Canonical Type Mapping

Exchange APIs return raw types (lowercase) which are mapped to canonical types (UPPERCASE):

```
Exchange API 'spot' → Canonical 'SPOT_PAIR'
Exchange API 'perpetual' → Canonical 'PERPETUAL'
Exchange API 'future' → Canonical 'FUTURE'
Exchange API 'option' → Canonical 'OPTION'
```

**Note**: Raw `symbol_type` is not stored to avoid confusion with canonical `instrument_type`.

## Worked Examples

### Spot Trading Workflow

1. **Execute trade**: `BINANCE-SPOT:SPOT_PAIR:BTC-USDT` (find best route)
2. **Update positions**: `BINANCE-SPOT:SPOT_ASSET:BTC`, `BINANCE-SPOT:SPOT_ASSET:USDT`

### Perpetuals

- `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN` (linear - USDT margin, chain="off-chain")
- `DERIBIT:PERPETUAL:BTC-USD@INV` (inverse - BTC margin, chain="off-chain")
- `HYPERLIQUID:PERPETUAL:BTC-USDC@LIN@HYPERLIQUID` (linear - USDC margin, chain="HYPERLIQUID")

### Crypto Futures

- `DERIBIT:FUTURE:BTC-USD-241225@INV` (inverse - BTC margin) with `attrs.expiry="2024-12-25T08:00:00Z"`
- `BINANCE-FUTURES:FUTURE:BTC-USDT-241225@LIN` (linear - USDT margin)

### TradFi Futures

- `CME:FUTURE:ES-USD-241225` with `attrs.exchange_raw_symbol="ESZ4"`, expiry normalized

### Options

- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@INV` (inverse - BTC margin)
- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@LIN` (linear - USD margin)
- `CME:OPTION:ES-USD-241225-4500-CALL`

### DeFi Lending/Staking

- `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM` (AAVE lending position)
- `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM` (AAVE borrowing position)
- `ETHERFI:LST:WEETH@ETHEREUM` (EtherFi staking position)

### DeFi DEX Pools

- `UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM` (Uniswap V3 pool, 0.3% fee)
- `UNISWAPV3-ETH:POOL:ETH-USDT:500@ETHEREUM` (Uniswap V3 pool, 0.05% fee)
- `CURVE-ETH:POOL:ETH-USDT@ETHEREUM` (Curve pool)

## Expiry Handling

- `attrs.expiry` holds precise UTC datetime (including half-hour if applicable)
- Daily snapshots store `attrs.expiry`
- Instruments automatically marked `active = false` at expiry

## Identity Tiers

- **instrument_id** (also called "instrument key"): Canonical routing/position identity
- **exchange_raw_symbol**: Provider-native identifier for execution

**Note**: The term "instrument ID" is preferred in documentation, while "instrument_key" is used in code field names for consistency with existing implementations.

## DeFi Instrument Enrichment

### Problem Statement

DeFi instruments need contract addresses and pool information for execution, but:

- Contract addresses are long and not user-intuitive
- Same trading pair can exist on multiple pools (e.g., ETH-USDT on Uniswap V3 with fees 500, 3000, 10000)
- Different pool versions coexist (Uniswap V2 and V3 both have ETH-USDT pools)
- Pool addresses change per fee tier and version

### Solution: Version-Aware Instrument IDs

**Differentiate pool versions and fee tiers in instrument ID, store contract addresses in attributes.**

**Recommended Pattern**: Version + Fee in Venue Name

```
UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM  # Uniswap V3, 0.3% fee
UNISWAPV3-ETH:POOL:ETH-USDT:500@ETHEREUM   # Uniswap V3, 0.05% fee
UNISWAPV2-ETH:POOL:ETH-USDT@ETHEREUM       # Uniswap V2 (0.3% fee implied)
```

**Execution Attributes** (stored in instrument definition):

```python
{
    "instrument_key": "UNISWAPV3-ETH:POOL:ETH-USDT:3000@ETHEREUM",
    "base_asset_contract_address": "0xC02aaA39b223FE8D0A0e5c4F27eAD9083c756Cc2",  # WETH
    "quote_asset_contract_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
    "pool_address": "0x...",  # Computed from factory + tokens + fee
    "pool_fee_tier": 3000,  # 0.3%
    "pool_version": "v3",
    "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "chain_id": 1
}
```

**Benefits**:

- Explicit routing: Middleware knows exactly which pool to use
- No inference needed: Direct pool address from instrument ID lookup
- Position tracking: Each pool can be tracked separately
- Configuration clarity: Clear which version/fee tier is being used

## FAQ

**Q: Why is SPOT_PAIR routing-only?**
A: Trades result in SPOT_ASSET deltas; SPOT_PAIR simplifies execution routing without becoming a held position.

**Q: What's the difference between SPOT_ASSET and SPOT_PAIR?**
A: SPOT_ASSET represents actual spot positions held on a specific venue, SPOT_PAIR is used for routing and execution.

**Q: How do SPOT_PAIR and SPOT_ASSET work together?**
A: SPOT_PAIR is used for finding the best exchange to trade a pair, SPOT_ASSET tracks your actual asset holdings after the trade.

**Q: Are CME futures different from crypto futures?**
A: Same `instrument_type=FUTURE`; differences live in attributes (`contract_size`, codes, settlement, `asset_class`).

**Q: Why UPPERCASE?**
A: Consistent formatting makes instrument IDs more readable and easier to parse programmatically.

**Q: What's the difference between "instrument ID" and "instrument key"?**
A: They refer to the same thing - the canonical instrument identifier. "Instrument ID" is preferred in documentation, while "instrument_key" is used in code field names.

**Q: How are multiple pools for the same pair handled?**
A: Each pool gets its own instrument definition with version + fee tier in the venue name (e.g., `UNISWAPV3-ETH:POOL:ETH-USDT:3000`, `UNISWAPV3-ETH:POOL:ETH-USDT:500`).

**Q: When should instruments be enriched with contract addresses?**
A: Enrichment is done on-demand when needed for execution. Don't require enrichment at instrument definition time.

## Related Documentation

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Service architecture and design decisions
- [`VENUE_ADAPTERS.md`](./VENUE_ADAPTERS.md) - Venue adapter pattern and data sources
- [`DEFI_GUIDE.md`](./DEFI_GUIDE.md) - DeFi protocols and integration details
- [`USAGE_GUIDE.md`](./USAGE_GUIDE.md) - Usage examples and client patterns
