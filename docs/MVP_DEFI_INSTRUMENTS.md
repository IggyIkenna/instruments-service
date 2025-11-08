# MVP DeFi Strategy Canonical Instrument IDs

## Purpose
This document defines the complete universe of canonical instrument IDs for **DeFi-only** strategies in the unified trading system. These instrument IDs follow the canonical format specified in [INSTRUMENT_KEY.md](./INSTRUMENT_KEY.md) and are used for position tracking, routing, and execution across DeFi venues.

> **Related Documentation**:
> - [`INSTRUMENT_KEY.md`](./INSTRUMENT_KEY.md) - Service-specific implementation details
> - **Canonical Spec**: [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md) - Complete canonical instrument ID specification
> - **CEX MVP**: [`docs/MVP_INSTRUMENT_UNIVERSE.md`](../../docs/MVP_INSTRUMENT_UNIVERSE.md) - CEX MVP instruments (complementary scope)

**Scope**: DeFi venues only (AAVE, EtherFi, Lido, Morpho, Uniswap, Curve). CEX venues (Binance, Bybit, OKX) are excluded.

**Architectural Flow**:
1. **Define Instruments Canonically** (this document)
2. **Look for Data** (SCRIPTS_DATA_GUIDE.md - verify data availability)
3. **Strategy Subscriptions** (position subscriptions for position/risk/PnL monitoring)
4. **Execution Subscriptions** (trading routes for execution middleware)

## Canonical Format Reference
- **Format**: `VENUE:INSTRUMENT_TYPE:SYMBOL[@CHAIN]`
- **All components**: UPPERCASE for consistency
- **Symbol conventions**: Hyphens for pairs (e.g., `ETH-USDT` not `ETHUSDT`)
- **Instrument types**: SPOT_ASSET, SPOT_PAIR, LST, A_TOKEN, DEBT_TOKEN
- **Venue format**: DEX venues include chain suffix (e.g., `UNISWAPV3-ETH`, `CURVE-ETH`, `AAVE_V3_ETH`)
- **Chain suffix**: User-intuitive chain names (e.g., `@ETHEREUM`, `@ARBITRUM`, `@POLYGON`, `@BASE`)
- **Canonical Reference**: See [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md) for complete canonical specification
- **Service Implementation**: See [INSTRUMENT_KEY.md](./INSTRUMENT_KEY.md) for service-specific implementation details

## Key Distinctions

### Position Instruments vs Trading Instruments

**Position Instruments** (`SPOT_ASSET`, `LST`, `A_TOKEN`, `DEBT_TOKEN`):
- Represent actual holdings/positions after trade execution
- Used for position monitoring, risk calculation, and PnL attribution
- Examples: `WALLET:SPOT_ASSET:ETH`, `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`, `ETHERFI:LST:WEETH@ETHEREUM`

**Trading Instruments** (`SPOT_PAIR`):
- Represent tradable routes for execution routing
- Used for finding best execution venue and routing trades
- Examples: `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`, `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM`

**Note**: For derivatives (futures, options, perps), the same instrument ID serves as both position and trading instrument. For DeFi, derivatives are CEX-only and excluded from this document.

## Legacy Format Conversion

The following mapping shows how legacy instrument identifiers (used in strategy configs) convert to canonical format:

| Legacy Format | Canonical Format | Notes |
|--------------|------------------|-------|
| `wallet:BaseToken:*` | `WALLET:SPOT_ASSET:*` | Wallet holdings |
| `aave_v3:aToken:*` | `AAVE_V3_ETH:A_TOKEN:*@ETHEREUM` | AAVE lending positions |
| `aave_v3:debtToken:*` | `AAVE_V3_ETH:DEBT_TOKEN:*@ETHEREUM` | AAVE borrowing positions |
| `etherfi:BaseToken:weETH` | `ETHERFI:LST:WEETH@ETHEREUM` | LST staking position |
| `etherfi:LST:weETH` | `ETHERFI:LST:WEETH@ETHEREUM` | Already canonical |
| `curve:BaseToken:weETH` | `CURVE-ETH:SPOT_ASSET:WEETH@ETHEREUM` | DEX market pricing |
| `morpho:SPOT_ASSET:WETH` | `MORPHO:SPOT_ASSET:WETH@ETHEREUM` | Flash loan provider |

---

## Part I: Position Instruments

Position instruments represent actual holdings/positions after trade execution. These are used for position monitoring, risk calculation, and PnL attribution.

### 1. Wallet Positions (SPOT_ASSET)
Non-custodial wallet holdings.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 1 | `WALLET:SPOT_ASSET:USDT` | USDT | Base currency for USDT share class strategies |
| 2 | `WALLET:SPOT_ASSET:ETH` | ETH | Base currency for ETH share class strategies |
| 3 | `WALLET:SPOT_ASSET:EIGEN` | EIGEN | Dust tokens from EtherFi staking rewards |
| 4 | `WALLET:SPOT_ASSET:ETHFI` | ETHFI | Dust tokens from EtherFi staking rewards |

### 2. AAVE V3 Lending Positions (A_TOKEN)
AAVE V3 supply positions that auto-compound via liquidity index.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 5 | `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM` | AUSDT | Pure lending USDT strategy |
| 6 | `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM` | AWETH | Pure lending ETH, recursive staking collateral |

### 3. AAVE V3 Borrowing Positions (DEBT_TOKEN)
AAVE V3 debt positions for leveraged strategies.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 7 | `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM` | DEBTWETH | Leveraged staking, recursive strategies |

### 4. EtherFi Staking Positions (LST)
Liquid staking token positions for ETH staking strategies.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 8 | `ETHERFI:LST:WEETH@ETHEREUM` | WEETH | ETH staking, leveraged staking, hedged staking |

### 5. Lido Staking Positions (LST)
Liquid staking token positions for Lido staking (alternative to EtherFi).

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 9 | `LIDO:LST:STETH@ETHEREUM` | STETH | ETH staking via Lido (rebasing token) |
| 10 | `LIDO:LST:WSTETH@ETHEREUM` | WSTETH | ETH staking via Lido (wrapped, non-rebasing) |

### 6. DEX Market Pricing (SPOT_ASSET)
DEX liquidity pool positions used for market pricing oracle fallback.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 11 | `CURVE-ETH:SPOT_ASSET:WEETH@ETHEREUM` | WEETH | Market pricing for WEETH exit valuation |

### 7. Flash Loan Provider (SPOT_ASSET)
Flash loan positions for atomic recursive strategy execution.

| # | Instrument ID | Asset | Usage |
|---|---------------|-------|-------|
| 12 | `MORPHO:SPOT_ASSET:WETH@ETHEREUM` | WETH | Flash loans for recursive leveraged staking |

---

## Part II: Trading Instruments (SPOT_PAIR)

Trading instruments represent tradable routes for execution routing. These are used for finding the best execution venue and routing trades.

### 1. DEX Swap Trading Pairs (SPOT_PAIR)
Decentralized exchange swap routes for on-chain asset conversion.

| # | Instrument ID | Venue | Base | Quote | Usage | Data Available |
|---|---------------|-------|------|-------|-------|----------------|
| 13 | `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM` | Uniswap V3 | ETH | USDT | USDT→ETH swaps for staking strategies | ✅ ETH/USDT spot prices |
| 14 | `CURVE-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM` | Curve | ETH | USDT | Alternative DEX route for ETH/USDT | ✅ ETH/USDT spot prices |
| 15 | `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM` | Curve | ETH | WEETH | ETH↔WEETH swaps (staking/unstaking) | ✅ weETH/ETH ratios (market) |
| 16 | `UNISWAPV3-ETH:SPOT_PAIR:ETH-WSTETH@ETHEREUM` | Uniswap V3 | ETH | WSTETH | ETH↔WSTETH swaps (Lido staking) | ✅ wstETH/ETH ratios (market) |

**Usage Notes**:
- **USDT→ETH**: Used for staking strategies when starting with USDT share class
- **ETH↔LST**: Used for staking/unstaking operations on DEX
- **Execution Cost**: 5-35 bps per presentation.html (ETH/USDT: 5-13 bps, ETH/WEETH: 20-35 bps)
- **Oracle vs Market**: Entry uses AAVE oracle pricing, exit uses DEX market pricing

---

## Strategy Usage Matrix

### Pure Lending Strategies
- **Pure Lending USDT**: `WALLET:SPOT_ASSET:USDT` → `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
- **Pure Lending ETH**: `WALLET:SPOT_ASSET:ETH` → `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM`

**Trading Routes**: None (direct supply to AAVE, no swaps)

### Staking Strategies
- **ETH Staking Only**: `WALLET:SPOT_ASSET:ETH` → `ETHERFI:LST:WEETH@ETHEREUM`
- **ETH Leveraged Staking**: `WALLET:SPOT_ASSET:ETH` → `MORPHO:SPOT_ASSET:WETH@ETHEREUM` → `ETHERFI:LST:WEETH@ETHEREUM` + `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM` + `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

**Trading Routes**:
- If starting with USDT: `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM` (USDT→ETH) → Stake ETH
- For unstaking: `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM` (WEETH→ETH)

### Leveraged Staking Strategies
- **ETH Leveraged**: Uses flash loan sequence via `MORPHO:SPOT_ASSET:WETH@ETHEREUM` → Result: `ETHERFI:LST:WEETH@ETHEREUM` + `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM` + `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM`

**Trading Routes**:
- Atomic flash loan sequence (no separate swap trades, all in one transaction)

---

## Complete Position Subscription Reference

### DeFi Position Subscriptions
```yaml
position_subscriptions:
  # Wallet positions
  - "WALLET:SPOT_ASSET:USDT"
  - "WALLET:SPOT_ASSET:ETH"
  - "WALLET:SPOT_ASSET:EIGEN"
  - "WALLET:SPOT_ASSET:ETHFI"
  
  # AAVE V3 positions
  - "AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM"
  - "AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM"
  - "AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM"
  
  # EtherFi staking
  - "ETHERFI:LST:WEETH@ETHEREUM"
  
  # Lido staking (alternative)
  - "LIDO:LST:STETH@ETHEREUM"
  - "LIDO:LST:WSTETH@ETHEREUM"
  
  # DEX market pricing
  - "CURVE-ETH:SPOT_ASSET:WEETH@ETHEREUM"
  
  # Flash loan provider
  - "MORPHO:SPOT_ASSET:WETH@ETHEREUM"
```

---

## Complete Trading Instrument Reference

### DEX Swap Trading Pairs
```yaml
trading_instruments:
  dex_swap:
    - "UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM"
    - "CURVE-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM"
    - "CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM"
    - "UNISWAPV3-ETH:SPOT_PAIR:ETH-WSTETH@ETHEREUM"
```

---

## Trading vs Position Instrument Mapping

### Example: ETH Staking (Starting with USDT)
**Trading Instruments** (for execution routing):
- `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM` (swap USDT→ETH)

**Position Instruments** (after execution):
- `WALLET:SPOT_ASSET:ETH` (ETH balance after swap)
- `ETHERFI:LST:WEETH@ETHEREUM` (staked position after staking)

### Example: ETH Leveraged Staking
**Trading Instruments** (for execution routing):
- Atomic flash loan sequence (no separate SPOT_PAIR trades)

**Position Instruments** (after execution):
- `ETHERFI:LST:WEETH@ETHEREUM` (staked position)
- `AAVE_V3_ETH:A_TOKEN:AWETH@ETHEREUM` (collateral)
- `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM` (debt)

### Example: Unstaking WEETH
**Trading Instruments** (for execution routing):
- `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM` (swap WEETH→ETH)

**Position Instruments** (after execution):
- `WALLET:SPOT_ASSET:ETH` (ETH balance)
- `ETHERFI:LST:WEETH@ETHEREUM` (before: WEETH balance, after: 0)

---

## Data Availability Matrix

### Spot Price Data (SCRIPTS_DATA_GUIDE.md)
| Trading Pair | Data Location | Status |
|--------------|---------------|--------|
| ETH/USDT | `data/market_data/spot_prices/eth_usd/` | ✅ Available |
| weETH/ETH | `data/market_data/spot_prices/lst_eth_ratios/curve_weETHWETH_*.csv` | ✅ Available (market pricing) |
| wstETH/ETH | `data/market_data/spot_prices/lst_eth_ratios/uniswapv3_wstETHWETH_*.csv` | ✅ Available (market pricing) |

### Oracle Price Data (AAVE)
| Trading Pair | Data Location | Status |
|--------------|---------------|--------|
| weETH/ETH | `data/protocol_data/aave/oracle/weETH_ETH_oracle_*.csv` | ✅ Available (oracle pricing) |
| weETH/USD | `data/protocol_data/aave/oracle/weETH_USD_oracle_*.csv` | ✅ Available (oracle pricing) |
| wstETH/ETH | `data/protocol_data/aave/oracle/wstETH_ETH_oracle_*.csv` | ✅ Available (oracle pricing) |
| wstETH/USD | `data/protocol_data/aave/oracle/wstETH_USD_oracle_*.csv` | ✅ Available (oracle pricing) |

### Execution Cost Data
| Trading Pair | Data Location | Status |
|--------------|---------------|--------|
| ETH/USDT (DEX) | `data/execution_costs/execution_cost_simulation_results.csv` | ✅ Available (5-13 bps) |
| ETH/WEETH (DEX) | `data/execution_costs/execution_cost_simulation_results.csv` | ✅ Available (20-35 bps) |

---

## Execution Middleware Subscriptions

### Instadapp Middleware (DeFi Atomic)
**Trading Instruments**:
- `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM` (for USDT→ETH swaps in atomic transactions)
- `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM` (for ETH↔WEETH swaps)

**Position Instruments**:
- All DeFi position instruments (for position monitoring after atomic transactions)

### Direct Protocol Integration
**Trading Instruments**:
- `CURVE-ETH:SPOT_PAIR:ETH-WEETH@ETHEREUM` (for staking/unstaking via Curve)
- `UNISWAPV3-ETH:SPOT_PAIR:ETH-WSTETH@ETHEREUM` (for Lido staking/unstaking)

**Position Instruments**:
- Protocol-specific positions (AAVE tokens, LST tokens)

---

## Verification Checklist

- [x] All DeFi position subscriptions from venue configs identified
- [x] All DEX swap pairs from strategy flows identified
- [x] All CEX references removed (Binance, Bybit, OKX, Binance-Futures)
- [x] Data availability verified per SCRIPTS_DATA_GUIDE.md
- [x] Strategy usage mapped per STRATEGY_MODES.md
- [x] Execution cost data verified per presentation.html
- [x] Trading vs position instruments clearly distinguished
- [x] Execution middleware subscriptions defined

---

## Notes

1. **Oracle vs Market Pricing**:
- **Entry**: Use AAVE oracle pricing for instant staking (`data/protocol_data/aave/oracle/`)
- **Exit**: Use DEX market pricing for trading (`data/market_data/spot_prices/lst_eth_ratios/`)
- **Instrument IDs**: Use canonical format with chain suffixes (e.g., `UNISWAPV3-ETH:SPOT_PAIR:ETH-USDT@ETHEREUM`)
   - **PnL Attribution**: Use oracle pricing for yield calculation
   - **Position Monitoring**: Use market pricing for current value

2. **Flash Loans**: MORPHO flash loans are used for atomic recursive strategy execution, enabling single-transaction leverage amplification without iterative loops.

3. **Dust Tokens**: EIGEN and ETHFI are seasonal reward tokens from EtherFi staking that accumulate over time. These are held as `WALLET:SPOT_ASSET:EIGEN` and `WALLET:SPOT_ASSET:ETHFI` until conversion.

4. **Auto-Compounding**: AAVE A_TOKEN positions auto-compound via liquidity index scaling - the balance remains constant while value grows.

5. **LST Selection**: EtherFi WEETH selected over Lido STETH per presentation.html analysis. Lido returns weaker when accounting for EigenLayer restaking and bonus rewards.

6. **Execution Costs** (per presentation.html):
   - DEX Swaps (ETH/USDT): 5-13 bps
   - DEX Swaps (ETH/WEETH): 20-35 bps

7. **Atomic Transactions**: Instadapp middleware enables atomic multi-step operations (flash borrow → stake → supply → borrow → repay) in single transaction, reducing gas costs by 80%+.

---

## Related Documentation

- [INSTRUMENT_KEY.md](./INSTRUMENT_KEY.md) - Complete instrument key specification
- [SERVICE_OVERVIEW.md](./SERVICE_OVERVIEW.md) - Instruments service overview
- [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) - Migration from legacy format

## Strategy Execution Flow Reference

For complete strategy execution flows, see:
- [STRATEGY_MODES.md](../../archive/basis-strategy-v1/docs/STRATEGY_MODES.md) - Strategy mode specifications
- [SCRIPTS_DATA_GUIDE.md](../../archive/basis-strategy-v1/docs/SCRIPTS_DATA_GUIDE.md) - Data availability guide
- [presentation.html](../../strategy-service/presentation/presentation.html) - Strategy workflow diagrams
