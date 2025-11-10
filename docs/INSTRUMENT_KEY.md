# Instrument ID Specification - Service Implementation Guide

> **Canonical Reference**: This document provides service-specific implementation details for instruments-service. For the complete canonical instrument ID specification, see [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md).
>
> **Note**: "Instrument ID" and "instrument key" are used interchangeably in this document. Both refer to the canonical instrument identifier format: `VENUE:INSTRUMENT_TYPE:PAYLOAD[@CHAIN]`.

---

## Purpose

This document provides implementation-specific details for instruments-service, including:
- Raw exchange type → canonical type mapping
- Service-specific validation rules
- Implementation verification examples
- Code-level details
- DeFi enrichment patterns (contract addresses, pool information)

---

## Canonical Specification Reference

**The canonical instrument ID specification is defined in**: [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md)

**Key Canonical Format**: `VENUE:INSTRUMENT_TYPE:PAYLOAD[@CHAIN]`

**Canonical Instrument Types**: `SPOT_ASSET`, `SPOT_PAIR`, `PERPETUAL`, `FUTURE`, `OPTION`, `POOL`, `LST`, `A_TOKEN`, `DEBT_TOKEN`, `EQUITY`, `INDEX`

**Canonical Venue Format**: `BINANCE-SPOT`, `BINANCE-FUTURES`, `BYBIT`, `OKX`, `DERIBIT`, `UNISWAPV3-ETH`, `CURVE-ETH`, `AAVE_V3_ETH`, `ETHERFI`, `LIDO`, `WALLET`, `CME`, `NASDAQ`

---

## Raw Exchange Type → Canonical Type Mapping

Exchange APIs return raw types (lowercase) which are mapped to canonical types (UPPERCASE):

```
Exchange API 'spot' → Canonical 'SPOT_PAIR'
Exchange API 'perpetual' → Canonical 'PERPETUAL'  
Exchange API 'future' → Canonical 'FUTURE'
Exchange API 'option' → Canonical 'OPTION'
```

**Note**: Raw `symbol_type` is not stored to avoid confusion with canonical `instrument_type`.

---

## SPOT_ASSET vs SPOT_PAIR Distinction

### SPOT_ASSET
- **Purpose**: Represents actual asset positions held on a specific venue
- **Format**: `VENUE:SPOT_ASSET:ASSET`
- **Examples**:
  - `BINANCE-SPOT:SPOT_ASSET:BTC` (actual BTC position on Binance)
  - `WALLET:SPOT_ASSET:ETH` (ETH held in wallet)
  - `WALLET:SPOT_ASSET:USDT` (USDT held in wallet)

### SPOT_PAIR
- **Purpose**: Represents trading routes for execution routing
- **Format**: `VENUE:SPOT_PAIR:BASE-QUOTE`
- **Examples**:
  - `BINANCE-SPOT:SPOT_PAIR:BTC-USDT` (trading route, never stored as position)
  - `UNISWAPV3-ETH:SPOT_PAIR:USDC-ETH@ETHEREUM` (DEX trading route)

### Trading Flow
1. **Execute trade**: Use `SPOT_PAIR` to find best route (e.g., `BINANCE-SPOT:SPOT_PAIR:BTC-USDT`)
2. **Update positions**: After trade, track `SPOT_ASSET` positions (e.g., `BINANCE-SPOT:SPOT_ASSET:BTC`, `BINANCE-SPOT:SPOT_ASSET:USDT`)

**Key Principle**: `SPOT_PAIR` is routing-only, never stored as a position. Trades result in `SPOT_ASSET` deltas.

---

## Symbol Formats by Instrument Type

- **SPOT_ASSET**: Asset code only (e.g., `BTC`, `ETH`, `USDT`, `WEETH`)
- **SPOT_PAIR**: `BASE-QUOTE` (e.g., `BTC-USDT`, `ETH-USDT`)
- **PERPETUAL**: `BASE-QUOTE@LIN` or `BASE-QUOTE@INV` (e.g., `ETH-USDT@LIN`, `BTC-USD@INV`)
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset
- **FUTURE**: `BASE-QUOTE-YYMMDD@LIN` or `BASE-QUOTE-YYMMDD@INV` (e.g., `BTC-USD-241225@LIN`, `BTC-USD-241225@INV`)
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset
- **OPTION**: `BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE@LIN` or `BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE@INV` (e.g., `BTC-USD-241225-50000-CALL@LIN`, `BTC-USD-241225-50000-CALL@INV`)
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset
- **LST**: Asset code (e.g., `WEETH`, `STETH`, `WSTETH`)
- **A_TOKEN**: Token code (e.g., `AUSDT`, `AWETH`)
- **DEBT_TOKEN**: Token code (e.g., `DEBTWETH`)

---

## InstrumentKey Dataclass Structure

The `InstrumentKey` dataclass parses the symbol but also stores parsed components as separate fields for query convenience:

**Example**: `DERIBIT:OPTION:BTC-USD-241225-50000-CALL`
- `symbol` = `BTC-USD-241225-50000-CALL` (full symbol string)
- `expiry` = `241225` (stored separately as datetime)
- `strike` = `50000` (stored separately)
- `option_type` = `CALL` (stored separately)

**Format**: Always `VENUE:TYPE:SYMBOL` where SYMBOL itself contains all components.

---

## Attributes Schema

Instrument definitions include extended attributes (non-exhaustive list):

### Standard Attributes
- `underlying`: Optional[str] - Canonical reference (e.g., `BTC-USDT`) or another instrument_key
- `base_asset`: Optional[str] - Base asset code
- `quote_asset`: Optional[str] - Quote asset code
- `settle_asset`: Optional[str] - Settlement asset
- `expiry`: Optional[datetime] - Precise UTC datetime
- `strike`: Optional[str] - Strike price (as string to support formats like "5K")
- `option_type`: Optional["CALL","PUT"] - Option type (uppercase)
- `contract_size`: Optional[float] - Contract size
- `tick_size`: Optional[float] - Tick size
- `min_size`: Optional[float] - Minimum order size
- `asset_class`: ["crypto","traditional"] - Asset class
- `venue_type`: ["exchange","protocol","wallet"] - Venue type
- `exchange_raw_symbol`: Optional[str] - Raw exchange symbol
- `ccxt_symbol`: Optional[str] - CCXT symbol
- `ccxt_exchange`: Optional[str] - CCXT exchange name
- `inverse`: Optional[bool] - Whether instrument is inverse
- `tardis_symbol`: Optional[str] - Tardis API symbol
- `tardis_exchange`: Optional[str] - Tardis API exchange name
- `data_provider`: Optional["tardis","databento"] - Data provider
- `data_types`: Optional[list[str]] - Available data types (e.g., `["trades","book_snapshot_5","liquidations"]`)

### DeFi-Specific Attributes

For DeFi instruments (DEX pairs, protocol tokens), additional attributes provide contract addresses and pool information:

- `base_asset_contract_address`: Optional[str] - ERC-20 contract address for base asset
- `quote_asset_contract_address`: Optional[str] - ERC-20 contract address for quote asset
- `pool_address`: Optional[str] - Pool contract address (for DEX pairs, computed from tokens + fee)
- `pool_fee_tier`: Optional[int] - Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%)
- `pool_version`: Optional[str] - Pool version (e.g., "v3", "v4")
- `factory_address`: Optional[str] - Factory contract address for pool computation
- `chain_id`: Optional[int] - Chain ID (1 = Ethereum mainnet, 137 = Polygon, etc.)
- `token_decimals`: Optional[Dict[str, int]] - Token decimals (e.g., `{"base": 18, "quote": 6}`)
- `pool_type`: Optional[str] - Pool type (e.g., "uniswap_v3", "curve_stable")

---

## Validation Rules

- `VENUE` ∈ Venue enum
- `INSTRUMENT_TYPE` ∈ InstrumentType enum
- **SPOT_ASSET**: Symbol is asset code (BTC, ETH, USDT, ...) - represents actual holdings
- **SPOT_PAIR**: Symbol is `BASE-QUOTE`; QUOTE ∈ {USD, USDT, USDC, ...}; routing only, never stored as position
- **PERPETUAL**: Symbol is `BASE-QUOTE@LIN` or `BASE-QUOTE@INV`; QUOTE ∈ {USD, USDT, USDC, ...}
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset
- **FUTURE**: Symbol is `BASE-QUOTE-YYMMDD@LIN` or `BASE-QUOTE-YYMMDD@INV`; `attrs.expiry` required; `attrs.contract_size` required
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset
- **OPTION**: Symbol is `BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE@LIN` or `BASE-QUOTE-YYMMDD-STRIKE-OPTION_TYPE@INV`; `attrs.expiry`, `attrs.strike`, `attrs.option_type` required
  - `@LIN` (linear): Quote asset == margin currency (settle_asset)
  - `@INV` (inverse): Margin currency == base asset

---

## Implementation Verification

### Working Examples from Real Data (2023-05-23)

The current implementation uses direct instrument-ids approach, which is simpler and more reliable:

```python
# Verified working instrument IDs from 2023-05-23 data:
'BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN'    # ✅ Linear perpetual (USDT margin)
'BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN'    # ✅ Linear perpetual (USDT margin)
'BINANCE-FUTURES:PERPETUAL:BNB-USDT@LIN'    # ✅ Linear perpetual (USDT margin)
'BINANCE-FUTURES:PERPETUAL:SOL-USDT@LIN'    # ✅ Linear perpetual (USDT margin)
'DERIBIT:PERPETUAL:BTC-USD@INV'              # ✅ Inverse perpetual (BTC margin)

# Deribit options (with auto-extracted strikes/expiry):
'DERIBIT:OPTION:BTC-USD-230630-100000-CALL'  # ✅ Working format
'DERIBIT:OPTION:BTC-USD-230630-100000-PUT'   # ✅ Working format
```

### Direct Instrument-ID Usage

The current implementation simplifies filtering by using instrument-ids directly:

```bash
# ✅ Working CLI approach (simplified)
python -m instruments_service.cli.main \
    --mode instruments \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --instrument-ids BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN \
    --force

# ✅ Multiple instruments
--instrument-ids BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN,BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN
```

### Client Usage Verification

```python
# ✅ Verified working with unified-cloud-services
from unified_cloud_services import create_instruments_client

client = create_instruments_client()

# Get specific instrument details (verified working)
details = client.get_instrument_details(
    date='2023-05-23',
    instrument_id='BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN'
)

# Confirmed fields in real data:
# - instrument_key: 'BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN'
# - venue: 'BINANCE-FUTURES'
# - instrument_type: 'PERPETUAL'
# - symbol: 'BTC-USDT@LIN'
# - base_asset: 'BTC'
# - quote_asset: 'USDT'
# - data_types: 'trades,book_snapshot_5,derivative_ticker,liquidations'
```

---

## Worked Examples

### Spot Trading Workflow

1. **Execute trade**: `BINANCE-SPOT:SPOT_PAIR:BTC-USDT` (find best route)
2. **Update positions**: `BINANCE-SPOT:SPOT_ASSET:BTC`, `BINANCE-SPOT:SPOT_ASSET:USDT`

### Perpetuals

- `BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN` (linear - USDT is margin currency)
- `DERIBIT:PERPETUAL:BTC-USD@INV` (inverse - BTC is margin currency)

### Crypto Futures

- `DERIBIT:FUTURE:BTC-USD-241225@INV` (inverse - BTC margin) with `attrs.expiry="2024-12-25T08:00:00Z"`
- `BINANCE-FUTURES:FUTURE:BTC-USDT-241225@LIN` (linear - USDT margin) with `attrs.expiry="2024-12-25T08:00:00Z"`

### TradFi Futures

- `CME:FUTURE:ES-202412` with `attrs.exchange_raw_symbol="ESZ4"`, expiry normalized

### Options

- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@INV` (inverse - BTC margin) with normalized expiry
- `DERIBIT:OPTION:BTC-USD-241225-50000-CALL@LIN` (linear - USD margin) with normalized expiry
- `CME:OPTION:ES-202412-4500-CALL` with month-code preserved in `attrs.exchange_raw_symbol`

### DeFi Lending/Staking

- `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM` (AAVE lending position)
- `AAVE_V3_ETH:DEBT_TOKEN:DEBTWETH@ETHEREUM` (AAVE borrowing position)
- `ETHERFI:LST:WEETH@ETHEREUM` (EtherFi staking position)

---

## Expiry Handling

- `attrs.expiry` holds precise UTC datetime (including half-hour if applicable)
- Daily snapshots store `attrs.expiry`
- An auxiliary expiry_calendar lists exact intra-day expiries per day

---

## Identity Tiers

- **instrument_id** (also called "instrument key"): Canonical routing/position identity
- **exchange_raw_symbol**: Provider-native identifier for IO

**Note**: The term "instrument ID" is preferred in documentation, while "instrument_key" is used in code field names for consistency with existing implementations.

---

## DeFi Instrument Enrichment

### Problem Statement

DeFi instruments need contract addresses and pool information for execution, but:
- Contract addresses are long and not user-intuitive (e.g., `0xC02aaA39b223FE8D0A0e5c4F27eAD9083c756Cc2`)
- **Same trading pair can exist on multiple pools** (e.g., ETH-USDT on Uniswap V3 with fees 500, 3000, 10000)
- **Different pool versions coexist** (Uniswap V2 and V3 both have ETH-USDT pools)
- Pool addresses are even longer and change per fee tier and version
- Keeping instrument IDs user-friendly while having execution details

### Key Findings

**YES - One venue can have multiple pools for the same pair:**
- Uniswap V3: Multiple fee tiers per pair (0.05%, 0.3%, 1% = 500, 3000, 10000 bps)
- Uniswap V2: One pool per pair (0.3% fee)
- Both V2 and V3 coexist with same pairs (e.g., ETH-USDT on both)

**YES - Different pool versions can have the same instrument pair:**
- Uniswap V2 and V3 both have ETH-USDT pools
- Same token pair exists across versions with different pool mechanics

**Conclusion: We need differentiation in instrument ID for routing clarity.**

### Recommended Approach: Version-Aware Instrument IDs

**Differentiate pool versions and fee tiers in instrument ID, store contract addresses in attributes.**

#### Why Version Differentiation is Needed

Since multiple pools exist for the same pair:
- **Different versions** (V2 vs V3 vs V4) have different mechanics
- **Different fee tiers** within V3 (500, 3000, 10000 bps) have different pool addresses
- **Execution middleware** needs explicit routing - can't infer from single instrument ID

#### Recommended Pattern

**Option A: Version + Fee in Venue Name** (Recommended for routing clarity)
```
UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT  # Uniswap V3, 0.3% fee
UNISWAP_V3_500:SPOT_PAIR:ETH-USDT   # Uniswap V3, 0.05% fee
UNISWAP_V2:SPOT_PAIR:ETH-USDT       # Uniswap V2 (0.3% fee implied)
```

**Option B: Version in Venue, Fee in Symbol**
```
UNISWAP_V3:SPOT_PAIR:ETH-USDT-3000  # Uniswap V3, 0.3% fee
UNISWAP_V3:SPOT_PAIR:ETH-USDT-500   # Uniswap V3, 0.05% fee
UNISWAP_V2:SPOT_PAIR:ETH-USDT       # Uniswap V2 (0.3% fee implied)
```

**Option C: Keep Simple ID, Differentiate in Attributes** (Only if middleware can infer)
```
UNISWAP:SPOT_PAIR:ETH-USDT  # Simple ID
# Attributes contain pool_version and pool_fee_tier
# Middleware must query attributes to find available pools
```

### Two-Tier Identification System

**Tier 1: User-Friendly Instrument ID** (for routing, configs, logs)
```
UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT
CURVE:SPOT_PAIR:ETH-WEETH
AAVE_V3:A_TOKEN:AWETH
```

**Tier 2: Execution Attributes** (for actual blockchain interaction)
```python
{
    "instrument_key": "UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT",
    "base_asset_contract_address": "0xC02aaA39b223FE8D0A0e5c4F27eAD9083c756Cc2",  # WETH
    "quote_asset_contract_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
    "pool_address": "0x...",  # Computed from factory + tokens + fee
    "pool_fee_tier": 3000,  # 0.3%
    "pool_version": "v3",
    "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    "chain_id": 1
}
```

### Multiple Pools Handling

**Recommended: Separate instrument definitions with version + fee in instrument ID**

```python
# Pool 1: Uniswap V3, 0.05% fee
{
    "instrument_key": "UNISWAP_V3_500:SPOT_PAIR:ETH-USDT",
    "pool_version": "v3",
    "pool_fee_tier": 500,
    "pool_address": "0x1234...",  # Unique pool address
    "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    # ... other attributes
}

# Pool 2: Uniswap V3, 0.3% fee  
{
    "instrument_key": "UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT",
    "pool_version": "v3",
    "pool_fee_tier": 3000,
    "pool_address": "0x5678...",  # Different pool address
    "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    # ... other attributes
}

# Pool 3: Uniswap V2 (0.3% fee implied)
{
    "instrument_key": "UNISWAP_V2:SPOT_PAIR:ETH-USDT",
    "pool_version": "v2",
    "pool_fee_tier": 3000,  # V2 always 0.3%
    "pool_address": "0x9abc...",  # Different pool address
    "factory_address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",  # V2 factory
    # ... other attributes
}
```

### Execution Middleware Routing

**Key Insight**: If all pools use the same API interface (just message instructions), middleware can:
1. Receive trade instruction with instrument ID: `UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT`
2. Look up instrument definition to get `pool_address` from attributes
3. Execute trade directly to that pool

**If API interfaces differ by version**, use separate venue names:
- `UNISWAP_V2` - Different interface than V3
- `UNISWAP_V3` - Modern interface
- `UNISWAP_V4` - Latest interface

**If all versions use same interface**, version in ID is just for routing clarity:
- Middleware can still route to correct pool using `pool_address` from attributes
- But explicit version in ID helps with:
  - Configuration clarity
  - Logging and debugging
  - Position tracking per pool

### Data Sources & Libraries

#### Recommended Libraries & Services

1. **Uniswap SDK** (Primary Source)
   - **Library**: `@uniswap/v3-sdk`, `@uniswap/v4-sdk`
   - **Purpose**: Compute pool addresses, get token metadata, discover pools
   - **Key Functions**:
     - `computePoolAddress()` - Deterministic pool address computation
     - `Pool.getAddress()` - Get pool address from tokens + fee
     - Token instances with contract addresses
     - `getPool()` - Check if pool exists for token pair + fee

2. **1inch Fusion SDK** (Aggregation & Routing)
   - **Library**: `@1inch/fusion-sdk`
   - **Purpose**: Token swap routing, pool discovery, best execution
   - **Key Functions**:
     - `getQuote()` - Get best quote across multiple pools
     - `placeOrder()` - Execute swaps with token addresses
     - Automatic pool selection and routing

3. **Web3 Ethereum DeFi** (Python Library)
   - **Library**: `web3-ethereum-defi`
   - **Purpose**: Comprehensive DeFi data access, pool discovery
   - **Key Functions**:
     - Pool address lookup by token pair
     - Token address resolution
     - Pool liquidity analysis

4. **Alchemy SDK** (Token Metadata)
   - **Library**: `@alchemyplatform/alchemy-sdk-js`
   - **Purpose**: Get token contract addresses, metadata, chain information
   - **Key Methods**:
     - `core.getTokenMetadata(address)` - Get token info by address
     - `portfolio.getTokensByWallet()` - Get tokens with addresses

5. **Ethers.js** (Blockchain Interaction)
   - **Library**: `ethers.js` (v5 or v6)
   - **Purpose**: Direct contract interaction, ERC-20 token lookups
   - **Key Functions**:
     - `getAddress()` - Address normalization
     - `Contract` - Interact with token/pool contracts

6. **The Graph** (Subgraph Queries)
   - **Service**: The Graph Protocol
   - **Purpose**: Indexed DeFi data including pools, tokens, addresses
   - **GraphQL Queries**: Query pools, tokens, contract addresses

7. **Token Lists** (Static References)
   - **Sources**: 
     - Uniswap Token Lists: `https://tokenlists.org/`
     - CoinGecko Token Lists
     - Chain-specific token registries
   - **Purpose**: Standardized token addresses per chain

### Implementation Pattern

#### Enrichment Service Pattern

Create an `InstrumentEnrichmentService` that enriches instrument definitions with contract addresses and pool info:

```python
class InstrumentEnrichmentService:
    """Enriches instrument definitions with contract addresses and pool info"""
    
    async def enrich_spot_pair(
        self, 
        instrument_key: str,
        base_symbol: str,
        quote_symbol: str,
        venue: str,
        chain_id: int = 1
    ) -> Dict[str, Any]:
        """
        Enrich SPOT_PAIR instrument with contract addresses and pool info
        
        Returns attributes dict with:
        - base_asset_contract_address
        - quote_asset_contract_address  
        - pool_address (if applicable)
        - pool_fee_tier (if applicable)
        - factory_address
        - chain_id
        """
        # 1. Resolve token addresses from symbol
        base_address = await self.resolve_token_address(base_symbol, chain_id)
        quote_address = await self.resolve_token_address(quote_symbol, chain_id)
        
        # 2. If DEX venue, compute pool addresses for available fee tiers
        if venue in ['UNISWAP', 'CURVE']:
            pools = await self.find_available_pools(
                base_address, 
                quote_address, 
                venue, 
                chain_id
            )
            return {
                'base_asset_contract_address': base_address,
                'quote_asset_contract_address': quote_address,
                'available_pools': pools,  # List of {pool_address, fee_tier}
                'chain_id': chain_id
            }
        
        return {
            'base_asset_contract_address': base_address,
            'quote_asset_contract_address': quote_address,
            'chain_id': chain_id
        }
```

#### Token Address Resolution Strategy

**Multi-source lookup with caching**:

```python
async def resolve_token_address(symbol: str, chain_id: int) -> str:
    """
    Resolution order:
    1. Built-in token registry (common tokens: ETH, USDT, USDC, WETH, etc.)
    2. Cached token list (tokenlists.org, CoinGecko)
    3. Alchemy SDK getTokenMetadata (if we have address guess)
    4. The Graph subgraph query
    5. Etherscan API (last resort)
    """
```

**Common Token Registry** (built-in):
```python
ETHEREUM_MAINNET_TOKENS = {
    'ETH': '0x0000000000000000000000000000000000000000',  # Native
    'WETH': '0xC02aaA39b223FE8D0A0e5c4F27eAD9083c756Cc2',
    'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
    'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
    'DAI': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
    'WEETH': '0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee',  # EtherFi
    'WSTETH': '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0',  # Lido
    # ... more common tokens
}
```

#### Pool Discovery

**For Uniswap V3**:
```python
# Known fee tiers
UNISWAP_V3_FEE_TIERS = [100, 500, 3000, 10000]  # 0.01%, 0.05%, 0.3%, 1%

# Compute pool address for each fee tier
# Verify pool exists on-chain (has liquidity)
# Return only active pools
```

**For Curve**:
```python
# Curve uses different pool types (stable, crypto, etc.)
# Query Curve registry or The Graph subgraph
# Pool addresses are deployed contracts, not computed
```

### Example: Enriched Instrument Definition

#### Before (Simple)
```yaml
instrument_key: "UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT"
venue: "UNISWAP_V3_3000"
instrument_type: "SPOT_PAIR"
symbol: "ETH-USDT"
base_asset: "ETH"
quote_asset: "USDT"
```

#### After (Enriched)
```yaml
instrument_key: "UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT"
venue: "UNISWAP_V3_3000"
instrument_type: "SPOT_PAIR"
symbol: "ETH-USDT"
base_asset: "ETH"
quote_asset: "USDT"

# Enrichment attributes
base_asset_contract_address: "0xC02aaA39b223FE8D0A0e5c4F27eAD9083c756Cc2"  # WETH
quote_asset_contract_address: "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # USDT
pool_address: "0x11b815efB8f581194ae79006d24E0d814B7697F6"  # Computed pool
pool_fee_tier: 3000  # 0.3%
pool_version: "v3"
factory_address: "0x1F98431c8aD98523631AE4a59f267346ea31F984"
chain_id: 1
token_decimals:
  base: 18
  quote: 6
```

### Implementation Recommendations

1. **Use Existing Attributes Pattern**
   - Your `InstrumentDefinition` model already supports extended attributes. Add DeFi-specific fields as optional attributes.

2. **Lazy Enrichment**
   - Don't require enrichment at instrument definition time
   - Enrich on-demand when needed for execution
   - Cache enriched data for performance

3. **Enrichment Sources Priority**
   1. **Token Lists** (fastest, static)
   2. **Uniswap SDK** (pool computation)
   3. **Alchemy SDK** (token metadata)
   4. **The Graph** (pool discovery)
   5. **On-chain queries** (verification)

4. **Validation**
   - Verify contract addresses are valid (checksum)
   - Verify pools exist and have liquidity
   - Validate fee tiers match pool addresses

5. **Caching Strategy**
   - Cache token address lookups (symbol → address)
   - Cache pool addresses (tokens + fee → pool address)
   - Invalidate on chain reorgs or contract upgrades

### Summary: DeFi Enrichment Approach

**Recommended Approach**:
1. ✅ **Differentiate version + fee in instrument ID**: `UNISWAP_V3_3000:SPOT_PAIR:ETH-USDT`
2. ✅ **Store execution details in attributes**: Contract addresses, pool addresses, factory addresses
3. ✅ **Multiple pools = multiple instrument definitions** with different IDs per pool
4. ✅ **Use Uniswap SDK + 1inch Fusion SDK + Alchemy SDK** for enrichment
5. ✅ **Lazy enrichment**: Enrich on-demand, cache results

**Benefits**:
- **Explicit routing**: Middleware knows exactly which pool to use
- **No inference needed**: Direct pool address from instrument ID lookup
- **Position tracking**: Each pool can be tracked separately
- **Configuration clarity**: Clear which version/fee tier is being used
- **Logging/debugging**: Easy to trace which pool executed trades

**Trade-offs**:
- Instrument IDs are slightly longer (but still readable)
- More instrument definitions to manage
- But: No ambiguity in routing, clearer execution path

---

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

**Q: Why direct instrument IDs?**  
A: Simpler and more reliable than symbol filtering - eliminates parsing complexity and ensures exact instrument targeting.

**Q: What's the difference between "instrument ID" and "instrument key"?**  
A: They refer to the same thing - the canonical instrument identifier. "Instrument ID" is preferred in documentation, while "instrument_key" is used in code field names.

**Q: How are multiple pools for the same pair handled?**  
A: Each pool gets its own instrument definition with version + fee tier in the venue name (e.g., `UNISWAP_V3_3000`, `UNISWAP_V3_500`).

**Q: When should instruments be enriched with contract addresses?**  
A: Enrichment is done on-demand when needed for execution. Don't require enrichment at instrument definition time.

---

## Related Documentation

- **Canonical Specification**: [`docs/INSTRUMENT_VENUE_SPECIFICATION.md`](../../docs/INSTRUMENT_VENUE_SPECIFICATION.md) - Complete canonical instrument ID specification
- **DeFi MVP Instruments**: [`MVP_DEFI_INSTRUMENTS.md`](./MVP_DEFI_INSTRUMENTS.md) - DeFi strategy instrument IDs
- **Service Architecture**: [`ARCHITECTURE.md`](./ARCHITECTURE.md) - Instruments service architecture
- **API Reference**: [`reference/API_REFERENCE.md`](./reference/API_REFERENCE.md) - Service API documentation
- **Usage Guide**: [`usage/USAGE_GUIDE.md`](./usage/USAGE_GUIDE.md) - Usage examples
