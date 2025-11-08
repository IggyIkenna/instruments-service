# Databento & DeFi Integration Migration Plan

**Date**: 2025-01-15  
**Status**: 🚧 In Progress  
**Goal**: Migrate instruments-service to support Databento (TradFi) and The Graph + Protocol SDKs (DeFi)

---

## Overview

This migration adds support for:
1. **Databento** - TradFi instruments (commodities, currencies, equities)
2. **The Graph** - DeFi DEX pools (Uniswap V3, Curve, etc.)
3. **Protocol SDKs** - DeFi protocol positions (AAVE, EtherFi, Lido)

---

## Current State

### ✅ What We Have
- **Tardis Integration**: Crypto instruments (perps, spot, options) ✅
- **Schema**: Parquet schema defined (now used in code) ✅
- **API Keys**: Available in `archive/basis-strategy-v1/scripts/env.downloaders`

### ❌ What's Missing
- Databento venue adapter
- The Graph integration for DEX pools
- Protocol SDKs integration
- DeFi-specific schema fields (contract addresses, pool addresses)

---

## API Keys Available

From `archive/basis-strategy-v1/scripts/env.downloaders`:

```bash
# Alchemy (for The Graph / web3)
BASIS_DOWNLOADERS__ALCHEMY_API_KEY=vV3z-UCRtQvWb26MH9v7A
BASIS_DOWNLOADERS__ALCHEMY_API_KEY_2=1M1peQWH1C6iT6c91YPMO

# AaveScan (for AAVE data)
BASIS_DOWNLOADERS__AAVESCAN_API_KEY=c2b49a72-9c73-48f9-aea2-5f6d8ec793b9

# Databento (from archive code)
# API Key: db-CLnuRBBp7tNPexMqAW3iqmvVEA7PK
```

**Note**: Need to verify Databento API key is still valid and get it from user if needed.

---

## Migration Steps

### Phase 1: Schema Updates ✅ (In Progress)

**Task**: Add DeFi-specific fields to `InstrumentDefinition` model and Parquet schema

**Fields to Add**:
- `base_asset_contract_address: Optional[str]` - ERC-20 contract address for base asset
- `quote_asset_contract_address: Optional[str]` - ERC-20 contract address for quote asset  
- `pool_address: Optional[str]` - Pool contract address (for DEX pairs)
- `pool_fee_tier: Optional[int]` - Pool fee in basis points (e.g., 500 = 0.05%, 3000 = 0.3%)

**Files to Update**:
- `instruments_service/models.py` - Add fields to `InstrumentDefinition`
- `instruments_service/schemas/parquet.py` - Add fields to schema definition

**Status**: ⏳ Pending

---

### Phase 2: Databento Integration

**Task**: Create Databento venue adapter for TradFi instruments

**Reference Code**: 
- `archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py`
- `archive/loadMarketDataHist/downloadUpload/dataBento/dataBentoDataLoader.py`

**Implementation Plan**:
1. Create `instruments_service/app/venues/databento/` directory
2. Create `databento_adapter.py` with:
   - `fetch_instrument_data()` - Fetch from Databento API
   - `process_databento_instruments()` - Convert to `InstrumentDefinition`
   - Symbol filtering logic (from archive code)
3. Add Databento venues to `Venue` enum:
   - `CME`, `NASDAQ`, `NYSE`, etc.
4. Add TradFi instrument types:
   - `EQUITY`, `COMMODITY`, `CURRENCY`, `INDEX`

**API Usage** (from archive):
```python
import databento as db
client = db.Historical(api_key)
zipped_data = client.timeseries.get_range(
    dataset=exchange_data['databento'],
    schema=db.Schema.DEFINITION,
    symbols=[...],
    stype_in="parent",
    stype_out="instrument_id",
    start=start_date_str,
    end=to_date_str,
)
```

**Status**: ⏳ Pending

---

### Phase 3: The Graph Integration (DeFi DEX Pools)

**Task**: Fetch DEX pool instruments from The Graph

**Reference**: 
- `archive/basis-strategy-v1/docs/MVP_DEFI_INSTRUMENTS.md`
- The Graph Uniswap V3 subgraph: https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3
- The Graph Curve subgraph: https://thegraph.com/hosted-service/subgraph/curvefi/curve

**Implementation Plan**:
1. Create `instruments_service/app/venues/defi/` directory
2. Create `the_graph_client.py`:
   - GraphQL queries for Uniswap V3 pools
   - GraphQL queries for Curve pools
   - Pool enumeration by base currency
3. Create `uniswapv3_adapter.py`:
   - Fetch pools from The Graph
   - Extract pool address, fee tier, token addresses
   - Generate canonical instrument keys: `UNISWAPV3-ETH:POOL:USDC-ETH:5@ETHEREUM`
4. Create `curve_adapter.py`:
   - Similar to Uniswap adapter
   - Generate: `CURVE-ETH:POOL:ETH-USDT@ETHEREUM`

**GraphQL Query Example** (Uniswap V3):
```graphql
{
  pools(
    where: {
      token0_: {symbol: "ETH"}
      OR: {token1_: {symbol: "ETH"}}
    }
  ) {
    id  # pool address
    token0 { id symbol decimals }
    token1 { id symbol decimals }
    feeTier
    liquidity
  }
}
```

**Status**: ⏳ Pending

---

### Phase 4: Protocol SDKs Integration

**Task**: Fetch protocol positions using SDKs

**Protocols**:
- **AAVE**: AAVE SDK or direct contract calls
- **EtherFi**: EtherFi SDK or The Graph
- **Lido**: Lido SDK or direct contract calls

**Implementation Plan**:
1. Create `aave_adapter.py`:
   - Fetch AAVE V3 markets (aTokens, debtTokens)
   - Generate: `AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`
2. Create `etherfi_adapter.py`:
   - Fetch EtherFi LST positions
   - Generate: `ETHERFI:LST:WEETH@ETHEREUM`
3. Create `lido_adapter.py`:
   - Fetch Lido LST positions
   - Generate: `LIDO:LST:STETH@ETHEREUM`

**Data Sources**:
- **AAVE**: AaveScan API (already have key) or AAVE SDK
- **EtherFi**: The Graph or EtherFi SDK
- **Lido**: Lido SDK or direct contract calls

**Status**: ⏳ Pending

---

## File Structure

```
instruments-service/
├── instruments_service/
│   ├── app/
│   │   ├── venues/
│   │   │   ├── databento/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── databento_adapter.py
│   │   │   │   └── databento_config.py
│   │   │   └── defi/
│   │   │       ├── __init__.py
│   │   │       ├── the_graph_client.py
│   │   │       ├── uniswapv3_adapter.py
│   │   │       ├── curve_adapter.py
│   │   │       ├── aave_adapter.py
│   │   │       ├── etherfi_adapter.py
│   │   │       └── lido_adapter.py
│   │   └── core/
│   ├── models.py  # Updated with DeFi fields
│   └── schemas/
│       └── parquet.py  # Updated with DeFi fields
```

---

## Testing Strategy

1. **Unit Tests**: Test each adapter independently
2. **Integration Tests**: Test full instrument generation pipeline
3. **E2E Tests**: Test GCS storage with DeFi instruments

---

## Next Steps

1. ✅ Update schema with DeFi fields
2. ⏳ Create Databento adapter
3. ⏳ Create The Graph client
4. ⏳ Create DeFi adapters
5. ⏳ Update instrument processing service to use new adapters
6. ⏳ Add tests

---

## References

- **Archive Code**: `archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py`
- **DeFi Spec**: `instruments-service/docs/MVP_DEFI_INSTRUMENTS.md`
- **Data Guide**: `archive/basis-strategy-v1/docs/SCRIPTS_DATA_GUIDE.md`
- **Strategy Modes**: `archive/basis-strategy-v1/docs/STRATEGY_MODES.md`

