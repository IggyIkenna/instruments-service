# Databento Translation Layer Implementation Plan

**Date**: 2025-01-15  
**Status**: Planning  
**Priority**: High (Week 7-8 per STATUS.md)

---

## Overview

Complete the Databento adapter translation layer to convert Databento's native instrument definitions to our canonical `InstrumentDefinition` format. Handle different instrument types (ETFs, futures normal/mini/micro) across multiple venues (CME, NASDAQ, NYSE, ICE).

---

## Requirements from STATUS.md

### Instrument Universe

**Commodities** (micro futures/ETF preferred):
- Sugar, Coffee, Pork Belly, Cotton, Cocoa, Orange Juice, Soybeans
- Crude Oil, Natural Gas, Gold

**Currencies** (micro futures/ETF preferred):
- G10 currencies (EUR, GBP, JPY, AUD, NZD, CAD, CHF, NOK, SEK, DKK)

**Equities** (micro futures/ETF preferred):
- Equity indices (micro futures/ETFs)
- S&P 500 index (SPY ETF, ES micro futures)
- S&P 500 stock components (individual stocks - most liquid micro futures/ETFs per stock)

### Key Requirements

1. **Liquidity-Based Selection**: Prefer most liquid micro futures or ETFs to avoid large contract sizes
2. **Symbol Filtering**: Use `allowed_databento_symbols.csv` for filtering (if available)
3. **Exchange Mapping**: Use `exchange_mappings_global.json` for exchange definitions (if available)
4. **Publisher Filtering**: Filter DBEQ.BASIC by `publisher_id == 39` (NASDAQ/NYSE)
5. **Settlement Filtering**: Exclude `instrument_class == "S"` (settlement-only)

---

## Databento Schema Fields (from Context7 & Archive Code)

### Key Fields in DEFINITION Schema

| Databento Field | Type | Description | Our Mapping |
|----------------|------|-------------|-------------|
| `symbol` | string | Instrument symbol | `exchange_raw_symbol` |
| `security_type` | string | FUT, STK, ETF, etc. | → `instrument_type` |
| `instrument_class` | string | S (settlement), etc. | Filter out "S" |
| `asset` | string | Base asset (e.g., "ES", "AAPL") | `base_asset` |
| `currency` | string | Quote currency (e.g., "USD") | `quote_asset`, `settle_asset` |
| `expiration` | datetime | Expiry date (futures/options) | `expiry` |
| `contract_size` | float | Contract multiplier | `contract_size` |
| `min_price_increment` | float | Tick size | `tick_size` |
| `publisher_id` | int | Data publisher (equities) | Filter by == 39 for DBEQ.BASIC |
| `ts_event` | datetime | Event timestamp | `available_from_datetime` |

### Security Types Mapping

| Databento `security_type` | Our `InstrumentType` | Notes |
|---------------------------|---------------------|-------|
| `FUT` | `FUTURE` | Futures contracts |
| `STK` | `EQUITY` | Individual stocks |
| `ETF` | `EQUITY` | Exchange-traded funds |
| `IDX` | `INDEX` | Index instruments |

### Contract Size Classification

**Futures Contract Sizes** (identify mini/micro):
- **Normal**: Standard contract size (e.g., ES = 50x S&P 500)
- **Mini**: 1/5 of normal (e.g., E7 = 10x S&P 500)
- **Micro**: 1/10 of normal (e.g., MES = 5x S&P 500)

**Identification Strategy**:
1. Check symbol prefix/suffix patterns (e.g., "MES" = micro ES, "E7" = mini ES)
2. Check `contract_size` field (micro < mini < normal)
3. Use symbol mapping table if available

---

## Translation Layer Architecture

### Component Structure

```
databento_adapter.py
├── DatabentoAdapter (existing)
│   ├── fetch_instrument_definitions() (existing)
│   ├── _convert_to_instrument_definition() (needs enhancement)
│   └── NEW: Translation methods
│       ├── _map_security_type()
│       ├── _identify_contract_size_category()
│       ├── _select_most_liquid_instrument()
│       ├── _normalize_futures_symbol()
│       ├── _normalize_equity_symbol()
│       └── _build_canonical_key()
```

### Translation Flow

```
Databento API Response
    ↓
Filter by instrument_class != "S"
    ↓
Filter by publisher_id == 39 (for DBEQ.BASIC)
    ↓
Group by underlying asset
    ↓
For each underlying:
    ├─ Identify all variants (normal/mini/micro futures, ETFs)
    ├─ Select most liquid (prefer micro/mini futures or ETFs)
    └─ Convert to InstrumentDefinition
        ↓
Map security_type → InstrumentType
    ↓
Build canonical instrument_key
    ↓
Populate InstrumentDefinition fields
```

---

## Implementation Plan

### Phase 1: Core Translation Logic

**Task 1.1**: Enhance `_convert_to_instrument_definition()`
- [ ] Map `security_type` to `InstrumentType` enum
- [ ] Handle `expiration` parsing (futures/options)
- [ ] Map `asset` → `base_asset`
- [ ] Map `currency` → `quote_asset`, `settle_asset`
- [ ] Map `contract_size` → `contract_size`
- [ ] Map `min_price_increment` → `tick_size`
- [ ] Handle missing `min_price_increment` (default 0.01 for non-GLBX.MDP3)

**Task 1.2**: Symbol Normalization
- [ ] `_normalize_futures_symbol()`: Handle futures symbol formats
  - Parse expiry from symbol (e.g., "ESZ24" → expiry "2024-12")
  - Handle parent symbols vs instrument IDs
- [ ] `_normalize_equity_symbol()`: Handle equity/ETF symbols
  - Map ticker symbols (e.g., "AAPL", "SPY")
  - Handle exchange suffixes if needed

**Task 1.3**: Contract Size Classification
- [ ] `_identify_contract_size_category()`: Classify futures contracts
  - Check symbol patterns (M* = micro, E7 = mini, etc.)
  - Compare `contract_size` values
  - Return: "normal", "mini", "micro"
- [ ] Create contract size mapping table (if needed)

### Phase 2: Liquidity-Based Selection

**Task 2.1**: Instrument Variant Discovery
- [ ] Group instruments by underlying asset
- [ ] Identify all variants:
  - Normal futures
  - Mini futures
  - Micro futures
  - ETFs (if available)

**Task 2.2**: Liquidity Selection Logic
- [ ] `_select_most_liquid_instrument()`: Choose best variant
  - Priority: Micro futures > Mini futures > ETFs > Normal futures
  - Fallback: If no micro/mini, use normal or ETF
  - Consider: Contract size, trading volume (if available)

**Task 2.3**: Symbol Filtering
- [ ] Integrate `allowed_databento_symbols.csv` (if available)
- [ ] Filter symbols by exchange/dataset
- [ ] Support parent symbol filtering

### Phase 3: Venue-Specific Handling

**Task 3.1**: CME (GLBX.MDP3) - Commodities & Futures
- [ ] Handle commodity futures (Sugar, Coffee, etc.)
- [ ] Handle currency futures (G10 currencies)
- [ ] Handle equity index futures (ES, NQ, etc.)
- [ ] Support micro/mini variants (MES, MNQ, etc.)

**Task 3.2**: NASDAQ/NYSE (DBEQ.BASIC) - Equities & ETFs
- [ ] Filter by `publisher_id == 39`
- [ ] Handle individual stocks (STK)
- [ ] Handle ETFs (ETF)
- [ ] Handle S&P 500 components
- [ ] Map to `EQUITY` instrument type

**Task 3.3**: ICE (ICE.NYBOT) - Additional Commodities
- [ ] Handle ICE-specific commodities
- [ ] Map to appropriate instrument types

### Phase 4: Canonical Key Generation

**Task 4.1**: Build Canonical Keys
- [ ] `_build_canonical_key()`: Generate canonical format
  - Format: `VENUE:INSTRUMENT_TYPE:SYMBOL`
  - Examples:
    - `CME:FUTURE:ES-USD-241225` (futures with expiry)
    - `CME:FUTURE:MES-USD-241225` (micro futures)
    - `NASDAQ:EQUITY:AAPL-USD` (equities)
    - `NYSE:EQUITY:SPY-USD` (ETFs)

**Task 4.2**: Symbol Formatting
- [ ] Futures: `{ASSET}-{CURRENCY}-{EXPIRY}`
  - Expiry format: YYMMDD (e.g., "241225" for Dec 25, 2024)
- [ ] Equities: `{TICKER}-{CURRENCY}`
- [ ] Handle micro/mini prefixes in symbol

### Phase 5: Testing & Validation

**Task 5.1**: Unit Tests
- [ ] Test `_map_security_type()` with all security types
- [ ] Test `_identify_contract_size_category()` with various futures
- [ ] Test `_select_most_liquid_instrument()` selection logic
- [ ] Test `_build_canonical_key()` for all instrument types
- [ ] Test symbol normalization for each venue

**Task 5.2**: Integration Tests
- [ ] Test full translation flow with real Databento responses
- [ ] Test filtering logic (instrument_class, publisher_id)
- [ ] Test liquidity selection with multiple variants
- [ ] Validate against expected instrument universe

**Task 5.3**: Validation
- [ ] Verify canonical keys match specification
- [ ] Verify all required fields populated
- [ ] Verify instrument types correct
- [ ] Verify contract sizes accurate

---

## Symbol Mapping Reference

### Futures Symbol Patterns (CME)

| Underlying | Normal | Mini | Micro | Notes |
|------------|--------|------|-------|-------|
| S&P 500 | ES | E7 | MES | ES = 50x, E7 = 10x, MES = 5x |
| Nasdaq 100 | NQ | NQ7 | MNQ | NQ = 20x, NQ7 = 2x, MNQ = 2x |
| Gold | GC | - | MGC | GC = 100 oz, MGC = 10 oz |
| Crude Oil | CL | - | MCL | CL = 1000 bbl, MCL = 100 bbl |
| Natural Gas | NG | - | MNG | NG = 10000 MMBtu, MNG = 2500 MMBtu |

### Commodity Futures (CME)

| Commodity | Symbol | Contract Size | Preferred Variant |
|-----------|--------|---------------|-------------------|
| Sugar | SB | 112,000 lbs | Micro if available |
| Coffee | KC | 37,500 lbs | Micro if available |
| Cotton | CT | 50,000 lbs | Micro if available |
| Cocoa | CC | 10 metric tons | Micro if available |
| Orange Juice | OJ | 15,000 lbs | Micro if available |
| Soybeans | ZS | 5,000 bushels | Micro if available |

### Currency Futures (CME)

| Currency Pair | Symbol | Contract Size | Preferred Variant |
|---------------|--------|---------------|-------------------|
| EUR/USD | 6E | 125,000 EUR | Micro (M6E) |
| GBP/USD | 6B | 62,500 GBP | Micro (M6B) |
| JPY/USD | 6J | 12,500,000 JPY | Micro (M6J) |
| AUD/USD | 6A | 100,000 AUD | Micro (M6A) |

### Equity ETFs (NASDAQ/NYSE)

| ETF | Symbol | Underlying |
|-----|--------|------------|
| S&P 500 ETF | SPY | S&P 500 Index |
| Nasdaq 100 ETF | QQQ | Nasdaq 100 Index |
| Gold ETF | GLD | Gold |
| Oil ETF | USO | Crude Oil |

---

## Configuration Files Needed

### 1. `allowed_databento_symbols.csv` (if available)

Columns:
- `parent_symbol`: Parent symbol (e.g., "ES", "AAPL")
- `databento`: Dataset (e.g., "GLBX.MDP3", "DBEQ.BASIC")
- `instrument_type`: Type (future, equity, etf)
- `contract_size_category`: normal, mini, micro
- `preferred`: Boolean (prefer this variant)

### 2. `exchange_mappings_global.json` (if available)

Structure:
```json
{
  "cme": {
    "databento": "GLBX.MDP3",
    "types": ["future"],
    "depot": "cme"
  },
  "nasdaq": {
    "databento": "DBEQ.BASIC",
    "types": ["equity", "etf"],
    "depot": "nasdaq"
  }
}
```

### 3. Contract Size Mapping (new)

```python
CONTRACT_SIZE_MAPPING = {
    "CME": {
        "ES": {"normal": 50, "mini": 10, "micro": 5},
        "NQ": {"normal": 20, "mini": 2, "micro": 2},
        "GC": {"normal": 100, "micro": 10},
        # ... more mappings
    }
}
```

---

## Implementation Checklist

### Core Translation
- [ ] Map `security_type` → `InstrumentType`
- [ ] Parse `expiration` for futures
- [ ] Map Databento fields to `InstrumentDefinition`
- [ ] Handle missing fields (defaults)

### Symbol Handling
- [ ] Normalize futures symbols (expiry parsing)
- [ ] Normalize equity symbols (ticker mapping)
- [ ] Handle parent symbols vs instrument IDs
- [ ] Support symbol filtering (if CSV available)

### Contract Size & Liquidity
- [ ] Identify contract size category (normal/mini/micro)
- [ ] Group instruments by underlying
- [ ] Select most liquid variant
- [ ] Prefer micro/mini futures or ETFs

### Venue-Specific
- [ ] CME: Commodities, currencies, equity index futures
- [ ] NASDAQ/NYSE: Equities, ETFs, S&P 500 components
- [ ] ICE: Additional commodities
- [ ] Publisher filtering (DBEQ.BASIC)

### Canonical Keys
- [ ] Generate canonical keys for all instrument types
- [ ] Format futures with expiry
- [ ] Format equities/ETFs
- [ ] Handle micro/mini prefixes

### Testing
- [ ] Unit tests for translation methods
- [ ] Integration tests with real data
- [ ] Validation against instrument universe
- [ ] Edge case handling

---

## Next Steps

1. **Review Databento API responses** to understand exact field formats
2. **Create symbol mapping tables** for contract size identification
3. **Implement core translation logic** (Phase 1)
4. **Add liquidity selection** (Phase 2)
5. **Test with real Databento data** (Phase 5)
6. **Validate against STATUS.md requirements**

---

## References

- **STATUS.md**: TradFi requirements (lines 237-277)
- **Archive Code**: `archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py`
- **Databento Docs**: Context7 library docs
- **Canonical Spec**: `docs/INSTRUMENT_VENUE_SPECIFICATION.md`
- **Models**: `instruments_service/models.py` (InstrumentDefinition, InstrumentType, Venue)

