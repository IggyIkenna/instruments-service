# Issue 005: Binance Venue Naming Inconsistency

**Status**: RESOLVED
**Severity**: HIGH
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

Binance spot instruments were being labeled as "BINANCE" in the output CSV, but the canonical specification requires "BINANCE-SPOT" for spot instruments. This caused:
- Incorrect venue names in generated CSVs
- Mismatch with canonical specification
- Warning: "Unknown venue in instrument key: BINANCE-SPOT"

## Root Cause

The venue mapping in `config.py` had:
- `'binance': 'BINANCE'` in `tardis_to_venue` mapping
- `'BINANCE': 'binance'` in `venue_to_ccxt` mapping

But the canonical specification requires:
- Spot: `BINANCE-SPOT`
- Futures: `BINANCE-FUTURES`

## Impact

- **Data Quality**: Incorrect venue names in generated instruments
- **Specification Mismatch**: Doesn't match canonical instrument ID specification
- **Downstream Systems**: Systems expecting `BINANCE-SPOT` would receive `BINANCE`

## Solution

Updated all venue mappings to use `BINANCE-SPOT` for spot instruments:

**Files Changed**:
- `instruments_service/config.py`:
  - `VenueMapping.venue_to_ccxt`: Changed `'BINANCE': 'binance'` → `'BINANCE-SPOT': 'binance'`
  - `VenueMapping.tardis_to_venue`: Changed `'binance': 'BINANCE'` → `'binance': 'BINANCE-SPOT'`
  - `VenueMapping.venue_instrument_type_to_tardis`: Updated all entries to use `'BINANCE-SPOT'`
  - `ExchangeInstrumentConfig.exchange_instrument_types`: Changed `'BINANCE'` → `'BINANCE-SPOT'`
  - `ExchangeInstrumentConfig.valid_quote_currencies`: Changed `'BINANCE'` → `'BINANCE-SPOT'`

- `instruments_service/models.py`:
  - `Venue` enum: Changed `BINANCE = "BINANCE"` → `BINANCE_SPOT = "BINANCE-SPOT"`

- `instruments_service/app/core/instrument_processing_service.py`:
  - `get_venue_mapping`: Updated `'binance': 'BINANCE'` → `'binance': 'BINANCE-SPOT'`

## Verification

After fix:
- All Binance spot instruments now have venue `BINANCE-SPOT`
- No more "Unknown venue" warnings
- Matches canonical specification

## Prevention

1. **Canonical Specification**: Always refer to canonical specification when making venue naming decisions
2. **Consistent Mappings**: Ensure all venue mappings are consistent across config files
3. **Validation**: Add validation to check venue names against canonical specification
