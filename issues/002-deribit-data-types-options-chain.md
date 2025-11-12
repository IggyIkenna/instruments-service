# Issue 002: Deribit Data Types Not Using Only options_chain

**Status**: RESOLVED
**Severity**: HIGH
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

Deribit instruments were using instrument-type-based data types instead of only `options_chain` as specified in the documentation. This resulted in:
- PERPETUAL instruments having: `trades,book_snapshot_5,derivative_ticker,liquidations`
- FUTURE instruments having: `trades,book_snapshot_5,derivative_ticker,liquidations`
- OPTION instruments having: `options_chain` (correct)
- Some instruments having mixed data types: `trades,book_snapshot_5,options_chain,derivative_ticker,liquidations`

## Root Cause

The data types were set based on `instrument_type` from the config (`DataTypeConfig.instrument_data_types`), which maps:
- `OPTION` → `['options_chain']`
- `PERPETUAL` → `['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']`
- `FUTURE` → `['trades', 'book_snapshot_5', 'derivative_ticker', 'liquidations']`

However, per documentation, **all Deribit instruments** (regardless of type) should use only `options_chain`.

## Impact

- **Data Consistency**: Deribit instruments had inconsistent data types
- **Documentation Mismatch**: Code didn't match documented behavior
- **Downstream Systems**: Systems expecting only `options_chain` for Deribit would receive incorrect data types

## Solution

Modified the data type assignment logic to check venue first before falling back to instrument-type-based config:

```python
# BEFORE (incorrect):
config_data_types = self.data_config.instrument_data_types.get(
    normalized_instrument_type or 'SPOT_PAIR',
    ['trades', 'book_snapshot_5']
)

# AFTER (correct):
if canonical_venue == 'DERIBIT':
    config_data_types = ['options_chain']
else:
    config_data_types = self.data_config.instrument_data_types.get(
        normalized_instrument_type or 'SPOT_PAIR',
        ['trades', 'book_snapshot_5']
    )
```

**Files Changed**:
- `instruments_service/app/core/instrument_processing_service.py` (lines 653-663, 842-849)

## Verification

After fix:
- All 2512 Deribit instruments now have only `options_chain` as data type
- Applies to all instrument types (PERPETUAL, FUTURE, OPTION)

## Prevention

1. **Venue-Specific Overrides**: When venue-specific rules exist, check venue before instrument type
2. **Documentation Alignment**: Ensure code matches documented behavior
3. **Validation**: Add validation to check venue-specific data type rules
