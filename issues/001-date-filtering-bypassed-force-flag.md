# Issue 001: Date Filtering Bypassed with --force Flag

**Status**: RESOLVED
**Severity**: CRITICAL
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

When using the `--force` flag, date filtering was completely bypassed, allowing instruments with future `availableFrom` dates to be included in the output CSV for historical dates. This resulted in 5350 instruments with `availableFrom > 2024-10-30` being incorrectly included when processing date `2024-10-30`.

## Root Cause

The date filtering logic in `fetch_exchange_instruments` checked `if not force:` before applying date filtering. This meant that when `force=True`, the code would skip all `availableSince`/`availableTo` filtering and only filter out expired instruments (using `_is_instrument_currently_active`).

The misunderstanding was that `force` mode should mean "force regeneration even if files exist", not "ignore date filtering for historical accuracy".

## Impact

- **Data Quality**: Historical date processing included instruments that weren't available on that date
- **Downstream Systems**: Systems consuming this data would attempt to download data for instruments that didn't exist on the target date
- **CSV Validation**: Generated CSVs contained incorrect data (5350 instruments with future dates)

## Solution

Modified `fetch_exchange_instruments` to always apply date filtering when `target_date` is provided, regardless of the `force` flag:

```python
# BEFORE (incorrect):
if not force:
    # Apply date filtering
else:
    # Skip date filtering - only filter expired

# AFTER (correct):
if target_date:
    # ALWAYS apply date filtering when target_date is provided
    # Force mode only affects whether we regenerate existing files
else:
    # No target_date: filter out expired instruments only
```

**Files Changed**:
- `instruments_service/app/core/instrument_processing_service.py` (lines 452-526)

## Prevention

1. **Clear Flag Semantics**: Document that `--force` means "force regeneration", not "ignore filtering"
2. **Date Filtering Always Applies**: When processing historical dates, date filtering should always apply regardless of flags
3. **Validation**: Add CSV validation to check that no instruments have `availableFrom > target_date`

## Related Issues

- Issue 003: Date comparison logic improvements
- Issue 004: Redundant date filtering removal
