# Issue 004: Redundant Date Filtering in Multiple Places

**Status**: RESOLVED
**Severity**: MEDIUM
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

Date filtering (`availableSince`/`availableTo`) was happening in both `fetch_exchange_instruments` and `process_exchange_instruments`, causing redundant filtering and making it unclear where the actual filtering occurred.

## Root Cause

The date filtering logic was duplicated:
1. In `fetch_exchange_instruments`: Filtered by `availableSince`/`availableTo` immediately after fetching from Tardis
2. In `process_exchange_instruments`: Had additional date filtering logic (though this was later removed)

This redundancy made it:
- Hard to track where filtering actually occurred
- Difficult to debug filtering issues
- Unclear which filtering stats were accurate

## Impact

- **Code Clarity**: Unclear where date filtering actually happened
- **Performance**: Redundant filtering (though minimal impact)
- **Debugging**: Hard to track filtering statistics accurately

## Solution

Removed redundant date filtering from `process_exchange_instruments` and ensured all date filtering happens in `fetch_exchange_instruments`:

1. **Centralized Filtering**: All `availableSince`/`availableTo` filtering now happens in `fetch_exchange_instruments`
2. **Return Filtering Stats**: `fetch_exchange_instruments` now returns `date_filtered_count` as part of the tuple
3. **Clear Separation**: `process_exchange_instruments` only handles expiry filtering for futures/options

**Files Changed**:
- `instruments_service/app/core/instrument_processing_service.py`
  - `fetch_exchange_instruments`: Returns `(instruments_data, date_filtered_count)`
  - `process_exchange_instruments`: Removed redundant date filtering, uses `date_filtered_count` from fetch

## Prevention

1. **Single Responsibility**: Each function should have one clear responsibility
2. **Filter Early**: Filter as early as possible (in `fetch_exchange_instruments`) to avoid processing unnecessary data
3. **Clear Stats**: Return filtering statistics from the function that does the filtering
