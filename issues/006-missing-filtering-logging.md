# Issue 006: Missing Filtering Logging

**Status**: RESOLVED
**Severity**: LOW
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

Date filtering was occurring but there was no logging to show which instruments were being filtered and why. This made it difficult to debug why certain instruments (like Deribit instruments) were being filtered out.

## Root Cause

The date filtering logic in `fetch_exchange_instruments` filtered instruments but didn't log:
- How many instruments were filtered
- Sample of filtered instruments
- Reason for filtering (availableFrom > target_date, etc.)

## Impact

- **Debugging Difficulty**: Hard to understand why instruments were filtered
- **Lack of Visibility**: No way to see what was being dropped
- **User Confusion**: Users couldn't see why their expected instruments weren't in the output

## Solution

Added comprehensive logging for date filtering:

1. **Filtering Statistics**: Log count of filtered vs available instruments
2. **Sample Filtered Instruments**: Log up to 5 sample instruments that were filtered with their `availableFrom` dates
3. **Filtering Breakdown**: Include `date_filtered` count in the final filtering breakdown

**Files Changed**:
- `instruments_service/app/core/instrument_processing_service.py` (lines 459-489)

**Example Log Output**:
```
📅 Date filter: 2843/300824 instruments available on 2024-10-30 (removed 297981)
📋 Sample filtered instruments (availableFrom > 2024-10-30):
   BTC-6NOV25-97000-P: availableFrom=2025-11-05T00:00:00.000Z
   BTC-28NOV25-101000-P: availableFrom=2025-11-05T00:00:00.000Z
```

## Prevention

1. **Always Log Filtering**: When filtering data, log what's being filtered and why
2. **Sample Output**: Show samples of filtered items to help users understand what's happening
3. **Statistics**: Include filtering statistics in final summary
