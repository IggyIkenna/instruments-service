# Issue 003: Date Comparison Using Datetime Instead of Date Objects

**Status**: RESOLVED
**Severity**: MEDIUM
**Date Found**: 2025-11-06
**Date Resolved**: 2025-11-06

## Description

The `_is_instrument_available_on_date` function was comparing `datetime` objects directly, which could lead to issues with time components and timezones. For day-level filtering, we should compare `date` objects to ensure that if an instrument is available at any point during a calendar day, it's considered available for that entire day.

## Root Cause

The function was parsing ISO datetime strings and comparing them directly:
```python
target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
since_date = datetime.fromisoformat(available_since.replace('Z', '+00:00'))
if target_date < since_date:  # Comparing datetime objects
```

This could cause issues when:
- `availableSince` is `2024-10-30T23:59:59.999Z`
- `target_date` is `2024-10-30T00:00:00.000Z`
- The comparison would incorrectly filter out the instrument even though it was available on that day

## Impact

- **False Negatives**: Instruments available on a given day might be incorrectly filtered out
- **Timezone Issues**: Time components could cause incorrect filtering across timezones
- **Day-Level Semantics**: The intent is "available on date X", not "available at exact time"

## Solution

Extract `date` objects and compare those for day-level filtering:

```python
# BEFORE (incorrect):
target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
since_date = datetime.fromisoformat(available_since.replace('Z', '+00:00'))
if target_date < since_date:  # datetime comparison

# AFTER (correct):
target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
target_date_only = target_date.date()
since_date = datetime.fromisoformat(available_since.replace('Z', '+00:00'))
since_date_only = since_date.date()
if target_date_only < since_date_only:  # date comparison
```

**Files Changed**:
- `instruments_service/app/core/instrument_processing_service.py` (function `_is_instrument_available_on_date`)

## Prevention

1. **Day-Level Semantics**: When filtering by date, always use `date` objects, not `datetime`
2. **Clear Intent**: Document that date filtering is day-level, not time-level
3. **Consistent Patterns**: Use the same pattern throughout the codebase for date comparisons
