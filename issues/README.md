# Issues Tracker

This directory tracks all issues found and resolved during development and testing of the instruments-service.

## Issue Format

Each issue file follows this format:
- **Issue ID**: Sequential number (e.g., `001`)
- **Title**: Brief description
- **Status**: `RESOLVED`, `OPEN`, `WONTFIX`
- **Severity**: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`
- **Date Found**: When issue was discovered
- **Date Resolved**: When issue was fixed
- **Description**: Detailed description of the issue
- **Root Cause**: Why the issue occurred
- **Solution**: How it was fixed
- **Prevention**: How to avoid this in the future

## Index

- [001: Date Filtering Bypassed with --force Flag](./001-date-filtering-bypassed-force-flag.md) - RESOLVED
- [002: Deribit Data Types Not Using Only options_chain](./002-deribit-data-types-options-chain.md) - RESOLVED
- [003: Date Comparison Using Datetime Instead of Date Objects](./003-date-comparison-datetime-vs-date.md) - RESOLVED
- [004: Redundant Date Filtering in Multiple Places](./004-redundant-date-filtering.md) - RESOLVED
- [005: Binance Venue Naming Inconsistency](./005-binance-venue-naming.md) - RESOLVED
- [006: Missing Filtering Logging](./006-missing-filtering-logging.md) - RESOLVED
- [007: Undefined Variable `inst_type`](./007-undefined-variable-inst-type.md) - RESOLVED

## Technical Decisions

Technical decisions are documented in:
- [`docs/SERVICE_OVERVIEW.md`](../docs/SERVICE_OVERVIEW.md) - Key Design Decisions section
- [`docs/INSTRUMENT_ENRICHMENT_PROPOSAL.md`](../docs/INSTRUMENT_ENRICHMENT_PROPOSAL.md) - Architecture patterns
