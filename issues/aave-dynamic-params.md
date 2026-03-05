# Replace Static AAVE Risk Parameters with Dynamic Fetching

## Issue Summary

The AAVE adapter currently uses `STATIC_RISK_PARAMS` for eMode and standard risk parameters (LTV, liquidation thresholds, liquidation bonus, reserve factors). These static values should be replaced with dynamic fetching from AAVE contracts via RPC or The Graph subgraph.

## Current Implementation

- **Location**: `instruments_service/app/venues/defi/aave_adapter.py`
- **Static Parameters**: Defined in `STATIC_RISK_PARAMS` class variable (lines ~41-82)
- **Usage**: Used in `_extract_lending_metadata()` method when RPC/Graph calls are skipped

## Problem

1. **Static values can become stale**: AAVE protocol parameters can change over time via governance
2. **Limited coverage**: Only covers MVP tokens (weETH, wstETH, WETH, USDT)
3. **No historical accuracy**: Static values don't reflect historical parameter changes
4. **Maintenance burden**: Requires manual updates when AAVE parameters change

## Proposed Solution

### Phase 1: Enable Dynamic eMode Category Fetching

1. **Re-enable `_fetch_emode_category_from_rpc()` method**:
   - Fetch eMode category details from AAVE Pool contract `getEModeCategoryData()` function
   - Support both current and historical queries (via block number)
   - Cache results to avoid repeated RPC calls

2. **Re-enable `_fetch_emode_category_from_graph()` method**:
   - Fetch eMode category details from The Graph subgraph as fallback
   - Support historical queries via block number
   - Use when RPC is unavailable

3. **Update `_extract_lending_metadata()` method**:
   - Remove the "OPTIMIZATION: Skip RPC/Graph calls" comment
   - Call `_fetch_emode_category_from_rpc()` or `_fetch_emode_category_from_graph()` when `emode_category_id` is present
   - Fall back to `STATIC_RISK_PARAMS` only when both RPC and Graph fail

### Phase 2: Enable Dynamic Reserve Configuration Fetching

1. **Enhance `_fetch_reserve_config_from_graph()` method**:
   - Already implemented and working
   - Ensure it's used for all reserves (not just when Graph is available)

2. **Add RPC-based reserve configuration fetching**:
   - Extract LTV, liquidation threshold, liquidation bonus from reserve configuration bitmap
   - Use AAVE Pool contract `getReserveData()` function
   - Parse configuration bitmap according to AAVE V3 ReserveConfiguration.sol

3. **Update `_extract_lending_metadata()` method**:
   - Use RPC/Graph fetched values as primary source
   - Fall back to `STATIC_RISK_PARAMS` only when both fail

### Phase 3: Remove Static Parameters

1. **Keep `STATIC_RISK_PARAMS` as final fallback**:
   - Only use when both RPC and Graph are unavailable
   - Log warnings when static fallback is used
   - Consider removing entirely if RPC/Graph reliability improves

## Implementation Details

### RPC Method: `getEModeCategoryData(uint8 id)`

Returns:
- `uint16 ltv` - Loan-to-value ratio (in basis points, divide by 10000)
- `uint16 liquidationThreshold` - Liquidation threshold (in basis points)
- `uint16 liquidationBonus` - Liquidation bonus (in basis points)
- `address priceSource` - Price source address
- `string label` - Category label (e.g., "ETH_CORRELATION")

### RPC Method: `getReserveData(address asset)`

Returns tuple with:
- `configuration` (uint256) - Configuration bitmap containing:
  - LTV (bits 0-15)
  - Liquidation threshold (bits 16-31)
  - Liquidation bonus (bits 32-47)
  - Reserve factor (bits 64-79)
  - eMode category ID (bits 168-175)

### GraphQL Query: EModeCategory

```graphql
query GetEModeCategory($categoryId: Int!, $blockNumber: Int) {
    eModeCategories(where: { id: $categoryId }, block: { number: $blockNumber }) {
        id
        label
        liquidationThreshold
        liquidationBonus
        priceSource
        oracleId
    }
}
```

## Testing Requirements

1. **Unit Tests**:
   - Test RPC-based eMode category fetching
   - Test Graph-based eMode category fetching
   - Test fallback to static parameters when both fail
   - Test historical queries (with block numbers)

2. **Integration Tests**:
   - Test full instrument generation with dynamic parameters
   - Verify parameters match AAVE protocol values
   - Test performance (RPC calls should be cached)

3. **Validation**:
   - Compare fetched values against AAVE protocol documentation
   - Verify historical accuracy for past dates
   - Monitor RPC/Graph success rates

## Performance Considerations

- **Caching**: Cache eMode category data per category ID (current data only)
- **Batch Queries**: Consider batching RPC calls for multiple reserves
- **Rate Limiting**: Respect RPC provider rate limits (Alchemy)
- **Fallback Strategy**: Use Graph when RPC fails, static when both fail

## Related Code

- **Removed Methods** (2025-12-04):
  - `_fetch_reserves_from_rpc()` - Always returned empty, never called
  - `_fetch_emode_category_from_rpc()` - Never called (removed as dead code)
  - `_fetch_emode_category_from_graph()` - Never called (removed as dead code)

- **Active Methods**:
  - `_fetch_reserve_config_from_graph()` - Working, used for reserve configuration
  - `_fetch_reserve_emode_from_rpc()` - Working, used for eMode category ID extraction
  - `_extract_lending_metadata()` - Uses STATIC_RISK_PARAMS, needs update

## References

- AAVE V3 Core Contracts: https://github.com/aave/aave-v3-core
- ReserveConfiguration.sol: https://github.com/aave/aave-v3-core/blob/master/contracts/protocol/libraries/configuration/ReserveConfiguration.sol
- AAVE V3 Ethereum Subgraph: https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g
- AAVE V3 Pool Contract: `0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2` (Ethereum)

## Status

- **Created**: 2025-12-04
- **Priority**: Medium
- **Estimated Effort**: 2-3 days
- **Dependencies**: None
