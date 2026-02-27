# Schema Validation - Instruments Service

This document describes the strict schema validation framework for GCS parquet outputs in the instruments-service.

## Overview

The instruments-service enforces strict schemas on all parquet outputs before uploading to GCS. The schema supports **dimension-aware nullability** - certain fields are required for specific categories (CEFI, TRADFI, DEFI) but optional for others.

**Key Principles:**
- All parquet outputs are validated against predefined schemas before GCS upload
- Files failing validation are logged as ERROR and skipped (fail-soft)
- Dimension-aware nullability: `tardis_exchange` required for CEFI, `databento_symbol` required for TRADFI

## Dimension-Aware Nullability

The instruments schema uses `nullable_overrides` to specify different nullability rules per category:

| Column | Default | CEFI | TRADFI | DEFI |
|--------|---------|------|--------|------|
| `tardis_exchange` | Nullable | **Required** | Nullable | Nullable |
| `databento_symbol` | Nullable | Nullable | **Required** | Nullable |
| `trading_hours_open` | Nullable | Nullable | **Required** | Nullable |
| `trading_hours_close` | Nullable | Nullable | **Required** | Nullable |

## Output Schema: `instruments`

### Required Core Fields (Always NOT NULL)

| Column | Type | Description |
|--------|------|-------------|
| `instrument_key` | string | Canonical key: VENUE:INSTRUMENT_TYPE:SYMBOL |
| `venue` | string | Venue identifier (BINANCE-FUTURES, CME, etc.) |
| `instrument_type` | string | Type (SPOT_PAIR, PERPETUAL, FUTURE, OPTION, etc.) |
| `symbol` | string | Symbol extracted from instrument_key |
| `available_from_datetime` | datetime64[ns] | When instrument became available |
| `timestamp` | datetime64[ns] | Generation timestamp |

### Metadata Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `venue_type` | string | Type: 'exchange', 'protocol', or 'wallet' |
| `data_provider` | string | Source: 'tardis' or 'databento' |
| `asset_class` | string | Class: 'crypto' or 'traditional' |
| `data_types` | string | Comma-separated available data types |
| `available_to_datetime` | datetime64[ns] | Expiry (None for SPOT/PERPETUAL) |

### Asset Information (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `base_asset` | string | Base asset symbol (BTC, ETH) |
| `quote_asset` | string | Quote asset symbol (USDT, USD) |
| `settle_asset` | string | Settlement asset symbol |

### Exchange-Specific Identifiers

| Column | Type | Nullable | Nullable Override | Description |
|--------|------|----------|-------------------|-------------|
| `exchange_raw_symbol` | string | Yes | - | Raw exchange code |
| `databento_symbol` | string | Yes | TRADFI: **No** | Databento query symbol |
| `tardis_exchange` | string | Yes | CEFI: **No** | Tardis exchange ID |
| `tardis_symbol` | string | Yes | - | Tardis API symbol |

### Trading Parameters (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `inverse` | bool | Whether inverse contract |
| `tick_size` | string | Minimum price increment |
| `min_size` | string | Minimum order size |

### Option-Specific Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `strike` | string | Strike price |
| `option_type` | string | CALL or PUT |

### Contract-Specific Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `expiry` | datetime64[ns] | Expiry datetime |
| `contract_size` | float64 | Contract size/multiplier |
| `underlying` | string | Underlying asset |

### CCXT Integration Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `ccxt_symbol` | string | CCXT library symbol format |
| `ccxt_exchange` | string | CCXT exchange identifier |

### DeFi-Specific Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `base_asset_contract_address` | string | ERC-20 contract address |
| `quote_asset_contract_address` | string | Quote asset contract |
| `pool_address` | string | Pool contract address |
| `pool_fee_tier` | int64 | Pool fee in basis points |

### Lending Protocol Fields (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `flash_loan_providers` | string | Flash loan provider addresses |
| `ltv` | float64 | Loan-to-Value ratio |
| `liquidation_threshold` | float64 | Liquidation threshold |
| `liquidation_bonus` | float64 | Liquidation bonus |
| `reserve_factor` | float64 | Reserve factor |
| `emode_category_id` | int64 | E-mode category ID |
| `emode_label` | string | E-mode category label |
| `optimal_utilization_rate` | float64 | Optimal utilization |
| `base_variable_borrow_rate` | float64 | Base borrow rate |
| `variable_rate_slope1` | float64 | Rate slope 1 |
| `variable_rate_slope2` | float64 | Rate slope 2 |

### CEFI Risk Parameters (Nullable)

| Column | Type | Description |
|--------|------|-------------|
| `max_position_size` | float64 | Max position in quote currency |
| `max_leverage` | float64 | Maximum leverage |
| `initial_margin_rate` | float64 | Initial margin rate |
| `maintenance_margin_rate` | float64 | Maintenance margin rate |
| `leverage_tiers_json` | string | JSON of leverage tiers |

### TradFi Trading Hours (Required for TRADFI)

| Column | Type | Nullable | Nullable Override | Description |
|--------|------|----------|-------------------|-------------|
| `trading_hours_open` | string | Yes | TRADFI: **No** | Trading hours open |
| `trading_hours_close` | string | Yes | TRADFI: **No** | Trading hours close |
| `trading_session` | string | Yes | - | Session identifier |
| `is_trading_day` | bool | Yes | - | Trades on given date |
| `holiday_calendar` | string | Yes | - | Holiday calendar ID |

## Usage

```python
from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA
from unified_trading_services import ParquetSchemaEnforcer

enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)

# Validate CEFI instruments (tardis_exchange required)
result = enforcer.validate_dataframe(df, {"category": "CEFI"})

# Validate TRADFI instruments (databento_symbol, trading_hours required)
result = enforcer.validate_dataframe(df, {"category": "TRADFI"})

if not result.valid:
    for error in result.errors:
        print(f"Validation error: {error}")
```

## GCS Output Paths

```
gs://instruments-{domain}-{project_id}/
├── instruments/
│   ├── category=CEFI/
│   │   └── instruments.parquet
│   ├── category=TRADFI/
│   │   └── instruments.parquet
│   └── category=DEFI/
│       └── instruments.parquet
```
