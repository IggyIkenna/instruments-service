# Codex Violations Manifest: instruments-service

**Total Violations**: 143

## Summary

- ❌ **60 print() statements** (use `logger.info()`)
- ❌ **6 os.getenv() calls** (use config classes)
- ❌ **1 datetime.now() calls** (use UTC)
- ❌ **requests library in async code** (use `aiohttp`)
- ❌ **4 asyncio.run() in loops**
- ❌ **2 time.sleep() in async**
- ❌ **7 files >1500 lines**
- ❌ **60 imports inside functions**

---

## Detailed Violations

### 1. Print Statements (use logger.info())

Replace `print()` with `logger.info()` or appropriate logging level.

```
./pytest_load_env.py:47:                            print(f"⚠️  Credentials file not found: {creds_path}")
./pytest_load_env.py:48:                            print(f"   Checked: {abs_creds_path}")
./pytest_load_env.py:49:                            print(f"   Checked: {parent_creds}")
./pytest_load_env.py:51:                    print(f"⚠️  Credentials file not found at absolute path: {creds_path}")
./pytest_load_env.py:64:            print(f"✅ Loaded .env from {env_path}")
./pytest_load_env.py:65:            print(f"   GOOGLE_APPLICATION_CREDENTIALS={final_creds}")
./pytest_load_env.py:66:            print(f"   Credentials file exists: {creds_exists}")
./pytest_load_env.py:68:            print(f"⚠️  .env file not found at {env_path}")
./pytest_load_env.py:70:        print("⚠️  python-dotenv not available, skipping .env file loading")
./pytest_load_env.py:72:        print(f"⚠️  Error loading .env file: {e}")
./examples/query_instruments.py:26:    print("=" * 60)
./examples/query_instruments.py:27:    print("Example: Query Instruments for Date")
./examples/query_instruments.py:28:    print("=" * 60)
./examples/query_instruments.py:37:    print(f"\n✅ Retrieved {len(instruments_df)} instruments for {date}")
./examples/query_instruments.py:39:        print("\nSample instruments:")
./examples/query_instruments.py:40:        print(instruments_df[["instrument_key", "venue", "instrument_type", "symbol"]].head())
./examples/query_instruments.py:47:    print("\n" + "=" * 60)
./examples/query_instruments.py:48:    print("Example: Query Instruments with Filters")
./examples/query_instruments.py:49:    print("=" * 60)
./examples/query_instruments.py:61:    print(f"\n✅ Retrieved {len(instruments_df)} BTC-USDT perpetuals")
./examples/query_instruments.py:63:        print("\nInstruments:")
./examples/query_instruments.py:64:        print(instruments_df[["instrument_key", "symbol", "base_asset", "quote_asset"]].head())
./examples/query_instruments.py:71:    print("\n" + "=" * 60)
./examples/query_instruments.py:72:    print("Example: Get Instrument Details")
./examples/query_instruments.py:73:    print("=" * 60)
./examples/query_instruments.py:81:        print(f"\n✅ Found instrument: {instrument_id}")
./examples/query_instruments.py:82:        print("\nDetails:")
./examples/query_instruments.py:85:                print(f"  {key}: {value}")
./examples/query_instruments.py:87:        print(f"\n⚠️ Instrument not found: {instrument_id}")
./examples/query_instruments.py:94:    print("\n" + "=" * 60)
./examples/query_instruments.py:95:    print("Example: Summary Statistics")
./examples/query_instruments.py:96:    print("=" * 60)
./examples/query_instruments.py:102:    print("\n✅ Summary statistics for 2023-05-23:")
./examples/query_instruments.py:103:    print(f"  Total instruments: {stats.get('total_instruments', 0)}")
./examples/query_instruments.py:104:    print(f"  Venues: {stats.get('venues', 0)}")
./examples/query_instruments.py:105:    print(f"  Instrument types: {stats.get('instrument_types', 0)}")
./examples/query_instruments.py:108:        print("\n  Venue breakdown:")
./examples/query_instruments.py:110:            print(f"    {venue}: {count}")
./examples/query_instruments.py:117:    print("\n" + "=" * 60)
./examples/query_instruments.py:118:    print("Example: Instruments by Data Type")
./examples/query_instruments.py:119:    print("=" * 60)
./examples/query_instruments.py:128:    print(f"\n✅ Found {len(instruments_df)} instruments with liquidations data")
./examples/query_instruments.py:130:        print("\nSample instruments:")
./examples/query_instruments.py:131:        print(instruments_df[["instrument_key", "symbol", "data_types"]].head())
./examples/query_instruments.py:138:    print("\n" + "=" * 60)
./examples/query_instruments.py:139:    print("Example: Query Instruments Across Date Range")
./examples/query_instruments.py:140:    print("=" * 60)
./examples/query_instruments.py:151:    print(f"\n✅ Retrieved {len(instruments_df)} unique instruments across date range")
./examples/query_instruments.py:153:        print("\nSample instruments:")
./examples/query_instruments.py:154:        print(instruments_df[["instrument_key", "venue", "symbol"]].head())
... and 10 more
```

### 2. os.getenv() Usage (use config classes)

Replace `os.getenv()` with proper config classes extending `UnifiedCloudServicesConfig`.

```
./pytest_load_env.py:33:            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
./pytest_load_env.py:54:            if not os.getenv("GCP_PROJECT_ID"):
./pytest_load_env.py:58:            if not os.getenv("ENVIRONMENT"):
./pytest_load_env.py:62:            final_creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
./instruments_service/cli/handlers/instrument_handler.py:34:DEPLOYMENT_ID = os.getenv("DEPLOYMENT_ID", "")
./instruments_service/cli/handlers/instrument_handler.py:35:SHARD_LAUNCHED_AT = os.getenv("SHARD_LAUNCHED_AT", "")
```

### 3. datetime.now() Without UTC

Replace `datetime.now()` with `datetime.now(timezone.utc)`.

```
./instruments_service/app/core/cloud_instrument_storage.py:159:            # IMPORTANT: Use the target date parameter, NOT datetime.now(), to avoid TIMESTAMP_DATE_MISMATCH
```

### 5. requests Library in Async Code

Replace `requests` with `aiohttp` for async HTTP calls.

```
./download_sample_data.py:13:import requests
./instruments_service/app/venues/onchain_perps/aster_adapter.py:14:import requests
./instruments_service/app/venues/defi/morpho_adapter.py:16:import requests
```

### 6. asyncio.run() in Loops

Use `await` instead of `asyncio.run()` inside async functions.

```
./examples/batch_generation.py:170:    result = asyncio.run(
./instruments_service/cli/handlers/instrument_handler.py:272:                result = asyncio.run(
./instruments_service/app/venues/defi/uniswapv2_adapter.py:109:            pairs = asyncio.run(self._fetch_pairs(base_currency=base_currency, min_liquidity=min_liquidity or 100000))
./instruments_service/app/venues/defi/uniswapv4_adapter.py:129:            pools = asyncio.run(self._fetch_pools(base_currency=base_currency, min_tx_count=1000))
```

### 7. time.sleep() in Async Functions

Replace `time.sleep()` with `await asyncio.sleep()`.

```
./instruments_service/corporate_actions/adapter.py:85:            time.sleep(delay - elapsed)
./instruments_service/cli/handlers/corporate_actions_production_handler.py:191:        time.sleep(REQUEST_DELAY_MS / 1000.0)
```

### 8. Files >1500 Lines (COD-SIZE)

Split these files into smaller modules following Single Responsibility Principle.

- instruments_service/config.py (1929 lines)
- instruments_service/app/core/instrument_processing_service.py (2431 lines)
- instruments_service/app/venues/defi/aave_adapter.py (2018 lines)
- instruments_service/app/venues/defi/aave/v3_adapter.py (1911 lines)
- instruments_service/config/venue_config.py (1583 lines)
- build/lib/instruments_service/app/venues/databento/databento_adapter.py (2149 lines)
- build/lib/instruments_service/app/venues/defi/aave_adapter.py (1976 lines)

### 9. Imports Inside Functions

Move all imports to the top of the file.

```
pytest_load_env.py:20: from dotenv import load_dotenv
instruments_service/__init__.py:30: from instruments_service.app.core.instrument_processing_service import (
instruments_service/__init__.py:36: from instruments_service.app.core.cloud_instrument_storage import CloudInstrumen
instruments_service/__init__.py:40: from instruments_service.app.core.batch_processor import InstrumentBatchProcesso
instruments_service/__init__.py:44: from instruments_service.models import (
instruments_service/__init__.py:63: from instruments_service.config import (
instruments_service/app/core/dependency_checker.py:125: from instruments_service.config import instruments_config
instruments_service/app/core/dependency_checker.py:194: from unified_cloud_services import get_secret
instruments_service/app/core/dependency_checker.py:257: from unified_cloud_services import CloudTarget, StandardizedDomainCloudService
instruments_service/app/core/cloud_instrument_storage.py:180: from unified_cloud_services import SchemaValidator
instruments_service/app/core/cloud_instrument_storage.py:182: from instruments_service.schemas.parquet import get_required_columns
instruments_service/app/core/cloud_instrument_storage.py:338: from datetime import date as date_type
instruments_service/app/core/adapter_loader.py:56: from instruments_service.app.venues.tardis import TardisAdapter
instruments_service/app/core/adapter_loader.py:62: from instruments_service.app.venues.databento import DatabentoAdapter
instruments_service/app/core/adapter_loader.py:68: from unified_cloud_services import AsterBaseClient
instruments_service/app/core/adapter_loader.py:70: from instruments_service.app.venues.onchain_perps import AsterAdapter
instruments_service/app/core/adapter_loader.py:75: from unified_cloud_services import HyperliquidBaseClient
instruments_service/app/core/adapter_loader.py:77: from instruments_service.app.venues.onchain_perps import HyperliquidAdapter
instruments_service/app/core/adapter_loader.py:116: from instruments_service.app.venues.defi import UniswapV2Adapter
instruments_service/app/core/adapter_loader.py:120: from instruments_service.app.venues.defi import UniswapV3Adapter
instruments_service/app/core/adapter_loader.py:124: from instruments_service.app.venues.defi import UniswapV4Adapter
instruments_service/app/core/adapter_loader.py:128: from instruments_service.app.venues.defi import AaveV3Adapter
instruments_service/app/core/adapter_loader.py:132: from instruments_service.app.venues.defi import CurveRPCAdapter
instruments_service/app/core/adapter_loader.py:136: from instruments_service.app.venues.defi import BalancerAdapter
instruments_service/app/core/adapter_loader.py:140: from instruments_service.app.venues.defi import MorphoAdapter
instruments_service/app/core/adapter_loader.py:144: from instruments_service.app.venues.defi import EulerAdapter
instruments_service/app/core/adapter_loader.py:148: from instruments_service.app.venues.defi import FluidAdapter
instruments_service/app/core/adapter_loader.py:152: from instruments_service.app.venues.defi import LidoAdapter
instruments_service/app/core/adapter_loader.py:156: from instruments_service.app.venues.defi import EtherFiAdapter
instruments_service/app/core/adapter_loader.py:160: from instruments_service.app.venues.defi import EthenaAdapter
instruments_service/app/core/instruments_service.py:70: from instruments_service.config import instruments_config
instruments_service/app/core/cloud_data_provider.py:36: from instruments_service.config import instruments_config
instruments_service/app/venues/onchain_perps/hyperliquid_adapter.py:163: import json
instruments_service/app/venues/onchain_perps/hyperliquid_adapter.py:280: import json
instruments_service/app/venues/databento/databento_adapter.py:62: from unified_cloud_services import clear_databento_api_key_cache, clear_databent
instruments_service/app/venues/defi/the_graph_client.py:34: from instruments_service.config import instruments_config
instruments_service/app/venues/defi/the_graph_client.py:78: from instruments_service.config import instruments_config
instruments_service/utils/ccxt_service.py:67: from concurrent.futures import ThreadPoolExecutor, as_completed
instruments_service/cli/main.py:22: from dotenv import load_dotenv
instruments_service/cli/handlers/instrument_handler.py:24: from instruments_service.app.core.cloud_data_provider import CloudDataProvider
instruments_service/cli/handlers/instrument_handler.py:25: from instruments_service.app.core.cloud_instrument_storage import CloudInstrumen
instruments_service/cli/handlers/instrument_handler.py:26: from instruments_service.app.core.instruments_service import InstrumentsService
instruments_service/cli/handlers/instrument_handler.py:27: from instruments_service.app.core.selective_validation import validate_required_
instruments_service/cli/handlers/instrument_handler.py:28: from instruments_service.cli.base_handler import ModeHandler
instruments_service/cli/handlers/instrument_handler.py:45: from instruments_service.config import get_config as get_service_config
instruments_service/cli/handlers/corporate_actions_production_handler.py:128: from unified_cloud_services import CloudTarget, StandardizedDomainCloudService
instruments_service/cli/handlers/corporate_actions_handler.py:138: from unified_cloud_services import CloudTarget, StandardizedDomainCloudService
instruments_service/cli/handlers/corporate_actions_handler.py:483: from unified_cloud_services import CloudTarget, StandardizedDomainCloudService
instruments_service/cli/handlers/corporate_actions_backfill_handler.py:93: from unified_cloud_services import CloudTarget, StandardizedDomainCloudService
instruments_service/corporate_actions/models.py:71: import math
... and 10 more
```

---

## Next Steps

1. Fix violations listed above
2. Run `bash scripts/quality-gates.sh` to verify
3. Run `bash scripts/quickmerge.sh` to create PR

Quality gates will **BLOCK** merge if violations remain.