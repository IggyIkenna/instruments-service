# Instrument Catalogue

SSOT for the per-category canonical instrument catalogue produced by
instruments-service. The catalogue is the expected-universe input used by
MTDS, features-\*, strategy, execution, risk, and position services via UAC's
`get_instruments_available_on` filter. It replaces per-service ad-hoc
"which instruments exist on date D?" logic.

## CatalogueBuilder API

Defined at
`instruments_service/reference_data/catalogue/catalogue_builder.py:113`.

```python
from instruments_service.reference_data.catalogue import CatalogueBuilder

builder = CatalogueBuilder(api_keys={...})  # URDI api keys, optional

cefi_records = builder.build_cefi()      # list[InstrumentRecord]
tradfi_records = builder.build_tradfi()
defi_records = builder.build_defi()
all_records = builder.build_all()        # CEFI + TRADFI + DEFI

uri = builder.write_to_gcs(cefi_records, category="CEFI")
# → reference_data/instruments/category=cefi/written_at=.../all.parquet
```

Supported categories: `CEFI`, `TRADFI`, `DEFI` (see
`CATALOGUE_SUPPORTED_CATEGORIES`). The builder is intentionally thin — it
delegates instrument discovery to the existing URDI
`fetch_instruments_for_all_venues` path and only enriches each record with
a canonical `instrument_key` and an explicit availability window.

### Enrichment per record

1. **Canonical `instrument_key`** via UAC
   `build_instrument_id(...)` (`canonical_id_builder.py:291`). For DeFi
   records whose venue is stored as `PROTOCOL-CHAIN` (legacy shape), the
   builder splits on `-` to produce a protocol-only venue plus a chain
   argument; CeFi venues with dashes (`BINANCE-FUTURES`, `OKX-SPOT`) are
   preserved verbatim. Records that already carry an `instrument_key` are
   left untouched.
2. **`available_to_datetime`** defaults to `expiry` for dated derivatives
   (`FUTURE`, `OPTION`) when the adapter did not set it. Everything else is
   left as-provided; `None` means "open-ended".

`available_from_datetime` is expected to come from the adapter (listing /
launch date). `None` = inception (open-ended on the left). UAC's
`get_instruments_available_on` treats these boundaries as inclusive.

## `refresh_catalogue` CLI Hook

Defined at `instruments_service/engine/orchestrator.py:4475`.

```bash
# Example wiring via the service CLI (standardised axes)
python -m instruments_service \
    --operation refresh-catalogue \
    --mode batch \
    --category cefi            # omit for CEFI + TRADFI + DEFI
```

Returns a `dict[str, str]` of `category -> written URI` for observability.
Unknown categories are logged and skipped (not raised).

## GCS Layout

Each category lands in its own instruments bucket, resolved through UTL's
`get_bucket_name("instruments", <category>)` (with `-test` suffix when
`is_test_run`):

```
gs://instruments-store-{category}-{project}/reference_data/instruments/
    category={category}/
    written_at={YYYY-MM-DDTHH:MM:SSZ}/
        all.parquet
```

`written_at` is stamped at UTC. Downstream readers take the newest
`written_at` partition for a point-in-time view; earlier partitions remain
as historical snapshots of the expected universe.

The `all.parquet` file contains one row per `InstrumentRecord` serialised
via `model_dump(mode="json")`. Consumers load it via UTL and pass the
iterable straight into UAC:

```python
from unified_api_contracts.internal import get_instruments_available_on

expected = get_instruments_available_on(
    ref_date,
    catalogue,
    category="defi",
    chain="ethereum",
)
```

## FootyStats `fetched_at_hour` Partition Convention

FootyStats is polled repeatedly throughout the day so we can compute odds
drift and snapshot pre-game vs half-time state. To preserve every poll
without overwrite the writer partitions by capture hour:

```
# Odds
fetched_at_hour={YYYY-MM-DDTHH}/league={LEAGUE_ID}/footystats_odds.parquet

# Predictions
fetched_at_hour={YYYY-MM-DDTHH}/league={LEAGUE_ID}/footystats_predictions.parquet
```

- `fetched_at_hour` is the UTC `strftime("%Y-%m-%dT%H")` of the poll start.
- Each hourly bucket is a full snapshot of the endpoint's payload for the
  day it targets, not a diff.
- Write path: `engine/orchestrator.py` FootyStats handlers
  (odds: `orchestrator.py:3470`; predictions: `orchestrator.py:3116`).

### How downstream consumers should read

1. Resolve the target read time `T` (e.g. a backtest's "as of" timestamp or
   a live service's `now()`).
2. List `fetched_at_hour` partitions for the relevant dates and pick the
   **largest** `fetched_at_hour` that is `<= T`. That partition is the
   point-in-time view — earlier partitions are historical snapshots (useful
   for drift analysis but NOT the current state).
3. For a full historical snapshot series (e.g. drift features), load every
   `fetched_at_hour` within the window and join on league + match id.

Never overwrite older `fetched_at_hour` partitions — they are the raw
capture history that makes odds-drift strategies reproducible.
