"""CatalogueBuilder — build per-asset-group canonical instrument catalogues.

Reads the existing URDI reference data for each venue (via
``fetch_instruments_for_all_venues``) and produces a list of
``InstrumentRecord`` with canonical ``instrument_key`` populated via UAC
``build_instrument_id(...)`` and ``available_from_datetime`` /
``available_to_datetime`` set from adapter-provided listing / expiry dates.

Output partition
----------------
Catalogues are written as a single parquet per asset group to
``reference_data/instruments/{asset_group}/all.parquet`` inside the
instruments write bucket resolved through UTL's cloud sink.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal

import pandas as pd
from unified_api_contracts import InstrumentType, build_instrument_id
from unified_api_contracts.internal import InstrumentRecord, MarketCategory
from unified_trading_library import DataSink, get_data_sink, get_write_bucket_name

from instruments_service.config import get_config
from instruments_service.engine.orchestrator import get_venues_for_asset_groups
from instruments_service.engine.urdi_reference_provider import fetch_instruments_for_all_venues

logger = logging.getLogger(__name__)

_CATEGORY_MEMBERS: tuple[MarketCategory, ...] = (
    MarketCategory.CEFI,
    MarketCategory.TRADFI,
    MarketCategory.DEFI,
)

#: Tuple of asset-group strings the catalogue currently covers. Imported
#: by ``engine.orchestrator`` to drive the ``refresh_catalogue`` CLI hook.
CATALOGUE_SUPPORTED_ASSET_GROUPS: tuple[str, ...] = tuple(c.value for c in _CATEGORY_MEMBERS)

_DEFI_TYPES: frozenset[InstrumentType] = frozenset(
    {
        InstrumentType.POOL,
        InstrumentType.LENDING,
        InstrumentType.LST,
        InstrumentType.YIELD_BEARING,
        InstrumentType.A_TOKEN,
        InstrumentType.DEBT_TOKEN,
        InstrumentType.STAKING,
        InstrumentType.SPOT_ASSET,
    }
)


def _ensure_canonical_id(record: InstrumentRecord) -> InstrumentRecord:
    """Return a copy of ``record`` with ``instrument_key`` normalised.

    If ``instrument_key`` is missing or empty, build it via UAC
    ``build_instrument_id`` using the record's venue / type / symbol (plus
    derivative fields when present). Existing keys are preserved verbatim.
    """
    existing = record.instrument_key
    if existing:
        return record

    symbol = record.raw_symbol or record.base_asset
    chain: str | None = None
    venue = record.venue
    # Only DeFi venues encode PROTOCOL-CHAIN in the venue string. CeFi venues
    # like ``BINANCE-FUTURES`` / ``OKX-SPOT`` also contain dashes but must not
    # be split — ``build_instrument_id`` treats them as opaque venue names.
    if record.instrument_type in _DEFI_TYPES and "-" in venue:
        proto, _, chain_part = venue.partition("-")
        if proto and chain_part:
            venue = proto
            chain = chain_part

    option_right: Literal["C", "P"] | None = None
    if record.option_type is not None:
        option_right = "C" if record.option_type.value.upper().startswith("C") else "P"

    expiry_date = record.expiry.date() if record.expiry is not None else None

    new_key = build_instrument_id(
        venue,
        record.instrument_type,
        symbol,
        expiry_date=expiry_date,
        strike=record.strike,
        option_right=option_right,
        underlying=record.underlying,
        chain=chain,
    )
    return record.model_copy(update={"instrument_key": new_key})


def _populate_availability(record: InstrumentRecord) -> InstrumentRecord:
    """Ensure ``available_to_datetime`` is populated for dated derivatives.

    - Dated derivatives (FUTURE, OPTION): ``available_to_datetime`` defaults
      to ``expiry`` when the adapter did not set it.
    - Everything else: leave as-provided; ``None`` means "open-ended".
    """
    if record.available_to_datetime is None and record.expiry is not None:
        return record.model_copy(update={"available_to_datetime": record.expiry})
    return record


class CatalogueBuilder:
    """Build and persist per-asset-group instrument catalogues.

    The builder is intentionally thin — it delegates all instrument discovery
    to the existing URDI ``fetch_instruments_for_all_venues`` path and only
    enriches each record with a canonical ``instrument_key`` and explicit
    availability window.
    """

    def __init__(
        self,
        api_keys: dict[str, str] | None = None,
        bucket_resolver: Callable[[str], str] | None = None,
    ) -> None:
        self._api_keys: dict[str, str] = dict(api_keys) if api_keys else {}
        self._bucket_resolver: Callable[[str], str] | None = bucket_resolver

    # ------------------------------------------------------------------
    # Per-asset-group builders (sync — convenient for CLI / tests)
    # ------------------------------------------------------------------
    def build_cefi(self) -> list[InstrumentRecord]:
        """Build and return cefi."""
        return asyncio.run(self.build_asset_group_async("CEFI"))

    def build_tradfi(self) -> list[InstrumentRecord]:
        """Build and return tradfi."""
        return asyncio.run(self.build_asset_group_async("TRADFI"))

    def build_defi(self) -> list[InstrumentRecord]:
        """Build and return defi."""
        return asyncio.run(self.build_asset_group_async("DEFI"))

    def build_all(self) -> list[InstrumentRecord]:
        """Concatenate CEFI + TRADFI + DEFI catalogues."""
        out: list[InstrumentRecord] = []
        for asset_group in CATALOGUE_SUPPORTED_ASSET_GROUPS:
            out.extend(asyncio.run(self.build_asset_group_async(asset_group)))
        return out

    async def build_asset_group_async(self, asset_group: str) -> list[InstrumentRecord]:
        """Build and return asset group async."""
        venues = get_venues_for_asset_groups([asset_group])
        if not venues:
            return []
        result = await fetch_instruments_for_all_venues(
            venues=venues,
            instrument_type=None,
            api_keys=self._api_keys,
            date=None,
            mode="batch",
        )
        enriched: list[InstrumentRecord] = []
        for rec in result.records:
            enriched.append(_populate_availability(_ensure_canonical_id(rec)))
        logger.info(
            "CatalogueBuilder[%s]: built %d records across %d venues",
            asset_group,
            len(enriched),
            len(venues),
        )
        return enriched

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def write_to_gcs(self, records: list[InstrumentRecord], asset_group: str) -> str:
        """Write ``records`` to ``reference_data/instruments/{asset_group}/all.parquet``.

        Returns the resolved partition identifier (a ``gs://``-style URI when
        a real sink is used, otherwise the local filesystem path returned by
        the sink).
        """
        bucket = self._resolve_bucket(asset_group)
        sink: DataSink = get_data_sink(
            bucket=bucket,
            prefix="reference_data/instruments",
        )
        df = _records_to_dataframe(records)
        written_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        partition = {"asset_group": asset_group.lower(), "written_at": written_at}
        uri = sink.write(
            data=df,
            partition=partition,
            format="parquet",
            filename="all.parquet",
        )
        logger.info(
            "CatalogueBuilder: wrote %d records to %s (asset_group=%s)",
            len(records),
            uri,
            asset_group,
        )
        return uri

    def _resolve_bucket(self, asset_group: str) -> str:
        """Look up the instruments bucket for an asset group.

        Delegates to UTL ``get_write_bucket_name`` which honours ``IS_TEST_RUN``
        by inserting ``-test-`` between segment and project_id — canonical
        SSOT per ``codex/02-data/per-category-bucket-layouts.md``. A custom
        resolver can be injected via the constructor for tests.
        """
        if self._bucket_resolver is not None:
            return self._bucket_resolver(asset_group)

        cfg = get_config()
        project = cfg.gcp_project_id or "test-project"
        try:
            return get_write_bucket_name("instruments", asset_group, project)
        except (ImportError, AttributeError):
            cat_lower = asset_group.lower() if asset_group else None
            prefix = cfg.instruments_bucket_prefix
            prod_bucket = f"{prefix}-{cat_lower}-{project}" if cat_lower else f"{prefix}-{project}"
            if not cfg.is_test_run:
                return prod_bucket
            return prod_bucket.replace(f"-{project}", f"-test-{project}", 1)


def _records_to_dataframe(records: list[InstrumentRecord]) -> pd.DataFrame:
    """Serialise records to a pandas DataFrame for parquet persistence."""
    if not records:
        return pd.DataFrame()
    rows = [rec.model_dump(mode="json") for rec in records]
    return pd.DataFrame(rows)
