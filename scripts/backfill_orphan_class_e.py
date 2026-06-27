#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + manifest captured count climbed + GCS orphan-sweep = 0
"""backfill_orphan_class_e.py — class-E orphan characterise → canonicalise → record_captured.

The R1 deliverable of the 2026-06-11 operator ratification (decision #1,
``master_data_canonicalisation_migration_catalogue_2026_06_07.md``): every class-E
object surfaced by ``migration_orphan_sweep.py`` (REAL data with NO manifest row) is
driven to a manifested state — **PRESERVE + backfill, never delete** — and per the
ratified "middle-ground-PLUS" bar each orphan is characterised per data_type against
its actual CODE use-case and **canonicalised to the v9-grade schema/path as part of
the backfill**: a non-canonical object is never stamped into the manifest as-is.

Pipeline (per asset_group):

1. **RE-VERIFY** — re-load the live ``_index`` and re-test every report-E row with
   the sweep's (refined) ``is_covered`` matcher. Rows that are now covered (the
   venue-token / object-blank-grain matcher refinements, R1 2026-06-11) are
   ``ALREADY_COVERED`` — they are legacy twins of manifested cells (class B), owned
   by the CF-21 verified-delete flow, NOT this backfill.
2. **CHARACTERISE** — map each still-orphan object to its canonical target
   (venue, chain, instrument_type, data_type, underlying, pipeline_mode) using the
   evidence-backed rules in :func:`characterize_object` (each rule cites the code
   use-case / capture-time evidence it rests on). Ambiguous mappings →
   ``ESCALATED`` (BLOCKED-OPERATOR-DECISION ledger entry), the run continues.
3. **CONVERT** — download each orphan, apply the UAC ``SchemaSpec.source_aliases``
   rename map (the SAME SSOT the CF-18 completeness audit consults via
   ``carried_column_names`` — renames cannot drift from the audit's verdict), stamp
   ``available_at`` (the source object's GCS write time = when the data became
   available in-system) + the row-level ``source`` provenance column, and upload to
   the canonical ``pipeline_mode={mode}_{source}/asset_group=...`` path.
   Copy-not-move: the source object is NEVER deleted (it becomes a class-B legacy
   twin on the re-sweep; CF-21 owns its verified cleanup).
4. **RECORD** — one ``record_captured`` per (day, venue, chain, instrument_type,
   data_type, underlying) cell with full provenance (``pipeline_mode`` + ``source``),
   through ``ManifestWriter(per_vm_shards=True)`` so the run never races the
   consolidator (run under ``MANIFEST_PER_VM_SHARDS=true VM_NAME=orphan-backfill-<ag>``).
   **EVERY characterised cell is recorded** (R1 tool fix 2026-06-11): the record df is
   a footer-exact schema frame built per cell on demand (zero rows; exact on-disk
   column names + dtypes from the parquet footer; ``available_at``/``source`` stamped) —
   never a frame retained from the convert pass. ``row_count`` is the footer-exact sum
   over the cell's objects. The prior design retained the first-converted frame per
   cell in memory; any cell whose frame didn't survive (multi-pass runs, first-object
   junk, memory pressure) errored ``no representative frame retained`` and was silently
   left unmanifested (tradfi 2026-06-11: 14,707 converted objects, only 249 cells
   recorded → re-sweep still E=28,495).
5. **SAMPLE-VERIFY** — per recorded cell, verify ONE canonical object at footer grade:
   rows>0 + (for objects this tool wrote) ``available_at`` present + columns ⊆ the
   carried canonical set. Record-only cells log column drift as a WARNING (the on-disk
   object is operator-ratified record-only; the G4 migrator owns object rewrites).

**CeFi (added R1 2026-06-11) is RECORD-ONLY — never converted.** Its class-E corpus is
the 2020+ Tardis flat-file tick corpus: mostly already-canonical
``pipeline_mode=batch_tardis/`` paths plus bare-legacy twins of the same cells (no
``pipeline_mode=`` segment). Recording the cell manifests the canonical objects (→A)
and re-classes the bare-legacy twins as (B) CF-21 candidates — no new object uploads
(the cefi SchemaSpec alias-carry is the R2-cefi follow-up; uploading raw-column copies
would violate the never-manifest-non-canonical bar).

Usage::

    cd instruments-service
    MANIFEST_PER_VM_SHARDS=true VM_NAME=orphan-backfill-defi \\
        .venv/bin/python scripts/backfill_orphan_class_e.py --asset-group defi --dry-run
    MANIFEST_PER_VM_SHARDS=true VM_NAME=orphan-backfill-defi \\
        .venv/bin/python scripts/backfill_orphan_class_e.py --asset-group defi --apply

After ``--apply``: run the manifest consolidator for the bucket, then re-run
``migration_orphan_sweep.py`` — acceptance is ``orphan_class_E == 0``.
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import logging
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

from unified_api_contracts import PipelineMode, ShardKey, is_valid_shard_key, source_string_for
from unified_api_contracts.registry.schema_spec import carried_column_names, find_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _load_sweep_module() -> ModuleType:
    """Load the sibling orphan-sweep script (its ``is_covered`` / ``build_covered_index``
    are the single coverage-matching SSOT — this backfill must agree with the sweep's
    verdicts or E==0 can never converge)."""
    script_path = Path(__file__).resolve().parent / "migration_orphan_sweep.py"
    spec = importlib.util.spec_from_file_location("_orphan_sweep_for_backfill", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_orphan_sweep_for_backfill"] = module
    spec.loader.exec_module(module)
    return module


_sweep = _load_sweep_module()

# ---------------------------------------------------------------------------
# Characterisation tables (each row cites its code-use-case / capture evidence)
# ---------------------------------------------------------------------------

# Legacy instrument-key class segments → canonical instrument_type tokens. The
# pre-hive tradfi adapter wrote ``data_type=<dt>/<class>/<VENUE>/<VENUE>:<ITYPE>:<SYM>.parquet``
# (sometimes without the ``<VENUE>/`` dir); the class segment is the legacy
# instrument-class vocabulary.
_CLASS_SEGMENT_TO_INSTRUMENT_TYPE: dict[str, str] = {
    "equities": "equity",
    "etf": "etf",
    "indices": "index",
    "spot": "spot_pair",
    "futures_chain": "futures_chain",
    "options_chain": "options_chain",
}

# Legacy hive instrument_type tokens → canonical tokens (the manifest + the
# canonical writers use the singular form).
_LEGACY_IT_TO_CANONICAL: dict[str, str] = {
    "equities": "equity",
}

# Prediction legacy ``instrument_type=BTC/ETH/...`` carried the UNDERLYING in the
# instrument_type slot; canonical rows are keyed ``instrument_type=prediction_market``
# with the asset in ``underlying`` (manifest precedent: every captured prediction
# trades cell is ``(POLYMARKET, POLYGON, prediction_market)``).
_PREDICTION_CANONICAL_IT = "prediction_market"
_PREDICTION_CHAIN = "POLYGON"  # Polymarket settles on Polygon (UAC prediction contract).


@dataclass(frozen=True)
class CanonicalTarget:
    """The characterised v9 destination for one orphan object."""

    venue: str
    chain: str
    instrument_type: str
    data_type: str
    underlying: str
    pipeline_mode: PipelineMode
    dest_path: str
    evidence: str


@dataclass(frozen=True)
class OrphanPlan:
    """One orphan object + its disposition."""

    uri: str
    day: str
    status: str  # ALREADY_COVERED | CONVERT | RECORD_ONLY | SKIPPED_JUNK | ESCALATED
    target: CanonicalTarget | None
    note: str = ""
    size_bytes: int = 0  # from the sweep report — lets footer reads skip a metadata GET


def _derive_tradfi_pipeline_mode(class_segment: str, data_type: str) -> tuple[PipelineMode, str]:
    """TradFi source characterisation, per capture-time evidence (R1 2026-06-11):

    * ``indices/`` ``ohlcv_15m`` → **databento**: the VIX 15m corpus; originally
      attributed to Barchart (BATCH_BARCHART RETIRED 2026-06-24 — VIX 15m now
      aggregates from VX futures via Databento per UAC pipeline_mode.py retirement
      comment).  Re-attributed to BATCH_DATABENTO for GCS migration.
    * ``spot/`` ``ohlcv_24h`` → **yahoo**: leaf keys are ``YAHOO_FINANCE:SPOT_PAIR:*``
      (the writer names its own source) and Databento carries no FX spot pairs.
    * everything else (tbbo / trades / ohlcv_1m on CME/NYSE/NASDAQ/ICE/CBOE) →
      **databento** (UAC ``SOURCE_PRIORITY`` primary; tbbo/trades are
      Databento-exclusive products, and the 2026-05-12 batch write predates the
      2026-05-28 Massive dual-source rollout, so Massive cannot be the producer).
    """
    if class_segment == "indices" and data_type == "ohlcv_15m":
        return (
            PipelineMode.BATCH_DATABENTO,
            "VIX 15m (BATCH_BARCHART RETIRED 2026-06-24 — re-attributed to databento per UAC retirement comment)",
        )
    if class_segment == "spot" and data_type == "ohlcv_24h":
        return PipelineMode.BATCH_YAHOO, "YAHOO_FINANCE:* instrument keys; Databento has no FX spot pairs"
    return PipelineMode.BATCH_DATABENTO, "SOURCE_PRIORITY primary; pre-Massive (2026-05-28) capture"


def _parse_instrument_key_shape(object_path: str, data_type: str) -> tuple[str, str, str, str] | None:
    """Parse the pre-hive ``data_type=<dt>/<class>[/<VENUE>]/<leaf>`` shape, where
    ``<leaf>`` is either an instrument key (``<VENUE>:<ITYPE>:<SYM>.parquet``) or a
    bare chain-root (``CORN.parquet`` under ``futures_chain/CME/``).

    Returns ``(venue, instrument_type, class_segment, underlying)`` or ``None``."""
    marker = f"data_type={data_type}/"
    idx = object_path.find(marker)
    if idx < 0:
        return None
    tail = object_path[idx + len(marker) :]
    parts = tail.split("/")
    if len(parts) < 2 or "=" in parts[0]:
        return None  # hive-shaped tail (underlying=... etc.), not instrument-key
    class_segment = parts[0]
    leaf = parts[-1]
    it = _CLASS_SEGMENT_TO_INSTRUMENT_TYPE.get(class_segment)
    if it is None:
        return None
    if len(parts) >= 3:
        leaf_venue = parts[1]  # explicit venue directory level
    elif ":" in leaf:
        leaf_venue = leaf.split(":", 1)[0]  # venue from the instrument key
    else:
        return None
    # ``spot/`` FX pairs: the leaf venue token is the SOURCE (YAHOO_FINANCE), not a
    # trading venue — the manifested venue for this corpus is ``FX`` (1,967 captured
    # (FX, spot_pair, ohlcv_24h) cells; code reads venue=FX).
    venue = "FX" if class_segment == "spot" else leaf_venue
    # Chain classes with a bare-root leaf carry the UNDERLYING as the leaf stem
    # (``futures_chain/CME/CORN.parquet`` → underlying=CORN), mirroring the hive
    # twin shape (``.../data_type=ohlcv_1m/underlying=ES/...``).
    underlying = ""
    if class_segment in ("futures_chain", "options_chain") and ":" not in leaf:
        underlying = leaf.rsplit(".", 1)[0]
    return venue, it, class_segment, underlying


def characterize_object(
    asset_group: str,
    object_path: str,
) -> tuple[CanonicalTarget | None, str]:
    """Characterise ONE orphan object → its canonical v9 target (pure; unit-tested).

    Returns ``(target, "")`` or ``(None, escalation_reason)`` when the canonical
    mapping is ambiguous (→ BLOCKED-OPERATOR-DECISION ledger entry).
    """
    ag = asset_group.lower()
    segments = _sweep.parse_hive_segments(object_path)
    key = _sweep.shard_key_from_segments(ag, segments)
    day = segments.get("day", "")
    data_type = key.data_type
    if not data_type or not day:
        return None, "missing data_type/day hive segment — cannot characterise"

    venue = key.venue
    chain = key.chain
    instrument_type = _LEGACY_IT_TO_CANONICAL.get(key.instrument_type, key.instrument_type)
    underlying = segments.get("underlying", "")
    evidence = ""
    class_segment = ""

    # --- already-canonical shape: keep its own pipeline_mode (record-only) -------
    existing_pm = segments.get("pipeline_mode", "")
    pm: PipelineMode | None = None
    if existing_pm:
        try:
            pm = PipelineMode(existing_pm)
            evidence = "object already at canonical pipeline_mode path — record-only"
        except ValueError:
            return None, f"unrecognised pipeline_mode token {existing_pm!r} on object path"

    # --- per-asset-group characterisation ----------------------------------------
    if ag == "defi":
        if pm is None:
            # Registry-first: when UAC SOURCE_PRIORITY registers the (defi, data_type)
            # pair, its primary source is THE write-gate-coherent stamp (the writer's
            # MissingSourceError / PipelineModeSourceMismatchError gates enforce it —
            # lending_indices / lst_rates → onchain_subgraph). Unregistered data_types
            # (dex_pools) fall back to the Solana capture handler's stamp
            # (mtds ``cli/handlers/solana_defi_handler.py:440`` → BATCH_ONCHAIN_RPC).
            from unified_api_contracts import read_with_source_priority

            try:
                _src, pm = read_with_source_priority(ag, data_type)
                evidence = f"UAC SOURCE_PRIORITY primary source for (defi, {data_type})"
            except KeyError:
                pm = PipelineMode.BATCH_ONCHAIN_RPC
                evidence = "unregistered in SOURCE_PRIORITY; mtds solana_defi_handler stamps BATCH_ONCHAIN_RPC"
        if not venue or not chain or not instrument_type:
            return None, f"defi orphan missing venue/chain/instrument_type ({venue!r}/{chain!r}/{instrument_type!r})"
    elif ag == "tradfi":
        if not venue:
            parsed = _parse_instrument_key_shape(object_path, data_type)
            if parsed is None:
                return None, "blank-venue tradfi object in an unrecognised path shape"
            venue, instrument_type, class_segment, parsed_underlying = parsed
            underlying = underlying or parsed_underlying
        if pm is None:
            pm, evidence = _derive_tradfi_pipeline_mode(class_segment, data_type)
        if not instrument_type:
            # Legacy ``venue=NYSE/data_type=...`` paths predate the instrument_type
            # axis; the NYSE/NASDAQ tbbo/trades/ohlcv corpus is the Databento EQUITY
            # capture (its migrated twin tree carries ``instrument_type=equity``).
            if venue in ("NYSE", "NASDAQ"):
                instrument_type = "equity"
            else:
                return None, f"blank instrument_type for venue={venue!r} — no canonical attribution rule"
    elif ag == "prediction":
        if pm is None:
            # Raw columns are the Polymarket CLOB trades API shape (proxyWallet /
            # conditionId / transactionHash ... — the R2 source_aliases map); the
            # CLOB adapter is the only producer of this corpus.
            pm = PipelineMode.BATCH_POLYMARKET_CLOB
            evidence = "Polymarket CLOB trades column shape; single-producer corpus"
        # Legacy paths carried the UNDERLYING asset in the instrument_type slot
        # (``instrument_type=BTC|ETH|…``). The sweep's shard key now canonicalises the
        # MATCH token to ``prediction_market`` (canonical_match_instrument_type), so the
        # underlying derives from the RAW hive segment — no identity lost.
        raw_it = segments.get("instrument_type", "")
        if raw_it and raw_it != _PREDICTION_CANONICAL_IT:
            underlying = underlying or raw_it
        instrument_type = instrument_type or _PREDICTION_CANONICAL_IT
        chain = chain or _PREDICTION_CHAIN
    elif ag == "cefi":
        # CeFi is RECORD-ONLY (operator-ratified, R1 2026-06-11): the class-E corpus is
        # the 2020+ Tardis flat-file tick download (raw tardis columns exchange/symbol/
        # timestamp/local_timestamp; venue-suffix tokens OKX-FUTURES/OKX-SPOT/…; UAC
        # SOURCE_PRIORITY registers ['tardis'] for every registered cefi data_type).
        # Canonical-shaped paths keep their parsed pipeline_mode (generic record-only
        # branch above); bare-legacy twins (no pipeline_mode= segment) attribute to
        # batch_tardis and are ALSO record-only — manifesting the cell turns the
        # canonical objects into (A) and the bare twins into (B) CF-21 candidates,
        # without uploading raw-column copies (the cefi SchemaSpec alias-carry is the
        # R2-cefi follow-up; G4 owns object canonicalisation).
        if pm is None:
            pm = PipelineMode.BATCH_TARDIS
            evidence = (
                "tardis flat-file corpus (bare-legacy twin of the batch_tardis tree) — "
                "record-only; cell manifest re-classes it as a CF-21 legacy twin"
            )
        if not venue or not instrument_type:
            return None, f"cefi orphan missing venue/instrument_type ({venue!r}/{instrument_type!r})"
    else:
        return None, f"asset_group {ag!r} has no backfill characterisation (sports has its own sweep)"

    dest_key = ShardKey(asset_group=ag, venue=venue, chain=chain, instrument_type=instrument_type, data_type=data_type)
    if not is_valid_shard_key(ag, dest_key):
        return None, f"characterised key outside valid could-exist space: {dest_key}"

    dest_path = _build_dest_path(
        ag,
        object_path,
        day=day,
        pipeline_mode=pm,
        venue=venue,
        chain=chain,
        instrument_type=instrument_type,
        data_type=data_type,
        underlying=underlying,
    )
    target = CanonicalTarget(
        venue=venue,
        chain=chain,
        instrument_type=instrument_type,
        data_type=data_type,
        underlying=underlying,
        pipeline_mode=pm,
        dest_path=dest_path,
        evidence=evidence,
    )
    return target, ""


def _build_dest_path(
    asset_group: str,
    object_path: str,
    *,
    day: str,
    pipeline_mode: PipelineMode,
    venue: str,
    chain: str,
    instrument_type: str,
    data_type: str,
    underlying: str,
) -> str:
    """The canonical v9 destination: ``raw_tick_data/by_date/day=<d>/pipeline_mode=<pm>/
    asset_group=<ag>/<AG segment shape>/<tail>`` (UAC ``possible_manifest`` shapes)."""
    leaf = object_path.rsplit("/", 1)[-1]
    # Preserve a hive tail (``underlying=E/<leaf>``) when the source path already
    # carries hive segments after data_type=; instrument-key shapes keep the leaf only.
    tail = leaf
    marker = f"data_type={data_type}/"
    idx = object_path.find(marker)
    if idx >= 0:
        rest = object_path[idx + len(marker) :]
        if "=" in rest.split("/", 1)[0]:
            tail = rest  # hive tail (e.g. underlying=ES/ticks.parquet) — preserved
        elif underlying:
            tail = f"underlying={underlying}/{leaf}"
    elif underlying:
        tail = f"underlying={underlying}/{leaf}"
    seg = f"venue={venue}/"
    if asset_group == "defi":
        seg += f"chain={chain}/"
    if instrument_type:
        seg += f"instrument_type={instrument_type}/"
    seg += f"data_type={data_type}/"
    if asset_group == "prediction" and chain:
        seg = f"venue={venue}/chain={chain}/instrument_type={instrument_type}/data_type={data_type}/"
    return f"raw_tick_data/by_date/day={day}/pipeline_mode={pipeline_mode.value}/asset_group={asset_group}/{seg}{tail}"


# ---------------------------------------------------------------------------
# Conversion (download → rename-per-SchemaSpec → stamp provenance → upload)
# ---------------------------------------------------------------------------


def _alias_rename_map(asset_group: str, data_type: str) -> dict[str, str]:
    """``{source_alias: canonical_name}`` from the UAC SchemaSpec — the SAME rename
    SSOT the CF-18 completeness audit consults (``carried_column_names``)."""
    spec = find_schema(asset_group, data_type)
    if spec is None:
        return {}
    renames: dict[str, str] = {}
    for col in spec.columns:
        for alias in col.source_aliases:
            renames[alias] = col.name
    return renames


def canonicalise_frame(
    df: object,
    *,
    asset_group: str,
    data_type: str,
    source: str,
    available_at: datetime,
) -> object:
    """Apply the v9 canonicalisation to one orphan frame (pure; unit-tested):

    * rename legacy/raw columns per ``SchemaSpec.source_aliases`` (only when the
      canonical name is not already present — never clobber);
    * stamp ``available_at`` when absent (the source object's GCS write time — the
      honest "when did this data become available in-system" for a backfill);
    * stamp the row-level ``source`` provenance column when absent (CF-4).
    """
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    renames = _alias_rename_map(asset_group, data_type)
    applicable = {a: c for a, c in renames.items() if a in df.columns and c not in df.columns}
    if applicable:
        df = df.rename(columns=applicable)
    if "available_at" not in df.columns:
        df = df.assign(available_at=pd.Timestamp(available_at))
    if "source" not in df.columns:
        df = df.assign(source=source)
    return df


def _read_parquet_bytes(raw: bytes) -> object:
    import pandas as pd

    return pd.read_parquet(io.BytesIO(raw))


def _available_at_from_metadata(uri: str) -> datetime:
    """The source object's GCS write time (tz-aware UTC). Falls back to now() only
    when the metadata is unavailable (still tz-aware, never naive)."""
    from unified_trading_library import get_storage_client

    bucket, _sep, blob_path = uri[len("gs://") :].partition("/")
    meta = get_storage_client().get_blob_metadata(bucket, blob_path)  # type: ignore[attr-defined]
    raw = getattr(meta, "last_modified", None) if meta is not None else None
    if raw:
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def _read_parquet_footer(bucket: str, blob_path: str, size: int = 0) -> tuple[int, list[str], object]:
    """Read ONLY the parquet footer via ranged GETs (a multi-GB object costs a few KB).

    Returns ``(num_rows, column_names, arrow_schema)``. ``size`` (from the sweep
    report) skips the metadata GET when known. The 64 KiB tail heuristic covers
    virtually every footer in one request; a larger footer triggers exactly one
    follow-up ranged read."""
    import pyarrow.parquet as pq
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    if size <= 0:
        meta = client.get_blob_metadata(bucket, blob_path)  # type: ignore[attr-defined]
        size = int(getattr(meta, "size", 0) or 0)
    tail_len = min(size, 65536)
    tail = client.download_bytes_range(bucket, blob_path, size - tail_len, size)  # type: ignore[attr-defined]
    footer_len = int.from_bytes(tail[-8:-4], "little")
    if footer_len + 8 > len(tail):
        tail = client.download_bytes_range(bucket, blob_path, size - footer_len - 8, size)  # type: ignore[attr-defined]
    md = pq.read_metadata(io.BytesIO(tail[-(footer_len + 8) :]))
    arrow_schema = md.schema.to_arrow_schema()
    return md.num_rows, list(md.schema.names), arrow_schema


def _empty_frame_from_schema(arrow_schema: object) -> object:
    """Footer-exact zero-row pandas frame (exact column names + dtypes)."""
    import pyarrow as pa

    assert isinstance(arrow_schema, pa.Schema)
    return arrow_schema.empty_table().to_pandas()


@dataclass
class ConvertResult:
    plan: OrphanPlan
    row_count: int
    uploaded: bool
    error: str = ""
    # Footer-exact zero-row frame of the cell's canonical object (or the head(0) of a
    # freshly converted frame) — record_cells canonicalises it into the record df.
    schema_df: object | None = None
    # The canonical object's footer column names (sample-verify consumes them).
    footer_cols: list[str] | None = None


def convert_object(
    plan: OrphanPlan, asset_group: str, bucket: str, *, apply: bool, overwrite: bool = False
) -> ConvertResult:
    """Characterised orphan → ConvertResult, at the cheapest honest grade:

    * ``RECORD_ONLY`` — footer read of the object itself (rows + columns + schema);
      no row data moves (cefi 74k-object scale).
    * ``CONVERT`` with an existing canonical twin (prior-run upload) — footer read of
      the TWIN; no re-download/re-upload unless ``overwrite``.
    * ``CONVERT`` fresh — full download → canonicalise → (apply) upload. Never touches
      the source object.

    Shard-isolated: every failure is captured on the result, never raised."""
    from unified_trading_library import get_storage_client

    assert plan.target is not None
    client = get_storage_client()
    src_path = plan.uri[len(f"gs://{bucket}/") :]
    try:
        if plan.status == "RECORD_ONLY":
            n_rows, cols, schema = _read_parquet_footer(bucket, src_path, plan.size_bytes)
            if n_rows == 0:
                return ConvertResult(
                    plan=replace(plan, status="SKIPPED_JUNK", note="zero rows"), row_count=0, uploaded=False
                )
            return ConvertResult(
                plan=plan,
                row_count=n_rows,
                uploaded=False,
                schema_df=_empty_frame_from_schema(schema),
                footer_cols=cols,
            )
        # CONVERT — reuse the canonical twin when it already exists (idempotent re-run).
        if not overwrite and client.blob_exists(bucket, plan.target.dest_path):  # type: ignore[attr-defined]
            n_rows, cols, schema = _read_parquet_footer(bucket, plan.target.dest_path)
            if n_rows == 0:
                return ConvertResult(
                    plan=replace(plan, status="SKIPPED_JUNK", note="zero rows (existing canonical twin)"),
                    row_count=0,
                    uploaded=False,
                )
            return ConvertResult(
                plan=replace(plan, note="canonical twin already exists"),
                row_count=n_rows,
                uploaded=False,
                schema_df=_empty_frame_from_schema(schema),
                footer_cols=cols,
            )
        raw = client.download_bytes(bucket, src_path)  # type: ignore[attr-defined]
        df = _read_parquet_bytes(raw)
        n_rows = len(df)  # type: ignore[arg-type]
        if n_rows == 0:
            return ConvertResult(
                plan=replace(plan, status="SKIPPED_JUNK", note="zero rows"), row_count=0, uploaded=False
            )
        source = source_string_for(plan.target.pipeline_mode) or ""
        available_at = _available_at_from_metadata(plan.uri)
        canonical_df = canonicalise_frame(
            df,
            asset_group=asset_group,
            data_type=plan.target.data_type,
            source=source,
            available_at=available_at,
        )
        uploaded = False
        if apply:
            buf = io.BytesIO()
            canonical_df.to_parquet(buf, index=False)  # type: ignore[attr-defined]
            buf.seek(0)
            client.upload_from_file_obj(bucket, plan.target.dest_path, buf)  # type: ignore[attr-defined]
            uploaded = True
        return ConvertResult(
            plan=plan,
            row_count=n_rows,
            uploaded=uploaded,
            schema_df=canonical_df.head(0),  # type: ignore[attr-defined]
            footer_cols=list(canonical_df.columns),  # type: ignore[attr-defined]
        )
    except Exception as exc:  # shard isolation: classify, never raise
        return ConvertResult(plan=plan, row_count=0, uploaded=False, error=f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def load_class_e(report_uri: str) -> list[dict[str, str]]:
    import pandas as pd
    from unified_trading_library import get_storage_client

    assert report_uri.startswith("gs://")
    bucket, _sep, blob_path = report_uri[len("gs://") :].partition("/")
    raw = get_storage_client().download_bytes(bucket, blob_path)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    e = df[df["obj_class"] == "E_orphan_real"]
    return e.to_dict("records")  # type: ignore[return-value]


def reverify_against_index(
    asset_group: str, bucket: str, rows: list[dict[str, str]]
) -> tuple[list[dict[str, str]], int]:
    """Split report-E rows into (still-orphan, already-covered-count) against the
    LIVE index with the refined sweep matcher."""
    from unified_trading_library import get_storage_client

    index = _sweep._load_manifested_cells(get_storage_client(), bucket)
    still: list[dict[str, str]] = []
    covered = 0
    for r in rows:
        key = ShardKey(
            asset_group=asset_group,
            venue=str(r.get("venue", "")),
            chain=str(r.get("chain", "")),
            # Reports written before the legacy-token remap carry raw tokens
            # (equities / prediction BTC|ETH|…) — canonicalise for matching exactly
            # as the sweep's shard_key_from_segments now does.
            instrument_type=_sweep.canonical_match_instrument_type(asset_group, str(r.get("instrument_type", ""))),
            data_type=str(r.get("data_type", "")),
        )
        if _sweep.is_covered(index, key, str(r.get("day", ""))):
            covered += 1
        else:
            still.append(r)
    return still, covered


def build_plans(asset_group: str, bucket: str, still_orphan: list[dict[str, str]]) -> list[OrphanPlan]:
    plans: list[OrphanPlan] = []
    prefix = f"gs://{bucket}/"
    for r in still_orphan:
        uri = str(r["uri"])
        object_path = uri[len(prefix) :] if uri.startswith(prefix) else uri
        day = str(r.get("day", ""))
        size_bytes = int(r.get("size_bytes", 0) or 0)
        target, reason = characterize_object(asset_group, object_path)
        if target is None:
            plans.append(OrphanPlan(uri=uri, day=day, status="ESCALATED", target=None, note=reason))
            continue
        status = "RECORD_ONLY" if "record-only" in target.evidence else "CONVERT"
        plans.append(OrphanPlan(uri=uri, day=day, status=status, target=target, size_bytes=size_bytes))
    return plans


def _cell_key(plan: OrphanPlan) -> tuple[str, str, str, str, str, str, str]:
    t = plan.target
    assert t is not None
    return (plan.day, t.venue, t.chain, t.instrument_type, t.data_type, t.underlying, t.pipeline_mode.value)


def record_cells(
    asset_group: str,
    bucket: str,
    results: list[ConvertResult],
    *,
    apply: bool,
) -> tuple[int, list[str]]:
    """One ``record_captured`` per characterised cell (full provenance). Returns
    (cells_recorded, per-cell errors)."""
    from unified_trading_library import ManifestWriter

    by_cell: dict[tuple[str, str, str, str, str, str, str], list[ConvertResult]] = defaultdict(list)
    for res in results:
        if res.plan.status in ("CONVERT", "RECORD_ONLY") and not res.error and res.row_count > 0:
            by_cell[_cell_key(res.plan)].append(res)
    if not apply:
        logger.info("[dry-run] would record_captured %d cells", len(by_cell))
        return 0, []
    # strict_validation=False (warn-only) — the sanctioned backfill mode per the
    # UnifiedCloudConfig.manifest_strict_schema_validation docstring ("set ... false
    # ... if a backfill needs to push through"). Rationale: the UTL row-contract
    # registry models the EVM defi variants (ts_event / supply_rate / price_a ...)
    # while these historical footers are the SOLANA variants that the UAC CF-18
    # SchemaSpecs (footer-derived, R2 2026-06-11) declare canonical — the mismatch
    # still logs MANIFEST_WRITE_SCHEMA_MISMATCH (visible), and the manifest row
    # truthfully reflects the on-disk parquet.
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        per_vm_shards=True,
        batch_size=200,
        strict_validation=False,
    )
    errors: list[str] = []
    recorded = 0
    for (day, venue, chain, it, dt, underlying, pm_value), cell_results in sorted(by_cell.items()):
        # R1 tool fix (2026-06-11): EVERY characterised cell records. The record df is
        # built per cell from the footer-exact schema frame any ok member carries (no
        # frame retention across the run; no full-object download). A zero-row frame
        # passes assert_available_at_present by design (the writer's empty path) and
        # gives the warn-only validator the TRUE on-disk column set; row_count is the
        # footer-exact sum. The prior retained-frame design left every cell without a
        # surviving frame unmanifested ("no representative frame retained").
        rep = next((r for r in cell_results if r.schema_df is not None), None)
        if rep is None:  # structurally unreachable: every ok result carries schema_df
            errors.append(f"cell {day}/{venue}/{dt}: no footer schema on any member")
            continue
        pm = PipelineMode(pm_value)
        source = source_string_for(pm) or ""
        rep_df = canonicalise_frame(
            rep.schema_df,
            asset_group=asset_group,
            data_type=dt,
            source=source,
            available_at=datetime.now(UTC),  # dtype-only on a zero-row frame
        )
        # Only per-chain shards (defi) carry ``chain`` in the row_key — UTL's
        # MalformedRowKeyError rejects an explicitly-empty chain (tradfi/
        # prediction cells are not chain-scoped; hard_schema Phase 4).
        row_key: dict[str, str] = {"date": day, "venue": venue}
        if chain:
            row_key["chain"] = chain
        try:
            writer.record_captured(
                row_key=row_key,
                df=rep_df,  # type: ignore[arg-type]
                asset_group=asset_group,
                instrument_type=it,
                data_type=dt,
                venue=venue,
                chain=chain,
                underlying=underlying,
                row_count=sum(r.row_count for r in cell_results),
                attempted_at=datetime.now(UTC),
                pipeline_mode=pm,
                source=source,
            )
            recorded += 1
        except Exception as exc:  # per-cell isolation
            errors.append(f"cell {day}/{venue}/{chain}/{it}/{dt}: {type(exc).__name__}: {exc}")
        if recorded and recorded % 1000 == 0:
            logger.info("  recorded %d cells", recorded)
    writer.close()
    return recorded, errors


def sample_verify(bucket: str, asset_group: str, results: list[ConvertResult]) -> list[str]:
    """Per recorded cell: verify ONE canonical object at FOOTER grade (rides the same
    ranged-read discipline as the convert pass — a multi-GB object costs a few KB).

    * ``CONVERT`` + freshly uploaded → re-read the DEST footer (proves the upload
      landed): rows>0, ``available_at`` column present (this tool stamped it), columns
      ⊆ the carried canonical set (when a SchemaSpec exists) — FAILURES.
    * ``CONVERT`` via an existing twin → same checks on the twin's footer columns
      (already in hand from the convert pass; no refetch).
    * ``RECORD_ONLY`` → rows>0 (FAILURE if not); column drift vs the carried set is a
      WARNING, not a failure — the on-disk object is operator-ratified record-only
      (cefi pre-G4 tardis corpus); the G4 migrator owns object rewrites and the cefi
      SchemaSpec alias-carry is the R2-cefi follow-up.

    Returns failure strings (empty = verified)."""
    seen: set[tuple[str, str, str, str, str, str, str]] = set()
    failures: list[str] = []
    drift_warned = 0
    meta_ok = _sweep_partition_meta_columns()
    for res in results:
        if res.plan.status not in ("CONVERT", "RECORD_ONLY") or res.error or res.row_count == 0:
            continue
        key = _cell_key(res.plan)
        if key in seen:
            continue
        seen.add(key)
        assert res.plan.target is not None
        dest = res.plan.target.dest_path
        try:
            if res.uploaded:
                n_rows, cols, _schema = _read_parquet_footer(bucket, dest)
            else:
                n_rows, cols = res.row_count, list(res.footer_cols or [])
            if n_rows == 0:
                failures.append(f"{dest}: zero rows after conversion")
                continue
            is_converted = res.plan.status == "CONVERT"
            if is_converted and "available_at" not in cols:
                failures.append(f"{dest}: available_at missing")
                continue
            spec = find_schema(asset_group, res.plan.target.data_type)
            if spec is not None and cols:
                carried = carried_column_names(spec) | meta_ok
                extra = {c for c in cols if c not in carried}
                if extra and is_converted:
                    failures.append(f"{dest}: columns outside carried canonical set: {sorted(extra)}")
                elif extra:
                    drift_warned += 1
                    if drift_warned <= 5:
                        logger.warning(
                            "record-only column drift (G4/R2-cefi follow-up, not a failure) %s: %s",
                            res.plan.uri,
                            sorted(extra),
                        )
        except Exception as exc:
            failures.append(f"{dest}: verify error {type(exc).__name__}: {exc}")
    if drift_warned:
        logger.warning("record-only cells with column drift vs carried set: %d (warn-only)", drift_warned)
    return failures


def _sweep_partition_meta_columns() -> frozenset[str]:
    """Partition/meta columns exempt from the carried-set check — mirrors the CF-18
    audit's ``_PARTITION_AND_META_COLUMNS`` (path keys + writer bookkeeping)."""
    return frozenset(
        {
            "asset_group",
            "category",
            "venue",
            "chain",
            "instrument_type",
            "instrument_id",
            "instrument_key",
            "data_type",
            "day",
            "date",
            "pipeline_mode",
            "source",
            "transport",
            "available_at",
            "schema_version",
            "underlying",
            "symbol",
            "timeframe",
        }
    )


def write_report(bucket: str, asset_group: str, plans_and_results: list[ConvertResult]) -> str:
    import pandas as pd
    from unified_trading_library import get_storage_client

    rows = []
    for res in plans_and_results:
        t = res.plan.target
        rows.append(
            {
                "uri": res.plan.uri,
                "day": res.plan.day,
                "status": res.plan.status if not res.error else "FAILED",
                "note": res.plan.note or res.error,
                "venue": t.venue if t else "",
                "chain": t.chain if t else "",
                "instrument_type": t.instrument_type if t else "",
                "data_type": t.data_type if t else "",
                "underlying": t.underlying if t else "",
                "pipeline_mode": t.pipeline_mode.value if t else "",
                "dest_path": t.dest_path if t else "",
                "evidence": t.evidence if t else "",
                "row_count": res.row_count,
            }
        )
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    blob_path = f"_index/audit/orphan_backfill_{asset_group}.parquet"
    get_storage_client().upload_from_file_obj(bucket, blob_path, buf)  # type: ignore[attr-defined]
    return f"gs://{bucket}/{blob_path}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Class-E orphan characterise→canonicalise→record_captured backfill (R1)."
    )
    parser.add_argument("--asset-group", required=True, choices=["defi", "tradfi", "prediction", "cefi"])
    parser.add_argument("--cloud", choices=["gcp", "aws"], default="gcp")
    parser.add_argument(
        "--report-uri", default="", help="orphan_sweep report parquet (default: the AG's _index/audit path)"
    )
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="re-upload destination objects even when they exist (re-run after a SchemaSpec alias extension)",
    )
    parser.add_argument("--limit", type=int, default=None, help="cap processed orphans (smoke)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    ag = args.asset_group
    # M-COORD-6: the ManifestWriter validation path emits events — init first
    # (batch mode requires an explicit GcsEventSink; UnifiedCloudConfig owns the
    # project id — no os.getenv).
    from unified_trading_library import GcsEventSink, UnifiedCloudConfig, setup_events

    project_id = UnifiedCloudConfig().gcp_project_id
    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(project_id=project_id, bucket=f"{project_id}-events", service_name="instruments-service"),
    )
    bucket = _sweep._resolve_bucket(ag, args.cloud)
    report_uri = args.report_uri or f"gs://{bucket}/_index/audit/orphan_sweep_{ag}.parquet"

    logger.info("backfill-orphan-E %s: bucket=%s report=%s", ag, bucket, report_uri)
    e_rows = load_class_e(report_uri)
    logger.info("report class-E rows: %d", len(e_rows))
    still, covered = reverify_against_index(ag, bucket, e_rows)
    logger.info("re-verify vs LIVE index: already-covered=%d (class-B reclass), still-orphan=%d", covered, len(still))
    if args.limit is not None:
        still = still[: args.limit]

    plans = build_plans(ag, bucket, still)
    escalated = [p for p in plans if p.status == "ESCALATED"]
    actionable = [p for p in plans if p.status in ("CONVERT", "RECORD_ONLY")]
    logger.info("characterised: convert/record=%d escalated=%d", len(actionable), len(escalated))
    for p in escalated[:20]:
        logger.warning("ESCALATED (BLOCKED-OPERATOR-DECISION): %s — %s", p.uri, p.note)

    # Per-cell summary of the plan (the dry-run eyeball surface).
    cell_summary: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for p in actionable:
        assert p.target is not None
        cell_summary[(p.target.venue, p.target.instrument_type, p.target.data_type, p.target.pipeline_mode.value)] += 1
    for (venue, it, dt, pm), n in sorted(cell_summary.items()):
        logger.info("  plan %-14s %-16s %-18s %-24s %d objects", venue, it, dt, pm, n)

    # Convert (dry-run: characterise + sample-validate only a few per family).
    if args.dry_run:
        sample_per_family: dict[tuple[str, str, str, str], int] = defaultdict(int)
        to_convert = []
        for p in actionable:
            assert p.target is not None
            fam = (p.target.venue, p.target.instrument_type, p.target.data_type, p.target.pipeline_mode.value)
            if sample_per_family[fam] < 2:
                sample_per_family[fam] += 1
                to_convert.append(p)
    else:
        to_convert = actionable

    results: list[ConvertResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(convert_object, p, ag, bucket, apply=args.apply, overwrite=args.overwrite) for p in to_convert
        ]
        for i, fut in enumerate(futures, start=1):
            results.append(fut.result())
            if i % 1000 == 0:
                logger.info("  converted %d/%d", i, len(futures))

    failed = [r for r in results if r.error]
    junk = [r for r in results if r.plan.status == "SKIPPED_JUNK"]
    ok = [r for r in results if not r.error and r.plan.status in ("CONVERT", "RECORD_ONLY")]
    logger.info("convert: ok=%d zero-row-junk=%d failed=%d", len(ok), len(junk), len(failed))
    for r in failed[:10]:
        logger.warning("  FAILED %s — %s", r.plan.uri, r.error)

    recorded, record_errors = record_cells(ag, bucket, results, apply=args.apply)
    for err in record_errors[:10]:
        logger.warning("  RECORD-ERROR %s", err)

    verify_failures: list[str] = []
    if args.apply:
        verify_failures = sample_verify(bucket, ag, results)
        for f in verify_failures[:10]:
            logger.warning("  VERIFY-FAIL %s", f)
        # Escalated plans ride the report too (status=ESCALATED, empty target).
        report_results = results + [ConvertResult(plan=p, row_count=0, uploaded=False) for p in escalated]
        report_path = write_report(bucket, ag, report_results)
        logger.info("backfill report: %s", report_path)

    logger.info(
        "=== VERDICT %s: already_covered=%d converted=%d recorded_cells=%d junk=%d "
        "escalated=%d convert_failed=%d verify_failed=%d ===",
        ag,
        covered,
        len(ok),
        recorded,
        len(junk),
        len(escalated),
        len(failed),
        len(verify_failures),
    )
    if args.apply and recorded:
        logger.info(
            "next: wait for the manifest consolidator (Cloud Scheduler */1; or run "
            "`python -m unified_trading_library.manifest_consolidator --bucket %s`), "
            "then re-run migration_orphan_sweep.py — acceptance is E==0.",
            bucket,
        )
    if args.apply and (failed or record_errors or verify_failures):
        return 1
    return 0 if not escalated else 1


if __name__ == "__main__":
    sys.exit(main())
