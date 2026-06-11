#!/usr/bin/env python3
"""migration_orphan_sweep.py — GCS→manifest orphan sweep + bucket taxonomy + sizing.

The INVERSE of ``reconcile_phantom_manifest_rows_all.py``:

* the phantom reconciler asks *manifest→GCS* — "a row claims captured; is the object
  there?" (a phantom = a claim with no object);
* this sweep asks *GCS→manifest* — "an object is on disk; does a manifest row point at
  it?" (an ORPHAN = real data with no row — the silent-write / manifest-completeness
  bug the operator fears as a "v10 trigger").

It is the CF-17 deliverable of ``migration_verification_orphan_safety_2026_06_10.md``.
A SINGLE bucket walk produces THREE outputs (single-walk discipline):

1. **Orphan classification** — every object bucketed into exactly one class:
     (A) CANONICAL_MANIFESTED   — v9-shape path, a captured manifest row resolves to it
     (B) LEGACY_DUPLICATE       — legacy-shape path whose canonical cell IS manifested
     (C) MANIFEST_INFRA         — ``_index/`` / ``*.tmp`` / ``*.partial`` / ``_SUCCESS``
     (C2) NON_DATA              — vm logs / run-artifacts / terraform / tarballs (kept,
                                  labelled, NEVER deleted — operator 2026-06-10)
     (D) JUNK                   — unparseable hive-key / invalid shard shape / zero-row
     (E) ORPHAN_REAL            — valid shape, rows>0, NO manifest row → **WE NEED IT**:
                                  ``record_captured`` backfill, NEVER delete
   → ``_index/audit/orphan_sweep_<ag>.parquet``  (uri, class, cell, crc32c, size, reason)

2. **Bucket prefix taxonomy** — every top-level prefix labelled; **0 ``unknown`` is the
   acceptance bar** ("every byte accounted for"). → printed + in the report.

3. **Sizing rollup** — bytes + object-count per (asset_group, data_type, venue,
   pipeline_mode). → ``_index/audit/data_sizing_<ag>.parquet``.

**Acceptance (CF-17, per AG):** ``orphan_class_E == 0`` (no real data without a row)
AND the phantom reconciler's ``phantom_count == 0`` ⇒ manifest ≡ GCS, exactly.

The canonical GCS path shapes come from the SINGLE SSOT
``unified_api_contracts.canonical_path_templates`` (CF-15) — this sweep never
hand-maintains a path list (the Axis-10 de-scatter). ``is_valid_shard_key`` decides
whether an object's hive-key is inside the valid could-exist space.

Default scan-only (writes the audit parquets; never deletes). Verified-delete of class
(B) twins is a SEPARATE operator-gated tool (CF-21, ``cleanup_legacy_twins_<ag>.py``).

Usage::

    cd instruments-service
    .venv/bin/python scripts/migration_orphan_sweep.py --asset-group cefi --dry-run
    .venv/bin/python scripts/migration_orphan_sweep.py --asset-group prediction \\
        --report-out gs://.../_index/audit/orphan_sweep_prediction.parquet
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from unified_api_contracts import ShardKey, canonical_path_templates, is_valid_shard_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_PARSER_CACHE: list[object] = []


def _backfill_parser() -> "Callable[[str, str], tuple[str, str, str, str] | None]":
    """The pre-hive instrument-key parser, loaded from the sibling backfill script so
    the path grammar stays single-source (``scripts/`` is not a package — same
    importlib pattern as ``tests/scripts/``)."""
    if not _PARSER_CACHE:
        spec = importlib.util.spec_from_file_location(
            "backfill_orphan_class_e", Path(__file__).parent / "backfill_orphan_class_e.py"
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load sibling backfill_orphan_class_e.py")
        mod = importlib.util.module_from_spec(spec)
        # dataclass field resolution requires the module registered BEFORE exec
        sys.modules.setdefault("backfill_orphan_class_e", mod)
        spec.loader.exec_module(mod)
        _PARSER_CACHE.append(mod._parse_instrument_key_shape)
    return _PARSER_CACHE[0]  # type: ignore[return-value]


class ObjectClass(StrEnum):
    """The forced 6-way classification (audit ⑬). Every object is EXACTLY one."""

    CANONICAL_MANIFESTED = "A_canonical_manifested"
    LEGACY_DUPLICATE = "B_legacy_duplicate"
    MANIFEST_INFRA = "C_manifest_infra"
    NON_DATA = "C2_non_data"
    JUNK = "D_junk"
    ORPHAN_REAL = "E_orphan_real"


# Bucket prefix taxonomy (CF-17): every top-level prefix → a label. ``raw_tick_data/``
# (+ the legacy top-level ``day=`` shape) is the RAW-tick service-data corpus this sweep
# classifies against the raw ``_index``; everything else is understood-and-labelled and
# EXCLUDED from the raw-orphan A/B/D/E logic (NEVER deleted by this sweep). A prefix not
# matched here surfaces as ``unknown`` — itself a finding (0 unknown is the bar). The
# OTHER-corpus prefixes (e.g. ``processed_candles/`` — the MDPS processed-candle corpus
# with its OWN manifest) are real data but NOT raw-tick: they must not be flagged as
# raw-tick orphans (the smoke 2026-06-10 surfaced 7,946 processed-candle objects mis-read
# as class-E before this label existed). They have their own re-runnable sweep.
_DATA_PREFIXES: tuple[str, ...] = ("raw_tick_data/", "day=")
# TOP-LEVEL-ONLY labels (matched via ``startswith`` exclusively — never substring,
# because tokens like ``dex_pools``/``lending_indices`` also appear as hive VALUES
# (``data_type=dex_pools``) deeper in real data paths). R1 2026-06-11 additions:
#   * ``dex_pools/`` + ``lending_indices/`` — the DeFi legacy top-level trees from the
#     ``solana_defi_legacy_migration_2026_05_27`` plan (own corpora, migrated by the
#     gated G4 defi walk; understood + never raw-tick-deleted).
#   * ``_manifests/`` — legacy manifest infra (pre ``_index/`` vocabulary).
#   * ``configs/`` — service config artifacts co-located in the AG buckets.
#   * ``databento-batch-registry/`` — the tradfi Databento batch-job registry
#     (vendor job metadata, not market data).
_NON_DATA_TOP_LEVEL_LABELS: dict[str, str] = {
    "_manifests/": "manifest-infra",
    "configs/": "configs",
    "dex_pools/": "legacy-data",
    "lending_indices/": "legacy-data",
    "databento-batch-registry/": "vendor-registry",
}
_NON_DATA_PREFIX_LABELS: dict[str, str] = {
    "_index/": "manifest-infra",
    "snapshots/": "manifest-infra",
    "logs/": "logs",
    "vm_logs/": "logs",
    "backfill-logs/": "logs",
    "_vm_staging/": "staging",
    "run_artifacts/": "run-artifact",
    "_audit/": "run-artifact",
    "terraform/": "terraform",
    "tarballs/": "tarball",
    "code/": "tarball",
    # other-corpus service data (NOT raw-tick; own manifest + own sweep) — labelled,
    # understood, never deleted/flagged by the raw-tick orphan logic.
    "processed_candles/": "processed-data",
    "processed_data/": "processed-data",
    "features/": "features-data",
}

# Object suffixes that are manifest-infra / non-data regardless of prefix.
_INFRA_SUFFIXES: tuple[str, ...] = (".tmp", ".partial", "_SUCCESS", ".lock")


@dataclass(frozen=True)
class SweptObject:
    """One classified GCS object."""

    uri: str
    asset_group: str
    obj_class: ObjectClass
    venue: str
    chain: str
    instrument_type: str
    data_type: str
    day: str
    pipeline_mode: str
    source: str
    size_bytes: int
    crc32c: str
    reason: str


# ---------------------------------------------------------------------------
# Pure parsing + classification (unit-tested without GCS)
# ---------------------------------------------------------------------------


def parse_hive_segments(object_path: str) -> dict[str, str]:
    """Extract hive ``key=value`` segments from a GCS object path.

    Tolerant of every shape ``canonical_path_templates`` produces: the canonical
    ``pipeline_mode=batch_<source>/asset_group=<ag>/`` shape, the legacy bare
    ``asset_group=`` / ``category=`` hives, the top-level ``day=`` shape, and the DeFi
    combined ``venue=PROTOCOL-CHAIN`` overload. Returns a dict of every ``k=v`` segment
    found (last-wins on duplicate keys). ``category`` is normalised to ``asset_group``.
    """
    segments: dict[str, str] = {}
    for part in object_path.split("/"):
        if "=" in part:
            key, _sep, value = part.partition("=")
            segments[key] = value
    if "category" in segments and "asset_group" not in segments:
        segments["asset_group"] = segments["category"]
    return segments


def _source_from_pipeline_mode(pipeline_mode: str) -> str:
    """``batch_polymarket_clob`` → ``polymarket_clob`` (drop the ``<mode>_`` prefix)."""
    for mode in ("batch_", "live_", "replay_"):
        if pipeline_mode.startswith(mode):
            return pipeline_mode[len(mode) :]
    return ""


# Legacy instrument_type tokens → canonical (matching + classification). The migrated
# twin trees + manifest rows carry the canonical singular vocabulary.
_LEGACY_IT_TOKENS: dict[str, str] = {"equities": "equity"}


def canonical_match_instrument_type(asset_group: str, instrument_type: str) -> str:
    """Canonicalise LEGACY instrument_type vocabulary so coverage matching converges.

    * tradfi pre-hive plural ``equities`` → ``equity`` (R1 2026-06-11: two canonical-twin
      paths carry the legacy plural in their preserved hive tail — without this remap
      they can never read as covered by the ``equity``-keyed manifest rows).
    * prediction legacy underlying-in-the-instrument_type-slot taxonomy
      (``instrument_type=BTC|ETH|SOL|…``) → ``prediction_market`` — the canonical market
      grain keys ``instrument_type=prediction_market`` with the asset in ``underlying``
      (codex/04-architecture/prediction-batch-live.md §4; manifest precedent: every
      captured prediction cell is ``(POLYMARKET, POLYGON, prediction_market)``). The
      backfill characterizer derives ``underlying`` from the RAW segment, so no identity
      is lost by this match-time remap.
    """
    it = _LEGACY_IT_TOKENS.get(instrument_type, instrument_type)
    if asset_group == "prediction" and it and it.lower() != "prediction_market":
        return "prediction_market"
    return it


def shard_key_from_segments(asset_group: str, segments: dict[str, str]) -> ShardKey:
    """Build a :class:`ShardKey` from parsed hive segments. Handles the DeFi combined
    ``venue=PROTOCOL-CHAIN`` overload (splits it back into venue + chain) and the legacy
    instrument_type vocabulary (:func:`canonical_match_instrument_type`)."""
    venue = segments.get("venue", "")
    chain = segments.get("chain", "")
    # DeFi combined venue-chain overload: ``venue=EIGENLAYER-ETHEREUM`` with no chain=.
    if asset_group == "defi" and not chain and "-" in venue:
        venue, _sep, chain = venue.partition("-")
    return ShardKey(
        asset_group=asset_group,
        venue=venue,
        chain=chain,
        instrument_type=canonical_match_instrument_type(asset_group, segments.get("instrument_type", "")),
        data_type=segments.get("data_type", ""),
    )


def _is_canonical_shape(segments: dict[str, str]) -> bool:
    """A canonical v9 object carries a ``pipeline_mode=batch_<source>/`` segment LEFT of
    ``asset_group=``. A legacy object has no ``pipeline_mode=`` (bare ``asset_group=`` /
    ``category=`` / top-level ``day=``)."""
    pm = segments.get("pipeline_mode", "")
    return bool(pm) and pm.startswith(("batch_", "live_", "replay_"))


def _prefix_label(object_path: str) -> str | None:
    """Return the non-data label for ``object_path`` if it is infra/non-data, else None
    (meaning: it is a service-data object to classify by hive-key)."""
    for suffix in _INFRA_SUFFIXES:
        if object_path.endswith(suffix):
            return "manifest-infra"
    # TOP-LEVEL-ONLY labels first (never substring — see _NON_DATA_TOP_LEVEL_LABELS).
    for prefix, label in _NON_DATA_TOP_LEVEL_LABELS.items():
        if object_path.startswith(prefix):
            return label
    for prefix, label in _NON_DATA_PREFIX_LABELS.items():
        if object_path.startswith(prefix) or f"/{prefix}" in object_path:
            return label
    return None


def top_level_prefix(object_path: str) -> str:
    """The first path segment (the bucket-taxonomy key). ``raw_tick_data/by_date/...`` →
    ``raw_tick_data/``; ``_index/foo`` → ``_index/``; ``day=2024.../...`` → ``day=``."""
    head = object_path.split("/", 1)[0]
    return f"{head}/" if not head.startswith("day=") else "day="


def classify_object(
    object_path: str,
    asset_group: str,
    manifested_cells: CoveredIndex,
    *,
    is_parquet: bool,
    row_count: int | None = None,
) -> tuple[ObjectClass, ShardKey, str]:
    """Classify ONE object (pure). ``manifested_cells`` is the grain-aware covered index
    from :func:`build_covered_index` (``(date, data_type)`` → captured ``(venue, chain,
    instrument_type)`` patterns, blank = wildcard).

    ``row_count`` (footer rows) is consulted ONLY to split class (D) zero-row junk from
    class (E) real orphan; pass ``None`` when unknown (treated as non-zero → the safe
    side: a genuinely-empty object surfaces for inspection rather than being silently
    dropped).
    """
    ag = asset_group.lower()
    # 1. infra / non-data — by suffix or top-level prefix.
    label = _prefix_label(object_path)
    if label is not None:
        cls = ObjectClass.MANIFEST_INFRA if label == "manifest-infra" else ObjectClass.NON_DATA
        return cls, _empty_key(ag), label
    # 2. a non-parquet object under a data prefix is not service data → non-data.
    if not is_parquet:
        return ObjectClass.NON_DATA, _empty_key(ag), "non-parquet under data prefix"
    segments = parse_hive_segments(object_path)
    key = shard_key_from_segments(ag, segments)
    day = segments.get("day", "")
    # 3. unparseable / out-of-space hive-key → junk.
    if not key.data_type or not day:
        return ObjectClass.JUNK, key, "missing data_type/day hive segment"
    # 3.5 pre-hive blank-venue derivation (R1 close-out 2026-06-11): paths like
    # ``data_type=ohlcv_15m/indices/CBOE/CBOE:INDEX:VIX-USD.parquet`` carry no
    # ``venue=`` segment, so the hive parse leaves venue blank — and a blank-venue
    # object can NEVER read as covered (venue is identity, never object-wildcarded),
    # making E==0 unreachable even after the backfill manifests its canonical twin
    # cell. Derive (venue, instrument_type) from the non-hive tail with the SAME
    # parser the backfill characterizer uses (single-source via importlib —
    # ``scripts/`` is not a package), so post-backfill these flip to class B.
    if not key.venue:
        parsed = _backfill_parser()(object_path, key.data_type)
        if parsed is not None:
            p_venue, p_it, _class_segment, _underlying = parsed
            key = ShardKey(
                asset_group=key.asset_group,
                venue=p_venue,
                chain=key.chain,
                instrument_type=canonical_match_instrument_type(ag, p_it),
                data_type=key.data_type,
            )
    if not is_valid_shard_key(ag, key):
        return ObjectClass.JUNK, key, "hive-key outside valid could-exist space"
    # 4. is the object covered by a manifest row? (grain-aware: blank manifest fields
    #    are WILDCARDS — the manifest is keyed at a coarser grain than the per-instrument
    #    object path, e.g. prediction manifest rows carry blank chain/instrument_type
    #    while objects carry chain=POLYGON. An exact 5-tuple match would false-flag every
    #    such object as an orphan — the smoke 2026-06-10 caught this as prediction A=0.)
    manifested = is_covered(manifested_cells, key, day)
    canonical = _is_canonical_shape(segments)
    if manifested:
        return (
            (ObjectClass.CANONICAL_MANIFESTED if canonical else ObjectClass.LEGACY_DUPLICATE),
            key,
            "canonical+manifested" if canonical else "legacy twin of a manifested cell",
        )
    # 5. valid shape, NO manifest row.
    if row_count == 0:
        return ObjectClass.JUNK, key, "zero-row object with no manifest row"
    return ObjectClass.ORPHAN_REAL, key, "real data (rows>0) with NO manifest row — backfill record_captured"


def _empty_key(asset_group: str) -> ShardKey:
    return ShardKey(asset_group=asset_group, venue="", chain="", instrument_type="", data_type="")


# The covered-index type: (date, data_type) → set of (venue, chain, instrument_type)
# captured-cell patterns. Blank pattern fields are wildcards at lookup time.
CoveredIndex = dict[tuple[str, str], set[tuple[str, str, str]]]


def _norm_v(value: str) -> str:
    """Venue/chain token normalisation for COVERAGE MATCHING only.

    Venue spelling drifted across writer generations — the 2026-04 migration +
    its manifest rows spell ``UNISWAPV3`` / ``AAVEV3`` (and combined
    ``UNISWAPV2-ETHEREUM``) while the legacy ``category=`` object paths + the UAC
    canonical registry spell ``UNISWAP_V3`` / ``AAVE_V3`` (R1 finding 2026-06-11:
    254,812 of the 254,984 defi class-E were this venue-token split, every cell
    exactly manifested under the no-underscore spelling). Separator characters
    carry no venue identity in this corpus, so the match collapses them; the
    canonical SPELLING fix on the manifest rows rides the per-AG G4 rebuild walk."""
    return value.upper().replace("_", "").replace("-", "")


def _norm_it(value: str) -> str:
    return value.lower()


def is_covered(index: CoveredIndex, key: ShardKey, day: str) -> bool:
    """Return whether a captured manifest row COVERS this object (grain-aware).

    The manifest is keyed at a coarser grain than the per-instrument object path — its
    ``chain`` / ``instrument_type`` (and sometimes ``venue``) are blank, meaning "any".
    An object ``(venue, chain, instrument_type)`` is covered iff the ``(day, data_type)``
    bucket contains a captured pattern whose non-blank fields all equal the object's —
    i.e. ANY of the 8 blank-combinations of (venue, chain, instrument_type) is present
    (a fixed-cost lookup, not an O(patterns) scan).

    GRAIN ALSO RUNS THE OTHER WAY (R1 refinement 2026-06-11): legacy object paths
    predate the ``chain=`` / ``instrument_type=`` axes (e.g.
    ``category=tradfi/venue=NYSE/data_type=ohlcv_1m/ABBV.parquet``) while the
    manifest row for the SAME corpus is keyed FINER
    (``(NYSE, '', 'equity')`` — prediction's ``(POLYMARKET, POLYGON,
    'prediction_market')`` likewise). A blank OBJECT ``chain``/``instrument_type``
    therefore matches a manifested pattern with ANY value in that slot. ``venue``
    is IDENTITY and is never object-wildcarded — a blank-venue object stays
    uncovered (class E) and routes to the backfill characterizer, which derives
    the venue from the non-hive path segments instead of guessing."""
    bucket = index.get((day, _norm_it(key.data_type)))
    if not bucket:
        return False
    v, c, it = _norm_v(key.venue), _norm_v(key.chain), _norm_it(key.instrument_type)
    if not v:
        return False
    for mv in (v, ""):
        for mc in (c, ""):
            for mit in (it, ""):
                if (mv, mc, mit) in bucket:
                    return True
    # Slow path: object-blank chain/instrument_type wildcard (manifest finer than the
    # legacy path). Buckets are small (per (day, data_type) pattern sets), so a linear
    # scan here is fixed-cost in practice and only runs when the fast path missed.
    if c and it:
        return False
    for mv, mc, mit in bucket:
        if mv not in (v, ""):
            continue
        if c and mc not in (c, ""):
            continue
        if it and mit not in (it, ""):
            continue
        return True
    return False


def build_covered_index(rows: list[dict[str, str]]) -> CoveredIndex:
    """Build the grain-aware covered index from captured manifest rows. Keyed by
    ``(date, data_type)`` → set of ``(venue, chain, instrument_type)`` patterns (blank
    fields preserved as wildcards). Replaces an exact 5-tuple set so the coarser manifest
    grain matches the finer object grain."""
    index: CoveredIndex = defaultdict(set)
    for r in rows:
        if str(r.get("capture_status", "")).lower() != "captured":
            continue
        date = str(r.get("date", "") or "")
        dt = _norm_it(str(r.get("data_type", "") or ""))
        if not date or not dt:
            continue
        pattern = (
            _norm_v(str(r.get("venue", "") or "")),
            _norm_v(str(r.get("chain", "") or "")),
            _norm_it(str(r.get("instrument_type", "") or "")),
        )
        index[(date, dt)].add(pattern)
    return index


# ---------------------------------------------------------------------------
# Sizing rollup (rides the same walk)
# ---------------------------------------------------------------------------


@dataclass
class SizingRollup:
    """bytes + object-count per (asset_group, data_type, venue, pipeline_mode)."""

    by_cell: dict[tuple[str, str, str, str], list[int]]

    @classmethod
    def empty(cls) -> SizingRollup:
        return cls(by_cell=defaultdict(lambda: [0, 0]))

    def add(self, obj: SweptObject) -> None:
        cell = (obj.asset_group, obj.data_type, obj.venue, obj.pipeline_mode)
        agg = self.by_cell.setdefault(cell, [0, 0])
        agg[0] += obj.size_bytes
        agg[1] += 1

    def biggest(self, n: int = 20) -> list[tuple[tuple[str, str, str, str], int, int]]:
        items = [(cell, agg[0], agg[1]) for cell, agg in self.by_cell.items()]
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:n]

    def total_bytes(self) -> int:
        return sum(agg[0] for agg in self.by_cell.values())


# ---------------------------------------------------------------------------
# GCS walk orchestration (thin; the heavy lifting is the pure classifier above)
# ---------------------------------------------------------------------------


def _resolve_bucket(asset_group: str, cloud: str = "gcp") -> str:
    """Resolve the canonical raw-tick bucket for an asset_group via the bucket-name SSOT.
    Mirrors ``reconcile_phantom_manifest_rows_all._BUCKET_KIND_MAP``."""
    from unified_trading_library import resolve_bucket_name

    kind_map = {
        "cefi": ("market-data", "cefi"),
        "defi": ("market-data", "defi"),
        "tradfi": ("market-data", "tradfi"),
        "sports": ("instruments-store", "sports"),
        "prediction": ("market-data-tick-prediction", None),
    }
    kind, ag_arg = kind_map[asset_group]
    if ag_arg is None:
        return resolve_bucket_name(cloud=cloud, kind=kind)
    return resolve_bucket_name(cloud=cloud, kind=kind, asset_group=ag_arg)


def run_sweep(
    asset_group: str,
    *,
    cloud: str = "gcp",
    workers: int = 32,
    limit: int | None = None,
) -> tuple[Counter[str], Counter[str], SizingRollup, list[SweptObject]]:
    """Walk the AG bucket once, classify every object, roll up sizing + the prefix
    taxonomy. Returns (class_counts, prefix_taxonomy, sizing, orphan_E_objects).

    The full GCS read uses ``get_storage_client().list_blobs`` (paginated; size + crc32c
    come free from the blob metadata). Class-(E) candidates' footer row_count is read
    lazily ONLY for the no-manifest-row subset (cheap) to split (D) zero-row from (E).
    """
    from unified_trading_library import get_storage_client

    bucket = _resolve_bucket(asset_group, cloud)
    client = get_storage_client()
    logger.info("orphan-sweep %s: bucket=%s — loading manifest cells", asset_group, bucket)
    manifested = _load_manifested_cells(client, bucket)
    logger.info("orphan-sweep %s: %d captured manifest cells", asset_group, len(manifested))

    class_counts: Counter[str] = Counter()
    prefix_taxonomy: Counter[str] = Counter()
    sizing = SizingRollup.empty()
    actionable: list[SweptObject] = []  # class-B (legacy twins for CF-21 cleanup) + class-E (orphans)
    t0 = time.time()
    for seen, blob in enumerate(client.list_blobs(bucket), start=1):
        name = blob.name
        prefix_taxonomy[_taxonomy_label(name)] += 1
        is_parquet = name.endswith(".parquet")
        cls, key, reason = classify_object(name, asset_group, manifested, is_parquet=is_parquet, row_count=None)
        class_counts[cls.value] += 1
        segs = parse_hive_segments(name)
        pm = segs.get("pipeline_mode", "")
        obj = SweptObject(
            uri=f"gs://{bucket}/{name}",
            asset_group=asset_group,
            obj_class=cls,
            venue=key.venue,
            chain=key.chain,
            instrument_type=key.instrument_type,
            data_type=key.data_type,
            day=segs.get("day", ""),
            pipeline_mode=pm,
            source=_source_from_pipeline_mode(pm),
            size_bytes=int(getattr(blob, "size", 0) or 0),
            crc32c=str(getattr(blob, "crc32c", "") or ""),
            reason=reason,
        )
        if is_parquet and cls != ObjectClass.NON_DATA:
            sizing.add(obj)
        if cls in (ObjectClass.ORPHAN_REAL, ObjectClass.LEGACY_DUPLICATE):
            actionable.append(obj)
        if seen % 50000 == 0:
            logger.info("  %d objects swept (%.0f/s)", seen, seen / max(0.01, time.time() - t0))
        if limit is not None and seen >= limit:
            logger.info("  --limit %d reached — stopping walk", limit)
            break
    return class_counts, prefix_taxonomy, sizing, actionable


def _taxonomy_label(object_path: str) -> str:
    """The bucket-prefix taxonomy label (CF-17): 0 ``unknown`` is the acceptance bar."""
    label = _prefix_label(object_path)
    if label is not None:
        return label
    tlp = top_level_prefix(object_path)
    if any(object_path.startswith(p) for p in _DATA_PREFIXES):
        return "service-data"
    return f"unknown:{tlp}"


def _load_manifested_cells(client: object, bucket: str) -> CoveredIndex:
    """Read ``_index/availability_index.parquet`` and build the grain-aware covered index."""
    import io

    import pandas as pd

    index_path = "_index/availability_index.parquet"
    if not client.blob_exists(bucket, index_path):  # type: ignore[attr-defined]
        logger.warning("orphan-sweep: no %s in %s", index_path, bucket)
        return {}
    raw = client.download_bytes(bucket, index_path)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    rows = df.to_dict("records")
    return build_covered_index(rows)  # type: ignore[arg-type]


def _print_report(
    asset_group: str,
    class_counts: Counter[str],
    prefix_taxonomy: Counter[str],
    sizing: SizingRollup,
    actionable: list[SweptObject],
) -> int:
    """Print the human report; return the exit code (0 only if class-E == 0 AND no
    unknown prefixes — the CF-17 acceptance bar)."""
    logger.info("=== orphan sweep report: %s ===", asset_group)
    for cls in ObjectClass:
        logger.info("  %-26s %d", cls.value, class_counts.get(cls.value, 0))
    logger.info("--- bucket prefix taxonomy ---")
    unknown = 0
    for label, count in sorted(prefix_taxonomy.items()):
        logger.info("  %-30s %d", label, count)
        if label.startswith("unknown:"):
            unknown += count
    logger.info("--- sizing: total=%.2f GiB across %d cells ---", sizing.total_bytes() / 2**30, len(sizing.by_cell))
    for cell, nbytes, nobj in sizing.biggest(15):
        logger.info("  %-55s %.2f GiB (%d objs)", "/".join(cell), nbytes / 2**30, nobj)
    n_e = class_counts.get(ObjectClass.ORPHAN_REAL.value, 0)
    logger.info("=== ACCEPTANCE: orphan_class_E=%d (target 0), unknown_prefixes=%d (target 0) ===", n_e, unknown)
    if n_e:
        logger.warning("orphan-E objects (record_captured backfill — NEVER delete); first 25:")
        for obj in [o for o in actionable if o.obj_class == ObjectClass.ORPHAN_REAL][:25]:
            logger.warning("  E %s (%s/%s/%s)", obj.uri, obj.venue, obj.data_type, obj.day)
    return 0 if (n_e == 0 and unknown == 0) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GCS→manifest orphan sweep + bucket taxonomy + sizing (CF-17).")
    parser.add_argument("--asset-group", required=True, choices=["cefi", "defi", "tradfi", "sports", "prediction"])
    parser.add_argument("--cloud", choices=["gcp", "aws"], default="gcp")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--limit", type=int, default=None, help="stop after N objects (smoke / partial sweep)")
    parser.add_argument("--dry-run", action="store_true", help="scan + report only; never write/delete (default)")
    parser.add_argument("--report-out", type=str, default="", help="gs:// URI to write the orphan_sweep parquet")
    args = parser.parse_args(argv)

    # Sports dispatches via its own UAC candidate_parquet_paths SSOT — canonical_path_
    # templates returns [] for sports, so the generic walk does not apply.
    if args.asset_group == "sports" and not canonical_path_templates("sports"):
        logger.info("sports uses candidate_parquet_paths — generic orphan sweep N/A; use the sports-specific path.")
        return 0

    class_counts, prefix_taxonomy, sizing, actionable = run_sweep(
        args.asset_group, cloud=args.cloud, workers=args.workers, limit=args.limit
    )
    code = _print_report(args.asset_group, class_counts, prefix_taxonomy, sizing, actionable)
    if args.report_out:
        # The report is an AUDIT artifact (never a deletion): it records class-E orphans
        # (record_captured backfill targets) + class-B legacy twins (the CF-21 verified-
        # delete candidates ``cleanup_legacy_twins.py`` consumes). Written even on a
        # --dry-run sweep (it moves no data objects).
        _write_report(args.report_out, actionable)
    return code


def _write_report(report_out: str, actionable: list[SweptObject]) -> None:
    """Write the actionable objects (class-E orphans + class-B legacy twins) to a parquet
    at ``report_out`` (gs:// URI). The CF-21 cleanup reads class-B from here."""
    import io

    import pandas as pd
    from unified_trading_library import get_storage_client

    df = pd.DataFrame([{**vars(o), "obj_class": o.obj_class.value} for o in actionable])
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    assert report_out.startswith("gs://")
    bucket, _sep, blob_path = report_out[len("gs://") :].partition("/")
    get_storage_client().upload_from_file_obj(bucket, blob_path, buf)  # type: ignore[attr-defined]
    n_e = sum(1 for o in actionable if o.obj_class == ObjectClass.ORPHAN_REAL)
    n_b = sum(1 for o in actionable if o.obj_class == ObjectClass.LEGACY_DUPLICATE)
    logger.info("wrote %d actionable rows (%d orphan-E + %d legacy-B) to %s", len(actionable), n_e, n_b, report_out)


if __name__ == "__main__":
    sys.exit(main())
