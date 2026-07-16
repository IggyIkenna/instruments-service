# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Cross-asset-group honest coverage measurement — schema_version 2.

Reads the availability manifest for every asset_group, computes honest
coverage at multiple aggregation levels:
  - per asset_group (workspace-wide rollup)
  - per (asset_group, venue)
  - per (asset_group, venue, data_type)
  - per (asset_group, venue, instrument_type)                 [NEW v2]
  - per (asset_group, venue, instrument_type, data_type)      [NEW v2]
  - per (asset_group, date)                                   [NEW v2]

Also computes Layer-1 enumeration-completeness (instrument denominator audit)
via check_enumeration_completeness.py and adds a top-level ``layer_1`` block
plus ``instrument_gates_download``, ``denominator_complete``, and
``layer1_completeness_pct`` additive fields on each ``by_asset_group`` cell.

Coverage formula:  captured / (captured + attempted_failed + expected_unattempted)

i.e. the fraction of *reachable* shard slots that were captured.
empty_confirmed rows (non-trading days, pre-genesis chain dates, source-confirmed
gaps) are excluded from the denominator — they represent legitimate absence, not
pipeline failures.  The old all-shards formula is preserved as
``all_shards_coverage_pct`` for reference.

SSOT: codex/02-data/honest-coverage-model.md (Two layers, two views, schema v2).
Output: gs://central-element-323112-honest-coverage/{YYYY-MM-DD}/coverage.json

execution:
  owner: Cron VM via deployment-service/scripts/vm/launch-measure-honest-coverage-vm.sh
  cadence: daily
  verifier: gs://central-element-323112-honest-coverage/{date}/coverage.json
             exists and parses without error
  last_executed: NEVER

Bucket selection (hardened 2026-07-06 vs. surgery-bumped mtimes):
  PRIMARY is pinned by tuple order in ``_MANIFEST_BUCKET_CANDIDATES`` — the first
  accessible candidate wins (which is the ``-prd`` bucket by construction, for every
  asset_group). This replaces the earlier mtime-based selection which was fragile to
  manifest surgery: rewriting the legacy bucket bumped its ``blob.updated`` past prd,
  flipping roles → prd's captured-only tuples dropped from ENUMERATED and 3 artifact
  "holes" appeared (2026-07-03 ASTER corrective pass on cefi legacy).
  ``blob.updated`` is still logged (a secondary bucket with a newer mtime raises a
  SURGERY-SIGNAL warning) but no longer drives selection. An operator override
  ``--primary-bucket=<name>`` picks a specific bucket when surgery or debugging
  requires it; if the override is not accessible, selection falls back to the
  tuple-order pin.

Manifest merge (Bug 2 fix):
  After picking the pinned bucket as PRIMARY, the secondary bucket is also read and
  merged. If the ``date`` column is present in both DataFrames, shards are deduplicated
  on (date, venue, data_type) keeping the PRIMARY row's capture_status (prd wins).
  This ensures the full expected_unattempted skeleton (from the legacy non-prd bucket)
  is combined with fresh captured/attempted_failed/empty_confirmed from prd.
  Use ``--no-merge`` to disable this merging and fall back to primary-only.

Instrument-type column (v2):
  Reads ``instrument_type`` as a 5th bounded column. Legacy buckets may not have this
  column; if absent, instrument_type projections degrade gracefully (empty sets),
  and a warning is logged. Bounded-column reads remain mandatory (cefi index is
  tens-of-millions of rows; loading the full frame OOM-kills the VM).

Usage:
  python measure_honest_coverage.py [--asset-group cefi|defi|tradfi|sports|prediction|all]
  python measure_honest_coverage.py --output-path /tmp/coverage.json   # local probe
  python measure_honest_coverage.py --no-merge                         # primary-only
  python measure_honest_coverage.py --primary-bucket <name>            # force primary
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, cast

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Protocols for the dynamically-loaded check_enumeration_completeness module.
# These mirror the AgLayer1Result dataclass fields and as_dict method without
# importing the sibling script at parse time (it has heavy UAC deps).
# ---------------------------------------------------------------------------


class _AgLayer1ResultProto(Protocol):
    """Structural interface matching AgLayer1Result in check_enumeration_completeness."""

    denominator_complete: bool
    denominator_status: str  # "COMPLETE" | "INCOMPLETE" | "UNDEFINED"
    completeness_pct: float | None  # None when denominator_status == "UNDEFINED"
    missing_tuples: list[object]

    def as_dict(self) -> dict[str, object]: ...


class _CompletenessModuleProto(Protocol):
    """Structural interface for the dynamically-loaded completeness module."""

    def check_enumeration_completeness(
        self, asset_group: str, df: pd.DataFrame, *, diagnose: bool = ...
    ) -> _AgLayer1ResultProto: ...

    def filter_manifest_to_expected(
        self,
        asset_group: str,
        df: pd.DataFrame,
        *,
        expected: set[tuple[str, str, str]] | None = ...,
    ) -> pd.DataFrame: ...


# ---------------------------------------------------------------------------
# Lazy-load check_enumeration_completeness (sibling script, not a package module)
# ---------------------------------------------------------------------------


def _load_completeness_module() -> _CompletenessModuleProto:
    """Load check_enumeration_completeness.py from the scripts/ sibling directory."""
    script_dir = Path(__file__).resolve().parent
    checker_path = script_dir / "check_enumeration_completeness.py"
    module_name = "_check_enumeration_completeness"
    spec = importlib.util.spec_from_file_location(module_name, checker_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {checker_path}")
    module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    if not hasattr(loader, "exec_module"):
        raise ImportError(f"Loader for {checker_path} does not support exec_module")
    # Register in sys.modules BEFORE exec_module so that @dataclass can resolve
    # the module's __dict__ (Python 3.13 dataclasses lookup uses sys.modules).
    sys.modules[module_name] = module
    loader.exec_module(module)
    return cast(_CompletenessModuleProto, module)


_completeness_module: _CompletenessModuleProto | None = None


def _get_completeness_module() -> _CompletenessModuleProto:
    global _completeness_module
    if _completeness_module is None:
        _completeness_module = _load_completeness_module()
    return _completeness_module


PROJECT_ID = "central-element-323112"

# Manifest bucket candidates per asset_group (scripts/ excluded from inline-URI QG ratchet).
# During the bucket-SSOT env-tiering migration (bucket_name_ssot_canonicalisation Phase 2.6)
# the LIVE bucket differs per asset_group: CeFi tick still writes the legacy FLAT bucket
# (get_write_bucket_name → cloud_constants legacy prefixes), while DeFi on-chain handlers
# already write the env-tiered `-prd` bucket (different write path). So we cannot assume a
# single naming scheme. List both candidates and read whichever is MOST RECENTLY UPDATED
# (blob.updated timestamp — not row count). Self-corrects after Phase 2.6 consolidates
# everything onto `-prd`.
# See plans/active/issues/cefi_tick_bucket_ssot_divergence_2026_05_25.md.
_MANIFEST_BUCKET_CANDIDATES: dict[str, tuple[str, ...]] = {
    "cefi": (f"market-data-tick-cefi-prd-{PROJECT_ID}", f"market-data-tick-cefi-{PROJECT_ID}"),
    "defi": (f"market-data-tick-defi-prd-{PROJECT_ID}", f"market-data-tick-defi-{PROJECT_ID}"),
    "tradfi": (f"market-data-tick-tradfi-prd-{PROJECT_ID}", f"market-data-tick-tradfi-{PROJECT_ID}"),
    "sports": (f"market-data-tick-sports-prd-{PROJECT_ID}", f"market-data-tick-sports-{PROJECT_ID}"),
    "prediction": (f"market-data-tick-pred-prd-{PROJECT_ID}", f"market-data-tick-prediction-{PROJECT_ID}"),
}

_OUTPUT_BUCKET = f"{PROJECT_ID}-honest-coverage"
_KNOWN_ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")
_CAPTURE_STATUSES = ("captured", "empty_confirmed", "attempted_failed", "expected_unattempted")
_INDEX_BLOB_PATH = "_index/availability_index.parquet"
# v2: added "instrument_type" for the new projections; "instrument_id" for dedup shard key.
# Legacy buckets that lack either column degrade gracefully (see _read_parquet_safe).
_READ_COLUMNS = ["capture_status", "venue", "data_type", "date", "instrument_id", "instrument_type"]
# Preferred shard key (instrument-level dedup); fallback used when instrument_id absent.
_READ_COLUMNS_FALLBACK = ["capture_status", "venue", "data_type", "date", "instrument_type"]
_READ_COLUMNS_LEGACY = ["capture_status", "venue", "data_type", "date"]
_READ_COLUMNS_MIN = ["capture_status", "venue", "data_type"]
# Column-prune hardening (defence-in-depth, 2026-07-16): the availability-index
# parquet stores every one of these columns PLAIN_DICTIONARY-encoded per row
# group (verified via pyarrow row-group metadata on real prd buckets). pandas'
# default `pd.read_parquet` DECODES that dictionary encoding into a plain
# python-object array per row — on a real 1.96M-row sports bucket this cost
# 613.8MB for the 6 columns; passing `read_dictionary=<columns>` (forwarded by
# pandas to `pyarrow.parquet.read_table`) instead preserves the on-disk
# dictionary as pandas `category` dtype, measured at 15.9MB for the identical
# read (~38.6x smaller, same values — `instrument_id.nunique()` verified
# identical before/after). This is what stops the read from scaling toward
# OOM on tens-of-millions-of-row corpora (cefi/tradfi/sports) — no row-group
# streaming needed, the parquet file already carries the compact encoding.
# NOTE: this changes 3 of _READ_COLUMNS' dtypes to `category`; see
# _merge_manifests (defensive .astype("int64") on the priority column — a
# Categorical.map() result stays Categorical and SORTS BY CATEGORY-DISCOVERY
# ORDER, not numeric order, if left uncast) and _compute_coverage (groupby
# calls pass observed=True to avoid phantom empty groups pandas' groupby
# would otherwise synthesise for every unobserved category combination).
_SHARD_KEY = ["date", "venue", "data_type"]
_SHARD_KEY_WITH_IID = ["date", "venue", "instrument_id", "data_type"]
# Priority order for deduplication: lower index = higher priority.
_STATUS_PRIORITY: dict[str, int] = {
    "captured": 0,
    "attempted_failed": 1,
    "empty_confirmed": 2,
    "expected_unattempted": 3,
}

# Asset groups whose Layer-2 counts are filtered to the EXPECTED-in-scope
# manifest rows (the MVP read-time gate, cefi Layer-1 denominator gaps plan
# task 2c, 2026-07-06).  For cefi this aligns Layer-2 numerator/denominator
# with the Layer-1 EXPECTED matrix (MVP-scoped via
# `get_mvp_data_types_for_cefi_venue`), killing the class of dishonest %
# where non-MVP captures inflate the Layer-2 "captured" count over a
# MVP-scoped EXPECTED denominator.  The MVP filter itself lives in
# `expected_universe.build_expected` (2a); this constant enables the
# read-time application in the Layer-2 harness.  Other AGs are excluded
# until their EXPECTED matrices are certified complete (defi PROTOCOL work
# in-flight; tradfi/sports/prediction on separate spines).  Layer-1
# strays remain visible — the check consumes the UNFILTERED df.
# SSOT: codex/02-data/honest-coverage-model.md § MVP filter (row `is_mvp`).
_MVP_READ_TIME_GATE_AGS: frozenset[str] = frozenset({"cefi"})


def _get_blob_updated(client: storage.Client, bucket_name: str) -> datetime | None:
    """Return the UTC-aware updated timestamp of the availability index blob, or None."""
    try:
        blob = client.bucket(bucket_name).get_blob(_INDEX_BLOB_PATH)
        if blob is None:
            return None
        return blob.updated  # already UTC-aware
    except Exception as exc:
        logger.info("  blob timestamp lookup failed for %s: %s", bucket_name, exc)
        return None


def _read_parquet_safe(
    bucket_name: str,
) -> pd.DataFrame | None:
    """Read the availability index parquet for a bucket, returning None on failure.

    v2: attempts to read all 6 columns (capture_status, venue, data_type, date,
    instrument_id, instrument_type). Falls back progressively:
      1. All 6 columns (preferred — v2 full)
      2. 5 columns without instrument_id (older pre-iid buckets)
      3. 4 columns without instrument_type (legacy pre-v2 buckets)
      4. 3 columns minimal (oldest buckets — no date/iid/itype)
    A warning is logged for each degraded mode.
    """
    uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
    try:
        # Preferred: all 6 columns incl. instrument_id + instrument_type (v2).
        # read_dictionary preserves the parquet's on-disk PLAIN_DICTIONARY
        # encoding as pandas `category` dtype instead of expanding every row
        # into a python-object string — the memory-bounded read (see the
        # _READ_COLUMNS comment above for the measured ~38.6x reduction).
        df = pd.read_parquet(uri, columns=_READ_COLUMNS, read_dictionary=_READ_COLUMNS)
        return df
    except Exception:
        pass  # fall through

    # Fallback 1: 5 columns (no instrument_id — bucket lacks iid column).
    try:
        df = pd.read_parquet(uri, columns=_READ_COLUMNS_FALLBACK, read_dictionary=_READ_COLUMNS_FALLBACK)
        logger.warning(
            "  [%s] 'instrument_id' column absent in parquet — "
            "merge dedup will fall back to (date, venue, data_type) key.",
            bucket_name,
        )
        return df
    except Exception:
        pass  # fall through

    # Fallback 2: 4 columns (no instrument_id, no instrument_type — legacy bucket).
    try:
        df = pd.read_parquet(uri, columns=_READ_COLUMNS_LEGACY, read_dictionary=_READ_COLUMNS_LEGACY)
        logger.warning(
            "  [%s] 'instrument_type' column absent in parquet — "
            "Layer-1 ENUMERATED set and instrument_type projections will be empty. "
            "Re-run after writer backfill stamps instrument_type.",
            bucket_name,
        )
        return df
    except Exception:
        pass  # fall through

    # Fallback 3: 3 columns minimal (oldest buckets — no date/iid/itype).
    try:
        df = pd.read_parquet(uri, columns=_READ_COLUMNS_MIN, read_dictionary=_READ_COLUMNS_MIN)
        logger.warning(
            "  [%s] Only 3 columns available — merge dedup, by_day and v2 projections unavailable.",
            bucket_name,
        )
        return df
    except Exception as exc:
        logger.info("  candidate not accessible (%s): %s", uri, exc)
        return None


def _read_parquet_eu_only(bucket_name: str) -> pd.DataFrame | None:
    """Read only expected_unattempted rows for memory-bounded prd+oracle merge.

    Uses pyarrow push-down filter so the cefi oracle (~35.8M rows) is never fully
    materialised — only the ~4.1M eu skeleton rows are loaded.
    """
    uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
    eu_filter = [("capture_status", "==", "expected_unattempted")]
    for cols in (_READ_COLUMNS, _READ_COLUMNS_FALLBACK, _READ_COLUMNS_LEGACY, _READ_COLUMNS_MIN):
        try:
            # read_dictionary: same column-prune hardening as _read_parquet_safe
            # (category dtype instead of python-object strings) — this read is
            # already row-filtered to expected_unattempted, but the dtype win is
            # free and keeps the two readers' output dtypes consistent.
            return pd.read_parquet(uri, columns=list(cols), filters=eu_filter, read_dictionary=list(cols))
        except Exception:
            pass
    logger.info("  eu-only read failed for all column variants: %s", uri)
    return None


def _merge_manifests(
    df_primary: pd.DataFrame,
    df_secondary: pd.DataFrame,
) -> pd.DataFrame:
    """Merge primary and secondary manifests, preferring primary's capture_status per shard.

    Strategy:
    - If ``date`` is present in both DataFrames, deduplicate on the shard key keeping
      primary's row first (prd wins over legacy stale rows).  When ``instrument_id`` is
      present in both frames the full shard key ``(date, venue, instrument_id, data_type)``
      is used so different instruments on the same date/venue/data_type are not collapsed.
      Falls back to ``(date, venue, data_type)`` with a warning when instrument_id is absent.
    - If ``date`` is absent in either, concatenate without dedup (worst case = double-count
      for overlapping shards; documented behaviour — caller should check the log warning).

    Returns a merged DataFrame with a superset of shards from both buckets.
    """
    has_date = "date" in df_primary.columns and "date" in df_secondary.columns

    if not has_date:
        logger.warning(
            "  MERGE: 'date' column absent in one or both buckets — concatenating without dedup "
            "(double-count possible for overlapping shards). Use --no-merge to suppress."
        )
        return pd.concat([df_primary, df_secondary], ignore_index=True)

    has_iid = "instrument_id" in df_primary.columns and "instrument_id" in df_secondary.columns
    shard_key = _SHARD_KEY_WITH_IID if has_iid else _SHARD_KEY
    if not has_iid:
        logger.warning(
            "  MERGE: 'instrument_id' absent in one or both buckets — deduplicating on %s only "
            "(instruments sharing a date/venue/data_type are collapsed into one row).",
            _SHARD_KEY,
        )

    # Add priority column so sort-then-drop_duplicates keeps the best status per shard.
    df_primary = df_primary.copy()
    df_secondary = df_secondary.copy()
    # .astype("int64") is mandatory, not cosmetic: when capture_status is
    # `category` dtype (read_dictionary hardening, see _read_parquet_safe),
    # Series.map() on a Categorical returns a Categorical result that SORTS BY
    # CATEGORY-DISCOVERY ORDER rather than numeric value order (verified: an
    # uncast map of {captured:0, attempted_failed:1, expected_unattempted:3}
    # sorted as [1, 0, 3] instead of [0, 1, 3]) — silently picking the WRONG
    # "best status" per shard. Casting to int64 forces correct numeric sort
    # regardless of the input column's dtype.
    df_primary["_priority"] = df_primary["capture_status"].map(lambda s: _STATUS_PRIORITY.get(s, 99)).astype("int64")
    df_secondary["_priority"] = (
        df_secondary["capture_status"].map(lambda s: _STATUS_PRIORITY.get(s, 99)).astype("int64")
    )

    combined = pd.concat([df_primary, df_secondary], ignore_index=True)
    # Sort ascending by priority so drop_duplicates(keep='first') keeps the best status.
    combined = combined.sort_values("_priority")
    combined = combined.drop_duplicates(subset=shard_key, keep="first")
    combined = combined.drop(columns=["_priority"])
    combined = combined.reset_index(drop=True)
    logger.info(
        "  MERGE: primary=%d rows + secondary=%d rows → merged=%d rows (dedup on %s)",
        len(df_primary),
        len(df_secondary),
        len(combined),
        shard_key,
    )
    return combined


def _select_primary_index(
    accessible: list[tuple[str, datetime | None, pd.DataFrame]],
    *,
    override: str | None,
    asset_group: str,
) -> int:
    """Return the index of the primary bucket in ``accessible``.

    Selection rules (hardened 2026-07-06 vs. surgery-bumped mtimes):
      1. ``override`` wins when it matches an accessible bucket by name.
      2. Otherwise return index 0 — accessible preserves tuple order from
         ``_MANIFEST_BUCKET_CANDIDATES[asset_group]``, whose first entry is the
         ``-prd`` bucket by construction.
      3. If ``override`` is set but no accessible bucket matches, log a warning
         and fall back to rule 2 (do not silently ignore an operator directive).
    """
    if override is not None:
        for i, (name, _ts, _df) in enumerate(accessible):
            if name == override:
                logger.info(
                    "  %s primary override active: %s",
                    asset_group,
                    override,
                )
                return i
        logger.warning(
            "  %s --primary-bucket=%s not accessible; falling back to tuple-order pin",
            asset_group,
            override,
        )
    return 0


def _warn_if_secondary_newer(
    asset_group: str,
    primary_name: str,
    primary_ts: datetime | None,
    secondaries: list[tuple[str, datetime | None, pd.DataFrame]],
) -> None:
    """Log a SURGERY-SIGNAL warning when a secondary bucket has a newer ``blob.updated``.

    ``blob.updated`` is no longer a selection criterion (see ``_select_primary_index``),
    but a secondary bucket with a newer mtime than the primary usually means the legacy
    bucket was rewritten (e.g. an ASTER-style corrective pass). Loudly surface that so
    reviewers can decide whether to switch primary via ``--primary-bucket`` for the run.
    """
    if primary_ts is None:
        return
    for name, ts, _df in secondaries:
        if ts is not None and ts > primary_ts:
            logger.warning(
                "  %s SURGERY-SIGNAL: secondary %s blob.updated=%s is NEWER than "
                "primary %s blob.updated=%s. Selection pinned to primary regardless; "
                "pass --primary-bucket=%s to override if the newer bucket is authoritative.",
                asset_group,
                name,
                ts.isoformat(),
                primary_name,
                primary_ts.isoformat(),
                name,
            )


def _read_manifest(
    asset_group: str,
    *,
    merge: bool = True,
    primary_bucket_override: str | None = None,
) -> pd.DataFrame | None:
    """Read the live availability manifest for an asset_group.

    Bucket selection (pinned-primary; hardened 2026-07-06):
      PRIMARY is the first accessible candidate in
      ``_MANIFEST_BUCKET_CANDIDATES[asset_group]`` tuple order. The tuple places
      ``-prd`` first for every asset_group, so prd wins by default. This is
      deterministic against surgery bumps on the legacy bucket's ``blob.updated``.
      Pass ``primary_bucket_override`` (CLI ``--primary-bucket=<name>``) to force a
      specific bucket when surgery or debugging demands it; if the override is not
      accessible, selection falls back to the tuple-order pin. A secondary bucket with
      a newer ``blob.updated`` than the primary triggers a SURGERY-SIGNAL log warning.

    Merge (when ``merge=True``): after primary is chosen, the secondary bucket is also
    read and merged. Non-prd holds the full expected_unattempted skeleton that prd
    lacks; merging gives accurate denominator counts without double-counting (dedup on
    day/venue/data_type preferring prd status).
    """
    candidates = _MANIFEST_BUCKET_CANDIDATES[asset_group]

    # Step 1: get blob timestamps + read parquets for all candidates.
    # bucket_info preserves tuple order — critical for the pinned-primary rule.
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket_info: list[tuple[str, datetime | None, pd.DataFrame | None]] = []
    for bucket_name in candidates:
        updated = _get_blob_updated(gcs_client, bucket_name)
        df = _read_parquet_safe(bucket_name)
        uri = f"gs://{bucket_name}/{_INDEX_BLOB_PATH}"
        if df is None:
            logger.info("  %s candidate %s: not accessible", asset_group, uri)
        else:
            ts_str = updated.isoformat() if updated else "unknown"
            logger.info(
                "  %s candidate %s: %d rows, blob.updated=%s",
                asset_group,
                uri,
                len(df),
                ts_str,
            )
        bucket_info.append((bucket_name, updated, df))

    # Step 2: filter to accessible candidates, preserving tuple order.
    accessible = [(name, ts, df) for name, ts, df in bucket_info if df is not None]
    if not accessible:
        logger.warning("  SKIP %s — no candidate manifest accessible", asset_group)
        return None
    if merge and len(accessible) < len(candidates):
        unreachable = [name for name in candidates if name not in {a[0] for a in accessible}]
        logger.warning(
            "  MERGE DISABLED for %s: legacy bucket(s) unreachable (%s), "
            "expected_unattempted skeleton may be incomplete",
            asset_group,
            ", ".join(unreachable),
        )

    # Step 3: pinned-primary selection (override wins, else tuple-order first).
    primary_idx = _select_primary_index(
        accessible,
        override=primary_bucket_override,
        asset_group=asset_group,
    )
    primary_name, primary_ts, primary_df = accessible[primary_idx]
    secondaries = [entry for i, entry in enumerate(accessible) if i != primary_idx]

    primary_uri = f"gs://{primary_name}/{_INDEX_BLOB_PATH}"
    primary_ts_str = primary_ts.isoformat() if primary_ts is not None else "unknown"
    logger.info(
        "  %s manifest SELECTED (pinned primary): %s (%d rows, blob.updated=%s)",
        asset_group,
        primary_uri,
        len(primary_df),
        primary_ts_str,
    )
    for name, ts, df in secondaries:
        uri = f"gs://{name}/{_INDEX_BLOB_PATH}"
        ts_str = ts.isoformat() if ts is not None else "unknown"
        logger.info(
            "  %s manifest NOT SELECTED (secondary): %s (%d rows, blob.updated=%s)",
            asset_group,
            uri,
            len(df),
            ts_str,
        )

    _warn_if_secondary_newer(asset_group, primary_name, primary_ts, secondaries)

    result_df = primary_df

    if merge and secondaries:
        # Re-read secondary as eu_only (pyarrow push-down filter) before merging.
        # The non-prd oracle can be 35.8M rows; only ~4.1M are expected_unattempted.
        # Reading eu_only keeps peak memory bounded while providing the full skeleton.
        for secondary_name, _ts, _secondary_full in secondaries:
            secondary_eu = _read_parquet_eu_only(secondary_name)
            if secondary_eu is not None:
                result_df = _merge_manifests(result_df, secondary_eu)
            else:
                logger.warning(
                    "  %s eu-only read failed for secondary %s — skipping merge",
                    asset_group,
                    secondary_name,
                )

    return result_df


def _count_statuses(df: pd.DataFrame) -> dict[str, int | float]:
    counts: dict[str, int | float] = {}
    for status in _CAPTURE_STATUSES:
        counts[status] = int((df["capture_status"] == status).sum())
    total = sum(int(v) for v in counts.values())
    counts["total"] = total
    # Reachable denominator excludes empty_confirmed (legitimate absence).
    reachable = counts["captured"] + counts["attempted_failed"] + counts["expected_unattempted"]
    counts["coverage_pct"] = round(counts["captured"] / reachable * 100, 2) if reachable else 100.0
    counts["all_shards_coverage_pct"] = round(counts["captured"] / total * 100, 2) if total else 0.0
    return counts


def _compute_coverage(
    dfs: dict[str, pd.DataFrame],
    *,
    diagnose: bool = False,
) -> dict[str, object]:
    """Compute all Layer-2 coverage projections + Layer-1 enumeration-completeness.

    Existing projections (preserved byte-for-byte compatible):
      by_asset_group, by_venue, by_venue_data_type

    New v2 projections:
      by_venue_instrument_type       — ag → venue → itype → counts
      by_venue_instrument_type_data_type — ag → venue → itype → dt → counts
      by_day                         — ag → date → counts

    New v2 top-level block:
      layer_1                        — AgLayer1Result per AG

    New v2 additive fields on by_asset_group[ag] cells:
      instrument_gates_download, denominator_complete, layer1_completeness_pct

    Args:
        diagnose: when True, the Layer-1 check populates per-AG diagnostic
            samples (EXPECTED-only / ENUMERATED-only / matched canonical keys)
            into layer_1.by_asset_group[ag].diagnostics.
    """
    by_asset_group: dict[str, object] = {}
    by_venue: dict[str, dict[str, object]] = {}
    by_venue_data_type: dict[str, dict[str, dict[str, object]]] = {}
    by_venue_instrument_type: dict[str, dict[str, dict[str, object]]] = {}
    by_venue_instrument_type_data_type: dict[str, dict[str, dict[str, dict[str, object]]]] = {}
    by_day: dict[str, dict[str, object]] = {}

    # Layer-1 check (enumeration completeness)
    layer_1_by_ag: dict[str, object] = {}
    checker = _get_completeness_module()
    check_fn = checker.check_enumeration_completeness
    filter_fn = checker.filter_manifest_to_expected

    for ag, df in dfs.items():
        # MVP read-time gate (task 2c, 2026-07-06) — filter df to
        # EXPECTED-in-scope rows for Layer-2 counting so that numerator +
        # denominator align at the MVP grain.  ZERO manifest rows mutated
        # (the input df is untouched; the gate returns a filtered VIEW).
        # Layer-1 continues to consume the FULL, unfiltered df below so that
        # stray tuples (writer emitting something UAC doesn't sanction) stay
        # visible in `stray_tuples`.
        if ag in _MVP_READ_TIME_GATE_AGS:
            logger.info("  Applying MVP read-time gate for %s (Layer-2 in-scope filter) …", ag)
            df_l2 = filter_fn(ag, df)
        else:
            df_l2 = df

        # level 1 — per asset_group
        ag_counts = _count_statuses(df_l2)
        by_asset_group[ag] = ag_counts

        # level 2 — per (ag, venue)
        # observed=True: mandatory once any grouper column can be `category`
        # dtype (read_dictionary hardening, see _read_parquet_safe) — without
        # it, pandas' groupby synthesises a phantom EMPTY group for every
        # category value that was never actually observed together in this
        # (possibly MVP-filtered) slice, injecting bogus zero-count cells into
        # the coverage output. A no-op when grouper columns are plain object
        # dtype (legacy buckets), so this is safe either way.
        venue_group: dict[str, object] = {}
        for venue, vdf in df_l2.groupby("venue", observed=True):
            venue_group[str(venue)] = _count_statuses(vdf)
        by_venue[ag] = venue_group

        # level 3 — per (ag, venue, data_type)
        vdt_group: dict[str, dict[str, object]] = defaultdict(dict)
        for (venue, data_type), vtdf in df_l2.groupby(["venue", "data_type"], observed=True):
            vdt_group[str(venue)][str(data_type)] = _count_statuses(vtdf)
        by_venue_data_type[ag] = dict(vdt_group)

        # level 4 — per (ag, venue, instrument_type) [v2]
        vit_group: dict[str, dict[str, object]] = defaultdict(dict)
        if "instrument_type" in df_l2.columns:
            for (venue, itype), vitdf in df_l2.groupby(["venue", "instrument_type"], observed=True):
                vit_group[str(venue)][str(itype)] = _count_statuses(vitdf)
        else:
            logger.warning(
                "  [%s] instrument_type column absent — by_venue_instrument_type will be empty",
                ag,
            )
        by_venue_instrument_type[ag] = dict(vit_group)

        # level 5 — per (ag, venue, instrument_type, data_type) [v2]
        vitdt_group: dict[str, dict[str, dict[str, object]]] = defaultdict(lambda: defaultdict(dict))
        if "instrument_type" in df_l2.columns:
            for (venue, itype, dt), vitdtdf in df_l2.groupby(["venue", "instrument_type", "data_type"], observed=True):
                vitdt_group[str(venue)][str(itype)][str(dt)] = _count_statuses(vitdtdf)
        by_venue_instrument_type_data_type[ag] = {v: dict(it_map) for v, it_map in vitdt_group.items()}

        # level 6 — per (ag, date) [v2]
        day_group: dict[str, object] = {}
        if "date" in df_l2.columns:
            for day_val, daydf in df_l2.groupby("date", observed=True):
                day_group[str(day_val)] = _count_statuses(daydf)
        else:
            logger.warning("  [%s] date column absent — by_day will be empty", ag)
        by_day[ag] = day_group

        # Layer-1 enumeration completeness check — uses the UNFILTERED df so
        # stray tuples (writer emissions UAC doesn't sanction) remain visible.
        logger.info("  Running Layer-1 completeness check for %s …", ag)
        try:
            l1_result = check_fn(ag, df, diagnose=diagnose)
            layer_1_by_ag[ag] = l1_result.as_dict()
            # Add additive fields onto the existing AG counts cell.
            # When denominator_status == "UNDEFINED" (EXPECTED==0), completeness_pct
            # is None and instrument_gates_download is True (fail closed).
            ag_cell = cast(dict[str, object], by_asset_group[ag])
            ag_cell["denominator_complete"] = l1_result.denominator_complete
            ag_cell["denominator_status"] = l1_result.denominator_status
            ag_cell["layer1_completeness_pct"] = l1_result.completeness_pct
            ag_cell["instrument_gates_download"] = not l1_result.denominator_complete
            if l1_result.denominator_status == "UNDEFINED":
                logger.error(
                    "  [%s] Layer-1 UNDEFINED (EXPECTED==0) — denominator not wired. CK3 cannot certify this AG.",
                    ag,
                )
            elif not l1_result.denominator_complete:
                logger.warning(
                    "  [%s] Layer-1 INCOMPLETE (%.1f%%) — Layer-2 coverage is a LOWER BOUND. Missing tuples: %d",
                    ag,
                    l1_result.completeness_pct,
                    len(l1_result.missing_tuples),
                )
        except Exception as exc:
            logger.warning("  [%s] Layer-1 check failed: %s — skipping", ag, exc)
            ag_cell = cast(dict[str, object], by_asset_group[ag])
            ag_cell["instrument_gates_download"] = True
            ag_cell["denominator_complete"] = False
            ag_cell["denominator_status"] = "UNDEFINED"
            ag_cell["layer1_completeness_pct"] = None
            layer_1_by_ag[ag] = {"error": str(exc)}

    return {
        "by_asset_group": by_asset_group,
        "by_venue": by_venue,
        "by_venue_data_type": by_venue_data_type,
        "by_venue_instrument_type": by_venue_instrument_type,
        "by_venue_instrument_type_data_type": by_venue_instrument_type_data_type,
        "by_day": by_day,
        "layer_1": {"by_asset_group": layer_1_by_ag},
    }


def _write_output(payload: dict[str, object], output_path: str | None) -> None:
    blob_bytes = json.dumps(payload, indent=2).encode()

    if output_path:
        with open(output_path, "wb") as f:
            f.write(blob_bytes)
        logger.info("Wrote coverage JSON to %s", output_path)
        return

    run_date = payload["date"]
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(_OUTPUT_BUCKET)
    blob = bucket.blob(f"{run_date}/coverage.json")
    blob.upload_from_string(blob_bytes, content_type="application/json")
    logger.info("Wrote gs://%s/%s/coverage.json", _OUTPUT_BUCKET, run_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure cross-asset-group honest coverage")
    parser.add_argument(
        "--asset-group",
        default="all",
        choices=[*_KNOWN_ASSET_GROUPS, "all"],
        help="Asset group to measure (default: all)",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Local file path for output (default: write to GCS)",
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        default=False,
        help=(
            "Disable prd/non-prd manifest merging. Falls back to primary-only. "
            "Use when you want to measure a single bucket in isolation without combining "
            "the expected_unattempted skeleton from the secondary bucket."
        ),
    )
    parser.add_argument(
        "--primary-bucket",
        default=None,
        help=(
            "Force PRIMARY selection to a specific bucket name (matched against "
            "_MANIFEST_BUCKET_CANDIDATES for the run's asset_groups). Overrides the "
            "default tuple-order pin. If not accessible for a given asset_group, that "
            "AG falls back to the pinned primary. Use for surgery/debugging."
        ),
    )
    parser.add_argument(
        "--diagnose-layer1",
        action="store_true",
        default=False,
        help=(
            "Populate per-AG Layer-1 diagnostic samples (EXPECTED-only / "
            "ENUMERATED-only / matched canonical keys) into "
            "layer_1.by_asset_group[ag].diagnostics so residual holes can be "
            "verified REAL vs vocabulary/grain artifacts."
        ),
    )
    args = parser.parse_args()

    asset_groups = list(_KNOWN_ASSET_GROUPS) if args.asset_group == "all" else [args.asset_group]
    merge = not args.no_merge
    primary_bucket_override = args.primary_bucket

    dfs: dict[str, pd.DataFrame] = {}
    for ag in asset_groups:
        df = _read_manifest(
            ag,
            merge=merge,
            primary_bucket_override=primary_bucket_override,
        )
        if df is not None and not df.empty:
            dfs[ag] = df

    if not dfs:
        logger.error("No manifests loaded — nothing to measure")
        sys.exit(1)

    # Honest-absence: a PARTIAL run — some requested asset_groups failed to load
    # (typically an availability-index parquet that OOM'd inside _read_parquet_safe,
    # which swallows the error and returns None) — must be stamped LOUDLY, never
    # served as if complete. Without this, a partial file (e.g. defi-only) is
    # indistinguishable from a healthy full run and the Honest Coverage card
    # silently renders only the asset groups that happened to load. Consumers read
    # ``partial`` + ``asset_groups_failed`` to surface a "coverage incomplete" banner.
    asset_groups_failed = [ag for ag in asset_groups if ag not in dfs]
    partial = bool(asset_groups_failed)
    if partial:
        logger.error(
            "PARTIAL coverage run: %d/%d asset_groups failed to load (%s) — output "
            "marked partial=true. Most likely an availability-index read failure "
            "(OOM/transient); verify the runner VM has enough RAM for the largest "
            "single-asset-group parquet.",
            len(asset_groups_failed),
            len(asset_groups),
            ", ".join(asset_groups_failed),
        )

    coverage = _compute_coverage(dfs, diagnose=args.diagnose_layer1)

    now_utc = datetime.now(UTC)
    payload: dict[str, object] = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": date.today().isoformat(),
        "schema_version": 2,
        "asset_groups_requested": asset_groups,
        "asset_groups_measured": list(dfs.keys()),
        "asset_groups_failed": asset_groups_failed,
        "partial": partial,
        **coverage,
    }

    _write_output(payload, args.output_path)

    # Print per-asset-group summary to stdout for event-stream visibility
    print(f"\n=== Honest Coverage — {now_utc.strftime('%Y-%m-%d %H:%M')} UTC ===")
    print("  (reachable: captured / (captured + attempted_failed + expected_unattempted))")
    ag_counts = cast(dict[str, dict[str, int | float]], payload["by_asset_group"])
    for ag, counts in ag_counts.items():
        pct = counts["coverage_pct"]
        cap = counts["captured"]
        af = counts["attempted_failed"]
        eu = counts["expected_unattempted"]
        reachable = cap + af + eu
        print(f"  {ag:12s}: {pct:6.2f}%  ({cap:,}/{reachable:,} reachable)")


if __name__ == "__main__":
    main()
