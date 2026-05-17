#!/usr/bin/env python3
"""Reconcile phantom-captured manifest rows for ANY asset_group.

A phantom row claims ``capture_status=captured`` in the manifest but no
parquet exists at the canonical GCS path. This blocks the orchestrator's
``_should_skip_shard`` pre-flight (which trusts the manifest) into
permanently skipping the shard — every backfill VM exits doing nothing.

Pattern matches the 2026-04-29 sports phantom incident. This script ports
the same audit/flip logic to CeFi, DeFi, and Sports manifests, using a
single bulk-listing strategy: list once per ``(date, venue, data_type)``
prefix triple, then check each captured manifest row for membership.

Idempotent: ``attempted_failed`` rows are skipped, real captures are
left at ``captured``, only true phantoms get flipped.

**v8 column shape (codified 2026-05-12)**: this reconciler reads the manifest
via ``pd.read_parquet`` and modifies a small fixed set of columns
(``capture_status`` / ``error_reason`` / ``attempted_at``) at the specified
row indices, then writes back via ``df.to_parquet``. By construction this is
**read-tolerant** to new schema columns (the read accepts whatever columns
the parquet carries) and **write-preserving** (pandas DataFrame.to_parquet
preserves every column already on the dataframe). The v8 emission-tracking
columns added by ``gcs_migration_bundle_pipeline_mode_2026_05_08`` —
``pipeline_mode`` / ``service_emission_state`` / ``last_emission_decision_at``
/ ``expected_window_completeness_fraction`` — pass through transparently
without any reconciler-side handling. Rows written by pre-v8 writers (no
new columns on disk) round-trip with the columns absent; rows written by
post-v8 writers round-trip with the columns intact. No special-case logic
needed in this script.

Usage::

    cd instruments-service
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group cefi --dry-run
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group defi
    .venv/bin/python scripts/reconcile_phantom_manifest_rows_all.py \\
        --asset-group sports --venues BINANCE-FUTURES,BINANCE-SPOT

The script reads/writes the canonical
``gs://market-data-tick-{asset_group}-{pid}/_index/availability_index.parquet``
(or ``instruments-store-sports-{pid}`` for sports). Per-VM shards untouched.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from requests.adapters import HTTPAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Asset-group → (canonical bucket, manifest blob, day-list prefix templates).
#
# The hive-key for asset-group has TWO live values in GCS due to the
# 2026-04 vocabulary rename:
#   - ``category=`` (legacy, dominant for pre-2024 + still emitted by
#     unmigrated writers)
#   - ``asset_group=`` (new canonical, emitted by all post-rename writers)
# A phantom audit MUST probe BOTH or we false-positive every legacy row.
# The 2026-05-01 incident (181k false phantoms on CeFi) was caused by
# probing only the new key.  We now list under each candidate prefix and
# treat the row as real if ANY prefix has at least one parquet.
ASSET_GROUP_CONFIG: dict[str, dict[str, list[str] | str]] = {
    "cefi": {
        "bucket": f"market-data-tick-cefi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # 4 path shapes coexist on disk; ALL must be probed:
        #   (a) raw_tick_data/by_date prefix + asset_group= hive (canonical)
        #   (b) raw_tick_data/by_date prefix + category= hive (legacy)
        #   (c) top-level + asset_group= hive (Tardis adapter via build_partition_path)
        #   (d) top-level + category= hive (older Tardis adapter)
        # Earlier audit only probed (a) + (b) and false-positived 130k rows
        # whose data lives at (c)/(d). 2026-05-03: extended to all 4 shapes.
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=cefi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=cefi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=cefi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=cefi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "defi": {
        "bucket": f"market-data-tick-defi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # DeFi layout has venue + chain (no instrument_type segment in older
        # paths). Probe new + legacy hive keys + no-asset-group + top-level
        # (no raw_tick_data/by_date/ prefix) variants.
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            # 2024-05-era DeFi paths skipped the asset-group hive segment.
            "raw_tick_data/by_date/day={date}/venue={venue}/chain={chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=defi/venue={venue}/"
            "chain={chain}/instrument_type={instrument_type}/data_type={data_type}/",
            # Legacy ``venue=PROTOCOL-CHAIN`` overload — pre-2026-04-29
            # EIGENLAYER restaking + a few other DeFi adapters wrote
            # ``venue=EIGENLAYER-ETHEREUM`` (no separate ``chain=`` segment).
            # ``rebuild_defi_manifest.py`` decomposes that back to
            # ``(venue=EIGENLAYER, chain=ETHEREUM)`` in manifest rows; the
            # audit must also probe the combined layout so those rows aren't
            # false-flagged as phantoms.  Verified 2026-05-04: 597 EIGENLAYER
            # restaking rewards live at this layout.
            "raw_tick_data/by_date/day={date}/asset_group=defi/venue={venue}-{chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=defi/venue={venue}-{chain}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "sports": {
        "bucket": f"instruments-store-sports-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # Sports has its own SSOT (per-league + bare paths) — handled
        # separately via the unified UAC dispatcher below.
        "prefix_tpls": [""],
    },
    "tradfi": {
        "bucket": f"market-data-tick-tradfi-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=tradfi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "raw_tick_data/by_date/day={date}/category=tradfi/venue={venue}/"
            "instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/asset_group=tradfi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
            "day={date}/category=tradfi/venue={venue}/instrument_type={instrument_type}/data_type={data_type}/",
        ],
    },
    "prediction": {
        "bucket": f"market-data-tick-prediction-{PROJECT_ID}",
        "index": "_index/availability_index.parquet",
        # Polymarket parquets live under a 9-segment hive layout that puts
        # ``data_source=POLYMARKET_CLOB`` BETWEEN ``category=prediction``
        # and ``venue=``, with ``market_category`` / ``underlying`` /
        # ``market_type`` / ``resolution_period`` segments between
        # ``chain=`` and ``data_type=``.  ``instrument_type=prediction_market``
        # is a manifest-row attribute, NOT a hive segment on disk.
        # We list at the day-level prediction prefix; the substring-match
        # logic below verifies ``venue={V}/`` + ``data_type={DT}/`` membership
        # (instrument_type check is skipped for ``prediction_market`` rows
        # because the segment doesn't exist on disk).
        "prefix_tpls": [
            "raw_tick_data/by_date/day={date}/asset_group=prediction/",
            "raw_tick_data/by_date/day={date}/category=prediction/",
            "day={date}/asset_group=prediction/",
            "day={date}/category=prediction/",
        ],
    },
}


# Protocol-name underscore drift (added 2026-05-07 for AAVE_V3-ARBITRUM
# C.9 audit per session_2026_05_07_data_status_audit_findings wrapper).
# Pre-canonicalisation DeFi writers spelled the protocol with an underscore
# between the protocol name and the version: ``AAVE_V3``, ``UNISWAP_V3``,
# ``COMPOUND_V3``. The post-2026-04 canonical form drops the underscore:
# ``AAVEV3``, ``UNISWAPV3``, ``COMPOUNDV3``. The migrate_mtds_defi_legacy_*
# scripts left BOTH spellings on disk under different ``venue=`` segments;
# the manifest carries only the canonical form. Without probing both
# variants, the audit false-positives 100% of AAVEV3 / UNISWAPV3 etc.
# rows as phantom (29,782 hits in the 2026-05-07 dry-run on AAVEV3 alone).
_PROTOCOL_VERSION_UNDERSCORE_RE = re.compile(r"^([A-Z]+?)(V\d+)(.*)$")

# Axis-7 (Databento per-schema-bundle, 2026-05-13): TradFi Databento
# downloads ``trades`` and ``tbbo`` as a PAIRED schema set in one API call.
# Both data_types share the same venue-level prefix on disk. A manifest row
# for ``trades`` may physically live under ``data_type=tbbo/`` (and vice
# versa) when the Databento adapter bundles both.  Accept the paired schema
# needle as evidence of capture — without this, both data_types produce
# identical 1,017-count false-positive phantoms (same instruments, same dates).
_TRADFI_DATABENTO_PAIRED_SCHEMAS: dict[str, list[str]] = {
    "trades": ["tbbo"],
    "tbbo": ["trades"],
}

# Axis-8 (cross-asset venue-less, 2026-05-13): manifest rows with
# venue=UNKNOWN have no resolvable canonical GCS path. Probing
# ``venue=UNKNOWN/`` never matches → ~565 TradFi + ~2k cross-asset false
# positives. Treat UNKNOWN as venue-agnostic (skip venue needle) so the
# data_type probe alone decides real-vs-phantom.
_VENUE_UNKNOWN_SENTINELS: frozenset[str] = frozenset({"UNKNOWN"})


def _defi_protocol_variants(venue: str) -> list[str]:
    """Return deduped list of venue spellings for DeFi.

    Both the underscored and concatenated forms of ``PROTOCOL[_]V<digits>``
    are returned. Works for plain venue (``AAVEV3``) and combined-venue
    (``AAVEV3-ETHEREUM`` — the protocol part before the first ``-``
    is transformed and the chain suffix preserved).

    Examples:
        AAVEV3            -> [AAVEV3, AAVE_V3]
        AAVE_V3           -> [AAVE_V3, AAVEV3]
        AAVEV3-ARBITRUM   -> [AAVEV3-ARBITRUM, AAVE_V3-ARBITRUM]
        UNISWAPV3         -> [UNISWAPV3, UNISWAP_V3]
        MORPHO            -> [MORPHO]                       (no version suffix)
        EIGENLAYER-ETHEREUM -> [EIGENLAYER-ETHEREUM]        (no version suffix)
    """
    if not venue:
        return [""]
    if "-" in venue:
        protocol, _sep, chain_suffix = venue.partition("-")
        chain_suffix = "-" + chain_suffix
    else:
        protocol = venue
        chain_suffix = ""
    variants = {venue}
    # Try removing underscore: AAVE_V3 -> AAVEV3
    if "_" in protocol:
        variants.add(protocol.replace("_", "") + chain_suffix)
    # Try inserting underscore: AAVEV3 -> AAVE_V3
    m = _PROTOCOL_VERSION_UNDERSCORE_RE.match(protocol)
    if m:
        head, ver, tail = m.groups()
        variants.add(f"{head}_{ver}{tail}{chain_suffix}")
    return list(variants)


def _venue_level_prefixes(asset_group: str, row: pd.Series) -> list[str]:
    """Return one prefix per ``(date, venue[, chain], hive-vocab)`` for
    a manifest row.

    We list ONCE per venue-level prefix, then substring-match
    ``data_type={dt}/`` and ``instrument_type={it}/`` (case-insensitive)
    in the returned keys.  This is robust to:

    1. **Hive-key vocabulary drift** — ``category=`` (legacy) and
       ``asset_group=`` (post-rename) coexist; both probed.
    2. **instrument_type casing** — manifest holds ``PERPETUAL`` /
       ``perpetual`` interchangeably; disk only has lowercase. Membership
       check is case-insensitive.
    3. **Empty ``instrument_type``** — schema-4 manifest rows omit the
       segment. We accept ANY parquet under
       ``venue/.../data_type={dt}/`` as evidence of capture.
    4. **Path-prefix drift** (2026-05-03) — Tardis-adapter writes via
       ``build_partition_path`` lived at top-level ``day={D}/...`` while
       orchestrator-direct writes used ``raw_tick_data/by_date/day={D}/...``.
       Both shapes coexist on disk; pre-2026-05-03 audits only probed the
       prefixed shape and false-positived 130k CeFi rows.
    5. **Chain-bundle equivalence** (2026-05-04) — manifest holds
       ``instrument_type=option`` / ``future`` (row-level) but the writer
       bundles those rows into ``options_chain/`` / ``futures_chain/``
       partitions on disk. The membership check accepts either form so
       OPTION/FUTURE manifest rows match their bundled disk locations.
    6. **DeFi protocol-name underscore drift** (2026-05-07) — manifest
       holds ``venue=AAVEV3`` (canonical, no underscore) but pre-2026-04
       writers used ``venue=AAVE_V3`` (underscored). Both spellings
       coexist on disk; we probe both via ``_defi_protocol_variants``.
       Without this, AAVEV3 / UNISWAPV3 / COMPOUNDV3 rows whose data
       lives under the legacy underscored prefix false-positive 100%
       as phantom (29,782 hits in the 2026-05-07 dry-run on AAVEV3 alone).

    Axes 7-9 are handled in ``_audit_generic`` / ``_audit_sports`` directly:
    7. **TradFi Databento per-schema-bundle** (2026-05-13) — ``trades`` and
       ``tbbo`` are downloaded and written as a paired set; accept either
       data_type needle as capture evidence. See ``_TRADFI_DATABENTO_PAIRED_SCHEMAS``.
    8. **Cross-asset venue=UNKNOWN** (2026-05-13) — UNKNOWN sentinel has no
       canonical path; skip the venue needle. See ``_VENUE_UNKNOWN_SENTINELS``.
    9. **Sports pre-coverage + known-gap** (2026-05-13) — rows before source
       launch date or in registered gaps are not phantoms; excluded in
       ``_audit_sports`` via ``is_pre_launch_date`` + ``is_in_known_gap``.
    """
    cfg = ASSET_GROUP_CONFIG[asset_group]
    raw_venue = str(row.get("venue", "") or "")
    raw_chain = str(row.get("chain", "") or "")
    # DeFi-only: probe BOTH protocol-name spellings (underscored + plain).
    venue_variants = _defi_protocol_variants(raw_venue) if asset_group == "defi" else [raw_venue]
    tpls = cfg["prefix_tpls"]
    if isinstance(tpls, str):
        tpls = [tpls]
    out: list[str] = []
    for v in venue_variants:
        base_fields = {
            "date": str(row["date"]),
            "venue": v,
            "chain": raw_chain,
        }
        for t in tpls:
            # Truncate template at the first hive segment AFTER venue (and
            # after chain for DeFi). The remainder narrows to one venue-day.
            stripped = t.split("instrument_type=")[0]
            # ``stripped`` may contain ``{venue}`` / ``{chain}`` / ``{date}``
            # placeholders only; safe to format with base_fields.
            out.append(stripped.format(**base_fields))
    return out


def _audit_sports(
    bucket: storage.Bucket,
    df: pd.DataFrame,
    captured_idx: pd.Index,
    workers: int,
) -> dict[int, bool]:
    """Sports uses per-league + bare path layout — delegate to UAC SSOT.

    Day-partitioned candidates (``sports_reference/by_date/day={D}/...``)
    are matched against a bulk listing per day.  Singleton flat-path
    candidates (``sports_reference/{folder}/{folder}.parquet``, e.g.
    VENUES) live OUTSIDE the day-partition tree and need direct
    ``bucket.blob(c).exists()`` probes — without that step every
    singleton row false-flags as phantom because it can't be in the
    day-partition listing by construction.
    """
    from unified_api_contracts.sports import (
        candidate_parquet_paths,
        get_source_for_data_type,
        is_in_known_gap,
        is_pre_launch_date,
    )

    # Bulk-list per day for all day-partitioned candidates.
    days = sorted({str(d) for d in df.loc[captured_idx, "date"].unique()})
    logger.info("sports phantom: listing %d unique days", len(days))
    day_blobs: dict[str, set[str]] = {}

    def _list_day(day: str) -> tuple[str, set[str]]:
        prefix = f"sports_reference/by_date/day={day}/"
        return day, {b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(_list_day, d) for d in days]):
            day, blobs = fut.result()
            day_blobs[day] = blobs

    # Per-singleton-path existence cache — populated lazily as rows are
    # audited so we only probe each unique singleton path once.
    day_partition_prefix = "sports_reference/by_date/day="
    singleton_exists: dict[str, bool] = {}

    def _singleton_real(path: str) -> bool:
        if path not in singleton_exists:
            try:
                singleton_exists[path] = bucket.blob(path).exists()
            except Exception:  # broad-except-ok: per-row failure isolation
                singleton_exists[path] = False
        return singleton_exists[path]

    # Probe each captured row.
    real_or_phantom: dict[int, bool] = {}  # idx -> True if real
    _axis9_pre_coverage = 0
    _axis9_known_gap = 0
    for idx in captured_idx:
        row = df.loc[idx]
        date = str(row["date"])
        data_type = str(row.get("data_type", "") or "")
        league_id = str(row.get("league_id", "") or "")
        # Axis-9 (sports per-league SSOT + UAC date-range clips, 2026-05-13):
        # Rows before the source's coverage start or inside a known gap are
        # NOT phantoms — the source never had data for that (data_type, date).
        # Flipping to attempted_failed would re-queue them for retry (wrong).
        # Mark real=True to exclude from phantom detection; the absence-reason
        # reconciler handles these rows separately.
        if is_pre_launch_date(data_type, date):
            real_or_phantom[idx] = True
            _axis9_pre_coverage += 1
            continue
        _src_key = get_source_for_data_type(data_type)
        if _src_key and is_in_known_gap(_src_key, data_type, date):
            real_or_phantom[idx] = True
            _axis9_known_gap += 1
            continue
        candidates = candidate_parquet_paths(data_type, date, league_id)
        blobs = day_blobs.get(date, set())
        is_real = False
        for c in candidates:
            if c.startswith(day_partition_prefix):
                if c in blobs:
                    is_real = True
                    break
            else:
                # Singleton flat-path (VENUES, future single-file entities) —
                # probe directly. The audit's day-listing strategy can't
                # cover paths outside the by_date/day=*/ tree.
                if _singleton_real(c):
                    is_real = True
                    break
        real_or_phantom[idx] = is_real
    if _axis9_pre_coverage or _axis9_known_gap:
        logger.info(
            "Sports axis-9 coverage clip: %d pre-launch + %d known-gap rows excluded from phantom check",
            _axis9_pre_coverage,
            _axis9_known_gap,
        )
    return real_or_phantom


def _audit_generic(
    asset_group: str,
    bucket: storage.Bucket,
    df: pd.DataFrame,
    captured_idx: pd.Index,
    workers: int,
) -> dict[int, bool]:
    """CeFi/DeFi/TradFi/Prediction use venue+data_type prefix layout.

    Strategy: list ONCE per unique ``(date, venue[, chain], hive-vocab)``
    prefix, then for each manifest row substring-match
    ``data_type={dt}/`` and (if specified) ``instrument_type={it}/``
    (case-insensitive) in the key set.  Robust to hive-key drift,
    instrument_type casing drift, and schema-4 empty-instrument_type
    rows — see ``_venue_level_prefixes`` for rationale.

    Phantom = no key in any candidate prefix's listing matches the
    row's ``data_type`` (and ``instrument_type`` if non-empty).
    """
    # Each captured row maps to a list of candidate venue-level prefixes.
    prefixes_by_idx: dict[int, list[str]] = {}
    for idx in captured_idx:
        prefixes_by_idx[idx] = _venue_level_prefixes(asset_group, df.loc[idx])
    unique_prefixes = sorted({p for plist in prefixes_by_idx.values() for p in plist if p})
    logger.info(
        "%s phantom: %d unique (date, venue[, chain], hive-vocab) prefixes to list",
        asset_group,
        len(unique_prefixes),
    )

    prefix_keys: dict[str, set[str]] = {}

    def _list(prefix: str) -> tuple[str, set[str]]:
        try:
            keys = {b.name for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")}
            return prefix, keys
        except Exception as exc:
            logger.warning("list error for %s: %s", prefix, exc)
            return prefix, set()

    completed = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_list, p) for p in unique_prefixes]
        for fut in as_completed(futs):
            prefix, keys = fut.result()
            prefix_keys[prefix] = keys
            completed += 1
            if completed % 500 == 0:
                rate = completed / max(0.01, time.time() - t0)
                logger.info(
                    "  %d/%d prefixes listed (%.1f/sec, ETA %.1fs)",
                    completed,
                    len(unique_prefixes),
                    rate,
                    (len(unique_prefixes) - completed) / max(0.01, rate),
                )

    # Chain-bundle equivalence: writers bundle OPTION rows into
    # ``instrument_type=options_chain/`` partitions and FUTURE rows into
    # ``instrument_type=futures_chain/`` (per
    # ``market_tick_data_service/.../cefi/tardis_shared.finalise_rows_and_path``).
    # Manifest holds the row-level type (``option`` / ``future``); disk has
    # the chain form. Audit must accept either.
    it_disk_equiv: dict[str, list[str]] = {
        "option": ["option", "options_chain"],
        "future": ["future", "futures_chain"],
    }

    # instrument_type values that exist as manifest attributes but NOT as
    # hive segments on disk. Skip the instrument_type substring check for
    # rows with these values — they're identifier-only, not partitioning.
    # ``prediction_market`` is the only example today (Polymarket layout
    # uses ``market_type=binary/range_bracket/...`` instead).
    it_not_on_disk: frozenset[str] = frozenset({"prediction_market"})

    # DeFi migrated-bundle wildcard (added 2026-05-07 for C.9 audit) —
    # ``migrate_mtds_defi_legacy_venue_underscore.py`` produced
    # ``ticks_migrated_*.parquet`` bundle files that live at the
    # combined-venue prefix (``raw_tick_data/by_date/day=*/asset_group=defi/
    # venue=PROTOCOL-CHAIN/``) WITHOUT the trailing ``instrument_type=*/
    # data_type=*/`` segments. The bundle pickle holds ALL data_types for
    # that (date, protocol, chain) tuple. Without this wildcard the
    # membership check fails because the path has no ``data_type={dt}/``
    # substring and the audit false-positives every row whose data was
    # migrated into a bundle.
    migrated_bundle_needle = "/ticks_migrated_"
    real_or_phantom: dict[int, bool] = {}
    for idx, plist in prefixes_by_idx.items():
        row = df.loc[idx]
        data_type = str(row.get("data_type", "") or "")
        raw_it = str(row.get("instrument_type", "") or "")
        venue = str(row.get("venue", "") or "")
        chain = str(row.get("chain", "") or "")
        dt_needle = f"data_type={data_type}/"
        # Axis-7: for TradFi, Databento writes ``trades`` + ``tbbo`` under
        # the same prefix. Accept either paired data_type needle as capture.
        extra_dt_needles: list[str] = []
        if asset_group == "tradfi":
            for _paired in _TRADFI_DATABENTO_PAIRED_SCHEMAS.get(data_type, []):
                extra_dt_needles.append(f"data_type={_paired}/")
        # Venue substring needle — required when prefix templates truncate
        # at category/asset_group level (e.g. prediction's Polymarket layout
        # interposes ``data_source=`` between category and venue, so the
        # prefix can't pin venue).  Empty venue → no needle (skip check).
        # Axis-8: venue=UNKNOWN has no resolvable path — skip the check.
        # DeFi-only: accept ANY protocol-name spelling variant (AAVEV3 vs
        # AAVE_V3 etc.) as the venue match. Without this, manifest rows holding
        # the underscored spelling are flagged phantom even though their data
        # lives at the non-underscored disk path (and vice versa) — confirmed
        # 2026-05-17 on the lending-indices bucket (60 false phantoms reported
        # for AAVEV3 + COMPOUNDV3 rows whose data lives at venue=AAVE_V3 /
        # venue=COMPOUND_V3). The PREFIX template already probes both via
        # ``_defi_protocol_variants`` (line 290) — the substring NEEDLE needs
        # the same treatment.
        if not venue or venue.upper() in _VENUE_UNKNOWN_SENTINELS:
            venue_needles_any: list[str] = []
        elif asset_group == "defi":
            venue_needles_any = [f"venue={v}/" for v in _defi_protocol_variants(venue)]
        else:
            venue_needles_any = [f"venue={venue}/"]
        # Case-insensitive instrument_type needle. Empty manifest value
        # means "any instrument_type counts" (schema-4 rows). Identifier-
        # only types like ``prediction_market`` skip the segment check.
        it_lower = raw_it.lower()
        if it_lower in it_disk_equiv:
            it_needles_lower = [f"instrument_type={v}/" for v in it_disk_equiv[it_lower]]
        elif it_lower and it_lower not in it_not_on_disk:
            it_needles_lower = [f"instrument_type={it_lower}/"]
        else:
            it_needles_lower = []
        # Combined-venue bundle needles: when probing ``venue=PROTOCOL-CHAIN/``
        # accept ``ticks_migrated_*.parquet`` files as evidence of capture
        # for ANY (data_type, instrument_type) under that protocol-chain
        # combo. DeFi-only — the migration bundle pattern is not used by
        # other asset_groups.
        bundle_venue_needles: list[str] = []
        if asset_group == "defi" and venue and chain:
            for v in _defi_protocol_variants(venue):
                # Take protocol-only part (strip pre-existing chain suffix
                # if the variant was already combined-form).
                proto = v.split("-", 1)[0] if "-" in v else v
                bundle_venue_needles.append(f"venue={proto}-{chain}/")
        is_real = False
        for prefix in plist:
            keys = prefix_keys.get(prefix, set())
            for k in keys:
                # Migrated-bundle wildcard: any ``ticks_migrated_*.parquet``
                # at a combined-venue prefix matching one of our protocol
                # spellings counts as capture for any data_type. Bundle
                # files have no data_type/instrument_type segments.
                if bundle_venue_needles and migrated_bundle_needle in k and any(b in k for b in bundle_venue_needles):
                    is_real = True
                    break
                # Axis-7: accept primary or Databento-paired schema needle.
                if dt_needle not in k and not any(pn in k for pn in extra_dt_needles):
                    continue
                if venue_needles_any and not any(vn in k for vn in venue_needles_any):
                    continue
                if it_needles_lower:
                    k_lower = k.lower()
                    if not any(it in k_lower for it in it_needles_lower):
                        continue
                is_real = True
                break
            if is_real:
                break
        real_or_phantom[idx] = is_real
    return real_or_phantom


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset-group", required=True, choices=list(ASSET_GROUP_CONFIG.keys()))
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--venues", type=str, default="", help="Comma-separated venues to scope (default: all)")
    p.add_argument("--data-types", type=str, default="", help="Comma-separated data_types to scope")
    p.add_argument("--workers", type=int, default=32)
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="Scope audit to dates >= YYYY-MM-DD (inclusive). Useful for local testing on a short window.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="Scope audit to dates <= YYYY-MM-DD (inclusive). Useful for local testing on a short window.",
    )
    p.add_argument(
        "--unphantom",
        action="store_true",
        help=(
            "Also re-validate rows previously flagged with "
            "error_reason='phantom_captured_no_parquet_at_canonical_path'. If a "
            "parquet now exists at any candidate path (UAC SSOT), flip the row "
            "back to capture_status='captured' and clear error_reason. Self-heals "
            "false-positives produced by earlier audit versions whose path probing "
            "didn't cover the row's layout (e.g. the 2026-05-04 sports audit "
            "missed FLAT-layout singletons like VENUES, leaving them stuck as "
            "attempted_failed even after the singleton-fallback fix landed)."
        ),
    )
    p.add_argument(
        "--manifest-bucket",
        type=str,
        default="",
        help=(
            "Override the manifest bucket (default: the per-asset_group bucket in "
            "ASSET_GROUP_CONFIG). Required for DeFi per-data-type buckets like "
            "lending-indices-{pid} / lst-rates-{pid} / oracle-prices-{pid} / "
            "perp-funding-{pid} / eigenlayer-rewards-{pid} that hold their own "
            "manifest separate from the central market-data-tick-defi-{pid}. "
            "Routes BOTH manifest read AND prefix-path probing to this bucket. "
            "Added 2026-05-17 after the lending-indices phantom-flip one-shot "
            "(plans/active/issues/lending_indices_phantom_manifest_rows_2026_05_17.md)."
        ),
    )
    p.add_argument(
        "--manifest-index",
        type=str,
        default="",
        help=(
            "Override the manifest blob path inside the manifest bucket (default: _index/availability_index.parquet)."
        ),
    )
    args = p.parse_args()

    cfg = dict(ASSET_GROUP_CONFIG[args.asset_group])
    # Per-data-type bucket overrides: rewire cfg in-place so all downstream
    # logic (manifest read + prefix-path probing + write-back) hits the
    # override bucket. The prefix templates remain the asset_group-level set —
    # the per-data-type buckets in DeFi all share the same path layout as
    # the central bucket (raw_tick_data/by_date/day=*/asset_group=defi/...).
    if args.manifest_bucket:
        cfg["bucket"] = args.manifest_bucket
        logger.info("Manifest bucket override: %s", args.manifest_bucket)
    if args.manifest_index:
        cfg["index"] = args.manifest_index
        logger.info("Manifest blob override: %s", args.manifest_index)
    # Bump GCS HTTP connection pool to match worker count; the default of 10
    # silently truncates list_blobs() results under high concurrency. The
    # 2026-05-04 CeFi audit produced 12k false-positive phantoms from this —
    # connections were "discarded" mid-listing and partial results bubbled
    # back as missing-key. Pattern from migrate_polymarket_canonical.py.
    pool_size = max(args.workers * 2, 64)
    client = storage.Client(project=PROJECT_ID)
    try:
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=3)
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
        logger.info("GCS HTTP pool tuned: pool_size=%d (workers=%d)", pool_size, args.workers)
    except (AttributeError, TypeError):
        logger.warning("Could not tune GCS HTTP pool — falling back to default 10")
    bucket = client.bucket(cfg["bucket"])
    blob = bucket.blob(cfg["index"])

    logger.info("Loading manifest from gs://%s/%s", cfg["bucket"], cfg["index"])
    # Per-invocation temp file so concurrent runs (one per asset_group) don't
    # clobber each other's downloads. Bandit B108: use tempfile, not /tmp.
    with tempfile.NamedTemporaryFile(prefix=f"recon-{args.asset_group}-", suffix=".parquet", delete=False) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        import contextlib

        with contextlib.suppress(OSError):
            os.unlink(manifest_path)
    logger.info("Manifest rows: %d", len(df))

    captured_mask = df["capture_status"].fillna("") == "captured"
    if args.venues:
        wanted_venues = {v.strip() for v in args.venues.split(",") if v.strip()}
        captured_mask = captured_mask & df["venue"].isin(wanted_venues)
    if args.data_types:
        wanted_dts = {d.strip() for d in args.data_types.split(",") if d.strip()}
        captured_mask = captured_mask & df["data_type"].isin(wanted_dts)
    if args.start_date:
        captured_mask = captured_mask & (df["date"].astype(str) >= args.start_date)
    if args.end_date:
        captured_mask = captured_mask & (df["date"].astype(str) <= args.end_date)

    # 2026-05-04: drop schema_v4 vestigial rows from audit scope. These are
    # pre-v5 daily-manifest records with only ``venue`` populated (no
    # ``data_type``, ``instrument_type``, etc.) and represent informational
    # "this venue was touched on this date" markers, not real shards. The
    # audit can't probe an empty ``data_type=`` substring, so these
    # systematically false-positive as phantoms (9,757 rows on 2026-05-04
    # CeFi). They're harmless legacy and should be filtered out, not flipped
    # to attempted_failed (which would force VMs to retry venues that don't
    # have a target data_type to retry against).
    if "schema_version" in df.columns:
        v4_empty_dt = (df["schema_version"] == 4) & (df["data_type"].fillna("").astype(str).str.len() == 0)
        v4_in_scope = (captured_mask & v4_empty_dt).sum()
        if v4_in_scope > 0:
            logger.info(
                "Dropping %d schema_v4 vestigial rows (empty data_type — "
                "pre-v5 informational manifest records, not real shards)",
                v4_in_scope,
            )
        captured_mask = captured_mask & ~v4_empty_dt

    captured_idx = df[captured_mask].index
    logger.info("Captured rows in scope: %d", len(captured_idx))
    # Early exit only when there's no forward pass AND no reverse-unphantom pass to do.
    # (--unphantom alone, with no captured rows in scope, is still meaningful.)
    if len(captured_idx) == 0 and not args.unphantom:
        logger.info("Nothing to audit. Exiting.")
        return 0

    real_or_phantom: dict[int, bool] = {}
    if len(captured_idx) > 0:
        if args.asset_group == "sports":
            real_or_phantom = _audit_sports(bucket, df, captured_idx, args.workers)
        else:
            real_or_phantom = _audit_generic(args.asset_group, bucket, df, captured_idx, args.workers)

    phantom_idx = [i for i, real in real_or_phantom.items() if not real]
    real_count = sum(1 for r in real_or_phantom.values() if r)
    logger.info("=" * 60)
    logger.info("Audit summary (forward — captured rows missing parquet):")
    logger.info("  Real captures:    %d", real_count)
    logger.info("  Phantom captures: %d  ← will flip to attempted_failed", len(phantom_idx))
    logger.info("=" * 60)

    # Reverse pass: re-validate previously phantom-flagged rows. The forward
    # audit can only ADD phantoms — it never UN-flags rows that earlier audit
    # versions wrongly marked. With --unphantom we also probe the
    # ``attempted_failed`` ∩ ``error_reason='phantom_*'`` subset and flip rows
    # back to ``captured`` if a parquet now exists at any UAC candidate path.
    unphantom_idx: list[int] = []
    if args.unphantom:
        phantom_flagged_mask = (df["capture_status"].fillna("") == "attempted_failed") & (
            df["error_reason"].fillna("") == "phantom_captured_no_parquet_at_canonical_path"
        )
        if args.venues:
            wanted_venues = {v.strip() for v in args.venues.split(",") if v.strip()}
            phantom_flagged_mask = phantom_flagged_mask & df["venue"].isin(wanted_venues)
        if args.data_types:
            wanted_dts = {d.strip() for d in args.data_types.split(",") if d.strip()}
            phantom_flagged_mask = phantom_flagged_mask & df["data_type"].isin(wanted_dts)
        phantom_flagged_idx = df[phantom_flagged_mask].index
        logger.info("Unphantom: re-validating %d previously phantom-flagged rows", len(phantom_flagged_idx))
        if len(phantom_flagged_idx) > 0:
            if args.asset_group == "sports":
                rev = _audit_sports(bucket, df, phantom_flagged_idx, args.workers)
            else:
                rev = _audit_generic(args.asset_group, bucket, df, phantom_flagged_idx, args.workers)
            unphantom_idx = [i for i, real in rev.items() if real]
            logger.info(
                "  Still phantom: %d  (parquet missing — leave as attempted_failed)",
                len(phantom_flagged_idx) - len(unphantom_idx),
            )
            logger.info("  Unphantomed:   %d  ← will flip back to captured", len(unphantom_idx))
            if unphantom_idx:
                up_df = df.loc[unphantom_idx]
                up_by_dt = up_df.groupby(["data_type"]).size().sort_values(ascending=False)
                logger.info("Unphantom distribution by data_type (top 15):\n%s", up_by_dt.head(15).to_string())
        logger.info("=" * 60)

    if not phantom_idx and not unphantom_idx:
        logger.info("No phantoms found and nothing to unphantom. Manifest is clean.")
        return 0

    # Show forward-phantom distribution
    if phantom_idx:
        phantom_df = df.loc[phantom_idx]
        by_dt = phantom_df.groupby(["data_type"]).size().sort_values(ascending=False)
        logger.info("Phantom distribution by data_type (top 15):\n%s", by_dt.head(15).to_string())
        if "venue" in phantom_df.columns:
            by_v = phantom_df.groupby(["venue"]).size().sort_values(ascending=False)
            logger.info("Phantom distribution by venue (top 15):\n%s", by_v.head(15).to_string())

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    if phantom_idx:
        df.loc[phantom_idx, "capture_status"] = "attempted_failed"
        df.loc[phantom_idx, "error_reason"] = "phantom_captured_no_parquet_at_canonical_path"
        df.loc[phantom_idx, "attempted_at"] = now_iso
    if unphantom_idx:
        df.loc[unphantom_idx, "capture_status"] = "captured"
        df.loc[unphantom_idx, "error_reason"] = ""
        df.loc[unphantom_idx, "attempted_at"] = now_iso

    # Write back. v8 columns (pipeline_mode / service_emission_state /
    # last_emission_decision_at / expected_window_completeness_fraction)
    # ride along untouched — pd.DataFrame.to_parquet preserves every
    # column on the frame, so any column present on disk at read time
    # round-trips back unchanged. Pre-v8 manifests (no new columns) also
    # round-trip cleanly because the read-side never invents columns.
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info(
        "Uploading reconciled manifest (%d rows, %d phantoms flipped, %d unphantomed)",
        len(df),
        len(phantom_idx),
        len(unphantom_idx),
    )
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
