#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
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
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime

import pandas as pd
from unified_api_contracts import canonical_path_templates
from unified_trading_library import StorageClient, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

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
# Maps asset_group → (bucket-kind, asset_group arg for resolve_bucket_name).
# The kind must match keys in deployment-service/configs/cloud-providers.yaml.
# Sports uses 'instruments-store'; prediction uses a flat kind (no per-AG key).
_BUCKET_KIND_MAP: dict[str, tuple[str, str | None]] = {
    "cefi": ("market-data", "cefi"),
    "defi": ("market-data", "defi"),
    "tradfi": ("market-data", "tradfi"),
    "sports": ("instruments-store", "sports"),
    "prediction": ("market-data-tick-prediction", None),
}

# Asset-group → {index blob, GCS prefix templates}. The prefix templates are now
# DERIVED from the canonical possible-manifest registry
# (``unified_api_contracts.canonical_path_templates`` — CF-15/V0, 2026-06-10), the
# SINGLE SSOT for GCS path shapes. This replaces the hand-maintained per-AG
# ``prefix_tpls`` lists that previously lived inline here (the Axis-10 drift bug: a
# newly-added ``pipeline_mode=batch_<source>`` had to be hand-copied into this list or
# the phantom ``--apply`` false-flagged that source's real captured rows
# captured→attempted_failed). The registry generates a prefix for every external batch
# source of the AG (canonical pipeline_mode-keyed shapes) + the transitional legacy
# hive vocabularies (bare ``asset_group=``, legacy ``category=``, top-level ``day=``,
# the DeFi combined ``venue=PROTOCOL-CHAIN`` overload, the ``hyperliquid_rest`` legacy
# token). Verified byte-identical to the prior hand-list at landing (the historical
# Axis-10 incident notes — cefi 130k 2026-05-03, AAVE_V3 29,782 2026-05-07, EIGENLAYER
# 597 2026-05-04 — now live in ``possible_manifest.py`` + ``codex/02-data/
# pipeline-mode-partition.md``). Sports keeps its own UAC ``candidate_parquet_paths``
# dispatch (``canonical_path_templates('sports')`` → ``[]``; the ``[""]`` sentinel is
# preserved so ``_audit_sports`` routes correctly).
ASSET_GROUP_CONFIG: dict[str, dict[str, list[str] | str]] = {
    ag: {
        "index": "_index/availability_index.parquet",
        "prefix_tpls": canonical_path_templates(ag) if ag != "sports" else [""],
    }
    for ag in ("cefi", "defi", "sports", "tradfi", "prediction")
}


# Protocol-name underscore drift (added 2026-05-07 for AAVE_V3-ARBITRUM
# C.9 audit per session_2026_05_07_data_status_audit_findings wrapper).
# Pre-canonicalisation DeFi writers spelled the protocol with an underscore
# between the protocol name and the version: ``AAVE_V3``, ``UNISWAP_V3``,
# ``COMPOUND_V3``. The post-2026-04 canonical form drops the underscore:
# ``AAVE_V3``, ``UNISWAP_V3``, ``COMPOUND_V3``. The migrate_mtds_defi_legacy_*
# scripts left BOTH spellings on disk under different ``venue=`` segments;
# the manifest carries only the canonical form. Without probing both
# variants, the audit false-positives 100% of AAVE_V3 / UNISWAP_V3 etc.
# rows as phantom (29,782 hits in the 2026-05-07 dry-run on AAVE_V3 alone).
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
    are returned. Works for plain venue (``AAVE_V3``) and combined-venue
    (``AAVE_V3-ETHEREUM`` — the protocol part before the first ``-``
    is transformed and the chain suffix preserved).

    Examples:
        AAVE_V3            -> [AAVE_V3, AAVE_V3]
        AAVE_V3           -> [AAVE_V3, AAVE_V3]
        AAVE_V3-ARBITRUM   -> [AAVE_V3-ARBITRUM, AAVE_V3-ARBITRUM]
        UNISWAP_V3         -> [UNISWAP_V3, UNISWAP_V3]
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
    # Try removing underscore: AAVE_V3 -> AAVE_V3
    if "_" in protocol:
        variants.add(protocol.replace("_", "") + chain_suffix)
    # Try inserting underscore: AAVE_V3 -> AAVE_V3
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
       holds ``venue=AAVE_V3`` (canonical, no underscore) but pre-2026-04
       writers used ``venue=AAVE_V3`` (underscored). Both spellings
       coexist on disk; we probe both via ``_defi_protocol_variants``.
       Without this, AAVE_V3 / UNISWAP_V3 / COMPOUND_V3 rows whose data
       lives under the legacy underscored prefix false-positive 100%
       as phantom (29,782 hits in the 2026-05-07 dry-run on AAVE_V3 alone).

    Axes 7-10 are handled in ``_audit_generic`` / ``_audit_sports`` directly:
    7. **TradFi Databento per-schema-bundle** (2026-05-13) — ``trades`` and
       ``tbbo`` are downloaded and written as a paired set; accept either
       data_type needle as capture evidence. See ``_TRADFI_DATABENTO_PAIRED_SCHEMAS``.
    8. **Cross-asset venue=UNKNOWN** (2026-05-13) — UNKNOWN sentinel has no
       canonical path; skip the venue needle. See ``_VENUE_UNKNOWN_SENTINELS``.
    9. **Sports pre-coverage + known-gap** (2026-05-13) — rows before source
       launch date or in registered gaps are not phantoms; excluded in
       ``_audit_sports`` via ``is_pre_launch_date`` + ``is_in_known_gap``.
    10. **Phase-3 pipeline_mode= prefix** (2026-05-19) — the GCS migration
        bundle (``gcs_migration_bundle_pipeline_mode_2026_05_08``) Phase 3
        prepended ``pipeline_mode={batch_*}/`` before ``asset_group=`` on all
        ``raw_tick_data/by_date/`` paths. Pre-migration paths had no
        ``pipeline_mode=`` segment; post-migration canonical paths do. Both
        shapes are covered by ``ASSET_GROUP_CONFIG[ag]["prefix_tpls"]``.
    """
    cfg = ASSET_GROUP_CONFIG[asset_group]
    raw_venue = str(row.get("venue", "") or "")
    raw_chain = str(row.get("chain", "") or "")  # noqa: qg-empty-fallback — chain is DeFi-only; non-DeFi asset_group rows never carry the column
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
    client: StorageClient,
    bucket_name: str,
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
        return day, {m.name for m in client.list_blobs(bucket_name, prefix=prefix) if m.name.endswith(".parquet")}

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
                singleton_exists[path] = client.blob_exists(bucket_name, path)
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
        data_type = str(row.get("data_type", "") or "")  # noqa: qg-empty-fallback — legacy schema vintages omit this column on some captured rows
        league_id = str(row.get("league_id", "") or "")  # noqa: qg-empty-fallback — bare/non-league sports rows legitimately have no league_id
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
        # Pass the row's pipeline_mode so the CANONICAL pipeline_mode= path is probed.
        # Post-Phase-3 migration the parquet lives at
        # day={D}/pipeline_mode=batch_<source>/entity=...; candidate_parquet_paths only
        # emits that path when pipeline_mode is supplied (it still appends the legacy
        # non-pipeline_mode fallback after). Omitting it probed ONLY the legacy path →
        # false phantom flip of real captured rows (e.g. PLAYER_VALUES/transfermarkt +
        # FIXTURE_LINEUPS/FIXTURE_STATS, golden-window 2026-06-24 — data was on disk).
        pipeline_mode = str(row.get("pipeline_mode", "") or "")  # noqa: qg-empty-fallback — pre-Phase-3 rows have no pipeline_mode segment by design (see comment above)
        candidates = candidate_parquet_paths(data_type, date, league_id, pipeline_mode=pipeline_mode or None)
        blobs = day_blobs.get(date, set())
        is_real = False
        for c in candidates:
            if c.startswith(day_partition_prefix):
                if "*" in c:
                    # Wildcard candidate (fetched_at_hour=*): the * represents a
                    # single unknown path segment (e.g. fetched_at_hour=2025-09-01T09).
                    # Resolve by checking if any blob starts with the prefix before *
                    # AND ends with the suffix after *.
                    pre, _, post = c.partition("*")
                    if any(b.startswith(pre) and b.endswith(post) for b in blobs):
                        is_real = True
                        break
                elif c in blobs:
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
    client: StorageClient,
    bucket_name: str,
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
            keys = {m.name for m in client.list_blobs(bucket_name, prefix=prefix) if m.name.endswith(".parquet")}
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
        data_type = str(row.get("data_type", "") or "")  # noqa: qg-empty-fallback — column may be absent (cross-AG report)
        raw_it = str(row.get("instrument_type", "") or "")  # noqa: qg-empty-fallback — schema-4 rows have no instrument_type (see docstring above)
        venue = str(row.get("venue", "") or "")  # noqa: qg-empty-fallback — column may be absent for this asset_group (cross-AG audit)
        chain = str(row.get("chain", "") or "")  # noqa: qg-empty-fallback — chain only applies to DeFi rows; absent elsewhere
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
        # DeFi-only: accept ANY protocol-name spelling variant (AAVE_V3 vs
        # AAVE_V3 etc.) as the venue match. Without this, manifest rows holding
        # the underscored spelling are flagged phantom even though their data
        # lives at the non-underscored disk path (and vice versa) — confirmed
        # 2026-05-17 on the lending-indices bucket (60 false phantoms reported
        # for AAVE_V3 + COMPOUND_V3 rows whose data lives at venue=AAVE_V3 /
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


def _build_triage_records(
    asset_group: str,
    phantom_df: pd.DataFrame,
    snapshot_time: str,
) -> list[dict]:
    """Build Gate-3 triage JSONL records from phantom rows.

    Schema per Gate-3 runbook:
    {venue, data_type, date, instrument_id, manifest_status,
     manifest_capture_time, parquet_row_count, reason, confidence, recommendation}
    """
    records: list[dict] = []
    for _, row in phantom_df.iterrows():
        # This report spans EVERY asset_group (cefi/defi/sports/tradfi/prediction) and
        # every manifest schema vintage — unlike the DeFi-only helpers above (which
        # unconditionally bracket-access these same column names because DeFi rows are
        # schema-homogeneous), a generic cross-asset-group row genuinely may lack any of
        # these columns (see the `"venue" in phantom_df.columns` guard a few dozen lines
        # below in main(), which treats venue as legitimately-optional for exactly this
        # reason). "" is the correct not-present marker for this best-effort triage JSONL
        # — a human reviews it, no downstream code branches on these fields being non-empty.
        venue = str(row.get("venue", "") or "")  # noqa: qg-empty-fallback — column may be absent for this asset_group
        data_type = str(row.get("data_type", "") or "")  # noqa: qg-empty-fallback — column may be absent (cross-AG report)
        date_str = str(row.get("date", "") or "")  # noqa: qg-empty-fallback — column may be absent (cross-AG report)
        instrument_id = str(row.get("instrument_id", "") or "")  # noqa: qg-empty-fallback — row-key col defaults to "" by schema design
        error_reason = str(row.get("error_reason", "") or "")  # noqa: qg-empty-fallback — pre-v8 rows lack this column entirely
        # "" is the documented available_at sentinel (readers test truthiness, not is-not-None).
        fallback_ts = row.get("available_at", "")  # noqa: qg-empty-fallback — available_at defaults to "" by schema design
        written_at = str(row.get("written_at", fallback_ts) or "")

        reason = "PHANTOM_NO_PARQUET"
        confidence = "MEDIUM"
        recommendation = "flip_to_attempted_failed"

        if error_reason and error_reason not in (
            "phantom_captured_no_parquet_at_canonical_path",
            "",
        ):
            reason = f"PHANTOM_KNOWN_ERROR_REASON:{error_reason}"
            confidence = "HIGH"
            recommendation = "accept_expected_gap"
        elif asset_group == "tradfi" and date_str:
            try:
                # date (not datetime) — weekday check only, no tz concept applies to a date-only value.
                dt = date.fromisoformat(date_str[:10])
                if dt.weekday() >= 5:  # 5=Saturday, 6=Sunday
                    reason = "PHANTOM_WEEKEND_TRADFI"
                    confidence = "HIGH"
                    recommendation = "accept_expected_gap"
            except ValueError:
                pass

        records.append(
            {
                "venue": venue,
                "data_type": data_type,
                "date": date_str,
                "instrument_id": instrument_id,
                "manifest_status": "captured",
                "manifest_capture_time": written_at or snapshot_time,
                "parquet_row_count": 0,
                "reason": reason,
                "confidence": confidence,
                "recommendation": recommendation,
            }
        )
    return records


def _write_triage_jsonl_gcs(
    storage_client: StorageClient,
    gcs_uri: str,
    records: list[dict],
) -> None:
    """Write triage records as JSONL to cloud storage (GCS or S3)."""
    for prefix in ("gs://", "s3://"):
        if gcs_uri.startswith(prefix):
            path = gcs_uri[len(prefix) :]
            break
    else:
        raise ValueError(f"--triage-output-gcs must start with gs:// or s3://, got: {gcs_uri!r}")
    slash_idx = path.index("/")
    triage_bucket = path[:slash_idx]
    blob_path = path[slash_idx + 1 :]
    lines = "\n".join(json.dumps(r, default=str) for r in records) + "\n"
    storage_client.upload_from_file_obj(triage_bucket, blob_path, io.BytesIO(lines.encode()))
    logger.info(
        "Triage JSONL written: %s (%d records)",
        gcs_uri,
        len(records),
    )


# Chain-/source-level DeFi data_types -> canonical chain-level venue. Fetched
# once per chain (or relay) at a synthetic infra venue, NEVER per protocol. The
# IS enumerator used to emit FALSE empty_confirmed cells keyed venue=<PROTOCOL>
# for these (gas/transfers/MEV exist from chain genesis; real key is the venue
# below). oracle_prices is DELIBERATELY ABSENT — it IS genuinely per-protocol.
_DEFI_CHAIN_LEVEL_VENUE: dict[str, str] = {
    "gas_fees": "ALCHEMY",
    "token_transfers": "ALCHEMY",
    "mev_events": "FLASHBOTS",
}


def _chain_level_phantom_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask for the chain-level DeFi wrong-key phantom set (SSOT predicate).

    A row is a phantom IFF it is a chain-level data_type
    (``gas_fees``/``token_transfers``/``mev_events``) with
    ``capture_status=='empty_confirmed'`` keyed on a venue that is NOT that
    data_type's canonical chain-level venue (ALCHEMY/FLASHBOTS) — i.e. the IS
    enumerator wrongly emitted a per-PROTOCOL ``empty_confirmed`` cell for a
    metric that is captured/expected at a single chain-level synthetic venue.

    This covers BOTH the ``EXPECTED_INSTRUMENT_NOT_LISTED`` and the
    ``EXPECTED_PRE_GENESIS_CHAIN`` wrong-key rows; the genuine pre-genesis cells
    re-seed at the canonical ``venue=ALCHEMY``/``FLASHBOTS`` via the fixed v2
    enumerator. ``oracle_prices`` is DELIBERATELY EXCLUDED (genuinely
    per-protocol). NEVER matches a ``captured``/``attempted_failed`` row, and
    NEVER matches a row already at the canonical venue.
    """
    status = df["capture_status"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    dt_col = df["data_type"].fillna("").astype(str)
    mask = pd.Series(False, index=df.index)
    for data_type, cv in _DEFI_CHAIN_LEVEL_VENUE.items():
        mask = mask | ((dt_col == data_type) & (status == "empty_confirmed") & (venue != cv))
    return mask


def _report_chain_level_defi_phantoms(df: pd.DataFrame) -> None:
    """DRY-RUN report (no mutation): chain-level DeFi data_types wrongly keyed
    ``venue=<PROTOCOL>``. Single-walk — reads only the already-loaded ``_index``
    ``df``. Per chain-level data_type, reports the ``empty_confirmed`` rows on a
    non-chain-level venue (the phantom set) split by ``error_reason`` + whether
    the canonical venue has captured rows (decides flip-vs-delete)."""
    status = df["capture_status"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    dt_col = df["data_type"].fillna("").astype(str)
    reason = df["error_reason"].fillna("").astype(str) if "error_reason" in df.columns else None
    logger.info("=== DRY-RUN: chain-level DeFi empty_confirmed phantom report ===")
    for data_type, cv in _DEFI_CHAIN_LEVEL_VENUE.items():
        is_dt = dt_col == data_type
        if not int(is_dt.sum()):
            logger.info("[%s] 0 manifest rows — skip", data_type)
            continue
        cap_cv = int((is_dt & (status == "captured") & (venue == cv)).sum())
        phantom = is_dt & (status == "empty_confirmed") & (venue != cv)
        n = int(phantom.sum())
        why = "captured dupes" if cap_cv else f"no capture yet; canonical key is venue={cv}+chain"
        logger.info(
            f"[{data_type}] rows={int(is_dt.sum())} | captured@{cv}={cap_cv} | "
            f"empty_confirmed@venue!={cv} (PHANTOM)={n} | DECISION: DELETE ({why} — "
            "flipping to attempted_failed would be wrong)"
        )
        if reason is not None and n:
            logger.info(f"    [{data_type}] phantom by reason: {reason[phantom].value_counts().to_dict()}")
            logger.info(
                f"    [{data_type}] DELETE PREDICATE: (data_type=='{data_type}') & "
                f"(capture_status=='empty_confirmed') & (venue != '{cv}') -> {n} rows "
                "(both NOT_LISTED + PRE_GENESIS_CHAIN are wrong-key; genuine pre-genesis "
                f"re-seeds at venue={cv})"
            )
    logger.info(
        "NOTE oracle_prices EXCLUDED — genuinely per-protocol (LIDO/ETHENA/AAVE_V3/… "
        "capture it at venue=<PROTOCOL>; those empties are CORRECT). Dry-run only — no writes."
    )


def _apply_delete_chain_level_defi_phantoms(
    storage_client: StorageClient,
    bucket_name: str,
    index_blob: str,
    df: pd.DataFrame,
) -> int:
    """APPLY mode: DELETE the chain-level DeFi wrong-key phantom rows from the
    ``_index`` and write the trimmed index back. Single-walk (reuses the
    already-loaded ``df``; one index read + one write, no whole-corpus GCS walk).

    The delete predicate is ``_chain_level_phantom_mask`` (SSOT) — it removes the
    BOTH wrong-key reason classes (NOT_LISTED + PRE_GENESIS_CHAIN) keyed
    ``venue=<PROTOCOL>`` for the chain-level data_types, and NEVER touches:
    ``oracle_prices`` (genuinely per-protocol), any ``captured`` /
    ``attempted_failed`` row, or any row already at the canonical
    ``venue=ALCHEMY``/``FLASHBOTS``. The genuine pre-genesis cells re-seed at the
    canonical venue via the FIXED v2 enumerator (``enumerate_expected_universe.py``).

    Asserts captured/attempted_failed totals are preserved before writing back.
    Returns the number of rows deleted.
    """
    status_before = df["capture_status"].fillna("").astype(str)
    captured_before = int((status_before == "captured").sum())
    attempted_failed_before = int((status_before == "attempted_failed").sum())

    phantom_mask = _chain_level_phantom_mask(df)
    n_delete = int(phantom_mask.sum())
    logger.info("APPLY chain-level DeFi phantom DELETE: %d rows match the predicate", n_delete)
    if n_delete == 0:
        logger.info("Nothing to delete — chain-level phantom set is empty. No write.")
        return 0

    # Defensive guard: the predicate must never select a captured/failed row.
    deleting = df[phantom_mask]
    deleting_status = deleting["capture_status"].fillna("").astype(str)
    bad = int((deleting_status != "empty_confirmed").sum())
    if bad:
        raise RuntimeError(
            f"REFUSING DELETE — predicate selected {bad} non-empty_confirmed rows "
            "(would destroy captured/attempted_failed data)"
        )

    kept = df[~phantom_mask].copy()
    status_after = kept["capture_status"].fillna("").astype(str)
    captured_after = int((status_after == "captured").sum())
    attempted_failed_after = int((status_after == "attempted_failed").sum())
    if captured_after != captured_before:
        raise RuntimeError(
            f"REFUSING WRITE — captured count changed {captured_before} -> {captured_after} (delete bug)"
        )
    if attempted_failed_after != attempted_failed_before:
        raise RuntimeError(
            f"REFUSING WRITE — attempted_failed count changed "
            f"{attempted_failed_before} -> {attempted_failed_after} (delete bug)"
        )

    out = io.BytesIO()
    kept.to_parquet(out, index=False)
    out.seek(0)
    logger.info(
        "Writing trimmed index: %d -> %d rows (%d deleted; captured=%d unchanged; attempted_failed=%d unchanged)",
        len(df),
        len(kept),
        n_delete,
        captured_after,
        attempted_failed_after,
    )
    storage_client.upload_from_file_obj(bucket_name, index_blob, out)
    logger.info("Done — chain-level DeFi phantom DELETE applied.")
    return n_delete


def _legacy_combined_venue_phantom_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask for the LEGACY-combined-venue DeFi phantom set (SSOT predicate).

    A row is a phantom IFF it is ``capture_status=='empty_confirmed'`` keyed on a
    LEGACY combined ``venue=PROTOCOL-CHAIN`` (the venue string contains ``-``)
    with a BLANK ``chain`` column. The fixed v2 enumerator
    (``enumerate_expected_universe.py`` @42dd37c) emits CANONICAL ``venue=PROTOCOL``
    + separate ``chain=X``; an earlier stale enumerator build (Cloud Run image /
    pre-fix tarball) seeded per-instrument ``empty_confirmed`` cells in the legacy
    combined form. Those legacy rows can NEVER be converted by a canonical capture
    (different shard key) and only poison the honest-cov DENOMINATOR. Same class as
    the 2.31M legacy-format ``expected_unattempted`` removed earlier (the prior
    driver re-seeded only ``expected_unattempted``; this stale build emitted the
    legacy form as ``empty_confirmed``).

    NEVER matches a ``captured``/``attempted_failed`` row (status gate), and NEVER
    matches a canonical row (canonical rows carry a bare ``venue`` without ``-`` AND
    a populated ``chain``). The genuine could-exist cells re-seed at the canonical
    ``venue=PROTOCOL``/``chain=X`` via the FIXED v2 enumerator.
    """
    status = df["capture_status"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    chain = df["chain"].fillna("").astype(str)
    return (status == "empty_confirmed") & venue.str.contains("-", regex=False) & (chain == "")


def _report_legacy_combined_venue_defi_phantoms(df: pd.DataFrame) -> None:
    """DRY-RUN report (no mutation) of the legacy-combined-venue DeFi phantom set.
    Single-walk — reads only the already-loaded ``_index`` ``df``."""
    mask = _legacy_combined_venue_phantom_mask(df)
    n = int(mask.sum())
    logger.info("=== DRY-RUN: legacy-combined-venue DeFi empty_confirmed phantom report ===")
    logger.info(
        "PHANTOM rows (empty_confirmed AND venue contains '-' AND chain==''): %d / %d total",
        n,
        len(df),
    )
    if n:
        ph = df[mask]
        reason = ph["error_reason"].fillna("").astype(str) if "error_reason" in ph.columns else None
        logger.info("    by error_reason: %s", reason.value_counts().to_dict() if reason is not None else "n/a")
        logger.info("    top venues: %s", ph["venue"].value_counts().head(10).to_dict())
        if "enumerator_run_id" in ph.columns:
            logger.info("    enumerator_run_id(s): %s", ph["enumerator_run_id"].dropna().unique().tolist()[:8])
    logger.info(
        "DECISION: DELETE (legacy combined venue=PROTOCOL-CHAIN + blank chain can never "
        "convert vs canonical venue=PROTOCOL+chain=X captures — denominator poison). "
        "Genuine could-exist cells re-seed canonically via the FIXED v2 enumerator. "
        "Dry-run only — no writes."
    )


def _apply_delete_legacy_combined_venue_defi_phantoms(
    storage_client: StorageClient,
    bucket_name: str,
    index_blob: str,
    df: pd.DataFrame,
) -> int:
    """APPLY mode: DELETE the legacy-combined-venue DeFi phantom rows from the
    ``_index`` and write the trimmed index back. Single-walk (one index read +
    one write, no whole-corpus GCS walk). Predicate =
    ``_legacy_combined_venue_phantom_mask`` (SSOT). Asserts captured/
    attempted_failed totals are preserved before writing back. Returns rows deleted."""
    status_before = df["capture_status"].fillna("").astype(str)
    captured_before = int((status_before == "captured").sum())
    attempted_failed_before = int((status_before == "attempted_failed").sum())

    phantom_mask = _legacy_combined_venue_phantom_mask(df)
    n_delete = int(phantom_mask.sum())
    logger.info("APPLY legacy-combined-venue DeFi phantom DELETE: %d rows match the predicate", n_delete)
    if n_delete == 0:
        logger.info("Nothing to delete — legacy-combined-venue phantom set is empty. No write.")
        return 0

    deleting = df[phantom_mask]
    deleting_status = deleting["capture_status"].fillna("").astype(str)
    bad = int((deleting_status != "empty_confirmed").sum())
    if bad:
        raise RuntimeError(
            f"REFUSING DELETE — predicate selected {bad} non-empty_confirmed rows "
            "(would destroy captured/attempted_failed data)"
        )

    kept = df[~phantom_mask].copy()
    status_after = kept["capture_status"].fillna("").astype(str)
    captured_after = int((status_after == "captured").sum())
    attempted_failed_after = int((status_after == "attempted_failed").sum())
    if captured_after != captured_before:
        raise RuntimeError(
            f"REFUSING WRITE — captured count changed {captured_before} -> {captured_after} (delete bug)"
        )
    if attempted_failed_after != attempted_failed_before:
        raise RuntimeError(
            f"REFUSING WRITE — attempted_failed count changed "
            f"{attempted_failed_before} -> {attempted_failed_after} (delete bug)"
        )

    out = io.BytesIO()
    kept.to_parquet(out, index=False)
    out.seek(0)
    logger.info(
        "Writing trimmed index: %d -> %d rows (%d deleted; captured=%d unchanged; attempted_failed=%d unchanged)",
        len(df),
        len(kept),
        n_delete,
        captured_after,
        attempted_failed_after,
    )
    storage_client.upload_from_file_obj(bucket_name, index_blob, out)
    logger.info("Done — legacy-combined-venue DeFi phantom DELETE applied.")
    return n_delete


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
        "--unphantom-only",
        action="store_true",
        help=(
            "Run ONLY the reverse re-validation pass (implies --unphantom) and "
            "SKIP the forward phantom-flagging pass entirely. Safe-by-construction: "
            "--unphantom-only --apply can ONLY flip phantom->captured "
            "(attempted_failed -> captured), NEVER captured -> attempted_failed, so "
            "there is ZERO forward-flip risk even while candidate_parquet_paths "
            "still misses some path shapes (e.g. the sports footystats "
            "fetched_at_hour= / transfermarkt_teams.parquet / league=-without-season= "
            "shapes that make the FORWARD pass over-flag real captured rows). Use this "
            "to heal genuinely-false phantoms (data IS on disk under pipeline_mode=) "
            "without the destructive forward --apply."
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
    p.add_argument(
        "--triage-output-gcs",
        type=str,
        default="",
        help=(
            "GCS URI for Gate-3 triage JSONL output (e.g. "
            "gs://central-element-323112-phantom-triage/triage-cefi-2026-05-17.jsonl). "
            "Written only in --dry-run mode. Schema: "
            "{venue, data_type, date, instrument_id, manifest_status, "
            "manifest_capture_time, parquet_row_count, reason, confidence, recommendation}."
        ),
    )
    p.add_argument(
        "--manifest-snapshot-time",
        type=str,
        default="",
        help=(
            "ISO timestamp of when the manifest was snapshotted (default: now). "
            "Used as manifest_capture_time fallback in triage records."
        ),
    )
    p.add_argument(
        "--cloud",
        choices=["gcp", "aws"],
        default="gcp",
        help="Cloud provider for storage backend (default: gcp).",
    )
    p.add_argument(
        "--report-chain-level-defi-phantoms",
        action="store_true",
        help=(
            "Chain-level DeFi phantom pass (single _index read). WITHOUT --apply: "
            "DRY-RUN report (no mutation) of empty_confirmed "
            "gas_fees/token_transfers/mev_events rows wrongly keyed venue=<PROTOCOL> "
            "vs canonical chain-level venue (ALCHEMY/FLASHBOTS) + the delete "
            "predicate + counts. WITH --apply: DELETE that phantom set and write the "
            "trimmed _index back (preserves captured/attempted_failed; oracle_prices "
            "excluded — genuinely per-protocol)."
        ),
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Mutation switch for --report-chain-level-defi-phantoms / "
            "--report-legacy-venue-defi-phantoms: DELETE the matching phantom rows "
            "and write the trimmed _index back. Without it the pass is dry-run."
        ),
    )
    p.add_argument(
        "--report-legacy-venue-defi-phantoms",
        action="store_true",
        help=(
            "Legacy-combined-venue DeFi phantom pass (single _index read). WITHOUT "
            "--apply: DRY-RUN report of empty_confirmed rows keyed legacy "
            "venue=PROTOCOL-CHAIN (venue contains '-') + blank chain — the stale "
            "v2-enumerator-build regression (denominator poison; can't convert vs "
            "canonical venue=PROTOCOL+chain=X captures). WITH --apply: DELETE that "
            "set + write the trimmed _index back (preserves captured/attempted_failed; "
            "genuine cells re-seed canonically via the FIXED v2 enumerator)."
        ),
    )
    args = p.parse_args()

    # --unphantom-only implies --unphantom (it runs the reverse re-validation only).
    if args.unphantom_only:
        args.unphantom = True

    os.environ.setdefault("CLOUD_PROVIDER", args.cloud)
    cfg = dict(ASSET_GROUP_CONFIG[args.asset_group])
    # Resolve bucket via UTL SSOT; --manifest-bucket overrides for per-data-type buckets
    # (e.g. DeFi lending-indices, lst-rates, oracle-prices).
    if args.manifest_bucket:
        bucket_name = args.manifest_bucket
        logger.info("Manifest bucket override: %s", args.manifest_bucket)
    else:
        kind, ag_arg = _BUCKET_KIND_MAP[args.asset_group]
        bucket_name = resolve_bucket_name(cloud=args.cloud, kind=kind, asset_group=ag_arg)
    if args.manifest_index:
        cfg["index"] = args.manifest_index
        logger.info("Manifest blob override: %s", args.manifest_index)

    storage_client = get_storage_client(provider=args.cloud)
    # GCS HTTP pool tuning (GCP only — boto3 manages its own pool automatically).
    # Bump connection pool to match worker count; the default of 10 silently
    # truncates list_blobs() under high concurrency (2026-05-04 CeFi: 12k
    # false-positive phantoms). Pattern from migrate_polymarket_canonical.py.
    if args.cloud == "gcp":
        from requests.adapters import HTTPAdapter

        pool_size = max(args.workers * 2, 64)
        try:
            _gcs_native = getattr(storage_client, "_client", None)
            _http = getattr(_gcs_native, "_http", None) if _gcs_native is not None else None
            if _http is not None:
                _adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=3)
                _http.mount("https://", _adapter)
                _http.mount("http://", _adapter)
                logger.info("GCS HTTP pool tuned: pool_size=%d (workers=%d)", pool_size, args.workers)
        except (AttributeError, TypeError):
            logger.warning("Could not tune GCS HTTP pool — falling back to default 10")

    logger.info("Loading manifest from %s://%s/%s", args.cloud, bucket_name, cfg["index"])
    raw_bytes = storage_client.download_bytes(bucket_name, cfg["index"])
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    # Chain-level DeFi phantom report — DRY-RUN ONLY, single-walk (reuses the
    # index read above). Returns before any disk-audit / mutation path.
    if args.report_chain_level_defi_phantoms:
        if args.asset_group != "defi":
            logger.error("--report-chain-level-defi-phantoms requires --asset-group defi")
            return 2
        # Always print the report first (the apply path then deletes the same set).
        _report_chain_level_defi_phantoms(df)
        if args.apply:
            _apply_delete_chain_level_defi_phantoms(storage_client, bucket_name, str(cfg["index"]), df)
        else:
            logger.info("DRY-RUN — pass --apply to DELETE the phantom set. No writes.")
        return 0

    # Legacy-combined-venue DeFi phantom pass — single-walk (reuses the index
    # read above). Returns before any disk-audit / mutation path.
    if args.report_legacy_venue_defi_phantoms:
        if args.asset_group != "defi":
            logger.error("--report-legacy-venue-defi-phantoms requires --asset-group defi")
            return 2
        _report_legacy_combined_venue_defi_phantoms(df)
        if args.apply:
            _apply_delete_legacy_combined_venue_defi_phantoms(storage_client, bucket_name, str(cfg["index"]), df)
        else:
            logger.info("DRY-RUN — pass --apply to DELETE the phantom set. No writes.")
        return 0

    # --unphantom-only: skip the FORWARD phantom-flagging pass ENTIRELY. We never
    # build captured_idx, never audit captured rows, never compute phantom_idx — so
    # no captured row can be flipped to attempted_failed. Only the reverse
    # re-validation (--unphantom logic) runs below.
    forward_pass_enabled = not args.unphantom_only

    captured_idx: pd.Index = df.index[:0]  # empty by default (overwritten if forward runs)
    if forward_pass_enabled:
        captured_idx = _build_forward_captured_idx(df, args)
        logger.info("Captured rows in scope: %d", len(captured_idx))
    else:
        logger.info("--unphantom-only: SKIPPING forward phantom-flagging pass (reverse re-validation only).")

    real_or_phantom: dict[int, bool] = {}
    if forward_pass_enabled and len(captured_idx) > 0:
        if args.asset_group == "sports":
            real_or_phantom = _audit_sports(storage_client, bucket_name, df, captured_idx, args.workers)
        else:
            real_or_phantom = _audit_generic(
                args.asset_group, storage_client, bucket_name, df, captured_idx, args.workers
            )

    phantom_idx = [i for i, real in real_or_phantom.items() if not real]
    real_count = sum(1 for r in real_or_phantom.values() if r)
    if forward_pass_enabled:
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
                rev = _audit_sports(storage_client, bucket_name, df, phantom_flagged_idx, args.workers)
            else:
                rev = _audit_generic(
                    args.asset_group, storage_client, bucket_name, df, phantom_flagged_idx, args.workers
                )
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
        if phantom_idx:
            triage_gcs = args.triage_output_gcs
            if not triage_gcs:
                ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
                triage_gcs = f"gs://central-element-323112-phantom-triage/triage_{args.asset_group}_{ts}.jsonl"
            snapshot_time = args.manifest_snapshot_time or datetime.now(UTC).isoformat()
            triage_records = _build_triage_records(args.asset_group, df.loc[phantom_idx], snapshot_time)
            _write_triage_jsonl_gcs(storage_client, triage_gcs, triage_records)
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
    storage_client.upload_from_file_obj(bucket_name, cfg["index"], out)
    logger.info("Done.")
    return 0


def _build_forward_captured_idx(df: pd.DataFrame, args: argparse.Namespace) -> pd.Index:
    """Build the captured-rows index for the FORWARD phantom-flagging pass.

    Applies the venue / data_type / start-date / end-date scope filters and the
    schema_v4 vestigial-row drop, returning the captured rows in scope. Extracted
    from ``main`` so ``--unphantom-only`` can cleanly skip the entire forward pass
    (it never calls this).
    """
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

    return df[captured_mask].index


if __name__ == "__main__":
    sys.exit(main())
