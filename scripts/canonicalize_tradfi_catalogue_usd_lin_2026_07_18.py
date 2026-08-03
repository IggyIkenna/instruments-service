#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: after the full tradfi FUTURE/OPTION `-USD@LIN` catalogue migration
#   (prod/catalog.parquet + the per-day instrument_availability/by_date corpus) is
#   applied full-sweep and re-verified, AND
#   tradfi_consolidated_closeout_2026_07_18.md Phase B's catalogue-migration todo is
#   flipped done.
"""Phase-B (Surface A) migration — re-derive the canonical
``VENUE:(FUTURE|OPTION):PRODUCT_ROOT-USD@LIN-YYYYMMDD[-STRIKE-C|P]`` shape for every
real tradfi FUTURE/OPTION row in ``prod/catalog.parquet`` AND the per-day
``instrument_availability/by_date/day=*/venue=*/instruments.parquet`` corpus it is
rolled up FROM (``instruments_service/scripts/build_instrument_catalogue.py``).

Background (``tradfi_consolidated_closeout_2026_07_18.md`` Phase B, scoping
workflow ``wf_2f2c9a39-164``, full empirical design in scratchpad ``scope_B.md``):
live ``prod/catalog.parquet`` has **0 of 1,111,322** FUTURE/OPTION rows in
``-USD@LIN`` form — every ``instrument_id`` is exchange-native
(``CME:OPTION:E1AF0 C1600``, ``CBOE:FUTURE:VX/F1``) and ``canonical_instrument_id``
is either empty or carries the deleted colon/month-only additive shape
(``CME:OPTION:SP500:2026-08:7525P``). This script re-derives both columns (plus
``instrument_type`` and ``underlying``) from each row's own already-captured
``raw_symbol`` via the ONE shared migration+writer+verify-gate primitive
(``unified_api_contracts.canonicalize_raw_tradfi_id`` /
``assert_tradfi_derivative_ids_canonical``, shipped ``unified-api-contracts@3bd4ec29``)
— no re-download from Databento, no re-implementation of the classify/build logic.

**Two independent surfaces, mirroring the proven durability pattern in
``canonicalize_okx_margin_type_2026_07_09.py``:**

1. ``prod/catalog.parquet`` (default target) — the rolled-up reference table
   consumed by the v2 expected-universe enumerator + the deployment-api
   data-status "Upcoming expiries" widget. 1,175,390 total rows, 1,111,322
   FUTURE/OPTION in scope.

2. ``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet``
   — the per-day-per-venue snapshots ``catalog.parquet`` is rolled up FROM
   (``build_instrument_catalogue.py``). **CRITICAL DURABILITY TRAP** (proven by the
   2026-07-08 combo-migration revert): a ``catalog.parquet``-only rewrite is
   SILENTLY REVERTED the next time ``build_instrument_catalogue.py`` runs its
   scheduled roll-up, because it re-derives ``catalog.parquet`` FROM these still-raw
   snapshots. Same target shape, but this corpus's id column is named
   ``instrument_key`` (not ``instrument_id`` — confirmed by direct read,
   2026-07-18) with the SAME semantics; ``canonical_instrument_id`` is present on
   both surfaces. After a full-sweep ``--by-day --apply --by-day-full-sweep``,
   operators MUST re-run ``build_instrument_catalogue.py --asset-group tradfi``
   (the existing roll-up producer) and re-verify ``prod/catalog.parquet`` stays
   canonical — that is the durability proof; this script does not invoke the
   roll-up itself (kept a single, reviewable responsibility, matching how the OKX
   script's per-day fix and the roll-up producer are separate scripts).

Every row canonicalizes to exactly one status (never a silent fallback — see
``CanonStatus`` in ``tradfi_id_canonicalizer.py``): ``OK`` /
``ALREADY_CANONICAL`` rows get ``instrument_id``/``instrument_key`` and
``canonical_instrument_id`` rewritten byte-equal, ``instrument_type`` re-stamped
to the classifier-derived UPPERCASE enum when it disagrees with the stored value
(catalogue SSOT decision, operator 2026-07-18), and ``underlying`` rewritten to
the human-readable root. Any ``QUARANTINE_*``/``NULL_OR_EMPTY`` row is left
UNCHANGED and recorded in a quarantine sidecar (bounded, counted by status +
best-effort reason) — never force-parsed, never silently dropped.

**Phase-B follow-ups (2026-07-18, this script's scope also covers)**:

(A) **Combo re-stamp** — a ``QUARANTINE_COMBO`` row (CBOE ``UD_...`` user-defined
strategies, CME prefix-spreads) keeps its id UNCHANGED (combo-ID canonicalization
is the separate ``canonical_id_p1_tradfi_combo_leg_canonicalization_2026_07_08.md``
track) but its ``instrument_type`` IS re-stamped to the classifier-derived UPPERCASE
``COMBO`` enum — fixes the ``future``/``FUTURE`` case-dupe these rows previously kept
and stops them inflating the FUTURE/OPTION canonical-%% denominator.

(B) **Cash-type ``-USD``** — EQUITY/CURRENCY/ETF/BOND/COMMODITY/INDEX rows now also
route through ``canonicalize_raw_tradfi_id`` (extended, ``unified-api-contracts``) to
pick up the explicit ``-USD`` quote suffix the builder emits (``uac@33e3f369``),
e.g. ``NASDAQ:EQUITY:AAPL`` -> ``NASDAQ:EQUITY:AAPL-USD``.

Both are reported in the dry-run (``cash_migrated_count``/``combo_restamp_count``)
and applied in every write mode (in-place ``--apply``, ``--full-sweep``, ``--by-day``).

Usage::

    cd instruments-service

    # Dry-run against prod/catalog.parquet (default: report only, no GCS writes)
    .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py

    # Dry-run fully offline against a local snapshot (zero GCS calls at all)
    .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py \\
        --local-catalog-snapshot /path/to/catalog.parquet

    # Full sweep of prod/catalog.parquet (backup + write + verify) — ORCHESTRATOR ONLY
    .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py \\
        --apply --full-sweep

    # Per-day corpus: bounded real smoke test (backup + write + verify each touched file)
    .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py \\
        --by-day --apply --by-day-sample 20

    # Per-day corpus: full real sweep (all real day/venue files)
    .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py \\
        --by-day --apply --by-day-full-sweep --workers 40

    # Per-day corpus: SHARDED full sweep — the only way to actually go faster.
    # The per-row canonicalization is pure Python and therefore GIL-bound: --workers saturates
    # ~1.3 cores no matter how high it is set. MEASURED 2026-07-20 on an in-region e2-standard-16
    # with --workers 64: 130%% of 1600%% CPU, 86%% idle, ~1.0 files/s => ~7.4h for the 27.1k-file
    # sweep — statistically indistinguishable from the cross-region laptop run it was supposed to
    # beat, because GCS latency was never the bottleneck. Run one PROCESS per core over disjoint
    # shards instead (16 shards x --workers 8 on a 16-vCPU box):
    for i in $(seq 0 15); do
        .venv/bin/python scripts/canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py \\
            --by-day --apply --by-day-full-sweep --workers 8 --shard-of 16 --shard-index "$i" &
    done; wait
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import (
    TRADFI_CASH_TYPE_VALUES,
    assert_tradfi_derivative_ids_canonical,
    canonicalize_raw_tradfi_id,
)
from unified_trading_library import get_config, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG_FILENAME = "catalog.parquet"
BY_DATE_PREFIX = "instrument_availability/by_date/"

#: The FUTURE/OPTION single-leg derivative id-format scope (this migration's original
#: purpose) plus the CASH quote-suffix scope (Phase B follow-up (B),
#: tradfi_consolidated_closeout_2026_07_18.md) — EQUITY/CURRENCY/ETF/BOND/COMMODITY/INDEX
#: rows now also route through ``canonicalize_raw_tradfi_id`` to pick up the ``-USD`` suffix
#: (``TRADFI_CASH_TYPE_VALUES``, uac@33e3f369 builder + this repo's primitive extension).
#: COMBO/SPOT_PAIR/chain atoms stay OUT of scope — a mislabeled-FUTURE combo row still
#: enters via "FUTURE" and gets re-stamped by the QUARANTINE_COMBO branch below (follow-up
#: (A)), but a row ALREADY stored COMBO is untouched (no id-format work needed there).
_DERIVATIVE_TYPES = ("FUTURE", "OPTION")
_TARGET_TYPES = _DERIVATIVE_TYPES + tuple(sorted(TRADFI_CASH_TYPE_VALUES))
_OK_STATUSES = ("OK", "ALREADY_CANONICAL")

_MAX_EXAMPLES = 6
_MAX_QUARANTINE_SAMPLE_PER_STATUS = 25

_UD_STRATEGY_RE = re.compile(r"^UD[_:]", re.IGNORECASE)
_NEGATIVE_STRIKE_RE = re.compile(r"[CP]-\d")


def _bucket_name() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


def _deployment_env() -> str:
    return get_config("DEPLOYMENT_ENV", "prod")


def _catalog_path(env: str) -> str:
    return f"{env}/{CATALOG_FILENAME}"


def _self_check() -> None:
    """Fixed regression self-checks — known raw shapes must canonicalize to the exact
    target shapes for the FUTURE/OPTION path AND both Phase-B follow-ups (cash -USD,
    combo re-stamp signal). Runs unconditionally at the top of ``main()`` (fail-loud if
    the shared primitive's behavior ever drifts from what this migration assumes).
    """
    result = canonicalize_raw_tradfi_id(raw="E1AF0 C1600", venue="CME", instrument_type="OPTION")
    expected = "CME:OPTION:SP500-USD@LIN-20200117-1600-C"
    if result.status != "OK" or result.canonical_id != expected:
        msg = (
            "Self-check FAILED: canonicalize_raw_tradfi_id(raw='E1AF0 C1600', venue='CME', "
            f"instrument_type='OPTION') = {result!r}, expected canonical_id={expected!r}. "
            "Refusing to proceed — the shared primitive's behavior no longer matches this "
            "migration's assumptions."
        )
        raise AssertionError(msg)
    logger.info("Self-check PASSED: CME weekly option 'E1AF0 C1600' -> %s", expected)

    cash_result = canonicalize_raw_tradfi_id(raw="NASDAQ:EQUITY:AAPL", venue="NASDAQ", instrument_type="EQUITY")
    cash_expected = "NASDAQ:EQUITY:AAPL-USD"
    if cash_result.status != "OK" or cash_result.canonical_id != cash_expected:
        msg = (
            "Self-check FAILED (Phase-B follow-up (B), cash -USD): "
            f"canonicalize_raw_tradfi_id(raw='NASDAQ:EQUITY:AAPL', venue='NASDAQ', "
            f"instrument_type='EQUITY') = {cash_result!r}, expected canonical_id={cash_expected!r}."
        )
        raise AssertionError(msg)
    logger.info("Self-check PASSED: cash equity 'NASDAQ:EQUITY:AAPL' -> %s", cash_expected)

    combo_result = canonicalize_raw_tradfi_id(raw="UD_1V__VT_0319838460", venue="CBOE", instrument_type="FUTURE")
    if combo_result.status != "QUARANTINE_COMBO" or combo_result.derived_instrument_type != "COMBO":
        msg = (
            "Self-check FAILED (Phase-B follow-up (A), combo re-stamp signal): "
            f"canonicalize_raw_tradfi_id(raw='UD_1V__VT_0319838460', venue='CBOE', "
            f"instrument_type='FUTURE') = {combo_result!r}, expected status=QUARANTINE_COMBO "
            "with derived_instrument_type='COMBO'."
        )
        raise AssertionError(msg)
    logger.info("Self-check PASSED: CBOE combo 'UD_1V__VT_0319838460' signals COMBO re-stamp (id unchanged).")


def _classify_quarantine_reason(status: str, raw: str, venue: str) -> str:
    """Best-effort human-readable bucket for the quarantine sidecar report only —
    NEVER used to decide the ``CanonStatus`` itself (that is the shared primitive's
    sole responsibility). Mirrors the categories measured in ``scope_B.md`` §1/§8.
    """
    if status == "NULL_OR_EMPTY":
        return "null_or_empty"
    if status == "QUARANTINE_CONTINUOUS":
        return "continuous_root"
    if status == "QUARANTINE_COMBO":
        return "cboe_user_defined_strategy" if _UD_STRATEGY_RE.match(raw.strip()) else "combo_multi_leg"
    if status == "QUARANTINE_UNPARSEABLE":
        if _NEGATIVE_STRIKE_RE.search(raw):
            return "negative_strike"
        if venue.strip().upper() == "ICE":
            return "ice_qualifier_variant"
        return "other_unparseable"
    return "unknown"


@dataclass(slots=True)
class MigrationResult:
    """Result of :func:`_migrate_dataframe` — never a silent fallback per row."""

    df: pd.DataFrame
    status_counts: dict[str, int] = field(default_factory=dict)
    retype_count: int = 0
    changed_idx: list[object] = field(default_factory=list)
    #: Rows whose ``instrument_type`` changed but whose id did NOT (a combo
    #: re-stamp, or a stored-type case-mismatch on an already-canonical id) —
    #: these need to land in the write-set too even though they never touch
    #: ``id_col`` (see ``_migrate_catalog``/``_migrate_one_by_day_file``).
    type_only_changed_idx: list[object] = field(default_factory=list)
    #: Phase-B follow-up (B) — count of CASH (EQUITY/CURRENCY/ETF/BOND/COMMODITY/INDEX)
    #: rows whose id actually changed (picked up the ``-USD`` suffix).
    cash_migrated_count: int = 0
    cash_examples: list[tuple[str, str]] = field(default_factory=list)
    #: Phase-B follow-up (A) — count of QUARANTINE_COMBO rows whose stored
    #: ``instrument_type`` got re-stamped to the UPPERCASE ``COMBO`` enum
    #: (id intentionally left unchanged — see module docstring).
    combo_restamp_count: int = 0
    #: True-derivative denominator/numerator computed from the CLASSIFIER-DERIVED type
    #: (never the stored/mislabeled column) — this is what "resulting true FUTURE/OPTION
    #: canonical %" means once combo rows are re-stamped out of the FUTURE/OPTION count.
    derivative_total: int = 0
    derivative_ok: int = 0
    #: EXACT count per "STATUS:reason" key — never sampled/bounded (cheap int increments),
    #: so the dry-run breakdown is never silently skewed by iteration order.
    quarantine_reason_counts: dict[str, int] = field(default_factory=dict)
    #: Bounded per-(status,reason) SAMPLE of raw ids — for the sidecar JSON only, not
    #: for the printed breakdown counts (which come from ``quarantine_reason_counts``).
    quarantine_records: list[dict[str, str]] = field(default_factory=list)
    examples: list[tuple[str, str, str, str]] = field(default_factory=list)
    total_target_rows: int = 0


def _migrate_dataframe(df: pd.DataFrame, *, id_col: str, canonical_col: str) -> MigrationResult:
    """Recompute ``id_col`` / ``canonical_col`` / ``instrument_type`` / ``underlying``
    for every FUTURE/OPTION/CASH row via the shared ``canonicalize_raw_tradfi_id`` primitive.

    Works identically for BOTH surfaces — the catalogue (``id_col="instrument_id"``)
    and the per-day corpus (``id_col="instrument_key"``) — same primitive, same
    logic, only the id column name differs. Non-OK rows are left UNCHANGED in the
    returned ``df`` and recorded in ``quarantine_records`` (never force-parsed,
    never silently dropped) — EXCEPT a ``QUARANTINE_COMBO`` row's ``instrument_type``,
    which IS re-stamped to the classifier-derived ``COMBO`` enum (Phase-B follow-up
    (A)) even though its id stays untouched (``canonical_id`` is ``None`` for combos).
    """
    if id_col not in df.columns or "instrument_type" not in df.columns or "venue" not in df.columns:
        return MigrationResult(df=df)

    mask = df["instrument_type"].isin(_TARGET_TYPES)
    target_idx = df.index[mask]
    has_raw_symbol_col = "raw_symbol" in df.columns
    has_underlying_col = "underlying" in df.columns
    has_canonical_col = canonical_col in df.columns

    result = MigrationResult(df=df.copy(), total_target_rows=len(target_idx))
    _sample_counts_by_key: dict[str, int] = {}
    _example_keys_seen: set[str] = set()

    for idx in target_idx:
        row = result.df.loc[idx]
        stored_type = str(row["instrument_type"])
        venue = str(row["venue"])
        raw_symbol = row["raw_symbol"] if has_raw_symbol_col else None
        old_id = str(row[id_col])
        raw = str(raw_symbol) if raw_symbol is not None and str(raw_symbol).strip() else old_id

        canon = canonicalize_raw_tradfi_id(raw=raw, venue=venue, instrument_type=stored_type)
        result.status_counts[canon.status] = result.status_counts.get(canon.status, 0) + 1

        # True FUTURE/OPTION denominator/numerator — by the CLASSIFIER-DERIVED type,
        # never the stored column, so a combo mislabeled FUTURE never inflates this.
        if canon.derived_instrument_type in _DERIVATIVE_TYPES:
            result.derivative_total += 1
            if canon.status in _OK_STATUSES:
                result.derivative_ok += 1

        # Phase-B follow-up (A): a QUARANTINE_COMBO row's id is intentionally left
        # unchanged, but its WRONG stored instrument_type (a mislabeled future/FUTURE)
        # must still be re-stamped to the operator-decided UPPERCASE COMBO enum — else
        # it keeps inflating the FUTURE/OPTION denominator AND stays a case/semantic
        # dupe in the dimension (tradfi_consolidated_closeout_2026_07_18.md tick 5/7/9).
        if canon.status == "QUARANTINE_COMBO":
            reason = _classify_quarantine_reason(canon.status, raw, venue)
            reason_key = f"{canon.status}:{reason}"
            result.quarantine_reason_counts[reason_key] = result.quarantine_reason_counts.get(reason_key, 0) + 1
            sampled_for_key = _sample_counts_by_key.get(reason_key, 0)
            if sampled_for_key < _MAX_QUARANTINE_SAMPLE_PER_STATUS:
                _sample_counts_by_key[reason_key] = sampled_for_key + 1
                result.quarantine_records.append(
                    {
                        "id": old_id,
                        "venue": venue,
                        "stored_type": stored_type,
                        "status": canon.status,
                        "reason": reason,
                    }
                )
            combo_type = (canon.derived_instrument_type or "COMBO").upper()
            if stored_type.upper() != combo_type:
                result.df.at[idx, "instrument_type"] = combo_type
                result.combo_restamp_count += 1
                result.retype_count += 1
                result.type_only_changed_idx.append(idx)
            continue

        if canon.status not in _OK_STATUSES or canon.canonical_id is None:
            reason = _classify_quarantine_reason(canon.status, raw, venue)
            reason_key = f"{canon.status}:{reason}"
            result.quarantine_reason_counts[reason_key] = result.quarantine_reason_counts.get(reason_key, 0) + 1
            sampled_for_key = _sample_counts_by_key.get(reason_key, 0)
            if sampled_for_key < _MAX_QUARANTINE_SAMPLE_PER_STATUS:
                _sample_counts_by_key[reason_key] = sampled_for_key + 1
                result.quarantine_records.append(
                    {
                        "id": old_id,
                        "venue": venue,
                        "stored_type": stored_type,
                        "status": canon.status,
                        "reason": reason,
                    }
                )
            continue

        new_id = canon.canonical_id
        new_type = (canon.derived_instrument_type or stored_type).upper()
        is_cash = new_type in TRADFI_CASH_TYPE_VALUES
        id_changed = old_id != new_id

        if id_changed:
            result.df.at[idx, id_col] = new_id
            result.changed_idx.append(idx)
            if is_cash:
                result.cash_migrated_count += 1
                if len(result.cash_examples) < _MAX_EXAMPLES:
                    result.cash_examples.append((old_id, new_id))
            # Diversity heuristic (report quality only): prefer one example per
            # distinct (venue, stored_type) pair before repeating a pattern, so the
            # printed sample isn't 6 copies of the same instrument family.
            example_key = f"{venue}:{stored_type}"
            if len(result.examples) < _MAX_EXAMPLES and example_key not in _example_keys_seen:
                _example_keys_seen.add(example_key)
                result.examples.append((old_id, new_id, stored_type, new_type))
        if has_canonical_col:
            result.df.at[idx, canonical_col] = new_id
        type_changed = new_type != stored_type
        if type_changed:
            result.df.at[idx, "instrument_type"] = new_type
            result.retype_count += 1
        if not id_changed and type_changed:
            # A retype with no id change (e.g. a case-mismatched stored type on an
            # already-canonical id) still needs to reach the write-set.
            result.type_only_changed_idx.append(idx)
        if has_underlying_col and canon.derived_underlying_human:
            result.df.at[idx, "underlying"] = canon.derived_underlying_human

    return result


def _report_dry_run(result: MigrationResult, *, source_label: str) -> None:
    total = result.total_target_rows
    ok = sum(result.status_counts.get(s, 0) for s in _OK_STATUSES)
    pct = 100.0 * ok / max(1, total)
    derivative_pct = 100.0 * result.derivative_ok / max(1, result.derivative_total)
    logger.info("=== DRY-RUN report (%s, READ-ONLY, 0 writes) ===", source_label)
    logger.info("Total FUTURE/OPTION/CASH rows in scope (stored-type mask): %d", total)
    logger.info("OK + ALREADY_CANONICAL: %d / %d (%.2f%%)", ok, total, pct)
    logger.info("Status distribution: %s", result.status_counts)
    logger.info("instrument_type re-stamp count (derived != stored, among OK rows): %d", result.retype_count)
    logger.info("Quarantine breakdown (status:reason -> EXACT count): %s", result.quarantine_reason_counts)

    logger.info("Phase-B follow-up (B) — CASH rows migrated to the -USD id (count): %d", result.cash_migrated_count)
    for old_id, new_id in result.cash_examples:
        logger.info("  [CASH] %r -> %r", old_id, new_id)

    logger.info(
        "Phase-B follow-up (A) — QUARANTINE_COMBO rows re-stamped instrument_type -> COMBO (id unchanged): %d",
        result.combo_restamp_count,
    )

    logger.info(
        "RESULTING TRUE FUTURE/OPTION canonical %% (classifier-derived denominator, combos excluded): %d / %d (%.4f%%)",
        result.derivative_ok,
        result.derivative_total,
        derivative_pct,
    )

    logger.info("Before -> after examples (id, old_type -> new_type):")
    for old_id, new_id, old_t, new_t in result.examples:
        marker = "  [RETYPE]" if old_t != new_t else ""
        logger.info("  %r -> %r  (%s -> %s)%s", old_id, new_id, old_t, new_t, marker)


def _upload_json_report(bucket_name: str, path: str, payload: dict[str, object]) -> None:
    storage_client = get_storage_client()
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    storage_client.upload_bytes(bucket_name, path, body)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Quarantine sidecar report written: gs://%s/%s (%d bytes)", bucket_name, path, len(body))


# --------------------------------------------------------------------------- catalog.parquet ----


def _migrate_catalog(*, apply: bool, full_sweep: bool, sample_size: int, local_snapshot: str | None) -> int:
    if local_snapshot:
        if apply:
            logger.error("--local-catalog-snapshot is read-only-offline; refusing to combine with --apply.")
            return 1
        logger.info("Reading LOCAL offline snapshot: %s (zero GCS calls)", local_snapshot)
        df = pd.read_parquet(local_snapshot)
        source_label = f"local snapshot {local_snapshot}"
        logger.info("Loaded %s — %d total rows", source_label, len(df))
        result = _migrate_dataframe(df, id_col="instrument_id", canonical_col="canonical_instrument_id")
        _report_dry_run(result, source_label=source_label)
        logger.info("DRY-RUN (no writes). Re-run against real GCS with --apply --full-sweep to migrate prod.")
        return 0

    env = _deployment_env()
    bucket_name = _bucket_name()
    catalog_path = _catalog_path(env)

    storage_client = get_storage_client()
    raw = storage_client.download_bytes(bucket_name, catalog_path)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    source_label = f"gs://{bucket_name}/{catalog_path}"
    logger.info("Loaded %s — %d total rows", source_label, len(df))

    result = _migrate_dataframe(df, id_col="instrument_id", canonical_col="canonical_instrument_id")

    if not apply:
        _report_dry_run(result, source_label=source_label)
        logger.info("DRY-RUN (no writes). Re-run with --apply --full-sweep to migrate all rows against prod.")
        return 0

    # write_idx = every row that needs a byte written: id changes (bounded by
    # full_sweep/sample_size) PLUS type-only changes — combo re-stamps (A) and any
    # stored-type case-mismatch on an already-canonical id — which never touch the id
    # column so they'd otherwise be silently dropped from the write-set.
    id_change_idx = result.changed_idx if full_sweep else result.changed_idx[:sample_size]
    write_idx = list(id_change_idx) + list(result.type_only_changed_idx)
    if not write_idx:
        logger.warning("Nothing to migrate — no writes performed.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = f"{env}/backups/{CATALOG_FILENAME}.pre_usd_lin_{ts}.bak.parquet"
    storage_client.upload_bytes(bucket_name, backup_path, raw)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Backup written: gs://%s/%s (%d bytes)", bucket_name, backup_path, len(raw))

    write_set = set(write_idx)
    out_df = df.copy()
    for idx in write_set:
        for col in ("instrument_id", "canonical_instrument_id", "instrument_type", "underlying"):
            if col in result.df.columns:
                out_df.at[idx, col] = result.df.at[idx, col]

    out = io.BytesIO()
    out_df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(bucket_name, catalog_path, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info(
        "APPLIED — %d row(s) migrated (%s), gs://%s/%s rewritten (%d bytes)",
        len(write_set),
        "full sweep" if full_sweep else f"bounded sample of {sample_size}",
        bucket_name,
        catalog_path,
        len(out.getvalue()),
    )

    quarantine_path = f"{env}/backups/{CATALOG_FILENAME}.pre_usd_lin_{ts}.quarantine.json"
    _upload_json_report(
        bucket_name,
        quarantine_path,
        {
            "generated_at": ts,
            "surface": "catalog.parquet",
            "status_counts": result.status_counts,
            "retype_count": result.retype_count,
            "cash_migrated_count": result.cash_migrated_count,
            "combo_restamp_count": result.combo_restamp_count,
            "derivative_total": result.derivative_total,
            "derivative_ok": result.derivative_ok,
            "quarantine_reason_counts": result.quarantine_reason_counts,
            "quarantine_sample": result.quarantine_records,
        },
    )

    # Verify: re-download and prove the target surface is canonical, netting out the
    # KNOWN quarantined rows (bounded + enumerated in the sidecar above).
    verify_raw = storage_client.download_bytes(bucket_name, catalog_path)  # pyright: ignore[reportAttributeAccessIssue]
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    verify_mask = verify_df["instrument_type"].isin(_TARGET_TYPES) | verify_df.index.isin(write_set)
    checked, canonical, violations = assert_tradfi_derivative_ids_canonical(
        verify_df.loc[verify_mask, "instrument_id"].astype(str).tolist(),
        verify_df.loc[verify_mask, "instrument_type"].astype(str).tolist(),
    )
    quarantined_ids = {rec["id"] for rec in result.quarantine_records}
    unexpected = [v for v in violations if not any(v.startswith(f"id={qid!r}") for qid in quarantined_ids)]
    logger.info(
        "Post-apply verify — checked=%d canonical=%d violations_sample=%d unexpected_new_violations=%d",
        checked,
        canonical,
        len(violations),
        len(unexpected),
    )
    if unexpected:
        logger.error("VERIFICATION found unexpected (non-quarantined) violations: %s", unexpected[:10])
        return 1
    logger.info("Verification PASSED.")
    return 0


# ------------------------------------------------------------------------------ per-day corpus ----


#: Bounded BFS depth for :func:`_resolve_venue_dirs` — the CURRENT layout is
#: ``day=*/venue=*/`` (depth 1); a real LEGACY layout was also confirmed live
#: (2026-07-18) one level under some early days:
#: ``day=*/pipeline_mode=batch_instruments_service/asset_group=tradfi/venue=*/``
#: (depth 3). 4 covers both with headroom without risking an unbounded walk.
_MAX_VENUE_RESOLVE_DEPTH = 4


def _resolve_venue_dirs(bucket, prefix: str) -> list[str]:  # Bucket (avoid deep-import for type-only use)
    """BFS through delimited (non-recursive) listings from ``prefix`` until every
    branch reaches a ``venue=`` directory or :data:`_MAX_VENUE_RESOLVE_DEPTH` is
    exhausted.

    Handles BOTH the current shallow ``day=*/venue=*/`` layout AND the legacy
    nested ``day=*/pipeline_mode=*/asset_group=*/venue=*/`` layout confirmed live
    on early (2019-01-xx) day partitions — a naive "every immediate child of
    ``day=`` is a venue dir" assumption 404s on those legacy days (real bug,
    caught by a live bounded dry-run, 2026-07-18). Still delimited listing only
    (one HTTP call per branch per depth) — never a full recursive object walk.
    """
    resolved: list[str] = []
    frontier = [prefix]
    depth = 0
    while frontier and depth < _MAX_VENUE_RESOLVE_DEPTH:
        next_frontier: list[str] = []
        for p in frontier:
            it = bucket.list_blobs(prefix=p, delimiter="/")
            list(it)
            for child in sorted(it.prefixes):
                leaf = child.rstrip("/").rsplit("/", 1)[-1]
                if leaf.startswith("venue="):
                    resolved.append(child)
                else:
                    next_frontier.append(child)
        frontier = next_frontier
        depth += 1
    return resolved


def shard_slice(files: list[str], *, shard_of: int, shard_index: int) -> list[str]:
    """Return this process's disjoint slice of ``files`` (stride partition).

    Public (no leading underscore) because it is the unit under test for the
    disjoint-AND-exhaustive property that makes an N-process ``--apply`` fan-out safe:
    every path lands in EXACTLY one shard, so two concurrent shards can never
    read-modify-write the same object and race each other's rewrite.

    Striding (``files[i::n]``) rather than contiguous blocks (``files[i*k:(i+1)*k]``)
    also spreads the large CME/CBOE files — which dominate per-file runtime — evenly
    across shards instead of piling them onto whichever shard owns that block.
    """
    if shard_of < 1:
        raise ValueError(f"shard_of must be >= 1 (got {shard_of})")
    if not 0 <= shard_index < shard_of:
        raise ValueError(f"shard_index must satisfy 0 <= index < shard_of={shard_of} (got {shard_index})")
    return files[shard_index::shard_of]


def _list_by_date_files(
    storage_client, bucket_name: str
) -> list[str]:  # StorageClient (avoid deep-import for type-only use)
    """Enumerate every real ``instruments.parquet`` per-day-per-venue file via
    delimited (non-recursive) listing — bounded by the real day-partition count
    (2,636 as of 2026-07-18) times a small bounded per-day BFS depth
    (:func:`_resolve_venue_dirs`), NOT a full recursive corpus walk. Mirrors
    ``canonicalize_okx_margin_type_2026_07_09.py::_list_okx_day_files`` but does
    not hardcode a venue allowlist (real venues include CME/CBOE/ICE/KRX/NASDAQ/
    NYSE — filtering to FUTURE/OPTION rows happens per-file via
    ``instrument_type``, not via a venue name check).
    """
    bucket = storage_client.bucket(bucket_name)
    it = bucket.list_blobs(prefix=BY_DATE_PREFIX, delimiter="/")
    list(it)
    day_prefixes = sorted(it.prefixes)
    logger.info("Real day partitions: %d", len(day_prefixes))

    def _check_day(day_prefix: str) -> list[str]:
        return [f"{venue_dir}instruments.parquet" for venue_dir in _resolve_venue_dirs(bucket, day_prefix)]

    files: list[str] = []
    with ThreadPoolExecutor(max_workers=40) as ex:
        futs = [ex.submit(_check_day, dp) for dp in day_prefixes]
        for fut in as_completed(futs):
            files.extend(fut.result())
    return files


@dataclass(slots=True)
class FileMigrationSummary:
    """Per-file result of :func:`_migrate_one_by_day_file` — typed (not a bare dict)
    so the by-day aggregation loop stays basedpyright-clean without ``# type: ignore``.
    """

    path: str
    rows_total: int = 0
    rows_in_scope: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)
    retype_count: int = 0
    changed_count: int = 0
    cash_migrated_count: int = 0
    combo_restamp_count: int = 0
    derivative_total: int = 0
    derivative_ok: int = 0
    quarantine_reason_counts: dict[str, int] = field(default_factory=dict)
    quarantine_sample: list[dict[str, str]] = field(default_factory=list)
    wrote: bool = False
    error: str | None = None


def _migrate_one_by_day_file(  # StorageClient (avoid deep-import for type-only use)
    storage_client, bucket_name: str, path: str, *, apply: bool
) -> FileMigrationSummary:
    """Migrate (or, in dry-run, just analyze) one real per-day-per-venue snapshot file.

    Never raises on a single-file parse issue (shard-level failure isolation) — a file
    that fails to parse or is missing the expected id column is reported as untouched.
    """
    raw = storage_client.download_bytes(bucket_name, path)
    try:
        df = pd.read_parquet(io.BytesIO(raw))
    except (OSError, ValueError) as exc:
        return FileMigrationSummary(path=path, error=str(exc))

    if "instrument_key" not in df.columns:
        return FileMigrationSummary(path=path, rows_total=len(df))

    result = _migrate_dataframe(df, id_col="instrument_key", canonical_col="canonical_instrument_id")
    summary = FileMigrationSummary(
        path=path,
        rows_total=len(df),
        rows_in_scope=result.total_target_rows,
        status_counts=result.status_counts,
        retype_count=result.retype_count,
        changed_count=len(result.changed_idx),
        cash_migrated_count=result.cash_migrated_count,
        combo_restamp_count=result.combo_restamp_count,
        derivative_total=result.derivative_total,
        derivative_ok=result.derivative_ok,
        quarantine_reason_counts=result.quarantine_reason_counts,
        quarantine_sample=result.quarantine_records[:5],
    )

    if not apply or (not result.changed_idx and result.retype_count == 0):
        return summary

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = path.replace("instruments.parquet", f"instruments.usdlin.{ts}.bak.parquet")
    storage_client.upload_bytes(bucket_name, backup_path, raw)

    out = io.BytesIO()
    result.df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(bucket_name, path, out.getvalue())
    summary.wrote = True
    return summary


def _migrate_by_day(
    *, apply: bool, full_sweep: bool, sample_size: int, workers: int, shard_of: int = 1, shard_index: int = 0
) -> int:
    bucket_name = _bucket_name()
    storage_client = get_storage_client()
    files = _list_by_date_files(storage_client, bucket_name)
    logger.info("Real per-day-per-venue files found (single delimited listing): %d", len(files))

    scope = files if full_sweep else files[:sample_size]
    if shard_of > 1:
        before = len(scope)
        scope = shard_slice(scope, shard_of=shard_of, shard_index=shard_index)
        logger.info(
            "Shard %d/%d: %d of %d file(s) in this process's disjoint slice.",
            shard_index,
            shard_of,
            len(scope),
            before,
        )
    logger.info(
        "%s %d file(s) (%s) with %d workers...",
        "Migrating" if apply else "Analyzing (DRY-RUN, no writes)",
        len(scope),
        "full sweep" if full_sweep else f"bounded sample of {sample_size}",
        workers,
    )

    t0 = time.time()
    n_scanned = 0
    n_touched = 0
    total_status_counts: dict[str, int] = {}
    total_retype = 0
    total_cash_migrated = 0
    total_combo_restamp = 0
    total_derivative_total = 0
    total_derivative_ok = 0
    total_reason_counts: dict[str, int] = {}
    all_quarantine: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_migrate_one_by_day_file, storage_client, bucket_name, path, apply=apply): path for path in scope
        }
        for fut in as_completed(futs):
            summary = fut.result()
            n_scanned += 1
            if summary.error:
                logger.warning("  file %s failed to parse: %s (skipped, shard-isolated)", summary.path, summary.error)
            for status, count in summary.status_counts.items():
                total_status_counts[status] = total_status_counts.get(status, 0) + count
            for reason_key, count in summary.quarantine_reason_counts.items():
                total_reason_counts[reason_key] = total_reason_counts.get(reason_key, 0) + count
            total_retype += summary.retype_count
            total_cash_migrated += summary.cash_migrated_count
            total_combo_restamp += summary.combo_restamp_count
            total_derivative_total += summary.derivative_total
            total_derivative_ok += summary.derivative_ok
            all_quarantine.extend(summary.quarantine_sample)
            if summary.wrote:
                n_touched += 1
            if n_scanned % 200 == 0:
                logger.info("  progress %d/%d files scanned (t=%.1fs)", n_scanned, len(scope), time.time() - t0)

    elapsed = time.time() - t0
    ok = sum(total_status_counts.get(s, 0) for s in _OK_STATUSES)
    total_checked = sum(total_status_counts.values())
    logger.info(
        "%s — %d/%d file(s) %s, elapsed=%.1fs. status_counts=%s OK=%d/%d (%.2f%%) retype=%d",
        "APPLIED" if apply else "DRY-RUN complete",
        n_touched if apply else n_scanned,
        len(scope),
        "touched" if apply else "scanned",
        elapsed,
        total_status_counts,
        ok,
        max(1, total_checked),
        100.0 * ok / max(1, total_checked),
        total_retype,
    )
    logger.info("Quarantine breakdown (status:reason -> EXACT count): %s", total_reason_counts)
    logger.info("Phase-B follow-up (B) — CASH rows migrated to the -USD id (count): %d", total_cash_migrated)
    logger.info(
        "Phase-B follow-up (A) — QUARANTINE_COMBO rows re-stamped instrument_type -> COMBO: %d", total_combo_restamp
    )
    logger.info(
        "RESULTING TRUE FUTURE/OPTION canonical %% (classifier-derived denominator, combos excluded): %d / %d (%.4f%%)",
        total_derivative_ok,
        total_derivative_total,
        100.0 * total_derivative_ok / max(1, total_derivative_total),
    )

    if not apply:
        logger.info("Re-run with --apply --by-day-sample N (smoke test) or --by-day-full-sweep to write.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    env = _deployment_env()
    # Shard-qualified so N concurrent shard PROCESSES each land their own sidecar instead of
    # racing one object name (two shards finishing in the same second would otherwise have one
    # silently overwrite the other's quarantine record). Unsharded runs keep the original name.
    shard_suffix = "" if shard_of <= 1 else f"_shard{shard_index}of{shard_of}"
    quarantine_path = f"{env}/backups/by_date_usdlin_quarantine_{ts}{shard_suffix}.json"
    _upload_json_report(
        bucket_name,
        quarantine_path,
        {
            "generated_at": ts,
            "surface": "instrument_availability/by_date",
            "shard_of": shard_of,
            "shard_index": shard_index,
            "files_scanned": n_scanned,
            "files_touched": n_touched,
            "status_counts": total_status_counts,
            "retype_count": total_retype,
            "cash_migrated_count": total_cash_migrated,
            "combo_restamp_count": total_combo_restamp,
            "derivative_total": total_derivative_total,
            "derivative_ok": total_derivative_ok,
            "quarantine_reason_counts": total_reason_counts,
            "quarantine_sample": all_quarantine[: _MAX_QUARANTINE_SAMPLE_PER_STATUS * 4],
        },
    )
    logger.info(
        "IMPORTANT (durability): re-run build_instrument_catalogue.py --asset-group tradfi "
        "next, then re-verify prod/catalog.parquet stays canonical — a catalog-only fix "
        "silently reverts on the next scheduled roll-up otherwise."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write migrated data (default: dry-run/report only).")
    parser.add_argument("--full-sweep", action="store_true", help="prod/catalog.parquet: migrate ALL mismatched rows.")
    parser.add_argument("--sample-size", type=int, default=25, help="prod/catalog.parquet bounded sample size.")
    parser.add_argument(
        "--local-catalog-snapshot",
        type=str,
        default=None,
        help="Dry-run ONLY: read catalog.parquet from this local path instead of GCS (zero GCS calls at all).",
    )
    parser.add_argument(
        "--by-day",
        action="store_true",
        help="Target the per-day instrument_availability corpus instead of prod/catalog.parquet.",
    )
    parser.add_argument(
        "--by-day-sample", type=int, default=20, help="Per-day corpus bounded file count (dry-run or apply)."
    )
    parser.add_argument("--by-day-full-sweep", action="store_true", help="Per-day corpus: process ALL real files.")
    parser.add_argument("--workers", type=int, default=40, help="Thread-pool concurrency for per-day corpus ops.")
    parser.add_argument(
        "--shard-of",
        type=int,
        default=1,
        help=(
            "Total PROCESS shard count for the per-day sweep (default 1 = unsharded). The per-row "
            "canonicalization is pure-Python and therefore GIL-bound, so --workers alone saturates ~1.3 "
            "cores no matter how high it is set (MEASURED 2026-07-20 on an e2-standard-16: 130%% of 1600%% "
            "CPU, 86%% idle, ~1.0 files/s => ~7.4h for the full 27.1k-file sweep). Real speed-up comes from "
            "running N SEPARATE PROCESSES over disjoint shards. Mirrors the --shard-of/--shard-index "
            "convention of market_tick_data_service.scripts.migrate_tradfi_canonical_2026_07."
        ),
    )
    parser.add_argument(
        "--shard-index", type=int, default=0, help="0-based index of THIS process's shard (see --shard-of)."
    )
    args = parser.parse_args()

    if args.shard_of < 1:
        parser.error("--shard-of must be >= 1")
    if not 0 <= args.shard_index < args.shard_of:
        parser.error(f"--shard-index must satisfy 0 <= index < --shard-of ({args.shard_of})")

    _self_check()

    if args.by_day:
        return _migrate_by_day(
            apply=args.apply,
            full_sweep=args.by_day_full_sweep,
            sample_size=args.by_day_sample,
            workers=args.workers,
            shard_of=args.shard_of,
            shard_index=args.shard_index,
        )
    return _migrate_catalog(
        apply=args.apply,
        full_sweep=args.full_sweep,
        sample_size=args.sample_size,
        local_snapshot=args.local_catalog_snapshot,
    )


if __name__ == "__main__":
    raise SystemExit(main())
