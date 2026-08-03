#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 rows with data_type="FIXTURES" remain in
#   instruments-store-sports-{env} (verified via this script's own dry-run
#   affected-count == 0, immediately after apply AND after >=2 manifest-
#   consolidator cycles).
"""Re-stamp legacy sports manifest `data_type="FIXTURES"` -> canonical
`FIXTURES_SCHEDULE` (`sports_closeout_batch1_ao_ready_2026_07_24.md`'s
`[DATA] P0` backfill todo; full analysis + corrected design:
`plans/active/issues/fixtures_manifest_legacy_backfill_2026_07_24.md`).

WHY THIS EXISTS
---------------
`sports_closeout_batch1_ao_ready_2026_07_24.md` todo 1's writer/reader code
migration shipped (`instruments-service@e19c5a7a`) -- every NEW manifest
write now uses `FIXTURES_SCHEDULE` instead of the hardcoded `"FIXTURES"`
literal, and the companion UAC `SCHEDULE_DEFINING_DATA_TYPES` change shipped
separately (`unified-api-contracts@6d9c7b59`). But 337,464 pre-cutover rows
in the sports manifest still carry the legacy `data_type="FIXTURES"` atom
(read-only prod census, 2026-07-24, bucket
`instruments-store-sports-prd-central-element-323112`) -- confirmed STATIC,
not growing: the one live leak that was continuously re-creating legacy rows
(`sports_fixture_status_refresh.py`) is already fixed + shipped
(`instruments-service@47c1ffb3`).

WHY A SIMPLE 1:1 RESTAMP, NOT A 1-TO-2 FAN-OUT
-----------------------------------------------
An earlier design pass assumed a legacy `FIXTURES` row must fan out into TWO
manifest rows (`FIXTURES_SCHEDULE` + `FIXTURES_OUTCOMES`), mirroring
`migrate_fixtures_split.py`'s GCS-object-level split. That is WRONG for the
manifest: an exhaustive grep across every repo for `FIXTURES_OUTCOMES` as a
manifest `data_type=` write found ZERO live call sites anywhere --
`FIXTURES_OUTCOMES` is a GCS-object-only entity label
(`entity=fixtures_outcomes/`), never a tracked manifest `data_type`. Both
already-migrated sibling call sites (`process_write.py`,
`sports_fixtures_daily_repoll.py`) emit `record_captured()`/
`record_failed()`/`record_empty()` using `FIXTURES_SCHEDULE` ONLY. The
established, PROVEN codebase convention is: one manifest atom per
fixture-capture event. So this is a 1:1 in-place restamp
(`data_type: "FIXTURES"` -> `"FIXTURES_SCHEDULE"`), NOT a fan-out -- no GCS
reads/re-derivation needed, no per-(date,league) enumeration, no
row-filtering convention question.

A row DELETE is verified-UNSAFE for this manifest family (`_legacy_seed.parquet`
resurrection -- see `plans/archive/issues/legacy_seed_captured_outranks_resurrection_risk_2026_07_15.md`),
hence a re-stamp, never a delete -- exactly mirroring this same plan's
already-completed sibling precedent:
`market-tick-data-service/scripts/restamp_sports_odds_horizon_bucket_2026_07_22.py`
(same repo family, same "re-stamp not delete", same snapshot -> mask ->
rewrite -> CAS pattern -- mirrored here, not re-derived).

GROUND TRUTH (live-measured 2026-07-24, GCP `central-element-323112`,
`instruments-store-sports-prd-central-element-323112/_index/availability_index.parquet`,
5,526,420 total rows)
----------------------------------------------------------------------------------
| data_type          | rows    |
|--------------------|---------|
| FIXTURES            | 337,464 | <- legacy atom, this script's target
| FIXTURES_SCHEDULE    | 57,458  | <- restamp DESTINATION, count grows by len(safe_idx)
| FIXTURES_OUTCOMES    | 57,039  | <- separate entity, untouched by construction

DRY-RUN RESULT (measured 2026-07-24, same corpus): of the 337,464 candidate rows,
282,231 are SAFE to restamp (no collision) and 55,233 ESCALATE (left untouched) --
their post-restamp dedup key already collides with an EXISTING `FIXTURES_SCHEDULE`
row (0 internal collisions -- the 337,464 candidates never collide with EACH
OTHER, only with rows already migrated). This is expected: the
`sports_fixture_status_refresh.py` leak (fixed at `instruments-service@47c1ffb3`)
continuously re-wrote legacy `FIXTURES` rows for dates that the correct writer
(`sports_fixtures_daily_repoll.py`) had ALREADY captured under `FIXTURES_SCHEDULE`
post-cutover -- so a genuine dual-write duplicate exists for those
(date, venue, league, ...) cells under two different atoms. `classify()`
correctly ESCALATES (never drops) these -- a follow-up decision on the 55,233
residual duplicate legacy rows (delete vs. leave) is OUT OF SCOPE for this
1:1 restamp and tracked separately; this script only ever touches the SAFE set.

SAFETY MODEL (mirrors `restamp_sports_odds_horizon_bucket_2026_07_22.py`)
------------------------------------------------------------------------------
Every apply attempt: fresh CAS read -> `classify()` (collision detection
against the real production dedup key) -> fast proof-based pre-write gate
(ABORTS with zero write on ANY invariant mismatch) -> serialize matching the
production writer's exact `to_parquet` convention -> CAS write
(`if_generation_match`) -> on success, full post-write verification (row
count, zero duplicate keys, `FIXTURES_SCHEDULE` grew by EXACTLY `len(safe_idx)`,
`FIXTURES_OUTCOMES` population untouched, remaining legacy rows == the
escalated count). A `gate_failed` status is a real problem and does not retry; a
`cas_failed` status is an expected race and retries against fresh state.

CONTENTION VERDICT: CONTENDED -- do not `--apply` without a paused-cron
window
------------------------------------------------------------------------------
`instruments-sports` has its OWN `*/1 * * * *` Cloud Scheduler cron
(`deployment-service/terraform/gcp/manifest_consolidator_scheduler.tf`'s
`manifest_consolidator_buckets["instruments-sports"]` entry, same uniform
`for_each` schedule as every other bucket -- every minute, no exception).
This bucket ALSO carries a documented history of consolidator
livelock/slow-run incidents specific to it
(`manifest_consolidator_instruments_sports_intermittent_slow_run_2026_07_14.md`,
`instruments_sports_manifest_consolidator_lock_livelock_2026_07_15.md` --
lock TTL bumped to 2400s / timeout to 1800s for this bucket specifically,
vs. the 300s default), so a CAS race here is MORE likely to eat the full
retry budget than on a quieter bucket. Per `SUB_AGENT_MANDATORY_RULES.md`:
do NOT pause this cron autonomously. `--apply` should only be run during an
operator-authorized paused-cron window -- mirror the documented
pause/impersonation/resume recipe from
`distinct_values_noncanonical_audit_2026_07_20.md`'s 2026-07-22 Progress Log
entry (same recipe, different bucket), do not re-derive.

DRY-RUN BY DEFAULT; `--apply` performs the live CAS write.

Usage::

    # Dry run (read-only, prints exactly what WOULD change, no write):
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python \\
      scripts/restamp_fixtures_manifest_legacy_atom_2026_07_24.py

    # Apply (ONLY during an operator-authorized paused-cron window):
    GCP_PROJECT_ID=central-element-323112 nohup .venv/bin/python \\
      scripts/restamp_fixtures_manifest_legacy_atom_2026_07_24.py --apply \\
      > /path/to/logfile 2>&1 &

Plan: unified-trading-pm/plans/active/sports_closeout_batch1_ao_ready_2026_07_24.md
Issue: unified-trading-pm/plans/active/issues/fixtures_manifest_legacy_backfill_2026_07_24.md
"""

from __future__ import annotations

import argparse
import io
import time
from dataclasses import dataclass

import pandas as pd
from unified_trading_library import gcs_copy_object, get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
IDX = "_index/availability_index.parquet"

LEGACY_DATA_TYPE = "FIXTURES"
CANONICAL_DATA_TYPE = "FIXTURES_SCHEDULE"

# The ONE population this script must NEVER change the COUNT of --
# `FIXTURES_OUTCOMES` is a separate entity `affected_mask` never touches.
# NOTE: `CANONICAL_DATA_TYPE` (`FIXTURES_SCHEDULE`) is NOT in this set -- it is
# the restamp DESTINATION, so its count is EXPECTED to grow by exactly
# `len(safe_idx)`; that invariant is checked separately (see
# `_pre_write_gate`/post-write verification), not via this untouched-count list.
UNTOUCHED_DATA_TYPES: tuple[str, ...] = ("FIXTURES_OUTCOMES",)

# Mirrors `unified_trading_library.manifest_consolidator._BASE_DEDUP_COLS` /
# `_OPTIONAL_DEDUP_COLS` EXACTLY (private module constants, hardcoded here
# rather than imported -- same convention as
# `restamp_sports_odds_horizon_bucket_2026_07_22.py`).
BASE_DEDUP_COLS: tuple[str, ...] = ("date", "venue", "data_type", "service_name")
OPTIONAL_DEDUP_COLS: tuple[str, ...] = (
    "timeframe",
    "league_id",
    "chain",
    "instrument_type",
    "underlying",
    "feature_group",
    "model_family",
    "training_period",
    "strategy_id",
    "client_id",
    "instruction_type",
    "instrument_id",
)

MAX_ATTEMPTS = 25
MAX_WALL_SECONDS = 3 * 3600  # 3h safety cap regardless of attempt count


def resolve_dedup_cols(columns: list[str]) -> list[str]:
    """The real production dedup key: 4 base cols + every optional dim present in
    the frame's schema. Mirrors
    `unified_trading_library.manifest_consolidator._resolve_dedup_cols` exactly.
    """
    return list(BASE_DEDUP_COLS) + [c for c in OPTIONAL_DEDUP_COLS if c in columns]


def normalize_key_cols(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """NaN/None -> "" per column (matches production's NULL/"" coalesce in
    `_dedup_key_sql` -- both mean "not applicable"). Deliberately native
    `.fillna` on the existing (already object/string) columns, NOT a blanket
    `.astype(str)` composite-string-key + Python `set()` -- that approach
    OOM-killed the first `--apply` attempt on the real 5.5M-row corpus
    (materializing 66M+ individual Python string objects, then a ~5.2M-entry
    Python set, is far more memory-hungry than pandas' native vectorized
    C-level `duplicated()`/`merge()` used below).
    """
    return frame[cols].fillna("")


def affected_mask(data_type: pd.Series) -> pd.Series:
    """True only for the legacy bare `FIXTURES` string -- never true for
    `FIXTURES_SCHEDULE`/`FIXTURES_OUTCOMES`, so those populations are
    structurally excluded by construction.
    """
    return data_type == LEGACY_DATA_TYPE


@dataclass
class ClassifyResult:
    safe_idx: pd.Index
    escalate_idx: pd.Index
    internal_collision_idx: pd.Index
    external_collision_idx: pd.Index
    new_data_type: pd.Series
    n_dup_existing: int
    n_affected: int = 0


def classify(df: pd.DataFrame) -> ClassifyResult:
    """Classify every affected row as SAFE (re-stamp it) or ESCALATE (leave it
    untouched -- a genuine collision). Never drops a row.

    Vectorized via pandas' native `duplicated()`/`merge()` (C-level hash
    tables) rather than a hand-rolled Python string composite key + `set()` --
    the latter OOM-killed the first `--apply` attempt at this corpus's real
    scale (5.5M rows). See :func:`normalize_key_cols`.
    """
    dedup_cols = resolve_dedup_cols(list(df.columns))
    norm = normalize_key_cols(df, dedup_cols)
    n_dup_existing = int(norm.duplicated(keep="first").sum())

    mask = affected_mask(df["data_type"])
    candidate_idx = df.index[mask]

    restamped_norm = norm.loc[candidate_idx].copy()
    restamped_norm["data_type"] = CANONICAL_DATA_TYPE

    internal_dup_mask = restamped_norm.duplicated(keep=False)
    internal_collision_idx = candidate_idx[internal_dup_mask.values]

    non_internal_idx = candidate_idx[~internal_dup_mask.values]
    rest_norm = norm.loc[~norm.index.isin(candidate_idx)]
    # Dedup the "rest" side before the join purely to keep the merge's output
    # small (membership is all that matters, not multiplicity) -- does not
    # change which candidate rows are found to collide.
    rest_norm_unique = rest_norm.drop_duplicates(subset=dedup_cols)

    to_check = restamped_norm.loc[non_internal_idx].copy()
    to_check["_orig_idx"] = non_internal_idx
    merged = to_check.merge(rest_norm_unique, on=dedup_cols, how="inner")
    external_collision_idx = pd.Index(merged["_orig_idx"].unique())

    external_dup_mask = non_internal_idx.isin(external_collision_idx)
    safe_idx = non_internal_idx[~external_dup_mask]
    escalate_idx = internal_collision_idx.union(external_collision_idx)

    return ClassifyResult(
        safe_idx=safe_idx,
        escalate_idx=escalate_idx,
        internal_collision_idx=internal_collision_idx,
        external_collision_idx=external_collision_idx,
        new_data_type=pd.Series(CANONICAL_DATA_TYPE, index=safe_idx),
        n_dup_existing=n_dup_existing,
        n_affected=len(candidate_idx),
    )


def build_final(df: pd.DataFrame, result: ClassifyResult) -> pd.DataFrame:
    """Copy-then-mutate-only-`safe_idx` -- every other row is byte-identical to
    `df` BY CONSTRUCTION (no full-frame equality check needed to prove it).
    """
    final_df = df.copy()
    final_df.loc[result.safe_idx, "data_type"] = result.new_data_type
    return final_df


def _print_report(df: pd.DataFrame, result: ClassifyResult) -> None:
    print(f"pre-existing duplicate keys in source corpus (production dedup key): {result.n_dup_existing}")
    print(f"affected rows (legacy data_type={LEGACY_DATA_TYPE!r}): {result.n_affected}")
    print(f"internal collisions (post-restamp keys collide with each other): {len(result.internal_collision_idx)}")
    print(
        f"external collisions (post-restamp keys collide with rest of manifest): {len(result.external_collision_idx)}"
    )
    print(f"SAFE to re-stamp: {len(result.safe_idx)}")
    print(f"ESCALATE (left untouched): {len(result.escalate_idx)}")
    n_dest_before = int((df["data_type"] == CANONICAL_DATA_TYPE).sum())
    print(
        f"restamp destination data_type={CANONICAL_DATA_TYPE!r} before: {n_dest_before} (expected to GROW by SAFE count)"
    )
    for dt in UNTOUCHED_DATA_TYPES:
        n = int((df["data_type"] == dt).sum())
        print(f"untouched-by-construction population data_type={dt!r}: {n}")


def _pre_write_gate(df: pd.DataFrame, final_df: pd.DataFrame, result: ClassifyResult) -> tuple[bool, str]:
    dedup_cols = resolve_dedup_cols(list(final_df.columns))

    if result.n_dup_existing != 0:
        return False, f"{result.n_dup_existing} pre-existing duplicate keys in source corpus"

    if len(final_df) != len(df):
        return False, f"row count changed: {len(df)} -> {len(final_df)} (this script never adds/drops rows)"

    n_post_dup = int(normalize_key_cols(final_df, dedup_cols).duplicated(keep="first").sum())
    if n_post_dup != 0:
        return False, f"{n_post_dup} duplicate keys AFTER restamp (classify() collision detection has a bug)"

    for dt in UNTOUCHED_DATA_TYPES:
        n_before = int((df["data_type"] == dt).sum())
        n_after = int((final_df["data_type"] == dt).sum())
        if n_before != n_after:
            return False, f"{dt} row count changed: {n_before} -> {n_after}"

    n_dest_before = int((df["data_type"] == CANONICAL_DATA_TYPE).sum())
    n_dest_after = int((final_df["data_type"] == CANONICAL_DATA_TYPE).sum())
    expected_dest_after = n_dest_before + len(result.safe_idx)
    if n_dest_after != expected_dest_after:
        return (
            False,
            f"{CANONICAL_DATA_TYPE} row count {n_dest_before} -> {n_dest_after}, "
            f"expected {n_dest_before} -> {expected_dest_after} (= before + SAFE count {len(result.safe_idx)})",
        )

    remaining_legacy = int(affected_mask(final_df["data_type"]).sum())
    if remaining_legacy != len(result.escalate_idx):
        return (
            False,
            f"remaining legacy FIXTURES rows {remaining_legacy} != escalate count {len(result.escalate_idx)}",
        )

    if set(df.columns) != set(final_df.columns):
        return (
            False,
            f"COLUMN MISMATCH: lost={set(df.columns) - set(final_df.columns)} gained={set(final_df.columns) - set(df.columns)}",
        )

    return True, ""


def try_once(c, t0: float, attempt: int) -> tuple[str, dict]:
    """One full read-classify-verify-write cycle.
    Returns (status, info) where status is 'success' | 'cas_failed' | 'gate_failed'.
    """
    raw, generation = c.download_bytes_with_generation(BUCKET, IDX)
    if raw is None:
        return "gate_failed", {"reason": "index object not found"}
    print(
        f"[{time.monotonic() - t0:.1f}s] (attempt {attempt}) read {len(raw)} bytes at generation={generation}",
        flush=True,
    )

    df = pd.read_parquet(io.BytesIO(raw))
    n0 = len(df)
    print(f"[{time.monotonic() - t0:.1f}s] loaded {n0} rows", flush=True)

    result = classify(df)
    print(
        f"[{time.monotonic() - t0:.1f}s] classified: safe={len(result.safe_idx)} "
        f"escalate={len(result.escalate_idx)} "
        f"(internal_collision={len(result.internal_collision_idx)} "
        f"external_collision={len(result.external_collision_idx)}) "
        f"pre_existing_dups={result.n_dup_existing}",
        flush=True,
    )

    final_df = build_final(df, result)

    ok, reason = _pre_write_gate(df, final_df, result)
    if not ok:
        print(f"ABORT: {reason}")
        return "gate_failed", {"reason": reason}

    print(f"[{time.monotonic() - t0:.1f}s] pre-write gate PASSED. Serializing + uploading...", flush=True)
    final_df = final_df.sort_values(["date", "venue", "data_type"]).reset_index(drop=True)
    out_buf = io.BytesIO()
    # EXACT match to the production ManifestWriter's to_parquet call
    # (unified_trading_library/manifest_writer/_writer_io.py:706/847/958).
    final_df.to_parquet(out_buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    out_bytes = out_buf.getvalue()
    print(
        f"[{time.monotonic() - t0:.1f}s] serialized {len(out_bytes)} bytes. "
        f"Writing with if_generation_match={generation}...",
        flush=True,
    )

    new_generation = None
    delays = [1.0, 2.0, 4.0]
    for sub_attempt in range(len(delays) + 1):
        try:
            new_generation = c.conditional_upload_bytes(BUCKET, IDX, out_bytes, if_generation_match=generation)
            break
        except Exception as exc:
            if sub_attempt >= len(delays) or "429" not in str(exc):
                raise
            print(
                f"[{time.monotonic() - t0:.1f}s] 429 on sub-attempt {sub_attempt}, retrying in {delays[sub_attempt]}s...",
                flush=True,
            )
            time.sleep(delays[sub_attempt])

    if new_generation is None:
        print(
            f"[{time.monotonic() - t0:.1f}s] CAS precondition FAILED (generation={generation} stale) — "
            f"a concurrent writer changed the index. NO WRITE PERFORMED THIS ATTEMPT.",
            flush=True,
        )
        return "cas_failed", {"generation": generation}

    print(f"[{time.monotonic() - t0:.1f}s] WRITE SUCCEEDED. New generation={new_generation}", flush=True)

    # ---- POST-WRITE VERIFICATION ----
    verify_raw, verify_gen = c.download_bytes_with_generation(BUCKET, IDX)
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    print(
        f"[{time.monotonic() - t0:.1f}s] post-write verify: generation={verify_gen} rows={len(verify_df)} "
        f"(expected {len(final_df)})",
        flush=True,
    )
    assert verify_gen == new_generation, "post-write generation mismatch — a write raced us right after ours landed"
    assert len(verify_df) == len(final_df)

    dedup_cols = resolve_dedup_cols(list(verify_df.columns))
    verify_dup = int(normalize_key_cols(verify_df, dedup_cols).duplicated(keep="first").sum())
    print(f"post-write duplicate keys: {verify_dup} (must be 0)", flush=True)
    assert verify_dup == 0

    for dt in UNTOUCHED_DATA_TYPES:
        n_expected = int((df["data_type"] == dt).sum())
        n_got = int((verify_df["data_type"] == dt).sum())
        print(f"post-write {dt} rows: {n_got} (expected {n_expected})", flush=True)
        assert n_got == n_expected

    n_dest_before = int((df["data_type"] == CANONICAL_DATA_TYPE).sum())
    n_dest_expected = n_dest_before + len(result.safe_idx)
    n_dest_got = int((verify_df["data_type"] == CANONICAL_DATA_TYPE).sum())
    print(
        f"post-write {CANONICAL_DATA_TYPE} rows: {n_dest_got} (expected {n_dest_expected} = "
        f"{n_dest_before} before + {len(result.safe_idx)} restamped)",
        flush=True,
    )
    assert n_dest_got == n_dest_expected

    verify_remaining = int(affected_mask(verify_df["data_type"]).sum())
    print(
        f"post-write remaining legacy FIXTURES rows: {verify_remaining} (expected {len(result.escalate_idx)})",
        flush=True,
    )
    assert verify_remaining == len(result.escalate_idx)

    cols_before = set(df.columns)
    cols_after = set(verify_df.columns)
    print(f"columns preserved: {cols_before == cols_after}", flush=True)
    assert cols_before == cols_after, (
        f"COLUMN MISMATCH: lost={cols_before - cols_after} gained={cols_after - cols_before}"
    )

    return "success", {
        "n0": n0,
        "n_final": len(final_df),
        "safe": len(result.safe_idx),
        "escalate": len(result.escalate_idx),
        "old_generation": generation,
        "new_generation": new_generation,
    }


def dry_run(c) -> None:
    raw, generation = c.download_bytes_with_generation(BUCKET, IDX)
    if raw is None:
        print("ABORT: index object not found")
        return
    print(f"read {len(raw)} bytes at generation={generation}")
    df = pd.read_parquet(io.BytesIO(raw))
    print(f"loaded {len(df)} rows\n")
    result = classify(df)
    _print_report(df, result)
    final_df = build_final(df, result)
    ok, reason = _pre_write_gate(df, final_df, result)
    print(f"\npre-write gate would: {'PASS' if ok else 'ABORT — ' + reason}")
    print("\nDRY RUN — manifest untouched. Re-run with --apply (ONLY during an operator-authorized")
    print("paused-cron window — see module docstring CONTENTION VERDICT) to write.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Perform the live CAS-guarded write. Default is dry-run.")
    args = parser.parse_args()

    t0 = time.monotonic()
    c = get_storage_client(project_id="central-element-323112")

    if not args.apply:
        dry_run(c)
        return

    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    backup_path = f"_index/backups/availability_index.pre_fixtures_legacy_restamp_apply_{ts}.parquet"
    gcs_copy_object(f"gs://{BUCKET}/{IDX}", f"gs://{BUCKET}/{backup_path}")
    print(f"[{time.monotonic() - t0:.1f}s] pre-apply snapshot: gs://{BUCKET}/{backup_path}", flush=True)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        if time.monotonic() - t0 > MAX_WALL_SECONDS:
            print(
                f"\n[{time.monotonic() - t0:.1f}s] WALL-CLOCK CAP ({MAX_WALL_SECONDS}s) reached after {attempt - 1} attempts. NO WRITE PERFORMED. Manifest unchanged."
            )
            return
        status, info = try_once(c, t0, attempt)
        if status == "success":
            print(f"\n[{time.monotonic() - t0:.1f}s] APPLY COMPLETE AND VERIFIED (attempt {attempt}).")
            print(
                f"SUMMARY: {info['n0']} -> {info['n_final']} rows. Re-stamped: {info['safe']}. Escalated (untouched): {info['escalate']}."
            )
            print(f"Pre-apply snapshot: gs://{BUCKET}/{backup_path}")
            print(f"Old generation: {info['old_generation']} -> New generation: {info['new_generation']}")
            return
        if status == "gate_failed":
            print(f"\n[{time.monotonic() - t0:.1f}s] GATE FAILED (attempt {attempt}): {info['reason']}")
            print("NOT retrying — this is a real invariant problem, not a race. Investigate before re-running.")
            return
        print(f"[{time.monotonic() - t0:.1f}s] Retrying (attempt {attempt + 1}/{MAX_ATTEMPTS})...\n", flush=True)

    print(
        f"\n[{time.monotonic() - t0:.1f}s] EXHAUSTED {MAX_ATTEMPTS} attempts, all lost the CAS race against "
        f"concurrent writers. NO WRITE PERFORMED. The manifest is unchanged from before this script ran. "
        f"Snapshot (unused): gs://{BUCKET}/{backup_path}"
    )


if __name__ == "__main__":
    main()
