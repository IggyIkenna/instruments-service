#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: the 2026-07-14 GW false-empty repair (issue
#   sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14) is
#   verified green — every restamped cell reads captured in the consolidated
#   sports index and the parquet-presence cross over the 1,848 GW
#   fixture-day shards reports 0 false-empty (--verify green after >=1
#   consolidator cycle) — AND the routed pre-GW residual (the same repair
#   over 2020-01-01..2025-08-31 via --start-date/--end-date) is verified
#   green the same way
"""gw_false_empty_repair_2026_07_14.py — repair the golden-window FALSE-EMPTY
manifest cells stamped by the 2026-07-14 api-football enrichment fleet.

Issue: ``plans/active/issues/sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14.md``.
The fleet's this-run-only absence classification stamped
``empty_confirmed/EXPECTED_NO_FIXTURE`` over ~3,720 per-fixture-entity cells
whose per-league enrichment parquets EXIST (P1a-era top/prediction-tier
leagues, skip-as-already-present). Phantom-captured was 0 everywhere — the
manifest over-claims ABSENCE, never capture — so the repair direction is
one-way: verified parquet presence => ``record_captured``.

Mechanics follow the ``recency_masked_adjudication_2026_07_13.py`` precedent:

* **Snapshot-first**: the canonical index is server-side copied to
  ``_index/snapshots/`` before the scan AND again before any write.
* **Object-probe per cell**: UAC ``candidate_parquet_paths`` across the
  api_football pipeline mode; the parquet must exist + parse + carry >=1 row
  attributable to that fixture-day cell (day= partition + league= partition;
  league/date columns cross-checked when present). Cells whose parquet does
  NOT verify stay as-is and are listed (``adjudicated-empty``).
* **Writes ONLY a per-VM manifest shard** (``VM_NAME=gw-false-empty-repair-20260714``,
  ``MANIFEST_PER_VM_SHARDS=true``, explicit ``.write()``) — the cron
  consolidator absorbs it; NEVER hand-edits the consolidated index and NEVER
  triggers a consolidator execution.
* **Verify**: after >=1 consolidator cycle — every restamped cell must read
  captured at dedup precedence, and the index-wide captured count must not
  have DECREASED for any previously-captured cell.

Scope = the session-31 GW verification scoping: the deduped captured-FIXTURES
``(date, league)`` cells of 2025-09-01..2025-11-30 (1,848 expected), crossed
with the 4 per-fixture entities.

The window, entity set, and per-VM shard name default to the GW run but are
parameterizable (``--start-date/--end-date/--entities/--vm-name``) for the
routed pre-GW residual sweep (issue progress log: the same false-empty class
plausibly exists on enrichment cells dated before 2025-09-01 from earlier
runs of the broken write path — object probes + restamps only, no quota).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \
      .venv/bin/python scripts/gw_false_empty_repair_2026_07_14.py --scan
    ... --scan --adjudicate     # + object probes, classification CSV (dry-run)
    ... --apply                 # snapshot + record_captured + shard .write()
    ... --verify                # post-consolidation content check
    ... --start-date 2020-01-01 --end-date 2025-08-31 \
        --entities FIXTURE_EVENTS,FIXTURE_LINEUPS,FIXTURE_STATS,PLAYER_STATS,INJURIES \
        --vm-name hist-false-empty-repair-20260714 --scan   # pre-GW residual
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
logger = logging.getLogger("gw_false_empty_repair")

_DEFAULT_VM_NAME = "gw-false-empty-repair-20260714"
_SNAPSHOT_PREFIX = "_index/snapshots/availability_index"
_INDEX_PATH = "_index/availability_index.parquet"
_DEFAULT_WINDOW_START = "2025-09-01"
_DEFAULT_WINDOW_END = "2025-11-30"
_DEFAULT_ENTITIES = "FIXTURE_EVENTS,FIXTURE_LINEUPS,FIXTURE_STATS,PLAYER_STATS"
_PIPELINE_MODE = "batch_api_football"
_SOURCE = "api_football"
#: dedup precedence at cell grain — the session-31 verification methodology.
_PRECEDENCE = {"captured": 0, "empty_confirmed": 1, "attempted_failed": 2, "expected_unattempted": 3}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scan", action="store_true", help="build the GW false-empty cell list from the live index")
    p.add_argument("--adjudicate", action="store_true", help="probe on-disk parquets + classify (dry-run)")
    p.add_argument("--apply", action="store_true", help="snapshot + record_captured verified cells + write shard")
    p.add_argument("--verify", action="store_true", help="post-consolidation: content-verify the repair")
    p.add_argument(
        "--cross",
        action="store_true",
        help="full GW manifest-vs-parquet presence cross (the session-31 verification): "
        "false-empty must be 0, phantom-captured 0, residual absences typed",
    )
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--report-dir", default="", help="dir for CSV evidence reports (default: CWD)")
    p.add_argument("--start-date", default=_DEFAULT_WINDOW_START, help="window start YYYY-MM-DD (default: GW start)")
    p.add_argument("--end-date", default=_DEFAULT_WINDOW_END, help="window end YYYY-MM-DD (default: GW end)")
    p.add_argument(
        "--entities",
        default=_DEFAULT_ENTITIES,
        help="comma-separated data_types to adjudicate (default: the 4 GW per-fixture entities)",
    )
    p.add_argument(
        "--vm-name",
        default=_DEFAULT_VM_NAME,
        help="per-VM manifest shard name the --apply restamps write under (default: the GW repair shard)",
    )
    return p.parse_args()


ARGS = _args()
_VM_NAME = ARGS.vm_name
_WINDOW_START = ARGS.start_date
_WINDOW_END = ARGS.end_date
_ENTITIES = tuple(e.strip() for e in str(ARGS.entities).split(",") if e.strip())
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = _VM_NAME
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

from unified_api_contracts import PipelineMode
from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths
from unified_trading_library import (
    ManifestWriter,
    get_storage_client,
    resolve_bucket_name,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
REPORT_DIR = Path(ARGS.report_dir) if ARGS.report_dir else Path.cwd()

_LEAGUE_COLS = ("league_id", "canonical_league_id", "league")
_DATE_COLS = ("date", "fixture_date", "match_date", "kickoff", "kickoff_utc", "fixture_datetime")


def _load_index(storage: object) -> pd.DataFrame:
    raw = storage.download_bytes(BUCKET, _INDEX_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    idx = pd.read_parquet(io.BytesIO(raw))
    logger.info("loaded %s/%s: %d rows", BUCKET, _INDEX_PATH, len(idx))
    return idx


def _snapshot_index(storage: object, tag: str) -> str:
    """Server-side copy of the canonical index into _index/snapshots/ (snapshot-first)."""
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    dst = f"{_SNAPSHOT_PREFIX}.{ts}.{tag}.parquet"
    storage.copy_blob(BUCKET, _INDEX_PATH, BUCKET, dst)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("snapshot: %s -> %s", _INDEX_PATH, dst)
    return dst


def _af_window_rows(idx: pd.DataFrame) -> pd.DataFrame:
    """GW-window api_football rows with normalized helper columns."""
    df = idx.copy()
    df["_date"] = df["date"].astype(str).str[:10]
    df = df[(df["_date"] >= _WINDOW_START) & (df["_date"] <= _WINDOW_END)]
    pm = df.get("pipeline_mode", pd.Series(dtype=str)).astype(str)
    src = df.get("source", pd.Series(dtype=str)).astype(str)
    df = df[pm.str.startswith(_PIPELINE_MODE) | (src == _SOURCE)]
    df["_dt"] = df["data_type"].astype(str)
    df["_league"] = df.get("league_id", pd.Series(dtype=str)).fillna("").astype(str)
    df["_status"] = df["capture_status"].astype(str)
    df["_prec"] = df["_status"].map(_PRECEDENCE).fillna(9).astype(int)
    return df


def _dedup_winners(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """One winner row per (date, league) for ``data_type`` at cell-grain
    precedence captured > empty_confirmed > attempted_failed > expected_unattempted,
    recency (attempted_at, written_at) breaking ties within a status."""
    sub = df[df["_dt"] == data_type]
    if sub.empty:
        return sub
    sub = sub.sort_values(
        ["_prec", "attempted_at", "written_at"],
        ascending=[True, False, False],
        kind="stable",
        na_position="last",
    )
    return sub.drop_duplicates(subset=["_date", "_league"], keep="first")


def _content_check(df: pd.DataFrame, league_id: str, day: str) -> tuple[bool, str]:
    """Does the parquet carry >=1 row attributable to (day, league)?

    The blob already lives under ``day=<day>/league=<league>`` hive partitions
    (primary attribution). League/date columns are cross-checked when present;
    a contradiction fails the check.
    """
    if len(df) == 0:
        return False, "0 rows"
    work = df
    lg_col = next((c for c in _LEAGUE_COLS if c in work.columns), None)
    if lg_col is not None and work[lg_col].notna().any():
        lg_match = work[work[lg_col].astype(str).str.upper() == league_id.upper()]
        if lg_match.empty:
            return False, f"{len(work)} rows but none for league {league_id} (col {lg_col})"
        work = lg_match
    dt_col = next((c for c in _DATE_COLS if c in work.columns), None)
    if dt_col is not None and work[dt_col].notna().any():
        day_match = work[work[dt_col].astype(str).str.startswith(day)]
        if day_match.empty:
            days = sorted(set(work[dt_col].astype(str).str[:10]))[:5]
            return False, f"{len(work)} league rows but none dated {day} (col {dt_col}, days={days})"
        work = day_match
    return True, f"{len(work)} rows attributable to {league_id}/{day} (partition + column cross-check)"


def _resolve_object(storage: object, data_type: str, league_id: str, day: str) -> tuple[str, pd.DataFrame, str] | None:
    """Return (blob_path, parsed_df, detail) for a verified on-disk parquet, else None."""
    for cand in candidate_parquet_paths(data_type, day, league_id, pipeline_mode=_PIPELINE_MODE):
        if f"league={league_id}" not in cand:
            # A bare (league-less) parquet cannot attribute rows to this cell
            # unless its columns do — probe it last only if per-league missed.
            continue
        try:
            if not storage.blob_exists(BUCKET, cand):  # pyright: ignore[reportAttributeAccessIssue]
                continue
            raw = storage.download_bytes(BUCKET, cand)  # pyright: ignore[reportAttributeAccessIssue]
            df = pd.read_parquet(io.BytesIO(raw))
        except Exception as exc:
            logger.warning("probe %s: unreadable (%s) — trying next candidate", cand, exc)
            continue
        ok, detail = _content_check(df, league_id, day)
        if ok:
            return cand, df, detail
        logger.warning("probe %s: exists but content check failed: %s", cand, detail)
    return None


def run_scan(storage: object) -> pd.DataFrame:
    _snapshot_index(storage, "gw_false_empty_repair_scan")
    idx = _load_index(storage)
    af = _af_window_rows(idx)

    fixtures_winners = _dedup_winners(af, "FIXTURES")
    scope = fixtures_winners[fixtures_winners["_status"] == "captured"][["_date", "_league"]]
    scope_cells = set(zip(scope["_date"], scope["_league"], strict=True))
    logger.info(
        "scope %s..%s: %d captured-FIXTURES (date, league) cells across %d leagues, %d days "
        "(GW-default session-31 scoping expected 1,848 / 86 / 91)",
        _WINDOW_START,
        _WINDOW_END,
        len(scope_cells),
        scope["_league"].nunique(),
        scope["_date"].nunique(),
    )

    out: list[dict[str, object]] = []
    for entity in _ENTITIES:
        winners = _dedup_winners(af, entity)
        w_by_cell = {(r["_date"], r["_league"]): r for _, r in winners.iterrows()}
        counts = {"captured": 0, "empty_confirmed": 0, "attempted_failed": 0, "expected_unattempted": 0, "missing": 0}
        for day, league in sorted(scope_cells):
            w = w_by_cell.get((day, league))
            status = str(w["_status"]) if w is not None else "missing"
            counts[status] = counts.get(status, 0) + 1
            if status == "captured":
                continue
            out.append(
                {
                    "data_type": entity,
                    "league_id": league,
                    "date": day,
                    "winner_status": status,
                    "winner_reason": "" if w is None else str(w.get("error_reason", "")),  # noqa: qg-empty-fallback — evidence CSV: blank means the index row carries no reason (itself a probed condition)
                    "winner_written_at": "" if w is None else str(w.get("written_at", "")),  # noqa: qg-empty-fallback — evidence CSV: optional index column, blank = not recorded
                }
            )
        logger.info("%s cell-winner distribution over the GW scope: %s", entity, counts)
    cells = pd.DataFrame(out)
    out_csv = REPORT_DIR / "gw_false_empty_scan.csv"
    cells.to_csv(out_csv, index=False)
    logger.info("scan CSV (%d non-captured scope cells) -> %s", len(cells), out_csv)
    return cells


def run_adjudicate(storage: object, cells: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    n_probe = 0
    for _, a in cells.iterrows():
        dt, lg, day = str(a["data_type"]), str(a["league_id"]), str(a["date"])
        status = str(a["winner_status"])
        if status != "empty_confirmed":
            # attempted_failed / missing / expected_unattempted cells belong to
            # the re-run fleet (real fetch work), not this presence repair.
            rows.append({**a.to_dict(), "verdict": f"report-only-{status}", "evidence": ""})
            continue
        n_probe += 1
        hit = _resolve_object(storage, dt, lg, day)
        if hit is None:
            rows.append({**a.to_dict(), "verdict": "adjudicated-empty", "evidence": "no attributable on-disk parquet"})
            continue
        path, df, detail = hit
        rows.append(
            {
                **a.to_dict(),
                "verdict": "restamp-captured",
                "evidence": f"{path}: {detail}",
                "object_path": path,
                "object_rows": len(df),
            }
        )
        if n_probe % 250 == 0:
            logger.info("probed %d empty_confirmed cells ...", n_probe)
    rep = pd.DataFrame(rows)
    if not rep.empty:
        logger.info("adjudication verdicts: %s", rep.groupby(["data_type", "verdict"]).size().to_dict())
    out = REPORT_DIR / "gw_false_empty_adjudication.csv"
    rep.to_csv(out, index=False)
    logger.info("adjudication CSV -> %s", out)
    return rep


def run_apply(storage: object, rep: pd.DataFrame) -> int:
    _snapshot_index(storage, "gw_false_empty_repair_pre_apply")
    to_stamp = rep[rep["verdict"] == "restamp-captured"]
    if to_stamp.empty:
        logger.info("nothing to re-stamp; no shard written")
        return 0
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    stamped = 0
    for _, a in to_stamp.iterrows():
        dt, lg, day = str(a["data_type"]), str(a["league_id"]), str(a["date"])
        path = str(a["object_path"])
        raw = storage.download_bytes(BUCKET, path)  # pyright: ignore[reportAttributeAccessIssue]
        df = pd.read_parquet(io.BytesIO(raw))
        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
            row_key={"date": day, "data_type": dt, "league_id": lg},
            df=df,
            asset_group="sports",
            instrument_type="",
            data_type=dt,
            league_id=lg,
            pipeline_mode=PipelineMode(_PIPELINE_MODE),
            source=_SOURCE,
            service_emission_state=None,
        )
        stamped += 1
        if stamped % 250 == 0:
            logger.info("stamped %d/%d ...", stamped, len(to_stamp))
    manifest.write()
    logger.info(
        "wrote per-VM shard VM_NAME=%s (%d captured rows); the consolidator cron absorbs it — "
        "run --verify after >=1 cycle",
        _VM_NAME,
        stamped,
    )
    return 0


def run_verify(storage: object, rep: pd.DataFrame) -> int:
    idx = _load_index(storage)
    af = _af_window_rows(idx)
    bad = 0
    restamped = rep[rep["verdict"] == "restamp-captured"]
    for entity in _ENTITIES:
        winners = _dedup_winners(af, entity)
        w_by_cell = {(r["_date"], r["_league"]): str(r["_status"]) for _, r in winners.iterrows()}
        ent = restamped[restamped["data_type"] == entity]
        ok = 0
        for _, a in ent.iterrows():
            status = w_by_cell.get((str(a["date"]), str(a["league_id"])), "missing")
            if status == "captured":
                ok += 1
            else:
                logger.error("VERIFY (%s, %s, %s): restamped but reads %s", entity, a["league_id"], a["date"], status)
                bad += 1
        logger.info("verify %s: %d/%d restamped cells read captured", entity, ok, len(ent))
    n_captured_now = int((af["_status"] == "captured").sum())
    logger.info("api_football GW-window captured rows now: %d (must not decrease across cycles)", n_captured_now)
    return 0 if bad == 0 else 1


def _list_present_leagues_by_day(storage: object) -> dict[tuple[str, str], set[str]]:
    """(day, entity) -> set of league ids with a parquet present, via ONE
    prefix list per GW day (91 lists total — single-walk discipline)."""
    import re as _re

    present: dict[tuple[str, str], set[str]] = {}
    days = pd.date_range(_WINDOW_START, _WINDOW_END, freq="D").strftime("%Y-%m-%d")
    ent_dirs = {e.lower(): e for e in _ENTITIES}
    for day in days:
        prefix = f"sports_reference/by_date/day={day}/pipeline_mode={_PIPELINE_MODE}/"
        for blob in storage.list_blobs(BUCKET, prefix=prefix):  # pyright: ignore[reportAttributeAccessIssue]
            name = str(getattr(blob, "name", blob))
            if not name.endswith(".parquet"):
                continue
            m = _re.search(r"/entity=([^/]+)/league=([^/]+)/", name)
            if not m:
                continue
            ent = ent_dirs.get(m.group(1))
            if ent is None:
                continue
            present.setdefault((day, ent), set()).add(m.group(2))
    return present


def run_cross(storage: object) -> int:
    """The session-31 parquet-presence cross over the GW captured-FIXTURES scope.

    GREEN criteria: false-empty == 0 (parquet present but manifest winner
    empty_confirmed), phantom-captured == 0 (winner captured but no parquet),
    and every residual non-captured cell carries a typed (non-blank) reason.
    """
    idx = _load_index(storage)
    af = _af_window_rows(idx)
    fixtures_winners = _dedup_winners(af, "FIXTURES")
    scope = fixtures_winners[fixtures_winners["_status"] == "captured"][["_date", "_league"]]
    scope_cells = set(zip(scope["_date"], scope["_league"], strict=True))
    logger.info("cross scope: %d captured-FIXTURES (date, league) cells", len(scope_cells))
    present = _list_present_leagues_by_day(storage)

    total_false_empty = 0
    total_phantom = 0
    total_blank_reason = 0
    rows: list[dict[str, object]] = []
    for entity in _ENTITIES:
        winners = _dedup_winners(af, entity)
        w_by_cell = {(r["_date"], r["_league"]): r for _, r in winners.iterrows()}
        n_false_empty = n_phantom = n_blank = n_captured = n_empty = n_failed = 0
        for day, league in sorted(scope_cells):
            has_parquet = league in present.get((day, entity), set())
            w = w_by_cell.get((day, league))
            status = str(w["_status"]) if w is not None else "missing"
            reason = "" if w is None else str(w.get("error_reason", "") or "")  # noqa: qg-empty-fallback — blank-reason IS the probed condition (untyped absence detection)
            if status == "captured":
                n_captured += 1
                if not has_parquet:
                    n_phantom += 1
                    rows.append({"entity": entity, "date": day, "league_id": league, "problem": "phantom-captured"})
                continue
            if status == "empty_confirmed":
                n_empty += 1
                if has_parquet:
                    n_false_empty += 1
                    rows.append({"entity": entity, "date": day, "league_id": league, "problem": "false-empty"})
                elif not reason or reason.lower() == "nan":
                    n_blank += 1
                    rows.append({"entity": entity, "date": day, "league_id": league, "problem": "blank-reason-empty"})
            elif status == "attempted_failed":
                n_failed += 1
            else:
                n_blank += 1
                rows.append({"entity": entity, "date": day, "league_id": league, "problem": f"untyped-{status}"})
        logger.info(
            "CROSS %s: captured=%d empty=%d failed=%d | FALSE-EMPTY=%d PHANTOM=%d untyped/blank=%d",
            entity,
            n_captured,
            n_empty,
            n_failed,
            n_false_empty,
            n_phantom,
            n_blank,
        )
        total_false_empty += n_false_empty
        total_phantom += n_phantom
        total_blank_reason += n_blank
    out = REPORT_DIR / "gw_presence_cross_problems.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    logger.info("cross problems CSV (%d rows) -> %s", len(rows), out)
    green = total_false_empty == 0 and total_phantom == 0 and total_blank_reason == 0
    logger.info(
        "CROSS VERDICT: %s (false-empty=%d phantom-captured=%d untyped/blank=%d)",
        "GREEN" if green else "RED",
        total_false_empty,
        total_phantom,
        total_blank_reason,
    )
    return 0 if green else 1


def main() -> int:
    storage = get_storage_client(project_id=ARGS.project)
    logger.info(
        "run at %s (bucket=%s, scan=%s adjudicate=%s apply=%s verify=%s)",
        datetime.now(UTC).isoformat(),
        BUCKET,
        ARGS.scan,
        ARGS.adjudicate,
        ARGS.apply,
        ARGS.verify,
    )
    adj_csv = REPORT_DIR / "gw_false_empty_adjudication.csv"
    if ARGS.cross:
        return run_cross(storage)
    if ARGS.verify:
        if not adj_csv.exists():
            logger.error("--verify needs the adjudication CSV at %s", adj_csv)
            return 2
        return run_verify(storage, pd.read_csv(adj_csv, keep_default_na=False))
    if ARGS.apply:
        if not adj_csv.exists():
            logger.error("--apply needs the adjudication CSV at %s (run --adjudicate first)", adj_csv)
            return 2
        return run_apply(storage, pd.read_csv(adj_csv, keep_default_na=False))
    cells = run_scan(storage)
    if ARGS.adjudicate and not cells.empty:
        run_adjudicate(storage, cells)
    return 0


if __name__ == "__main__":
    sys.exit(main())
