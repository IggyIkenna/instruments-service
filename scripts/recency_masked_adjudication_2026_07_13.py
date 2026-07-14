#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: one-off
# Delete-when: the 189 recency-masked atoms (issue
#   sports_index_recency_masked_captured_atoms_2026_07_13) are adjudicated and
#   every re-stamped atom reads captured in the consolidated sports index
#   across >=2 consolidator cycles (--verify green twice)
"""recency_masked_adjudication_2026_07_13.py — adjudicate the 189 atoms where a
later ``empty_confirmed`` row recency-masks a still-present ``captured`` row.

Issue: ``plans/active/issues/sports_index_recency_masked_captured_atoms_2026_07_13.md``.

Subclasses (regenerated live, NOT hardcoded — the doc's single-index recipe):

* (a) PLAYER_STATS — captured rows written 2026-05-06 by
  ``service_name=fill-missing-player-stats`` (row_count NaN) masked by
  EXPECTED_NO_FIXTURE empty rows re-stamped 2026-07-13. Adjudication: probe
  the on-disk object (UAC ``candidate_parquet_paths``); object exists +
  parses + >=1 row => captured is TRUE => re-stamp captured at the masking
  empty row's dedup identity; absent => the empty stamp stands
  (adjudicated-empty, NO write — the stale captured row stays recency-masked,
  which is the honest read).
* (b) FIXTURES — DELIBERATE truthset-evidenced flips
  (``...truthset_20260628_confirms_no_fixtures``) contradicting captured rows
  (venue=API_FOOTBALL). An on-disk object here is a CONTRADICTION: inspect
  the parquet content (fixtures for that exact league/date?); genuine fixture
  rows => re-stamp captured + record the truthset gap; empty/mismatched
  parquet => flip stands. Ambiguous => decision packet, NO write.

Writes ONLY a per-VM manifest shard (``VM_NAME=recency-repair-20260713``,
``MANIFEST_PER_VM_SHARDS=true``, explicit ``.write()``) — the cron
consolidator absorbs it; NEVER hand-edits the consolidated index and NEVER
triggers a consolidator execution (prune-race issue 2026-07-13).
Snapshot-first: the canonical index is copied to ``_index/snapshots/`` before
any write phase.

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \
      .venv/bin/python scripts/recency_masked_adjudication_2026_07_13.py --scan
    ... --adjudicate            # dry-run probes + classification CSV
    ... --apply                 # snapshot + record_captured + shard .write()
    ... --verify                # post-consolidation content check
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
logger = logging.getLogger("recency_masked_adjudication")

_VM_NAME = "recency-repair-20260713"
_SNAPSHOT_PREFIX = "_index/snapshots/availability_index"
_INDEX_PATH = "_index/availability_index.parquet"
#: probe order — PLAYER_STATS/FIXTURES captured rows are api_football-sourced
#: (fill_missing_player_stats.py SOURCE="api_football"; FIXTURES venue=API_FOOTBALL);
#: footystats probed second for completeness.
_PIPELINE_MODES = ("batch_api_football", "batch_footystats")
_SOURCE_BY_MODE = {"batch_api_football": "api_football", "batch_footystats": "footystats"}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scan", action="store_true", help="regenerate the masked-atom list from the live index")
    p.add_argument("--adjudicate", action="store_true", help="probe on-disk objects + classify (dry-run)")
    p.add_argument("--apply", action="store_true", help="snapshot + record_captured verified atoms + write shard")
    p.add_argument("--verify", action="store_true", help="post-consolidation: content-verify the adjudication")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--report-dir", default="", help="dir for CSV evidence reports (default: CWD)")
    return p.parse_args()


ARGS = _args()
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

_IDENTITY_DIMS = ("service_name", "venue", "instrument_type", "timeframe", "source")


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


def _masked_atoms(idx: pd.DataFrame) -> pd.DataFrame:
    """The doc's regeneration recipe: group by (data_type, league_id, date);
    keep groups holding BOTH captured and empty_confirmed rows; sort by
    (attempted_at, written_at); flag groups whose LAST row is not captured.

    Returns one row per flagged atom carrying the winner's status + the
    masking (winner) row's identity dims (needed to stamp captured into the
    SAME consolidator dedup group).
    """
    df = idx.copy()
    df["league_id"] = df.get("league_id", pd.Series(dtype=str)).fillna("").astype(str)
    df["_dt"] = df["data_type"].astype(str)
    df["_date"] = df["date"].astype(str)
    df = df.sort_values(["attempted_at", "written_at"], kind="stable", na_position="first")
    out: list[dict[str, object]] = []
    for (dt, lg, day), grp in df.groupby(["_dt", "league_id", "_date"], sort=False):
        statuses = set(grp["capture_status"].astype(str))
        if "captured" not in statuses or "empty_confirmed" not in statuses:
            continue
        winner = grp.iloc[-1]
        w_status = str(winner["capture_status"])
        if w_status == "captured":
            continue
        cap_rows = grp[grp["capture_status"].astype(str) == "captured"]
        last_cap = cap_rows.iloc[-1]
        tied = str(winner.get("attempted_at", "")) == str(last_cap.get("attempted_at", "")) and str(
            winner.get("written_at", "")
        ) == str(last_cap.get("written_at", ""))
        rec: dict[str, object] = {
            "data_type": dt,
            "league_id": lg,
            "date": day,
            "winner_status": w_status,
            "winner_reason": str(winner.get("error_reason", "")),
            "winner_written_at": str(winner.get("written_at", "")),
            "timestamp_tie": tied,
            "captured_row_count": str(last_cap.get("row_count", "")),
            "captured_service_name": str(last_cap.get("service_name", "")),
            "captured_venue": str(last_cap.get("venue", "")),
        }
        for dim in _IDENTITY_DIMS:
            rec[f"winner_{dim}"] = "" if dim not in grp.columns or pd.isna(winner.get(dim)) else str(winner.get(dim))
        out.append(rec)
    return pd.DataFrame(out)


def _resolve_object(storage: object, data_type: str, league_id: str, day: str) -> tuple[str, str, pd.DataFrame] | None:
    """Return (blob_path, pipeline_mode, parsed_df) for the atom's on-disk object, else None.

    Probes every UAC ``candidate_parquet_paths`` candidate across both sports
    pipeline_modes; ``fetched_at_hour=*`` wildcards resolved by prefix listing
    (latest hour partition wins). Content = parquet parses AND >=1 row.
    """
    seen: set[str] = set()
    for pm in _PIPELINE_MODES:
        cands = candidate_parquet_paths(data_type, day, league_id, pipeline_mode=pm)
        resolved: list[str] = []
        for cand in cands:
            if cand in seen:
                continue
            seen.add(cand)
            if "fetched_at_hour=*" not in cand:
                resolved.append(cand)
                continue
            prefix, suffix = cand.split("fetched_at_hour=*", 1)
            hits = sorted(
                str(getattr(b, "name", b))
                for b in storage.list_blobs(BUCKET, prefix=prefix)  # pyright: ignore[reportAttributeAccessIssue]
                if str(getattr(b, "name", b)).endswith(suffix.lstrip("/"))
                and "fetched_at_hour=" in str(getattr(b, "name", b))
            )
            resolved.extend(reversed(hits))
        for path in resolved:
            try:
                if not storage.blob_exists(BUCKET, path):  # pyright: ignore[reportAttributeAccessIssue]
                    continue
                raw = storage.download_bytes(BUCKET, path)  # pyright: ignore[reportAttributeAccessIssue]
                df = pd.read_parquet(io.BytesIO(raw))
            except Exception as exc:
                logger.warning("probe %s: unreadable (%s) — trying next candidate", path, exc)
                continue
            if len(df) >= 1:
                return path, pm, df
            logger.warning("probe %s: exists but 0 rows — not capture evidence", path)
    return None


_FIXTURE_DATE_COLS = ("date", "fixture_date", "match_date", "kickoff", "kickoff_utc", "fixture_datetime")
_FIXTURE_LEAGUE_COLS = ("league_id", "canonical_league_id", "league")


def _fixtures_content_check(df: pd.DataFrame, league_id: str, day: str) -> tuple[str, str]:
    """Return (verdict, detail) for a FIXTURES parquet vs the truthset flip.

    verdict: 'genuine' (real fixture rows for this league/day — truthset gap),
    'mismatch' (rows exist but for a different league/day — flip stands),
    'ambiguous' (cannot attribute rows to league/day — decision packet).
    """
    n = len(df)
    if n == 0:
        return "mismatch", "0 rows"
    work = df
    lg_col = next((c for c in _FIXTURE_LEAGUE_COLS if c in work.columns), None)
    if lg_col is not None:
        lg_match = work[work[lg_col].astype(str).str.upper() == league_id.upper()]
        if lg_match.empty:
            return "mismatch", f"{n} rows but none for league {league_id} (col {lg_col})"
        work = lg_match
    dt_col = next((c for c in _FIXTURE_DATE_COLS if c in work.columns), None)
    if dt_col is not None:
        day_match = work[work[dt_col].astype(str).str.startswith(day)]
        if day_match.empty:
            days = sorted(set(work[dt_col].astype(str).str[:10]))[:5]
            return "mismatch", f"{len(work)} league rows but none dated {day} (col {dt_col}, days={days})"
        return "genuine", f"{len(day_match)} fixture rows for {league_id} on {day} (cols {lg_col}/{dt_col})"
    if lg_col is not None:
        # league confirmed; the day= partition is the only date scoping available
        return "genuine", f"{len(work)} rows for {league_id} under day={day} partition (no date col)"
    return "ambiguous", f"{n} rows, no league/date columns to attribute (cols={list(df.columns)[:12]})"


def run_scan(storage: object) -> pd.DataFrame:
    _snapshot_index(storage, "recency_masked_adjudication_scan")
    idx = _load_index(storage)
    atoms = _masked_atoms(idx)
    by_dt = atoms.groupby("data_type").size().to_dict() if not atoms.empty else {}
    logger.info(
        "masked atoms (winner != captured in a captured+empty group): %d total; by data_type=%s", len(atoms), by_dt
    )
    out = REPORT_DIR / "recency_masked_atoms_scan.csv"
    atoms.to_csv(out, index=False)
    logger.info("scan CSV -> %s", out)
    return atoms


def run_adjudicate(storage: object, atoms: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, a in atoms.iterrows():
        dt, lg, day = str(a["data_type"]), str(a["league_id"]), str(a["date"])
        if str(a["winner_status"]) != "empty_confirmed":
            rows.append({**a.to_dict(), "verdict": "report-only-non-empty-winner", "evidence": ""})
            continue
        hit = _resolve_object(storage, dt, lg, day)
        if hit is None:
            rows.append({**a.to_dict(), "verdict": "adjudicated-empty", "evidence": "no readable on-disk object"})
            continue
        path, pm, df = hit
        if dt == "FIXTURES":
            verdict, detail = _fixtures_content_check(df, lg, day)
            v = {"genuine": "restamp-truthset-gap", "mismatch": "flip-stands", "ambiguous": "decision-packet"}[verdict]
            rows.append(
                {
                    **a.to_dict(),
                    "verdict": v,
                    "evidence": f"{path} [{pm}]: {detail}",
                    "object_path": path,
                    "pipeline_mode": pm,
                    "object_rows": len(df),
                }
            )
        else:
            rows.append(
                {
                    **a.to_dict(),
                    "verdict": "restamp-captured",
                    "evidence": f"{path} [{pm}]: {len(df)} rows",
                    "object_path": path,
                    "pipeline_mode": pm,
                    "object_rows": len(df),
                }
            )
    rep = pd.DataFrame(rows)
    if not rep.empty:
        logger.info("adjudication verdicts: %s", rep.groupby(["data_type", "verdict"]).size().to_dict())
    out = REPORT_DIR / "recency_masked_atoms_adjudication.csv"
    rep.to_csv(out, index=False)
    logger.info("adjudication CSV -> %s", out)
    return rep


def run_apply(storage: object, rep: pd.DataFrame) -> int:
    _snapshot_index(storage, "recency_masked_adjudication_pre_apply")
    to_stamp = rep[rep["verdict"].isin(["restamp-captured", "restamp-truthset-gap"])]
    if to_stamp.empty:
        logger.info("nothing to re-stamp; no shard written")
        return 0
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    stamped = 0
    for _, a in to_stamp.iterrows():
        dt, lg, day = str(a["data_type"]), str(a["league_id"]), str(a["date"])
        path = str(a["object_path"])
        pm = str(a["pipeline_mode"])
        raw = storage.download_bytes(BUCKET, path)  # pyright: ignore[reportAttributeAccessIssue]
        df = pd.read_parquet(io.BytesIO(raw))
        manifest.record_captured(  # QG-allow: emission-policy-not-applicable
            row_key={"date": day, "data_type": dt, "league_id": lg},
            df=df,
            asset_group="sports",
            instrument_type="",
            data_type=dt,
            league_id=lg,
            pipeline_mode=PipelineMode(pm),
            source=_SOURCE_BY_MODE[pm],
            service_emission_state=None,
        )
        stamped += 1
        logger.info("STAMPED captured %s %s %s <- %s (%s rows)", dt, lg, day, path, a["object_rows"])
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
    n_captured_now = int((idx["capture_status"].astype(str) == "captured").sum())
    logger.info("index captured rows now: %d", n_captured_now)
    atoms_now = _masked_atoms(idx)
    key_now = set(
        zip(atoms_now.get("data_type", []), atoms_now.get("league_id", []), atoms_now.get("date", []), strict=True)
    )
    bad = 0
    for _, a in rep.iterrows():
        key = (str(a["data_type"]), str(a["league_id"]), str(a["date"]))
        want_captured = str(a["verdict"]) in ("restamp-captured", "restamp-truthset-gap")
        still_masked = key in key_now
        if want_captured and still_masked:
            logger.error("VERIFY %s: re-stamped but STILL masked", key)
            bad += 1
        elif want_captured:
            logger.info("VERIFY %s: captured wins ✓", key)
        elif str(a["verdict"]) == "adjudicated-empty" and not still_masked:
            logger.warning("VERIFY %s: adjudicated-empty group no longer flagged (rows collapsed?)", key)
    logger.info(
        "verify: %d/%d re-stamped atoms captured; %d remaining masked atoms overall",
        len(rep[rep["verdict"].str.startswith("restamp")]) - bad,
        len(rep[rep["verdict"].str.startswith("restamp")]),
        len(atoms_now),
    )
    return 0 if bad == 0 else 1


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
    adj_csv = REPORT_DIR / "recency_masked_atoms_adjudication.csv"
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
    atoms = run_scan(storage)
    if ARGS.adjudicate and not atoms.empty:
        run_adjudicate(storage, atoms)
    return 0


if __name__ == "__main__":
    sys.exit(main())
