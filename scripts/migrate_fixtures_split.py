#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + GCS orphan-sweep = 0 for migration targets
"""One-shot manifest migration: split ``entity=fixtures`` parquets into
``entity=fixtures_schedule`` + ``entity=fixtures_outcomes``.

The FIXTURES schema-split (lookahead-bias fix) gates the GCS entity-folder
split until the sports canonicalisation migration ships (single-walk discipline,
CLAUDE.md). This script is the write-side half that runs AFTER the writer
cutover + writegate strict-mode flip so there is no mid-migration window where
writegate rejects old entity=fixtures writes.

Source layout:
    sports_reference/by_date/day={date}/entity=fixtures/league={L}/fixtures.parquet
    sports_reference/by_date/day={date}/entity=fixtures/fixtures.parquet  (legacy bare)

Destination layout:
    sports_reference/by_date/day={date}/entity=fixtures_schedule/league={L}/fixtures_schedule.parquet
    sports_reference/by_date/day={date}/entity=fixtures_outcomes/league={L}/fixtures_outcomes.parquet

Column split (per UTL ``joined_reader.OUTCOME_COLUMNS`` + ``available_at`` stamp):

* **Schedule shard** — all non-outcome columns; ``available_at`` re-stamped to
  ``announced_at`` (the per-row floor below which a schedule column is not yet
  known). When ``announced_at`` is null, ``available_at`` is preserved from the
  source row (legacy).
* **Outcomes shard** — ``fixture_id`` + outcome columns + ``available_at``
  re-stamped to ``match_end_time`` (only known at match end). When
  ``match_end_time`` is null (unplayed / abandoned), ``available_at = NaT``.
  Consumers guard against NaT via ``outcomes_available_at`` (UTL
  ``read_fixtures_joined``).

Idempotency: if BOTH destination blobs already exist (non-zero size), the source
blob is skipped. A partial migration (only one destination exists) is treated as
a retry candidate and re-migrated.

CAS: destination blobs are uploaded with ``if_generation_match=0`` (create-only
— write fails instead of silently overwriting an already-migrated blob; retry
with ``--overwrite`` to force).

Reference plan:
    unified-trading-pm/plans/active/sports_fixtures_schema_split_completion_2026_06_20.md

Operator usage (run on same-region GCE VM in asia-northeast1-c):

    cd instruments-service
    .venv/bin/python scripts/migrate_fixtures_split.py \\
        --bucket instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 16 \\
        --dry-run

    # Review the dry-run report, then apply:
    .venv/bin/python scripts/migrate_fixtures_split.py \\
        --bucket instruments-store-sports-${PROJECT_ID} \\
        --prefix sports_reference/by_date/ \\
        --workers 16

Pre-flight (operator responsibility — this script does NOT enforce it):
    1. Confirm the writer cutover (entity=fixtures_schedule + entity=fixtures_outcomes
       emitting) is live and writegate strict-mode flip has shipped.
    2. Pause sports backfill VMs writing entity=fixtures to avoid mid-migration
       interference (forward-poll VMs already emit the new entities after cutover).
    3. Run this script in dry-run mode; inspect summary.
    4. Run without --dry-run.
    5. Resume backfill VMs.
    6. Confirm manifest consolidator reflects split-entity row counts.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage
from requests.adapters import HTTPAdapter
from unified_api_contracts.canonical.crosscutting.availability_semantics import (
    FIXTURE_ANNOUNCEMENT_FLOOR_DAYS_DEFAULT,
    get_fixture_announcement_floor_days,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIXTURES_ENTITY = "fixtures"
SCHEDULE_ENTITY = "fixtures_schedule"
OUTCOMES_ENTITY = "fixtures_outcomes"

# Outcome columns (per UTL joined_reader.OUTCOME_COLUMNS + match_end_time guard).
# These + fixture_id go into the outcomes shard; all other columns go to schedule.
_OUTCOME_COLS: frozenset[str] = frozenset(
    {
        "home_score",
        "away_score",
        "home_score_fulltime",
        "away_score_fulltime",
        "home_score_extratime",
        "away_score_extratime",
        "home_score_penalty",
        "away_score_penalty",
        "match_end_time",
        "af_winner_id",
        # Additional score-distinction columns added by extract_match_lifecycle (Q6).
        "home_score_regulation",
        "away_score_regulation",
        "home_score_after_extra_time",
        "away_score_after_extra_time",
        "home_score_after_penalty_shootout",
        "away_score_after_penalty_shootout",
        "home_penalty_shootout_score",
        "away_penalty_shootout_score",
        "went_to_extra_time",
        "went_to_penalties",
        "match_result",
    }
)

# Shared columns that appear in BOTH shards (identity + availability marker).
_SHARED_COLS: frozenset[str] = frozenset({"fixture_id", "available_at"})

# ---------------------------------------------------------------------------
# Cross-source backfill helpers
# ---------------------------------------------------------------------------

# Thread-safe per-(date, league) cache for footystats announced-at maps so
# parallel workers for the same date never re-read the same GCS blobs.
_FT_CACHE: dict[str, dict[str, datetime]] = {}
_FT_CACHE_LOCK = threading.Lock()


def _parse_date_league_from_path(blob_name: str) -> tuple[str, str]:
    """Extract ``(date, league)`` from a fixtures blob path.

    Example::

        'sports_reference/by_date/day=2025-09-14/entity=fixtures/league=EPL/fixtures.parquet'
        → ('2025-09-14', 'EPL')

    Returns ``('', '')`` when either component is absent (legacy bare path).
    """
    date_part = ""
    league_part = ""
    for segment in blob_name.split("/"):
        if segment.startswith("day="):
            date_part = segment[4:]
        elif segment.startswith("league="):
            league_part = segment[7:]
    return date_part, league_part


def _load_footystats_map(
    bucket: storage.Bucket,
    date: str,
    league: str,
) -> dict[str, datetime]:
    """Return a ``{canonical_fixture_id: min(available_at)}`` map for ``date``/``league``.

    Reads all ``fetched_at_hour=*`` footystats_predictions partitions for the
    date/league pair and returns the EARLIEST ``available_at`` per fixture
    (≈ kickoff − 72 h for live-period data).  Returns an empty dict when no
    footystats data exists for the cell.

    Thread-safe: results are cached per ``"date:league"`` key.
    """
    cache_key = f"{date}:{league}"
    with _FT_CACHE_LOCK:
        if cache_key in _FT_CACHE:
            return _FT_CACHE[cache_key]

    prefix = f"sports_reference/by_date/day={date}/entity=footystats_predictions/"
    target_suffix = f"league={league}/footystats_predictions.parquet"
    result: dict[str, datetime] = {}
    try:
        for blob in bucket.list_blobs(prefix=prefix):
            if not blob.name.endswith(target_suffix):
                continue
            try:
                raw = blob.download_as_bytes()
                ft_table = pq.read_table(io.BytesIO(raw))
                if "canonical_fixture_id" not in ft_table.column_names:
                    continue
                if "available_at" not in ft_table.column_names:
                    continue
                for row in ft_table.to_pylist():
                    fid: str | None = row.get("canonical_fixture_id")
                    avail: datetime | None = row.get("available_at")
                    if not fid or avail is None:
                        continue
                    if hasattr(avail, "tzinfo") and avail.tzinfo is None:
                        avail = avail.replace(tzinfo=UTC)
                    existing = result.get(fid)
                    if existing is None or avail < existing:
                        result[fid] = avail
            except Exception:
                pass  # per-blob isolation: skip unreadable footystats blobs
    except Exception:
        pass  # list failure: return empty map; migration continues with UAC floor

    with _FT_CACHE_LOCK:
        _FT_CACHE[cache_key] = result
    return result


def _fill_null_announced_at(
    table: pa.Table,
    footystats_map: dict[str, datetime] | None,
) -> pa.Table:
    """Back-fill null ``announced_at`` cells using a two-source priority stack.

    For each row where ``announced_at`` is null the function applies (in order):

    1. **footystats cross-source**: look up ``fixture_id`` in ``footystats_map``
       (``available_at`` ≈ kickoff − 72 h for live-period data).
    2. **UAC per-league floor**: ``kickoff_utc − floor_days`` using
       :func:`get_fixture_announcement_floor_days` keyed by ``af_league_id``
       (21–28 d for Tier-1 EU leagues, 14 d default for unobserved leagues).

    When both sources are available the function takes ``min()`` so the
    EARLIEST observed time is stamped (tightest known PIT bound).

    SFI progressive-stats are post-match data with no fixture-schedule
    publication-time semantics; SFI is intentionally excluded here.

    Rows whose ``timestamp`` (kickoff UTC) is absent/null are left unchanged
    — a junk stamp is worse than null.
    """
    if "announced_at" not in table.column_names:
        return table

    announced_col = table.column("announced_at")
    if not pc.any(pc.is_null(announced_col)).as_py():
        return table  # nothing to fill

    col_names = set(table.column_names)
    has_fixture_id = "fixture_id" in col_names
    has_timestamp = "timestamp" in col_names
    has_af_league_id = "af_league_id" in col_names

    if not has_timestamp:
        return table  # can't compute fallback without kickoff

    announced_list: list[datetime | None] = announced_col.to_pylist()
    timestamp_list: list[str | datetime | None] = table.column("timestamp").to_pylist()
    fixture_ids: list[str | None] = (
        table.column("fixture_id").to_pylist() if has_fixture_id else [None] * len(table)
    )
    af_league_ids: list[int | float | None] = (
        table.column("af_league_id").to_pylist() if has_af_league_id else [None] * len(table)
    )

    for i, val in enumerate(announced_list):
        if val is not None:
            continue  # already populated — leave untouched

        raw_kickoff = timestamp_list[i]
        if raw_kickoff is None:
            continue  # no kickoff — can't stamp

        # Normalise to a timezone-aware UTC datetime.
        if isinstance(raw_kickoff, str):
            try:
                kickoff = datetime.fromisoformat(raw_kickoff)
                if kickoff.tzinfo is None:
                    kickoff = kickoff.replace(tzinfo=UTC)
            except ValueError:
                continue
        else:
            kickoff = raw_kickoff
            if hasattr(kickoff, "tzinfo") and kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=UTC)

        # UAC per-league floor: primary anchor.
        raw_lid = af_league_ids[i]
        if raw_lid is not None:
            try:
                floor_days = get_fixture_announcement_floor_days(int(raw_lid))
            except (TypeError, ValueError):
                floor_days = FIXTURE_ANNOUNCEMENT_FLOOR_DAYS_DEFAULT
        else:
            floor_days = FIXTURE_ANNOUNCEMENT_FLOOR_DAYS_DEFAULT

        uac_floor: datetime = kickoff - timedelta(days=floor_days)

        # footystats cross-source: if found take min with UAC floor.
        if footystats_map:
            fid = fixture_ids[i]
            if fid and fid in footystats_map:
                ft_avail = footystats_map[fid]
                announced_list[i] = min(uac_floor, ft_avail)
                continue

        announced_list[i] = uac_floor

    new_announced = pa.array(announced_list, type=pa.timestamp("us", tz="UTC"))
    col_idx = table.column_names.index("announced_at")
    return table.set_column(col_idx, "announced_at", new_announced)


@dataclass(frozen=True)
class MigrationResult:
    """Outcome of a single entity=fixtures blob migration."""

    blob_name: str
    case: str  # "split" / "skip_both_exist" / "skip_partial_rerun" / "dry_run" / "cas_failed" / "error"
    schedule_rows: int
    outcomes_rows: int
    error: str | None


def _make_storage_client(project_id: str, workers: int) -> storage.Client:
    client = storage.Client(project=project_id)
    pool_size = max(64, workers * 2)
    try:
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=3,
        )
        client._http.mount("https://", adapter)
        client._http.mount("http://", adapter)
    except (AttributeError, TypeError):
        pass
    return client


def _list_fixtures_blobs(bucket: storage.Bucket, prefix: str) -> list[str]:
    logger.info("Listing entity=fixtures blobs under gs://%s/%s …", bucket.name, prefix)
    t0 = time.time()
    names: list[str] = []
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        # Only entity=fixtures/ paths (not fixtures_schedule / fixtures_outcomes).
        if f"/entity={FIXTURES_ENTITY}/" in name and f"/entity={FIXTURES_ENTITY}_" not in name:
            names.append(name)
    elapsed = time.time() - t0
    logger.info(
        "Found %d entity=fixtures blobs (%.1f sec)",
        len(names),
        elapsed,
    )
    return names


def _destination_paths(source: str) -> tuple[str, str]:
    """Derive the schedule + outcomes destination blob names from the source."""
    sched = source.replace(
        f"/entity={FIXTURES_ENTITY}/",
        f"/entity={SCHEDULE_ENTITY}/",
    ).replace(
        f"/{FIXTURES_ENTITY}.parquet",
        f"/{SCHEDULE_ENTITY}.parquet",
    )
    out = source.replace(
        f"/entity={FIXTURES_ENTITY}/",
        f"/entity={OUTCOMES_ENTITY}/",
    ).replace(
        f"/{FIXTURES_ENTITY}.parquet",
        f"/{OUTCOMES_ENTITY}.parquet",
    )
    return sched, out


def _blob_exists(bucket: storage.Bucket, name: str) -> bool:
    return bucket.blob(name).exists()


def _split_table(table: pa.Table) -> tuple[pa.Table, pa.Table]:
    """Split one entity=fixtures pyarrow Table into (schedule, outcomes).

    Schedule = all non-outcome columns, with ``available_at`` overridden to
    ``announced_at`` where present. Outcomes = fixture_id + outcome columns +
    ``available_at`` overridden to ``match_end_time``.
    """
    col_names = set(table.column_names)

    # --- Schedule shard ---
    sched_cols = [c for c in table.column_names if c not in (_OUTCOME_COLS - _SHARED_COLS)]
    sched_table = table.select(sched_cols)
    # Override available_at with announced_at when the column exists.
    if "announced_at" in col_names and "available_at" in sched_table.column_names:
        announced_at = table.column("announced_at")
        sched_table = sched_table.set_column(
            sched_table.column_names.index("available_at"),
            "available_at",
            announced_at,
        )

    # --- Outcomes shard ---
    out_cols = (
        [c for c in table.column_names if c in (_OUTCOME_COLS | _SHARED_COLS) and c in col_names]
    )
    if not out_cols:
        # No outcome columns in this parquet (pre-Q6 data) — emit a stub with
        # fixture_id + available_at = NaT so the entity path exists.
        fixture_id_col = table.column("fixture_id") if "fixture_id" in col_names else pa.array([""] * len(table))
        nat_ts = pa.array(
            [None] * len(table),
            type=pa.timestamp("us", tz="UTC"),
        )
        out_table = pa.table({"fixture_id": fixture_id_col, "available_at": nat_ts})
    else:
        out_table = table.select(out_cols)
        # Override available_at with match_end_time where it exists.
        if "match_end_time" in col_names and "available_at" in out_table.column_names:
            match_end = table.column("match_end_time")
            out_table = out_table.set_column(
                out_table.column_names.index("available_at"),
                "available_at",
                match_end,
            )

    return sched_table, out_table


def _upload_table(
    bucket: storage.Bucket,
    dest_name: str,
    table: pa.Table,
    *,
    overwrite: bool,
) -> str:
    """Upload a pyarrow Table to GCS. Returns "uploaded" | "cas_failed" | "error:<msg>"."""
    out = io.BytesIO()
    pq.write_table(table, out, coerce_timestamps="us", allow_truncated_timestamps=True)
    blob = bucket.blob(dest_name)
    try:
        generation_match = None if overwrite else 0
        blob.upload_from_string(
            out.getvalue(),
            content_type="application/octet-stream",
            if_generation_match=generation_match,
        )
        return "uploaded"
    except PreconditionFailed:
        return "cas_failed"
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return f"error:{exc}"


def _migrate_one(
    bucket: storage.Bucket,
    blob_name: str,
    *,
    dry_run: bool,
    overwrite: bool,
    cross_source: bool = False,
) -> MigrationResult:
    sched_name, out_name = _destination_paths(blob_name)

    # Idempotency check — skip only when BOTH destinations exist.
    if not dry_run and not overwrite:
        sched_exists = _blob_exists(bucket, sched_name)
        out_exists = _blob_exists(bucket, out_name)
        if sched_exists and out_exists:
            return MigrationResult(
                blob_name=blob_name,
                case="skip_both_exist",
                schedule_rows=0,
                outcomes_rows=0,
                error=None,
            )

    try:
        raw = bucket.blob(blob_name).download_as_bytes()
        table = pq.read_table(io.BytesIO(raw))
    except Exception as exc:  # broad-except-ok: per-blob isolation
        return MigrationResult(
            blob_name=blob_name,
            case="error",
            schedule_rows=0,
            outcomes_rows=0,
            error=f"read: {exc}",
        )

    # Phase-3 cross-source backfill: fill null announced_at using footystats
    # data for this date/league + UAC per-league announcement floor.
    if cross_source:
        date, league = _parse_date_league_from_path(blob_name)
        ft_map = _load_footystats_map(bucket, date, league) if date and league else {}
        table = _fill_null_announced_at(table, ft_map)

    sched_table, out_table = _split_table(table)

    if dry_run:
        return MigrationResult(
            blob_name=blob_name,
            case="dry_run",
            schedule_rows=sched_table.num_rows,
            outcomes_rows=out_table.num_rows,
            error=f"dry-run → {sched_name} / {out_name}",
        )

    sched_status = _upload_table(bucket, sched_name, sched_table, overwrite=overwrite)
    out_status = _upload_table(bucket, out_name, out_table, overwrite=overwrite)

    if "cas_failed" in (sched_status, out_status):
        return MigrationResult(
            blob_name=blob_name,
            case="cas_failed",
            schedule_rows=sched_table.num_rows,
            outcomes_rows=out_table.num_rows,
            error=f"sched={sched_status} out={out_status} — rerun or use --overwrite",
        )
    if sched_status.startswith("error") or out_status.startswith("error"):
        return MigrationResult(
            blob_name=blob_name,
            case="error",
            schedule_rows=sched_table.num_rows,
            outcomes_rows=out_table.num_rows,
            error=f"sched={sched_status} out={out_status}",
        )

    return MigrationResult(
        blob_name=blob_name,
        case="split",
        schedule_rows=sched_table.num_rows,
        outcomes_rows=out_table.num_rows,
        error=None,
    )


def _run_migration(
    bucket: storage.Bucket,
    blob_names: list[str],
    *,
    dry_run: bool,
    overwrite: bool,
    workers: int,
    cross_source: bool = False,
) -> list[MigrationResult]:
    results: list[MigrationResult] = []
    completed = 0
    total = len(blob_names)
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [
            ex.submit(
                _migrate_one, bucket, n,
                dry_run=dry_run, overwrite=overwrite, cross_source=cross_source,
            )
            for n in blob_names
        ]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            completed += 1
            if completed % 100 == 0 or completed == total:
                rate = completed / max(0.01, time.time() - t0)
                logger.info(
                    "  %d/%d (%.1f/sec) — last: %s [%s]",
                    completed,
                    total,
                    rate,
                    res.blob_name.rsplit("/", 1)[-1],
                    res.case,
                )
    return results


def _summarise(results: list[MigrationResult]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for r in results:
        summary[r.case] = summary.get(r.case, 0) + 1
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="GCS bucket URI (gs://...) or bare name.")
    parser.add_argument(
        "--prefix",
        default="sports_reference/by_date/",
        help="GCS prefix to enumerate (default: sports_reference/by_date/).",
    )
    parser.add_argument("--project-id", required=True, help="GCP project ID.")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers.")
    parser.add_argument("--dry-run", action="store_true", help="Plan only, no uploads.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing destination blobs (default: CAS create-only).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N blobs (0 = all). For spot-check / smoke run.",
    )
    parser.add_argument(
        "--cross-source-backfill",
        action="store_true",
        help=(
            "Phase-3 optional: fill null announced_at using footystats GCS data for the "
            "same date/league + UAC per-league announcement floor. Takes the earliest of "
            "the two sources (tightest known PIT bound). No-op when announced_at is already "
            "populated. Off by default — enable for historical parquets written before the "
            "announced_at field was added."
        ),
    )
    args = parser.parse_args(argv)

    bucket_uri: str = args.bucket
    bucket_name = bucket_uri.removeprefix("gs://").split("/", 1)[0]

    started = datetime.now(UTC)
    logger.info(
        "fixtures-split migration start: bucket=%s prefix=%s workers=%d "
        "dry_run=%s overwrite=%s cross_source=%s",
        bucket_name,
        args.prefix,
        args.workers,
        args.dry_run,
        args.overwrite,
        args.cross_source_backfill,
    )

    client = _make_storage_client(args.project_id, args.workers)
    bucket = client.bucket(bucket_name)

    blob_names = _list_fixtures_blobs(bucket, args.prefix)
    if args.limit > 0:
        logger.info("Limiting to first %d blobs (per --limit)", args.limit)
        blob_names = blob_names[: args.limit]

    if not blob_names:
        logger.info("No entity=fixtures blobs found; nothing to do.")
        return 0

    results = _run_migration(
        bucket,
        blob_names,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        workers=args.workers,
        cross_source=args.cross_source_backfill,
    )

    summary = _summarise(results)
    logger.info("--- Summary ---")
    logger.info("  split (schedule + outcomes written):  %d", summary.get("split", 0))
    logger.info("  skip_both_exist (idempotent skip):    %d", summary.get("skip_both_exist", 0))
    logger.info("  dry_run (no upload):                  %d", summary.get("dry_run", 0))
    logger.info("  cas_failed (rerun needed):            %d", summary.get("cas_failed", 0))
    logger.info("  error (hard failure):                 %d", summary.get("error", 0))
    elapsed = (datetime.now(UTC) - started).total_seconds()
    logger.info(
        "Migration end: %d total, %.1f sec, dry_run=%s",
        len(results),
        elapsed,
        args.dry_run,
    )

    errors = [r for r in results if r.case in ("error", "cas_failed")]
    if errors:
        logger.warning("--- Failures (first 20) ---")
        for r in errors[:20]:
            logger.warning("  %s [%s]: %s", r.blob_name, r.case, r.error)

    return 1 if any(r.case == "error" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
