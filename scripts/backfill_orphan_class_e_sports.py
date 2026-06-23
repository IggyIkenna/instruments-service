#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + manifest captured count climbed + GCS orphan-sweep = 0
"""backfill_orphan_class_e_sports.py — sports class-E orphan → record_captured (R8).

The SPORTS recorder companion to ``migration_orphan_sweep_sports.py`` (the analogue
of ``backfill_orphan_class_e.py``, which owns the four hive AGs — sports gets its
OWN recorder because its grain is league-keyed, not hive (venue, chain,
instrument_type): a sports cell is ``(date, data_type, league_id)``, paths are
already at ``candidate_parquet_paths`` SSOT shapes (NO object conversion / move
needed — never-manifest-non-canonical is satisfied by construction), and the
manifest surface conventions come from ``backfill_sports_per_entity_manifest.py``
(venue='' + ``_SOURCE_TO_PIPELINE_MODE`` — source='footystats' → BATCH_FOOTYSTATS for ODDS).

Drives every report-E object to a manifested state — **PRESERVE + backfill, never
delete** (operator ratification R1/R8: backfill ``--apply`` is sanctioned; the only
HARD-STOP is the G4 migrator ``--apply``):

1. **RE-VERIFY** — every report-E row is re-checked against the LIVE consolidated
   index with the sweep's grain-aware matcher (rows covered since the report was
   written are dropped, never re-recorded).
2. **CHARACTERIZE** — group still-orphan objects into cells:
   * by_date / flat entity objects → ``(day, data_type, league_id)``;
   * ``instrument_definitions`` objects (the bare ``day=/venue=`` tree) → the
     IS definitions availability surface ``availability_index/
     instruments-service.parquet`` keyed ``(date, venue)`` — appended directly
     (that corpus is NOT ``_index``-manifested; same surface its writer uses).
3. **FOOTER-VERIFY** — per cell, footer-read EVERY member (rows>0 proves real
   data; zero-row members are excluded from row_count; an all-zero cell is
   skipped — junk, not a backfill target).
4. **RECORD** — one ``ManifestWriter.add(processing_date, row_count, data_type,
   league_id, venue='', source, pipeline_mode)`` per cell into PER-VM shards.

After ``--apply``: run the ONE-SHOT ``manifest_consolidator.consolidate(bucket,
force=True)`` (this script does it with ``--consolidate``), then re-run the sports
sweep to the ``E==0`` acceptance.

Usage::

    cd instruments-service
    .venv/bin/python scripts/backfill_orphan_class_e_sports.py \\
        --report-uri gs://<ref-bucket>/_index/audit/orphan_sweep_sports.parquet   # dry-run
    .venv/bin/python scripts/backfill_orphan_class_e_sports.py \\
        --report-uri gs://.../orphan_sweep_sports.parquet --apply --consolidate

Plan refs: ``migration_verification_orphan_safety_2026_06_10.md`` § V2 (R8) +
``master_data_canonicalisation_migration_catalogue_2026_06_07.md`` R8 todo.
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from unified_api_contracts import PipelineMode
from unified_api_contracts.sports import SPORTS_DATA_TYPE_TO_SOURCE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MOD_CACHE: dict[str, ModuleType] = {}


def _load_sibling(stem: str) -> ModuleType:
    """importlib loader for sibling scripts (``scripts/`` is not a package — the
    same single-source pattern as ``backfill_orphan_class_e._load_sweep_module``)."""
    if stem not in _MOD_CACHE:
        spec = importlib.util.spec_from_file_location(stem, Path(__file__).parent / f"{stem}.py")
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load sibling {stem}.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules.setdefault(stem, mod)
        spec.loader.exec_module(mod)
        _MOD_CACHE[stem] = mod
    return _MOD_CACHE[stem]


# Source → batch PipelineMode (mirrors backfill_sports_per_entity_manifest.py —
# operator-ratified conventions; source='footystats' → BATCH_FOOTYSTATS for ODDS).
_SOURCE_TO_PIPELINE_MODE: dict[str, PipelineMode] = {
    "api_football": PipelineMode.BATCH_API_FOOTBALL,
    "footystats": PipelineMode.BATCH_FOOTYSTATS,
    "understat": PipelineMode.BATCH_UNDERSTAT,
    "open_meteo": PipelineMode.BATCH_OPEN_METEO,
    "transfermarkt": PipelineMode.BATCH_TRANSFERMARKT,
    "soccer_football_info": PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
}


def resolve_source_and_mode(data_type: str, fallback_source: str) -> tuple[str, PipelineMode]:
    """Resolve (source, pipeline_mode) for a sports cell.

    The ENFORCEMENT truth is UAC ``SOURCE_PRIORITY`` (the ManifestWriter raises
    ``MissingSourceError`` on a source outside it — first apply 2026-06-11:
    ``SPORTS_DATA_TYPE_TO_SOURCE`` says footystats for STANDINGS/TEAMS while
    SOURCE_PRIORITY allows only api_football → 858 cell errors), and for batch rows
    the writer requires ``source == source_string_for(pipeline_mode)``
    (``PipelineModeSourceMismatchError`` — so the per-entity precedent's
    ODDS→BATCH_ODDS_API was wrong for footystats ODDS (fixed 2026-06-22 in
    __init__.py); the mode follows the resolved source. Resolution: keep the entity-map source when
    SOURCE_PRIORITY allows it, else take the SOURCE_PRIORITY primary; data_types
    with no SOURCE_PRIORITY entry (e.g. PLAYER_STATS) keep the entity-map source.
    """
    from unified_api_contracts import get_source_priority

    try:
        allowed = get_source_priority("sports", data_type)
    except KeyError:
        allowed = []
    source = fallback_source if (fallback_source in allowed or not allowed) else allowed[0]
    return source, _SOURCE_TO_PIPELINE_MODE.get(source, PipelineMode.BATCH_INSTRUMENTS_SERVICE)


@dataclass
class CellPlan:
    """One ``(day, data_type, league_id)`` cell to record, with its member objects."""

    day: str
    data_type: str
    league_id: str
    source: str
    members: list[tuple[str, int]]  # (object_path, size_bytes)
    row_count: int = 0


def load_report_e(client: object, report_uri: str) -> list[dict[str, str]]:
    import pandas as pd

    assert report_uri.startswith("gs://")
    bucket, _sep, blob_path = report_uri[len("gs://") :].partition("/")
    raw = client.download_bytes(bucket, blob_path)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    return df[df["obj_class"] == "E_orphan_real"].to_dict("records")  # type: ignore[return-value]


def reverify(sweep: ModuleType, index: object, rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    """Drop rows the LIVE index already covers (grain-aware sweep matcher)."""
    still: list[dict[str, str]] = []
    covered = 0
    for r in rows:
        if str(r.get("entity", "")) == "instrument_definitions":
            if (str(r.get("day", "")), sweep._norm(str(r.get("venue", "")))) in index.definition_days:  # type: ignore[attr-defined]
                covered += 1
            else:
                still.append(r)
            continue
        ok = sweep.is_covered_sports(
            index,
            day=str(r.get("day", "")),
            data_type=str(r.get("data_type", "")),
            league=str(r.get("league_id", "")),
            venue=str(r.get("venue", "")),
            timeframe=str(r.get("timeframe", "")),
        )
        if ok:
            covered += 1
        else:
            still.append(r)
    return still, covered


def build_cells(bucket: str, still: list[dict[str, str]]) -> tuple[list[CellPlan], list[dict[str, str]]]:
    """Group still-orphan entity rows into (day, data_type, league) cells; split off
    the instrument_definitions rows (different availability surface)."""
    prefix = f"gs://{bucket}/"
    cells: dict[tuple[str, str, str], CellPlan] = {}
    definition_rows: list[dict[str, str]] = []
    for r in still:
        if str(r.get("entity", "")) == "instrument_definitions":
            definition_rows.append(r)
            continue
        day, dt = str(r.get("day", "")), str(r.get("data_type", ""))
        league = str(r.get("league_id", ""))
        key = (day, dt, league)
        if key not in cells:
            cells[key] = CellPlan(
                day=day, data_type=dt, league_id=league, source=SPORTS_DATA_TYPE_TO_SOURCE.get(dt, ""), members=[]
            )
        uri = str(r["uri"])
        path = uri[len(prefix) :] if uri.startswith(prefix) else uri
        cells[key].members.append((path, int(r.get("size_bytes", 0) or 0)))
    return sorted(cells.values(), key=lambda c: (c.day, c.data_type, c.league_id)), definition_rows


def footer_verify(sweep: ModuleType, client: object, bucket: str, cells: list[CellPlan], *, workers: int) -> None:
    """Footer-read every member in parallel; cell.row_count = sum of member rows
    (zero-row members contribute 0; an all-zero cell is skipped at record time)."""
    from concurrent.futures import ThreadPoolExecutor

    hive = _load_sibling("migration_orphan_sweep")
    flat = [(cell, path) for cell in cells for path, _size in cell.members]
    logger.info("footer-verifying %d member objects across %d cells (%d workers)", len(flat), len(cells), workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        rows_list = list(pool.map(lambda t: hive._footer_row_count(client, bucket, t[1]) or 0, flat))
    for (cell, _path), rows in zip(flat, rows_list, strict=True):
        cell.row_count += int(rows)


def record_cells(bucket: str, cells: list[CellPlan], *, apply: bool) -> tuple[int, int, list[str]]:
    """One ``ManifestWriter.add`` per rows>0 cell (per-VM shards). Returns
    (recorded, skipped_zero_row, errors)."""
    from datetime import date as date_type

    from unified_trading_library import ManifestWriter

    recordable = [c for c in cells if c.row_count > 0]
    zero = [c for c in cells if c.row_count == 0]
    if not apply:
        logger.info("[dry-run] would record %d cells (%d all-zero-row cells skipped)", len(recordable), len(zero))
        return 0, len(zero), []
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        per_vm_shards=True,
        batch_size=200,
    )
    errors: list[str] = []
    recorded = 0
    for cell in recordable:
        source, pipeline_mode = resolve_source_and_mode(cell.data_type, cell.source)
        try:
            writer.add(
                asset_group="sports",
                processing_date=date_type.fromisoformat(cell.day),
                row_count=cell.row_count,
                data_type=cell.data_type,
                league_id=cell.league_id,
                venue="",
                source=source,
                pipeline_mode=pipeline_mode,
            )
            recorded += 1
        except Exception as exc:  # per-cell isolation
            errors.append(f"cell {cell.day}/{cell.data_type}/{cell.league_id}: {type(exc).__name__}: {exc}")
        if recorded and recorded % 2000 == 0:
            logger.info("  recorded %d cells", recorded)
    writer.close()
    return recorded, len(zero), errors


def record_definitions(client: object, bucket: str, rows: list[dict[str, str]], *, apply: bool) -> int:
    """Append ``(date, venue, service_name, row_count)`` rows to the IS definitions
    availability surface ``availability_index/instruments-service.parquet`` (footer
    row counts; the same surface + grain its writer maintains)."""
    import pandas as pd

    if not rows:
        return 0
    hive = _load_sibling("migration_orphan_sweep")
    prefix = f"gs://{bucket}/"
    by_cell: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        path = str(r["uri"])[len(prefix) :]
        n = hive._footer_row_count(client, bucket, path) or 0
        if n > 0:
            by_cell[(str(r.get("day", "")), str(r.get("venue", "")))] += int(n)
    if not by_cell:
        return 0
    if not apply:
        logger.info("[dry-run] would append %d definitions availability rows: %s", len(by_cell), dict(by_cell))
        return 0
    defs_path = "availability_index/instruments-service.parquet"
    raw = client.download_bytes(bucket, defs_path)  # type: ignore[attr-defined]
    df = pd.read_parquet(io.BytesIO(raw))
    add = pd.DataFrame(
        [
            {"date": day, "venue": venue, "service_name": "instruments-service", "row_count": count}
            for (day, venue), count in sorted(by_cell.items())
        ]
    )
    merged = pd.concat([df, add], ignore_index=True).drop_duplicates(subset=["date", "venue"], keep="last")
    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    buf.seek(0)
    client.upload_from_file_obj(bucket, defs_path, buf)  # type: ignore[attr-defined]
    logger.info("appended %d definitions availability rows (%d → %d)", len(add), len(df), len(merged))
    return len(add)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sports class-E orphan record_captured backfill (R8).")
    parser.add_argument("--bucket", choices=["odds", "reference"], default="reference")
    parser.add_argument(
        "--report-uri", type=str, default="", help="gs:// orphan_sweep_sports parquet (default: bucket's)"
    )
    parser.add_argument("--apply", action="store_true", help="write manifest rows (default dry-run)")
    parser.add_argument("--consolidate", action="store_true", help="one-shot consolidate(force=True) after --apply")
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args(argv)

    from unified_trading_library import GcsEventSink, UnifiedCloudConfig, get_storage_client, setup_events

    project_id = UnifiedCloudConfig().gcp_project_id
    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(project_id=project_id, bucket=f"{project_id}-events", service_name="instruments-service"),
    )
    sweep = _load_sibling("migration_orphan_sweep_sports")
    client = get_storage_client()
    bucket = sweep._resolve_sports_bucket(args.bucket)
    report_uri = args.report_uri or f"gs://{bucket}/_index/audit/orphan_sweep_sports.parquet"

    e_rows = load_report_e(client, report_uri)
    logger.info("report %s: %d class-E rows", report_uri, len(e_rows))
    index = sweep._load_covered_index(client, bucket, with_definitions=(args.bucket == "reference"))
    still, covered = reverify(sweep, index, e_rows)
    logger.info("re-verify vs LIVE index: %d already covered, %d still orphan", covered, len(still))
    cells, definition_rows = build_cells(bucket, still)
    logger.info(
        "characterized: %d (day, data_type, league) cells + %d instrument-definitions objects",
        len(cells),
        len(definition_rows),
    )
    per_dt: dict[str, int] = defaultdict(int)
    for c in cells:
        per_dt[c.data_type] += 1
    logger.info("cells per data_type: %s", dict(sorted(per_dt.items())))
    footer_verify(sweep, client, bucket, cells, workers=args.workers)
    recorded, zero_cells, errors = record_cells(bucket, cells, apply=args.apply)
    n_defs = record_definitions(client, bucket, definition_rows, apply=args.apply)
    for e in errors[:20]:
        logger.error("  %s", e)
    logger.info(
        "=== backfill %s: recorded=%d cells, zero-row-skipped=%d, definitions=%d, errors=%d ===",
        "APPLIED" if args.apply else "DRY-RUN",
        recorded,
        zero_cells,
        n_defs,
        len(errors),
    )
    if args.apply and args.consolidate:
        from unified_trading_library import manifest_consolidator as mc

        report = mc.consolidate(bucket, force=True)
        logger.info("consolidation: success=%s rows_out=%d", report.success, report.rows_out)
        if not report.success:
            return 1
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
