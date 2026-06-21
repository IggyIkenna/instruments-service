"""Sports reference blob layout, per-league fixtures writes/merges, team + fixture mappings.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_build_fixture_league_map_from_gcs",
    "_merge_with_existing_per_league_parquet",
    "_per_league_fixtures_data_unchanged",
    "_read_existing_per_league_fixture_ids",
    "_read_fixture_ids_from_gcs",
    "_resolve_sports_ref_blob",
    "_sports_ref_canonical_blob_path",
    "_sports_ref_legacy_blob_path",
    "_sports_ref_pm",
    "_sports_ref_sink_for",
    "_sports_ref_source",
    "_write_fixture_mapping",
    "_write_fixtures_per_league",
    "_write_team_mapping",
]


def _sports_ref_pm(entity_name: str) -> str:
    """Return the pipeline_mode value string for a sports_reference entity name.

    Delegates to the UAC SSOT ``pipeline_mode_for_sports_entity``.
    Falls back to ``BATCH_INSTRUMENTS_SERVICE`` for unknown entities.
    """
    return _orch.pipeline_mode_for_sports_entity(entity_name).value


# Sports entities whose canonical manifest SOURCE legitimately differs from the
# pipeline_mode PATH key (``batch_<pathkey>``). The path key partitions GCS objects;
# the source must match the UAC ``SOURCE_PRIORITY``/``SPORTS_DATA_TYPE_TO_SOURCE`` SSOT
# accepted by ``record_captured`` (a mismatch raises ``MissingSourceError`` fail_fast).
#
# ``footystats_odds``: the odds snapshot is FETCHED by the FootyStats adapter
# (``_fetch_footystats_odds``), so the upstream vendor/source is ``footystats``
# (UAC ``SPORTS_DATA_TYPE_TO_SOURCE[ODDS] == "footystats"``), even though its GCS
# pipeline_mode path key is ``batch_odds_api``. Stamping the path-key-derived
# ``odds_api`` here previously made the ``record_captured`` for data_type ``ODDS``
# fail (``source='odds_api' … not a registered source … Allowed: ['footystats']``).
_SPORTS_REF_SOURCE_OVERRIDE: dict[str, str] = {
    "footystats_odds": "footystats",
}


def _sports_ref_source(entity_name: str) -> str:
    """Return the lower-case manifest source key for a sports_reference entity name.

    Defaults to the ``batch_``-stripped pipeline_mode value (which matches what the
    manifest rebuild's ``_source_from_row`` derives for entities whose path key equals
    their source). For the closed set of entities in ``_SPORTS_REF_SOURCE_OVERRIDE``
    whose canonical UAC source legitimately differs from their GCS path key, returns
    the overridden source so ``record_captured`` accepts it.
    """
    override = _SPORTS_REF_SOURCE_OVERRIDE.get(entity_name.lower())
    if override is not None:
        return override
    pm_val = _orch._sports_ref_pm(entity_name)
    if pm_val.startswith("batch_"):
        return pm_val[len("batch_") :]
    return pm_val


def _sports_ref_sink_for(bucket: str, date: str, entity_name: str) -> _orch.DataSink:
    """Return a DataSink for sports_reference writes with canonical pipeline_mode= in prefix.

    Prefix: ``sports_reference/by_date/day={date}/pipeline_mode={pm}``

    Callers pass ``partition={"entity": entity_name, "league": L}`` (or other keys
    WITHOUT ``day``/``pipeline_mode`` — those are already in the prefix).  Since
    DataSink sorts partition keys alphabetically, ``entity`` (e) comes before
    ``league`` (l) — producing the correct canonical segment order.
    """
    pm = _orch._sports_ref_pm(entity_name)
    return _orch.get_data_sink(
        bucket=bucket,
        prefix=f"sports_reference/by_date/day={date}/pipeline_mode={pm}",
    )


def _sports_ref_canonical_blob_path(
    date: str,
    entity: str,
    *,
    league: str | None = None,
    filename: str | None = None,
) -> str:
    """Return the canonical GCS blob path (with pipeline_mode=) for a sports_reference object."""
    pm = _orch._sports_ref_pm(entity)
    fname = filename or f"{entity}.parquet"
    if league:
        return f"sports_reference/by_date/day={date}/pipeline_mode={pm}/entity={entity}/league={league}/{fname}"
    return f"sports_reference/by_date/day={date}/pipeline_mode={pm}/entity={entity}/{fname}"


def _sports_ref_legacy_blob_path(
    date: str,
    entity: str,
    *,
    league: str | None = None,
    filename: str | None = None,
) -> str:
    """Return the legacy GCS blob path (no pipeline_mode=) for a sports_reference object."""
    fname = filename or f"{entity}.parquet"
    if league:
        return f"sports_reference/by_date/day={date}/entity={entity}/league={league}/{fname}"
    return f"sports_reference/by_date/day={date}/entity={entity}/{fname}"


def _resolve_sports_ref_blob(
    storage_client: object,
    bucket: str,
    canonical: str,
    legacy: str,
) -> str:
    """Return the canonical blob path if it exists, else the legacy path.

    Used by read helpers to probe the new canonical path first (post-migration)
    and fall back to the legacy path for pre-migration objects, until Phase E8
    deletes stale objects.
    """
    try:
        canon_blob = storage_client.bucket(bucket).blob(canonical)  # type: ignore[union-attr]
        if canon_blob.exists():
            return canonical
    except Exception:
        pass
    return legacy


def _read_fixture_ids_from_gcs(bucket: str, date: str) -> list[int]:
    """Read completed fixture IDs from existing GCS fixtures parquet.

    Returns fixture IDs with status FT/AET/PEN. Falls back to empty list
    if no fixtures parquet exists for the date (zero-fixture day).
    """
    _canon_prefix = _orch._sports_ref_canonical_blob_path(date, "fixtures", filename="fixtures.parquet")
    _legacy_prefix = _orch._sports_ref_legacy_blob_path(date, "fixtures", filename="fixtures.parquet")
    try:
        storage_client = _orch.get_storage_client()
        prefix = _orch._resolve_sports_ref_blob(storage_client, bucket, _canon_prefix, _legacy_prefix)
        blob = storage_client.bucket(bucket).blob(prefix)
        if not blob.exists():
            _orch.logger.debug("No fixtures parquet at gs://%s/%s", bucket, prefix)
            return []
        local = f"{_orch.tempfile.gettempdir()}/_fixture_ids_{date}.parquet"
        blob.download_to_filename(local)
        df = _orch.pd.read_parquet(local)
        completed = {"FT", "AET", "PEN"}
        if "status_short" in df.columns and "af_fixture_id" in df.columns:
            mask = df["status_short"].isin(completed)
            ids = df.loc[mask, "af_fixture_id"].dropna().astype(int).tolist()
            _orch.logger.info("GCS fixture lookup date=%s: %d completed fixture IDs", date, len(ids))
            return ids
        _orch.logger.debug("Fixtures parquet missing expected columns for date=%s", date)
        return []
    except Exception as exc:
        _orch.logger.debug("Failed to read fixtures from GCS for date=%s: %s", date, exc)
        return []


def _write_fixtures_per_league(
    sink: _orch.DataSink,
    fixture_df: _orch.pd.DataFrame,
    date: str,
    *,
    source_label: str,
    bucket: str | None = None,
    skip_if_unchanged: bool = False,
) -> None:
    """Write canonical fixtures parquet per league (single-SSOT, no bare fallback).

    FIXTURES is a league-axis data type. We split the dataframe by league and
    write one parquet per partition — the bare-path date-aggregate is dropped
    per ``sports_manifest_single_ssot_2026_04_30``. When the dataframe is
    missing both ``league_id`` and ``af_league_id`` (or all rows lack a
    league assignment), the call logs a warning and skips the write to keep
    the manifest honest. Caller is responsible for emitting empty/failure
    manifest rows.
    """
    if fixture_df.empty:
        return

    # Prefer canonical league_id if it's already on the frame; otherwise
    # derive from af_league_id via the prediction-league reverse mapping.
    if "league_id" in fixture_df.columns and fixture_df["league_id"].notna().any():
        _league_col = "league_id"
        # Stringify, then blank-out empties so .notna() below treats them
        # as missing (matching the af_league_id branch's behaviour).
        _stringified = fixture_df["league_id"].astype(str)
        _league_series = _stringified.mask(_stringified.str.strip() == "")
    elif "af_league_id" in fixture_df.columns and fixture_df["af_league_id"].notna().any():
        _af_to_canonical: dict[int, str] = {}
        for _league_def in _orch.get_prediction_leagues():
            if _league_def.api_football_id is not None:
                _af_to_canonical[_league_def.api_football_id] = _league_def.league_id
        # af_league_id may be float-typed in pandas after read_parquet; coerce.
        _af_int_series = _orch.pd.to_numeric(fixture_df["af_league_id"], errors="coerce").astype("Int64")
        _league_col = "_canonical_league_id"
        fixture_df = fixture_df.copy()
        fixture_df[_league_col] = _af_int_series.map(
            lambda v: _af_to_canonical.get(int(v)) if _orch.pd.notna(v) else None
        )
        # Fallback for unmapped leagues — partition by stringified af_league_id.
        _missing_canonical = fixture_df[_league_col].isna() & _af_int_series.notna()
        if _missing_canonical.any():
            fixture_df.loc[_missing_canonical, _league_col] = _af_int_series[_missing_canonical].astype(str)
        _league_series = fixture_df[_league_col]
    else:
        _orch.logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "fixture_df missing both league_id and af_league_id columns (rows=%d). "
            "Skipping write to keep manifest honest.",
            date,
            source_label,
            len(fixture_df),
        )
        return

    _has_league = _league_series.notna() & (_league_series.astype(str).str.strip() != "")
    _with_league = fixture_df[_has_league]
    _without_league = fixture_df[~_has_league]

    if _with_league.empty:
        _orch.logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "no rows had a derivable league (rows=%d). Skipping write to keep manifest honest.",
            date,
            source_label,
            len(fixture_df),
        )
        return

    for _lid, _ldf in _with_league.groupby(_league_col):
        _lid_str = str(_lid)
        _ldf_clean = _orch.stamp_available_at_explicit(
            _ldf.drop(columns=["_canonical_league_id"], errors="ignore"), when=_orch.datetime.now(_orch.UTC)
        )
        # Write-skip guard (opt-in, e.g. the daily re-poll): if the on-disk per-league
        # parquet already holds identical fixture data (ignoring the re-stamped
        # available_at), skip the re-write. Avoids churning a fresh object version each
        # run and preserves the earliest available_at. Bias is to write on any doubt.
        if (
            skip_if_unchanged
            and bucket
            and _orch._per_league_fixtures_data_unchanged(
                bucket, date, "fixtures", _orch._canonical_league_id(_lid_str), _ldf_clean
            )
        ):
            _orch.logger.debug(
                "FIXTURES skip-if-unchanged: day=%s league=%s (source=%s) unchanged — skipping re-write",
                date,
                _orch._canonical_league_id(_lid_str),
                source_label,
            )
            continue
        # v9 canonical write: use entity-specific sink so pipeline_mode= lands
        # in the prefix (DataSink sorts partition keys alphabetically — p > l > e,
        # so pipeline_mode cannot go in the partition dict without breaking order).
        _fix_canonical_sink = _orch._sports_ref_sink_for(bucket, date, "fixtures") if bucket else sink
        _orch._gated_sink_write(
            _fix_canonical_sink,
            data=_ldf_clean,
            partition={"entity": "fixtures", "league": _orch._canonical_league_id(_lid_str)},
            filename="fixtures.parquet",
            venue="api_football",
            entity="fixtures",
        )

    if not _without_league.empty:
        _orch.logger.warning(
            "FIXTURES bare-path fallback triggered for date=%s (source=%s) — data shape regression: "
            "%d rows missing league assignment. Skipping bare write to keep manifest honest.",
            date,
            source_label,
            len(_without_league),
        )


def _read_existing_per_league_fixture_ids(
    bucket: str,
    date: str,
    entity_name: str,
    canonical_league_id: str,
) -> frozenset[int]:
    """Return the set of af_fixture_ids already captured in a per-league parquet.

    Reads ``sports_reference/by_date/day={date}/entity={entity}/league={L}/{entity}.parquet``
    and returns ``frozenset({af_fixture_id, ...})`` of rows present. Used by the
    per-fixture pre-fetch skip path to avoid wasting api_football calls on
    fixtures whose data is already on disk.

    Returns empty frozenset on any miss / read failure (the caller treats that
    as "no captured fixtures known, fetch everything in scope"). Logs at debug
    level so operators can confirm the skip path engaged.
    """
    _canon_path = _orch._sports_ref_canonical_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    _legacy_path = _orch._sports_ref_legacy_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    try:
        storage_client = _orch.get_storage_client()
        blob_path = _orch._resolve_sports_ref_blob(storage_client, bucket, _canon_path, _legacy_path)
        blob = storage_client.bucket(bucket).blob(blob_path)
        if not blob.exists():
            return frozenset()
        existing_bytes = storage_client.download_bytes(bucket=bucket, blob_path=blob_path)
        existing = _orch.pd.read_parquet(_orch.io.BytesIO(existing_bytes))
    except Exception as exc:
        _orch.logger.debug(
            "Pre-fetch skip read failed for gs://%s — proceeding without skip: %s",
            bucket,
            exc,
        )
        return frozenset()
    fid_col = "af_fixture_id" if "af_fixture_id" in existing.columns else "fixture_id"
    if fid_col not in existing.columns:
        return frozenset()
    fids = _orch.pd.to_numeric(existing[fid_col], errors="coerce").dropna().astype(int)
    return frozenset(int(x) for x in fids.tolist())


def _per_league_fixtures_data_unchanged(
    bucket: str,
    date: str,
    entity_name: str,
    canonical_league_id: str,
    new_df: _orch.pd.DataFrame,
) -> bool:
    """Return True iff the existing per-league parquet holds identical DATA to ``new_df``.

    Compares the new frame against the on-disk parquet **excluding ``available_at``**
    (which the writer re-stamps to ``now()`` on every write, so it always differs and
    is provenance, not fixture data). Used as a write-skip guard for the daily fixtures
    re-poll: when a (date, league) cell's fixtures are unchanged, re-writing only churns
    a fresh GCS object version (and would wrongly bump ``available_at`` to a later time).
    Skipping preserves the original — earliest — ``available_at``.

    **Safety bias:** any uncertainty (missing object, read failure, column/length/dtype
    mismatch) returns ``False`` → the caller writes. This guard NEVER skips a real change;
    worst case it fails to skip an unchanged write (no correctness impact, just no saving).
    Both frames are normalised through an in-memory parquet round-trip so dtypes match the
    read-back form.
    """
    _canon_path = _orch._sports_ref_canonical_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    _legacy_path = _orch._sports_ref_legacy_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    try:
        storage_client = _orch.get_storage_client()
        blob_path = _orch._resolve_sports_ref_blob(storage_client, bucket, _canon_path, _legacy_path)
        blob = storage_client.bucket(bucket).blob(blob_path)
        if not blob.exists():
            return False
        existing = _orch.pd.read_parquet(
            _orch.io.BytesIO(storage_client.download_bytes(bucket=bucket, blob_path=blob_path))
        )
        # Round-trip the new frame so its dtypes match the parquet read-back form.
        new_norm = _orch.pd.read_parquet(_orch.io.BytesIO(new_df.to_parquet(index=False)))
    except Exception as exc:
        _orch.logger.debug(
            "skip-if-unchanged read/normalise failed for gs://%s — will write: %s",
            bucket,
            exc,
        )
        return False

    def _norm(frame: _orch.pd.DataFrame) -> _orch.pd.DataFrame:
        out = frame.drop(columns=["available_at"], errors="ignore")
        _key = (
            "af_fixture_id"
            if "af_fixture_id" in out.columns
            else ("fixture_id" if "fixture_id" in out.columns else None)
        )
        if _key is not None:
            out = out.sort_values(_key, kind="stable")
        return out.reindex(sorted(out.columns), axis=1).reset_index(drop=True)

    left = _norm(existing)
    right = _norm(new_norm)
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    return bool(left.equals(right))


def _merge_with_existing_per_league_parquet(
    bucket: str,
    date: str,
    entity_name: str,
    canonical_league_id: str,
    new_rows: _orch.pd.DataFrame,
    fid_col: str,
) -> _orch.pd.DataFrame:
    """Read-modify-write merge for recovery-mode per-league parquet writes.

    The default per-fixture entity write path overwrites the per-league
    parquet at ``sports_reference/by_date/day={date}/entity={entity}/league={L}/{entity}.parquet``.
    That's correct when we always fetch ALL fixtures for the (date, league)
    cell — but in recovery mode we only fetch a subset (the
    ``--recovery-fixture-ids`` allowlist), so a plain overwrite would drop
    the rest of the cell's previously-captured fixtures.

    This helper reads the existing parquet (if any), drops rows whose
    ``af_fixture_id`` matches our new_rows (we just refetched them, prefer
    the fresh values), and concatenates new_rows. Result: existing fixtures
    survive, our targeted re-fetches replace any stale rows for those
    fixture_ids.

    Returns the merged DataFrame ready for the standard sink write.
    """
    _canon_path = _orch._sports_ref_canonical_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    _legacy_path = _orch._sports_ref_legacy_blob_path(
        date, entity_name, league=canonical_league_id, filename=f"{entity_name}.parquet"
    )
    try:
        storage_client = _orch.get_storage_client()
        blob_path = _orch._resolve_sports_ref_blob(storage_client, bucket, _canon_path, _legacy_path)
        blob = storage_client.bucket(bucket).blob(blob_path)
        if not blob.exists():
            return new_rows
        existing_bytes = storage_client.download_bytes(bucket=bucket, blob_path=blob_path)
        existing = _orch.pd.read_parquet(_orch.io.BytesIO(existing_bytes))
    except Exception as exc:
        _orch.logger.warning(
            "Recovery-mode merge: could not read existing parquet at gs://%s — "
            "proceeding with overwrite (existing fixture rows for this cell will be lost): %s",
            bucket,
            exc,
        )
        return new_rows

    if fid_col not in existing.columns:
        # Schema drift — existing parquet lacks the fixture_id column we'd dedup on.
        # Safer to overwrite + log than to concat-with-mismatched-schema.
        _orch.logger.warning(
            "Recovery-mode merge: existing parquet at gs://%s (canonical/legacy) missing %r column "
            "(found: %s) — overwriting rather than risk schema mismatch",
            bucket,
            fid_col,
            list(existing.columns),
        )
        return new_rows

    new_fids = set(_orch.pd.to_numeric(new_rows[fid_col], errors="coerce").dropna().astype(int).tolist())
    existing_fid_int = _orch.pd.to_numeric(existing[fid_col], errors="coerce")
    keep_mask = ~existing_fid_int.isin(new_fids)
    survivors = existing[keep_mask]
    merged = _orch.pd.concat([survivors, new_rows], ignore_index=True, sort=False)
    _orch.logger.info(
        "Recovery-mode merge for %s/league=%s on %s: %d existing rows + %d new = %d total "
        "(replaced %d rows with same af_fixture_id)",
        entity_name,
        canonical_league_id,
        date,
        len(existing),
        len(new_rows),
        len(merged),
        len(existing) - len(survivors),
    )
    return merged


def _build_fixture_league_map_from_gcs(bucket: str, date: str) -> dict[str, str]:
    """Build a mapping from AF fixture_id (str) to canonical league_id.

    Reads the fixtures parquet from GCS (sports_reference/by_date/day={date}/entity=fixtures/)
    which contains af_fixture_id and league_id columns. Returns an empty dict if the
    fixtures file is missing or lacks the required columns.
    """
    # Build reverse mapping from UAC: af_league_id -> canonical league_id
    _af_league_to_canonical: dict[int, str] = {}
    for league_def in _orch.get_prediction_leagues():
        if league_def.api_football_id is not None:
            _af_league_to_canonical[league_def.api_football_id] = league_def.league_id

    _canon_pfx = _orch._sports_ref_canonical_blob_path(date, "fixtures", filename="fixtures.parquet")
    _legacy_pfx = _orch._sports_ref_legacy_blob_path(date, "fixtures", filename="fixtures.parquet")
    try:
        storage_client = _orch.get_storage_client()
        prefix = _orch._resolve_sports_ref_blob(storage_client, bucket, _canon_pfx, _legacy_pfx)
        blob = storage_client.bucket(bucket).blob(prefix)
        if not blob.exists():
            _orch.logger.debug("No fixtures parquet at gs://%s/%s for league mapping", bucket, prefix)
            return {}
        local = f"{_orch.tempfile.gettempdir()}/_fixture_league_map_{date}.parquet"
        blob.download_to_filename(local)
        df = _orch.pd.read_parquet(local)
        result: dict[str, str] = {}
        if "af_fixture_id" in df.columns:
            # Prefer league_id column if present
            if "league_id" in df.columns:
                for _, row in df[["af_fixture_id", "league_id"]].dropna().iterrows():
                    result[str(int(row["af_fixture_id"]))] = str(row["league_id"])
            # Fallback: use af_league_id -> canonical mapping
            elif "af_league_id" in df.columns:
                for _, row in df[["af_fixture_id", "af_league_id"]].dropna().iterrows():
                    af_lid = int(row["af_league_id"])
                    canonical = _af_league_to_canonical.get(af_lid)
                    if canonical:
                        result[str(int(row["af_fixture_id"]))] = canonical
        _orch.logger.info("Fixture league map: %d mappings built from GCS for date=%s", len(result), date)
        return result
    except Exception as exc:
        _orch.logger.debug("Failed to build fixture league map for date=%s: %s", date, exc)
        return {}


# Process-level write-once guard for the STATIC team-mapping table. The table is
# built entirely from UAC constants (EPL/Bundesliga aliases) — byte-identical on
# every call — but was being re-written to the SAME GCS blob on EVERY backfill
# date (~1.1k writes/run to one object) → GCS hot-object 429s dropped ~16% of the
# writes (no retry). One write per process is correct: the content cannot change
# mid-run. (Per-season roster changes are handled by the per-date FIXTURES/TEAMS
# entities, not this static lookup.)
_TEAM_MAPPING_WRITTEN_THIS_PROCESS: bool = False


def _write_team_mapping(bucket: str) -> None:
    """Build and write TeamMapping table to GCS — once per process.

    Combines UAC team_mappings.py (API-Football names) and team_names.py
    (Odds API / Understat names) into a single lookup table keyed by
    canonical team_id. Used by FSS to resolve provider-specific IDs.

    The table is STATIC (UAC constants) so it is written ONCE per process; later
    per-date calls short-circuit to avoid hammering the single GCS object.

    Path: sports_reference/mappings/team_mapping.parquet
    """
    global _TEAM_MAPPING_WRITTEN_THIS_PROCESS
    if _TEAM_MAPPING_WRITTEN_THIS_PROCESS:
        return

    rows: list[dict[str, str]] = []

    # EPL teams
    epl_ids: set[str] = set(_orch.EPL_TEAM_ALIASES.keys()) | set(_orch.CANONICAL_TO_ODDS_API_EPL.keys())
    for canonical_id in sorted(epl_ids):
        aliases = _orch.EPL_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": _orch.CANONICAL_TO_ODDS_API_EPL.get(canonical_id, ""),
                "understat_name": _orch.CANONICAL_TO_UNDERSTAT_EPL.get(canonical_id, ""),
                "league": "EPL",
            }
        )

    # Bundesliga teams
    bun_ids: set[str] = set(_orch.BUNDESLIGA_TEAM_ALIASES.keys()) | set(_orch.CANONICAL_TO_ODDS_API_BUNDESLIGA.keys())
    for canonical_id in sorted(bun_ids):
        aliases = _orch.BUNDESLIGA_TEAM_ALIASES.get(canonical_id, [])
        display_name = aliases[0] if aliases else canonical_id
        rows.append(
            {
                "canonical_team_id": canonical_id,
                "display_name": display_name,
                "odds_api_name": _orch.CANONICAL_TO_ODDS_API_BUNDESLIGA.get(canonical_id, ""),
                "understat_name": "",
                "league": "BUNDESLIGA",
            }
        )

    if not rows:
        return

    try:
        df = _orch.pd.DataFrame(rows)
        mapping_sink = _orch.get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=df, partition={}, format="parquet", filename="team_mapping.parquet")
        _TEAM_MAPPING_WRITTEN_THIS_PROCESS = True
        _orch.logger.info("Team mapping: %d entries written (once per process)", len(df))
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="team_mapping_write",
        )


def _write_fixture_mapping(bucket: str, date: str) -> None:
    """Build and write FixtureMapping table to GCS.

    Reads the fixture instruments already written for today, extracts the
    canonical_fixture_id and the API-Football numeric fixture_id, and
    writes a mapping table that FSS uses to resolve provider-specific IDs.

    Path: sports_reference/mappings/fixture_mapping.parquet
    """
    try:
        # Read today's fixtures from the instruments parquet we just wrote
        blob_path = f"instrument_availability/by_date/day={date}/venue=API_FOOTBALL/instruments.parquet"
        storage = _orch.get_storage_client()
        raw = storage.download_bytes(bucket, blob_path)
        if raw is None:
            _orch.logger.debug("Fixture mapping: no fixtures parquet found for %s", date)
            return
        df = _orch.pd.read_parquet(_orch.io.BytesIO(raw))
        if df.empty:
            _orch.logger.debug("Fixture mapping: no fixtures found for %s", date)
            return

        rows: list[dict[str, str]] = []
        for _, row in df.iterrows():
            instrument_key = str(row["instrument_key"]) if "instrument_key" in row.index else ""
            raw_symbol = str(row["raw_symbol"]) if "raw_symbol" in row.index else ""
            # Extract API-Football numeric ID from the symbol or key
            # The instrument_key is canonical: ENGLAND_PREMIER_LEAGUE:ARSENAL_v_CHELSEA:20260322
            # The raw_symbol is: Arsenal vs Chelsea
            rows.append(
                {
                    "canonical_fixture_id": instrument_key,
                    "raw_symbol": raw_symbol,
                    "date": date,
                    "venue": "API_FOOTBALL",
                }
            )

        if not rows:
            return

        mapping_df = _orch.pd.DataFrame(rows)
        mapping_sink = _orch.get_data_sink(bucket=bucket, prefix="sports_reference/mappings")
        mapping_sink.write(data=mapping_df, partition={}, format="parquet", filename="fixture_mapping.parquet")
        _orch.logger.info("Fixture mapping: %d entries written for %s", len(mapping_df), date)
    except Exception as exc:
        _exc_name = type(exc).__name__
        _msg = str(exc)
        # GCS blob not found (404): the instruments.parquet for this (date, venue) was
        # never written. This is benign for ANY date (not just forward-poll window) —
        # for historical forward-polled days the per-fixture entities were captured
        # via enrichment-only mode without ever rolling up an availability parquet at
        # `instrument_availability/by_date/.../venue=API_FOOTBALL/instruments.parquet`.
        # Fixture-mapping is a best-effort secondary write; absent the upstream
        # parquet there is nothing to map. Silently no-op rather than escalating to
        # classify_and_emit_error. Reference: issue doc
        # `plans/active/issues/api_football_enrichment_preflight_runtime_mismatch_2026_05_13.md`.
        if _exc_name == "NotFound" or "404" in _msg or "No such object" in _msg:
            _orch.logger.info(
                "Fixture mapping: no API_FOOTBALL instruments parquet for %s — skipping (no upstream availability rollup written)",
                date,
            )
            return
        if isinstance(exc, (FileNotFoundError, OSError)):
            _orch.logger.debug("Fixture mapping: could not read fixtures for %s: %s", date, exc)
            return
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="fixture_mapping_write",
        )
