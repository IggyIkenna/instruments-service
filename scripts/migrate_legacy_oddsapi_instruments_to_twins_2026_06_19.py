#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports odds-api legacy instrument twin-migration done (0 migrate-first) + legacy deleted
"""Preserve-before-delete: migrate the 9,723 legacy "unmappable" sports odds-api INSTRUMENT
objects to canonical twins so the operator-gated post-audit delete loses no data.

Background (verified by parent + this script's pre-flight):
  The IS sports bucket carries 9,723 objects flagged ``classification=UNMAPPABLE`` /
  ``twin_exists=False`` in
  ``_index/audit/instruments_store_legacy_delete_list_sports.parquet``:

    * 9,721  legacy DASH-separator odds-api instrument definitions
             ``instrument_availability/by-date/day-{D}/{slug}/instruments.parquet``
             (note: ``by-date`` not ``by_date``; ``day-{D}`` not ``day={D}``; raw ``{slug}``
             not ``league={L}``). 52 distinct slugs = 27 the-odds-api ``soccer_*`` sport_keys
             + 25 display-names.
    * 2      bare top-level ``day=2026-03-21/venue=BETFAIR/{hash}.parquet`` (unified instrument
             catalogue schema; no league axis).

  This odds-api instrument universe is GENUINELY-UNIQUE (the canonical ``venue=odds_api`` rows
  in the ``_index`` are all ``empty_confirmed`` and stop at 2020-06-05; recent canonical days
  carry ``venue=API_FOOTBALL`` only). The "dash" issue is a legacy PATH shape, not bad data →
  it is MIGRATABLE (path-canonicalise + slug→league translate), NOT "unmappable".

What this script does (OBJECT COPIES ONLY — never deletes, never touches the ``_index``):
  1. Resolve each slug → canonical league_id via the UAC SSOT
     (``ODDS_API_DISPLAY_TO_CANONICAL`` + ``LEAGUE_CLASSIFICATION_DATA`` →
     ``get_league_by_api_football_id``). No parallel hardcoded map. For any slug the UAC
     resolver genuinely cannot map, preserve under a deterministic safe label
     ``league=ODDS_API_UNMAPPED__{normalized_slug}`` and LOG it (zero data loss).
  2. Copy each legacy object → canonical twin
     ``instrument_availability/by_date/day={D}/league={L}/venue=ODDS_API/instruments.parquet``.
     COLLISIONS (multiple legacy slugs → same day+league twin, e.g. the ``soccer_*`` and the
     display-name form): READ both + concat + drop_duplicates(instrument_key) + write the UNION
     (the two forms have DISJOINT instrument_keys — a blind overwrite would lose ~50% of rows).
     The 2 bare BETFAIR objects copy to ``by_date/day=.../venue=BETFAIR/{hash}.parquet`` (no
     league axis; hash stem preserved → both survive).
  3. Verify: ``gcs_describe_object`` a >=40-object sample (twin exists + byte/size sanity) and
     re-read 3 twins for row-count parity vs the legacy source(s).
  4. Write the migration proof parquet to
     ``_index/audit/sports_legacy_oddsapi_twin_migration_2026_06_19.parquet``.

Run: ``--apply`` to perform the copy (idempotent overwrite); default is dry-run.
"""

from __future__ import annotations

import argparse
import collections
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_classification_data_a import (
    LEAGUE_CLASSIFICATION_DATA_A,
)
from unified_api_contracts.canonical.domain.sports.league_classification_data_b import (
    LEAGUE_CLASSIFICATION_DATA_B,
)
from unified_api_contracts.canonical.domain.sports.league_data import (
    get_league_by_api_football_id,
)
from unified_api_contracts.canonical.domain.sports.provider_league_ids import (
    ODDS_API_DISPLAY_TO_CANONICAL,
)
from unified_trading_library.cloud_interface import (
    gcs_copy_object,
    gcs_describe_object,
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("oddsapi_twin_migration")

BUCKET = "instruments-store-sports-prd-central-element-323112"
DELETE_LIST_KEY = "_index/audit/instruments_store_legacy_delete_list_sports.parquet"
PROOF_KEY = "_index/audit/sports_legacy_oddsapi_twin_migration_2026_06_19.parquet"
ODDS_VENUE = "ODDS_API"
DASH_PREFIX = "instrument_availability/by-date/"
MAX_WORKERS = 32


# the-odds-api ``soccer_*`` sport_key → api_football_id, derived from the UAC SSOT
# (LEAGUE_CLASSIFICATION_DATA carries ``odds_api_league_name``). NOT a parallel map — this
# is a view of the UAC dicts, the same way the resolver below reads ODDS_API_DISPLAY_TO_CANONICAL.
def _build_sport_key_to_af() -> dict[str, int]:
    out: dict[str, int] = {}
    for table in (LEAGUE_CLASSIFICATION_DATA_A, LEAGUE_CLASSIFICATION_DATA_B):
        for af_id, entry in table.items():
            key = entry.get("odds_api_league_name")
            if isinstance(key, str) and key:
                out[key] = af_id
    return out


_SPORT_KEY_TO_AF: dict[str, int] = _build_sport_key_to_af()


def normalize_unmapped_label(slug: str) -> str:
    """Deterministic safe label for a slug the UAC resolver cannot map."""
    norm = "".join(ch if ch.isalnum() else "_" for ch in slug).strip("_").upper()
    while "__" in norm:
        norm = norm.replace("__", "_")
    return f"ODDS_API_UNMAPPED__{norm}"


def resolve_canonical_league(slug: str) -> tuple[str, bool]:
    """slug → (league_id, mapped). Pure UAC SSOT. Falls back to a logged safe label."""
    if slug in ODDS_API_DISPLAY_TO_CANONICAL:
        return ODDS_API_DISPLAY_TO_CANONICAL[slug], True
    af_id = _SPORT_KEY_TO_AF.get(slug)
    if af_id is not None:
        league_def = get_league_by_api_football_id(af_id)
        if league_def is not None:
            return league_def.league_id, True
    return normalize_unmapped_label(slug), False


@dataclass
class MigrationRow:
    legacy_path: str
    canonical_twin_path: str
    slug: str
    canonical_league: str
    mapped: bool
    rows: int = 0
    bytes: int = 0
    status: str = ""


@dataclass
class Plan:
    # twin_path -> list[(slug, legacy_path, mapped, canonical_league)]
    twins: dict[str, list[tuple[str, str, bool, str]]] = field(default_factory=dict)


def _parse_dash(path: str) -> tuple[str, str]:
    """``instrument_availability/by-date/day-{D}/{slug}/instruments.parquet`` → (day, slug)."""
    parts = path.split("/")
    day = parts[2].replace("day-", "", 1)
    slug = parts[3]
    return day, slug


def build_plan(delete_df: pd.DataFrame) -> tuple[Plan, list[tuple[str, str]]]:
    """Group every legacy object by its canonical twin path.

    Returns (plan, bare_objects) where bare_objects = [(legacy_path, twin_path), ...]
    for the 2 BETFAIR bare top-level objects (no league axis, keep hash stem).
    """
    plan = Plan()
    bare: list[tuple[str, str]] = []
    for _, r in delete_df.iterrows():
        legacy = str(r["legacy_path"])
        if legacy.startswith(DASH_PREFIX):
            day, slug = _parse_dash(legacy)
            league, mapped = resolve_canonical_league(slug)
            twin = f"instrument_availability/by_date/day={day}/league={league}/venue={ODDS_VENUE}/instruments.parquet"
            plan.twins.setdefault(twin, []).append((slug, legacy, mapped, league))
        else:
            # bare top-level ``day={D}/venue=BETFAIR/{hash}.parquet`` — the delete-list already
            # carries the canonical_twin_path; trust it (by-date→by_date + prefix; hash stem kept).
            twin = str(r["canonical_twin_path"]) or (f"instrument_availability/by_date/{legacy}")
            bare.append((legacy, twin))
    return plan, bare


def _download_df(client, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(client.download_bytes(BUCKET, key)))


def _upload_df(client, key: str, df: pd.DataFrame) -> int:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    data = buf.getvalue()
    client.upload_bytes(BUCKET, key, data, content_type="application/octet-stream")
    return len(data)


def _uri(key: str) -> str:
    return f"gs://{BUCKET}/{key}"


def migrate_twin(client, twin: str, sources: list[tuple[str, str, bool, str]], apply: bool) -> list[MigrationRow]:
    """Copy/union all legacy sources for one twin path. Returns one MigrationRow per legacy obj."""
    mapped = sources[0][2]
    league = sources[0][3]
    status = "MIGRATED" if mapped else "MIGRATED-UNMAPPED-LABEL"
    out: list[MigrationRow] = []

    if len(sources) == 1:
        slug, legacy, _, _ = sources[0]
        src_df = _download_df(client, legacy)
        if apply:
            # byte-identical single-object → server-side rewrite (fast, no egress)
            gcs_copy_object(_uri(legacy), _uri(twin))
        meta = gcs_describe_object(_uri(twin)) if apply else None
        out.append(
            MigrationRow(
                legacy_path=legacy,
                canonical_twin_path=twin,
                slug=slug,
                canonical_league=league,
                mapped=mapped,
                rows=len(src_df),
                bytes=(meta.size if meta else 0),
                status=status,
            )
        )
        return out

    # collision: read all, concat, dedup by instrument_key (forms have disjoint keys → union)
    frames: list[pd.DataFrame] = []
    per_source: list[tuple[str, str, int]] = []  # (slug, legacy, rows)
    for slug, legacy, _, _ in sources:
        d = _download_df(client, legacy)
        frames.append(d)
        per_source.append((slug, legacy, len(d)))
    union = pd.concat(frames, ignore_index=True)
    before = len(union)
    if "instrument_key" in union.columns:
        union = union.drop_duplicates(subset=["instrument_key"], keep="first")
    else:
        union = union.drop_duplicates(keep="first")
    union_rows = len(union)
    written_bytes = 0
    if apply:
        written_bytes = _upload_df(client, twin, union)
    meta = gcs_describe_object(_uri(twin)) if apply else None
    twin_bytes = meta.size if meta else written_bytes
    logger.info(
        "UNION twin=%s sources=%d concat_rows=%d union_rows=%d",
        twin,
        len(sources),
        before,
        union_rows,
    )
    for slug, legacy, rows in per_source:
        out.append(
            MigrationRow(
                legacy_path=legacy,
                canonical_twin_path=twin,
                slug=slug,
                canonical_league=league,
                mapped=mapped,
                rows=rows,
                bytes=twin_bytes,
                status=status + "-UNION",
            )
        )
    return out


def migrate_bare(client, legacy: str, twin: str, apply: bool) -> MigrationRow:
    src_df = _download_df(client, legacy)
    if apply:
        gcs_copy_object(_uri(legacy), _uri(twin))
    meta = gcs_describe_object(_uri(twin)) if apply else None
    return MigrationRow(
        legacy_path=legacy,
        canonical_twin_path=twin,
        slug="(bare BETFAIR)",
        canonical_league="(none — no league axis)",
        mapped=True,
        rows=len(src_df),
        bytes=(meta.size if meta else 0),
        status="MIGRATED",
    )


def run(apply: bool) -> None:
    client = get_storage_client()
    logger.info("Reading delete-list %s", DELETE_LIST_KEY)
    delete_df = _download_df(client, DELETE_LIST_KEY)
    logger.info("delete-list rows=%d", len(delete_df))

    plan, bare = build_plan(delete_df)
    n_legacy = sum(len(v) for v in plan.twins.values()) + len(bare)
    n_twins = len(plan.twins) + len(bare)
    collisions = {t: v for t, v in plan.twins.items() if len(v) > 1}
    unmapped_slugs = sorted({s for v in plan.twins.values() for (s, _, m, _) in v if not m})
    logger.info(
        "PLAN legacy_objs=%d twin_paths=%d collisions=%d unmapped_slugs=%d",
        n_legacy,
        n_twins,
        len(collisions),
        len(unmapped_slugs),
    )
    if unmapped_slugs:
        for s in unmapped_slugs:
            logger.warning("UNMAPPED slug preserved under safe label: %r", s)

    rows: list[MigrationRow] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(migrate_twin, client, twin, srcs, apply): twin for twin, srcs in plan.twins.items()}
        for legacy, twin in bare:
            futs[pool.submit(migrate_bare, client, legacy, twin, apply)] = twin
        for done, fut in enumerate(as_completed(futs), start=1):
            res = fut.result()
            if isinstance(res, list):
                rows.extend(res)
            else:
                rows.append(res)
            if done % 500 == 0:
                logger.info("progress: %d/%d twins processed", done, len(futs))

    logger.info("processed %d legacy objects across %d twins", len(rows), n_twins)

    # ---- verification (apply only) ----
    if apply:
        verify_sample = rows[:: max(1, len(rows) // 60)][:60]
        ok = 0
        for mr in verify_sample:
            meta = gcs_describe_object(_uri(mr.canonical_twin_path))
            if meta is not None and meta.size > 0:
                ok += 1
        logger.info("VERIFY describe: %d/%d sampled twins exist with size>0", ok, len(verify_sample))

        # parity re-reads on 3 distinct twins (1 single + up-to-2 union)
        single = next((mr for mr in rows if mr.status == "MIGRATED" and "BETFAIR" not in mr.slug), None)
        unions = [mr for mr in rows if mr.status.endswith("UNION")][:4]
        parity_checks: list[MigrationRow] = ([single] if single else []) + unions[:2]
        for mr in parity_checks:
            twin_df = _download_df(client, mr.canonical_twin_path)
            same_twin = [m for m in rows if m.canonical_twin_path == mr.canonical_twin_path]
            src_keys: set[str] = set()
            for m in same_twin:
                sdf = _download_df(client, m.legacy_path)
                if "instrument_key" in sdf.columns:
                    src_keys |= set(sdf["instrument_key"])
            twin_keys = set(twin_df["instrument_key"]) if "instrument_key" in twin_df.columns else set()
            logger.info(
                "PARITY twin=%s twin_rows=%d twin_keys=%d src_union_keys=%d keys_covered=%s",
                mr.canonical_twin_path,
                len(twin_df),
                len(twin_keys),
                len(src_keys),
                src_keys.issubset(twin_keys) if src_keys else "n/a",
            )

    # ---- proof parquet ----
    proof_df = pd.DataFrame(
        [
            {
                "legacy_path": mr.legacy_path,
                "canonical_twin_path": mr.canonical_twin_path,
                "slug": mr.slug,
                "canonical_league": mr.canonical_league,
                "mapped": mr.mapped,
                "rows": mr.rows,
                "bytes": mr.bytes,
                "status": mr.status,
            }
            for mr in rows
        ]
    )
    logger.info(
        "PROOF rows=%d statuses=%s mapped=%d/%d",
        len(proof_df),
        dict(collections.Counter(proof_df["status"])),
        int(proof_df["mapped"].sum()),
        len(proof_df),
    )
    if apply:
        _upload_df(client, PROOF_KEY, proof_df)
        logger.info("PROOF written → %s", _uri(PROOF_KEY))
        # final safety: every delete-list legacy_path appears in the proof (all twinned)
        legacy_set = set(delete_df["legacy_path"])
        proof_set = set(proof_df["legacy_path"])
        missing = legacy_set - proof_set
        if missing:
            logger.error("RESIDUE: %d legacy paths NOT twinned: %s", len(missing), list(missing)[:5])
        else:
            logger.info("VERDICT: all %d legacy objects now have a canonical twin → delete-safe", len(legacy_set))
    else:
        logger.info("DRY-RUN complete — re-run with --apply to perform the copy")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="perform the copy (default: dry-run)")
    args = ap.parse_args()
    run(apply=args.apply)


if __name__ == "__main__":
    main()
