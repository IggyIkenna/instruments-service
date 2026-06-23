#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after cleanup confirmed + GCS orphan-sweep = 0
"""cleanup_legacy_twins.py - verified-delete of legacy duplicate objects (CF-21 / G4.5).

The migration is **copy-not-move** (rebuild dedup + migrate-before-rebuild), so after a
green G4 ``--apply`` the legacy-shape objects (orphan-sweep class B) still exist and must
be cleaned. This is where the operator's "only delete what's in the manifest" instinct is
exactly right - and where a careless ``gsutil rm`` would be catastrophic. This tool makes
the delete **genetically safe, not trust-based**.

> **A legacy object is eligible for deletion ⟺ ALL of:**
> 1. its **canonical twin** is in the post-apply ``_index`` (``capture_status=captured``)
>    for the same cell - established by the orphan sweep labelling it class (B); **and**
> 2. ``crc32c(legacy) == crc32c(canonical_twin)`` - byte-for-byte identical (NOT merely
>    name-mapped); **and**
> 3. the legacy URI is itself a LEGACY shape (no ``pipeline_mode=batch_<source>/`` key) -
>    never delete a canonical object.
>
> Anything failing (1)-(3) is **NOT** a class-B duplicate → it is left untouched (it
> routes back to the sweep's class (D)/(E) and is never deleted by this gate). Class
> (C)/(C2)/(E) are never candidates here.

Input: the orphan-sweep report parquet (``migration_orphan_sweep.py --report-out``), which
carries the class-B legacy objects. The canonical twin is resolved from the manifest's
canonical source for that cell; both objects' ``crc32c`` are fetched per-object via
``get_blob_metadata`` (the list-walk does not populate crc32c).

Default ``--dry-run`` (lists deletable + the gate verdict per object, deletes nothing).
``--apply`` is **operator-gated like G4** and additionally requires ``--i-understand`` -
it is the only mode that deletes. Post-apply, re-run the orphan sweep (class-E must still
be 0). SSOT: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V6/CF-21.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from dataclasses import dataclass

from unified_api_contracts import ShardKey

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeleteVerdict:
    """The per-object verified-delete verdict (the 'genetic' gate result)."""

    legacy_uri: str
    canonical_uri: str
    deletable: bool
    reason: str


def is_deletable(
    *,
    legacy_is_canonical_shape: bool,
    cell_captured_in_manifest: bool,
    legacy_crc32c: str,
    canonical_crc32c: str,
) -> tuple[bool, str]:
    """The PURE verified-delete gate (the operator's 'genetic' guarantee).

    Deletable ⟺ the object is a LEGACY shape (not canonical), its cell IS captured in the
    manifest, and its crc32c equals the canonical twin's crc32c (byte-identical). Any other
    state → NOT deletable, with the reason (so a near-miss is never silently dropped)."""
    if legacy_is_canonical_shape:
        return False, "object is the CANONICAL shape - never delete a canonical object"
    if not cell_captured_in_manifest:
        return False, "canonical twin NOT captured in manifest - would delete the only copy"
    if not legacy_crc32c or not canonical_crc32c:
        return False, "missing crc32c on legacy or canonical - cannot prove byte-identity"
    if legacy_crc32c != canonical_crc32c:
        return False, "crc32c MISMATCH - content differs (NOT a true duplicate; route to class D/E)"
    return True, "crc32c-identical canonical twin is in the manifest - safe to delete the legacy copy"


def canonical_twin_path(legacy_object_path: str, canonical_source: str) -> str:
    """Reconstruct the canonical-shape GCS object path for a legacy object's cell.

    Inserts the ``pipeline_mode=batch_<source>/`` key (the canonical v9 shape) for the
    cell's ``canonical_source`` (read from the manifest) and strips any legacy hive
    artifacts (``category=`` → ``asset_group=``, the combined ``venue=PROTOCOL-CHAIN``).
    The leaf filename is preserved (the migration is copy-not-move, so the canonical twin
    carries the same filename)."""
    parts = legacy_object_path.split("/")
    rebuilt: list[str] = []
    has_pipeline_mode = any(p.startswith("pipeline_mode=") for p in parts)
    for part in parts:
        if part.startswith("category="):
            part = part.replace("category=", "asset_group=", 1)
        rebuilt.append(part)
        # Insert pipeline_mode= immediately AFTER the day= key (canonical position: LEFT
        # of asset_group=), only if the legacy path lacked it.
        if part.startswith("day=") and not has_pipeline_mode:
            rebuilt.append(f"pipeline_mode=batch_{canonical_source}")
    return "/".join(rebuilt)


@dataclass(frozen=True)
class LegacyTwin:
    """A class-B legacy object + its cell, from the orphan-sweep report."""

    uri: str
    venue: str
    chain: str
    instrument_type: str
    data_type: str
    day: str


def _is_canonical_shape_uri(uri: str) -> bool:
    return "/pipeline_mode=batch_" in uri or "/pipeline_mode=live_" in uri or "/pipeline_mode=replay_" in uri


def load_legacy_twins(report_uri: str, bucket: str) -> list[LegacyTwin]:
    """Read the orphan-sweep report parquet and return the class-B legacy objects."""
    import pandas as pd
    from unified_trading_library import get_storage_client

    raw = get_storage_client().download_bytes(bucket, report_uri)
    df = pd.read_parquet(io.BytesIO(raw))
    df = df[df["obj_class"] == "B_legacy_duplicate"]
    return [
        LegacyTwin(
            uri=str(r["uri"]),
            venue=str(r["venue"]),
            chain=str(r["chain"]),
            instrument_type=str(r["instrument_type"]),
            data_type=str(r["data_type"]),
            day=str(r["day"]),
        )
        for r in df.to_dict("records")
    ]


def _canonical_source_for_cell(
    source_by_cell: dict[tuple[str, str, str, str, str], str],
    twin: LegacyTwin,
) -> str:
    cell = (
        twin.venue.upper(),
        twin.chain.upper(),
        twin.instrument_type.lower(),
        twin.data_type.lower(),
        twin.day,
    )
    return source_by_cell.get(cell, "")


def verify_twins(
    twins: list[LegacyTwin],
    bucket: str,
    source_by_cell: dict[tuple[str, str, str, str, str], str],
) -> list[DeleteVerdict]:
    """Gate each legacy twin: reconstruct its canonical path, fetch both crc32c, apply the
    pure :func:`is_deletable` gate. Pure-gate-driven; the only I/O is the per-object
    crc32c fetch."""
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    verdicts: list[DeleteVerdict] = []
    for twin in twins:
        legacy_path = twin.uri.split("/", 3)[-1] if twin.uri.startswith("gs://") else twin.uri
        source = _canonical_source_for_cell(source_by_cell, twin)
        cell_captured = bool(source)
        canonical_path = canonical_twin_path(legacy_path, source) if source else ""
        legacy_md = client.get_blob_metadata(bucket, legacy_path)
        canonical_md = client.get_blob_metadata(bucket, canonical_path) if canonical_path else None
        legacy_crc = str(getattr(legacy_md, "crc32c", "") or "") if legacy_md else ""
        canonical_crc = str(getattr(canonical_md, "crc32c", "") or "") if canonical_md else ""
        deletable, reason = is_deletable(
            legacy_is_canonical_shape=_is_canonical_shape_uri(twin.uri),
            cell_captured_in_manifest=cell_captured and canonical_md is not None,
            legacy_crc32c=legacy_crc,
            canonical_crc32c=canonical_crc,
        )
        verdicts.append(
            DeleteVerdict(
                legacy_uri=twin.uri,
                canonical_uri=f"gs://{bucket}/{canonical_path}" if canonical_path else "",
                deletable=deletable,
                reason=reason,
            )
        )
    return verdicts


def _source_by_cell_from_manifest(bucket: str) -> dict[tuple[str, str, str, str, str], str]:
    """Build ``cell → canonical source`` from the captured manifest rows."""
    import pandas as pd
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    raw = client.download_bytes(bucket, "_index/availability_index.parquet")
    df = pd.read_parquet(io.BytesIO(raw))
    out: dict[tuple[str, str, str, str, str], str] = {}
    for r in df.to_dict("records"):
        if str(r.get("capture_status", "")).lower() != "captured":
            continue
        key = ShardKey(
            asset_group=str(r.get("asset_group", "")),
            venue=str(r.get("venue", "") or ""),
            chain=str(r.get("chain", "") or ""),
            instrument_type=str(r.get("instrument_type", "") or ""),
            data_type=str(r.get("data_type", "") or ""),
        )
        cell = (
            key.venue.upper(),
            key.chain.upper(),
            key.instrument_type.lower(),
            key.data_type.lower(),
            str(r.get("date", "") or ""),
        )
        out[cell] = str(r.get("source", "") or "")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verified-delete of legacy duplicate objects (CF-21 / G4.5).")
    parser.add_argument("--asset-group", required=True, choices=["cefi", "defi", "tradfi", "sports", "prediction"])
    parser.add_argument("--report-uri", required=True, help="orphan-sweep report parquet path within the bucket")
    parser.add_argument("--cloud", choices=["gcp", "aws"], default="gcp")
    parser.add_argument("--apply", action="store_true", help="DELETE the verified twins (operator-gated)")
    parser.add_argument("--i-understand", action="store_true", help="required alongside --apply to actually delete")
    args = parser.parse_args(argv)

    from migration_orphan_sweep import _resolve_bucket  # reuse the bucket SSOT resolver

    bucket = _resolve_bucket(args.asset_group, args.cloud)
    twins = load_legacy_twins(args.report_uri, bucket)
    logger.info("cleanup %s: %d class-B legacy twins from %s", args.asset_group, len(twins), args.report_uri)
    source_by_cell = _source_by_cell_from_manifest(bucket)
    verdicts = verify_twins(twins, bucket, source_by_cell)

    deletable = [v for v in verdicts if v.deletable]
    blocked = [v for v in verdicts if not v.deletable]
    logger.info("=== CF-21 verified-delete: %d deletable, %d blocked ===", len(deletable), len(blocked))
    for v in blocked[:25]:
        logger.warning("  BLOCKED %s - %s", v.legacy_uri, v.reason)

    if not (args.apply and args.i_understand):
        logger.info("DRY-RUN - nothing deleted. Re-run with --apply --i-understand (operator-gated) to delete.")
        return 0

    from unified_trading_library import get_storage_client

    client = get_storage_client()
    n_deleted = 0
    for v in deletable:
        legacy_path = v.legacy_uri.split("/", 3)[-1]
        if client.delete_blob(bucket, legacy_path):
            n_deleted += 1
    logger.info("CF-21 verified-delete APPLIED: deleted %d crc32c-identical legacy twins", n_deleted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
