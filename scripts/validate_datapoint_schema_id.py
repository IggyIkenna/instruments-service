#!/usr/bin/env python3
# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
"""Tier-2 per-datapoint id-canonical (G2) + schema (G3) validator.

THE one sanctioned single walk of the reconciliation two-tier compute model
(`codex/02-data/reconciliation-census-and-compute-tiers.md` §§ 2, 3). Runs on a
SPOT VM (`launch-datapoint-validation-vm.sh`); enumerates the market-data corpus
for ONE asset_group EXACTLY ONCE and, per parquet, in order:

  * **G2 — id-canonical** (`§ 2.1`): rebuild the instrument id from the row's own
    structured axes + ``lookup_contract().symbol_column`` via the UAC SSOT builder
    ``build_canonical_instrument_id`` (the builder-as-judge, NEVER a regex) and
    byte-compare to the stored id → ``non_canonical_id`` on mismatch. Legitimately
    id-less shapes (pattern-#2 ``options_chain`` / ``futures_chain`` bundles, the
    symbol-less ``ticks.parquet`` fan-in, the prediction CQG filename-id bundle,
    and sports fixtures — event-keyed, no TYPE/SYMBOL) emit NO id finding. DeFi
    pools compare against ``canonical_instrument_id`` (AE-3: pool ``instrument_id``
    != ``canonical_instrument_id`` is the intentional two-id model, not a finding).

  * **G3 — schema** (`§ 2.2`): resolve the ``SchemaContract`` via ``lookup_contract``
    and run the read-side ``validate_dataframe`` → ``shard_pillar_fail`` on any
    violation. The tz-aware-UTC pillar falls out of ``wrong_dtype`` (the contract
    dtype is the ``datetime64[ns, UTC]`` literal). The placeholder-that-looks-
    populated pillar (``masked_empty_row``) is checked explicitly here — a 0-row /
    all-NaN parquet is added to the failure codes.

``lookup_contract`` already case-normalises (contracts.py:1121-1182), so the
writer's on-disk vocabulary (lowercase ``solana_amm_pool`` / uppercase ``POOL``)
resolves either way — the vocabulary-slip guard.

Results: ONE row per validated object written as a per-VM shard to the flat
``datapoint-validation`` results bucket at ``_index/per_vm/{VM_NAME}.parquet``
(the ManifestWriter per-VM convention, ``_state.py`` ``_PER_VM_PATH_TEMPLATE``),
so the fleet write-progress monitor keys on results-row growth and the standard
consolidator / the reconciliation skill read the shards back. ``asset_group`` and
``campaign_id`` are carried as COLUMNS (the flat bucket holds every AG/campaign);
the skill filters on them.

Idempotent by presence-skip: on (re)start the validator reads every existing
per-VM shard for the campaign and skips objects already carrying a results row,
so a SPOT-preemption relaunch resumes without ``--force`` (the force-PAGE guard
never fires). Resume is fully presence-skip driven — the flat results shards ARE
the checkpoint: the per-VM shard is flushed on each day-frontier advance so the
fleet write-progress monitor observes growing results-row counts (no separate
``PROGRESS.json`` marker, which would require a non-public UTL import).

CLI (workspace ``--operation``/``--mode``/``--asset-group`` convention):
    python validate_datapoint_schema_id.py --asset-group defi \
        --campaign-id 20260721-101500 --vm-name datapoint-validation-defi-20260721-101500
"""

from __future__ import annotations

import argparse
import io
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

import pandas as pd
from unified_api_contracts import (
    InstrumentType,
    build_canonical_instrument_id,
    lookup_contract,
    validate_dataframe,
)
from unified_trading_library import (
    StorageClient,
    get_storage_client,
    resolve_bucket_name,
)

logger = logging.getLogger("validate_datapoint_schema_id")

# Per-VM shard dir — MUST match the UTL ManifestWriter convention
# (`manifest_writer/_state.py` `_PER_VM_PATH_TEMPLATE`) so the fleet
# write-progress monitor + consolidator key on the same prefix.
_PER_VM_DIR = "_index/per_vm/"

# Objects that are NOT per-datapoint validation targets (sidecars / non-data).
_SKIP_PATH_MARKERS = (
    "_index/",
    "_migration_backups/",
    "_migrated_",
    "_quarantine/",
    "_content_repair/",
    "_needs_attribution",
    ".bak",
)

# data_types whose parquet legitimately carries no per-row instrument id
# (pattern-#2 bundles + the symbol-less tick fan-in). id-form is NOT-APPLICABLE.
_ID_LESS_DATA_TYPES = frozenset({"options_chain", "futures_chain"})
_ID_LESS_FILENAMES = frozenset({"ticks.parquet"})

# How many validated objects to buffer before rewriting the per-VM shard, so the
# fleet monitor observes a GROWING results-row count (not one write at the end).
_DEFAULT_FLUSH_EVERY = 500

_Record = dict[str, object]

# Partition axes seeded on every parsed path (absent segment → "" is the
# meaningful not-present value, e.g. `chain` off cefi/tradfi) so downstream reads
# are a direct index, not a `.get(key, "")` empty-string fallback.
_KNOWN_AXES = ("day", "pipeline_mode", "asset_group", "venue", "chain", "instrument_type", "data_type", "source")


@dataclass(frozen=True)
class _IdFields:
    """Typed id-relevant columns the UAC builder consumes for one row."""

    symbol: str
    quote_asset: str
    margin_type: str
    option_right: Literal["C", "P"] | None
    strike: Decimal | None
    expiry_date: date | None


def _parse_axes_from_path(object_path: str) -> dict[str, str]:
    """Extract ``key=value`` partition segments from a canonical GCS object path.

    Generic over asset-group grammars: splits on ``/`` and reads every
    ``key=value`` node. Missing keys resolve to ``""`` (e.g. ``chain`` off defi).
    """
    axes: dict[str, str] = dict.fromkeys(_KNOWN_AXES, "")
    for segment in object_path.split("/"):
        key, sep, value = segment.partition("=")
        if sep:
            axes[key] = value
    return axes


def _should_skip_object(object_path: str) -> bool:
    """True for non-parquet objects and manifest/migration/quarantine sidecars."""
    if not object_path.endswith(".parquet"):
        return True
    return any(marker in object_path for marker in _SKIP_PATH_MARKERS)


def _to_instrument_type(raw: str) -> InstrumentType | None:
    """Map a path ``instrument_type=`` value to the enum, or None if unmappable."""
    if not raw:
        return None
    try:
        return InstrumentType(raw.lower())
    except ValueError:
        return None


def _cell_str(rec: _Record, column: str) -> str:
    """Return ``str(rec[column])`` when present + non-null/non-blank, else ``""``."""
    value = rec.get(column)
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "<na>"} else text


def _coerce_date(value: object) -> date | None:
    """Coerce a cell to a ``date`` for the builder; None on any failure/blank."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        ts = pd.Timestamp(cast("str", value))
    except (ValueError, TypeError):
        return None
    return None if pd.isna(ts) else ts.date()


def _extract_id_fields(rec: _Record, symbol_column: str) -> _IdFields:
    """Pull the id-relevant columns the UAC builder consumes from one row record.

    Best-effort per field — the stored id was built from these SAME columns, so a
    faithful extraction reproduces it; a genuine divergence is the finding.
    """
    right = _cell_str(rec, "option_right")
    option_right: Literal["C", "P"] | None = "C" if right == "C" else "P" if right == "P" else None
    strike: Decimal | None = None
    strike_str = _cell_str(rec, "strike")
    if strike_str:
        try:
            strike = Decimal(strike_str)
        except (InvalidOperation, ValueError):
            strike = None
    return _IdFields(
        symbol=_cell_str(rec, symbol_column),
        quote_asset=_cell_str(rec, "quote_asset"),
        margin_type=_cell_str(rec, "margin_type"),
        option_right=option_right,
        strike=strike,
        expiry_date=_coerce_date(rec.get("expiry_date")),
    )


def _schema_verdict(df: pd.DataFrame, asset_group: str, itype: str, dtype: str, venue: str) -> tuple[str, list[str]]:
    """G3 — resolve the contract and validate ``df``; add the masked-empty pillar."""
    codes: list[str] = []
    # Placeholder-that-looks-populated (`§ 2.2` pillar #5 / masked_empty_row): a
    # captured object over a 0-row or all-NaN parquet. Content-grain expression of
    # the honest-absence rule — checked here since validate_dataframe cannot see it.
    if len(df) == 0 or bool(df.isna().all().all()):
        codes.append("masked_empty_row")
    try:
        contract = lookup_contract(asset_group=asset_group, instrument_type=itype, data_type=dtype, venue=venue or None)
    except Exception as exc:  # no contract for this shard: report, never crash the walk
        logger.debug("no contract for (%s,%s,%s): %s", asset_group, itype, dtype, exc)
        return ("NO_CONTRACT", codes)
    codes.extend(sorted({v.kind for v in validate_dataframe(df, contract)}))
    verdict = "FAIL" if codes else "PASS"
    return (verdict, codes)


def _id_verdict(df: pd.DataFrame, asset_group: str, axes: dict[str, str], object_path: str) -> tuple[str, list[str]]:
    """G2 — rebuild each row's id via the SSOT builder and byte-compare to stored.

    Returns ``("NOT_APPLICABLE", [])`` for id-less shapes, ``("PASS", [])`` when
    every row matches, or ``("non_canonical_id", [samples])`` on any mismatch /
    builder error.
    """
    dtype = axes["data_type"]
    itype_raw = axes["instrument_type"]
    filename = object_path.rsplit("/", 1)[-1]
    if asset_group == "sports" or dtype in _ID_LESS_DATA_TYPES or filename in _ID_LESS_FILENAMES:
        return ("NOT_APPLICABLE", [])

    itype = _to_instrument_type(itype_raw)
    if itype is None:
        return ("NOT_APPLICABLE", [])  # vocabulary belongs to the census, not G2

    # AE-3: defi compares the CANONICAL id column (pool instrument_id may differ by design).
    target_col = (
        "canonical_instrument_id"
        if asset_group == "defi" and "canonical_instrument_id" in df.columns
        else "instrument_id"
    )
    if target_col not in df.columns:
        return ("NOT_APPLICABLE", [])

    try:
        contract = lookup_contract(
            asset_group=asset_group, instrument_type=itype_raw, data_type=dtype, venue=axes["venue"] or None
        )
    except Exception:  # no contract → cannot resolve symbol_column; G3 reports NO_CONTRACT
        return ("NO_CONTRACT", [])

    venue = axes["venue"]
    chain = axes["chain"] or None
    mismatches: list[str] = []
    for rec in cast("list[_Record]", df.to_dict(orient="records")):
        stored = _cell_str(rec, target_col)
        if not stored:
            continue  # null id row (pattern-#2 bundle member) — NOT-APPLICABLE per-row
        fields = _extract_id_fields(rec, contract.symbol_column)
        try:
            rebuilt = build_canonical_instrument_id(
                asset_group=asset_group,
                venue=venue,
                instrument_type=itype,
                symbol=fields.symbol,
                chain=chain,
                quote_asset=fields.quote_asset,
                margin_type=fields.margin_type,
                option_right=fields.option_right,
                strike=fields.strike,
                expiry_date=fields.expiry_date,
            )
        except (ValueError, TypeError) as exc:
            mismatches.append(f"builder_error:{stored}:{exc}")
        else:
            if rebuilt != stored:
                mismatches.append(f"{stored}!={rebuilt}")
        if len(mismatches) >= 5:  # bounded sample — presence of the finding, not every row
            break
    return ("non_canonical_id", mismatches) if mismatches else ("PASS", [])


def _validate_one(client: StorageClient, data_bucket: str, object_path: str, campaign_id: str, vm_name: str) -> _Record:
    """Validate a single parquet object; return its results row (never raises)."""
    axes = _parse_axes_from_path(object_path)
    row: _Record = {
        "asset_group": axes["asset_group"],
        "venue": axes["venue"],
        "chain": axes["chain"],
        "instrument_type": axes["instrument_type"],
        "data_type": axes["data_type"],
        "day": axes["day"],
        "object_path": object_path,
        "file_generation": "",
        "campaign_id": campaign_id,
        "vm_name": vm_name,
        "validated_at": datetime.now(UTC).isoformat(),
    }
    try:
        raw = client.download_bytes(data_bucket, object_path)
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:  # shard-level failure isolation: record + continue the walk
        logger.warning("read failed %s: %s", object_path, exc)
        row.update(
            row_count=0,
            schema_verdict="READ_ERROR",
            schema_failure_codes=str(exc)[:200],
            id_verdict="READ_ERROR",
            id_failure_codes="",
        )
        return row

    ag = str(row["asset_group"])
    schema_verdict, schema_codes = _schema_verdict(
        df, ag, str(row["instrument_type"]), str(row["data_type"]), str(row["venue"])
    )
    id_verdict, id_codes = _id_verdict(df, ag, axes, object_path)
    row.update(
        row_count=len(df),
        schema_verdict=schema_verdict,
        schema_failure_codes=",".join(schema_codes),
        id_verdict=id_verdict,
        id_failure_codes=",".join(id_codes),
    )
    return row


def _read_shard_rows(client: StorageClient, results_bucket: str, blob_name: str) -> list[_Record]:
    """Download one results shard → list of row records (empty on any failure)."""
    try:
        raw = client.download_bytes(results_bucket, blob_name)
        return cast("list[_Record]", pd.read_parquet(io.BytesIO(raw)).to_dict(orient="records"))
    except Exception as exc:  # a missing/corrupt shard just means no prior rows
        logger.debug("shard read skipped %s: %s", blob_name, exc)
        return []


def _row_field(rec: _Record, key: str) -> str:
    """Read a results-shard field as str (own schema; absent field = not-present)."""
    value = rec.get(key)
    return str(value) if value is not None else ""


def _load_prior(
    client: StorageClient, results_bucket: str, campaign_id: str, vm_name: str
) -> tuple[set[str], list[_Record]]:
    """Return (already-validated object_paths for the campaign, this VM's own rows).

    Presence-skip idempotency: every per-VM shard in the campaign contributes to
    the skip set; only THIS VM's rows are re-buffered so a flush never drops them.
    """
    done: set[str] = set()
    own_rows: list[_Record] = []
    own_blob = f"{_PER_VM_DIR}{vm_name}.parquet"
    for blob in client.list_blobs(results_bucket, prefix=_PER_VM_DIR):
        if not blob.name.endswith(".parquet"):
            continue
        rows = _read_shard_rows(client, results_bucket, blob.name)
        for r in rows:
            if _row_field(r, "campaign_id") == campaign_id:
                done.add(_row_field(r, "object_path"))
        if blob.name == own_blob:
            own_rows = [r for r in rows if _row_field(r, "campaign_id") == campaign_id]
    return (done, own_rows)


def _flush_shard(client: StorageClient, results_bucket: str, vm_name: str, rows: list[_Record]) -> None:
    """Rewrite this VM's per-VM results shard with the full accumulated rows."""
    if not rows:
        return
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, index=False, engine="pyarrow", compression="zstd")
    buffer.seek(0)
    client.upload_from_file_obj(
        results_bucket, f"{_PER_VM_DIR}{vm_name}.parquet", buffer, content_type="application/octet-stream"
    )


def _run(client: StorageClient, args: argparse.Namespace) -> int:
    asset_group = str(args.asset_group)
    campaign_id = str(args.campaign_id)
    vm_name = str(args.vm_name)
    walk_prefix = str(args.walk_prefix)
    flush_every = int(args.flush_every)
    limit = int(args.limit)

    data_bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=asset_group)
    results_bucket = resolve_bucket_name(cloud="gcp", kind="datapoint-validation")
    logger.info("Tier-2 validate: ag=%s corpus=%s results=%s", asset_group, data_bucket, results_bucket)

    done, rows = _load_prior(client, results_bucket, campaign_id, vm_name)
    logger.info("resume: %d objects already validated for campaign=%s", len(done), campaign_id)

    validated = 0
    last_day = ""
    # THE single walk — one enumeration of the corpus for this asset_group.
    for blob in client.list_blobs(data_bucket, prefix=walk_prefix):
        object_path = blob.name
        if _should_skip_object(object_path) or object_path in done:
            continue
        rows.append(_validate_one(client, data_bucket, object_path, campaign_id, vm_name))
        done.add(object_path)
        validated += 1

        day = _parse_axes_from_path(object_path)["day"]
        if day and day != last_day:
            last_day = day
            # Day-frontier advance: flush so results are durable + the fleet
            # write-progress monitor observes a growing per-VM shard. Resume is
            # presence-skip idempotent (a re-run skips already-validated objects),
            # so no separate PROGRESS.json day-checkpoint is required.
            _flush_shard(client, results_bucket, vm_name, rows)
            logger.info("day-frontier advanced to %s (%d validated)", day, validated)

        if validated % flush_every == 0:
            _flush_shard(client, results_bucket, vm_name, rows)
            logger.info("progress: %d validated (flushed shard)", validated)
        if limit and validated >= limit:
            logger.info("--limit %d reached — stopping the walk (canary scope)", limit)
            break

    _flush_shard(client, results_bucket, vm_name, rows)
    logger.info("DONE: validated=%d total_rows_in_shard=%d", validated, len(rows))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-group", required=True, choices=["cefi", "defi", "tradfi", "sports", "prediction"])
    parser.add_argument("--campaign-id", required=True, help="Campaign timestamp id (columns filter for read-back).")
    parser.add_argument("--vm-name", required=True, help="Per-VM shard id — must equal VM_NAME (fleet monitor key).")
    parser.add_argument("--env", default="prod", choices=["prod", "staging", "dev"])
    parser.add_argument(
        "--walk-prefix", default="raw_tick_data/", help="Single-walk root prefix (default tick corpus)."
    )
    parser.add_argument("--flush-every", type=int, default=_DEFAULT_FLUSH_EVERY)
    parser.add_argument("--limit", type=int, default=0, help="Stop after N objects (0 = whole corpus; canary scope).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = get_storage_client()
    return _run(client, args)


if __name__ == "__main__":
    raise SystemExit(main())
