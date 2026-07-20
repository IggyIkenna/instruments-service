"""Unit tests — the ``--shard-of/--shard-index`` PROCESS partition of
``canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py``.

Why this is worth a test: the per-day sweep runs with ``--apply``, so each file is
read-modify-written in place. An N-process fan-out is only safe if the partition is
**disjoint** (no path in two shards — otherwise two processes race the same object's
rewrite and one silently clobbers the other) AND **exhaustive** (no path in zero shards
— otherwise the sweep reports success having silently skipped files, exactly the
"looks complete but isn't" class the honest-absence rules exist to prevent).

Sharding was added 2026-07-20 after the in-region VM measured 130% of 1600% CPU
(86% idle, ~1.0-1.6 files/s): the per-row canonicalization is pure Python and
GIL-bound, so ``--workers`` alone cannot use the box and only separate PROCESSES help.

No GCS — pure function, module-by-path load (mirrors test_build_instrument_catalogue.py).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def canon() -> ModuleType:
    return _load_script_module(
        "canonicalize_tradfi_catalogue_usd_lin_2026_07_18.py",
        "_canonicalize_tradfi_catalogue_test_module",
    )


def _corpus(n: int) -> list[str]:
    """A stand-in for the real listing: distinct day/venue instruments.parquet paths."""
    return [
        f"instrument_availability/by_date/day=2026-01-{i % 28 + 1:02d}/venue=V{i}/instruments.parquet" for i in range(n)
    ]


# The real corpus size measured 2026-07-20 (27,100 files over 2,638 day partitions), plus
# edge shapes: fewer files than shards, exactly one shard, a prime count that divides unevenly.
@pytest.mark.parametrize("n_files", [27_100, 0, 1, 7, 100, 1_693])
@pytest.mark.parametrize("shard_of", [1, 2, 3, 16, 20])
def test_partition_is_disjoint_and_exhaustive(canon: ModuleType, n_files: int, shard_of: int) -> None:
    """Every path lands in EXACTLY one shard — the safety property for a concurrent --apply."""
    files = _corpus(n_files)

    shards = [canon.shard_slice(files, shard_of=shard_of, shard_index=i) for i in range(shard_of)]
    flat = [p for shard in shards for p in shard]

    # Exhaustive: nothing dropped.
    assert sorted(flat) == sorted(files), "partition lost or invented paths"
    # Disjoint: nothing duplicated across shards.
    assert len(flat) == len(set(flat)) == len(files), "a path appears in more than one shard"
    # Union is exactly the input set.
    assert set(flat) == set(files)


@pytest.mark.parametrize("shard_of", [2, 3, 16, 20])
def test_shards_are_balanced(canon: ModuleType, shard_of: int) -> None:
    """Stride partition keeps shard sizes within 1 — no straggler shard doing all the work."""
    files = _corpus(27_100)
    sizes = [len(canon.shard_slice(files, shard_of=shard_of, shard_index=i)) for i in range(shard_of)]
    assert max(sizes) - min(sizes) <= 1, f"unbalanced shard sizes: {sizes}"
    assert sum(sizes) == 27_100


def test_unsharded_is_identity(canon: ModuleType) -> None:
    """shard_of=1 must return the whole list unchanged (the default, unsharded path)."""
    files = _corpus(500)
    assert canon.shard_slice(files, shard_of=1, shard_index=0) == files


def test_partition_is_deterministic(canon: ModuleType) -> None:
    """Re-running the same shard yields the same slice — a relaunch after a SPOT
    preemption resumes the SAME work, and never silently swaps to another shard's files."""
    files = _corpus(1_000)
    first = canon.shard_slice(files, shard_of=16, shard_index=5)
    second = canon.shard_slice(files, shard_of=16, shard_index=5)
    assert first == second


@pytest.mark.parametrize(
    ("shard_of", "shard_index"),
    [(0, 0), (-1, 0), (4, 4), (4, 5), (4, -1)],
)
def test_invalid_shard_args_rejected(canon: ModuleType, shard_of: int, shard_index: int) -> None:
    """An out-of-range shard must raise, never silently return an empty slice — a silent
    empty slice would let a fan-out 'succeed' while a subset of the corpus was never swept."""
    with pytest.raises(ValueError):
        canon.shard_slice(_corpus(10), shard_of=shard_of, shard_index=shard_index)
