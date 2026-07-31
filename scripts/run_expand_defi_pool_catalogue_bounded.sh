#!/usr/bin/env bash
# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md
#   is resolved and every default DEX protocol's catalogue population has been verified
#   against the historical capture corpus.
#
# run_expand_defi_pool_catalogue_bounded.sh — bounded-by-construction wrapper for
# expand_defi_pool_catalogue_from_manifest_2026_07_31.py's remaining runs (the other 11
# default DEX protocols; todo 1 of
# issues/defi_dex_pools_catalogue_undercoverage_vs_historical_capture_2026_07_28.md).
#
# Why: the script's own column-pruning fix bounds its peak RSS BY MEASUREMENT (one
# observed ~9.5GiB run — see
# issues/expand_defi_pool_catalogue_script_unbounded_memory_2026_07_31.md), but nothing
# ENFORCES a ceiling. Per
# codex/05-infrastructure/vm-launcher-runbook.md § "Heavy COMPUTE/MEMORY on the shared
# planning-vm", a materialization this size run directly on the shared planning-vm must
# be bounded-by-construction, not just bounded-by-observation. This wraps the script
# under unified-trading-pm's run-bounded-analysis.sh cgroup/ulimit memory cap (a sibling
# repo clone in the same slot worktree — see RULES.md § 3's "sibling repo" convention).
#
# Usage:
#   bash scripts/run_expand_defi_pool_catalogue_bounded.sh [--apply] [--project-id ID]
#   ANALYSIS_MEM_CAP=16G bash scripts/run_expand_defi_pool_catalogue_bounded.sh --apply
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BOUNDED_RUNNER="$REPO_ROOT/../unified-trading-pm/scripts/dev/run-bounded-analysis.sh"

if [[ ! -f "$BOUNDED_RUNNER" ]]; then
    echo "run_expand_defi_pool_catalogue_bounded: unified-trading-pm sibling clone not found at $BOUNDED_RUNNER" >&2
    echo "  (expected a sibling repo checkout in the same slot worktree, e.g. .tabs/<slot>/unified-trading-pm)" >&2
    exit 1
fi

# The wrapper's own generic default (4G) targets an unbounded ad-hoc scratchpad script;
# this script is a real, bounded workload with a documented ~9.5GiB observed peak, so the
# default here leaves real headroom above that measurement instead of reusing 4G, which
# this script's legitimate run would exceed and get SIGKILLed for no reason.
MEM_CAP="${ANALYSIS_MEM_CAP:-12G}"

exec bash "$BOUNDED_RUNNER" --mem-cap "$MEM_CAP" -- \
    python3 "$REPO_ROOT/scripts/expand_defi_pool_catalogue_from_manifest_2026_07_31.py" "$@"
