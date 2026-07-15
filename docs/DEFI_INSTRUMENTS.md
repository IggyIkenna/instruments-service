# DeFi Instruments

> Consolidates the former `DEFI_INSTRUMENTS.md` + `specs/DEFI_GUIDE.md` (near-duplicates) + the DeFi slices of
> `specs/MVP_INSTRUMENTS.md` and `specs/INSTRUMENT_SPECIFICATION.md`. Covers DEX pools, lending, yield-bearing/LST, and
> the 5 on-chain-perp DEXes. CeFi/TradFi/Prediction/Sports instruments live in their own consolidated docs; the general
> `VENUE:TYPE:PAYLOAD[@CHAIN]` grammar and shared instrument-type vocabulary live in the instrument-ID spec doc — this
> doc covers DeFi-specific payload shapes, chain handling, and the real adapter registry only.
>
> Live drilldown mockup (DeFi tab, current-vs-target-canonical samples): https://claude.ai/code/artifact/e2824e52-3a51-43e0-b4b1-933bee469f9d

## Scope and key distinction

Instruments-service fetches **instrument definitions** for DeFi protocols — pool/vault/market addresses, token
addresses, fee tiers — not market data. Raw per-swap events, supply/borrow rates, oracle prices, DEX OHLCV, staking
yields, and gas costs are fetched by market-tick-data-service (MTDS) against the instrument list this service
produces — see "Downstream: MTDS integration" below for the real (not derived-from-raw) OHLCV relationship and the
batch-vs-live swap-capture split.

**In scope for this doc**:

- **DEX pools**: Uniswap V2/V3/V4, Balancer, Curve, plus 7 Uniswap-V3-fork/Messari-schema DEXes (PancakeSwap_V3,
  Sushiswap_V3/Sushiswap, Camelot_V3, Aerodrome_V3, TraderJoe_V2, Velodrome_V2) and GMX (perps-via-pool).
- **Lending**: Aave_V3, Spark, Compound_V3, Morpho, Euler_V2, Fluid, Radiant, Venus, Benqi.
- **Yield-bearing / LST / restaking**: Lido, EtherFi, Ethena, RocketPool, Renzo, KelpDAO, Puffer, Symbiotic, Karak,
  Convex, Idle, Yearn(\_V3), Beefy, Pendle, EigenLayer.
- **On-chain-perp DEXes**: Hyperliquid, Aster, Pacifica-Solana, Extended-Starknet, Lighter-Zksync.

**Out of scope** (covered in other consolidated docs / not yet written up anywhere): Solana-native AMMs and perps
(Drift, Raydium, Orca, Phoenix, Jupiter, Meteora, Lifinity, Kamino, Camino) and Solana
staking/restaking/lending (Marinade, Jito, JitoRestaking, Sanctum, Solblaze, Solana-native, MarginFi, Solend) — these
have a real adapter presence too (see `instruments_service/reference_data/adapters/defi/`) but are organizationally a
separate "Solana DeFi" surface from this doc's EVM-centric + on-chain-perp scope, per the 2026-07-08 docs-consolidation
task split. **MarginFi + Solend adapters shipped 2026-07-09** (see "Solana lending — MarginFi, Solend" under Lending
below for the brief real-data-source writeup; full per-chain tables for the Solana surface are still tracked as a
future doc split, not written up here).

---

## Instrument ID format: current state vs. decided target

Target canonical formats for several real format divergences have been decided (operator-decided, final) but not yet
migrated in production data. Full detail, decision rationale, and open todos:
`unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`; underlying audit:
`unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md`.

### DEX pools

- **Target format**: `VENUE-CHAIN:POOL:TOKEN0-TOKEN1[-FEE_TIER]`, fee tier a real Uniswap-V3-style basis-point value
  (100/500/3000/10000), dash-separated. `pool_address` is its own column, not the entire identity key.
- **Current adapter code**: all 13 DEX-pool protocols in scope (the 5 native adapter classes — Uniswap V2/V3/V4,
  Balancer, Curve — plus the 8 protocols that share `UniswapV3ReferenceDataAdapter` via `protocol_slug`, see "Adapter
  architecture" below) build a structured key — `instrument_key = f"{venue_tag}:POOL:{base}-{quote}[-{fee_bps}]"`
  (confirmed in `uniswap_v3.py`, and equivalently in `uniswap_v2.py`/`uniswap_v4.py`/`balancer.py`) — with the
  pool/market address kept separately as `raw_symbol`, not as the instrument_id. Curve uses its own REST-API-derived
  key shape (see the Curve row under "Protocol × chain coverage" below).
- **Fee-tier code gap — FIXED 2026-07-09** (`uniswap_v3.py`/`uniswap_v4.py::_build_pool_record`): the fee-tier
  segment was colon-separated (`:{fee_str}`) rather than dash-separated, and embedded Uniswap's raw on-wire `feeTier`
  value (e.g. `3000`) rather than the real basis-points value. Since the 8 fork/config-variant protocols share
  Uniswap V3's `_build_pool_record` code path, fixing it there fixes all 8 too; V4 (separate adapter class) fixed
  the same way. Now: `symbol = f"{base}-{quote}-{pool_fee_tier_bps}"` when a real fee tier exists, else
  `f"{base}-{quote}"` (the fee segment is OMITTED, not a fabricated `-0`, matching the target's `[-FEE_TIER]`
  optional-bracket grammar). Verified via `_build_pool_record` unit tests (264 passed,
  `tests/unit/test_defi_adapters_comprehensive.py` + `tests/unit/reference_data/adapters/defi/test_dex_metadata_population.py`).
  This fix affects `instrument_key`/`pool_fee_tier` on freshly-discovered rows and (once regenerated, see below)
  `glued_pair_id` — it does **not** touch `instrument_id` for POOL rows, which is a separate, deliberately-different
  field (next bullet).

- **RE-VERIFIED 2026-07-09, finding corrected — this is NOT a simple catalog-regeneration gap.** The
  2026-07-08 framing ("the persisted catalog predates the current adapter code... the fix is likely a catalog
  regeneration/backfill against the current adapter code, not a from-scratch code change") is **incomplete**: reading
  `scripts/build_instrument_catalogue.py::_defi_pool_dual_form()` directly shows the catalogue ROLLUP step (which
  runs strictly downstream of adapter discovery and is what actually writes `prod/catalog.parquet`) **unconditionally**
  sets `instrument_id = pool_address.lower()` for every DeFi POOL-typed row, via UAC's
  `DefiPoolIdentity.canonical_instrument_id` (`unified_api_contracts/canonical/crosscutting/defi.py:299-301`) — by
  design, not staleness. Re-running instrument discovery against the current (now-fixed) adapter code and re-rolling
  the catalogue would **still** write the bare pool address into `instrument_id`, because that decision is made in
  the rollup, not the adapter. This is **load-bearing, live, cross-repo**: `market-tick-data-service`'s
  `engine/defi_catalog_reader.py` reads `instrument_id` from this exact catalogue expecting `pool_address.lower()`
  for POOL rows (its own fallback deriver `_canonical_defi_id` independently recomputes the identical value) to
  build its expected-universe join for DEX swap/pool market data — flipping `instrument_id` to the structured form
  for POOL rows without a coordinated MTDS-side change would silently break that join for all 13 protocols. This is
  exactly the class of change the canonicalization doc's already-planned "ground-up migration (UAC →
  instruments-service → MTDS → strategy-service → deployment, live breakage explicitly authorized)" exists for — it
  is **not** achievable as an instruments-service-only regen, and needs an explicit operator go-ahead + an MTDS-side
  companion change shipped in lockstep, not a same-session smoke test. (Independently corroborated by
  `scripts/balancer_cross_chain_pool_address_collision_backfill_2026_07_08.py`'s own header, which already flagged
  the bare-`pool_address` `instrument_id` as "a known, already-documented architectural gap... that has not yet
  shipped as a catalog-wide migration" — this pass adds the root-cause mechanism and the MTDS blocking dependency.)

  **The good news**: the target structured form already exists TODAY, in a separate, already-populated column —
  `glued_pair_id` (0 nulls across all 6,352 real POOL rows for the 13 protocols, verified live against
  `prod/catalog.parquet` 2026-07-09) — built by the same `DefiPoolIdentity.glued_pair_id` property, documented as
  "the human-readable UI form." It has 3 remaining format bugs relative to the operator's exact target grammar,
  all cosmetic/string-level (no re-fetch needed to fix):
  1. Ghost/no-underscore venue-chain prefix — `UNISWAPV3-ARBITRUM` instead of `UNISWAP_V3-ARBITRUM` (UAC's
     `_strip_version_underscore()` deliberately builds this as "the glued-prefix display form"; the catalogue's own
     `venue` column is already correctly spelled with the underscore, so a rewrite can just use it directly instead
     of re-deriving from the ghost prefix).
  2. Colon before the fee-tier segment instead of dash (`:POOL:SOL-WETH:500` vs target `:POOL:SOL-WETH-5`).
  3. Raw on-wire feeTier units instead of bps for the Uniswap-V3-family + Uniswap V4 protocols (e.g. `10000` should
     be `100`) — same root cause as the just-fixed adapter bug, further baked in by
     `build_instrument_catalogue.py::_fee_from_instrument_key()` preferring the (buggy, pre-fix) legacy
     `instrument_key`'s raw fee token over the already-correct bps `pool_fee_tier` field.

  **WRITTEN BACK FOR REAL 2026-07-09** (was: smoke-tested dry-run only, same day). The dry-run smoke test (a pure
  in-place string rewrite of `glued_pair_id` using the catalogue's own `venue`/`chain` columns plus the existing
  `glued_pair_id`'s pair/fee segments) confirmed 100% (6,352/6,352) parse success with 0 grammar mismatches; the real
  write-back ran the identical transform for real via
  `scripts/dex_pool_glued_pair_id_canonicalize_2026_07_09.py --apply --confirm`: downloaded live
  `prod/catalog.parquet` (7,888 total rows, re-verified in-scope POOL count still 6,352 across the 13 protocols — no
  drift since the smoke test earlier the same day), wrote a full pre-write backup
  (`prod/catalog.20260709-112522.gluedpairfix.bak.parquet`), rewrote `glued_pair_id` for all 6,352 in-scope rows
  (6,332 actually changed, 20 already-canonical CURVE rows left byte-identical — idempotent no-op on those), and
  **independently re-downloaded the written blob from GCS afterward** to verify: row count unchanged (7,888),
  0 remaining ghost venue-prefixes, 0 remaining colon-before-fee delimiters, 6,352/6,352 in-scope `glued_pair_id`
  values match the target grammar (`VENUE-CHAIN:POOL:BASE-QUOTE[-FEE_BPS]`), and `instrument_id` confirmed
  byte-for-byte **unchanged** (still `pool_address.lower()`, 0 rows with a `:` in `instrument_id`) — the MTDS join
  key was deliberately not touched (see the migration-blocker note below). This is safe to ship independently of the
  `instrument_id`/MTDS question above — `glued_pair_id` has no external consumers today (grepped the full workspace:
  only `build_instrument_catalogue.py` and the Balancer collision backfill script read it; no UI or other service
  joins on it yet). Real per-protocol row counts + write-back results (2026-07-09):

  | Protocol              | Real POOL rows (2026-07-09) | `glued_pair_id` write-back result                                   |
  | --------------------- | --------------------------- | ------------------------------------------------------------------- |
  | BALANCER              | 2,423                       | 2,423/2,423 rewritten, verified                                     |
  | UNISWAP_V3            | 2,192                       | 2,192/2,192 rewritten, verified                                     |
  | PANCAKESWAP_V3        | 614                         | 614/614 rewritten, verified                                         |
  | UNISWAP_V4            | 413                         | 413/413 rewritten, verified                                         |
  | TRADER_JOE_V2         | 304                         | 304/304 rewritten, verified                                         |
  | SUSHISWAP_V3          | 122                         | 122/122 rewritten, verified                                         |
  | VELODROME_V2          | 96                          | 96/96 rewritten, verified                                           |
  | AERODROME_V3          | 76                          | 76/76 rewritten, verified                                           |
  | CAMELOT_V3            | 63                          | 63/63 rewritten, verified                                           |
  | UNISWAP_V2            | 24                          | 24/24 rewritten, verified (no fee segment — V2 has none)            |
  | CURVE                 | 20                          | 20/20 already-canonical, 0 changed (no fee segment — Curve none)    |
  | SUSHISWAP (legacy V2) | 4                           | 4/4 rewritten, verified                                             |
  | GMX                   | 1                           | 1/1 rewritten, verified                                             |
  | **Total**             | **6,352**                   | **6,352/6,352 verified canonical (100%), 6,332 changed / 20 no-op** |

  Example before → after (real rows, spot-checked post-write): `UNISWAPV3-ARBITRUM:POOL:SOL-WETH:500` →
  `UNISWAP_V3-ARBITRUM:POOL:SOL-WETH-5`; `BALANCER-ARBITRUM:POOL:DAI-USDT:30` (already-bps, colon-only bug) →
  `BALANCER-ARBITRUM:POOL:DAI-USDT-30`; `CURVE-AVALANCHE:POOL:DAI.E-USDC` unchanged (no fee segment, prefix already
  canonical). (Row-count history: 6,180 on 2026-07-08 → 6,352 on 2026-07-09, both smoke-test and real write-back —
  ongoing backfill grew the population between the two dates but it was stable within 2026-07-09 itself between the
  smoke test and the real write a few hours later.) Idempotent — re-running the script against the now-canonical
  catalog reports 0 rewrites (dry-run-verified). Script kept in `scripts/` per its own `Delete-when:` marker (delete
  once a re-run shows 0 remaining mismatches AND the `instrument_id` migration below lands, at which point
  `glued_pair_id` becomes redundant).

- **`instrument_id` catalogue regeneration — CONFIRMED STILL BLOCKED 2026-07-09, not attempted.** Re-checked both
  halves of the blocker for real before deciding not to touch `instrument_id`: (1)
  `market-tick-data-service/market_tick_data_service/engine/defi_catalog_reader.py` still reads `instrument_id`
  directly off this exact catalogue expecting `pool_address.lower()` for POOL rows (its own fallback deriver
  `_canonical_defi_id` independently recomputes the identical bare-address value) to build its expected-universe
  join for DEX swap/pool market data — confirmed live in the current MTDS tree, not stale; (2)
  `build_instrument_catalogue.py::_defi_pool_dual_form()` still unconditionally sets `instrument_id =
pool_address.lower()` for every POOL row by design (the catalogue rollup step, not the adapter) — a regen/backfill
  alone, even against the now-fixed adapter code, would still write the bare address. So this is **not** a bounded,
  safely-scriptable regeneration the way `glued_pair_id` was: flipping `instrument_id` to the structured
  `VENUE-CHAIN:POOL:...` form for POOL rows requires a coordinated MTDS-side change shipped in lockstep (MTDS's
  reader + its `_canonical_defi_id` fallback both need the new shape at the same time `instrument_id` changes, or the
  live DEX swap/pool market-data join breaks for all 13 protocols simultaneously) plus an explicit operator
  go-ahead for that cross-repo, live-breakage-authorized migration — exactly the class of change
  `instrument_id_format_canonicalization_2026_07_08.md`'s already-scoped "ground-up migration (UAC →
  instruments-service → MTDS → strategy-service → deployment)" exists for. **Not run in this pass** — the real,
  precise blocker (not a guess) is: no MTDS-side companion change exists yet, and none was authorized for this pass.
  `glued_pair_id` (now canonical, see above) is the safe interim substitute for any human-readable/UI consumer until
  that cross-repo migration ships.

- **Known gap, data state**: the `instrument_id`/MTDS cross-repo migration above remains the only unresolved piece
  of finding 2 — see the migration doc above for current status and the dedicated todo.

### Lending — A_TOKEN/DEBT_TOKEN split (from `defi_lending_atoken_debttoken_instrument_split_2026_07_07.md`)

Real per-protocol current state, verified directly against adapter code:

| Protocol        | Current key shape                                                                                                                             | Current `instrument_type` | Status                                                                                                                                                                                                                                                          |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **AAVE_V3**     | `{venue}:A_TOKEN:{a_symbol}` / `{venue}:DEBT_TOKEN:{debt_symbol}` (`aave_v3.py:424,434`)                                                      | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-13** — field relabeled from the shared `LENDING` mislabel to match the already-correct key (`instruments-service@72e0113`)                                                                                                                       |
| **SPARK**       | Same pattern (`spark.py:317,327`)                                                                                                             | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-13** — same field-only fix as AAVE_V3                                                                                                                                                                                                            |
| **COMPOUND_V3** | `{venue}:A_TOKEN:{supply_symbol}` / `{venue}:DEBT_TOKEN:{borrow_symbol}` (`compound_v3.py:269,279`)                                           | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-13** — key segment renamed from the invalid `SUPPLY`/`BORROW` (real `UnknownInstrumentTypeError` crash risk) to `A_TOKEN`/`DEBT_TOKEN`, field updated to match; historical catalogue rows migrated too (`instruments-service@72e0113`+`5226818`) |
| **MORPHO**      | `{venue}:A_TOKEN:A{collateral}-{loan}-{market_key[:8]}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{loan}-{market_key[:8]}` (`morpho.py:259-283`) | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two `InstrumentRecord`s per isolated market, correct field + key                                                                                                                                                                          |
| **EULER_V2**    | `{venue}:A_TOKEN:A{collateral}-{borrow}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{borrow}` (`euler_v2.py:79-137`)                              | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two records per curated vault                                                                                                                                                                                                             |
| **FLUID**       | `{venue}:A_TOKEN:A{collateral}-{borrow}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{borrow}` (`fluid.py:88-149`)                                 | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two records per curated vault                                                                                                                                                                                                             |
| **RADIANT**     | `{venue}:A_TOKEN:A{collateral}-{borrow}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{borrow}` (`radiant.py:94-159`)                               | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two records per curated rToken market                                                                                                                                                                                                     |
| **VENUS**       | `{venue}:A_TOKEN:A{collateral}-{borrow}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{borrow}` (`venus.py:78-142`)                                 | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two records per curated vToken market                                                                                                                                                                                                     |
| **BENQI**       | `{venue}:A_TOKEN:A{collateral}-{borrow}` / `{venue}:DEBT_TOKEN:DEBT{collateral}-{borrow}` (`benqi.py:75-140`)                                 | `A_TOKEN` / `DEBT_TOKEN`  | **Done 2026-07-09** — two records per curated qiToken market                                                                                                                                                                                                    |

Real before/after example (Morpho, one WETH/USDC market, `marketId="0xmarketkey123456789"`, disambiguator
`marketId[:8]`):

- **Before** (one flat record): venue `MORPHO-ETHEREUM`, type `LENDING_MARKET`, symbol
  `WETH` + `-` + `USDC` + `-` + disambiguator — `instrument_type=LENDING`.
- **After — supply side**: venue `MORPHO-ETHEREUM`, type `A_TOKEN`, symbol
  `A` + `WETH` + `-` + `USDC` + `-` + disambiguator (collateral deposited into the market, earns yield).
- **After — borrow side**: venue `MORPHO-ETHEREUM`, type `DEBT_TOKEN`, symbol
  `DEBT` + `WETH` + `-` + `USDC` + `-` + disambiguator (loan asset drawn against that collateral, accrues interest).

**Target (operator-decided 2026-07-08)**: every lending protocol's adapter emits exactly two `InstrumentRecord`s per
position-bearing entity — `A_TOKEN` for the supply side, `DEBT_TOKEN` for the borrow side — with the field matching
the key. No protocol keeps a bespoke type name (`SUPPLY`/`BORROW`, `LENDING_MARKET`, or a `LENDING` field mislabel)
once this lands. This generalizes the fix from "AAVE_V3/SPARK/COMPOUND_V3/MORPHO only" to all 9 lending protocols in
this doc's scope. The strategy/execution layer already assumes this split exists
(`unified_api_contracts/internal/domain/execution_service/defi_position.py:97-109`'s `is_supply`/`is_borrow`;
`PositionPortfolio.net_value = total_supply_value - total_borrow_value`) — this is a reference-data-layer catch-up,
not a new architectural decision. Fixing is staged per-protocol, not a single PR (operator: "fixing will be in stages
ofc"). **All 9 protocols now emit the correct A_TOKEN/DEBT_TOKEN split**: Morpho/Euler_V2/Fluid/Radiant/Venus/Benqi
shipped 2026-07-09; AAVE_V3/SPARK's `instrument_type` field mislabel and COMPOUND_V3's `SUPPLY`/`BORROW` →
`A_TOKEN`/`DEBT_TOKEN` key-segment rename — including a full historical-catalogue migration, not just a forward-fix —
shipped 2026-07-13 (`instruments-service@72e0113`+`5226818`, see
`unified-trading-pm/plans/active/issues/defi_lending_atoken_debttoken_instrument_split_2026_07_07.md`).
MARGINFI/SOLEND (Solana lending, out of this doc's EVM-centric scope) **now have real instruments-service adapters**
(`marginfi.py`, `solend.py`, shipped 2026-07-09) — both emit the same A_TOKEN/DEBT_TOKEN split from day one (no
flat-`LENDING`-record legacy phase to migrate through). See "Solana lending — MarginFi, Solend" below.

**Shared canonical-id builder adoption**: these 6 protocols' `instrument_key` construction now routes through
`unified_api_contracts.build_canonical_instrument_id(AssetGroup.DEFI, venue, InstrumentType.A_TOKEN | DEBT_TOKEN,
symbol, chain=..., passthrough=True)` instead of an ad hoc f-string — part of the workspace-wide retrofit tracked in
`canonical_id_builder_retrofit_checklist_2026_07_08.md`. `passthrough=True` preserves DeFi's on-chain symbol case
(the builder dispatches DeFi passthrough calls to the same `VENUE-CHAIN:TYPE:SYMBOL` construction as the structured
path) while still centralising venue-token composition + validation. AAVE_V3's existing `_build_reserve_records` is
NOT yet on the shared builder either (still an ad hoc f-string, `aave_v3.py:424,434`) — out of this round's scope,
tracked in the same retrofit checklist. COMPOUND_V3's key construction (`compound_v3.py:269,279`) is in the same
not-yet-retrofitted state: its `A_TOKEN`/`DEBT_TOKEN` key segments (migrated 2026-07-13, see the lending table above)
are now real enum members the shared builder CAN represent — unlike its pre-migration `SUPPLY`/`BORROW` shape — but
the ad hoc f-string hasn't been swapped over yet; also tracked in the retrofit checklist, not a new gap.

**Downstream consumer check (2026-07-09, before shipping the 6-protocol split)**: grepped the full workspace for the
old flat `LENDING_MARKET` key/type. Zero live consumers read instruments-service's reference-data catalog output for
these 6 venues and branch on that exact string — `market-tick-data-service`'s `fluid_adapter.py` /
`morpho_adapter.py` / `morpho_defi_ws.py` independently construct their **own** `LENDING_MARKET`-typed market-data
keys (rate/index/utilization time series) by querying the same upstream protocol APIs directly, not by reading
instruments-service's catalog — so this split does not break them mechanically, but it does widen the existing
cross-repo naming divergence between MTDS's market-data keys and instruments-service's reference-data keys for
Morpho/Fluid specifically (the same class of drift already tracked for MTDS's restaking adapters). The completed
one-off migration script `scripts/instrument_id_venue_spelling_backfill_2026_07_08.py` and the UI's
`instruments-snapshot.json` cache both reference the old shape but are point-in-time artifacts (a completed backfill
and a materialized catalog snapshot respectively) that pick up the new shape on the next regen — the standard DATA
follow-up already tracked for every canonical-id migration in this doc, not a code break.

### On-chain-perp DEXes — instrument key format

All 5 on-chain-perp adapters live under `reference_data/adapters/cefi/` (not `defi/`) organizationally, despite being
economically on-chain perpetual DEXes — a real filing quirk worth knowing when hunting for the code.

Canonical format: `VENUE:PERPETUAL:BASE-QUOTE@LIN|@INV` uniformly (no `PERP` shorthand in the key, matching the
`instrument_type=InstrumentType.PERPETUAL` field), with the real per-venue settlement currency as the quote and a real
`@LIN`/`@INV` margin marker as a trailing suffix (2026-07-09 scope-expansion of the finding 1 dated-derivative margin
marker to PERPETUAL — a venue's quote currency alone cannot disclose margin type, e.g. Kraken-Futures has both a
linear and an inverse PERPETUAL quoted in the same `USD`):

| Venue               | Settlement currency (quote)                                                     | Margin type (verification)                                                                                                                                                    |
| ------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HYPERLIQUID`       | USD (notional quote; vault collateral is USDC)                                  | `@LIN` — `hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications`: "Instrument type \| Linear perpetual" (venue's own explicit classification).              |
| `ASTER`             | Per-symbol real `quoteAsset` (USDT/USD1/"U" depending on symbol, not hardcoded) | `@LIN` — `docs.asterdex.com`: perpetuals "fully settled in USDT"; live `exchangeInfo` shows 100% stablecoin `quoteAsset` across all 509 real perps, zero coin-margined pairs. |
| `PACIFICA-SOLANA`   | USDC                                                                            | `@LIN` — real web research 2026-07-09: "Pacifica's core product is linear perpetual contracts", consistent with the already-confirmed USDC unified margin.                    |
| `EXTENDED-STARKNET` | USD (`collateralAssetName="USD"` uniformly across markets)                      | `@LIN` — `docs.extended.exchange`: "USDC as the base collateral", uniformly USDC-settled markets, no inverse product offered.                                                 |
| `LIGHTER-ZKSYNC`    | USDC                                                                            | `@LIN` — `docs.lighter.xyz/trading/multi-asset-margin`: "Portfolio Balance is the USDC value of the account" venue-wide (linear, not coin-margined).                          |

None of the 5 on-chain-perp CLOBs offer an inverse (coin-margined) product — structurally consistent with the
on-chain-perp DEX category as a whole (USD-stablecoin vault/cross-margin, never margined in the base crypto itself).
The marker is embedded directly in the `symbol` argument passed to the shared UAC builder
(`build_instrument_id(venue, InstrumentType.PERPETUAL, f"{base}-{quote}@{marker}")` — `_build_cefi_simple` upper-cases
the symbol verbatim, so no UAC-side builder change was needed) rather than derived by the builder itself.

`HYPERLIQUID`/`ASTER` carry no chain suffix (each is effectively its own app-chain — `chain="HYPERLIQUID"` lives in
the instrument's `chain` attribute, not the venue token), while `PACIFICA-SOLANA`/`EXTENDED-STARKNET`/`LIGHTER-ZKSYNC`
carry an explicit chain suffix in the venue itself. **No trailing `@VENUE`** on top — venue is already the first
colon-segment.

**Historical migration — real, shipped, fully applied (2026-07-09)**:
`migrate_onchain_perp_perpetual_canonical_2026_07_08.py` in market-tick-data-service (dry-run by default, requires
`--apply` to mutate) combines the PERP→PERPETUAL rename with the `@LIN` margin marker in one pass so historical rows
are touched once. The script was extended (same pass) to ALSO recognize an EVEN OLDER bare-symbol GCS filename shape —
no venue prefix at all, e.g. HL `AAVE-PERP.parquet`, ASTER `AAVEUSDT.parquet` — by parsing the venue from the object's
GCS **path** (`venue=` partition) rather than requiring it in the filename, mirroring the 2026-06-22 precedent's own
path-based venue resolution. This was the real, largest remaining gap: the 2026-06-22 migration had NOT actually
renamed the majority of real HL/ASTER objects to its own `{VENUE}:PERP:{SYMBOL}` intermediate shape, despite the
manifest's `instrument_id` column already reading that shape for 100% of in-scope rows (a real manifest/GCS-filename
divergence — the manifest was updated but the backing objects were not).

Real before → after, measured against the production `market-data-tick-cefi-prd-central-element-323112` bucket +
`_index/availability_index.parquet`:

- **Manifest** (7,219,598 total rows, 498,388 HL/ASTER `batch_hyperliquid`/`batch_aster` rows in scope): 100% of
  in-scope rows were ALREADY in the `:PERP:` shape (`instrument_ids_transformed_from_venue_perp_shape=498388`,
  `instrument_ids_transformed_from_bare_legacy_shape=0`) — the manifest-side transform was a real no-op confirmation,
  not new work; 0 dedup collisions, 0 row-count drift (7,219,598 rows before AND after). Post-apply verification:
  100% of the 498,388 in-scope rows now read the final `VENUE:PERPETUAL:BASE-QUOTE@LIN` shape with `instrument_type`
  uniformly `PERPETUAL` — confirmed by re-downloading and re-inspecting the real post-write manifest.
- **GCS objects — real full-corpus scan, not a narrow sample.** A real listing of the entire
  `raw_tick_data/by_date/` prefix (4,120,516 objects scanned across ALL CeFi venues; a single, expensive,
  ~82-minute walk — the pipeline_mode partition sits below `day=` so there is no cheaper prefix) found **136,814**
  real HL/ASTER objects in scope, split:
  - **97,138** in the EVEN OLDER bare-symbol shape (the real target of this pass's extension) — e.g.
    `AAVE-PERP.parquet` (HL) / `AAVEUSDT.parquet` (ASTER), confirmed coexisting ALONGSIDE the 2026-06-22-migrated
    shape in the same `data_type=` partition on multiple real sampled dates.
  - **39,202** already in the 2026-06-22-migrated `{VENUE}:PERP:{SYMBOL}` shape.
  - 471 bundled legacy `ticks.parquet` (multi-symbol, no per-file identity) correctly left untouched; 3 already
    fully canonical.
  - **Real `--apply` result**: 134,855 renamed + 1,453 duplicate-old-shape sources cleaned up (cases where both the
    bare-legacy AND `:PERP:`-shaped source existed for the same symbol/day, both correctly collapsing onto the SAME
    canonical target) + 32 transient errors (SSL/connection-pool contention under high worker concurrency,
    0.02% of 136,340) — all 32 resolved via a targeted idempotent retry (16 genuine renames, 11 duplicate-source
    cleanups, 5 already-resolved), for a **final 0 errors, 0 remaining old-format objects**. Post-apply spot-checks
    across 15+ real dates spanning 2023-04 through 2026-01 confirm every sampled `data_type=` partition now contains
    exactly one canonical `VENUE:PERPETUAL:BASE-QUOTE@LIN.parquet` file per symbol — zero trace of the old `-PERP`
    shorthand, the bare-symbol shape, or the intermediate `:PERP:` shape.
  - Real backup: `gs://market-data-tick-cefi-prd-central-element-323112/_index/backups/availability_index.pre_perpetual_canonical_20260709T1323Z.parquet`.
  - Real measured throughput note for future similar migrations: worker concurrency above the underlying HTTP
    connection pool's default size (`--workers 96` here) causes transient "connection pool is full" churn and a
    slow initial ramp (~400 renames/min), but throughput self-stabilizes as connections settle (~1,300-1,700
    renames/min once warm) — net real wall-clock for the ~136K-object rename phase was ~2h20m at that profile
    (excluding the ~82min discovery walk).

See
[`canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md`](../../../unified-trading-pm/plans/active/canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md).

### AAVE_V3-OPTIMISM venue-token spelling

`AAVE_V3-OPTIMISM` is the sole canonical spelling (the underscore is part of the protocol name, not a joiner) —
`aave_v3.py` builds `venue_tag = f"{self._venue_prefix}-{self._chain}"` → `AAVE_V3-OPTIMISM`. The misspelled variant
`AAVEV3` (no chain suffix, missing underscore) is listed in `DEPRECATED_DEFI_GHOST_VENUE_NAMES`
(`unified_api_contracts/registry/capability_declarations/_defi_coverage.py`) as a ghost superseded by `AAVE_V3`, and
is filtered by the one live prefix-matching consumer
(`deployment-api/deployment_api/services/data_status/rollup_cache.py::strip_defi_ghost_venues`, which strips on
`venue.split("-", 1)[0]`).

### MORPHO market-address disambiguator

`morpho.py` dash-separates the market-disambiguator segment (not colon-separated, since colon is the reserved
top-level `VENUE:TYPE:SYMBOL` delimiter). Post-split (2026-07-09, see "Lending — A_TOKEN/DEBT_TOKEN split" above),
the disambiguator is appended to the A_TOKEN/DEBT_TOKEN symbol rather than a flat `LENDING_MARKET` symbol — e.g. for
a real USDC/EURC market whose `market_key[:8]` disambiguator is `305dd1c2`: venue `MORPHO-BASE`, type `A_TOKEN`,
symbol `A` + `USDC-EURC-305dd1c2` (supply) and type `DEBT_TOKEN`, symbol `DEBT` + `USDC-EURC-305dd1c2` (borrow) —
same disambiguator, now on both halves of the split.

**What is `market_key` / that trailing `0x305dd1` segment, in plain language?** It is NOT a fee tier, a version
number, or arbitrary padding — it's a (truncated) piece of Morpho Blue's own on-chain **market identifier**, and
it's load-bearing, not decorative. Confirmed directly in `morpho.py:198` (`market_key = str(market.get("marketId",
""))`, sourced from the `blue-api.morpho.org` GraphQL API) plus the query itself (`morpho.py:48-65`), which also
fetches `lltv` per market. Here's why Morpho needs a 3rd identity segment at all, when every other lending protocol
in this doc (Aave_V3, Compound_V3, Euler_V2, Fluid, ...) identifies a market by token symbol alone:

- On Aave/Compound-style pooled lending, there is exactly **one** market per asset (e.g. one USDC pool) — the token
  symbol IS a unique key, so `{venue}:LENDING_MARKET:USDC` is unambiguous.
- Morpho Blue is architecturally different: it's a **permissionless, isolated-market** protocol. Anyone can create a
  brand-new market for the exact same collateral/loan token pair, but with a _different_ LLTV (max loan-to-value,
  the `lltv` field this adapter already queries), a different price oracle, and a different interest-rate model. Each
  combination is its own fully isolated market with its own risk profile — a USDC/EURC market at 86% LLTV is a
  completely different (and differently risky) instrument from a USDC/EURC market at 77% LLTV, even though "USDC-EURC"
  as a symbol would be identical for both.
- So the token-pair symbol alone is genuinely NOT a unique key on Morpho — Morpho's own protocol design solves this by
  hashing the market's full parameter set (loan token, collateral token, oracle, interest-rate model, LLTV) into one
  `bytes32` **market ID** on-chain, which is exactly the `marketId` this adapter reads from the API. In the
  `MORPHO-BASE` / `A_TOKEN` / `AUSDC-EURC-305dd1c2` example above, the trailing `305dd1c2` is the first 8 hex
  characters of that market ID (`market_key[:8]`) — just enough to disambiguate two markets that would otherwise
  collide on symbol alone, without inflating the instrument key with the full 66-character hash.

### YEARN_V3 canonical venue prefix

`YEARN_V3` is the canonical venue prefix (not bare `YEARN`) across every UAC registry — `defi_venue_capabilities.py`,
`venue_launch_dates.py`, `defi_venues.py`, `PROTOCOL_CAPABILITIES` (`_defi.py`), `chain_env.py`,
`DEPRECATED_DEFI_GHOST_VENUE_NAMES` (which lists the glued `YEARNV3` as the retired ghost of `YEARN_V3`) — and in
`yearn.py`'s adapter code (`venue_tag = f"YEARN_V3-{self._chain}"`) and `VENUE_TO_ADAPTER_KEY`
(`YEARN_V3-ETHEREUM`/`YEARN_V3-ARBITRUM`).

**Known gap, cross-repo**: `market-tick-data-service`'s `vault_yearn_adapter.py:37` and `execution-service`'s
`defi_execution/protocols/yearn.py:50` + `defi_execution/protocols/base.py:100` still hardcode the superseded bare
`"YEARN-ETHEREUM"`. Per the canonicalization doc's staged migration order (UAC → instruments-service → MTDS →
strategy-service → deployment), those 2 repos need a follow-up fix before Yearn capability/venue-gated checks are
consistent end-to-end.

### Balancer cross-chain pool-address collision

`balancer.py` builds a fully chain-qualified key (`venue_tag = f"BALANCER-{self._chain}"`,
`instrument_key = f"{venue_tag}:POOL:{base}-{quote}"`), and the catalog's `glued_pair_id` column is likewise
chain-differentiated. A small number of Balancer pools happen to share a bit-for-bit identical on-chain contract
address across Ethereum and Polygon (Balancer deployed its pool factory to both chains within days of each other in
2021, via sequential `CREATE` from each per-chain factory address, so the Nth-pool deployment nonce can line up
across the two chains' independent histories even though the pools are economically unrelated). Since every DEX-pool
row's **primary** `instrument_id` today is the bare on-chain pool contract address alone (`pool_address.lower()`, no
chain — the "DEX pools" gap above), these specific pools are disambiguated with an explicit `@CHAIN` suffix (the
general `VENUE:TYPE:PAYLOAD[@CHAIN]` grammar's escape hatch) rather than waiting on the full finding-2 migration.

---

## Systemic venue-token duplicate-spelling pattern

Beyond AAVE_V3/AAVEV3, this class is systemic: `MORPHO_VAULTS`/`MORPHOVAULTS`, `YEARN_V3`/`YEARNV3`,
`UNISWAP_V3`/`UNISWAPV3` also occur (the last 2 live in the MTDS manifest / GCS-object-path layer — a different
bucket/layer than the `instruments-service` catalog this doc covers — acknowledged in a code comment inside
market-tick-data-service's `rebuild_defi_manifest.py`). The registry carries a `DEPRECATED_DEFI_GHOST_VENUE_NAMES`
frozenset (`_defi_coverage.py`) that the manifest consolidator and data-status service both filter against at read
time: `UNISWAPV3`/`UNISWAPV2`/`UNISWAPV4`, `AAVEV2`/`AAVEV3`, `CAMELOTV3`, `COMPOUNDV3`, `MORPHO_VAULTS` (legacy
underscore form superseded by the fully-glued `MORPHOVAULTS`), `PANCAKESWAPV3`, `SUSHISWAPV3`, `VELODROMEV2`,
`TRADER_JOEV2`/`TRADERJOEV2`, `YEARNV3`, `AERODROMEV3`. This filters the symptom downstream (keeps ghost names out
of the manifest/UI) but doesn't stop new duplicate writes at the adapter/writer source.

**Key-vs-field abbreviation convention.** Yield/LST/restaking adapters historically diverged on whether the
instrument key or the `instrument_type` field carries the more specific classification. The current convention,
per group:

- **`LST`-keyed adapters (Lido, EtherFi, Renzo, KelpDAO, Puffer, RocketPool)**: the key uses `:LST:` and
  `instrument_type` stamps `InstrumentType.LST` (a real, distinct enum member, not a shorthand for `YIELD_BEARING`).
  `LST` has its own downstream ledger treatment — `ledger_asset_resolution.py` maps
  `InstrumentType.LST → LedgerAssetClass.LST` and `InstrumentType.YIELD_BEARING → LedgerAssetClass.VAULT_SHARE`, a
  real accounting distinction — and execution-service's `catalog_validator.py`/`dependency_validator.py`/
  `data_availability_validator.py` and strategy-service's `pnl_calculator.py`/`settlement_service.py`/
  `risk_monitor.py` parse the key's `:LST:` segment directly. `get_instruments(instrument_type=...)` accepts either
  `InstrumentType.LST` or `InstrumentType.YIELD_BEARING`.
- **`VAULT`-keyed adapters (Yearn, Beefy, Karak, Idle, Symbiotic, Convex)**: the key's middle segment is
  `YIELD_BEARING`, not `VAULT` — `VAULT` is not a real `InstrumentType` enum member, and no consumer in
  instruments-service/strategy-service/execution-service parses a `:VAULT:` key-substring for classification.
- **Sanctum, Solblaze, and Jito-restaking are explicitly out-of-scope Solana venues** per this doc's own scope
  section (a separate "Solana DeFi" surface) with real instruments-service adapters (`sanctum.py`, `solblaze.py`,
  `jito_restaking.py`). **Fixed 2026-07-09** (`instruments_docs_audit_outstanding_items_2026_07_08.md` finding C4):
  `sanctum.py`/`solblaze.py` both keyed `:LST:` while stamping `instrument_type=InstrumentType.YIELD_BEARING` — the
  field was fixed to `InstrumentType.LST` (LST is the real, distinct enum member; same "field follows the already-real
  key" convention as the `LST`-keyed group above). `jito_restaking.py` keyed `:VAULT:` against the same field — the
  key was fixed to `:YIELD_BEARING:` (same "key follows the already-real field" convention as the `VAULT`-keyed group
  above, since `VAULT` isn't a real `InstrumentType`). Real before/after:
  `SANCTUM-SOLANA:LST:INF` (field YIELD_BEARING → LST, key unchanged) and
  `JITORESTAKING-SOLANA:VAULT:JTORK-EZSOL` → `JITORESTAKING-SOLANA:YIELD_BEARING:JTORK-EZSOL` (key changed, field
  unchanged). `get_instruments(instrument_type=...)` on Sanctum/Solblaze now accepts either `InstrumentType.LST` or
  `InstrumentType.YIELD_BEARING` (back-compat), mirroring the `LST`-keyed group.
- **Drift, Jupiter — PERP/SPOT shorthand fixed 2026-07-09** (same C4 finding, same convention as the
  on-chain-perp `PERP`-vs-`PERPETUAL` canonicalization above): `drift.py` keyed `:PERP:`/`:SPOT:` against
  `instrument_type=PERPETUAL`/`SPOT_PAIR`; `jupiter.py` keyed `:SPOT:` against `SPOT_PAIR`. Both keys were fixed to the
  real enum spelling (`PERPETUAL`/`SPOT_PAIR` aren't shorthand-able — `PERP`/`SPOT` are not real `InstrumentType`
  members). Real before/after:
  `DRIFT-SOLANA:PERP:SOL-PERP` → `DRIFT-SOLANA:PERPETUAL:SOL-PERP`,
  `DRIFT-SOLANA:SPOT:SOL` → `DRIFT-SOLANA:SPOT_PAIR:SOL`,
  `JUPITER-SOLANA:SPOT:SOL-USDC` → `JUPITER-SOLANA:SPOT_PAIR:SOL-USDC`.
  (Flash Trade's equivalent fix is moot — `flash_trade.py` was removed 2026-07-15, operator ruling; see
  "Remaining known limitation" below.)
- **EigenLayer, EthFi — `GOVERNANCE_TOKEN` shorthand fixed 2026-07-09** (new finding, same C4 class): both adapters
  keyed `:GOVERNANCE_TOKEN:` while the field already correctly said `InstrumentType.SPOT_PAIR` (`GOVERNANCE_TOKEN` is
  not a real `InstrumentType` member). Keys fixed to `:SPOT_PAIR:` to match the field. This also fixed a real
  canonical-form type-filter bug (same class as
  `canonical_id_p0_defi_adapter_type_filter_bug_2026_07_08.md`): both adapters' `get_instruments(instrument_type=...)`
  guard only matched the literal strings `"GOVERNANCE_TOKEN"`/`"governance_token"`, so filtering by the adapters' own
  real field value (`InstrumentType.SPOT_PAIR`) silently returned `[]`; the guard now accepts
  `InstrumentType.SPOT_PAIR` too (plus the legacy strings, back-compat). Real before/after:
  `EIGENLAYER-ETHEREUM:GOVERNANCE_TOKEN:EIGEN` → `EIGENLAYER-ETHEREUM:SPOT_PAIR:EIGEN`,
  `ETHERFI-GOV-ETHEREUM:GOVERNANCE_TOKEN:ETHFI` → `ETHERFI-GOV-ETHEREUM:SPOT_PAIR:ETHFI`.
- **COMPOUND_V3's `SUPPLY`/`BORROW` key segments — migrated 2026-07-13** (supersedes the 2026-07-09 "deliberately
  left as-is" review this bullet originally described): the real crash-risk key segments (`SUPPLY`/`BORROW`, neither
  a valid `InstrumentType` — a real `UnknownInstrumentTypeError` risk had either ever reached
  `InstrumentKey.from_string()`) were renamed to `A_TOKEN`/`DEBT_TOKEN`, with the `instrument_type` field updated to
  match (previously the shared `LENDING` value). This is the same key-segment rename this doc's lending table above
  tracked as a GCS-partition-migration-gated follow-up — now shipped for both live writes and the historical
  catalogue (`instruments-service@72e0113`+`5226818`, see
  `unified-trading-pm/plans/active/issues/defi_lending_atoken_debttoken_instrument_split_2026_07_07.md`).
  `build_canonical_instrument_id` can now represent both real enum members, but `compound_v3.py`'s `instrument_key`
  construction has not yet been retrofitted to route through it (still an ad hoc f-string) — that remains open,
  tracked in `canonical_id_builder_retrofit_checklist_2026_07_08.md`.
- **Remaining known limitation** — 5 more Solana/DeFi venues (meteora, phoenix, lifinity, kamino,
  marinade) still share the `PERP`/`SPOT`-vs-`PERPETUAL`/`SPOT_PAIR` shorthand mismatch class (e.g. `meteora.py` keys
  `:SPOT:` against `instrument_type=InstrumentType.SPOT_PAIR`) — out of this pass's scope, a known follow-up for
  whichever pass covers the rest of Solana-native DeFi conventions. (`mango`/`zeta` — which shared this same
  limitation — were removed 2026-07-15 along with `flash_trade`, operator ruling; no longer applicable.)
- **Shared canonical-id builder adoption (2026-07-09)**: Sanctum/Solblaze/Jito-restaking/Drift/EigenLayer/EthFi/
  Jupiter above, plus Balancer/Curve/Ethena/Jito/Beefy/Convex/Idle/Karak/EtherFi/KelpDAO (the
  already-field-fixed `LST`/`VAULT`-keyed group from 2026-07-08), now all route `instrument_key` construction through
  `unified_api_contracts.build_canonical_instrument_id(AssetGroup.DEFI, venue, InstrumentType.X, symbol,
passthrough=True)` instead of an ad hoc f-string — part of the same workspace-wide retrofit tracked in
  `canonical_id_builder_retrofit_checklist_2026_07_08.md` as the 6-protocol lending split above. `COMPOUND_V3` is
  the one adapter in this batch NOT retrofitted — as of the 2026-07-13 `A_TOKEN`/`DEBT_TOKEN` key-segment migration
  above, its key IS now representable by the shared builder (unlike its pre-migration `SUPPLY`/`BORROW` shape), the
  retrofit itself just hasn't happened yet — tracked as an open item in the same checklist, not a builder limitation.
- **Cross-repo divergence — fixed 2026-07-09** (`instruments_docs_audit_outstanding_items_2026_07_08.md` finding
  C5): `market-tick-data-service` has its own separate, live/wired adapters for some of these same protocols
  (`restaking_karak_adapter.py`, `restaking_jito_adapter.py`, `restaking_symbiotic_adapter.py`,
  `vault_pendle_adapter.py` — wired into MTDS's `factory.py` `VENUE_REGISTRY` under `"karak"`, `"jito_restaking"`,
  `"symbiotic"`, `"pendle"`) that built `:VAULT:` keys with `instrument_type="RESTAKING_VAULT"` — neither is a real
  `InstrumentType` enum member, the same class of bug this doc's `VAULT`-keyed-adapter convention above already
  fixed on the instruments-service side. Reconciled to `:YIELD_BEARING:` + `instrument_type="YIELD_BEARING"` on the
  MTDS side too. Real before/after: `KARAK-ETHEREUM:VAULT:{pool_id}` → `KARAK-ETHEREUM:YIELD_BEARING:{pool_id}`
  (same pattern for Jito-Restaking/Symbiotic/Pendle). Pendle's real caveat: instruments-service's `pendle.py`
  canonically encodes the PT/YT/SY leg role in the key segment instead (e.g.
  `PENDLE-ETHEREUM:PT:PT-stETH-25JUN2026`) because it discovers markets leg-by-leg from Pendle's own API; MTDS's
  adapter is DefiLlama-pool-level (no PT/YT/SY split available from that data source), so full reconciliation to
  the leg-level shape isn't achievable without a data-source change — `YIELD_BEARING` (`pendle.py`'s own module
  docstring documents it as "the closest UAC `InstrumentType`" for PT/YT/SY, no PT/YT-specific enum exists) is used
  for both key and field instead, consistent with the Karak/Jito/Symbiotic fix. Shipped
  `market-tick-data-service@f3ff5ea0`.

---

## Legacy GCS naming audit — real per-protocol findings and migration (2026-07-09)

Generalizes the on-chain-perp bare-filename finding (operator 2026-07-09: "audit CeFi ... and DeFi ... historical GCS
data for the same problem") to the 13 DEX-pool protocols + 25 lending/staking/yield/restaking venues. Real, narrow
(single-venue-prefix) GCS listings against `instrument_availability/by_date/` — not a manifest summary — covering
BOTH `instruments-store-defi-prd-{pid}` (2,363 real day-partitions, 2020-01-20 .. 2026-07-09) and the legacy env-less
`instruments-store-defi-{pid}` (2,315 day-partitions, confirmed FROZEN since 2026-05-22 — zero real writes past that
date, verified via a high-water-mark check; every current reader/writer resolves the `-prd-` bucket via
`_get_instruments_bucket` → `resolve_bucket_name`).

### Finding 1 — ghost/canonical venue-token duplicate GCS history (28 real venue×chain pairs, ~4 years) — MIGRATED

A real ghost (no-underscore) vs canonical (underscore) venue-token spelling was found written **in parallel** for the
same real protocol×chain×day, spanning 2022-03-27 .. ~2026-05-11 (then stopped for good — `writers.py`'s
`canonicalize_defi_venue_combined(venue_str)` call, landed per `defi_coverage_capability_alignment_2026_05_22.md`,
already fixed this going forward; this audit is the historical backfill that fix never covered). Real per-venue
ghost-day counts (full real scan, both buckets, 2026-07-09):

| Ghost venue                                               |     Ghost days | Canon days (pre-migration) | Note                                               |
| --------------------------------------------------------- | -------------: | -------------------------: | -------------------------------------------------- |
| AAVEV3-ARBITRUM/AVALANCHE/BASE/BSC/ETHEREUM/LINEA/POLYGON | 446–1,514 each |             495–1,572 each |                                                    |
| AAVEV3-OPTIMISM                                           |          1,514 |                        108 | canon was a MINORITY of real history pre-migration |
| AERODROMEV3-BASE                                          |            710 |                        792 |                                                    |
| CAMELOTV3-ARBITRUM                                        |          1,032 |                      1,106 |                                                    |
| COMPOUNDV3-ARBITRUM/BASE/ETHEREUM/OPTIMISM                | 757–1,359 each |             822–1,422 each |                                                    |
| PANCAKESWAPV3-BASE/BSC/ETHEREUM                           | 954–1,105 each |           1,032–1,182 each |                                                    |
| PANCAKESWAPV3-ZKSYNC                                      |            446 |                          0 | 100% orphan pre-migration                          |
| SUSHISWAPV3-AVALANCHE/BASE/ETHEREUM                       | 245–1,103 each |             883–1,192 each |                                                    |
| UNISWAPV2-ETHEREUM                                        |          2,189 |                      2,243 |                                                    |
| UNISWAPV3-ARBITRUM/BASE/ETHEREUM/OPTIMISM/POLYGON         | 974–1,825 each |           1,006–1,883 each |                                                    |
| UNISWAPV4-ETHEREUM                                        |            459 |                        525 |                                                    |
| VELODROMEV2-OPTIMISM (legacy env-less bucket ONLY)        |          1,044 |          0 (either bucket) | 100% orphan + wrong bucket                         |

**Total real scope: 33,003 (bucket, ghost_venue, day) triples** — 31,968 in `-prd-` + 1,044 legacy-bucket-only (9
additional triples were processed during script validation ahead of the full run, for a real grand total of
**33,012 real ghost objects**).

**Not a byte-identical duplicate — a real content diff mattered.** 81 sampled ghost/canon same-day pairs (both
parquets read, compared on the `raw_symbol` identity column) showed most pairs are NOT duplicates: each side commonly
holds real pools the other is missing (e.g. `UNISWAPV3-OPTIMISM` day=2023-11-18: 168 ghost rows vs 282 canonical, 6
ghost-only + 120 canonical-only; `PANCAKESWAPV3-BSC` day=2024-10-07: 129 ghost vs 86 canonical, 59 ghost-only). The
canonical schema is also wider (51 real columns vs ghost's 40 — `canonical_instrument_id`/`product_root`/
`available_at`/etc. were added to the writer after most ghost objects were captured). A blind rename in either
direction would have destroyed real, non-redundant pool history.

**Migration executed for real** —
`instruments-service/scripts/legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py`: per-(bucket, day, venue)
column-union + row-union merge (canonical wins on identity-column conflict, ghost-only rows carried over honestly,
never fabricated), backup-first (every pre-migration object server-side-copied under
`_migration_backup/legacy_naming_audit_dexpool_2026_07_09/` before any write), post-write verify-then-delete (ghost
object only deleted after a re-read confirms the canonical row count ≥ the union floor), real `ThreadPoolExecutor`
concurrency (48 workers). **Real results, both passes**: 33,003/33,003 succeeded (100%), 0 remaining failures, a
follow-up idempotent full re-scan of all 29 ghost-venue prefixes across both buckets confirmed **0 real ghost objects
remain anywhere** — durable, not just catalog-level. Real elapsed time: ~78 minutes wall-clock (main pass 4,568s +
mop-up 41s + verification listings), real concurrency throughout (measured throughput ramped 4.4 → 7.6 objects/sec as
the run progressed). Real content recovered: **2,314,285 total real rows read from ghost objects**, merged into
canonical objects now totaling **2,828,070 rows** (the delta is real, previously-orphaned pool/reserve history now
visible under the canonical venue name for the first time — most concentrated in `AAVE_V3-OPTIMISM`,
`PANCAKESWAP_V3-ZKSYNC`, and `VELODROME_V2-OPTIMISM`, the 3 venues where canonical was a minority-or-zero share of
real history pre-migration).

**Real mid-run finding, fixed in the same pass**: 11 of 33,003 objects (1 transient GCS 503 on a backup-copy call, 10
`UNISWAPV3-POLYGON` days) initially failed a post-write verify check — safely (ghost object was NOT deleted in either
case, no data at risk). Root-caused: the verify floor compared the merged row count against the ghost object's RAW
row count, but a real source file can carry an internal duplicate identity value (`UNISWAPV3-POLYGON` day=2025-01-10's
real ghost object has 476 raw rows but only 475 unique `raw_symbol` values — a real subgraph-pagination-overlap
re-listing, not corruption) — the merge already correctly deduped on the identity column, so the raw-row-count floor
rejected a genuinely correct de-duplicated result as a false-positive "regression." Fixed (floor now uses the unique
identity-column count) and all 11 re-ran clean (0 failures) in a scoped mop-up pass.

**Real contamination bug found + fixed same day (2026-07-09 adversarial verification), fully remediated**: an
independent verification pass on the FIRST shipped version of the migration (`instruments-service@11192be2`) found
`_merge_frames` fixed the GCS _path_ to canonical but never rewrote a surviving ghost-only row's own
`venue`/`instrument_key` COLUMN VALUES — e.g. `instrument_key='AAVEV3-OPTIMISM:A_TOKEN:ALINK'` (no underscore) lived
on inside an otherwise-canonical-path parquet, confirmed for real on
`AAVE_V3-OPTIMISM` day=2022-04-23 (3/15 rows), `UNISWAP_V3-OPTIMISM` day=2023-11-18 (6/288 rows), and
`PANCAKESWAP_V3-BSC` day=2024-10-07 (59/145 rows). Any downstream reader filtering/joining on the `venue` COLUMN
(not the GCS path) would have silently missed or mis-bucketed these specific rows.

**Fix**: `legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py`'s `_merge_frames` now calls a new
`_rewrite_ghost_venue_columns` (generic over every column, not hardcoded to `venue`/`instrument_key`) on the ghost
frame BEFORE any dedup/concat, so a surviving row can never carry the ghost spelling in its data. Covered by a new
real unit test suite (`tests/scripts/test_legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py`, 10 cases)
that asserts the surviving rows' COLUMN VALUES are canonical post-merge, not merely that the row count/identity
survived — verified to fail against the pre-fix code (10/10 fail with the old 2-arg `_merge_frames` signature) and
pass against the fix.

**Real, targeted remediation of already-migrated data** (`legacy_naming_audit_dexpool_ghost_venue_contamination_remediation_2026_07_09.py`
— NOT a fresh full-corpus walk; every row this bug could reach lives under exactly the 29 canonical venue prefixes
in `GHOST_TO_CANON`, in `-prd-` only, since `_process_one` always wrote the merged frame to the PRD bucket
regardless of source): a real scoped per-venue-prefix listing found **35,594 real (canon_venue, day) pairs** across
all 29 venues; **10,823 of them (30.4%) were genuinely contaminated** (390,784 real ghost-spelled cells found and
rewritten to canonical — backup-first under
`_migration_backup/legacy_naming_audit_dexpool_contamination_remediation_2026_07_09/`, verify-after on every write:
row count unchanged, 0 ghost cells remain). 0 failures across all 35,594 pairs. An independent full re-run in
dry-run mode immediately after confirmed **0 contaminated pairs remain anywhere** (`contaminated_pairs=0` across
all 35,594). The 3 originally-cited sample files (`AAVE_V3-OPTIMISM` 2022-04-23, `UNISWAP_V3-OPTIMISM` 2023-11-18,
`PANCAKESWAP_V3-BSC` 2024-10-07) were independently re-downloaded post-fix and confirmed genuinely clean (0
ghost-spelled cells in any column). Interesting real finding: `VELODROME_V2-OPTIMISM` and the 3 `SUSHISWAP_V3-*`
venues had **zero** contamination despite being in scope — verified for real (`VELODROME_V2-OPTIMISM`, a 100%
pre-migration orphan, was independently spot-checked and its row-level `venue`/`instrument_key` values were ALREADY
canonical-spelled at capture time; only its legacy GCS _path_ was ghost-shaped, a narrower bug than the general
case).

### Finding 2 — lending/staking/yield venues: 25 requested, only 5 have ANY real object here (not a naming gap)

A full real venue-token inventory (every distinct `venue=` token across the ENTIRE 2020-2026 history, both buckets —
87 distinct tokens total) was cross-checked against all 25 requested lending/staking venues (Euler_V2, Fluid, Radiant,
Venus, Benqi, MarginFi, Solend, Lido, EtherFi, Renzo, KelpDAO, Puffer, RocketPool, Sanctum, Solblaze, Yearn, Beefy,
Karak, Idle, Symbiotic, Convex, plus Aave_V3/Spark/Compound_V3/Morpho already covered above). Only **Aave_V3, Spark,
Compound_V3, Morpho, and Fluid** have ever written a single real object to `instrument_availability/by_date/`, under
any spelling — Spark/Morpho/Fluid canonical-only (no ghost variant ever existed for these 3, nothing to migrate).
The other **18 requested venues have ZERO real objects here, under any naming variant**: Euler_V2, Radiant, Venus,
Benqi, MarginFi, Solend, Renzo, KelpDAO, Puffer, RocketPool, Sanctum, Solblaze, Yearn(\_V3), Beefy, Karak, Idle,
Symbiotic, Convex. **This is NOT a naming-convention gap** (there is nothing mis-spelled to rename — these adapters
have real code (`instruments_service/reference_data/adapters/defi/`) but have never run a real production capture
against this specific GCS path) — it is a separate, already-tracked "not yet backfilled" state (see
`DEFI_INSTRUMENTS_NOT_YET_COLLECTED`/`EMPTY_OR_DEPRECATED_DEFI_VENUES` under "Known gaps" above for the venues
already flagged there; the remainder are simply un-backfilled, out of THIS audit's naming-migration scope). Two bonus
finds beyond the requested 25 were also present with clean canonical-only real data, no gap: `EIGENLAYER-ETHEREUM`,
`ETHENA-ETHEREUM`.

### Finding 3 (flagged, NOT executed this pass) — orphaned `pipeline_mode=` duplicate-write tree

While auditing Finding 1's day-partitions, a SECOND, fully distinct real path shape was found:
`instrument_availability/by_date/day={D}/pipeline_mode=batch_instruments_service/asset_group=defi/venue={V}/instruments.parquet`
— present in 2,353 of 2,363 real day-partitions in `-prd-` (stopped ~2026-06-30, evidence it's already dead
going-forward too), mirroring the SAME venue set as the flat `day={D}/venue={V}/...` tree. Real hash comparison
(CRC32C + MD5) on 2 samples spanning the oldest (`day=2020-01-20`) and a recent (`day=2026-06-10`) date confirmed
**byte-for-byte identical content** to the flat-shape sibling for the same (day, venue). Crucially: **zero real
readers consume this shape** — every real consumer (`unified_trading_library/instrument_lifecycle_loader.py`,
`domain/instruments_client.py`, `domain_client/clients/instruments.py`, `options_cluster_lookup.py`,
`core/cloud_data_provider.py`) reads only the flat `day=/venue=/instruments.parquet` shape;
`pipeline_mode=`/`asset_group=` partitioning is a real, documented convention for MTDS's `raw_tick_data` (per
`codex/02-data/defi-canonical-naming-ssot.md`) but was never wired to an `instrument_availability` reader. This
represents an estimated ~104K real orphaned duplicate objects in `-prd-` alone (mirrors the flat tree's real
venue-day objects for 2,353 of 2,363 days) — pure storage waste, not a correctness/migration-blocking gap (nothing
silently drops data because nothing reads it). Left as a flagged follow-up (real evidence above; a dedicated
SAFE-TO-DELETE audit + delete-list script, same pattern as MTDS's own
`e2e-testing/scripts/defi/audit_legacy_gcs_dup_delete_list.py` precedent, is the right next step) rather than executed
in this pass — out of THIS audit's "naming convention" scope (nothing here is mis-named; it's a write path that was
never read) and a genuinely separate, large, operator-notify-worthy finding on its own.

---

## DEX pools

### Adapter architecture: 5 native adapters, 8 protocols reuse one via config

Uniswap V2, Uniswap V3, Uniswap V4, Balancer, and Curve each have a dedicated adapter class. Seven more DEX
protocols/forks (PancakeSwap_V3, Sushiswap_V3, Sushiswap, Aerodrome_V3, Camelot_V3, Velodrome_V2, TraderJoe_V2) plus
GMX are **not** separate adapter classes — they all instantiate `UniswapV3ReferenceDataAdapter` with a
`protocol_slug` constructor argument (`uniswap_v3.py:118,123`) that swaps the venue prefix and the subgraph-ID lookup
table. The adapter has a 3-tier subgraph-schema cascade (primary schema → Algebra CL fallback → SushiSwap-pairs
fallback) to handle the fact these forks don't all use an identical Graph schema. This is real, working
architecture — not a gap — but means "13 DEX protocols" in the audit's finding 2 is really "5 adapter classes + 8
config variants of one of them."

### Protocol × chain coverage (real, from `SUBGRAPH_IDS`)

| Protocol              | Chains with a live subgraph ID today                                                                                     |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Uniswap V2            | Ethereum                                                                                                                 |
| Uniswap V3            | Ethereum, Arbitrum, Base, Optimism, Polygon (Polygon subgraph returns 0 instruments — see gaps below)                    |
| Uniswap V4            | Ethereum                                                                                                                 |
| Balancer              | Ethereum, Arbitrum, Polygon, Optimism, Avalanche, Base                                                                   |
| Curve                 | Ethereum, Avalanche, Optimism, Arbitrum, Polygon, Base, Fantom (via the Curve REST API `api.curve.finance`, no subgraph) |
| PancakeSwap_V3        | BSC, Ethereum, Base                                                                                                      |
| Sushiswap_V3          | Ethereum, Base, Avalanche                                                                                                |
| Sushiswap (legacy V2) | Arbitrum                                                                                                                 |
| Aerodrome_V3          | Base                                                                                                                     |
| Velodrome_V2          | Optimism (subgraph has 881 instruments but zero parquets ever written — see gaps below)                                  |
| Camelot_V3            | Arbitrum                                                                                                                 |
| TraderJoe_V2          | Avalanche                                                                                                                |
| GMX                   | Arbitrum, Avalanche (Avalanche has ~1 instrument / minimal historical data)                                              |

### Known gaps (real, tracked in `_defi_coverage.py` — this is the SSOT data-status reads, not a TODO list to re-derive)

- **Empty/deprecated subgraphs** (`EMPTY_OR_DEPRECATED_DEFI_VENUES`): `TRADER_JOE_V2-AVALANCHE` (0 instruments),
  `UNISWAP_V3-POLYGON` (subgraph returns 0), `GMX-AVALANCHE` (minimal/no historical parquets).
- **Subgraph exists, never onboarded** (`DEFI_INSTRUMENTS_NOT_YET_COLLECTED`): `VELODROME_V2-OPTIMISM` (881
  instruments available, zero parquets written), `SPARK-ETHEREUM` (adapter shipped, subgraph has 17 markets, no
  historical parquets yet). `SANCTUM-SOLANA` / `SOLBLAZE-SOLANA` are also listed here (out of this doc's EVM-centric
  scope — see the scope section); their instruments-service adapters (`sanctum.py`, `solblaze.py`) exist and are
  wired into the factory registry, but no historical parquets have been backfilled yet — the registry's own comment
  describing the adapters as "not yet created" is itself stale. data-status intentionally does not flag these as
  "missing" until the first real write lands.

### Fee tiers

Uniswap V3's `feeTier` field is hundredths-of-a-basis-point on the wire (500 = 0.05%, 3000 = 0.3%, 10000 = 1%); the
adapter converts to whole basis points for the canonical symbol. V2-style pools (Uniswap V2, most forks) imply a flat
0.3% fee with no on-chain fee-tier field.

### Subgraph fetch: TVL-ranked, not exhaustive — real ceiling is ~6,000 pools

DEX pool adapters (Uniswap V2/V3/V4, and the config-variant protocols that reuse `UniswapV3ReferenceDataAdapter`) query
the subgraph with `orderBy: totalValueLockedUSD, orderDirection: desc`, paginated (`uniswap_v3.py:55,57`:
`_FETCH_LIMIT = 1000` per page, `_MAX_SKIP = 5000` → up to 6 pages, so up to **~6,000** pools fetched per protocol per
run, ordered highest-TVL-first). The major-assets whitelist filter (below) runs on top of this TVL-ranked fetch, not
instead of it — it's a two-stage pipeline: (1) fetch the highest-TVL pools up to the real ceiling, (2) keep only the
ones where both `base_asset` and `quote_asset` pass the whitelist.

A genuine major-asset pool that ranks below the ~6,000-pool cutoff on a given day is never fetched at all, so the
whitelist filter never gets a chance to keep it — the TVL ceiling is a hard cutoff applied _before_ curation, not a
fallback safety net for curated pairs. `uniswap_v3.py` mitigates this with a supplementary query
(`_fetch_major_asset_pools`) that asks the subgraph directly for pools where both `token0` AND `token1` are in
`DEFI_MAJOR_ASSET_ADDRESS_LIST` (`unified_api_contracts/registry/defi_major_assets.py`), merging any pools the
TVL-ranked cascade missed into the result set. Because this fires on top of the existing cascade, it also covers the
8 protocols that share `UniswapV3ReferenceDataAdapter` via `protocol_slug` whenever they run on Ethereum.

**Known remaining gaps**: (1) `DEFI_MAJOR_ASSET_ADDRESS_LIST` is Ethereum-mainnet-only (every address in it is a
commented `DERIVED ethereum etherscan` entry) — the same 9 protocols still have the uncovered TVL-ceiling gap on
every other chain (Arbitrum, Base, Optimism, Polygon, BSC, Avalanche, zkSync); (2) Uniswap V2, Uniswap V4, and
Balancer (3 separate adapter classes, each with their own independent fetch/pagination code) don't run this
supplementary query — they still have the original TVL-rank-then-filter gap on every chain. A full fix would need
either a per-chain major-asset address registry (doesn't exist today) or a symbol-based nested-entity query
applied to the remaining adapter files.

---

## Lending

9 protocols in scope: Aave_V3, Spark, Compound_V3, Morpho, Euler_V2, Fluid, Radiant, Venus, Benqi. See "Lending —
A_TOKEN/DEBT_TOKEN split" under Current-vs-target above for the real per-protocol current state — this section covers
data sources and chain coverage instead.

| Protocol    | Data source                                                                        | Chains (real, from `SUBGRAPH_IDS`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Aave_V3     | The Graph (+ AaveScan REST fallback)                                               | Ethereum, Arbitrum, Optimism (RPC-fallback only — subgraph abandoned by the Aave team, see note below), Polygon, Avalanche, Base, Linea, BSC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Spark       | The Graph (MakerDAO fork of Aave V3, same schema)                                  | Ethereum                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Compound_V3 | The Graph                                                                          | Ethereum, Arbitrum, Base, Optimism (Polygon explicitly removed — subgraph returns 0 active markets)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Morpho      | `blue-api.morpho.org` GraphQL (not The Graph)                                      | Ethereum, Base (Arbitrum/Optimism/Polygon have 0 major-asset markets as of 2026-03, not queried)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Euler_V2    | Goldsky subgraph (routed via an endpoint override, not the standard Graph gateway) | Ethereum, Arbitrum                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Fluid       | On-chain RPC (`FluidVaultResolver` + `FluidLiquidityResolver`), not The Graph      | Ethereum only. UAC's `capability_declarations/_defi.py` still lists a `"fluid": {"ETHEREUM": "fluid-mainnet"}` subgraph-ID entry with a "Multi-chain: Fluid subgraph IDs need verification" comment, but neither instruments-service's `fluid.py` nor MTDS's `fluid_adapter.py` actually queries a Fluid subgraph (both are RPC-only, confirmed by grep) — that entry appears to be unused/orphaned metadata rather than a live gap; real verified multi-chain deployment data exists instead (Fluid's `LiquidityResolver`/`VaultResolver` family: same CREATE2 address `0xca13A15de31235A37134B4717021C35A3CF25C60` on mainnet/arbitrum/base/polygon/plasma/bnb, per Instadapp's official `deployments.md` — 2026-07-09) |
| Radiant     | The Graph (Messari Lending schema)                                                 | Arbitrum, Ethereum                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Venus       | The Graph (Compound-fork schema)                                                   | BSC (isolated pools), Ethereum                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| Benqi       | The Graph (Compound-fork schema)                                                   | Avalanche                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

**AAVE_V3-OPTIMISM data-source note**: the subgraph on Optimism was silently abandoned by the Aave team between
2026-05-08 and 2026-05-29 (returns empty `reserves`/`reserveParamsHistoryItems` despite reporting no indexing errors);
canonical data source for this one chain is the RPC fallback (14-row daily resolution), a deliberate policy decision,
not a bug.

**Data-type coverage varies by protocol** — Venus and Benqi's subgraph schemas expose neither a daily-snapshot history
entity nor a dedicated liquidation/risk-param entity, so those two are `lending_indices` only. Aave_V3/Morpho
additionally collect `liquidation_events`, `flash_loan_events` (Aave_V3 only), and top-position `position_data`.

**Fluid's `lending_indices` MTDS collector** reads vault state via Fluid's own periphery `FluidVaultResolver`
contract (`0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC`, Ethereum mainnet) and its `getVaultEntireData(vault)` method —
not `totalSupply()`/`totalBorrow()`/`exchangePrice()` directly on the vault contract, which don't exist on Fluid's
VaultT1 implementation (an ERC-4626-style ABI shape Fluid doesn't use). The collector also does per-token ERC-20
`decimals()` lookups (not a hardcoded 1e18 assumption, which would be wrong for 6-decimal assets like USDC/USDT) and
native-ETH sentinel handling (Fluid vaults can hold native ETH directly, with no ERC-20 contract to query).

**`utilization_rate` — fixed 2026-07-09** (`instruments_docs_audit_outstanding_items_2026_07_08.md` finding C7): was
`total_borrow / total_supply`, a raw cross-asset ratio (Fluid vaults hold DIFFERENT collateral and debt assets,
unlike Aave's shared-pool model — dividing e.g. raw ETH collateral units by raw USDC debt units is not a real
utilization rate). Fixed to read Fluid's own protocol-computed, same-asset, per-token utilization directly from the
Liquidity Layer instead: `FluidLiquidityResolver.getOverallTokenData(borrow_token).lastStoredUtilization` (1e4
precision; resolver at `0xca13A15de31235A37134B4717021C35A3CF25C60`, verified via Instadapp's official
`deployments.md` + a live `eth_getCode` bytecode check). Live-verified 2026-07-09 against real Ethereum mainnet
data: USDC `lastStoredUtilization=8871` (88.71%) cross-checks against an independently-read
`totalBorrow`/`totalSupply` of ~88.77% (small diff = interest-accrual timing between the two reads, not a decoding
error); USDT=89.22%, native-ETH-sentinel (the real on-chain `borrowToken` for Fluid's "ETH" MVP vaults)=79.21%,
WSTETH=34.78%. A failed/untracked Liquidity Layer read now yields `None` (honest-absence), never a fabricated 0.0.
Shipped `market_tick_data_service/market_interface/adapters/defi/fluid_adapter.py` +
`fluid_liquidity_resolver.py` (new sibling module) @ `market-tick-data-service@4bb92b28`.

Gas-fee data is collected once per chain under the synthetic venue `ALCHEMY`, not once per protocol — a lending
protocol's chain being covered there is sufficient, it doesn't need its own `gas_fees` data type.

### Solana lending — MarginFi, Solend (shipped 2026-07-09)

Two real Solana lending adapters, wired the same way as the 9 EVM lending protocols above (real network fetch, real
A_TOKEN/DEBT_TOKEN split, real on-chain addresses) — documented here rather than under a separate "Solana DeFi" doc
since they're lending protocols first, Solana-native second.

| Protocol     | Venue             | Data source (real, live-verified 2026-07-09)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Scope                                                |
| ------------ | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| **MarginFi** | `MARGINFI-SOLANA` | MarginFi has no dedicated public REST API — bank state lives on-chain as Anchor accounts under the marginfi-v2 program (`MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA`); the officially-documented access path is the TS/Rust SDK reading those accounts directly. `marginfi.py` instead reads the same lightweight public JSON caches MarginFi's own `app.marginfi.com` frontend uses (`storage.googleapis.com/mrgn-public/mrgn-bank-metadata-cache.json` + `mrgn-token-metadata-cache.json`) — no API key, no RPC rate limits. Live-verified: 82 real Banks, 162 A_TOKEN/DEBT_TOKEN instruments (1 bank skipped honestly — its mint has no token-metadata-cache decimals entry, never a fabricated value). | 1 Solana Bank pool (no per-chain axis — Solana only) |
| **Solend**   | `SOLEND-SOLANA`   | Real public REST API: `api.solend.fi/v1/markets/configs?scope=solend&deployment=production`. Scoped to the primary/flagship market (`isPrimary=true`) — the same curation discipline Kamino (`status=LIVE`) applies, rather than surfacing all 202 raw markets including long-tail permissionless pools. Live-verified: 89 real reserves in the primary market, 178 A_TOKEN/DEBT_TOKEN instruments. Solend Labs rebranded its consumer product to "Save" in 2025; the on-chain program, canonical venue name, and this API host are unchanged.                                                                                                                                                              | 1 Solana primary market                              |

Both protocols' Banks/reserves are, by design, always both depositable AND borrowable (isolated cross-collateral
pooled lending, unlike Aave's per-reserve `borrowingEnabled` flag) — neither API exposes a per-reserve "borrow
disabled" signal, so both adapters unconditionally emit both legs per Bank/reserve. `available_from_datetime` resolves
per-instrument via Solana RPC creation-timestamp lookup (`batch_resolve_creation_timestamps`, same GCS-cached pattern
Kamino uses), falling back to the protocol's real mainnet launch date (Solend: 2021-08-13; MarginFi v2: 2023-07-01,
conservative) when RPC resolution is unavailable/rate-limited.

Wired into the venue registry the same way every other DeFi adapter is: `VENUE_TO_ADAPTER_KEY` (UAC,
`unified_api_contracts/registry/venue_adapter_keys.py`) maps `MARGINFI-SOLANA`/`SOLEND-SOLANA` → `marginfi`/`solend`;
`instruments-service`'s `factory.py::_ADAPTERS` maps those keys to the adapter classes; both venues were flipped from
`DEFI_VENUE_PHASE="pipeline"` to `"live"` (`unified_api_contracts/registry/defi_venues.py`) now that they're
IS-producible (`_build_defi_venues()` includes them via `engine/orchestrator/defi.py::_SOLANA_DEFI_VENUES`).

---

## Yield-bearing / LST / restaking

15 protocols in scope, split by real `ProtocolClass` in the capability registry:

| Class                                   | Protocols                                                       | Chains                                                                                                                                                                                                                                                                      |
| --------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Yield** (vaults / yield-tokenization) | Lido, EtherFi, Ethena, Convex, Idle, Yearn(\_V3), Beefy, Pendle | Lido/EtherFi/Ethena/Convex/Idle: Ethereum (Idle also Arbitrum); Yearn: Ethereum + Arbitrum; Beefy: Ethereum, Arbitrum, Base, BSC, Avalanche (Polygon deliberately excluded — every Polygon vault was `status=eol` on the curated snapshot date); Pendle: Ethereum, Arbitrum |
| **Restaking** (`SPOT_ASSET`-typed)      | EigenLayer, Symbiotic, Karak, Renzo, KelpDAO, Puffer            | EigenLayer/Symbiotic/KelpDAO/Puffer: Ethereum; Karak/Renzo: Ethereum + Arbitrum                                                                                                                                                                                             |
| **Staking** (native LST, non-restaking) | RocketPool                                                      | Ethereum                                                                                                                                                                                                                                                                    |

All of these are **static curated adapters** (Alchemy SDK + direct on-chain calls), not subgraph-driven — unlike DEX
pools and most lending protocols, there's no "top-N by TVL" query; each adapter has a fixed, hand-maintained token/vault
list. Required governance/reward tokens per protocol are declared alongside chain coverage in the same
`PROTOCOL_CAPABILITIES` registry (e.g. Lido requires `LDO`/`STETH`/`WSTETH` in the major-assets filter, EtherFi
requires `ETHFI`/`EETH`/`WEETH`).

---

## On-chain-perp DEXes

Covered in detail in "On-chain-perp DEXes — instrument key format" under Current-vs-target above.
Summary of what each venue actually supports today, confirmed in adapter code:

- **Hyperliquid**, **Aster**: perpetuals only — both adapters explicitly reject `OPTION`/`FUTURE` instrument-type
  filter requests with a `CapabilityResolutionError` ("does not support X instruments. Only PERPETUAL is available").
- **Extended-Starknet**, **Pacifica-Solana**, **Lighter-Zksync**: perpetuals only, same rejection pattern for any
  non-`PERPETUAL` filter.
- None of the 5 offer listed options contracts; this is a real capability limit, not an unimplemented gap.

---

## Filtering

### Major-assets whitelist

`DEFI_MAJOR_ASSET_SYMBOLS` (`unified_api_contracts/registry/defi_major_assets.py`) is the real current whitelist —
**this has grown substantially past the "55 curated symbols" the old docs cited** and now includes a large Solana
token set (JUP, RAY, ORCA, BONK, PYTH, JTO, WIF, HNT, RNDR, W, TENSOR, KMNO, DRIFT, and more) alongside the original
ETH/BTC/stablecoin/governance-token families. Recommend treating the file itself as the source of truth going forward
rather than re-copying a symbol count into docs, since it's actively growing.

**Token equivalence groups** (`TOKEN_EQUIVALENCE_GROUPS`) let the filter treat wrapped/staked variants as matches for
their base asset — e.g. a pool with `WETH`/`stETH`/`wstETH`/`cbETH`/`rETH`/`weETH` all pass if `ETH` is in the major
assets list, without every variant needing to be listed explicitly. Groups exist for ETH, BTC, USD (stablecoins), SOL,
MATIC, AVAX, BNB.

### DEX pool filter

Both `base_asset` AND `quote_asset` must be in the major-assets whitelist (with equivalence-group tolerance). Applies
to any venue matching `DEX_VENUE_KEYWORDS` (`unified_api_contracts/registry/defi_major_assets.py`) — this keyword set
has also grown past the old docs' `["UNISWAP", "CURVE", "BALANCER"]`: it now includes `PANCAKESWAP`, `SUSHISWAP`,
`AERODROME`, `CAMELOT`, `VELODROME`, `TRADERJOE`, `GMX`, plus the Solana DEXes `ORCA`, `RAYDIUM`, `KAMINO`.

### Lending market filter

Both `base_asset` (collateral) AND `quote_asset` (borrow asset) must be in the whitelist — tightened from base-only
after Morpho was found leaking exotic quote assets (JPYC, WARS, EURCV) through a base-only filter.

### Venue launch-date filtering + `createdAtTimestamp` filtering

Unchanged in principle from the prior docs: instruments from a venue are excluded before its real launch date
(`VenueMapping.venue_start_dates`), and DEX pools are further filtered by their on-chain `createdAtTimestamp` against
the target date for accurate historical snapshots. Chain-native tokens (e.g. `MATIC`/`WMATIC` on Polygon,
`BNB`/`WBNB` on BSC, `SOL`/`WSOL` on Solana) are auto-included in the major-assets filter for any protocol deployed on
that chain (`CHAIN_REQUIRED_TOKENS`).

---

## MVP universe

**A real, dedicated `MVP_SCOPE["defi"]` rule now exists (shipped 2026-07-09, `DeFiMvpRule` in
`unified_api_contracts/canonical/crosscutting/mvp_scope.py`, config version 13).** This supersedes the prior state
this section used to describe (no dedicated DeFi MVP set, distinct only from "every protocol the factory registry
knows how to instantiate") — that framing was operator-ruled on 2026-07-09
(`unified-trading-pm/plans/active/issues/defi_perp_funding_mvp_scope_contradiction_2026_06_29.md` §E5: _"DeFi MVP
framing — define for now, just keep all as MVP though"_).

**The ruling, in one line: DeFi MVP == "everything we capture."** Unlike CeFi/TradFi/Sports/Prediction (each a real,
deliberately curated/narrowed subset — e.g. CeFi's ~490-asset base universe, TradFi's CME-futures-only scope), DeFi
MVP is **not** narrowed at all — it is every IS-producible venue, every real instrument_type a live adapter emits,
and every DeFi data_type the pipeline produces. A deliberate, simple starting point, not a permanent design
(the config-version docstring notes a future ruling may narrow it the way CeFi/TradFi are narrowed).

All 3 axes are **derived**, not hand-curated literals, so the rule can never silently drift stale:

- **Venues** (57, up from a prior curated 11): `DeFiMvpRule.venues` == `VENUES_BY_ASSET_GROUP["defi"]` — the same
  "P" (IS-producible) set `_build_defi_venues()` computes in this repo, i.e. **exactly the venues this repo's own
  factory registry actually produces day-to-day**, not the broader `ALL_DEFI_VENUES` declarative UAC registry (which
  also carries "pipeline"-phase venues UAC has declared but this repo doesn't yet actually walk — e.g.
  `ROCKETPOOL-ETHEREUM` has a real, wired adapter but stays out of MVP because it isn't in the per-day venue walk;
  same for `YEARN_V3-*`, `CONVEX-ETHEREUM`, and the other curated-static-registry adapters documented above). This
  MVP⊆P invariant is deliberate (`instrument_universe_registry_consolidation_2026_06_29.md` Decision D) — MVP
  membership feeds the honest-coverage reachable denominator, so tagging a not-yet-producible venue MVP=true would
  mint a phantom expected-but-never-captured cell.
- **Instrument types** (9, up from a prior curated 4): every real `InstrumentType` value a live adapter actually
  emits — `POOL`, `LENDING`, `A_TOKEN`, `DEBT_TOKEN`, `LST`, `YIELD_BEARING`, `PERPETUAL`, `SPOT_PAIR`, `STAKING`
  (verified against live adapter code; drops the never-real `DEX_POOL` placeholder the prior rule carried).
- **Data types** (25, up from a prior curated 6): the full `DATA_TYPES_BY_ASSET_GROUP["defi"]` list — dex pool
  state/swaps, lending/utilization indices, LST rates, perp funding, oracle prices, gas fees, rewards/risk params,
  liquidation/flash-loan/bridge/mev/governance events, vault share price/APY/TVL, native staking rates, and more.

**MarginFi + Solend are in DeFi MVP** (decision #2 of the same operator ruling — "MARGINFI/SOLEND — yeah in MVP, fix
it") — both were flipped from `DEFI_VENUE_PHASE="pipeline"` to `"live"` in the same pass their real adapters shipped
(see "Solana lending — MarginFi, Solend" under Lending above), so they're automatically part of the derived 57-venue
MVP set with no separate hand-edit needed.

**Side effect worth flagging**: this ruling also resolves the previously-open
`defi_perp_funding_mvp_scope_contradiction_2026_06_29.md` contradiction as its "Option 2" — `PERPETUAL` is now a real
DeFi MVP `instrument_type`, so `DRIFT-SOLANA PERPETUAL perp_funding` evaluates `is_mvp()=True` (it previously
evaluated `False` under every prior rule version, the exact contradiction that issue tracked).

The 3 registries this section used to point at as "the real governing surface" (in lieu of a dedicated MVP rule)
remain real and unchanged — they're now the SOURCE the derived MVP rule reads from, not a substitute for one:

- `instruments_service/reference_data/factory.py`'s `_ADAPTERS` dict (every protocol key with a working adapter
  class — CeFi/DeFi/TradFi/prediction/sports).
- `unified_api_contracts/registry/capability_declarations/_defi.py`'s `PROTOCOL_CAPABILITIES` dict (which
  `InstrumentType`s, MTDS data types, and required tokens each protocol produces).
- `unified_api_contracts/registry/venue_adapter_keys.py`'s `VENUE_TO_ADAPTER_KEY` (the venue→adapter routing table).

The known-gaps list above (`EMPTY_OR_DEPRECATED_DEFI_VENUES` / `DEFI_INSTRUMENTS_NOT_YET_COLLECTED`) is still the
honest "what's expected vs. what's actually collected" answer for data STATE (has the backfill actually run), a
separate question from MVP SCOPE (is this cell supposed to be collected at all) that this section covers.

---

## Data sources and API keys

| Source                                                          | Used by                                                                           | Auth                                                                                                           |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| The Graph (`gateway.thegraph.com`)                              | Uniswap V2/V3/V4 + forks, Aave_V3, Compound_V3, Fluid, Radiant, Venus, Benqi, GMX | `thegraph-api-key` secret; free tier 100k queries/month, paid $2/100k queries (billed in GRT on Arbitrum)      |
| Balancer API v3                                                 | Balancer                                                                          | No key required                                                                                                |
| Curve REST API (`api.curve.finance`)                            | Curve (no subgraph, no RPC — pure REST)                                           | No key required                                                                                                |
| Goldsky                                                         | Euler_V2 (routed via an endpoint override, not the standard Graph gateway)        | —                                                                                                              |
| `blue-api.morpho.org` GraphQL                                   | Morpho (subgraph is the fallback, not primary)                                    | No key required                                                                                                |
| Alchemy SDK                                                     | Token metadata / contract resolution for the static yield/LST/restaking adapters  | `alchemy-api-key` secret; free tier 300M compute units/month                                                   |
| Hyperliquid / Aster / Extended / Pacifica / Lighter native REST | On-chain-perp DEXes                                                               | Public endpoints, no key needed for instrument discovery (trading credentials are execution-service's concern) |

**Never commit real API keys to `.env`** — configure secret _names_ only (`THEGRAPH_SECRET_NAME`,
`ALCHEMY_SECRET_NAME`), resolved via GCP Secret Manager at runtime.

---

## Schema notes

- `asset_group` is always `crypto` for everything in this doc.
- DeFi instruments have no `tick_size`/`lot_size` in the traditional sense — continuous liquidity, not an order book —
  though the current adapters do stamp a small fixed `tick_size`/`min_size` (e.g. `Decimal("0.000001")`) for downstream
  code that expects a non-null value; this is a placeholder convention, not a real market microstructure fact.
- Session/trading-hours metadata is always `None` — DeFi is 24/7 on-chain.
- `available_since` is populated from the on-chain `createdAtTimestamp` (DEX pools) or the adapter's own launch-date
  constant (static yield/LST adapters).
- `raw_symbol` carries the real on-chain address (pool, market, or token contract) — this is the field to use for
  execution routing / on-chain lookups, not the instrument key.

### `instrument_type` is a real GCS shard axis

MTDS GCS paths always include `instrument_type=` as a partition segment — two real shapes occur:
`.../venue=X/chain=Y/instrument_type=Z/data_type=W/...` and `.../venue=X/instrument_type=Z/data_type=W/...` (the
`chain=` segment is present only when it's distinct from `venue`, e.g. cross-chain data like `gas_fees`).
`instrument_type` is both part of the canonical `instrument_id` string (identity, e.g. the `POOL`/`A_TOKEN`/
`YIELD_BEARING` segment in `VENUE:TYPE:SYMBOL`) **and** a real physical GCS partition directory — these are two
separate, compatible facts (one about identity encoding, one about storage layout), not the same claim.

## Downstream: MTDS integration

DeFi instruments flow to market-tick-data-service via each adapter's declared `data_types` /`mtds_operations` in
`PROTOCOL_CAPABILITIES` (see the per-protocol tables above) — this replaces the old docs' hand-maintained "what MTDS
currently downloads" table, which drifts out of date every time a new data type ships. Query the registry directly
for the current answer rather than trusting a static table in a doc.

### Raw DEX swaps vs. OHLCV — not the CeFi/TradFi "OHLCV only as a fallback" pattern

Unlike CeFi/TradFi (where OHLCV bars only get synthesized when raw tick data isn't available), DeFi captures raw
per-swap events and hourly OHLCV as **two independently-fetched data_types today, not one derived from the other**:

- **Raw swaps** (`market_tick_data_service/cli/handlers/dex_swaps_handler.py`) — tick-level, per-transaction
  price/volume/liquidity-impact from each protocol's subgraph `swaps` entity (distinct from `dex_pools_handler.py`'s
  daily `poolDayDatas` aggregates). This is real, shipped, **batch-mode only**.
- **DEX OHLCV** (e.g. `uniswap_v3_adapter.py:687`, `download_market_data()`) — a _separately fetched_ hourly
  OHLCV/liquidity query straight from the subgraph's own pre-aggregated `poolHourData`-style entity, not computed
  locally from the raw swaps captured above. Whether OHLCV should instead be derived from the already-captured raw
  swaps (avoiding a second subgraph query for data that's arguably redundant) is an open architecture question.
- **Live (real-time, per-block) swap streaming — one venue shipped 2026-07-09, 21 still placeholder**
  (`instruments_docs_audit_outstanding_items_2026_07_08.md` finding C8). `live/connectors/dex_swap_scaffold_ws.py`
  originally covered all 22 real `PROTOCOL-CHAIN` venue keys under an explicit placeholder
  (`DexSwapPlaceholderWSFeedConnector`, Phase 3.5 / `wsfeedconnector_phase35_gap_2026_07_06.md`) whose `connect()`
  unconditionally raised `NotImplementedError("BLOCKED-BUILD...")` — it satisfied the live-connector Protocol surface
  just enough to move those venues from `blocked-not-registered` to `schema-only` in the smoke-matrix, without
  actually streaming swap data. A new `live/connectors/dex_swap_uniswap_v3_ws.py` now re-registers just the
  `UNISWAP_V3-ETHEREUM` row with a real connector (per the scaffold module's own documented follow-on mechanism —
  the other 21 venues are untouched). It polls the real Uniswap V3 Ethereum subgraph every 15s for `dex_pool_swaps`
  (+ every 30s for `dex_pool_state`), reusing the exact same GraphQL field shape + `SubgraphService` client the
  batch `uniswap_v3_adapter.py` already relies on — deliberately polling-based rather than a raw `eth_subscribe`
  Swap-event log decode, so live and batch read the identical data source (this workspace's "Live = batch" hard
  rule). Instrument IDs match the batch adapter byte-for-byte:
  `UNISWAP_V3-ETHEREUM:POOL:{base}-{quote}:{fee}@ETHEREUM`. **Known gap, explicit**: a live authenticated
  round-trip against the real Graph-Network gateway could not be verified in the building agent's sandbox (no
  Secret-Manager access there for a real `thegraph-api-key`); query-field correctness was instead verified by
  construction against this repo's own already-shipped batch query + UAC's `GraphPoolHourData` schema. Separately
  discovered (not fixed, out of this fix's scope): `SubgraphService`'s pre-existing free-tier Studio fallback URL
  for `uniswap_v3` returns a real HTTP 404 today (The Graph's old hosted service is fully sunset) — the new
  connector degrades safely against that failure (logged warning, empty tick list, reconnect-flag backoff, no fake
  ticks), matching every other real DeFi live connector's honest-absence behavior in this repo. "Swap rates each
  block" for the remaining 21 `PROTOCOL-CHAIN` keys is still the real target architecture but not shipped; today's
  real-time DeFi capture for those is batch/polling-based (subgraph queries on a schedule), not a genuine live feed.
  Shipped `market-tick-data-service@d02cf88f`.

---

## Related documents

- Instrument ID grammar, shared types, and the CeFi-side `PERPETUAL`/`SPOT_PAIR` conventions this doc's targets align
  to: the consolidated instrument-ID specification doc.
- Full canonicalization decision + migration sequencing (UAC → instruments-service → MTDS → strategy-service →
  deployment-api/UI): `unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`.
- Underlying 7-agent compliance audit (P0 live bugs, full P1/P2 finding list):
  `unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md`.
- A_TOKEN/DEBT_TOKEN lending-split decision origin:
  `unified-trading-pm/plans/active/issues/defi_lending_atoken_debttoken_instrument_split_2026_07_07.md`.
- `codex/04-architecture/instruments-service-as-ssot-for-mtds.md`,
  `codex/04-architecture/instrument-universe-registry-consolidation.md` — venue-list/adapter-key ownership rules.
