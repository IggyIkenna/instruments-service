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
(Drift, Raydium, Orca, Phoenix, Jupiter, Mango, Zeta, Flash_Trade, Meteora, Lifinity, Kamino, Camino) and Solana
staking/restaking (Marinade, Jito, JitoRestaking, Sanctum, Solblaze, Solana-native, MarginFi, Solend) — these have a
real adapter presence too (see `instruments_service/reference_data/adapters/defi/`) but are organizationally a
separate "Solana DeFi" surface from this doc's EVM-centric + on-chain-perp scope, per the 2026-07-08 docs-consolidation
task split.

---

## Instrument ID format: current state vs. decided target (not yet implemented)

On 2026-07-08 the operator reviewed real production `catalog.parquet` samples and **decided target canonical formats**
for several real divergences — decisions are final, but **no migration has shipped yet**. Everything below is
current-vs-target, not current-vs-already-fixed. Full detail, decision rationale, and open todos:
`unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`; underlying 7-agent audit:
`unified-trading-pm/plans/audit/results/canonical_instrument_id_audit_2026_07_08.md`.

### DEX pools (finding 2)

- **Target** (operator-decided): `VENUE-CHAIN:POOL:TOKEN0-TOKEN1[-FEE_TIER]`, fee tier a real Uniswap-V3-style
  basis-point value (100/500/3000/10000), dash-separated. `pool_address` stays its own column, it stops being the
  entire identity key.
- **Audit's stated current-state**: a bare on-chain pool address with zero `VENUE:TYPE:SYMBOL` structure, confirmed
  across 6,180 real rows / 13 protocols (Uniswap V2/V3/V4, Balancer, Curve, PancakeSwap_V3, Sushiswap/\_V3, Camelot_V3,
  Aerodrome_V3, TraderJoe_V2, Velodrome_V2, GMX) in the production catalog.
- **Reconciled 2026-07-08**: the current adapter code for the native-schema DEX adapters already builds a structured
  key, not a bare address — `instrument_key = f"{venue_tag}:POOL:{base}-{quote}:{fee_str}"` (`uniswap_v3.py:490-492`,
  and the equivalent in `uniswap_v2.py`/`uniswap_v4.py`/`balancer.py`/`curve.py`) — with the pool/market address kept
  separately as `raw_symbol`. But re-reading the REAL, CURRENT `prod/catalog.parquet` directly (7,284 DeFi rows,
  2,030 real Uniswap V3 rows) confirms the audit's bare-address finding is still accurate for the **persisted data** —
  every real row shows the bare pool address as `instrument_id` and a bare `UNISWAP_V3` venue with **no `-ETHEREUM`
  chain suffix at all** — a shape the current adapter code doesn't even produce anymore (it always builds
  `venue_tag = f"{prefix}-{chain}"`). This means the persisted catalog predates the current adapter code (or was
  built by an older/different write path) and has never been regenerated since. **This is a data-regeneration/backfill
  gap layered on top of 2 real remaining code gaps, not a from-scratch code problem**: (a) the code's own fee-tier
  segment uses a colon (`:{fee_str}`) where the target wants a dash, and (b) `fee_str` embeds Uniswap's raw feeTier
  units (e.g. `3000`) rather than real basis points (a correct bps value is computed separately as
  `pool_fee_tier_bps` on the same record but isn't the one written into the instrument_key string). Only Uniswap V3
  was re-checked against the live catalog for this reconciliation — the other 12 protocols in finding 2's scope
  haven't been individually re-verified against current code, so don't assume the same shape holds for all of them.

### Lending — A_TOKEN/DEBT_TOKEN split (from `defi_lending_atoken_debttoken_instrument_split_2026_07_07.md`)

Real per-protocol current state, verified directly against adapter code:

| Protocol        | Current key shape                                                                        | Current `instrument_type`                                       | Status                                                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| **AAVE_V3**     | `{venue}:A_TOKEN:{a_symbol}` / `{venue}:DEBT_TOKEN:{debt_symbol}` (`aave_v3.py:424,433`) | `LENDING` (hardcoded, `aave_v3.py:400`)                         | Split correct, field mislabeled (cheap fix — downstream ledger resolution already parses the key, not the field)         |
| **SPARK**       | Same pattern (`spark.py:318,327`)                                                        | `LENDING`                                                       | Same mislabel as AAVE_V3                                                                                                 |
| **COMPOUND_V3** | `{venue}:SUPPLY:{symbol}` / `{venue}:BORROW:{symbol}` (`compound_v3.py:263,272`)         | `SUPPLY`/`BORROW` — **not valid `InstrumentType` enum members** | Real crash risk: `asset_class_for_instrument_type()` raises `UnknownInstrumentTypeError` on unrecognized types by design |
| **MORPHO**      | `{venue}:LENDING_MARKET:{collateral}-{loan}:{market_key[:8]}` (`morpho.py:190-191`)      | `LENDING`                                                       | No supply/borrow split at all — one flat record per market                                                               |
| **EULER_V2**    | `{venue}:LENDING_MARKET:{symbol}` (`euler_v2.py:93`)                                     | `LENDING`                                                       | Same no-split gap as Morpho                                                                                              |
| **FLUID**       | `{venue}:LENDING_MARKET:{symbol}` (`fluid.py:113`)                                       | `LENDING`                                                       | Same no-split gap                                                                                                        |
| **RADIANT**     | `{venue}:LENDING_MARKET:{symbol}` (`radiant.py:121`)                                     | `LENDING`                                                       | Same no-split gap                                                                                                        |
| **VENUS**       | `{venue}:LENDING_MARKET:{symbol}` (`venus.py:105`)                                       | `LENDING`                                                       | Same no-split gap                                                                                                        |
| **BENQI**       | `{venue}:LENDING_MARKET:{symbol}` (`benqi.py:98`)                                        | `LENDING`                                                       | Same no-split gap                                                                                                        |

**Target (operator-decided 2026-07-08)**: every lending protocol's adapter emits exactly two `InstrumentRecord`s per
position-bearing entity — `A_TOKEN` for the supply side, `DEBT_TOKEN` for the borrow side — with the field matching
the key. No protocol keeps a bespoke type name (`SUPPLY`/`BORROW`, `LENDING_MARKET`, or a `LENDING` field mislabel)
once this lands. This generalizes the fix from "AAVE_V3/SPARK/COMPOUND_V3/MORPHO only" to all 9 lending protocols in
this doc's scope. The strategy/execution layer already assumes this split exists
(`unified_api_contracts/internal/domain/execution_service/defi_position.py:97-109`'s `is_supply`/`is_borrow`;
`PositionPortfolio.net_value = total_supply_value - total_borrow_value`) — this is a reference-data-layer catch-up,
not a new architectural decision. Fixing is staged per-protocol, not a single PR (operator: "fixing will be in stages
ofc"). MARGINFI/SOLEND (Solana lending, out of this doc's scope) have no instruments-service adapter at all yet — a
separate gap from the split question.

### On-chain-perp DEXes — PERP-vs-PERPETUAL key/field mismatch + base-quote inconsistency (findings 3+4) — FIXED 2026-07-08

All 5 on-chain-perp adapters live under `reference_data/adapters/cefi/` (not `defi/`) organizationally, despite being
economically on-chain perpetual DEXes — a real filing quirk worth knowing when hunting for the code. All 5 previously
stamped `instrument_type=InstrumentType.PERPETUAL` correctly in the field while using the `PERP` shorthand (and an
inconsistent base-quote shape) in the key — the mismatch was between key and field, not a field-level bug.

**Shipped**: `VENUE:PERPETUAL:BASE-QUOTE` uniformly, dropping the `PERP` shorthand, with the REAL per-venue settlement
currency (confirmed live 2026-07-08 — method noted per row; HYPERLIQUID/EXTENDED-STARKNET were already confirmed USD
in an earlier session pass):

| Venue               | Real settlement currency + verification                                                                                                                                    | Before → after (`instrument_key`)                                                        |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `HYPERLIQUID`       | USD (notional quote; vault collateral is USDC — pre-confirmed)                                                                                                             | `HYPERLIQUID:PERP:BTC` → `HYPERLIQUID:PERPETUAL:BTC-USD`                                 |
| `ASTER`             | Real per-symbol `quoteAsset`, confirmed live `fapi.asterdex.com/fapi/v1/exchangeInfo`: 504/509 real perps quote USDT, 3 quote USD1, 2 quote a bare "U" — no longer assumed | `ASTER:PERP:BTCUSDT` → `ASTER:PERPETUAL:BTC-USDT` (per-symbol real quote, not hardcoded) |
| `PACIFICA-SOLANA`   | USDC, confirmed live `docs.pacifica.fi/trading-on-pacifica/unified-margin`: "Pacifica users' account's USDC balance, unrealized PnL... are margined together"              | `PACIFICA-SOLANA:PERP:SOL-PERP` → `PACIFICA-SOLANA:PERPETUAL:SOL-USDC`                   |
| `EXTENDED-STARKNET` | USD (pre-confirmed live: `collateralAssetName="USD"` uniformly across markets) — already dash-normalized pre-fix, only the `PERP`→`PERPETUAL` rename applied               | `EXTENDED-STARKNET:PERP:ETH-USD` → `EXTENDED-STARKNET:PERPETUAL:ETH-USD`                 |
| `LIGHTER-ZKSYNC`    | USDC, confirmed live `docs.lighter.xyz/trading/multi-asset-margin`: "Portfolio Balance is the USDC value of the account including unrealized PnL on perpetual positions"   | `LIGHTER-ZKSYNC:PERP:BTC` → `LIGHTER-ZKSYNC:PERPETUAL:BTC-USDC`                          |

Note the venue-naming asymmetry too: `HYPERLIQUID`/`ASTER` carry no chain suffix (each is effectively its own
app-chain — `chain="HYPERLIQUID"` lives in the instrument's `chain` attribute, not the venue token), while
`PACIFICA-SOLANA`/`EXTENDED-STARKNET`/`LIGHTER-ZKSYNC` carry an explicit chain suffix in the venue itself. Both are
real, intentional, just asymmetric — not itself part of the PERP/PERPETUAL finding.

**No trailing `@VENUE`** on top (operator explicitly rejected that pattern — venue is already the first colon-segment).
Live connectors (MTDS `hyperliquid_ws.py`/`hyperliquid_l2book_ws.py`/`hyperliquid_ticker_ws.py`/`aster_book_liq_ws.py`)

- the onchain-perp batch handler's catalogue-driven symbol enumeration were updated in the same pass to keep live=batch
  consistent — see `market-tick-data-service@c20ea464`. Historical batch tick-data GCS objects + the availability
  manifest were NOT yet migrated in this pass (real, non-trivial volume — a dry-run-scoped migration script + follow-up
  apply is tracked as its own todo, not silently skipped); see
  [`canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md`](../../../unified-trading-pm/plans/active/canonical_id_p1_onchain_perp_perp_shorthand_2026_07_08.md).
  Shipped: `instruments-service@f7cf3ea5` (the 5 adapters above), `unified-api-contracts@58a03793` (ASTER's UAC
  normalize.py, same fix for the live/WS-tick normalization path).

### AAVE_V3-OPTIMISM misspelled venue-token duplicate (finding 5) — FIXED 2026-07-08

`AAVEV3-OPTIMISM` (missing underscore, 4 real rows) coexisted with the correctly-spelled `AAVE_V3-OPTIMISM` (12 real
rows) in production, fragmenting the real per-chain reserve set into 2 disjoint keys. Root-cause check: the CURRENT
`aave_v3.py` adapter code already builds the correct spelling (`venue_tag = f"{self._venue_prefix}-{self._chain}"` →
`AAVE_V3-OPTIMISM`, since the underscore in `AAVE_V3` is part of the protocol name, not a joiner) — confirmed no
caller anywhere passes a misspelled `protocol_slug`. The 4 ghost rows (`AAAVE`/`ALINK`/`ALUSD`/`ASUSD` reserves, since
retired from the curated static-reserve list) were pure historical drift: a prior `collapse_defi_drift_to_canonical_
2026_06_25.py` pass had already normalised the `venue`/`chain` COLUMNS (all 16 rows show `venue=AAVE_V3
chain=OPTIMISM`) but never touched the `instrument_id` STRING column itself, leaving the ghost prefix baked into 4
`instrument_id` values even though the row's own `venue` column was already correct.
**Fixed**: consolidated all 16 rows under `AAVE_V3-OPTIMISM:` — no adapter-code change needed (already correct
go-forward); `instruments-service/scripts/instrument_id_venue_spelling_backfill_2026_07_08.py` relabelled the 4
stale `instrument_id` strings in place directly on `gs://instruments-store-defi-prd-central-element-323112/
prod/catalog.parquet` (backup: `prod/catalog.20260708-184138.venuefix.bak.parquet`), re-deriving the corrected key
from the row's own already-correct `venue`/`chain` columns — no re-download from Aave. Verified post-write: 0 rows
matching the `AAVEV3-OPTIMISM:` prefix, 16 rows under `AAVE_V3-OPTIMISM:`, total row count unchanged (7,284), no new
duplicate `instrument_id` introduced. The registry layer separately tracks a related-but-distinct problem — bare
`AAVEV3` (no chain suffix) is listed in `DEPRECATED_DEFI_GHOST_VENUE_NAMES`
(`unified_api_contracts/registry/capability_declarations/_defi_coverage.py`) as an already-known ghost superseded by
`AAVE_V3` — already confirmed sufficient for the one live prefix-matching consumer
(`deployment-api/deployment_api/services/data_status/rollup_cache.py::strip_defi_ghost_venues`, which strips on
`venue.split("-", 1)[0]`, so `AAVEV3-OPTIMISM` was already filtered there even before this fix); no separate registry
change was needed for that consumer.

### MORPHO market-address disambiguator uses a reserved delimiter (finding 6) — FIXED 2026-07-08

Confirmed in real code, not just captured data: `morpho.py:191` built
`instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}:{market_key[:8]}"` — a 3rd colon inside the symbol segment,
ambiguous to any naive `split(":")` parser since colon is the reserved top-level `VENUE:TYPE:SYMBOL` delimiter.
**Fixed**: `morpho.py` now dash-separates instead —
`instrument_key = f"{venue_tag}:LENDING_MARKET:{symbol}-{market_key[:8]}"` (e.g.
`MORPHO-BASE:LENDING_MARKET:USDC-EURC-0x305dd1`). All 466 real captured Morpho rows in production (100% of the
venue's catalog, spanning Ethereum + Base) carried the old 3-colon shape — relabelled in place by the same
`instrument_id_venue_spelling_backfill_2026_07_08.py` script (last colon → dash), verified zero collisions (dash-join
produces no duplicate `instrument_id` across the 466 rows) and zero remaining 3-colon Morpho rows post-write.

### YEARN vs YEARN_V3 canonical venue-prefix contradiction — FIXED 2026-07-08

Real registry contradiction found writing this doc (see "Systemic venue-token duplicate-spelling pattern" below for
the full evidence + decision rationale): `yearn.py`'s adapter code hardcoded `venue_tag = f"YEARN-{self._chain}"`
(bare `YEARN`), which matched only `VENUE_TO_ADAPTER_KEY`'s 2 stale entries — every other UAC registry
(`defi_venue_capabilities.py`, `venue_launch_dates.py`, `defi_venues.py`, `PROTOCOL_CAPABILITIES`, `chain_env.py`,
`DEPRECATED_DEFI_GHOST_VENUE_NAMES`) already treated `YEARN_V3` as canonical. **Fixed**: `yearn.py` now builds
`venue_tag = f"YEARN_V3-{self._chain}"`; `VENUE_TO_ADAPTER_KEY` (`venue_adapter_keys.py`) relabelled
`YEARN-ETHEREUM`/`YEARN-ARBITRUM` → `YEARN_V3-ETHEREUM`/`YEARN_V3-ARBITRUM`. No production-catalog backfill was
needed — 0 real Yearn rows exist in `prod/catalog.parquet` today (confirmed directly), so this is a pure go-forward
fix, not a data migration. **Known follow-up, out of this fix's repo scope**: `market-tick-data-service`'s
`vault_yearn_adapter.py:37` and `execution-service`'s `defi_execution/protocols/yearn.py:50` +
`defi_execution/protocols/base.py:100` still hardcode the now-superseded bare `"YEARN-ETHEREUM"` — per the
canonicalization doc's explicitly-authorized staged migration order (UAC → instruments-service → MTDS →
strategy-service → deployment, "live breakage explicitly authorized"), those 2 repos need a follow-up fix to match
before Yearn capability/venue-gated checks are consistent end-to-end. Not fixed here (outside this pass's repo
scope: instruments-service + unified-api-contracts only).

---

## Systemic venue-token duplicate-spelling pattern

Beyond AAVE_V3/AAVEV3 above, the audit found this class is systemic: `MORPHO_VAULTS`/`MORPHOVAULTS`,
`YEARN_V3`/`YEARNV3`, `UNISWAP_V3`/`UNISWAPV3` (real, though the last 2 are acknowledged only in a code comment inside
a manifest-rebuild script, never fixed at the GCS-object-path level — that's a different bucket/layer
(`market-tick-data-service`'s manifest) than the `instruments-service` catalog this doc covers, out of this pass's
repo scope). The registry already carries a real `DEPRECATED_DEFI_GHOST_VENUE_NAMES` frozenset (`_defi_coverage.py`)
that the manifest consolidator and data-status service both filter against at read time: `UNISWAPV3`/`UNISWAPV2`/
`UNISWAPV4`, `AAVEV2`/`AAVEV3`, `CAMELOTV3`, `COMPOUNDV3`, `MORPHO_VAULTS` (legacy underscore form superseded by the
fully-glued `MORPHOVAULTS`), `PANCAKESWAPV3`, `SUSHISWAPV3`, `VELODROMEV2`, `TRADER_JOEV2`/`TRADERJOEV2`, `YEARNV3`,
`AERODROMEV3`. This filters the symptom downstream (keeps ghost names out of the manifest/UI) but doesn't stop new
duplicate writes at the adapter/writer source — the underlying fix is still the canonicalization migration above.

**YEARN vs YEARN_V3 — RESOLVED 2026-07-08.** Yearn's canonical venue prefix disagreed across registries: `VENUE_TO_
ADAPTER_KEY` (`unified_api_contracts/registry/venue_adapter_keys.py`) declared static entries `YEARN-ETHEREUM`/
`YEARN-ARBITRUM` → adapter key `yearn`, while `PROTOCOL_CAPABILITIES` (`_defi.py`) declared the protocol under key
`yearn_v3` with `venue_prefix="YEARN_V3"`, and `DEPRECATED_DEFI_GHOST_VENUE_NAMES` separately listed `YEARNV3` as a
ghost superseded by `YEARN_V3`. **Decision evidence** (real registry-consumer count, since 0 real Yearn rows exist in
`prod/catalog.parquet` today to compare row counts on): `YEARN_V3` is the canonical form in 6 separate real
registries/locations — `defi_venue_capabilities.py` (3 chain entries: ETHEREUM/ARBITRUM/OPTIMISM),
`venue_launch_dates.py`, `defi_venues.py` (8 entries across `ALL_DEFI_VENUES` / `LEGACY_DEFI_VENUE_ALIASES` /
`DATA_SOURCE_MAP`), `_defi.py`'s `PROTOCOL_CAPABILITIES`, `chain_env.py` (3 entries), and
`DEPRECATED_DEFI_GHOST_VENUE_NAMES` (which explicitly names `YEARNV3` — the glued form — as the retired ghost of
`YEARN_V3`, confirming the underscore form is the one this registry's own design treats as real). `VENUE_TO_ADAPTER_
KEY`'s 2 bare `YEARN-*` entries (no `OPTIMISM` entry even, unlike the other 6 registries) were the sole outlier — and
the real adapter bug this uncovered: `yearn.py` itself emitted `venue_tag = f"YEARN-{self._chain}"`, matching ONLY
that one outlier registry and none of the other 6. **Fixed**: `yearn.py` → `YEARN_V3-{chain}`; `VENUE_TO_ADAPTER_KEY`
→ `YEARN_V3-ETHEREUM`/`YEARN_V3-ARBITRUM`. See "Instrument ID format" above for the full writeup + the MTDS/
execution-service follow-up this surfaces.

**Post-fix full-catalog scan (2026-07-08)**: re-ran the same ghost-detection check
(`canonicalize_defi_venue_combined` applied to every real `instrument_id` prefix in `prod/catalog.parquet`, 7,284
rows) after the AAVE_V3-OPTIMISM + MORPHO fixes above landed — 0 remaining venue-prefix mismatches found anywhere in
the live DeFi catalog. The `MORPHO_VAULTS`/`MORPHOVAULTS`/`UNISWAP_V3`/`UNISWAPV3` instances named above are real but
live in the MTDS manifest / GCS-object-path layer (a different repo/bucket, out of this pass's scope), not in this
catalog. Also checked and explicitly ruled out as a same-class bug (informational, not touched — the file is CeFi-
classified, not a DeFi adapter): `EXTENDED` (1 row, `venue=EXTENDED chain=STARKNET`) vs `EXTENDED-STARKNET` (14 rows,
`venue=EXTENDED-STARKNET chain=""`) is a `venue`/`chain` COLUMN-split inconsistency, not an `instrument_id` spelling
duplicate — the `instrument_id` string itself is identical (`EXTENDED-STARKNET:PERP:...`) in both cases, so it's a
different bug class living in `reference_data/adapters/cefi/extended.py`, outside this pass's DeFi-adapter-naming
scope.

**Key-vs-field abbreviation mismatch, extended pattern — FIXED 2026-07-08 for this doc's 12 in-scope protocols.**
Every yield/LST/restaking adapter in this doc's scope stamped `LST` or `VAULT` in the instrument key while the
`instrument_type` field said `YIELD_BEARING` — e.g. `lido.py:90,94` (`{venue}:LST:{symbol}` / `YIELD_BEARING`),
`etherfi.py`, `renzo.py`, `kelpdao.py`, `puffer.py`, `rocket_pool.py` (all `:LST:`); `yearn.py`, `beefy.py`,
`karak.py`, `idle.py`, `symbiotic.py`, `convex.py` (all `:VAULT:`). Same divergence class as PERP-vs-PERPETUAL, but
**which side won differs per group — checked real downstream consumers before picking, per the PERP/PERPETUAL
precedent's own caveat**:

- **The 6 `LST`-keyed adapters (Lido, EtherFi, Renzo, KelpDAO, Puffer, RocketPool): the FIELD was fixed to match the
  KEY**, i.e. `instrument_type` now stamps `InstrumentType.LST` (a real, distinct enum member — not a shorthand for
  `YIELD_BEARING`). This is the OPPOSITE direction from the PERP/PERPETUAL fix, for good reason: `LST` is a real
  `InstrumentType` enum member (`unified_api_contracts/_instrument_enums.py`) with its own real downstream ledger
  treatment — `ledger_asset_resolution.py` maps `InstrumentType.LST → LedgerAssetClass.LST` and
  `InstrumentType.YIELD_BEARING → LedgerAssetClass.VAULT_SHARE`, a real accounting distinction, not a cosmetic one —
  and real consumers (execution-service's `catalog_validator.py`/`dependency_validator.py`/`data_availability_
validator.py`, strategy-service's `pnl_calculator.py`/`settlement_service.py`/`risk_monitor.py`) already parse the
  KEY's `:LST:` segment directly for real validation and PnL/risk logic, so the field — which was the one lying —
  was fixed to match. The `get_instruments(instrument_type=...)` filter guard was widened (not narrowed) to accept
  either `InstrumentType.LST` or `InstrumentType.YIELD_BEARING`, so no existing caller regresses.
- **The 6 `VAULT`-keyed adapters (Yearn, Beefy, Karak, Idle, Symbiotic, Convex): the KEY was fixed to match the
  FIELD**, i.e. the key's middle segment is now `YIELD_BEARING`, dropping the `VAULT` shorthand. Unlike `LST`,
  `VAULT` is NOT a real `InstrumentType` enum member (checked: no `InstrumentType.VAULT` exists), and no real
  consumer in instruments-service/strategy-service/execution-service parses a `:VAULT:` key-substring for
  classification — so there was no real consumer to break, and the field (`YIELD_BEARING`, already correct) won.
- **Sanctum, Solblaze, and Jito-restaking were checked and intentionally NOT touched** — all 3 are explicitly
  out-of-scope Solana venues per this doc's own scope section (a separate "Solana DeFi" surface), and share the
  same `LST`/`VAULT` pattern; flagged for whichever future pass covers Solana-native DeFi, along with the 10 more
  Solana/DeFi venues (drift, mango, zeta, flash_trade, meteora, jupiter, phoenix, lifinity, kamino, marinade) the
  original audit found with the same mismatch class.
- **Known related-but-unfixed finding**: `market-tick-data-service` has its own SEPARATE, independently-written
  adapters for some of these same protocols (`restaking_karak_adapter.py`, `vault_pendle_adapter.py`,
  `restaking_symbiotic_adapter.py`, `restaking_jito_adapter.py` — all still build `:VAULT:` keys) — checked and
  confirmed these are NOT wired into MTDS's `factory.py` dispatch table (no `_ADAPTERS`-equivalent entry references
  them; only reachable from their own dedicated unit tests), so they appear to be orphaned/unwired scaffolding, not
  a live parallel path that would now disagree with instruments-service's fixed convention. Not touched in this
  pass (out of this doc's repo scope) — worth a cleanup pass if/when they're ever wired in.

---

## DEX pools

### Adapter architecture: 5 native adapters, 7 protocols reuse one via config

Uniswap V2, Uniswap V3, Uniswap V4, Balancer, and Curve each have a dedicated adapter class. Seven more DEX
protocols/forks (PancakeSwap_V3, Sushiswap_V3, Sushiswap, Aerodrome_V3, Camelot_V3, Velodrome_V2, TraderJoe_V2) plus
GMX are **not** separate adapter classes — they all instantiate `UniswapV3ReferenceDataAdapter` with a
`protocol_slug` constructor argument (`uniswap_v3.py:118,123`) that swaps the venue prefix and the subgraph-ID lookup
table. The adapter has a 3-tier subgraph-schema cascade (primary schema → Algebra CL fallback → SushiSwap-pairs
fallback) to handle the fact these forks don't all use an identical Graph schema. This is real, working
architecture — not a gap — but means "13 DEX protocols" in the audit's finding 2 is really "5 adapter classes + 8
config variants of one of them."

### Protocol × chain coverage (real, from `SUBGRAPH_IDS`)

| Protocol              | Chains with a live subgraph ID today                                                                                                 |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Uniswap V2            | Ethereum                                                                                                                             |
| Uniswap V3            | Ethereum, Arbitrum, Base, Optimism, Polygon (Polygon subgraph returns 0 instruments — see gaps below)                                |
| Uniswap V4            | Ethereum                                                                                                                             |
| Balancer              | Ethereum, Arbitrum, Polygon, Optimism, Avalanche, Base                                                                               |
| Curve                 | Ethereum, Optimism, Avalanche (Arbitrum/Polygon only exist on the deprecated hosted service — use `api.curve.fi` instead, not wired) |
| PancakeSwap_V3        | BSC, Ethereum, Base                                                                                                                  |
| Sushiswap_V3          | Ethereum, Base, Avalanche                                                                                                            |
| Sushiswap (legacy V2) | Arbitrum                                                                                                                             |
| Aerodrome_V3          | Base                                                                                                                                 |
| Velodrome_V2          | Optimism (subgraph has 881 instruments but zero parquets ever written — see gaps below)                                              |
| Camelot_V3            | Arbitrum                                                                                                                             |
| TraderJoe_V2          | Avalanche                                                                                                                            |
| GMX                   | Arbitrum, Avalanche (Avalanche has ~1 instrument / minimal historical data)                                                          |

### Known gaps (real, tracked in `_defi_coverage.py` — this is the SSOT data-status reads, not a TODO list to

re-derive)

- **Empty/deprecated subgraphs** (`EMPTY_OR_DEPRECATED_DEFI_VENUES`): `TRADER_JOE_V2-AVALANCHE` (0 instruments),
  `UNISWAP_V3-POLYGON` (subgraph returns 0), `GMX-AVALANCHE` (minimal/no historical parquets).
- **Subgraph exists, never onboarded** (`DEFI_INSTRUMENTS_NOT_YET_COLLECTED`): `VELODROME_V2-OPTIMISM` (881
  instruments available, zero parquets written), `SPARK-ETHEREUM` (adapter shipped, subgraph has 17 markets, no
  historical parquets yet), `SANCTUM-SOLANA` / `SOLBLAZE-SOLANA` (out of this doc's scope — Solana LST adapters not
  built yet). data-status intentionally does not flag these as "missing" until the first real write lands.

### Fee tiers

Uniswap V3's `feeTier` field is hundredths-of-a-basis-point on the wire (500 = 0.05%, 3000 = 0.3%, 10000 = 1%); the
adapter converts to whole basis points for the canonical symbol. V2-style pools (Uniswap V2, most forks) imply a flat
0.3% fee with no on-chain fee-tier field.

### Subgraph fetch: TVL-ranked, not exhaustive — real ceiling is ~6,000 pools, not the old docs' "500"

DEX pool adapters (Uniswap V2/V3/V4, and the config-variant protocols that reuse `UniswapV3ReferenceDataAdapter`) query
the subgraph with `orderBy: totalValueLockedUSD, orderDirection: desc`, paginated (`uniswap_v3.py:55,57`:
`_FETCH_LIMIT = 1000` per page, `_MAX_SKIP = 5000` → up to 6 pages, so up to **~6,000** pools fetched per protocol per
run, ordered highest-TVL-first). **The old pre-consolidation docs cited a stale "top-500-by-TVL" figure** — that
number no longer matches the real code. The major-assets whitelist filter (below) then runs on TOP of this
TVL-ranked fetch, not instead of it — it's a two-stage pipeline: (1) fetch the highest-TVL pools up to the real
ceiling, (2) keep only the ones where both `base_asset` and `quote_asset` pass the whitelist.

**Coverage gap — PARTIALLY FIXED 2026-07-08 (Uniswap V3 + the 8 shared-class protocols only).** A genuine major-asset
pool (e.g. a real DAI/USDT pool on a smaller-TVL fork) that ranks below the ~6,000-pool cutoff on a given day was
never fetched at all, so the whitelist filter never got a chance to keep it — the TVL ceiling was a hard cutoff
applied _before_ curation, not a fallback safety net for curated pairs. **Fix implemented**: `uniswap_v3.py` now runs
a supplementary query (`_fetch_major_asset_pools`) that asks the subgraph directly for pools where both `token0` AND
`token1` are in `DEFI_MAJOR_ASSET_ADDRESS_LIST` (`unified_api_contracts/registry/defi_major_assets.py` — an
Ethereum-mainnet address list that already existed in the registry for exactly this purpose but had never been wired
into any adapter), merging any pools the TVL-ranked cascade missed into the result set. **Verified live against the
production gateway (2026-07-08)**: this query found a real `DAI-USDT` pool
(`0x3196f48548c3b8c901bc4cc5ad662ba97c9c0b2b`, feeTier 10000) with `totalValueLockedUSD ≈ $0.0004` — many orders of
magnitude below the ~$2,195 TVL of the pool ranked #6000 in the plain TVL-ranked pagination, proving the old pipeline
would have silently dropped it; a full live `get_instruments()` run went from 5,958 pools (TVL-ranked only) to 6,169
(+172 major-asset pools recovered) on Ethereum mainnet. Because this fires on top of (not instead of) the existing
cascade, it also transitively covers the 8 protocols that share `UniswapV3ReferenceDataAdapter` via `protocol_slug`
(PancakeSwap*V3, Sushiswap_V3, Sushiswap, Camelot_V3, Aerodrome_V3, Velodrome_V2, TraderJoe_V2, GMX) whenever they run
on Ethereum. **Known remaining gaps, not fixed in this pass**: (1) `DEFI_MAJOR_ASSET_ADDRESS_LIST` is Ethereum-only —
the same 9 protocols still have the uncovered TVL-ceiling gap on every other chain (Arbitrum, Base, Optimism, Polygon,
BSC, Avalanche, zkSync); (2) Uniswap V2, Uniswap V4, Balancer, and Curve (4 separate adapter classes, each with their
own independent fetch/pagination code) were not touched — they still have the original TVL-rank-then-filter gap on
every chain. A full fix would need either a per-chain major-asset address registry (doesn't exist today, would need
real per-chain token address verification before use) or a symbol-based nested-entity query
(`where: { token0*: { symbol_in: [...] } }`) applied to all 4 remaining adapter files.

---

## Lending

9 protocols in scope: Aave_V3, Spark, Compound_V3, Morpho, Euler_V2, Fluid, Radiant, Venus, Benqi. See "Lending —
A_TOKEN/DEBT_TOKEN split" under Current-vs-target above for the real per-protocol current state — this section covers
data sources and chain coverage instead.

| Protocol    | Data source                                                                        | Chains (real, from `SUBGRAPH_IDS`)                                                                                                           |
| ----------- | ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Aave_V3     | The Graph (+ AaveScan REST fallback)                                               | Ethereum, Arbitrum, Optimism (RPC-fallback only — subgraph abandoned by the Aave team, see note below), Polygon, Avalanche, Base, Linea, BSC |
| Spark       | The Graph (MakerDAO fork of Aave V3, same schema)                                  | Ethereum                                                                                                                                     |
| Compound_V3 | The Graph                                                                          | Ethereum, Arbitrum, Base, Optimism (Polygon explicitly removed — subgraph returns 0 active markets)                                          |
| Morpho      | `blue-api.morpho.org` GraphQL (not The Graph)                                      | Ethereum, Base (Arbitrum/Optimism/Polygon have 0 major-asset markets as of 2026-03, not queried)                                             |
| Euler_V2    | Goldsky subgraph (routed via an endpoint override, not the standard Graph gateway) | Ethereum, Arbitrum                                                                                                                           |
| Fluid       | The Graph                                                                          | Ethereum only (multi-chain subgraph IDs not yet verified)                                                                                    |
| Radiant     | The Graph (Messari Lending schema)                                                 | Arbitrum, Ethereum                                                                                                                           |
| Venus       | The Graph (Compound-fork schema)                                                   | BSC (isolated pools), Ethereum                                                                                                               |
| Benqi       | The Graph (Compound-fork schema)                                                   | Avalanche                                                                                                                                    |

**AAVE_V3-OPTIMISM data-source note**: the subgraph on Optimism was silently abandoned by the Aave team between
2026-05-08 and 2026-05-29 (returns empty `reserves`/`reserveParamsHistoryItems` despite reporting no indexing errors);
canonical data source for this one chain is the RPC fallback (14-row daily resolution), a deliberate policy decision,
not a bug.

**Data-type coverage varies by protocol** — Venus and Benqi's subgraph schemas expose neither a daily-snapshot history
entity nor a dedicated liquidation/risk-param entity (introspected 2026-06-02), so those two are `lending_indices`
only. Aave_V3/Morpho additionally collect `liquidation_events`, `flash_loan_events` (Aave_V3 only), and top-position
`position_data`. **Fluid's `lending_indices` MTDS collector — FIXED 2026-07-08** (previously tracked in
`defi_lending_atoken_debttoken_instrument_split_2026_07_07.md` as "100% broken on an uncaught `ContractCustomError`,
not yet fixed"). Root cause confirmed live: `market-tick-data-service/market_tick_data_service/market_interface/
adapters/defi/fluid_adapter.py` called `totalSupply()`/`totalBorrow()`/`exchangePrice()` directly on the Fluid vault
contract — an ERC-4626-style ABI shape that doesn't exist on Fluid's real VaultT1 implementation. Calling those
selectors hits the proxy's fallback dispatcher, which reverts with an unrecognized custom error
(`0x60121cca...`) that web3.py surfaces as `ContractCustomError` — reproduced live against all 8 curated MVP vaults
(100% failure rate, confirming the doc's "100% broken" claim exactly). **Fix**: read vault state via Fluid's own
periphery `FluidVaultResolver` contract (`0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC`, Ethereum mainnet — verified
against both the official `Instadapp/fluid-contracts-public` deployments manifest and Etherscan's verified contract
name) and its `getVaultEntireData(vault)` method, which is the resolver Fluid itself provides for exactly this read
path; also added real per-token ERC-20 `decimals()` lookups (the old code assumed 1e18 for both collateral and debt,
which was wrong for 6-decimal assets like USDC/USDT) and native-ETH sentinel handling (Fluid vaults can hold native
ETH directly, which has no ERC-20 contract to query). **Verified with a real on-chain call**: after the fix, the same
8 vaults return real, sane data — e.g. the ETH-USDC vault now returns `total_supply=0.65 ETH`,
`total_borrow=131.75 USDC`, `supply_exchange_price=1.089`, `borrow_exchange_price=1.207` at a real historical block,
through the actual `download_market_data()` production code path (not a standalone probe). **Known remaining gap**:
`utilization_rate = total_borrow / total_supply` is still a raw cross-asset ratio (Fluid vaults hold DIFFERENT
collateral and debt assets, unlike Aave's shared-pool model) — this was a pre-existing conceptual issue before this
fix too; a true utilization metric would need an oracle price conversion to a common unit, not implemented here.
Gas-fee data is collected once per chain under the synthetic venue `ALCHEMY`, not once per protocol — a lending
protocol's chain being covered there is sufficient, it doesn't need its own `gas_fees` data type.

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

Covered in detail in "On-chain-perp DEXes — PERP-vs-PERPETUAL key/field mismatch" under Current-vs-target above.
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

## MVP universe — corrected (the old spec's DeFi section is confirmed stale)

`specs/MVP_INSTRUMENTS.md`'s DeFi section describes a **16 position + 4 trading instrument** universe spanning exactly
6 venues (Wallet, Aave_V3_ETH, EtherFi, Lido, Curve-ETH, Morpho, Uniswap_V3-ETH) with old-style instrument-key
formatting (`AAVE_V3_ETH:A_TOKEN:AUSDT@ETHEREUM`, underscore-glued venue+chain rather than the current dash-separated
`AAVE_V3-ETHEREUM` convention). This does not reflect anything close to the real current adapter registry, which spans
**24 DeFi protocol adapters in this doc's scope alone** (5 DEX + 8 fork-config-variants sharing one class + 9 lending

- 15 yield/LST/restaking, with some protocols like Karak/Renzo/Idle/Yearn/Beefy live on 2+ chains each) plus another
  ~20+ Solana-native protocols outside this doc's scope. There is no dedicated "DeFi MVP" instrument set today distinct
  from "every protocol the factory registry knows how to instantiate" — the real governing surface is:

* `instruments_service/reference_data/factory.py`'s `_ADAPTERS` dict (which protocol keys have a working adapter
  class today — 50+ entries spanning CeFi/DeFi/TradFi/prediction/sports).
* `unified_api_contracts/registry/capability_declarations/_defi.py`'s `PROTOCOL_CAPABILITIES` dict (the real SSOT for
  which `InstrumentType`s, MTDS data types, and required tokens each protocol produces).
* `unified_api_contracts/registry/venue_adapter_keys.py`'s `VENUE_TO_ADAPTER_KEY` (the real venue→adapter routing
  table, including auto-generated multi-chain entries from `SUBGRAPH_IDS`).

Recommend retiring the "MVP instruments" framing for DeFi specifically (it made sense when the universe was 6 venues;
it doesn't scale to today's registry) in favor of pointing at these 3 registries directly, with the known-gaps list
above (`EMPTY_OR_DEPRECATED_DEFI_VENUES` / `DEFI_INSTRUMENTS_NOT_YET_COLLECTED`) as the honest "what's expected vs.
what's actually collected" answer. Historical instrument-count snapshots from the old doc (e.g. "331 instruments,
2026-03-23") are stale given how much the registry has grown since and should be regenerated from a live query rather
than carried forward.

---

## Data sources and API keys

Unchanged in substance from the prior `DEFI_GUIDE.md`:

| Source                                                          | Used by                                                                                | Auth                                                                                                           |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| The Graph (`gateway.thegraph.com`)                              | Uniswap V2/V3/V4 + forks, Aave_V3, Compound_V3, Fluid, Radiant, Venus, Benqi, GMX      | `thegraph-api-key` secret; free tier 100k queries/month, paid $2/100k queries (billed in GRT on Arbitrum)      |
| Balancer API v3                                                 | Balancer                                                                               | No key required                                                                                                |
| Curve Registry contract (RPC)                                   | Curve (subgraph deprecated; RPC is the primary fallback, not a fallback-of-a-fallback) | RPC access only                                                                                                |
| Goldsky                                                         | Euler_V2 (routed via an endpoint override, not the standard Graph gateway)             | —                                                                                                              |
| `blue-api.morpho.org` GraphQL                                   | Morpho (subgraph is the fallback, not primary)                                         | No key required                                                                                                |
| Alchemy SDK                                                     | Token metadata / contract resolution for the static yield/LST/restaking adapters       | `alchemy-api-key` secret; free tier 300M compute units/month                                                   |
| Hyperliquid / Aster / Extended / Pacifica / Lighter native REST | On-chain-perp DEXes                                                                    | Public endpoints, no key needed for instrument discovery (trading credentials are execution-service's concern) |

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

### `instrument_type` is a real GCS shard axis today — this is a correction, not a restatement

**A pre-consolidation version of this doc claimed `instrument_type` is "display-only, NOT a shard axis" for DeFi
(only `chain` partitions the data). That claim is stale.** Real current MTDS GCS paths (checked directly, June 2026
data) always include `instrument_type=` as a partition segment — two real shapes seen:
`.../venue=X/chain=Y/instrument_type=Z/data_type=W/...` and `.../venue=X/instrument_type=Z/data_type=W/...` (the
`chain=` segment is present only when it's distinct from `venue`, e.g. cross-chain data like `gas_fees`).
`instrument_type` is both part of the canonical `instrument_id` string (identity, e.g. the `POOL`/`A_TOKEN`/
`YIELD_BEARING` segment in `VENUE:TYPE:SYMBOL`) **and** a real physical GCS partition directory — these are two
separate, compatible facts (one about identity encoding, one about storage layout), not the same claim, and both are
real today (the identity-encoding part always was; the storage-partition part is the part the old docs got wrong).

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
  swaps (avoiding a second subgraph query for data that's arguably redundant) is a real open architecture question,
  not resolved in this pass — flagging it here rather than picking a side.
- **Live (real-time, per-block) swap streaming does not exist yet** — `live/connectors/dex_swap_scaffold_ws.py` is
  an explicit placeholder (`DexSwapPlaceholderWSFeedConnector`, Phase 3.5 / `wsfeedconnector_phase35_gap_2026_07_06.md`)
  covering 22 real `PROTOCOL-CHAIN` venue keys — it satisfies the live-connector Protocol surface just enough to move
  those venues from `blocked-not-registered` to `schema-only` in the smoke-matrix, it does not actually stream swap
  data. "Swap rates each block" is the real target architecture but is not shipped; today's real-time DeFi capture is
  batch/polling-based (subgraph queries on a schedule), not a genuine live feed.

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
