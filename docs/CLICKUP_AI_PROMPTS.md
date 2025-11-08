# ClickUp AI Prompts for Instruments Service

> **Purpose**: Copy-paste these prompts into ClickUp AI Assistant to automatically generate tasks from `STATUS.md`

## ClickUp Plan Compatibility

**Free Forever Plan Includes:**
- ✅ CSV Import (Option 2) - Available in free plan
- ✅ Basic API Access (Option 3) - Available in free plan with rate limits
- ✅ ClickUp AI Assistant - Available in free plan
- ✅ Custom Fields - Available in free plan
- ✅ Gantt Charts - Available in free plan
- ✅ Filtered Views - Available in free plan

**Note**: Free plan has API rate limits. For heavy automation, consider upgrading to Business Plus or Enterprise for higher limits.

---

## How to Use

1. Open ClickUp AI Assistant (click the AI icon in ClickUp)
2. Select "Create items" or "Create tasks"
3. Copy-paste one prompt at a time
4. Review and adjust the generated tasks
5. Repeat for each section

---

## Prompt 1: Create Main Milestone Tasks

```
Create tasks in the "Instruments Service" list with the following milestones:

1. Task: "Core Batch Processing"
   - Status: Complete
   - Due Date: 2025-01-15
   - Description: TradFi instruments working. Core batch processing implementation complete.

2. Task: "Quality Gates (75% coverage)"
   - Status: Complete
   - Due Date: 2025-01-15
   - Description: 75.82% test coverage achieved (target: 75%+)

3. Task: "Options Instrument Support"
   - Status: Complete
   - Due Date: 2025-01-15
   - Description: Crypto options (DERIBIT venue), beyond MVP definitions in GCS

4. Task: "DeFi Instrument Support"
   - Status: Planned
   - Due Date: Week 5-6 (set based on sprint calendar)
   - Description: Early priority - dependency for other services. Requires DeFi venue adapters and contract enrichment.

5. Task: "Databento Integration (TradFi)"
   - Status: Planned
   - Due Date: Week 7-8 (set based on sprint calendar)
   - Description: TradFi strategy support (commodities, currencies, equities). Requires Databento API access.

6. Task: "Daily Backfill Job"
   - Status: Planned
   - Description: Configure daily backfill scheduler for instrument definitions

7. Task: "Performance Benchmarks"
   - Status: Planned
   - Description: Implement performance benchmarks for batch processing

Tag all tasks with "instruments-service" and "milestone".
```

---

## Prompt 2: Create In-Progress Tasks

```
Create tasks in the "Instruments Service" list under the parent task "Quality Gates (75% coverage)":

1. Subtask: "Fix test failures in test_instrument_handler.py"
   - Status: In Progress
   - Due Date: 2025-01-20
   - Description: Fix 12 test errors related to mock/import issues in test_instrument_handler.py
   - Priority: High

2. Subtask: "Fix test failures in test_cli_main.py"
   - Status: In Progress
   - Due Date: 2025-01-20
   - Description: Fix 10 test failures related to attribute patching issues in test_cli_main.py
   - Priority: High

Tag both tasks with "instruments-service", "bug-fix", and "quality-gates".
```

---

## Prompt 3: Create DeFi Implementation Tasks

```
Create subtasks under the parent task "DeFi Instrument Support" in the "Instruments Service" list:

1. Subtask: "Create venue adapters for DEX protocols"
   - Description: Create adapters for UNISWAPV3-ETH, CURVE-ETH, AERODROME-BASE, BALANCER-ETH
   - Dependencies: None

2. Subtask: "Create venue adapters for lending protocols"
   - Description: Create adapter for AAVE_V3_ETH
   - Dependencies: None

3. Subtask: "Create venue adapters for staking protocols"
   - Description: Create adapters for ETHERFI and LIDO
   - Dependencies: None

4. Subtask: "Implement contract address enrichment"
   - Description: Add functionality to enrich pool addresses and token addresses
   - Dependencies: DEX protocols adapter

5. Subtask: "Implement pair discovery for DEX pools"
   - Description: Implement pair discovery per base currency per INSTRUMENT_VENUE_SPECIFICATION.md
   - Dependencies: DEX protocols adapter

6. Subtask: "Implement chain suffix handling"
   - Description: Support chain suffixes (@ETHEREUM, @ARBITRUM, @BASE, etc.)
   - Dependencies: Contract address enrichment

7. Subtask: "Integrate The Graph API"
   - Description: Integrate The Graph API for DEX pool enumeration
   - Dependencies: DEX protocols adapter

8. Subtask: "Integrate Alchemy/web3"
   - Description: Integrate Alchemy/web3 for on-chain data
   - Dependencies: Contract address enrichment

9. Subtask: "Integrate Protocol SDKs"
   - Description: Integrate AAVE SDK, EtherFi SDK, and Lido SDK
   - Dependencies: Lending protocols adapter, Staking protocols adapter

10. Subtask: "Support DeFi instrument types"
    - Description: Support POOL, LST, A_TOKEN, DEBT_TOKEN instrument types
    - Dependencies: All venue adapters

11. Subtask: "Support WALLET venue"
    - Description: Add WALLET venue support for wallet positions
    - Dependencies: None

12. Subtask: "Generate DEX swap routes"
    - Description: Generate DEX swap routes (SPOT_PAIR) for execution routing
    - Dependencies: Pair discovery, Chain suffix handling

Set all subtasks to status "Planned" and tag with "instruments-service", "defi", and "week-5-6".
Link dependencies between subtasks where specified.
```

---

## Prompt 4: Create TradFi Implementation Tasks

```
Create subtasks under the parent task "Databento Integration (TradFi)" in the "Instruments Service" list:

1. Subtask: "Create Databento venue adapter"
   - Description: Create Databento venue adapter similar to Tardis adapter
   - Dependencies: Databento API access
   - Reference: archive/genConfig/instrumentDefinitionConfig/dataBentoInstrumentSelection.py

2. Subtask: "Implement commodity instruments"
   - Description: Implement instruments for Sugar, Coffee, Pork Belly, Cotton, Cocoa, Orange Juice, Soybeans, Crude Oil, Natural Gas, Gold. Use most liquid micro futures/ETFs.
   - Dependencies: Databento venue adapter

3. Subtask: "Implement G10 currency instruments"
   - Description: Implement G10 currency instruments using most liquid micro futures/ETFs
   - Dependencies: Databento venue adapter

4. Subtask: "Implement equity index instruments"
   - Description: Implement equity index instruments using most liquid micro futures/ETFs
   - Dependencies: Databento venue adapter

5. Subtask: "Implement S&P 500 stock instruments"
   - Description: Implement S&P 500 stock instruments using most liquid micro futures/ETFs
   - Dependencies: Databento venue adapter

6. Subtask: "Add liquidity-based selection logic"
   - Description: Implement logic to prefer most liquid micro futures/ETFs to avoid large contract sizes
   - Dependencies: All instrument implementations
   - Reference: archive/loadMarketDataHist/downloadUpload/dataBento/dataBentoDataLoader.py

Set all subtasks to status "Planned" and tag with "instruments-service", "tradfi", "databento", and "week-7-8".
Link dependencies between subtasks where specified.
```

---

## Prompt 5: Create Strategy Support Tracking Tasks

```
Create tasks in the "Instruments Service" list to track strategy support:

1. Task: "Delta-One ML Strategy Support"
   - Status: Complete
   - Description: TradFi instruments via Tardis API complete. MVP instruments (63 perps + 63 spot) complete. Beyond MVP instruments complete in GCS.
   - Tag: "delta-one-ml", "complete"

2. Task: "DeFi Strategy Support"
   - Status: Not Started
   - Description: DeFi instruments not yet implemented. Requires venue adapters, contract enrichment, and protocol integrations.
   - Tag: "defi", "not-started", "week-5-6"

3. Task: "Options Strategy Support (Crypto)"
   - Status: Complete
   - Description: Crypto options (DERIBIT venue) complete. Beyond MVP definitions in GCS. TradFi options not supported.
   - Tag: "options", "crypto", "complete"

4. Task: "TradFi Strategy Support"
   - Status: In Progress
   - Description: TradFi strategy is 4th strategy. Databento integration planned Week 7-8. Commodities, currencies, and equities not yet implemented.
   - Tag: "tradfi", "in-progress", "week-7-8"

Link these strategy tasks to their respective implementation milestone tasks.
```

---

## Prompt 6: Create Data Completion Tracking Tasks

```
Create tasks in the "Instruments Service" list to track data completion:

1. Task: "Delta-One ML Data Completion"
   - Status: Complete
   - Description: Date range: 2023-05-23 to 2025-01-15. Coverage: 100% of required TradFi instruments.
   - Custom Field "Coverage %": 100
   - Tag: "data-completion", "delta-one-ml"

2. Task: "DeFi Data Completion"
   - Status: Not Started
   - Description: Date range: N/A. Coverage: 0%. Requires DeFi venue adapters and contract address enrichment.
   - Custom Field "Coverage %": 0
   - Tag: "data-completion", "defi"

3. Task: "Options Data Completion"
   - Status: Complete
   - Description: Date range: 2023-05-23 to 2025-11-05. Coverage: 100% (crypto options - DERIBIT venue, beyond MVP definitions).
   - Custom Field "Coverage %": 100
   - Tag: "data-completion", "options"

4. Task: "TradFi Data Completion"
   - Status: Not Started
   - Description: Date range: N/A. Coverage: 0%. Databento integration planned Week 7-8.
   - Custom Field "Coverage %": 0
   - Tag: "data-completion", "tradfi"

Link these data completion tasks to their respective strategy support tasks.
```

---

## Prompt 7: Create Quality Gates Tasks

```
Create tasks in the "Instruments Service" list for quality gates:

1. Task: "Test Coverage Target"
   - Status: Complete
   - Description: Current coverage: 75.82% (target: 75%+). ✅ Target achieved.
   - Custom Field "Test Coverage %": 75.82
   - Tag: "quality-gates", "test-coverage", "complete"

2. Task: "DRY Compliance"
   - Status: Complete
   - Description: 100% DRY compliance - all cloud operations use unified-cloud-services.
   - Custom Field "DRY Compliance %": 100
   - Tag: "quality-gates", "dry-compliance", "complete"

3. Task: "Performance Benchmarks"
   - Status: Planned
   - Description: Implement performance benchmarks for batch processing. Target: Compute time for 1 day (~30-60 seconds), Memory usage (~500MB), Throughput (~100-200 instruments/second).
   - Tag: "quality-gates", "performance", "planned"

Link "Performance Benchmarks" to the "Performance Benchmarks" milestone task.
```

---

## Prompt 8: Create Dependency Tracking Tasks

```
Create tasks in the "Instruments Service" list to track dependencies:

1. Task: "unified-cloud-services Integration"
   - Status: Complete
   - Description: Fully integrated. Non-blocking. All cloud operations use unified-cloud-services (100% DRY compliance).
   - Tag: "dependencies", "unified-cloud-services", "complete"

2. Task: "Tardis API Integration"
   - Status: Complete
   - Description: Working. Non-blocking. Tardis client tested in source code.
   - Tag: "dependencies", "tardis-api", "complete"

3. Task: "Databento API Access"
   - Status: Planned
   - Description: Required for TradFi strategy support. Planned Week 7-8.
   - Tag: "dependencies", "databento-api", "planned", "week-7-8"

Link "Databento API Access" to "Databento Integration (TradFi)" milestone task.
```

---

## Prompt 9: Create Daily Backfill Subtasks

```
Create subtasks under the parent task "Daily Backfill Job" in the "Instruments Service" list:

1. Subtask: "Configure scheduler"
   - Description: Set up daily backfill scheduler for instrument definitions
   - Dependencies: None

2. Subtask: "Implement incremental processing"
   - Description: Add incremental processing logic for daily backfill
   - Dependencies: Configure scheduler

3. Subtask: "Add error recovery"
   - Description: Implement error recovery mechanisms for daily backfill
   - Dependencies: Incremental processing

4. Subtask: "Set up monitoring"
   - Description: Configure monitoring and alerts for daily backfill job
   - Dependencies: Error recovery

Set all subtasks to status "Planned" and tag with "instruments-service", "daily-backfill", and "scheduler".
```

---

## Prompt 10: Create Performance Benchmarks Subtasks

```
Create subtasks under the parent task "Performance Benchmarks" in the "Instruments Service" list:

1. Subtask: "Benchmark compute time"
   - Description: Measure and track compute time for 1 day of batch processing (target: ~30-60 seconds)
   - Dependencies: None

2. Subtask: "Benchmark memory usage"
   - Description: Measure and track memory usage for typical exchange (target: ~500MB)
   - Dependencies: None

3. Subtask: "Benchmark throughput"
   - Description: Measure and track throughput (target: ~100-200 instruments/second)
   - Dependencies: None

4. Subtask: "Set up performance regression testing"
   - Description: Implement automated performance regression testing
   - Dependencies: All benchmarks

Set all subtasks to status "Planned" and tag with "instruments-service", "performance", and "benchmarks".
```

---

## Prompt 11: Create Gantt Chart View Setup

```
Create a Gantt chart view in ClickUp for "Instruments Service" with the following configuration:

- View Name: "Instruments Service Timeline"
- Group by: Status
- Show dependencies: Yes
- Show milestones: Yes
- Date range: Start from 2025-01-15, extend to Week 10
- Include all tasks tagged with "instruments-service"

Set up the following task relationships:
- "DeFi Instrument Support" depends on "unified-cloud-services Integration" (complete)
- "Databento Integration (TradFi)" depends on "Databento API Access"
- All DeFi subtasks depend on their parent "DeFi Instrument Support"
- All TradFi subtasks depend on their parent "Databento Integration (TradFi)"
- "Daily Backfill Job" has no dependencies
- "Performance Benchmarks" has no dependencies
```

---

## Prompt 12: Create Custom Fields Setup

```
Set up the following custom fields in the "Instruments Service" list:

1. Custom Field: "Coverage %"
   - Type: Number
   - Description: Data coverage percentage for strategy
   - Default: 0

2. Custom Field: "Test Coverage %"
   - Type: Number
   - Description: Test coverage percentage
   - Default: 0

3. Custom Field: "DRY Compliance %"
   - Type: Number
   - Description: Percentage of code using unified-cloud-services
   - Default: 0

4. Custom Field: "Week"
   - Type: Dropdown
   - Options: Week 1-2, Week 3-4, Week 5-6, Week 7-8, Week 9-10, TBD
   - Description: Target week for completion

5. Custom Field: "Strategy"
   - Type: Multi-select
   - Options: Delta-One ML, DeFi, Options, TradFi
   - Description: Strategy this task supports

Apply these custom fields to all tasks in the "Instruments Service" list.
```

---

## Prompt 13: Create Filtered Views

```
Create the following filtered views in the "Instruments Service" list:

1. View: "By Strategy - Delta-One ML"
   - Filter: Strategy contains "Delta-One ML"
   - Group by: Status
   - Sort by: Due Date

2. View: "By Strategy - DeFi"
   - Filter: Strategy contains "DeFi"
   - Group by: Status
   - Sort by: Due Date

3. View: "By Strategy - Options"
   - Filter: Strategy contains "Options"
   - Group by: Status
   - Sort by: Due Date

4. View: "By Strategy - TradFi"
   - Filter: Strategy contains "TradFi"
   - Group by: Status
   - Sort by: Due Date

5. View: "Week 5-6 Tasks"
   - Filter: Week equals "Week 5-6"
   - Group by: Status
   - Sort by: Due Date

6. View: "Week 7-8 Tasks"
   - Filter: Week equals "Week 7-8"
   - Group by: Status
   - Sort by: Due Date

7. View: "In Progress"
   - Filter: Status equals "In Progress"
   - Group by: Assignee
   - Sort by: Due Date

8. View: "Planned"
   - Filter: Status equals "Planned"
   - Group by: Week
   - Sort by: Due Date
```

---

## Tips for Using These Prompts

1. **Run prompts in order**: Start with Prompt 1 (milestones), then work through the others sequentially
2. **Adjust dates**: Replace "Week 5-6" and "Week 7-8" with actual dates from your sprint calendar
3. **Review generated tasks**: ClickUp AI may need adjustments - review and refine as needed
4. **Link dependencies**: Manually verify and link task dependencies after creation
5. **Update custom fields**: Fill in custom field values (Coverage %, Test Coverage %, etc.) after tasks are created
6. **Set up automations**: Consider setting up ClickUp automations to update status based on custom fields
7. **Regular sync**: Update ClickUp tasks weekly as STATUS.md is updated

---

## Quick Reference: Key Sections from STATUS.md

- **Timeline Tracking** (lines 476-488): Main milestones with dates
- **Next Steps** (lines 512-545): Detailed task breakdowns
- **Strategy Support** (lines 133-255): Strategy-specific requirements
- **Data Completion** (lines 259-310): Data coverage tracking
- **Quality Gates** (lines 350-390): Test coverage and compliance metrics
- **Dependencies** (lines 490-492): External dependencies

---

## Next Steps After Import

1. ✅ Review all generated tasks
2. ✅ Assign owners to tasks (use ClickUp AI "Assign" feature)
3. ✅ Set priorities (use ClickUp AI "Prioritize" feature)
4. ✅ Link dependencies manually
5. ✅ Fill in custom field values
6. ✅ Set up Gantt chart view
7. ✅ Create filtered views
8. ✅ Set up weekly sync process

