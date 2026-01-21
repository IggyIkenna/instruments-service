# Instruments Service - Deployment Guide (Femi)

**Last Updated:** January 21, 2026  
**Owner:** Femi  
**Service:** `instruments-service`

---

## Overview

This guide covers deploying `instruments-service` to GCP for batch historical backfill and T+1 daily updates.

---

## Success Criteria

| Criteria | Target | Notes |
|----------|--------|-------|
| **CeFi Mode** | All dates from **Nov 17, 2019** to today | Tardis.dev started recording late 2019 |
| **TradFi Mode** | All dates from **Jan 1, 2020** to today | Databento API - historical equity/futures data |
| **DeFi Mode** | All dates from **Dec 18, 2020** to today | Most DeFi protocols launched 2020-2021 |
| **GCS Output** | Data in correct buckets | See bucket paths below |
| **T+1 Scheduler** | Running after 8am UTC daily | Cloud Scheduler configured |

**Note:** You CAN try running from Jan 1, 2019 but expect empty results for dates before the data sources have coverage. The system will handle missing data gracefully.

### Success Criteria Command

The deployment should be verified using the following command:

```bash
python -m instruments_service --mode instruments \
    --start-date 2020-01-01 \
    --end-date 2026-01-12 \
    --CEFI --TRADFI --DEFI --force
```

**Litmus Test Date: May 23rd, 2023**

May 23rd, 2023 is used as the benchmark/litmus test date to verify the system is running properly. This date was chosen because it represents a period when most products across all domains (CeFi, TradFi, and DeFi) were active and operational.

**Expected Gaps:**

- **DeFi and CeFi:** Gaps are expected for dates before product launch dates, especially for DeFi and CeFi domains, as some products hadn't launched yet. For example:
  - Many DeFi protocols launched in 2020-2021 (see Data Availability section below)
  - Some CeFi exchanges may have limited historical coverage
  - The system handles missing data gracefully and will skip dates/products that don't exist yet

- **Verification:** Use May 23rd, 2023 as the primary verification date to confirm the system is working correctly, as this date should have comprehensive coverage across all domains. See the benchmark data in the "Expected File Sizes & Row Counts" section below for expected results on this date.

---

## Data Availability by Domain

### CeFi (via Tardis.dev)

| Exchange | Launch Date | Tardis Recording Start | Notes |
|----------|-------------|------------------------|-------|
| **Binance** | July 2017 | **Nov 2019** | Tardis.dev started recording late 2019 |
| **Binance Futures** | Sept 2019 | **Nov 2019** | Available from Tardis start |
| **OKX (OKEx)** | 2017 | **Nov 2019** | Available from Tardis start |
| **Bybit** | March 2018 | **Nov 2019** | Available from Tardis start |
| **Deribit** | 2016 | **Nov 2019** | Options exchange |

**Earliest CeFi data in GCS:** `day-2019-11-17`

### TradFi (via Databento)

| Asset Class | Availability | Notes |
|-------------|--------------|-------|
| **Equities (NYSE, NASDAQ)** | 2000+ | Historical data available |
| **Futures (CME, CBOT)** | 2000+ | E-mini S&P, commodities |
| **Micro Futures** | 2019+ | Micro E-mini launched May 2019 |
| **Bitcoin Futures (CME)** | Dec 2017+ | BTC futures launched Dec 2017 |
| **Bitcoin ETFs (IBIT, etc.)** | **Jan 2024** | Spot Bitcoin ETFs approved Jan 10, 2024 |
| **Bitcoin Futures ETF (BITO)** | Oct 2021+ | First futures-based BTC ETF |

**Earliest TradFi data in GCS:** `day-2020-01-01`

### DeFi (via The Graph / Protocol SDKs)

| Protocol | Launch Date | Notes |
|----------|-------------|-------|
| **Uniswap V2** | May 2020 | First major DEX |
| **Uniswap V3** | May 2021 | Concentrated liquidity |
| **Lido (stETH)** | **Dec 2020** | Liquid staking |
| **AAVE V2** | Dec 2020 | Lending protocol |
| **AAVE V3** | March 2022 | Multi-chain |
| **Curve** | Aug 2020 | Stablecoin DEX |
| **Balancer** | March 2020 | Weighted pools |
| **EtherFi** | 2023 | Restaking |
| **Ethena (USDe)** | **2024** | Synthetic dollar |
| **Morpho** | 2022 | Lending optimizer |

**Earliest DeFi data in GCS:** `day-2020-12-18`

---

## Prerequisites

### 1. GCP Access

```bash
# Service account with:
# - Storage Admin (for GCS writes)
# - Secret Manager Accessor (for API keys)
# - Compute Instance Admin (for VM deployment)

export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GCS_PROJECT_ID=central-element-323112
```

### 2. API Keys (in Secret Manager)

| Secret Name | Purpose |
|-------------|---------|
| `tardis-api-key` | CeFi crypto exchange data |
| `databento-api-key` | TradFi equity/futures data |
| `alchemy-api-key` | DeFi on-chain data |
| `the-graph-api-key` | DeFi DEX pool enumeration |

### 3. Python Environment

```bash
# On VM
python3 -m venv venv
source venv/bin/activate
pip install -e .
pip install git+https://github.com/IggyIkenna/unified-cloud-services.git
```

---

## GCS Bucket Structure

### Output Buckets

| Domain | Bucket | Path Pattern |
|--------|--------|--------------|
| **CeFi** | `gs://instruments-store-cefi-central-element-323112/` | `instrument_availability/by_date/day-{YYYY-MM-DD}/instruments.parquet` |
| **TradFi** | `gs://instruments-store-tradfi-central-element-323112/` | `instrument_availability/by_date/day-{YYYY-MM-DD}/instruments.parquet` |
| **DeFi** | `gs://instruments-store-defi-central-element-323112/` | `instrument_availability/by_date/day-{YYYY-MM-DD}/instruments.parquet` |

### Expected File Sizes & Row Counts (Benchmarks)

#### Historical Reference 1: May 23, 2023 (Baseline)

This date provides a baseline before Bitcoin ETFs and newer DeFi protocols.

| Domain | File Size | Instruments | Top Venues |
|--------|-----------|-------------|------------|
| **CeFi** | **168.71 KB** | **2,905** | DERIBIT (1,299), BYBIT (523), OKX (503), BINANCE-SPOT (360), BINANCE-FUTURES (180), COINBASE (17), UPBIT (16) |
| **TradFi** | **310.19 KB** | **9,577** | CME (9,010), NYSE (463), NASDAQ (102), CBOE (1), YAHOO_FINANCE (1) |
| **DeFi** | **45.81 KB** | **116** | HYPERLIQUID (38), UNISWAPV3-ETH (31), ASTER (20), BALANCER-ETH (18), AAVE_V3_ETH (7), LIDO (2) |

**Total for 2023-05-23:** ~525 KB, **12,598 instruments**

**What you'll see:**
- ✅ CeFi: All crypto exchanges (Binance, OKX, Bybit, Deribit, Coinbase, Upbit)
- ✅ TradFi: Equities, CME futures, KRW/USD FX
- ✅ DeFi: Uniswap V3, AAVE V3, Hyperliquid, Aster, Balancer, Lido
- ❌ Missing: Bitcoin ETFs (launched Jan 2024), Ethena (2024), EtherFi, Morpho

---

#### Historical Reference 2: July 1, 2024 (With Bitcoin ETFs & More DeFi)

This date shows the expanded universe after Bitcoin ETF launches and additional DeFi protocol support.

| Domain | File Size | Instruments | Top Venues |
|--------|-----------|-------------|------------|
| **CeFi** | **253.69 KB** | **4,795** | DERIBIT (2,681), BYBIT (887), OKX (539), BINANCE-SPOT (385), BINANCE-FUTURES (262), COINBASE (18), UPBIT (16) |
| **TradFi** | **360.26 KB** | **11,234** | CME (10,664), NYSE (468), NASDAQ (100), CBOE (1), YAHOO_FINANCE (1) |
| **DeFi** | **48.27 KB** | **128** | HYPERLIQUID (38), UNISWAPV3-ETH (37), ASTER (20), BALANCER-ETH (18), AAVE_V3_ETH (7), MORPHO (4), LIDO (2), ETHERFI (1), ETHENA (1) |

**Total for 2024-07-01:** ~662 KB, **16,157 instruments**

**What July 2024 has that May 2023 doesn't:**
- ✅ Bitcoin ETFs: IBIT, FBTC, ARKB (NASDAQ:ETF)
- ✅ EtherFi (weETH) - more mature by 2024
- ✅ Ethena (sUSDe) - launched 2024
- ✅ Morpho (Ethereum) - lending protocol
- ✅ +65% more CeFi instruments overall

---

**Note:** 
- File sizes and row counts vary by date based on:
  - **Earlier dates (2020-2023):** Fewer instruments existed (new exchanges/protocols launched over time)
  - **Recent dates (2024+):** More instruments as new assets, exchanges, and protocols are added
  - **TradFi:** Largest domain due to thousands of equity tickers and futures contracts
  - **CeFi:** Moderate size with major crypto exchanges
  - **DeFi:** Smallest domain with curated protocol instruments

**Success Criteria for Deployment:**
- Verify files exist in GCS for target dates
- Check file sizes are within expected ranges (see benchmarks above)
- Confirm row counts match expected instrument counts per domain
- All files should have 59 columns (standard instrument definition schema)

---

## CLI Commands

### Single Day Run

```bash
# CeFi mode only (with --force to regenerate)
python -m instruments_service --mode instruments --start-date 2024-06-03 --CEFI --force

# TradFi mode only
python -m instruments_service --mode instruments --start-date 2024-06-03 --TRADFI --force

# DeFi mode only
python -m instruments_service --mode instruments --start-date 2024-06-03 --DEFI --force

# All modes (default - no domain flags needed)
python -m instruments_service --mode instruments --start-date 2024-06-03 --force
```

### Date Range Run (Historical Backfill)

**IMPORTANT:** Always use `--force` to wipe existing data and ensure latest code is used.

```bash
# CeFi backfill (from Tardis start date Nov 2019) until Jan 5, 2026
python -m instruments_service --mode instruments \
  --start-date 2019-11-17 \
  --end-date 2026-01-05 \
  --CEFI --force

# TradFi backfill (from Jan 2020) until Jan 5, 2026
python -m instruments_service --mode instruments \
  --start-date 2020-01-01 \
  --end-date 2026-01-05 \
  --TRADFI --force

# DeFi backfill (from Dec 2020) until Jan 5, 2026
python -m instruments_service --mode instruments \
  --start-date 2020-12-18 \
  --end-date 2026-01-05 \
  --DEFI --force

# OR all domains at once (from Jan 1, 2019 - will have empty results for early dates)
python -m instruments_service --mode instruments \
  --start-date 2019-01-01 \
  --end-date 2026-01-05 \
  --force
```

**Note:** 
- Backfill ends on **Jan 5, 2026** (yesterday)
- T+1 scheduler starts from **Jan 6, 2026** (today)
- Always use `--force` to ensure data is regenerated with latest code

### T+1 Daily Run (Starts Jan 6, 2026)

```bash
# Single day for yesterday (get yesterday's date first)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
python -m instruments_service --mode instruments --start-date $YESTERDAY --force
```

**Note:** T+1 scheduler should start running from **Jan 6, 2026** onwards (after backfill completes up to Jan 5, 2026).

---

## VM Deployment

### 1. Create VM

```bash
# Use asia-northeast1 (Tokyo) - same region as other UTS resources
gcloud compute instances create instruments-service-vm \
  --zone=asia-northeast1-c \
  --machine-type=e2-medium \
  --image-family=debian-11 \
  --image-project=debian-cloud \
  --boot-disk-size=50GB \
  --service-account=instruments-service@central-element-323112.iam.gserviceaccount.com \
  --scopes=cloud-platform
```

**Note:** Use `asia-northeast1-c` (Tokyo) to match other UTS infrastructure and minimize GCS egress costs.

### 2. SSH and Setup

```bash
gcloud compute ssh instruments-service-vm --zone=asia-northeast1-c

# Install Python 3.11+
sudo apt update && sudo apt install -y python3.11 python3.11-venv git

# Clone and install
git clone https://github.com/IggyIkenna/instruments-service.git
cd instruments-service
python3.11 -m venv venv
source venv/bin/activate
pip install -e .
pip install git+https://github.com/IggyIkenna/unified-cloud-services.git
```

### 3. Run Backfill

```bash
# Screen session for long-running backfill
screen -S backfill

# Run CeFi backfill (may take hours)
python -m instruments_service.cli.main --mode cefi \
  --start-date 2019-01-01 \
  --end-date 2026-01-06

# Detach: Ctrl+A, D
# Reattach: screen -r backfill
```

---

## Cloud Scheduler (T+1 Daily)

### Create Scheduler Job

```bash
# Create Cloud Scheduler job for daily T+1 run
# Use asia-northeast1 to match VM region
gcloud scheduler jobs create http instruments-daily-t1 \
  --location=asia-northeast1 \
  --schedule="0 9 * * *" \
  --time-zone="UTC" \
  --uri="https://asia-northeast1-central-element-323112.cloudfunctions.net/instruments-t1" \
  --http-method=POST \
  --message-body='{"mode": "instruments", "start_date": "yesterday"}'
```

### Alternative: VM-based Cron

```bash
# On VM, add to crontab
crontab -e

# Add line for 9am UTC daily
0 9 * * * /home/user/instruments-service/venv/bin/python -m instruments_service.cli.main --mode all --date yesterday >> /var/log/instruments-t1.log 2>&1
```

---

## Verification

### Check GCS Output

```bash
# List CeFi output files
gsutil ls "gs://instruments-store-cefi-central-element-323112/instrument_availability/by_date/" | head -20

# Check specific date
gsutil cat "gs://instruments-store-cefi-central-element-323112/instrument_availability/by_date/day-2024-01-15/instruments.parquet" | head

# Count files
gsutil ls "gs://instruments-store-cefi-central-element-323112/instrument_availability/by_date/" | wc -l
```

### Expected Output

After successful backfill:

| Domain | Expected Days | Start Date | Notes |
|--------|---------------|------------|-------|
| **CeFi** | ~1,880+ | **2019-11-17** | Tardis.dev recording start |
| **TradFi** | ~2,190+ | **2020-01-01** | Databento historical data |
| **DeFi** | ~1,480+ | **2020-12-18** | Most protocols launched 2020-2021 |

**Total storage estimate:** ~600 MB (CeFi) + ~600 MB (TradFi) + ~65 MB (DeFi) = ~1.3 GB

### Verification Commands

```bash
# Check specific date (2023-05-23 benchmark)
gsutil ls gs://instruments-store-cefi-central-element-323112/instrument_availability/by_date/day-2023-05-23/
gsutil ls gs://instruments-store-tradfi-central-element-323112/instrument_availability/by_date/day-2023-05-23/
gsutil ls gs://instruments-store-defi-central-element-323112/instrument_availability/by_date/day-2023-05-23/

# Download and verify file sizes and row counts
gsutil cp gs://instruments-store-cefi-central-element-323112/instrument_availability/by_date/day-2023-05-23/instruments.parquet /tmp/cefi.parquet
gsutil cp gs://instruments-store-tradfi-central-element-323112/instrument_availability/by_date/day-2023-05-23/instruments.parquet /tmp/tradfi.parquet
gsutil cp gs://instruments-store-defi-central-element-323112/instrument_availability/by_date/day-2023-05-23/instruments.parquet /tmp/defi.parquet

# Check file sizes
ls -lh /tmp/*.parquet

# Count rows (requires pandas)
python3 -c "
import pandas as pd
import os
for domain in ['cefi', 'tradfi', 'defi']:
    file_path = f'/tmp/{domain}.parquet'
    df = pd.read_parquet(file_path)
    size_kb = os.path.getsize(file_path) / 1024
    print(f'{domain.upper()}: {len(df):,} rows, {size_kb:.2f} KB')
"
```

**Expected Results for 2023-05-23:**
- CeFi: ~2,905 rows, ~169 KB
- TradFi: ~9,577 rows, ~310 KB
- DeFi: ~116 rows, ~46 KB

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `TARDIS_API_KEY not found` | Secret not in Secret Manager | Add via `gcloud secrets create` |
| `Permission denied on GCS` | Missing Storage Admin role | Update service account IAM |
| `Rate limit exceeded` | API throttling | Add retry logic or slow down |
| `No instruments found` | Date before exchange existed | Expected - log and continue |

### Logs

```bash
# Check logs
tail -f /var/log/instruments-t1.log

# Check for errors
grep -i error /var/log/instruments-t1.log
```

---

## Milestones (Femi Contract)

| Milestone | Description | Due | Payment |
|-----------|-------------|-----|---------|
| **B2** | instruments-service batch deployed + T+1 | Jan 2 | Part of $625 |

### Acceptance Criteria for B2

1. ✅ VM deployment working in `asia-northeast1-c`
2. ✅ CeFi mode: **Nov 17, 2019 - Jan 5, 2026** populated in GCS (with `--force`)
3. ✅ TradFi mode: **Jan 1, 2020 - Jan 5, 2026** populated in GCS (with `--force`)
4. ✅ DeFi mode: **Dec 18, 2020 - Jan 5, 2026** populated in GCS (with `--force`)
5. ✅ T+1 scheduler starts **Jan 6, 2026** running at 9am UTC daily
6. ✅ Ikenna has verified sample outputs in GCS:
   - **2023-05-23 benchmark:** CeFi ~169KB (2,905 rows), TradFi ~310KB (9,577 rows), DeFi ~46KB (116 rows)
   - **2024-06-03 reference:** CeFi ~35KB, TradFi ~289KB, DeFi ~45KB
   - All files have 59 columns (standard schema)

---

## Contact

- **Technical Questions:** Ikenna (code/specs)
- **Deployment Issues:** Femi (VM/scheduler)
- **Progress Tracking:** Julian (ClickUp)

---

*Last updated: January 6, 2026*

