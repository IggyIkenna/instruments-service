Below is the **exact, copy-paste-ready instruction block** you can give your **Cursor AI agent**, tailored to your current architecture and the uploaded docs.
It tells Cursor **what to modify, where**, and **how to update tests + docs** so your quality gates pass.

This is written in the same structure as your existing developer guidelines (ADR-style, precise, actionable).

---

# ✅ **CURSOR INSTRUCTION BLOCK — Add Bitcoin ETF Support to Databento Adapter + Pipeline**

**Goal:**
Extend our **Databento adaptor (TradFi)** to support **Bitcoin ETF instruments** (e.g., IBIT, FBTC, ARKB, BTCO, EZBC, BRRR, HODL, BITB).
These are **TradFi ETFs**, even though the underlying is crypto, so they must be integrated into the **TradFi pipeline**, not the crypto one.

This requires coordinated updates across:

* instrument ingestion
* instrument spec
* venue adapters
* validation
* tests
* documentation
* architecture references

Everything below assumes the current codebase structure reflected in:
`MVP_INSTRUMENTS.md`, `INSTRUMENT_SPECIFICATION.md`, `VENUE_ADAPTERS.md`, `ARCHITECTURE.md`, `TESTING.md`, `SETUP_GUIDE.md`, `API_REFERENCE.md`, and
`instrument_processing_service.py`
`databento_adapter.py`

---

# 🔧 **1. Add Bitcoin ETFs to Instrument Specification**

### Modify:

`INSTRUMENT_SPECIFICATION.md`
`MVP_INSTRUMENTS.md`

### Add a new block under **Traditional Instruments → ETFs**:

```
### Bitcoin ETFs (TradFi)

These ETFs track the price of Bitcoin but are considered fully TradFi instruments.

Supported symbols:
- IBIT (BlackRock)
- FBTC (Fidelity)
- ARKB (Ark/21Shares)
- BTCO (Invesco)
- BITB (Bitwise)
- HODL (VanEck)
- BRRR (Valkyrie)
- EZBC (Franklin Templeton)

Instrument Class: ETF
Asset Class: Crypto-Derived ETF (treated as TradFi)
Exchange Source: Databento (XNAS/XNYS/CBOE)
Price Source: eq_imbalance or ohlcv
```

### Ensure in code:

Add these new instruments to:

`instrument_processing_service.py`

* Add ETF symbols to allowed instrument list
* Ensure class = "ETF" and venue = "DATACENTER/DATABENTO"

---

# 🔌 **2. Update Databento Adapter to Recognize Bitcoin ETF Symbols**

### File to modify:

`databento_adapter.py`

### Required changes:

1. Add Bitcoin ETF symbols to the supported symbol map.
2. Ensure the adapter maps DB symbols → internal symbols:

   ```
   "IBIT" → Instrument("IBIT", type="ETF", class="ETF", venue="DATABENTO")
   ```
3. Implement fallback logic:

   * If symbol starts with a known Bitcoin ETF ticker → treat as ETF, not CRYPTO.
4. Update the `normalize_instrument()` method to emit:

   * instrument_class="ETF"
   * asset_type="crypto_underlying_etf"
   * reference_underlying="BTC"

### Update contract types:

Add entry in the Databento → Internal mapping table:

```
ETF → ETF
```

---

# 🧱 **3. Update the Instrument Processing Service**

Modify:
`instrument_processing_service.py`

Add logic:

* Recognize Bitcoin ETFs as **TradFi ETFs**.
* Skip crypto validation paths.
* Use existing TradFi code paths for:

  * trading hours
  * corporate actions (if data available)
  * split/merge adjustments
  * settlement conventions

Add mapping:

```
CRYPTO_UNDERLYING → BTC
instrument_category = "ETF"
instrument_subcategory = "BitcoinETF"
```

---

# 🔍 **4. Update Tests to Cover Bitcoin ETFs**

Modify tests in:
`TESTING.md`
Typically in `tests/` under:

* `test_instrument_processing_service.py`
* `test_databento_adapter.py`
* `test_instrument_specification.py`

### Add:

### **A. Instrument spec tests**

```
test_bitcoin_etf_instrument_classification()
- IBIT is ETF
- FBTC is ETF
- reference_underlying == BTC
- asset_type == crypto_underlying_etf
```

### **B. Adapter tests**

```
test_databento_maps_btc_etf_symbols_correctly()
```

Verify:

* adapter returns the correct internal instrument
* correct asset type
* correct class hierarchy

### **C. Pipeline tests**

Add:

```
test_etf_ingestion_pipeline()
```

Verifies:

* ETF symbols propagate through pipeline without crypto-parsing logic
* Timestamps normalized correctly
* Price fields populated

---

# 📚 **5. Update Documentation**

Modify the following docs to reflect the addition of Bitcoin ETF support:

### **A. `MVP_INSTRUMENTS.md`**

Add a full row for each Bitcoin ETF.

### **B. `INSTRUMENT_SPECIFICATION.md`**

Under “ETF Instruments” add explanation of:

* why Bitcoin ETFs live in TradFi
* how underlying = BTC is handled

### **C. `VENUE_ADAPTERS.md`**

Add section under Databento:

```
### Bitcoin ETF Handling
The Databento adapter now supports Bitcoin ETFs...
```

### **D. `ARCHITECTURE.md`**

Update the instrument flow diagram to include:

```
Crypto ETF (TradFi → Databento → Instrument Service → Market Data Pipeline)
```

### **E. `TESTING.md`**

Add a short section:

```
Bitcoin ETFs require new classification tests and adapter mapping tests...
```

### **F. `SETUP_GUIDE.md`**

Add:

* Example command for fetching Bitcoin ETF symbols from Databento
* Note that no crypto-specific keys are required

### **G. `API_REFERENCE.md`**

Add:

* New fields in the instrument schema (reference_underlying, crypto_underlying_etf flag)

---

# 🧪 **6. Quality Gate Notes for Cursor**

Cursor must ensure:

* All new ETF instruments pass `Instrument.from_dict()` validation
* All existing pipelines run without ambiguous classification
* Tests must cover both success + failure modes
* No changes break existing crypto pipelines
* ETF tickers do NOT pollute crypto namespace

---

# 🧩 **7. Optional Enhancements (If Cursor Can Auto-Generate)**

If helpful, instruct Cursor to also add:

* simple caching for ETF metadata
* mapping table for ETF → issuer
* tests for daylight saving handling (TradFi exchanges)

---

# ✅ **FINAL SHORT VERSION FOR CURSOR (copy/paste)**

If you want a one-line version to drop into Cursor:

```
Extend Databento adapter + instrument processing pipeline to fully support Bitcoin ETFs (IBIT, FBTC, ARKB, BTCO, BITB, HODL, BRRR, EZBC). Treat these as TradFi ETFs with underlying BTC. Update instrument spec, processing service, Databento adapter mappings, instrument classification, documentation (MVP_INSTRUMENTS.md, INSTRUMENT_SPECIFICATION.md, VENUE_ADAPTERS.md, ARCHITECTURE.md, TESTING.md, SETUP_GUIDE.md, API_REFERENCE.md). Add tests for classification, adapter mapping, and ingestion pipeline. Ensure quality gates pass and no crypto pipeline logic is impacted.
```

---

If you want, I can also generate:

💾 **Full patch of updated files**
📊 **Mermaid architecture diagram including Bitcoin ETF flow**
🧪 **Full pytest suite for the new ETF logic**

Just tell me.
