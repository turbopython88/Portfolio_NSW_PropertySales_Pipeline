**📦 Pipeline Overview:**

Transform NSW Valuer General property sales into an analysis-ready, geocoded dataset suitable for BI dashboards (e.g., Looker Studio). The pipeline is compliant (open data only), reproducible (one-command run), and explainable (version-controlled alias map, data tests).

You can find the looker output here: https://lookerstudio.google.com/reporting/d6fb75bb-3db3-4d03-97ba-71cdf3f52365

Raw ZIPs (.zip)  →  DAT files (.DAT)  →  Staging CSVs (one per DAT)
          →  Clean sales (psi_sales_clean.csv)
              →  Alias-applied (psi_sales_clean_alias.csv)
                  →  Geocoded (psi_sales_geo.csv)
                      →   Fuzzy+Centroid (psi_sales_geo_fuzzy.csv)

**🛠️ Scripts Reference (Purpose, Inputs, Outputs, How to Run)**

Replace [your_file_location] with your local path, e.g., C:\Code\psi-pipeline (Windows) or ~/code/psi-pipeline (macOS/Linux).

0) Environment bootstrap (once per machine)
cd [your_file_location]
python -m venv .venv

# Windows
.\.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

python -m pip install --upgrade pip
pip install pandas rapidfuzz requests  *** requests only needed if you experiment with APIs later

**1) scripts/run_all.ps1 — End-to-end unzip → parse → aggregate**

Purpose: Unzip all weekly PSI ZIPs, parse all .DAT files to CSV, and aggregate to a single clean file.

**Inputs:**

data/raw_zip/*.zip Sourced from public data here: https://valuation.property.nsw.gov.au/embed/propertySalesInformation

Internally calls the two parsers below

**Outputs:**

> data/raw_dat/*.DAT

> data/staging_csv/*.csv (one per DAT)

> data/processed/psi_sales_clean.csv

Run (PowerShell):

# From repo root
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1

**2) scripts/psi_to_csv.py — Parse one .DAT → one staging CSV**

**Purpose:** Read a PSI 2001-current semicolon-delimited .DAT, keep B records (sales), normalize columns.
**Input:** One .DAT file in data/raw_dat/
**Output:** One CSV in data/staging_csv/ with columns like:

Column Headers:
address, locality, postcode, purchase_price, contract_date, settlement_date, area_sqm, zoning, nature_of_property, primary_purpose, property_id, district_code, dealing_number, is_strata

Run:

python scripts/psi_to_csv.py data/raw_dat/XXXX.DAT data/staging_csv/XXXX.csv


Notes:

Converts Hectares → sqm

Assembles standardized address

Leaves all postcodes intact for downstream slicing (no filtering here)

**3) scripts/aggregate_csvs.py — Concatenate & clean**

**Purpose:** Concatenate all staging CSVs, dedupe, and add modeling fields.
**Inputs:** data/staging_csv/*.csv
**Output:** data/processed/psi_sales_clean.csv with extra columns:

year, month (YYYY-MM), price_per_sqm, region (postcode-to-region classification per your spec)

Run:

python scripts/aggregate_csvs.py data/staging_csv data/processed/psi_sales_clean.csv


Deduping:

Prefer dealing_number (unique transaction)

Fallback to (property_id, contract_date, purchase_price)

**4) data/aux/alias_localities.csv — Version-controlled manual reference**

Purpose: Map new/gazetted or marketing locality names to canonical suburb names present in the postcode dataset (e.g., Gables → Box Hill (2765)).
Columns: locality, postcode, canonical_locality

This accomodated newer suburbs and increased our postcode match rate from 87% --> 99%

**5) scripts/apply_aliases.py — Apply alias mappings**

Purpose: Replace locality in psi_sales_clean.csv with canonical_locality where an alias exists.

**Input:**

data/processed/psi_sales_clean.csv

data/aux/alias_localities.csv

**Output:**

data/processed/psi_sales_clean_alias.csv

Run:

python scripts/apply_aliases.py

**6) scripts/merge_postcode_geocodes_auto.py — Exact join to postcode lat/lon**

Purpose: Merge psi_sales_clean_alias.csv to an open postcode dataset (no APIs) using (locality/place_name, postcode) exact match.
Inputs:

data/processed/psi_sales_clean_alias.csv

data/aux/au_postcodes.csv

Required headers: postcode,place_name,state_name,state_code,latitude,longitude,accuracy
Output:

data/processed/psi_sales_geo.csv

Adds latitude, longitude

Run:

python scripts/merge_postcode_geocodes_auto.py


Notes:

Filters to NSW where state_code == "NSW"

Aggregates duplicates in postcode file (uses mean lat/lon per (place_name, postcode))

**7) scripts/fuzzy_fix_postcode_scope.py — Fuzzy + postcode centroid fallback**

Purpose: Push match rate to ~99.9% safely:

Fuzzy match remaining (locality, postcode) within the same postcode only

If still unmatched, fallback to postcode centroid (keeps maps complete for dashboard visualisations)

**Inputs:**

data/processed/psi_sales_clean_alias.csv

data/aux/au_postcodes.csv

**Output:**

data/processed/psi_sales_geo_fuzzy.csv

Run:

pip install rapidfuzz
python scripts/fuzzy_fix_postcode_scope.py

**🔄 One-Command Refresher (Day-2 Ops)**

When new weekly ZIPs are dropped into data/raw_zip/:

# Windows, from repo root
.\.venv\Scripts\activate
powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
python scripts/apply_aliases.py
python scripts/merge_postcode_geocodes_auto.py
# ADded to optimise match rates for future files
python scripts/fuzzy_fix_postcode_scope.py


Resulting files to publish:

data/processed/psi_sales_geo.csv (preferred)

data/processed/psi_sales_geo_fuzzy.csv (if we want the centroid fills)

**✅ Data Governance & Compliance**

No scraping; no restricted APIs. We use Valuer General bulk PSI and an open postcode CSV (attribution in repo).

Transparent remediation via alias_localities.csv (version-controlled).

Scoped fuzzy matching (postcode-bounded) avoids cross-region contamination.

Lineage & reproducibility: each script’s inputs/outputs are deterministic; the run order is documented.

Extensible: swap postcode file  without changing modeling logic.

**🧠 Data Modeling (Conceptual & Physical)
Conceptual Model (Business View)**

Entities & relationships:

Sale — a transaction (price, dates, purpose, zoning)

Property — address + land size (+ strata flag)

Location — (Locality/Suburb, Postcode, Region)

Geocode — latitude/longitude for (Locality, Postcode)

Date — standard calendar attributes (Year, Month) for time trending

Property (address, locality, postcode, area_sqm, is_strata)
        1 ───<  Sale (purchase_price, contract_date, settlement_date, purpose, zoning, dealing_number)
Location (locality, postcode, region)
        1 ───<  Geocode (locality, postcode, lat, lon)   [1:1 at the grain we use]
Date     (date_key, year, month) 
        1 ───<  Sale (contract_date_key, settlement_date_key)


Reporting grains:

Row grain: one row per sale (post-dedupe)

Slice by: region, locality, year, month, is_strata, nature_of_property

Metrics: purchase_price, price_per_sqm, counts/medians/averages

**Physical Model (Analytics Warehouse View)**

If we ever considered moving to a warehouse/dbt table designs can be lifted directly from below, think of these as CSV schemas.

dim_location

location_id (PK, surrogate; optional in files)

locality (TEXT)

postcode (TEXT, 4-char padded)

region (TEXT)

Natural key: (locality, postcode)

dim_geocode

locality (TEXT)

postcode (TEXT)

latitude (FLOAT)

longitude (FLOAT)

geocode_method (TEXT; exact|fuzzy|postcode_centroid) 

Natural key: (locality, postcode)

dim_date

date_key (DATE)

year (INT)

month (TEXT; YYYY-MM)

(Optionally day-of-week, quarter)

fact_sales

address (TEXT)

locality (TEXT)

postcode (TEXT)

purchase_price (INT)

contract_date (DATE)

settlement_date (DATE)

area_sqm (FLOAT)

zoning (TEXT)

nature_of_property (TEXT; R|V|3)

primary_purpose (TEXT)

property_id (TEXT)

district_code (TEXT)

dealing_number (TEXT)

is_strata (BOOL)

Derived: price_per_sqm (FLOAT), year (INT), month (TEXT), region (TEXT)

Joined fields (post-merge): latitude, longitude (FLOAT), geocode_method (optional)

Recommended indexes/partitioning (if in a DB):

contract_date (for time-range queries)

(locality, postcode) (for locality rollups)

region

Example SQL for running to dbt.

CREATE TABLE dim_location (
  locality TEXT NOT NULL,
  postcode CHAR(4) NOT NULL,
  region   TEXT,
  PRIMARY KEY (locality, postcode)
);

CREATE TABLE dim_geocode (
  locality TEXT NOT NULL,
  postcode CHAR(4) NOT NULL,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  geocode_method TEXT,
  PRIMARY KEY (locality, postcode)
);

CREATE TABLE fact_sales (
  address TEXT,
  locality TEXT,
  postcode CHAR(4),
  purchase_price BIGINT,
  contract_date DATE,
  settlement_date DATE,
  area_sqm DOUBLE PRECISION,
  zoning TEXT,
  nature_of_property TEXT,
  primary_purpose TEXT,
  property_id TEXT,
  district_code TEXT,
  dealing_number TEXT,
  is_strata BOOLEAN,
  price_per_sqm DOUBLE PRECISION,
  year INT,
  month TEXT,
  region TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION
);

**🧪 Data Quality & Tests **

Schema checks: required columns exist in each stage 

Type coercion: purchase_price numeric; contract_date parsable; postcode 4-digit padded

**Business rules:**

purchase_price > 0

area_sqm >= 0

contract_date <= settlement_date (where both exist)

Coverage: % rows with lat/lon >= 99.5% (warn if below; fail if below 98%)

