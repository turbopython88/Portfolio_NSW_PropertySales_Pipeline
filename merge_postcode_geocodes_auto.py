#Import assets
import pandas as pd
from pathlib import Path

SALES_PATH = Path("data/processed/psi_sales_clean_alias.csv")  # after aliases
PC_PATH    = Path("data/aux/au_postcodes.csv")
OUT_PATH   = Path("data/processed/psi_sales_geo.csv")

# Load and normalize sales
sales = pd.read_csv(SALES_PATH, dtype={"postcode":"string"}, parse_dates=["contract_date","settlement_date"])
sales.columns = sales.columns.str.strip().str.lower().str.replace(r"[\s\-]+","_", regex=True)

if "locality" not in sales.columns:
    raise SystemExit(f"Expected 'locality' in sales columns, found: {list(sales.columns)}")

sales["locality"] = (sales["locality"].astype("string")
                     .str.replace(r"\s+"," ", regex=True)
                     .str.strip()
                     .str.title())
sales["postcode"] = sales["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)

# Load postcode reference (your header layout)
pc = pd.read_csv(PC_PATH, dtype={"postcode":"string"})
pc.columns = pc.columns.str.strip().str.lower()
# Keep only NSW rows if a state_code column exists
if "state_code" in pc.columns:
    pc = pc[pc["state_code"].str.upper() == "NSW"]

# Standardize fields for merge
required = {"postcode","place_name","latitude","longitude"}
missing  = required - set(pc.columns)
if missing:
    raise SystemExit(f"Postcode file missing required columns: {missing}. Found: {list(pc.columns)}")

pc["postcode"]  = pc["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)
pc["place_name"] = (pc["place_name"].astype("string")
                    .str.replace(r"\s+"," ", regex=True)
                    .str.strip()
                    .str.title())

# Aggregate duplicates (some localities appear multiple times)
pc_unique = (pc[["place_name","postcode","latitude","longitude"]]
             .dropna(subset=["latitude","longitude"])
             .astype({"latitude":"float64","longitude":"float64"})
             .groupby(["place_name","postcode"], as_index=False)
             .agg({"latitude":"mean","longitude":"mean"}))

# Merge
merged = sales.merge(
    pc_unique,
    left_on=["locality","postcode"],
    right_on=["place_name","postcode"],
    how="left",
    validate="m:1"
)

# Cleanup right-side helper
if "place_name" in merged.columns and "locality" in merged.columns:
    merged.drop(columns=["place_name"], inplace=True)

# Report
total   = len(merged)
matched = merged["latitude"].notna().sum()
print(f"Matched {matched}/{total} rows ({matched/total:.1%}).")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
merged.to_csv(OUT_PATH, index=False)
print(f"Wrote {OUT_PATH}")

