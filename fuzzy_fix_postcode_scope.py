import pandas as pd
from pathlib import Path
from rapidfuzz import process, fuzz

SALES = Path("data/processed/psi_sales_clean_alias.csv")
PC    = Path("data/aux/au_postcodes.csv")
OUT   = Path("data/processed/psi_sales_geo_fuzzy.csv")

sales = pd.read_csv(SALES, dtype={"postcode":"string"})
pc    = pd.read_csv(PC, dtype={"postcode":"string"})

# normalize columns
sales.columns = sales.columns.str.strip().str.lower().str.replace(r"[\s\-]+","_", regex=True)
pc.columns    = pc.columns.str.strip().str.lower()

# tidy values
sales["locality"] = (sales["locality"].astype("string")
                     .str.replace(r"\s+"," ", regex=True)
                     .str.strip()
                     .str.title())
sales["postcode"] = sales["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)

pc["postcode"]   = pc["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)
pc["place_name"] = (pc["place_name"].astype("string")
                    .str.replace(r"\s+"," ", regex=True)
                    .str.strip()
                    .str.title())

# NSW only if present
if "state_code" in pc.columns:
    pc = pc[pc["state_code"].str.upper() == "NSW"]

# Exact merge first
merged = sales.merge(pc[["place_name","postcode","latitude","longitude"]],
                     left_on=["locality","postcode"], right_on=["place_name","postcode"],
                     how="left")

# Build postcode -> candidates
cand = pc.groupby("postcode")["place_name"].apply(list).to_dict()

miss = merged[merged["latitude"].isna()][["locality","postcode"]].drop_duplicates()
fixmap = {}
for _, r in miss.iterrows():
    loc, pcx = r["locality"], r["postcode"]
    pool = cand.get(pcx, [])
    if not pool:
        continue
    match = process.extractOne(loc, pool, scorer=fuzz.WRatio)
    if match and match[1] >= 90:
        fixmap[(loc, pcx)] = match[0]

if fixmap:
    # apply fuzzy fixes in-memory
    def fix_loc(row):
        key = (row["locality"], row["postcode"])
        return fixmap.get(key, row["locality"])
    merged["locality_fixed"] = merged.apply(fix_loc, axis=1)
    merged.drop(columns=["latitude","longitude","place_name"], inplace=True, errors="ignore")
    merged = merged.merge(pc[["place_name","postcode","latitude","longitude"]],
                          left_on=["locality_fixed","postcode"], right_on=["place_name","postcode"],
                          how="left")
    merged.drop(columns=["place_name","locality_fixed"], inplace=True, errors="ignore")

# Postcode centroid fallback for any remaining misses
pc_centroids = (pc.dropna(subset=["latitude","longitude"])
                  .groupby("postcode", as_index=False)[["latitude","longitude"]]
                  .mean()
                  .rename(columns={"latitude":"pc_lat","longitude":"pc_lon"}))
merged = merged.merge(pc_centroids, on="postcode", how="left")
merged["latitude"]  = merged["latitude"].fillna(merged["pc_lat"])
merged["longitude"] = merged["longitude"].fillna(merged["pc_lon"])
merged.drop(columns=["pc_lat","pc_lon"], inplace=True)

OUT.parent.mkdir(parents=True, exist_ok=True)
merged.to_csv(OUT, index=False)
print(f"Wrote {OUT}")
