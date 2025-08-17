import sys, pandas as pd
from pathlib import Path

def classify_region(pc: str) -> str:
    """Map postcode -> region per your ranges. Returns 'Unknown' if no match."""
    try:
        p = int(str(pc))
    except:
        return "Unknown"

    # Ranges/sets you provided
    # Sydney Metro: 2000-2234
    if 2000 <= p <= 2234:
        return "Sydney Metropolitan"
    # Central Coast: 2250-2263
    if 2250 <= p <= 2263:
        return "Central Coast"
    # Illawarra: 2500-2530
    if 2500 <= p <= 2530:
        return "Illawarra"
    # Shoalhaven: 2535, 2536, 2538-2541
    if p in (2535, 2536) or 2538 <= p <= 2541:
        return "Shoalhaven"
    # Southern Highlands: 2533, 2571, 2575-2579
    if p in (2533, 2571) or 2575 <= p <= 2579:
        return "Southern Highlands"
    # Hunter: 2280-2330
    if 2280 <= p <= 2330:
        return "Hunter"
    # Newcastle: 2280-2326 (overlaps hunter – prioritise Newcastle window first)
    if 2280 <= p <= 2326:
        return "Newcastle"
    # Blue Mountains: 2773-2787
    if 2773 <= p <= 2787:
        return "Blue Mountains"
    # Northern NSW: 2431-2490, 2830-2844
    if 2431 <= p <= 2490 or 2830 <= p <= 2844:
        return "Northern NSW"
    # Riverina: 2640-2737
    if 2640 <= p <= 2737:
        return "Riverina"
    # Central West and Orana: 2329, 2798
    if p in (2329, 2798):
        return "Central West and Orana"
    # Far West: 2831, 2877-2879
    if p == 2831 or 2877 <= p <= 2879:
        return "Far West"

    return "Unknown"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python aggregate_csvs.py <staging_csv_folder> <final_csv>")
        sys.exit(1)

    staging = Path(sys.argv[1])
    final_csv = Path(sys.argv[2])

    files = sorted(staging.glob("*.csv"))
    if not files:
        print(f"No CSVs found in {staging}")
        sys.exit(0)

    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f, parse_dates=["contract_date","settlement_date"], dtype={"postcode":"string"}))

    df = pd.concat(dfs, ignore_index=True)

    # Dedupe: prefer dealing_number if present, else fallback combo
    if df["dealing_number"].notna().any():
        df = df.sort_values(["dealing_number","contract_date","purchase_price"]).drop_duplicates("dealing_number")
    else:
        df = df.sort_values(["property_id","contract_date","purchase_price"]).drop_duplicates(["property_id","contract_date","purchase_price"])

    # Add Region column from postcode
    df["region"] = df["postcode"].map(classify_region)

    # BI-friendly features
    df["year"]  = df["contract_date"].dt.year
    df["month"] = df["contract_date"].dt.to_period("M").astype(str)
    df["price_per_sqm"] = (df["purchase_price"] / df["area_sqm"]).where(df["area_sqm"] > 0)

    # Tidy strings
    df["locality"] = df["locality"].astype("string").str.title().str.strip()

    final_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(final_csv, index=False)
    print(f"Wrote {len(df)} rows -> {final_csv}")
