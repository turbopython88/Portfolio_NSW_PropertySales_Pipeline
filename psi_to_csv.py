# Parses a single NSW PSI .DAT (2001-current) into a tidy CSV.
# Keeps B-records; maps key fields; basic typing and address assembly.
# Import Assets
import sys, pandas as pd
from pathlib import Path

def parse_psi_dat(dat_path: Path) -> pd.DataFrame:
    rows = []
    with dat_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.rstrip("\n").split(";")
            if not parts or parts[0] != "B":
                continue
            def at(i): return parts[i] if i < len(parts) and parts[i] != "" else None
            rows.append({
                "district_code": at(1),
                "property_id": at(2),
                "sale_counter": at(3),
                "downloaded_at": at(4),
                "property_name": at(5),
                "unit_no": at(6),
                "house_no": at(7),
                "street_name": at(8),
                "locality": at(9),
                "postcode": at(10),
                "area_raw": at(11),
                "area_type": at(12),              # H=hectares, M=sqm
                "contract_date_raw": at(13),      # CCYYMMDD
                "settlement_date_raw": at(14),    # CCYYMMDD
                "purchase_price": at(15),
                "zoning": at(16),
                "nature_of_property": at(17),     # V,R,3
                "primary_purpose": at(18),
                "strata_lot_no": at(19),
                "component_code": at(20),
                "sale_code": at(21),
                "percent_interest": at(22),
                "dealing_number": at(23),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Coerce types
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce")
    df["area_raw"] = pd.to_numeric(df["area_raw"], errors="coerce")

    # Dates
    for c in ["contract_date_raw", "settlement_date_raw"]:
        df[c.replace("_raw", "")] = pd.to_datetime(df[c], format="%Y%m%d", errors="coerce")

    # Address tidy
    df["locality"] = df["locality"].astype("string").str.title().str.strip()
    df["street_name"] = df["street_name"].astype("string").str.title().str.strip()

    import pandas as pd
    def s(v):
        # Turn None/<NA>/nan into empty string; strip whitespace
        return "" if pd.isna(v) else str(v).strip()

    def make_addr(r):
        left_parts  = [s(r.get("unit_no")), s(r.get("house_no")), s(r.get("street_name"))]
        left        = " ".join(p for p in left_parts if p)
        right_parts = [s(r.get("locality")), s(r.get("postcode"))]
        right       = ", ".join(p for p in right_parts if p)
        return (left + (" " if left and right else "") + right).strip()

    df["address"] = df.apply(make_addr, axis=1)


    # Area to sqm
    df["area_sqm"] = df.apply(lambda r: r["area_raw"]*10000 if pd.notna(r["area_raw"]) and r["area_type"]=="H" else r["area_raw"], axis=1)

    # Flags & tidy cols
    df["is_strata"] = df["strata_lot_no"].notna()

    keep = [
        "address","locality","postcode",
        "purchase_price","contract_date","settlement_date",
        "area_sqm","zoning","nature_of_property","primary_purpose",
        "property_id","district_code","dealing_number","is_strata"
    ]
    return df[keep]

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python psi_to_csv.py <input_dat> <output_csv>")
        sys.exit(1)
    src = Path(sys.argv[1])
    out = Path(sys.argv[2])
    df = parse_psi_dat(src)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote {len(df)} rows -> {out}")

