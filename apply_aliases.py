#Import assets
import pandas as pd
from pathlib import Path

SALES_IN  = Path("data/processed/psi_sales_clean.csv")
ALIAS_CSV = Path("data/aux/alias_localities.csv")
SALES_OUT = Path("data/processed/psi_sales_clean_alias.csv")

sales = pd.read_csv(SALES_IN, dtype={"postcode":"string"})
alias = pd.read_csv(ALIAS_CSV, dtype={"postcode":"string"})

# tidy key fields
sales["locality"] = sales["locality"].astype("string").str.replace(r"\s+"," ", regex=True).str.strip().str.title()
sales["postcode"] = sales["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)

alias["locality"] = alias["locality"].astype("string").str.replace(r"\s+"," ", regex=True).str.strip().str.title()
alias["canonical_locality"] = alias["canonical_locality"].astype("string").str.replace(r"\s+"," ", regex=True).str.strip().str.title()
alias["postcode"] = alias["postcode"].astype("string").str.replace(r"\D+","", regex=True).str.zfill(4)

# apply canonical_locality where a pair matches
sales = sales.merge(alias, on=["locality","postcode"], how="left")
sales["locality"] = sales["canonical_locality"].fillna(sales["locality"])
sales.drop(columns=["canonical_locality"], inplace=True)

SALES_OUT.parent.mkdir(parents=True, exist_ok=True)
sales.to_csv(SALES_OUT, index=False)
print(f"Wrote {SALES_OUT}")

