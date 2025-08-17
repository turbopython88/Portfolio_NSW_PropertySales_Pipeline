# Usage:
#   powershell -ExecutionPolicy Bypass -File .\scripts\run_all.ps1
# (Optionally pass -Py to point to a specific Python, e.g., -Py "C:\Python313\python.exe")

param(
  [string]$Py = "python"
)

# go to repo root
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$Here\.."

$RawZip = "data\raw_zip"
$RawDat = "data\raw_dat"
$Stage  = "data\staging_csv"
$Proc   = "data\processed"
$Final  = "$Proc\psi_sales_clean.csv"

# venv bootstrap (optional)
if (-not (Test-Path ".\.venv")) {
  & $Py -m venv .venv
}
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip > $null
pip install pandas > $null

# 1) Expand all ZIPs -> raw_dat
New-Item -ItemType Directory -Force -Path $RawDat | Out-Null
Get-ChildItem $RawZip -Filter *.zip | ForEach-Object {
  $zip = $_.FullName
  Write-Host "Extracting $($_.Name) ..."
  Expand-Archive -Path $zip -DestinationPath $RawDat -Force
}

# 2) Parse each .DAT -> staging CSV
New-Item -ItemType Directory -Force -Path $Stage | Out-Null
Get-ChildItem $RawDat -Filter *.DAT | ForEach-Object {
  $in  = $_.FullName
  $out = Join-Path $Stage ($_.BaseName + ".csv")
  Write-Host "Parsing $($_.Name) -> $out"
  python scripts\psi_to_csv.py $in $out
}

# 3) Aggregate/dedupe/clean -> final CSV (adds Region column)
New-Item -ItemType Directory -Force -Path $Proc | Out-Null
python scripts\aggregate_csvs.py $Stage $Final

Write-Host "`nDone. Final file: $Final"
