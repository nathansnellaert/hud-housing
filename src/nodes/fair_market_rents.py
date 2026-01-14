"""Transform Fair Market Rents data from HUD.

Combines FY2024 and FY2025 FMR data into a unified dataset with columns:
- state_code: 2-letter state code
- state_fips: State FIPS code
- county_name: County name
- fips: Full FIPS code
- hud_area_code: HUD area code
- hud_area_name: HUD area name
- metro: Whether the area is metro (1) or non-metro (0)
- fiscal_year: Fiscal year (2024 or 2025)
- population: Population from census
- fmr_0br: Fair Market Rent for 0-bedroom
- fmr_1br: Fair Market Rent for 1-bedroom
- fmr_2br: Fair Market Rent for 2-bedroom
- fmr_3br: Fair Market Rent for 3-bedroom
- fmr_4br: Fair Market Rent for 4-bedroom
"""

import pandas as pd
import pyarrow as pa
from pathlib import Path
from python_calamine import CalamineWorkbook
from subsets_utils import upload_data, validate
from subsets_utils.environment import get_data_dir
from subsets_utils.testing import assert_valid_year, assert_positive, assert_in_set

from nodes.hud_data import run as download

DATASET_ID = "hud_fair_market_rents"

METADATA = {
    "title": "HUD Fair Market Rents",
    "description": "Fair Market Rents (FMRs) by county from HUD. FMRs are used to determine payment standards for Housing Choice Voucher programs.",
    "source": "HUD Office of Policy Development and Research",
    "source_url": "https://www.huduser.gov/portal/datasets/fmr.html",
    "column_descriptions": {
        "state_code": "2-letter state code (e.g., AL, AK)",
        "state_fips": "State FIPS code",
        "county_name": "County name",
        "fips": "Full FIPS code (state + county)",
        "hud_area_code": "HUD metropolitan area code",
        "hud_area_name": "HUD metropolitan area name",
        "metro": "1 if metropolitan area, 0 if non-metropolitan",
        "fiscal_year": "Federal fiscal year (October-September)",
        "population": "Population from census",
        "fmr_0br": "Fair Market Rent for efficiency/0-bedroom unit ($/month)",
        "fmr_1br": "Fair Market Rent for 1-bedroom unit ($/month)",
        "fmr_2br": "Fair Market Rent for 2-bedroom unit ($/month)",
        "fmr_3br": "Fair Market Rent for 3-bedroom unit ($/month)",
        "fmr_4br": "Fair Market Rent for 4-bedroom unit ($/month)",
    },
}


def _load_fmr_year(asset_id: str, fiscal_year: str) -> pd.DataFrame:
    """Load and standardize FMR data for a single fiscal year."""
    filepath = Path(get_data_dir()) / "raw" / f"{asset_id}.xlsx"
    wb = CalamineWorkbook.from_path(str(filepath))
    rows = wb.get_sheet_by_name(wb.sheet_names[0]).to_python()
    headers = [str(h).lower() for h in rows[0]]
    df = pd.DataFrame(rows[1:], columns=headers)

    # Determine population column name (varies by year)
    pop_col = "pop2020" if "pop2020" in df.columns else "pop2022"

    return pd.DataFrame({
        "state_code": df["stusps"].astype(str),
        "state_fips": df["state"].astype(str).str.zfill(2),
        "county_name": df["countyname"].astype(str),
        "fips": df["fips"].astype(str).str.zfill(9),
        "hud_area_code": df["hud_area_code"].astype(str),
        "hud_area_name": df["hud_area_name"].astype(str),
        "metro": df["metro"].astype(int),
        "fiscal_year": fiscal_year,
        "population": df[pop_col].astype(int),
        "fmr_0br": df["fmr_0"].astype(int),
        "fmr_1br": df["fmr_1"].astype(int),
        "fmr_2br": df["fmr_2"].astype(int),
        "fmr_3br": df["fmr_3"].astype(int),
        "fmr_4br": df["fmr_4"].astype(int),
    })


def test(table: pa.Table) -> None:
    """Validate Fair Market Rents output."""
    validate(table, {
        "columns": {
            "state_code": "string",
            "state_fips": "string",
            "county_name": "string",
            "fips": "string",
            "hud_area_code": "string",
            "hud_area_name": "string",
            "metro": "int",
            "fiscal_year": "string",
            "population": "int",
            "fmr_0br": "int",
            "fmr_1br": "int",
            "fmr_2br": "int",
            "fmr_3br": "int",
            "fmr_4br": "int",
        },
        "not_null": ["state_code", "county_name", "fips", "fiscal_year", "fmr_2br"],
        "unique": ["fips", "fiscal_year"],
        "min_rows": 9000,  # ~4764 rows per year x 2 years
    })

    # Validate fiscal years
    assert_valid_year(table, "fiscal_year")
    fiscal_years = set(table.column("fiscal_year").to_pylist())
    assert fiscal_years == {"2024", "2025"}, f"Expected FY2024 and FY2025, got {fiscal_years}"

    # Validate FMRs are positive
    assert_positive(table, "fmr_0br")
    assert_positive(table, "fmr_1br")
    assert_positive(table, "fmr_2br")
    assert_positive(table, "fmr_3br")
    assert_positive(table, "fmr_4br")

    # Validate metro is 0 or 1
    assert_in_set(table, "metro", {0, 1})

    # Validate state codes
    state_codes = set(table.column("state_code").to_pylist())
    assert len(state_codes) >= 50, f"Expected at least 50 states, got {len(state_codes)}"

    print(f"  Validated {len(table):,} Fair Market Rent records")


def run():
    """Transform Fair Market Rents data."""
    print("Transforming Fair Market Rents...")

    # Load both years
    df_2024 = _load_fmr_year("hud_fmr_2024", "2024")
    print(f"  Loaded FY2024: {len(df_2024):,} rows")

    df_2025 = _load_fmr_year("hud_fmr_2025", "2025")
    print(f"  Loaded FY2025: {len(df_2025):,} rows")

    # Combine
    df = pd.concat([df_2024, df_2025], ignore_index=True)
    print(f"  Combined: {len(df):,} rows")

    table = pa.Table.from_pandas(df, preserve_index=False)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
NODES = {
    download: [],
    run: [download],
}

if __name__ == "__main__":
    download()
    run()
