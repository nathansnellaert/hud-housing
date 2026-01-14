"""Transform Income Limits data from HUD.

Section 8 income limits for FY2024, defining eligibility thresholds.

Income limit categories:
- l50_*: 50% of Area Median Income (Very Low Income)
- ELI_*: 30% of Area Median Income (Extremely Low Income)
- l80_*: 80% of Area Median Income (Low Income)

The *_N suffix indicates household size (1-8 persons).
"""

import pandas as pd
import pyarrow as pa
from pathlib import Path
from python_calamine import CalamineWorkbook
from subsets_utils import upload_data, validate
from subsets_utils.environment import get_data_dir
from subsets_utils.testing import assert_valid_year, assert_positive, assert_in_set

from nodes.hud_data import run as download

DATASET_ID = "hud_income_limits"

METADATA = {
    "title": "HUD Income Limits",
    "description": "Section 8 income limits by area, defining eligibility thresholds for housing assistance programs. Includes Very Low (50% AMI), Extremely Low (30% AMI), and Low (80% AMI) income limits.",
    "source": "HUD Office of Policy Development and Research",
    "source_url": "https://www.huduser.gov/portal/datasets/il.html",
    "column_descriptions": {
        "fips": "Full FIPS code (state + county)",
        "state_code": "2-letter state code",
        "state_fips": "State FIPS code",
        "state_name": "Full state name",
        "hud_area_code": "HUD area code",
        "hud_area_name": "HUD area name",
        "county_fips": "County FIPS code",
        "county_name": "County name",
        "metro": "1 if metropolitan area, 0 if non-metropolitan",
        "fiscal_year": "Federal fiscal year",
        "median_income": "Area median income for 4-person household",
        "eli_1": "Extremely Low Income limit (30% AMI) for 1 person",
        "eli_2": "Extremely Low Income limit for 2 persons",
        "eli_3": "Extremely Low Income limit for 3 persons",
        "eli_4": "Extremely Low Income limit for 4 persons",
        "eli_5": "Extremely Low Income limit for 5 persons",
        "eli_6": "Extremely Low Income limit for 6 persons",
        "eli_7": "Extremely Low Income limit for 7 persons",
        "eli_8": "Extremely Low Income limit for 8 persons",
        "vli_1": "Very Low Income limit (50% AMI) for 1 person",
        "vli_2": "Very Low Income limit for 2 persons",
        "vli_3": "Very Low Income limit for 3 persons",
        "vli_4": "Very Low Income limit for 4 persons",
        "vli_5": "Very Low Income limit for 5 persons",
        "vli_6": "Very Low Income limit for 6 persons",
        "vli_7": "Very Low Income limit for 7 persons",
        "vli_8": "Very Low Income limit for 8 persons",
        "li_1": "Low Income limit (80% AMI) for 1 person",
        "li_2": "Low Income limit for 2 persons",
        "li_3": "Low Income limit for 3 persons",
        "li_4": "Low Income limit for 4 persons",
        "li_5": "Low Income limit for 5 persons",
        "li_6": "Low Income limit for 6 persons",
        "li_7": "Low Income limit for 7 persons",
        "li_8": "Low Income limit for 8 persons",
    },
}


def test(table: pa.Table) -> None:
    """Validate Income Limits output."""
    validate(table, {
        "columns": {
            "fips": "string",
            "state_code": "string",
            "state_fips": "string",
            "state_name": "string",
            "hud_area_code": "string",
            "hud_area_name": "string",
            "county_fips": "string",
            "county_name": "string",
            "metro": "int",
            "fiscal_year": "string",
            "median_income": "int",
            "eli_1": "int",
            "eli_4": "int",
            "vli_1": "int",
            "vli_4": "int",
            "li_1": "int",
            "li_4": "int",
        },
        "not_null": ["fips", "state_code", "county_name", "fiscal_year", "median_income"],
        "unique": ["fips"],
        "min_rows": 4500,
    })

    # Validate fiscal year
    assert_valid_year(table, "fiscal_year")
    fiscal_years = set(table.column("fiscal_year").to_pylist())
    assert fiscal_years == {"2024"}, f"Expected FY2024, got {fiscal_years}"

    # Validate income limits are positive
    assert_positive(table, "median_income")
    assert_positive(table, "eli_4")
    assert_positive(table, "vli_4")
    assert_positive(table, "li_4")

    # Validate income hierarchy: ELI < VLI < LI
    df = table.to_pandas()
    assert (df["eli_4"] <= df["vli_4"]).all(), "ELI should be <= VLI"
    assert (df["vli_4"] <= df["li_4"]).all(), "VLI should be <= LI"

    # Validate metro is 0 or 1
    assert_in_set(table, "metro", {0, 1})

    # Validate state codes
    state_codes = set(table.column("state_code").to_pylist())
    assert len(state_codes) >= 50, f"Expected at least 50 states, got {len(state_codes)}"

    print(f"  Validated {len(table):,} Income Limit records")


def run():
    """Transform Income Limits data."""
    print("Transforming Income Limits...")

    filepath = Path(get_data_dir()) / "raw" / "hud_income_limits_2024.xlsx"
    wb = CalamineWorkbook.from_path(str(filepath))
    rows = wb.get_sheet_by_name(wb.sheet_names[0]).to_python()
    headers = [str(h) for h in rows[0]]
    df = pd.DataFrame(rows[1:], columns=headers)
    print(f"  Loaded {len(df):,} rows")

    result = pd.DataFrame({
        "fips": df["fips"].astype(str).str.zfill(9),
        "state_code": df["stusps"].astype(str),
        "state_fips": df["state"].astype(str).str.zfill(2),
        "state_name": df["state_name"].astype(str),
        "hud_area_code": df["hud_area_code"].astype(str),
        "hud_area_name": df["hud_area_name"].astype(str),
        "county_fips": df["county"].astype(str).str.zfill(3),
        "county_name": df["County_Name"].astype(str),
        "metro": df["metro"].astype(int),
        "fiscal_year": "2024",
        "median_income": df["median2024"].astype(int),
        # Extremely Low Income (30% AMI)
        "eli_1": df["ELI_1"].astype(int),
        "eli_2": df["ELI_2"].astype(int),
        "eli_3": df["ELI_3"].astype(int),
        "eli_4": df["ELI_4"].astype(int),
        "eli_5": df["ELI_5"].astype(int),
        "eli_6": df["ELI_6"].astype(int),
        "eli_7": df["ELI_7"].astype(int),
        "eli_8": df["ELI_8"].astype(int),
        # Very Low Income (50% AMI)
        "vli_1": df["l50_1"].astype(int),
        "vli_2": df["l50_2"].astype(int),
        "vli_3": df["l50_3"].astype(int),
        "vli_4": df["l50_4"].astype(int),
        "vli_5": df["l50_5"].astype(int),
        "vli_6": df["l50_6"].astype(int),
        "vli_7": df["l50_7"].astype(int),
        "vli_8": df["l50_8"].astype(int),
        # Low Income (80% AMI)
        "li_1": df["l80_1"].astype(int),
        "li_2": df["l80_2"].astype(int),
        "li_3": df["l80_3"].astype(int),
        "li_4": df["l80_4"].astype(int),
        "li_5": df["l80_5"].astype(int),
        "li_6": df["l80_6"].astype(int),
        "li_7": df["l80_7"].astype(int),
        "li_8": df["l80_8"].astype(int),
    })

    print(f"  Transformed: {len(result):,} rows")

    table = pa.Table.from_pandas(result, preserve_index=False)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
NODES = {
    download: [],
    run: [download],
}

if __name__ == "__main__":
    download()
    run()
