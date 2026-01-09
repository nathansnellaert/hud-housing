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
from io import BytesIO
from subsets_utils import load_raw_file, sync_data, sync_metadata
from .test import test

DATASET_ID = "hud_income_limits"

METADATA = {
    "id": DATASET_ID,
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


def run():
    """Transform Income Limits data."""
    print("Transforming Income Limits...")

    data = load_raw_file("hud_income_limits_2024", extension="xlsx")
    if isinstance(data, str):
        data = data.encode("latin-1")

    df = pd.read_excel(BytesIO(data), engine="calamine")
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

    sync_data(table, DATASET_ID, mode="overwrite")
    sync_metadata(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
