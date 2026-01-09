"""Transform Point-in-Time Homeless Counts from HUD.

Creates a time-series dataset of homeless counts by Continuum of Care (CoC),
combining data from 2007-2024. The raw data has varying column structures
across years, so we extract core metrics that are consistent.

Output columns:
- coc_number: Continuum of Care identifier (e.g., AK-500)
- coc_name: Full name of the CoC
- year: Year of the count
- count_type: 'Sheltered', 'Unsheltered', or 'Overall'
- total: Total homeless count
- under_18: Homeless under 18 years old
- age_18_to_24: Homeless aged 18-24
- over_24: Homeless over 24 years old
- individuals: Individual homeless (not in families)
- people_in_families: People in homeless families
- veterans: Homeless veterans
- chronically_homeless: Chronically homeless individuals
"""

import pandas as pd
import pyarrow as pa
from io import BytesIO
from subsets_utils import load_raw_file, sync_data, sync_metadata
from .test import test

DATASET_ID = "hud_homeless_counts"

METADATA = {
    "id": DATASET_ID,
    "title": "HUD Point-in-Time Homeless Counts",
    "description": "Annual Point-in-Time (PIT) homeless counts by Continuum of Care (CoC) from 2007-2024. PIT counts are conducted on a single night in January each year.",
    "source": "HUD Office of Community Planning and Development",
    "source_url": "https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/",
    "column_descriptions": {
        "coc_number": "Continuum of Care identifier (e.g., AK-500)",
        "coc_name": "Full name of the Continuum of Care region",
        "year": "Year of the Point-in-Time count",
        "count_type": "Type of count: 'Sheltered', 'Unsheltered', or 'Overall'",
        "total": "Total homeless count",
        "under_18": "Count of homeless under 18 years old",
        "age_18_to_24": "Count of homeless aged 18-24 years",
        "over_24": "Count of homeless over 24 years old",
        "individuals": "Individual homeless (not part of family units)",
        "people_in_families": "People who are homeless as part of family units",
        "veterans": "Homeless veterans",
        "chronically_homeless": "Chronically homeless individuals",
    },
}

# Years to process (each is a sheet in the Excel file)
YEARS = list(range(2007, 2025))


def _safe_int(val):
    """Convert to int, handling NaN/None."""
    if pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _get_col(df: pd.DataFrame, patterns: list[str]):
    """Find first matching column from patterns list."""
    for pattern in patterns:
        matching = [c for c in df.columns if pattern.lower() in c.lower()]
        if matching:
            return matching[0]
    return None


def _extract_shelter_type(df: pd.DataFrame, year: int, shelter_type: str) -> list[dict]:
    """Extract counts for a specific shelter type from a year's data.

    shelter_type: 'Overall', 'Sheltered', or 'Unsheltered'
    """
    rows = []

    # Column name patterns vary by year and shelter type
    if shelter_type == "Overall":
        prefix = "Overall Homeless"
    elif shelter_type == "Sheltered":
        # Sheltered = ES + TH + SH (or just ES + TH in earlier years)
        # We'll use the combined values where available
        prefix = "Sheltered"
    else:  # Unsheltered
        prefix = "Unsheltered"

    # Find relevant columns
    total_col = _get_col(df, [f"{prefix} Homeless", f"{prefix}"])
    under_18_col = _get_col(df, [f"{prefix} Homeless - Under 18", f"{prefix} - Under 18"])
    age_18_24_col = _get_col(df, [f"{prefix} Homeless - Age 18 to 24", f"{prefix} - 18 to 24"])
    over_24_col = _get_col(df, [f"{prefix} Homeless - Over 24", f"{prefix} - Over 24"])
    individuals_col = _get_col(df, [f"{prefix} Homeless - Homeless Individuals", f"{prefix} - Individuals"])
    families_col = _get_col(df, [f"{prefix} Homeless - Homeless People in Families", f"{prefix} - Families"])
    veterans_col = _get_col(df, [f"{prefix} Homeless - Veterans", f"{prefix} Veterans"])
    chronic_col = _get_col(df, [f"{prefix} Homeless - Chronically Homeless", f"{prefix} Chronically"])

    for _, row in df.iterrows():
        coc_number = row.get("CoC Number", "")
        coc_name = row.get("CoC Name", "")

        if not coc_number or pd.isna(coc_number):
            continue

        record = {
            "coc_number": str(coc_number).strip(),
            "coc_name": str(coc_name).strip() if coc_name and not pd.isna(coc_name) else "",
            "year": str(year),
            "count_type": shelter_type,
            "total": _safe_int(row.get(total_col)) if total_col else None,
            "under_18": _safe_int(row.get(under_18_col)) if under_18_col else None,
            "age_18_to_24": _safe_int(row.get(age_18_24_col)) if age_18_24_col else None,
            "over_24": _safe_int(row.get(over_24_col)) if over_24_col else None,
            "individuals": _safe_int(row.get(individuals_col)) if individuals_col else None,
            "people_in_families": _safe_int(row.get(families_col)) if families_col else None,
            "veterans": _safe_int(row.get(veterans_col)) if veterans_col else None,
            "chronically_homeless": _safe_int(row.get(chronic_col)) if chronic_col else None,
        }

        # Only include if we have at least a total count
        if record["total"] is not None:
            rows.append(record)

    return rows


def run():
    """Transform Point-in-Time Homeless Counts data."""
    print("Transforming Point-in-Time Homeless Counts...")

    data = load_raw_file("hud_pit_2024", extension="xlsb")
    if isinstance(data, str):
        data = data.encode("latin-1")

    all_rows = []

    for year in YEARS:
        try:
            df = pd.read_excel(BytesIO(data), sheet_name=str(year), engine="calamine")
        except Exception as e:
            print(f"  Warning: Could not read year {year}: {e}")
            continue

        year_rows = []
        for shelter_type in ["Overall", "Sheltered", "Unsheltered"]:
            rows = _extract_shelter_type(df, year, shelter_type)
            year_rows.extend(rows)

        print(f"  {year}: {len(year_rows):,} records")
        all_rows.extend(year_rows)

    print(f"  Total: {len(all_rows):,} records")

    df = pd.DataFrame(all_rows)
    table = pa.Table.from_pandas(df, preserve_index=False)

    test(table)

    sync_data(table, DATASET_ID, mode="overwrite")
    sync_metadata(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
