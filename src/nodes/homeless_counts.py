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
from pathlib import Path
from python_calamine import CalamineWorkbook
from subsets_utils import upload_data, validate
from subsets_utils.environment import get_data_dir
from subsets_utils.testing import assert_valid_year, assert_in_set

from nodes.hud_data import run as download

DATASET_ID = "hud_homeless_counts"

METADATA = {
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


def test(table: pa.Table) -> None:
    """Validate Homeless Counts output."""
    validate(table, {
        "columns": {
            "coc_number": "string",
            "coc_name": "string",
            "year": "string",
            "count_type": "string",
            "total": "int",
        },
        "not_null": ["coc_number", "year", "count_type", "total"],
        "unique": ["coc_number", "year", "count_type"],
        "min_rows": 10000,  # ~400 CoCs x 18 years x ~3 shelter types
    })

    # Validate years are in expected range
    assert_valid_year(table, "year")
    years = set(table.column("year").to_pylist())
    assert "2024" in years, "Should include 2024 data"
    assert "2007" in years or "2008" in years, "Should include early years"

    # Validate count types
    assert_in_set(table, "count_type", {"Overall", "Sheltered", "Unsheltered"})

    # Validate CoC number format (XX-NNN)
    coc_numbers = table.column("coc_number").to_pylist()
    sample = coc_numbers[:10]
    for coc in sample:
        assert len(coc) >= 6, f"CoC number should be at least 6 chars: {coc}"
        assert "-" in coc, f"CoC number should contain dash: {coc}"

    # Check we have reasonable coverage
    unique_cocs = len(set(coc_numbers))
    assert unique_cocs >= 300, f"Expected at least 300 CoCs, got {unique_cocs}"

    print(f"  Validated {len(table):,} Homeless Count records across {unique_cocs} CoCs")


def run():
    """Transform Point-in-Time Homeless Counts data."""
    print("Transforming Point-in-Time Homeless Counts...")

    filepath = Path(get_data_dir()) / "raw" / "hud_pit_2024.xlsb"
    wb = CalamineWorkbook.from_path(str(filepath))

    all_rows = []

    for year in YEARS:
        sheet_name = str(year)
        if sheet_name not in wb.sheet_names:
            print(f"  Warning: Sheet {year} not found")
            continue
        try:
            rows = wb.get_sheet_by_name(sheet_name).to_python()
            headers = [str(h) for h in rows[0]]
            df = pd.DataFrame(rows[1:], columns=headers)
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

    # Define explicit schema to avoid null type columns
    schema = pa.schema([
        pa.field("coc_number", pa.string(), nullable=False),
        pa.field("coc_name", pa.string(), nullable=True),
        pa.field("year", pa.string(), nullable=False),
        pa.field("count_type", pa.string(), nullable=False),
        pa.field("total", pa.int64(), nullable=True),
        pa.field("under_18", pa.int64(), nullable=True),
        pa.field("age_18_to_24", pa.int64(), nullable=True),
        pa.field("over_24", pa.int64(), nullable=True),
        pa.field("individuals", pa.int64(), nullable=True),
        pa.field("people_in_families", pa.int64(), nullable=True),
        pa.field("veterans", pa.int64(), nullable=True),
        pa.field("chronically_homeless", pa.int64(), nullable=True),
    ])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    test(table)

    upload_data(table, DATASET_ID, mode="overwrite")
NODES = {
    download: [],
    run: [download],
}

if __name__ == "__main__":
    download()
    run()
