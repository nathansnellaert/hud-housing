"""Tests for Income Limits transform."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_positive, assert_in_set


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
