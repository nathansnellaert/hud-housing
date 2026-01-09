"""Tests for Fair Market Rents transform."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_positive, assert_in_set


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
