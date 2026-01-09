"""Tests for Point-in-Time Homeless Counts transform."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_in_set


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
