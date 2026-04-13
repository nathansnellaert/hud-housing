"""Ingest housing data from HUD USER.

Downloads CSV/Excel files from HUD USER portal.
No authentication required.
"""

from subsets_utils import get, save_raw_file, load_state, save_state

# HUD USER datasets
DATASETS = {
    "fmr_2024": {
        "url": "https://www.huduser.gov/portal/datasets/fmr/fmr2024/FY24_FMRs.xlsx",
        "name": "Fair Market Rents FY2024",
        "desc": "Fair market rents by county for FY2024",
        "format": "xlsx",
    },
    "fmr_2025": {
        "url": "https://www.huduser.gov/portal/datasets/fmr/fmr2025/FY25_FMRs.xlsx",
        "name": "Fair Market Rents FY2025",
        "desc": "Fair market rents by county for FY2025",
        "format": "xlsx",
    },
    "income_limits_2024": {
        "url": "https://www.huduser.gov/portal/datasets/il/il24/Section8-FY24.xlsx",
        "name": "Income Limits FY2024",
        "desc": "Section 8 income limits by area",
        "format": "xlsx",
    },
    "pit_2024": {
        "url": "https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb",
        "name": "Point-in-Time Homeless Counts",
        "desc": "Annual homeless counts by Continuum of Care (2007-2024)",
        "format": "xlsb",
    },
}


def run():
    """Fetch all HUD housing datasets."""
    print("Fetching HUD Housing data...")

    state = load_state("hud_housing")
    completed = set(state.get("completed", []))

    pending = [(k, v) for k, v in DATASETS.items() if k not in completed]

    if not pending:
        print("All datasets up to date")
        return

    print(f"  Datasets to fetch: {len(pending)}")

    for i, (dataset_key, dataset_info) in enumerate(pending, 1):
        print(f"\n[{i}/{len(pending)}] Fetching {dataset_info['name']}...")

        try:
            response = get(dataset_info["url"], timeout=120)
            response.raise_for_status()

            # Save binary content directly (xlsx files are binary)
            save_raw_file(
                response.content,
                f"hud_{dataset_key}",
                extension=dataset_info["format"],
            )
            print(f"    Saved {len(response.content):,} bytes")

            completed.add(dataset_key)
            save_state("hud_housing", {"completed": list(completed)})

        except Exception as e:
            print(f"    Error: {e}")

    print(f"\nIngested {len(completed)} datasets")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
