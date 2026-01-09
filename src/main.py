"""HUD Housing Data Connector - fetches U.S. housing statistics.

Data sources:
- HUD USER: https://www.huduser.gov/portal/datasets/

Datasets:
- Fair Market Rents
- Income limits
- Homelessness counts
"""

import argparse
import os

os.environ["RUN_ID"] = os.getenv("RUN_ID", "local-run")

from subsets_utils import validate_environment
from ingest import hud_data as ingest_hud
from transforms import fair_market_rents, income_limits, homeless_counts


def main():
    parser = argparse.ArgumentParser(description="HUD Housing Data Connector")
    parser.add_argument(
        "--ingest-only", action="store_true", help="Only fetch data from API"
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only transform existing raw data",
    )
    parser.add_argument(
        "transform",
        nargs="?",
        choices=["fair_market_rents", "income_limits", "homeless_counts"],
        help="Run a specific transform only",
    )
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only and not args.transform
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_hud.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transforms = {
            "fair_market_rents": fair_market_rents.main,
            "income_limits": income_limits.main,
            "homeless_counts": homeless_counts.main,
        }

        if args.transform:
            print(f"Running transform: {args.transform}")
            transforms[args.transform].run()
        else:
            for name, module in transforms.items():
                print(f"\n--- {name} ---")
                module.run()


if __name__ == "__main__":
    main()
