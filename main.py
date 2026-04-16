

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from db.database import init_db
from scrapers.qdr_scraper import run as run_qdr
from scrapers.cessda_scraper import run as run_cessda


def main():
    parser = argparse.ArgumentParser(
        description="SQ26 QDArchive Seeding Pipeline — Part 1: Data Acquisition"
    )
    parser.add_argument(
        "--max-projects", type=int, default=100,
        help="Maximum number of projects to download per source (default: 100)"
    )
    parser.add_argument(
        "--source", choices=["qdr", "cessda", "both"], default="both",
        help="Which repository to run (default: both)"
    )
    args = parser.parse_args()

    print("Initialising database …")
    init_db()
    print("Database ready.\n")

    if args.source in ("qdr", "both"):
        run_qdr(max_projects=args.max_projects)

    if args.source in ("cessda", "both"):
        run_cessda(max_projects=args.max_projects)

    print("\nAll done. Database: seeding.db")


if __name__ == "__main__":
    main()
