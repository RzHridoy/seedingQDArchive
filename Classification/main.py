import argparse
import sys
import os

#Ensure the project roots

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db.database import migrate


def main():
    parser = argparse.ArgumentParser(
        description="SQ26 Part 2: Data Classification Pipeline"
    )
    parser.add_argument(
        "--step", choices=["1", "2", "export", "all"], default="all",
        help="Which step to run (default: all)"
    )
    parser.add_argument(
        "--no-api", action="store_true",
        help="Skip Claude API; use keyword heuristic only (works offline)"
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("SQ26 Part 2 — Data Classification Pipeline")
    print("=" * 60)
    print("Database : 23047806_seeding.db")
    print("Student  : 23047806")

    print("\nRunning database migration ...")
    migrate()

    if args.step in ("1", "all"):
        from classifier.type_classifier import run as step1
        step1()

    if args.step in ("2", "all"):
        from classifier.isic_classifier import run as step2
        step2(use_api=not args.no_api)

    if args.step in ("export", "all"):
        from export.xlsx_export import run as export_xlsx
        export_xlsx()

    print("\n" + "=" * 60)
    print("Done. Output files:")
    print("  23047806_seeding.db                       <- updated database")
    print("  23047806-sq26-classification.db           <- submission copy")
    print("  23047806_sq26_classification.xlsx          <- Step 4c")
    print("  23047806_sq26_classification_report.pdf    <- Step 4d")
    print("=" * 60)

    #Make the submission

    import shutil
    src = os.path.join(ROOT, "23047806_seeding.db")
    dst = os.path.join(ROOT, "23047806-sq26-classification.db")
    shutil.copy2(src, dst)
    print(f"\nSubmission DB copied -> {dst}")


if __name__ == "__main__":
    main()
