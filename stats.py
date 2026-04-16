

import os
import sys
import sqlite3
import csv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db.database import DB_PATH


def print_stats():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("\n" + "=" * 60)
    print("QDArchive Seeding — Part 1 Statistics")
    print("=" * 60)

    # Projects per repository
    rows = conn.execute("""
        SELECT repository_url, COUNT(*) AS n FROM PROJECTS GROUP BY repository_url
    """).fetchall()
    print("\nProjects per repository:")
    total_projects = 0
    for r in rows:
        print(f"  {r['repository_url']:<45} {r['n']:>5}")
        total_projects += r['n']
    print(f"  {'TOTAL':<45} {total_projects:>5}")

    # File statuses
    rows = conn.execute("""
        SELECT status, COUNT(*) AS n FROM FILES GROUP BY status
    """).fetchall()
    print("\nFile download results:")
    total_files = 0
    for r in rows:
        print(f"  {r['status']:<40} {r['n']:>6}")
        total_files += r['n']
    print(f"  {'TOTAL':<40} {total_files:>6}")

    # QDA-specific files
    qda_exts = (
        "qdpx","qdc","mqda","mqbac","mqtc","mqex","mqmtr",
        "mx24","mx24bac","mc24","mex24","mx22","mx20","mx18","mx12",
        "mx11","mx5","mx4","mx3","mx2","m2k","loa","sea","mtr",
        "mod","mex22","nvp","nvpx"
    )
    placeholders = ",".join("?" * len(qda_exts))
    qda_count = conn.execute(
        f"SELECT COUNT(*) FROM FILES WHERE LOWER(file_type) IN ({placeholders})", qda_exts
    ).fetchone()[0]
    print(f"\nQDA files found: {qda_count}")

    # Keywords
    kw_count = conn.execute("SELECT COUNT(*) FROM KEYWORDS").fetchone()[0]
    print(f"Keywords extracted: {kw_count}")

    # People
    person_count = conn.execute("SELECT COUNT(*) FROM PERSON_ROLE").fetchone()[0]
    print(f"Person-role entries: {person_count}")

    # Licenses
    rows = conn.execute("""
        SELECT license, COUNT(*) AS n FROM LICENSES GROUP BY license ORDER BY n DESC LIMIT 10
    """).fetchall()
    print("\nTop licenses:")
    for r in rows:
        print(f"  {r['license']:<35} {r['n']:>5}")

    print("\n" + "=" * 60)
    conn.close()

    # Export to CSV
    export_to_csv()


def export_to_csv():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    export_dir = os.path.dirname(__file__)

    tables = ["PROJECTS", "FILES", "KEYWORDS", "PERSON_ROLE", "LICENSES"]
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            continue
        csv_path = os.path.join(export_dir, f"{table.lower()}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows([dict(r) for r in rows])
        print(f"Exported {len(rows):>5} rows → {csv_path}")

    conn.close()


if __name__ == "__main__":
    print_stats()
