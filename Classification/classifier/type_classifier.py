import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import (QDA_EXT, PRIMARY_EXT, get_all_projects,
                          get_project_files, update_project_type, update_file_category)


def classify_file_category(file_type):
    ft = file_type.lower().strip()
    if ft in QDA_EXT:
        return "QDA_FILE"
    if ft in PRIMARY_EXT:
        return "PRIMARY_DATA_FILE"
    return "ADDITIONAL_FILE"


def classify_project_type(project_id):
    files     = get_project_files(project_id)
    succeeded = [f for f in files if f["status"] == "SUCCEEDED"]
    stypes    = {f["file_type"].lower().strip() for f in succeeded}
    if stypes & QDA_EXT:
        return "QDA_PROJECT"
    if stypes & PRIMARY_EXT:
        return "QD_PROJECT"
    if succeeded:
        return "OTHER_PROJECT"
    return "NOT_A_PROJECT"


def run():
    print("\n" + "=" * 60)
    print("Step 1 — Project Type Classification")
    print("=" * 60)

    projects = get_all_projects()
    counts   = {"QDA_PROJECT": 0, "QD_PROJECT": 0,
                "OTHER_PROJECT": 0, "NOT_A_PROJECT": 0}

    for i, p in enumerate(projects, 1):
        pid = p["id"]
        for f in get_project_files(pid):
            update_file_category(f["id"], classify_file_category(f["file_type"]))
        ptype = classify_project_type(pid)
        update_project_type(pid, ptype)
        counts[ptype] += 1
        if i % 100 == 0:
            print(f"  Processed {i}/{len(projects)} projects ...")

    total = sum(counts.values())
    print("\n  Results:")
    for k, v in counts.items():
        bar = "X" * int(v / total * 40)
        print(f"  {k:<20s} {v:>4}  {bar}")
    print(f"  {'TOTAL':<20s} {total:>4}")
    return counts


if __name__ == "__main__":
    run()
