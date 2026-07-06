import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "23047806_seeding.db")

QDA_EXT = frozenset({
    'qdpx','qdc','mqda','mqbac','mqtc','mqex','mqmtr',
    'mx24','mx24bac','mc24','mex24','mx22','mx20','mx18','mx12',
    'mx11','mx5','mx4','mx3','mx2','m2k','loa','sea','mtr',
    'mod','mex22','nvp','nvpx'
})

PRIMARY_EXT = frozenset({
    'pdf','doc','docx','txt','rtf','odt','csv','xls','xlsx',
    'ppt','pptx','mp3','mp4','m4a','wav','mpg','mpeg','avi',
    'png','jpg','jpeg','gif','heic','amr','tab','json','html','nt','zip'
})

REPO_NAMES = {
    1: "QDR (data.qdr.syr.edu)",
    2: "CESSDA (datacatalogue.cessda.eu)"
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def migrate():
    conn = get_conn()
    cur  = conn.cursor()

    existing = {r[1] for r in cur.execute("PRAGMA table_info(PROJECTS)").fetchall()}
    for col, dtype in [("project_type","TEXT"), ("primary_class","TEXT"),
                       ("secondary_class","TEXT"), ("tags","TEXT")]:
        if col not in existing:
            cur.execute(f"ALTER TABLE PROJECTS ADD COLUMN {col} {dtype}")
            print(f"  Added column: PROJECTS.{col}")

    existing_f = {r[1] for r in cur.execute("PRAGMA table_info(FILES)").fetchall()}
    if "file_category" not in existing_f:
        cur.execute("ALTER TABLE FILES ADD COLUMN file_category TEXT")
        print("  Added column: FILES.file_category")

    conn.commit()
    conn.close()
    print("Migration complete.")


def get_all_projects():
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, repository_id, title, description,
               language, doi, project_url, project_type
        FROM PROJECTS ORDER BY id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_project_files(project_id):
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, file_name, file_type, status FROM FILES WHERE project_id=?",
        (project_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_project_keywords(project_id):
    conn = get_conn()
    rows = conn.execute(
        "SELECT keyword FROM KEYWORDS WHERE project_id=?", (project_id,)
    ).fetchall()
    conn.close()
    return [r["keyword"] for r in rows]


def update_project_type(project_id, project_type):
    conn = get_conn()
    conn.execute("UPDATE PROJECTS SET project_type=? WHERE id=?",
                 (project_type, project_id))
    conn.commit()
    conn.close()


def update_project_classification(project_id, primary_class, secondary_class, tags):
    conn = get_conn()
    conn.execute(
        "UPDATE PROJECTS SET primary_class=?, secondary_class=?, tags=? WHERE id=?",
        (primary_class, secondary_class, tags, project_id)
    )
    conn.commit()
    conn.close()


def update_file_category(file_id, category):
    conn = get_conn()
    conn.execute("UPDATE FILES SET file_category=? WHERE id=?", (category, file_id))
    conn.commit()
    conn.close()


def get_all_for_export():
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.repository_id, p.project_type, p.title,
               p.primary_class, p.secondary_class,
               COUNT(f.id) as no_project_files
        FROM PROJECTS p
        LEFT JOIN FILES f ON f.project_id = p.id
        GROUP BY p.id
        ORDER BY p.repository_id, p.project_type, p.id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
