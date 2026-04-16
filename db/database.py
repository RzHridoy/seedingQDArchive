import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "seeding.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    with open(SCHEMA_PATH, "r") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()


def insert_project(data: dict) -> int:
    conn = get_connection()
    cur = conn.execute("""
        INSERT INTO PROJECTS (
            query_string, repository_id, repository_url, project_url,
            version, title, description, language, doi,
            upload_date, download_date,
            download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (
            :query_string, :repository_id, :repository_url, :project_url,
            :version, :title, :description, :language, :doi,
            :upload_date, :download_date,
            :download_repository_folder, :download_project_folder,
            :download_version_folder, :download_method
        )
    """, data)
    project_id = cur.lastrowid
    conn.commit()
    conn.close()
    return project_id


def insert_file(data: dict):
    conn = get_connection()
    conn.execute("""
        INSERT INTO FILES (project_id, file_name, file_type, status)
        VALUES (:project_id, :file_name, :file_type, :status)
    """, data)
    conn.commit()
    conn.close()


def insert_keyword(project_id: int, keyword: str):
    conn = get_connection()
    conn.execute(
        "INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)",
        (project_id, keyword.strip())
    )
    conn.commit()
    conn.close()


def insert_person_role(project_id: int, name: str, role: str):
    conn = get_connection()
    conn.execute(
        "INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)",
        (project_id, name.strip(), role)
    )
    conn.commit()
    conn.close()


def insert_license(project_id: int, license_str: str):
    conn = get_connection()
    conn.execute(
        "INSERT INTO LICENSES (project_id, license) VALUES (?, ?)",
        (project_id, license_str.strip())
    )
    conn.commit()
    conn.close()


def project_url_exists(project_url: str) -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM PROJECTS WHERE project_url = ?", (project_url,)
    ).fetchone()
    conn.close()
    return row is not None
