

import os
import time
import requests
from datetime import datetime, timezone

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db.database import (
    insert_project, insert_file, insert_keyword,
    insert_person_role, insert_license, project_url_exists
)

REPO_URL        = "https://data.qdr.syr.edu"
REPO_ID         = 1
REPO_FOLDER     = "qdr"
DATA_DIR        = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", REPO_FOLDER)
MAX_FILE_BYTES  = 500 * 1024 * 1024   # 500 MB hard limit
DELAY           = 1.0                  # seconds between requests

QUERIES         = ["interview", "qualitative data", "qualitative interview"]

QDA_EXTENSIONS  = {
    "qdpx","qdc","mqda","mqbac","mqtc","mqex","mqmtr",
    "mx24","mx24bac","mc24","mex24","mx22","mx20","mx18","mx12",
    "mx11","mx5","mx4","mx3","mx2","m2k","loa","sea","mtr",
    "mod","mex22","nvp","nvpx"
}

session = requests.Session()
session.headers.update({"User-Agent": "SQ26-QDArchive-Seeder/1.0"})


def _api(path: str, params: dict = None):
    url = f"{REPO_URL}/api{path}"
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (401, 403):
                return None   # login required — caller handles
            time.sleep(DELAY)
        except Exception:
            time.sleep(DELAY * 2)
    return None


def _normalize_license(raw: str) -> str:
    if not raw:
        return "UNKNOWN"
    r = raw.strip()
    mapping = {
        "cc0":          "CC0",
        "cc-zero":      "CC0",
        "public domain":"CC0",
        "cc by":        "CC BY",
        "cc-by":        "CC BY",
        "cc by-sa":     "CC BY-SA",
        "cc-by-sa":     "CC BY-SA",
        "cc by-nc":     "CC BY-NC",
        "cc-by-nc":     "CC BY-NC",
        "cc by-nd":     "CC BY-ND",
        "cc by-nc-nd":  "CC BY-NC-ND",
        "cc by-nc-sa":  "CC BY-NC-SA",
        "odbl":         "ODbL",
        "odbl-1.0":     "ODbL-1.0",
        "odc-by":       "ODC-By",
        "odc-by-1.0":   "ODC-By-1.0",
        "pddl":         "PDDL",
    }
    for key, val in mapping.items():
        if key in r.lower():
            # preserve version suffix if present (e.g. CC BY 4.0)
            import re
            ver = re.search(r"\d+\.\d+", r)
            return f"{val} {ver.group()}" if ver else val
    return r   # keep original for later cleanup pass


def _extract_metadata(ds_json: dict) -> dict:
    """Pull fields from a Dataverse dataset JSON response."""
    lv = ds_json.get("latestVersion", {})
    blocks = lv.get("metadataBlocks", {})
    citation = blocks.get("citation", {}).get("fields", [])

    def field(type_name):
        for f in citation:
            if f.get("typeName") == type_name:
                return f.get("value")
        return None

    # Title
    title = field("title") or "Untitled"

    # Description
    desc_raw = field("dsDescription")
    description = ""
    if isinstance(desc_raw, list):
        parts = []
        for item in desc_raw:
            if isinstance(item, dict):
                v = item.get("dsDescriptionValue", {})
                parts.append(v.get("value", "") if isinstance(v, dict) else str(v))
        description = " ".join(parts)
    elif isinstance(desc_raw, str):
        description = desc_raw
    description = description or "No description available."

    # Authors
    authors = []
    author_raw = field("author")
    if isinstance(author_raw, list):
        for a in author_raw:
            if isinstance(a, dict):
                name_field = a.get("authorName", {})
                name = name_field.get("value", "") if isinstance(name_field, dict) else str(name_field)
                if name:
                    authors.append(name)

    # Keywords / subjects
    keywords = []
    kw_raw = field("keyword")
    if isinstance(kw_raw, list):
        for k in kw_raw:
            if isinstance(k, dict):
                kv = k.get("keywordValue", {})
                val = kv.get("value", "") if isinstance(kv, dict) else str(kv)
                if val:
                    keywords.append(val)
    subj_raw = field("subject")
    if isinstance(subj_raw, list):
        keywords.extend(subj_raw)

    # Language
    lang_raw = field("language")
    language = None
    if isinstance(lang_raw, list) and lang_raw:
        language = lang_raw[0]
    elif isinstance(lang_raw, str):
        language = lang_raw

    # DOI
    doi = ds_json.get("persistentUrl") or lv.get("datasetPersistentId")

    # Upload date
    upload_date = None
    pub_date = lv.get("releaseTime") or ds_json.get("publicationDate")
    if pub_date:
        try:
            upload_date = pub_date[:10]   # YYYY-MM-DD
        except Exception:
            pass

    # License
    lic_raw = lv.get("license", {})
    if isinstance(lic_raw, dict):
        lic_str = lic_raw.get("name") or lic_raw.get("uri") or ""
    else:
        lic_str = str(lic_raw) if lic_raw else ""
    lic_str = _normalize_license(lic_str)

    return {
        "title":        title,
        "description":  description,
        "authors":      authors,
        "keywords":     keywords,
        "language":     language,
        "doi":          doi,
        "upload_date":  upload_date,
        "license":      lic_str,
        "files":        lv.get("files", []),
    }


def _download_file(file_id: int, dest_path: str) -> str:
    """Download a single file. Returns DOWNLOAD_RESULT status string."""
    url = f"{REPO_URL}/api/access/datafile/{file_id}"
    try:
        r = session.get(url, stream=True, timeout=60)
        if r.status_code in (401, 403):
            return "FAILED_LOGIN_REQUIRED"
        if r.status_code != 200:
            return "FAILED_SERVER_UNRESPONSIVE"

        size = 0
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as fh:
            for chunk in r.iter_content(65536):
                size += len(chunk)
                if size > MAX_FILE_BYTES:
                    fh.close()
                    os.remove(dest_path)
                    return "FAILED_TOO_LARGE"
                fh.write(chunk)
        return "SUCCEEDED"
    except requests.exceptions.Timeout:
        return "FAILED_SERVER_UNRESPONSIVE"
    except Exception:
        return "FAILED_SERVER_UNRESPONSIVE"


def _search_datasets(query: str, max_results: int) -> list:
    """Return list of dataset persistent IDs matching query."""
    ids = []
    page = 1
    per_page = 20
    while len(ids) < max_results:
        data = _api("/search", {
            "q": query,
            "type": "dataset",
            "start": (page - 1) * per_page,
            "per_page": per_page,
        })
        if not data:
            break
        items = data.get("data", {}).get("items", [])
        if not items:
            break
        for item in items:
            pid = item.get("global_id") or item.get("identifier")
            if pid and pid not in ids:
                ids.append(pid)
        total = data.get("data", {}).get("total_count", 0)
        if page * per_page >= total or page * per_page >= max_results:
            break
        page += 1
        time.sleep(DELAY)
    return ids[:max_results]


def run(max_projects: int = 100):
    print(f"\n{'='*60}")
    print(f"QDR Scraper — target: {max_projects} projects")
    print(f"{'='*60}")

    seen_pids = set()
    all_pids  = []

    for query in QUERIES:
        print(f"  Searching: '{query}'")
        pids = _search_datasets(query, max_projects)
        for pid in pids:
            if pid not in seen_pids:
                seen_pids.add(pid)
                all_pids.append((pid, query))
        time.sleep(DELAY)

    print(f"  Found {len(all_pids)} unique datasets across all queries.")
    processed = 0

    for pid, query in all_pids:
        if processed >= max_projects:
            break

        # Build project URL
        project_url = f"{REPO_URL}/dataset.xhtml?persistentId={pid}"
        if project_url_exists(project_url):
            print(f"  [SKIP] Already in DB: {pid}")
            continue

        # Fetch full metadata
        ds_data = _api(f"/datasets/:persistentId", {"persistentId": pid})
        if not ds_data or "data" not in ds_data:
            print(f"  [WARN] Could not fetch metadata for {pid}")
            time.sleep(DELAY)
            continue

        meta = _extract_metadata(ds_data["data"])
        if not meta["license"] or meta["license"] in ("UNKNOWN", ""):
            print(f"  [SKIP] No open license: {pid}")
            continue

        # Derive folder names from PID (e.g. doi:10.5064/F6ABC → F6ABC)
        pid_slug = pid.replace("doi:", "").replace("/", "_").replace(":", "_")
        project_folder = pid_slug

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        project_id = insert_project({
            "query_string":               query,
            "repository_id":              REPO_ID,
            "repository_url":             REPO_URL,
            "project_url":                project_url,
            "version":                    None,
            "title":                      meta["title"],
            "description":                meta["description"],
            "language":                   meta["language"],
            "doi":                        meta["doi"],
            "upload_date":                meta["upload_date"],
            "download_date":              now,
            "download_repository_folder": REPO_FOLDER,
            "download_project_folder":    project_folder,
            "download_version_folder":    None,
            "download_method":            "API-CALL",
        })

        insert_license(project_id, meta["license"])

        for kw in meta["keywords"]:
            if kw.strip():
                insert_keyword(project_id, kw)

        for author in meta["authors"]:
            if author.strip():
                insert_person_role(project_id, author, "AUTHOR")

        # Download files
        dest_root = os.path.join(DATA_DIR, project_folder)
        os.makedirs(dest_root, exist_ok=True)
        file_count = 0

        for f in meta["files"]:
            df   = f.get("dataFile", {})
            fid  = df.get("id")
            fname = df.get("filename") or f.get("label") or f"file_{fid}"
            ext  = os.path.splitext(fname)[1].lstrip(".").lower()

            if not fid:
                continue

            dest_path = os.path.join(dest_root, fname)
            # Skip if already downloaded
            if os.path.exists(dest_path):
                status = "SUCCEEDED"
            else:
                status = _download_file(fid, dest_path)
                time.sleep(DELAY)

            insert_file({
                "project_id": project_id,
                "file_name":  fname,
                "file_type":  ext or "unknown",
                "status":     status,
            })
            file_count += 1

        processed += 1
        print(f"  [{processed:>4}] {meta['title'][:60]} | files={file_count} | license={meta['license']}")
        time.sleep(DELAY)

    print(f"\nQDR done. Projects inserted: {processed}")
