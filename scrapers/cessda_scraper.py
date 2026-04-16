
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db.database import (
    insert_project, insert_file, insert_keyword,
    insert_person_role, insert_license, project_url_exists,
)

# Constants ────────────────────────────────────────────────────────────────
OAI_BASE        = "https://datacatalogue.cessda.eu/oai-pmh/v0/oai"
REPO_URL        = "https://datacatalogue.cessda.eu"
REPO_ID         = 2
REPO_FOLDER     = "cessda"
DATA_DIR        = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", REPO_FOLDER)

USER_AGENT      = "QDArchiveSeeder/1.0 (+educational use)"
TIMEOUT         = 60
PAUSE           = 0.4
MAX_DISC_PAGES  = 3
MAX_FILE_BYTES  = 2 * 1024 * 1024 * 1024   # 2 GiB

QUERIES         = ["interview", "qualitative data", "qualitative interview"]

ALLOWED_EXT = {
    ".qdpx", ".qdc", ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr",
    ".mx24", ".mx24bac", ".mc24", ".mex24", ".mx22", ".mx20", ".mx18",
    ".mx12", ".mx11", ".mx5", ".mx4", ".mx3", ".mx2", ".m2k", ".loa",
    ".sea", ".mtr", ".mod", ".mex22", ".nvp", ".nvpx", ".pdf", ".docx",
}

NS = {
    "oai":      "http://www.openarchives.org/OAI/2.0/",
    "dc":       "http://purl.org/dc/elements/1.1/",
    "oai_dc":   "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "datacite": "http://datacite.org/schema/kernel-4",
}

HEADERS = {"User-Agent": USER_AGENT}


# Helpers ──────────────────────────────────────────────────────────────────
def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _ext(filename_or_url: str) -> str:
    return Path(urlparse(filename_or_url).path).suffix.lower()


def _slugify(text: str, max_len: int = 120) -> str:
    cleaned = re.sub(r"[^\w\s\-.]", "", text, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", "_", cleaned.strip())
    return (cleaned[:max_len] or "untitled_record").strip("._") or "untitled_record"


def _unique_path(dest_dir: Path, filename: str) -> Path:
    candidate = dest_dir / filename
    if not candidate.exists():
        return candidate
    stem, suffix = candidate.stem, candidate.suffix
    i = 2
    while True:
        alt = dest_dir / f"{stem}_{i}{suffix}"
        if not alt.exists():
            return alt
        i += 1


# OAI-PMH harvesting ───────────────────────────────────────────────────────
def _oai_request(params: dict) -> ET.Element:
    r = requests.get(OAI_BASE, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return ET.fromstring(r.content)


def _iter_records(metadata_prefix: str = "oai_dc") -> Iterator[ET.Element]:
    params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
    while True:
        root = _oai_request(params)
        for record in root.findall(".//oai:record", NS):
            yield record
        token_el = root.find(".//oai:resumptionToken", NS)
        token = (token_el.text or "").strip() if token_el is not None else ""
        if not token:
            break
        params = {"verb": "ListRecords", "resumptionToken": token}
        time.sleep(PAUSE)


def _text(parent: ET.Element, path: str) -> Optional[str]:
    node = parent.find(path, NS)
    if node is None or not node.text:
        return None
    return node.text.strip() or None


def _texts(parent: ET.Element, path: str) -> list:
    return [n.text.strip() for n in parent.findall(path, NS) if n.text and n.text.strip()]


def _parse_record(record: ET.Element) -> Optional[dict]:
    header = record.find("oai:header", NS)
    if header is not None and header.attrib.get("status") == "deleted":
        return None

    identifier = _text(record, "oai:header/oai:identifier")
    datestamp  = _text(record, "oai:header/oai:datestamp")
    metadata   = record.find("oai:metadata", NS)
    if metadata is None:
        return None

    # Try DataCite schema first, fall back to oai_dc
    resource = metadata.find(".//datacite:resource", NS)
    if resource is not None:
        title       = _text(resource, ".//datacite:titles/datacite:title") or identifier or "Untitled"
        description = "\n".join(_texts(resource, ".//datacite:descriptions/datacite:description")) or "No description available."
        subjects    = _texts(resource, ".//datacite:subjects/datacite:subject")
        creators    = _texts(resource, ".//datacite:creators/datacite:creator/datacite:creatorName")
        language    = _text(resource, ".//datacite:language")
        doi         = _text(resource, ".//datacite:identifier")
        rights      = _texts(resource, ".//datacite:rightsList/datacite:rights")
        urls        = (_texts(resource, ".//datacite:alternateIdentifiers/datacite:alternateIdentifier")
                    or _texts(resource, ".//datacite:relatedIdentifiers/datacite:relatedIdentifier")
                    or _texts(resource, ".//datacite:identifier"))
    else:
        dc = metadata.find(".//oai_dc:dc", NS)
        if dc is None:
            return None
        title       = _text(dc, "dc:title") or identifier or "Untitled"
        description = "\n".join(_texts(dc, "dc:description")) or "No description available."
        subjects    = _texts(dc, "dc:subject")
        creators    = _texts(dc, "dc:creator")
        language    = _text(dc, "dc:language")
        doi         = _text(dc, "dc:identifier")
        rights      = _texts(dc, "dc:rights")
        urls        = _texts(dc, "dc:identifier")

    project_url = next(
        (u for u in urls if u.startswith(("http://", "https://"))),
        identifier or f"oai:{title}"
    )

    return {
        "oai_identifier": identifier,
        "upload_date":    datestamp,
        "title":          title,
        "description":    description,
        "keywords":       subjects,
        "creators":       creators,
        "language":       language,
        "doi":            doi,
        "rights":         rights,
        "project_url":    project_url,
        "identifiers":    urls,
    }


def _query_matches(record_data: dict, query: str) -> bool:
    haystack = "\n".join([
        record_data.get("title") or "",
        record_data.get("description") or "",
        " ".join(record_data.get("keywords") or []),
    ]).lower()
    return query.lower() in haystack


# File discovery & download ────────────────────────────────────────────────
def _links_from_html(base_url: str, html: str) -> set:
    found = set()
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, tag["href"])
            if _ext(abs_url) in ALLOWED_EXT:
                found.add(abs_url)
    else:
        for href in re.findall(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE):
            abs_url = urljoin(base_url, href)
            if _ext(abs_url) in ALLOWED_EXT:
                found.add(abs_url)
    return found


def _discover_links(record_data: dict) -> set:
    candidates = set()

    for ident in record_data.get("identifiers", []):
        if ident.startswith(("http://", "https://")) and _ext(ident) in ALLOWED_EXT:
            candidates.add(ident)

    page_urls = [u for u in record_data.get("identifiers", []) if u.startswith(("http://", "https://"))]
    proj_url  = record_data.get("project_url", "")
    if proj_url.startswith(("http://", "https://")):
        page_urls.insert(0, proj_url)

    seen = set()
    for page_url in page_urls[:MAX_DISC_PAGES]:
        if page_url in seen:
            continue
        seen.add(page_url)
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            if "html" not in r.headers.get("Content-Type", "").lower():
                if _ext(r.url) in ALLOWED_EXT:
                    candidates.add(r.url)
                continue
            candidates.update(_links_from_html(r.url, r.text))
        except requests.RequestException:
            pass
        time.sleep(PAUSE)

    return candidates


def _status_from_exc(exc: Exception) -> str:
    msg = str(exc).lower()
    if "401" in msg or "403" in msg:
        return "FAILED_LOGIN_REQUIRED"
    return "FAILED_SERVER_UNRESPONSIVE"


def _download_file(url: str, dest_path: Path) -> str:
    try:
        with requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True, allow_redirects=True) as r:
            if r.status_code in (401, 403):
                return "FAILED_LOGIN_REQUIRED"
            r.raise_for_status()
            cl = r.headers.get("Content-Length")
            if cl:
                try:
                    if int(cl) > MAX_FILE_BYTES:
                        return "FAILED_TOO_LARGE"
                except ValueError:
                    pass
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_path, "wb") as fh:
                for chunk in r.iter_content(512 * 1024):
                    if chunk:
                        fh.write(chunk)
        return "SUCCEEDED"
    except Exception as exc:
        return _status_from_exc(exc)


# Per-record processing ────────────────────────────────────────────────────
def _process_record(record_data: dict, query: str, out_root: Path) -> bool:
    project_url = record_data["project_url"]
    if project_url_exists(project_url):
        return False

    slug = _slugify(record_data["title"] or record_data.get("oai_identifier") or "record")

    project_id = insert_project({
        "query_string":               query,
        "repository_id":              REPO_ID,
        "repository_url":             REPO_URL,
        "project_url":                project_url,
        "version":                    None,
        "title":                      record_data["title"] or "Untitled",
        "description":                record_data["description"] or "No description available.",
        "language":                   record_data.get("language"),
        "doi":                        record_data.get("doi"),
        "upload_date":                record_data.get("upload_date"),
        "download_date":              _now(),
        "download_repository_folder": REPO_FOLDER,
        "download_project_folder":    slug,
        "download_version_folder":    None,
        "download_method":            "SCRAPING",
    })

    for kw in record_data.get("keywords", []):
        if kw.strip():
            insert_keyword(project_id, kw)

    for creator in record_data.get("creators", []):
        if creator.strip():
            insert_person_role(project_id, creator, "AUTHOR")

    for lic in (record_data.get("rights") or []):
        if lic.strip():
            insert_license(project_id, lic)

    file_links = _discover_links(record_data)

    if not file_links:
        insert_file({
            "project_id": project_id,
            "file_name":  "study_package",
            "file_type":  "unknown",
            "status":     "FAILED_LOGIN_REQUIRED",
        })
        return True

    dest_dir = out_root / slug
    for file_url in sorted(file_links):
        file_name = Path(urlparse(file_url).path).name or "downloaded_file"
        ext       = _ext(file_name).lstrip(".")
        dest_path = _unique_path(dest_dir, file_name)

        if dest_path.exists():
            status = "SUCCEEDED"
        else:
            status = _download_file(file_url, dest_path)
            time.sleep(PAUSE)

        insert_file({
            "project_id": project_id,
            "file_name":  file_name,
            "file_type":  ext or "unknown",
            "status":     status,
        })

    return True


# Public entry point ───────────────────────────────────────────────────────
def run(max_projects: int = 100):
    if BeautifulSoup is None:
        print("  [NOTE] beautifulsoup4 not installed — using regex fallback for HTML parsing.")

    print(f"\n{'='*60}")
    print(f"CESSDA Scraper (OAI-PMH) — target: {max_projects} projects")
    print(f"{'='*60}")

    out_root = Path(DATA_DIR)
    out_root.mkdir(parents=True, exist_ok=True)

    matched_total = 0

    for query in QUERIES:
        if matched_total >= max_projects:
            break
        print(f"\n  Query: '{query}'")
        matched_this_query = 0

        try:
            for record in _iter_records(metadata_prefix="oai_dc"):
                if matched_total >= max_projects:
                    break
                record_data = _parse_record(record)
                if not record_data or not _query_matches(record_data, query):
                    continue
                inserted = _process_record(record_data, query, out_root)
                if inserted:
                    matched_total += 1
                    matched_this_query += 1
                    print(f"  [{matched_total:>4}] {record_data['title'][:70]}")

        except Exception as exc:
            print(f"  [ERROR] OAI-PMH stream interrupted: {exc}")

        print(f"  -> matched {matched_this_query} records for query '{query}'")

    print(f"\nCESSDA done. Projects inserted: {matched_total}")
