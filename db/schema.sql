
CREATE TABLE IF NOT EXISTS PROJECTS (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string              TEXT,
    repository_id             INTEGER NOT NULL,
    repository_url            TEXT NOT NULL,
    project_url               TEXT NOT NULL,
    version                   TEXT,
    title                     TEXT NOT NULL,
    description               TEXT NOT NULL,
    language                  TEXT,
    doi                       TEXT,
    upload_date               DATE,
    download_date             TIMESTAMP NOT NULL,
    download_repository_folder TEXT NOT NULL,
    download_project_folder   TEXT NOT NULL,
    download_version_folder   TEXT,
    download_method           TEXT NOT NULL CHECK(download_method IN ('SCRAPING','API-CALL'))
);

CREATE TABLE IF NOT EXISTS FILES (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
    file_name   TEXT NOT NULL,
    file_type   TEXT NOT NULL,
    status      TEXT NOT NULL CHECK(status IN (
                    'SUCCEEDED',
                    'FAILED_SERVER_UNRESPONSIVE',
                    'FAILED_LOGIN_REQUIRED',
                    'FAILED_TOO_LARGE'
                ))
);

CREATE TABLE IF NOT EXISTS KEYWORDS (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
    keyword     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS PERSON_ROLE (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
    name        TEXT NOT NULL,
    role        TEXT NOT NULL CHECK(role IN ('UPLOADER','AUTHOR','OWNER','OTHER','UNKNOWN'))
);

CREATE TABLE IF NOT EXISTS LICENSES (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id  INTEGER NOT NULL REFERENCES PROJECTS(id),
    license     TEXT NOT NULL
);
