
Seeding QDArchive
Data Acquisition


Project Report
Friedrich Alexander Universität Erlangen-Nürnberg

Md Rearuzzaman Hridoy

ID: 23047806

Supervisor: Prof. Dirk Riehle
 
1. Introduction
QDArchive is a web service developed that allows researchers to publish, archive, and later retrieve qualitative research data. It puts a particular emphasis on Qualitative Data Analysis files the structured, annotated files researchers produce when they interpret primary data such as interview transcripts or fieldwork recordings. Because QDArchive is still in active development and essentially a prototype, it faces a classic cold-start problem: without existing data it is hard to attract researchers, and without researchers it is hard to accumulate data.

Part 1 of this project Data Acquisition addresses that problem directly. The goal was to build an automated download pipeline that could harvest as much openly licensed qualitative research data as possible from established repositories, along with all associated metadata, and store everything in a well-structured SQLite database. This would serve as the initial seed for QDArchive and provide the foundation.

The two repositories assigned for this part were the Qualitative Data Repository and the CESSDA Data Catalogue, a pan-European infrastructure aggregating metadata from national social science data archives. Both are authoritative sources of qualitative research data and together give a useful breadth of coverage, one being a direct data host with its own API.

2. Tools and Working Process
Tools: I have to use some tools for the purpose of the work.
a.	Programming language Python
b.	Some external libraries: 
-	requests: HTTP requests, API calls, and file streaming downloads
-	beautifulsoup: HTML parsing for file link discovery on publisher pages
-	pathlib: Cross-platform path handling for file and directory operations
-	argparse: Command-line argument parsing for main.py
-	xml.etree.ElementTree: OAI-PMH XML parsing for CESSDA records.
c.	SQLite as Database
d.	Two different APIs for searching dataset metadata and downloading.
e.	Target file extensions.
f.	Keywords for file search [interview, qualitative data, interview data, qualitative interview data]
Process: 
a.	Databases are organized according to the requirements in the excel schema file.
b.	The pipeline was structured as a Python project with a clear separation of concerns. A central database module [db/database.py] handles all SQLite interactions and exposes simple insert and lookup functions.
c.	Two scraper modules [scrapers/qdr_scraper.py and scrapers/cessda_scraper.py] contain all repository specific logic.
d.	Supporting utilities handle statistics export [stats.py].
e.	Using a pipeline QDR which is built on the Harvard Dataverse platform, which exposes a well-documented REST API.
f.	OAI-PMH [Open Archives Initiative Protocol for Metadata Harvesting] endpoint for the repository CESSDA.

3. Technical Challenges
There are some big technical challenges for this metadata search. Most of the time keywords and file extensions did not find anything. Besides pipeline also need to manual search. Other limitations are:
a.	CESSDA is one of the repositories which is a metadata catalogue. It does not host the data. The actual data files live on each of those national archives, not on CESSDA itself. Most of the time it needs to be login requirements and also there are license issues.
b.	The OAI-PMH protocol was designed for bulk harvesting, not targeted search. There is no way to ask a CESSDA OAI-PMH endpoint to return only records matching the keywords.
c.	For QDR repositories there are also login and license issues for most of the files.
d.	For both repositories maximus files have been found which are .pdf and .docx.
For the Part 1 of the Seeding QDArchive project produced a working, documented, and ready-to-run data acquisition pipeline targeting two qualitative research repositories: QDR at Syracuse University and the CESSDA Data Catalogue. The pipeline collects metadata and files, stores everything in a schema-compliant SQLite database, and exports results to CSV.
