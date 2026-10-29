# OMOP-Appender

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
[![Container image](https://img.shields.io/badge/image-ghcr.io-2496ED?logo=docker&logoColor=white)](https://github.com/TRE-Text-Analytics/OMOP_Appender/pkgs/container/omop_appender)

**Append patient data from one OMOP CDM database into another, without breaking links or creating duplicates.**

OMOP-Appender is a web application for combining two [OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/) databases. Point it at a source and a target, choose the tables, preview exactly what will change, and then append. It matches patients across the two databases, gives every new row a fresh ID in the target, keeps every link between records pointing at the right patient, visit and note, and skips rows the target already has.

<!-- TODO: add a screenshot of the Scan & preview step, e.g. docs/screenshot.png -->

---

## Contents

- [Features](#features)
- [Quick start](#quick-start)
- [Using the app](#using-the-app)
- [How it works](#how-it-works)
- [Configuring duplicate detection](#configuring-duplicate-detection)
- [Supported databases and CDM versions](#supported-databases-and-cdm-versions)
- [Good to know](#good-to-know)
- [Deployment](#deployment)
- [Development](#development)
- [License](#license)

---

## Features

- **Patient matching across databases**: patients are matched by `person_source_value`, so the same person isn't created twice.
- **Safe ID handling**: new rows get the next free ID in the target by default (or keep their source IDs, or use a fixed offset), and every foreign key is re-pointed to match. This includes links within a table, such as preceding visits and parent visit details.
- **Duplicate prevention**: rows already in the target are skipped. What counts as "the same row" is set per table in a readable config file.
- **Preview before you write**: a scan shows how many new rows each table and patient would receive, and a dry run goes through the whole merge without writing anything.
- **All or nothing**: a live merge runs in a single database transaction. If anything fails, the target is left exactly as it was, and the app shows the table, column and row responsible.
- **Sampling**: limit a run to the first *N* patients to try things out quickly on large databases.
- **Audit exports**: download the source-to-target ID mapping and a per-patient audit (matched, newly created or skipped) as CSV after each run.
- **Large datasets**: data is streamed from the source in batches, and duplicate checks use a compact in-memory format, so tables with millions of rows fit in a few GB of memory.
- **Easy to deploy**: a ready-made container image, plus Docker Compose, a Helm chart and plain Kubernetes files.

## Quick start

With [Docker](https://docs.docker.com/get-docker/) installed:

```bash
docker run --rm -p 8000:8000 \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/tre-text-analytics/omop_appender:latest
```

Open **http://localhost:8000**. If your databases run on the same machine, enter `host.docker.internal` as the host instead of `localhost`.

For Docker Compose, Kubernetes and Helm, see the **[deployment guide](DEPLOY.md)**.

> [!WARNING]
> The app has no login. Anyone who can reach it can connect it to any database it can reach. Run it on a private network and never expose it directly to the internet.

## Using the app

The interface walks you through five steps:

1. **Connections**: enter the source and target PostgreSQL details (host, port, database, schema, username, password) and test each connection.
2. **Tables**: choose which OMOP tables to append. Tables are always processed in dependency order (care sites and providers, then people, visits, clinical tables, and derived tables last), whatever order you select them in.
3. **Conflict rules**: decide how patients and IDs are handled:
   - **Patient scope**: append data for existing *and* new patients, or for patients already in the target only.
   - **Existing patients**: leave their target `person` row as it is (default), overwrite it with the source's demographics, or abort the merge if any patient already exists.
   - **IDs for new rows**: auto-assign the next free ID in the target (default, safest), keep source IDs, or add a fixed offset.
   - **Duplicate prevention**: on by default.
   - **Sampling**: optionally limit the run to the first *N* patients.
4. **Scan & preview**: a read-only comparison showing how many new rows each table would receive and which patients have new data.
5. **Run**: perform a dry run (the default) or a live merge, follow its progress, and export the ID mapping and patient audit.

> [!TIP]
> Back up your target database and run a dry run before any live merge.

## How it works

1. **Match patients.** Source patients are matched to target patients by `person_source_value`. Depending on your settings, unmatched patients are either created in the target or left out.
2. **Process tables in dependency order**, so a referenced row always exists in the target before anything links to it.
3. **For each source row:**
   - re-point `person_id` to the matched target patient
   - re-point other foreign keys (`visit_occurrence_id`, `visit_detail_id`, `provider_id`, `care_site_id`, `note_id`) to the target rows created earlier in the run. A reference that can't be resolved is set to `NULL` rather than left pointing at an unrelated row.
   - skip the row if the target already holds a matching row (see below)
   - assign the row's new ID and record the mapping
4. **Resolve links within a table**, such as `preceding_visit_occurrence_id` and `parent_visit_detail_id`, in a second pass once all of that table's rows exist, so the order rows arrive in doesn't matter.
5. **Commit** everything in one transaction, or roll it all back on error or in a dry run.

`note_nlp` rows have no `person_id`; they follow their parent note. They're only appended if their note was appended or matched in the same run.

## Configuring duplicate detection

OMOP doesn't define what makes two clinical records "the same", so each table's duplicate rule is a list of columns (`dedup_cols`) in [`backend/features/omop_appender/omop_appender_config.yaml`](backend/features/omop_appender/omop_appender_config.yaml). A source row is skipped when the target already has a row with identical values in all of those columns (for patient-level tables, for the same patient). For example, `measurement` uses the patient, concept, date and time, value and unit.

The file explains every field and the reasoning behind each table's columns. Too few columns can drop genuine repeat events (two readings on the same day); too many can let duplicates through.

To use your own rules without changing the code, copy the file, edit it, and point the app at it with the `OMOP_APPENDER_CONFIG` environment variable:

```bash
docker run --rm -p 8000:8000 \
  -v "$PWD/my_config.yaml:/config/omop_appender_config.yaml:ro" \
  -e OMOP_APPENDER_CONFIG=/config/omop_appender_config.yaml \
  ghcr.io/tre-text-analytics/omop_appender:latest
```

The app checks the file when it starts and refuses to start if it's invalid, naming the table and field at fault.

## Supported databases and CDM versions

- **Databases**: PostgreSQL, for both source and target.
- **CDM versions**: built for OMOP CDM **v5.4**. Appending from a **v5.3** source into a v5.4 target is also tested: columns that only exist in v5.4 are left at their default (usually `NULL`), and source columns the target doesn't have are dropped, with a warning in the run log.
- **Tables**: `care_site`, `provider`, `person`, `death`, `visit_occurrence`, `visit_detail`, `condition_occurrence`, `drug_exposure`, `measurement`, `observation`, `procedure_occurrence`, `device_exposure`, `specimen`, `note`, `note_nlp`, `observation_period`, `condition_era`, `drug_era`, `dose_era`.

## Good to know

- **Vocabularies aren't translated.** Concept IDs are copied as they are, so the source and target should use the same OHDSI vocabulary version.
- **Duplicates are checked against the target only**, not within the source. If the source contains the same record twice, both copies are appended. Remove duplicates in your ETL beforehand if needed.
- **Derived tables** (`observation_period` and the `*_era` tables) can be appended, but regenerating them on the target after a merge is usually cleaner.
- **Matching depends on `person_source_value`.** Source patients without one can't be matched to existing target patients; they're treated as new patients (or skipped in existing-patients-only mode).
- **Timestamp types must agree.** If a source column is `timestamp with time zone` and the matching target column is `timestamp`, the live merge for that table fails and the whole run is rolled back.
- **Memory grows with the size of the target**, not with how much you append. See the [sizing guide](DEPLOY.md#sizing).

## Deployment

The **[deployment guide](DEPLOY.md)** covers Docker Compose, Kubernetes with Helm, and plain Kubernetes files, along with database connectivity, sizing and troubleshooting.

Images are published to the GitHub Container Registry for `linux/amd64` and `linux/arm64` on every release:

```
ghcr.io/tre-text-analytics/omop_appender:<version>
```

## Development

Prerequisites: Python 3.11+, [uv](https://docs.astral.sh/uv/), and Node.js 18+ with npm.

```bash
git clone https://github.com/TRE-Text-Analytics/OMOP_Appender.git
cd OMOP_Appender

# Backend (FastAPI) on http://localhost:8000
cd backend
uv sync
uv run uvicorn main:app --port 8000 --reload

# Frontend (React + Vite) on http://localhost:5173, in a second terminal.
# API calls are proxied to the backend on port 8000.
cd frontend
npm install
npm run dev
```

`npm run build` compiles the frontend into `backend/frontend/`, which the backend serves at `/`; this is how the container image is built. `./launch.sh` does the build and starts the backend in one go.

To build the container locally: `docker compose up --build`, after changing `image:` to `build: .` in `docker-compose.yml`.

**Tests**: the duplicate-check key encoding is tested against Python's own equality rules on millions of value combinations:

```bash
cd backend && uv run python tests/test_dedup_key.py
```

### Project layout

```
backend/
  main.py                       FastAPI app; serves the API and the built frontend
  features/omop_appender/
    logic.py                    scan and merge engine
    dedup_key.py                compact, exact duplicate-check keys
    omop_appender_config.yaml   per-table metadata and duplicate rules
    router.py, models.py        API routes and request models
  tests/
frontend/                       React + Vite user interface
helm/omop-appender/             Helm chart
k8s/                            plain Kubernetes manifests
Dockerfile, docker-compose.yml
```

## Contributing

Issues and pull requests are welcome. For a bug, please include the CDM version, which tables were selected, and the relevant lines from the run log. Never include patient data or credentials.

## License

Released under the [GNU General Public License v3.0](LICENSE).

OMOP and the OMOP Common Data Model are maintained by the [OHDSI](https://www.ohdsi.org/) community. This project is not affiliated with or endorsed by OHDSI.