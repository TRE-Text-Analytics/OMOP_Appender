# OMOP-Appender

OMOP-Appender is a streamlined web application built with FastAPI. It is designed to upsert data from a source OMOP Common Data Model (CDM) to another OMOP CDM, providing an intuitive interface to test and merge databases.

## Key Features

- FastAPI Powered: High-performance, asynchronous backend for handling large-scale data operations.

- CDM Compatibility: Primary support for CDM v5.4, with proven backwards compatibility for migrating/appending data from v5.3.

- Docker Ready: Fully containerized for rapid deployment across any environment.

## Supported CDM Versions

While the suite is optimized for CDM v5.4, it has been successfully tested for appending data from v5.3 into v5.4 environments.

> [!NOTE]
> When migrating from v5.3 to v5.4, the application automatically handles structural differences by populating new v5.4-specific columns with NULL values.

## Installation & Setup

### Local Development

This project utilizes uv for lightning-fast Python dependency management.

1. Navigate to the backend directory:

    ```Bash
    cd <path_to_project>/backend
    ```

2. Sync dependencies:

    ```Bash
    uv sync
    ```

3. Start the development server:

    ```Bash
    uvicorn main:app --port 8080 --reload
    ```

### Running with Docker

For a production-ready environment or quick testing, use Docker Compose:

```Bash
docker-compose up -d
```

## Duplicate Prevention
Each OMOP CDM table has configured columns which are used by OMOP-Appender to detect duplicate rows before insertion. These columns are listed at the beginning of the backend Python file (main.py), such as the condition_occurrence table which uses the following columns for duplicate prevention: **person_id**, **condition_concept_id** and **condition_start_date**. Meaning a person cannot have the same **condition_concept_id** on the same **condition_start_date** twice. 
