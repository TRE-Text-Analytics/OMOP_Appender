"""
OMOP Patient Data Merge Tool - FastAPI Backend v1.2
Appends patient data from a source PostgreSQL OMOP database to a target.
Patients are discovered automatically by diffing the two databases.

Changes in v1.2
---------------
* Fixed build_person_map: was using person_id as the source-value key instead
  of person_source_value, breaking cross-DB identity matching entirely.
* Added "target_found" key to person_map so diff_scan no longer throws KeyError.
* Added provider / care_site to OMOP_TABLES (migrated before person).
* Replaced the single visit_id_map with a generalised IDRemapper that handles
  visit_occurrence_id, visit_detail_id, provider_id, care_site_id, and
  parent_visit_detail_id across all tables.
* Each table now declares fk_remaps: {column_name: map_name} so every FK is
  remapped at insert time rather than only visit_occurrence_id.
* When a remapped FK points at a source ID that was never migrated (e.g. a
  provider that was skipped) the FK is set to NULL rather than pointing at a
  stale / foreign ID.
"""

import json
from datetime import datetime
from typing import AsyncGenerator

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------
# fk_remaps: dict mapping column_name -> id_map name.
#   Columns listed here will have their value looked up in the corresponding
#   IDRemapper map and replaced with the target ID before insert.
#   If the source ID is not found in the map the column is set to NULL.
#
# insert_order determines processing sequence — lower numbers run first so
# that referenced tables are populated before referencing ones.
#   1  care_site   (referenced by provider, person, visit_occurrence)
#   2  provider    (referenced by person, visit_occurrence, clinical tables)
#   3  person
#   4  death
#   5  visit_occurrence
#   6  visit_detail
#   7  clinical tables
#   8  derived tables

OMOP_TABLES = {
    # ---- Reference / administrative (no person FK) -------------------------
    "care_site": {
        "label": "care_site", "domain": "admin",
        "description": "Care sites / facilities",
        "person_fk": None, "self_pk": "care_site_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["care_site_source_value"], "insert_order": 1,
    },
    "provider": {
        "label": "provider", "domain": "admin",
        "description": "Clinicians and providers",
        "person_fk": None, "self_pk": "provider_id", "visit_fk": None,
        "fk_remaps": {
            "care_site_id": "care_site",
        },
        "dedup_cols": ["provider_source_value"], "insert_order": 2,
    },
    # ---- Core --------------------------------------------------------------
    "person": {
        "label": "person", "domain": "core",
        "description": "Demographics, DOB, gender, race",
        "person_fk": "person_id", "self_pk": "person_id", "visit_fk": None,
        "fk_remaps": {
            "provider_id":  "provider",
            "care_site_id": "care_site",
        },
        "dedup_cols": ["person_source_value"], "insert_order": 3,
    },
    "death": {
        "label": "death", "domain": "core",
        "description": "Cause of death records",
        "person_fk": "person_id", "self_pk": None, "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id"], "insert_order": 4,
    },
    # ---- Visit -------------------------------------------------------------
    "visit_occurrence": {
        "label": "visit_occurrence", "domain": "visit",
        "description": "Inpatient / outpatient visits",
        "person_fk": "person_id", "self_pk": "visit_occurrence_id", "visit_fk": None,
        "fk_remaps": {
            "provider_id":  "provider",
            "care_site_id": "care_site",
        },
        "dedup_cols": ["person_id", "visit_start_date", "visit_concept_id"], "insert_order": 5,
    },
    "visit_detail": {
        "label": "visit_detail", "domain": "visit",
        "description": "Sub-visit encounter detail",
        "person_fk": "person_id", "self_pk": "visit_detail_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id":      "visit_occurrence",
            "parent_visit_detail_id":   "visit_detail",   # self-referencing
            "provider_id":              "provider",
            "care_site_id":             "care_site",
        },
        "dedup_cols": ["person_id", "visit_detail_start_date", "visit_detail_concept_id"], "insert_order": 6,
    },
    # ---- Clinical ----------------------------------------------------------
    "condition_occurrence": {
        "label": "condition_occurrence", "domain": "clinical",
        "description": "Diagnoses and conditions",
        "person_fk": "person_id", "self_pk": "condition_occurrence_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "condition_concept_id", "condition_start_date"], "insert_order": 7,
    },
    "drug_exposure": {
        "label": "drug_exposure", "domain": "clinical",
        "description": "Medications and prescriptions",
        "person_fk": "person_id", "self_pk": "drug_exposure_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "drug_concept_id", "drug_exposure_start_date"], "insert_order": 7,
    },
    "measurement": {
        "label": "measurement", "domain": "clinical",
        "description": "Labs, vitals, test results",
        "person_fk": "person_id", "self_pk": "measurement_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "measurement_concept_id", "measurement_date"], "insert_order": 7,
    },
    "observation": {
        "label": "observation", "domain": "clinical",
        "description": "Clinical observations",
        "person_fk": "person_id", "self_pk": "observation_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "observation_concept_id", "observation_date"], "insert_order": 7,
    },
    "procedure_occurrence": {
        "label": "procedure_occurrence", "domain": "clinical",
        "description": "Surgeries and procedures",
        "person_fk": "person_id", "self_pk": "procedure_occurrence_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "procedure_concept_id", "procedure_date"], "insert_order": 7,
    },
    "device_exposure": {
        "label": "device_exposure", "domain": "clinical",
        "description": "Medical devices",
        "person_fk": "person_id", "self_pk": "device_exposure_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "device_concept_id", "device_exposure_start_date"], "insert_order": 7,
    },
    "specimen": {
        "label": "specimen", "domain": "clinical",
        "description": "Biological specimens",
        "person_fk": "person_id", "self_pk": "specimen_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "specimen_concept_id", "specimen_date"], "insert_order": 7,
    },
    "note": {
        "label": "note", "domain": "clinical",
        "description": "Free-text clinical notes",
        "person_fk": "person_id", "self_pk": "note_id", "visit_fk": "visit_occurrence_id",
        "fk_remaps": {
            "visit_occurrence_id": "visit_occurrence",
            "visit_detail_id":     "visit_detail",
            "provider_id":         "provider",
        },
        "dedup_cols": ["person_id", "note_date", "note_type_concept_id"], "insert_order": 7,
    },
    # ---- Derived -----------------------------------------------------------
    "observation_period": {
        "label": "observation_period", "domain": "derived",
        "description": "Observation period windows",
        "person_fk": "person_id", "self_pk": "observation_period_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "observation_period_start_date", "observation_period_end_date"], "insert_order": 8,
    },
    "condition_era": {
        "label": "condition_era", "domain": "derived",
        "description": "Derived condition eras",
        "person_fk": "person_id", "self_pk": "condition_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "condition_concept_id", "condition_era_start_date"], "insert_order": 8,
    },
    "drug_era": {
        "label": "drug_era", "domain": "derived",
        "description": "Derived drug eras",
        "person_fk": "person_id", "self_pk": "drug_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "drug_concept_id", "drug_era_start_date"], "insert_order": 8,
    },
    "dose_era": {
        "label": "dose_era", "domain": "derived",
        "description": "Derived dose eras",
        "person_fk": "person_id", "self_pk": "dose_era_id", "visit_fk": None,
        "fk_remaps": {},
        "dedup_cols": ["person_id", "drug_concept_id", "dose_era_start_date"], "insert_order": 8,
    },
}

# Tables that have no person_fk (admin/reference tables).
ADMIN_TABLES = {name for name, m in OMOP_TABLES.items() if m["person_fk"] is None}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DBConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    schema_name: str = "cdm"
    username: str
    password: str

class MergeConfig(BaseModel):
    source: DBConfig
    target: DBConfig
    tables: list[str]
    person_conflict: str = "skip"
    dedup_enabled: bool = True
    id_strategy: str = "auto"
    id_offset: int = 0
    dry_run: bool = True

class ConnectionTestRequest(BaseModel):
    config: DBConfig

class ScanRequest(BaseModel):
    source: DBConfig
    target: DBConfig
    tables: list[str]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def open_conn(cfg: DBConfig) -> asyncpg.Connection:
    try:
        return await asyncpg.connect(
            host=cfg.host, port=cfg.port, database=cfg.database,
            user=cfg.username, password=cfg.password, timeout=10,
        )
    except Exception as e:
        raise HTTPException(400, detail=f"Connection failed: {e}")


async def table_exists(conn, schema, table):
    return await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=$1 AND table_name=$2)",
        schema, table,
    )


async def get_columns(conn, schema, table):
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position",
        schema, table,
    )
    return [r["column_name"] for r in rows]


# ---------------------------------------------------------------------------
# PK allocator
# ---------------------------------------------------------------------------

class PKCounter:
    """
    Per-table in-memory PK allocator.

    Initialised once per merge run by querying MAX(pk) from the target.
    Each call to next() increments the counter, so repeated calls during
    a dry run still produce unique, ascending IDs.
    """
    def __init__(self):
        self._counters: dict[str, int] = {}

    async def init_table(self, conn, schema: str, table: str, pk_col: str):
        if table not in self._counters:
            val = await conn.fetchval(
                f'SELECT COALESCE(MAX("{pk_col}"), 0) FROM "{schema}"."{table}"'
            )
            self._counters[table] = int(val)

    def next(self, table: str) -> int:
        self._counters[table] += 1
        return self._counters[table]


# ---------------------------------------------------------------------------
# Generalised ID remapper
# ---------------------------------------------------------------------------

class IDRemapper:
    """
    Maintains one src→tgt mapping dict per named domain
    (visit_occurrence, visit_detail, provider, care_site, …).

    Usage
    -----
    remapper = IDRemapper()

    # After inserting a visit_occurrence row:
    remapper.record("visit_occurrence", src_id=42, tgt_id=9001)

    # Before inserting a condition_occurrence row:
    row_data["visit_occurrence_id"] = remapper.remap(
        "visit_occurrence", row_data.get("visit_occurrence_id")
    )
    # Returns None if src_id is unknown, so the FK becomes NULL rather than
    # pointing at a stale / wrong record in the target.
    """

    def __init__(self):
        self._maps: dict[str, dict[int, int]] = {}

    def record(self, map_name: str, src_id: int, tgt_id: int):
        self._maps.setdefault(map_name, {})[src_id] = tgt_id

    def remap(self, map_name: str, src_id) -> int | None:
        if src_id is None:
            return None
        return self._maps.get(map_name, {}).get(int(src_id))

    def apply_row(self, row_data: dict, fk_remaps: dict[str, str]) -> dict:
        """
        Apply all FK remaps declared for a table to a mutable row_data dict.
        Returns the same dict (mutated in place) for convenience.
        """
        for col, map_name in fk_remaps.items():
            if col in row_data and row_data[col] is not None:
                row_data[col] = self.remap(map_name, row_data[col])
        return row_data


# ---------------------------------------------------------------------------
# SSE / NDJSON helper
# ---------------------------------------------------------------------------

def emit(type_: str, **kwargs) -> str:
    return json.dumps({"type": type_, "ts": datetime.now().isoformat(), **kwargs}) + "\n"


# ---------------------------------------------------------------------------
# Person identity map
# ---------------------------------------------------------------------------

async def build_person_map(src, tgt, ss, ts):
    """
    Returns person_map: source_person_id -> {
        source_value: str | None,
        target_person_id: int | None,   # None = not yet in target
        target_found: bool,
    }

    Patients are matched across DBs by person_source_value.

    FIX (v1.2): previously `sv = r["person_id"]` caused every patient to be
    keyed on their numeric ID rather than their source value, so cross-DB
    matching always failed silently.
    """
    src_persons = await src.fetch(
        f'SELECT person_id, person_source_value FROM "{ss}"."person"'
    )
    tgt_persons = await tgt.fetch(
        f'SELECT person_id, person_source_value FROM "{ts}"."person"'
    )

    tgt_by_sv = {
        r["person_source_value"]: r["person_id"]
        for r in tgt_persons
        if r["person_source_value"]
    }

    person_map = {}
    for r in src_persons:
        sv = r["person_source_value"]   # FIX: was r["person_id"]
        tgt_pid = tgt_by_sv.get(sv) if sv else None
        person_map[r["person_id"]] = {
            "source_value":     sv,
            "target_person_id": tgt_pid,
            "target_found":     tgt_pid is not None,   # FIX: key was missing
        }
    return person_map


# ---------------------------------------------------------------------------
# Dedup helper
# ---------------------------------------------------------------------------

async def get_tgt_dedup_keys(tgt, ts, table_name, pfk, dedup_cols, tgt_cols, tgt_person_ids):
    """Bulk-fetch existing target dedup keys for a table."""
    non_pk_dedup = [c for c in dedup_cols if c != pfk and c in tgt_cols]
    if not non_pk_dedup or not tgt_person_ids:
        return set(), non_pk_dedup
    col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
    rows = await tgt.fetch(
        f'SELECT "{pfk}", {col_list} FROM "{ts}"."{table_name}" '
        f'WHERE "{pfk}" = ANY($1::int[])',
        tgt_person_ids,
    )
    keys = set()
    for r in rows:
        keys.add(tuple([r[pfk]] + [r[c] for c in non_pk_dedup]))
    return keys, non_pk_dedup


# ---------------------------------------------------------------------------
# Scan endpoint
# ---------------------------------------------------------------------------

async def diff_scan(req: ScanRequest) -> AsyncGenerator[str, None]:
    yield emit("log", level="info", msg="Connecting to databases…")
    try:
        src = await open_conn(req.source)
        tgt = await open_conn(req.target)
    except HTTPException as e:
        yield emit("error", msg=e.detail)
        return

    ss, ts = req.source.schema_name, req.target.schema_name
    yield emit("log", level="ok", msg="Both databases connected")

    try:
        yield emit("log", level="info",
                   msg="Building person identity map via person_source_value…")
        person_map = await build_person_map(src, tgt, ss, ts)

        # FIX: use "target_found" key (was KeyError in original)
        new_patients = sum(1 for p in person_map.values() if not p["target_found"])
        total_src = len(person_map)
        yield emit("log", level="ok",
                   msg=f"{total_src} patients in source — "
                       f"{new_patients} new, {total_src - new_patients} already in target")

        tables_to_scan = sorted(
            [t for t in req.tables if t in OMOP_TABLES],
            key=lambda t: OMOP_TABLES[t]["insert_order"],
        )

        patient_summary: dict[int, dict[str, int]] = {}
        table_totals: dict[str, dict] = {}

        for table_name in tables_to_scan:
            meta = OMOP_TABLES[table_name]
            pfk = meta["person_fk"]
            dedup_cols = meta["dedup_cols"]

            yield emit("log", level="info", msg=f"Scanning {table_name}…")

            if not await table_exists(src, ss, table_name):
                yield emit("log", level="warn",
                           msg=f"{table_name}: not found in source, skipping")
                table_totals[table_name] = {
                    "new_rows": 0, "affected_patients": 0, "missing": True,
                }
                continue
            if not await table_exists(tgt, ts, table_name):
                yield emit("log", level="warn",
                           msg=f"{table_name}: not found in target, skipping")
                table_totals[table_name] = {
                    "new_rows": 0, "affected_patients": 0, "missing": True,
                }
                continue

            # Admin tables (provider, care_site) have no person FK — count
            # all rows that don't already exist in the target.
            if table_name in ADMIN_TABLES:
                src_rows = await src.fetch(f'SELECT * FROM "{ss}"."{table_name}"')
                tgt_cols = await get_columns(tgt, ts, table_name)
                non_pk_dedup = [c for c in dedup_cols if c in tgt_cols]
                tgt_keys = set()
                if non_pk_dedup:
                    col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
                    tgt_rows = await tgt.fetch(
                        f'SELECT {col_list} FROM "{ts}"."{table_name}"'
                    )
                    tgt_keys = {
                        tuple(r[c] for c in non_pk_dedup) for r in tgt_rows
                    }
                new_rows_count = sum(
                    1 for row in src_rows
                    if tuple(row[c] for c in non_pk_dedup if c in dict(row)) not in tgt_keys
                )
                table_totals[table_name] = {
                    "new_rows": new_rows_count, "affected_patients": 0,
                }
                yield emit("table_scan", table=table_name,
                           new_rows=new_rows_count, affected_patients=0)
                continue

            if table_name == "person":
                affected = new_patients
                table_totals["person"] = {
                    "new_rows": new_patients, "affected_patients": affected,
                }
                for src_pid, info in person_map.items():
                    if not info["target_found"]:
                        patient_summary.setdefault(src_pid, {})["person"] = 1
                yield emit("table_scan", table="person",
                           new_rows=new_patients, affected_patients=affected)
                continue

            tgt_cols = await get_columns(tgt, ts, table_name)
            tgt_person_ids = [
                p["target_person_id"]
                for p in person_map.values()
                if p["target_found"]
            ]
            tgt_keys, non_pk_dedup = await get_tgt_dedup_keys(
                tgt, ts, table_name, pfk, dedup_cols, tgt_cols, tgt_person_ids
            )

            src_rows = await src.fetch(f'SELECT * FROM "{ss}"."{table_name}"')

            new_rows_count = 0
            affected_patients: set[int] = set()

            for row in src_rows:
                src_pid = row[pfk]
                if src_pid not in person_map:
                    continue
                tgt_pid = (
                    person_map[src_pid]["target_person_id"]
                    if person_map[src_pid]["target_found"]
                    else None
                )

                if tgt_pid is not None and non_pk_dedup:
                    key = tuple(
                        [tgt_pid] + [row[c] for c in non_pk_dedup if c in dict(row)]
                    )
                    if key in tgt_keys:
                        continue

                new_rows_count += 1
                affected_patients.add(src_pid)
                patient_summary.setdefault(src_pid, {})[table_name] = (
                    patient_summary.get(src_pid, {}).get(table_name, 0) + 1
                )

            table_totals[table_name] = {
                "new_rows": new_rows_count,
                "affected_patients": len(affected_patients),
            }
            yield emit("table_scan", table=table_name,
                       new_rows=new_rows_count,
                       affected_patients=len(affected_patients))

        # Build patient list with new data
        patients_with_new_data = []
        for src_pid, table_counts in patient_summary.items():
            info = person_map[src_pid]
            patients_with_new_data.append({
                "source_person_id": src_pid,
                "source_value":     info["source_value"],
                "target_person_id": info["target_person_id"],
                "is_new_patient":   not info["target_found"],
                "new_rows_by_table": table_counts,
                "total_new_rows":   sum(table_counts.values()),
            })
        patients_with_new_data.sort(key=lambda x: -x["total_new_rows"])

        total_new_rows = sum(t["new_rows"] for t in table_totals.values())
        yield emit("scan_complete",
                   total_patients_with_new_data=len(patients_with_new_data),
                   total_new_rows=total_new_rows,
                   table_totals=table_totals,
                   patients=patients_with_new_data)
        yield emit("log", level="ok",
                   msg=f"Scan complete — {len(patients_with_new_data)} patients "
                       f"have new data, {total_new_rows} total rows to import")

    except Exception as e:
        print(f"ERROR {e}")
        yield emit("error", msg=f"Scan error: {e}")
    finally:
        await src.close()
        await tgt.close()


# ---------------------------------------------------------------------------
# Merge endpoint
# ---------------------------------------------------------------------------

async def run_merge(cfg: MergeConfig) -> AsyncGenerator[str, None]:
    yield emit("log", level="info",
               msg=f"Starting {'dry run' if cfg.dry_run else 'live merge'}…")

    try:
        src = await open_conn(cfg.source)
        tgt = await open_conn(cfg.target)
    except HTTPException as e:
        yield emit("error", msg=e.detail)
        return

    ss, ts = cfg.source.schema_name, cfg.target.schema_name

    yield emit("log", level="info", msg="Building person identity map…")
    person_map = await build_person_map(src, tgt, ss, ts)

    tables_to_run = sorted(
        [t for t in cfg.tables if t in OMOP_TABLES],
        key=lambda t: OMOP_TABLES[t]["insert_order"],
    )

    total_inserted = total_skipped = total_conflicts = 0
    mapping_log: list[dict] = []
    pk_counter = PKCounter()
    remapper = IDRemapper()

    try:
        async with tgt.transaction():
            for step_idx, table_name in enumerate(tables_to_run):
                meta = OMOP_TABLES[table_name]
                pfk        = meta["person_fk"]
                self_pk    = meta["self_pk"]
                dedup_cols = meta["dedup_cols"]
                fk_remaps  = meta["fk_remaps"]

                yield emit("progress",
                           step=step_idx + 1,
                           total=len(tables_to_run),
                           table=table_name)

                if not await table_exists(src, ss, table_name):
                    yield emit("log", level="warn",
                               msg=f"{table_name}: not in source, skipping")
                    continue
                if not await table_exists(tgt, ts, table_name):
                    yield emit("log", level="warn",
                               msg=f"{table_name}: not in target, skipping")
                    continue

                src_cols    = await get_columns(src, ss, table_name)
                tgt_cols    = await get_columns(tgt, ts, table_name)
                common_cols = [c for c in src_cols if c in tgt_cols]

                # ---- ADMIN TABLES (no person FK: care_site, provider) ------
                if table_name in ADMIN_TABLES:
                    all_src_rows = await src.fetch(
                        f'SELECT * FROM "{ss}"."{table_name}"'
                    )

                    # Build dedup key set from target
                    non_pk_dedup = [
                        c for c in dedup_cols
                        if c != self_pk and c in tgt_cols
                    ]
                    tgt_admin_keys: set = set()
                    if non_pk_dedup:
                        col_list_d = ", ".join(f'"{c}"' for c in non_pk_dedup)
                        tgt_admin_rows = await tgt.fetch(
                            f'SELECT {col_list_d} FROM "{ts}"."{table_name}"'
                        )
                        tgt_admin_keys = {
                            tuple(r[c] for c in non_pk_dedup)
                            for r in tgt_admin_rows
                        }

                    inserted = skipped = 0
                    for row in all_src_rows:
                        row_data = dict(row)

                        # Dedup check
                        if non_pk_dedup:
                            key = tuple(row_data.get(c) for c in non_pk_dedup)
                            if key in tgt_admin_keys:
                                skipped += 1
                                continue

                        # Remap any FKs (e.g. provider.care_site_id)
                        remapper.apply_row(row_data, fk_remaps)

                        # Assign new PK
                        src_self_id = row_data.get(self_pk)
                        if self_pk and src_self_id is not None:
                            if cfg.id_strategy == "preserve":
                                new_self_id = src_self_id
                            elif cfg.id_strategy == "offset":
                                new_self_id = src_self_id + cfg.id_offset
                            else:
                                await pk_counter.init_table(
                                    tgt, ts, table_name, self_pk
                                )
                                new_self_id = pk_counter.next(table_name)

                            row_data[self_pk] = new_self_id
                            remapper.record(table_name, src_self_id, new_self_id)
                            mapping_log.append({
                                "table":     table_name,
                                "source_id": src_self_id,
                                "target_id": new_self_id,
                            })

                        insert_cols  = [c for c in common_cols if c in row_data]
                        col_list     = ", ".join(f'"{c}"' for c in insert_cols)
                        placeholders = ", ".join(
                            f"${i+1}" for i in range(len(insert_cols))
                        )

                        if not cfg.dry_run:
                            try:
                                await tgt.execute(
                                    f'INSERT INTO "{ts}"."{table_name}" '
                                    f'({col_list}) VALUES ({placeholders})',
                                    *[row_data.get(c) for c in insert_cols],
                                )
                                if non_pk_dedup:
                                    tgt_admin_keys.add(
                                        tuple(row_data.get(c) for c in non_pk_dedup)
                                    )
                            except asyncpg.UniqueViolationError:
                                skipped += 1
                                continue

                        inserted += 1

                    total_inserted += inserted
                    total_skipped  += skipped
                    yield emit("log", level="ok",
                               msg=f"{table_name}: {inserted} "
                                   f"{'would be ' if cfg.dry_run else ''}inserted, "
                                   f"{skipped} skipped")
                    continue

                # ---- PERSON ------------------------------------------------
                if table_name == "person":
                    inserted = skipped = 0
                    for src_pid, info in person_map.items():
                        tgt_pid = info["target_person_id"]

                        if tgt_pid is not None:
                            total_conflicts += 1
                            if cfg.person_conflict == "abort":
                                yield emit("error",
                                           msg=f"Conflict on person {src_pid} — aborting.")
                                return
                            elif cfg.person_conflict == "upsert":
                                row = await src.fetchrow(
                                    f'SELECT * FROM "{ss}"."person" WHERE person_id=$1',
                                    src_pid,
                                )
                                if row and not cfg.dry_run:
                                    row_data = dict(row)
                                    remapper.apply_row(row_data, fk_remaps)
                                    upd_cols = [
                                        c for c in common_cols if c != "person_id"
                                    ]
                                    set_clause = ", ".join(
                                        f'"{c}"=${i+2}' for i, c in enumerate(upd_cols)
                                    )
                                    await tgt.execute(
                                        f'UPDATE "{ts}"."person" SET {set_clause} '
                                        f'WHERE person_id=$1',
                                        tgt_pid,
                                        *[row_data[c] for c in upd_cols],
                                    )
                            # Existing person: ensure remapper knows src→tgt
                            remapper.record("person", src_pid, tgt_pid)
                            skipped += 1
                            continue

                        row = await src.fetchrow(
                            f'SELECT * FROM "{ss}"."person" WHERE person_id=$1',
                            src_pid,
                        )
                        if not row:
                            continue

                        row_data = dict(row)
                        remapper.apply_row(row_data, fk_remaps)

                        if cfg.id_strategy == "preserve":
                            new_pid = src_pid
                        elif cfg.id_strategy == "offset":
                            new_pid = src_pid + cfg.id_offset
                        else:
                            await pk_counter.init_table(tgt, ts, "person", "person_id")
                            new_pid = pk_counter.next("person")

                        row_data["person_id"] = new_pid

                        insert_cols  = [c for c in common_cols if c in row_data]
                        col_list     = ", ".join(f'"{c}"' for c in insert_cols)
                        placeholders = ", ".join(
                            f"${i+1}" for i in range(len(insert_cols))
                        )

                        if not cfg.dry_run:
                            await tgt.execute(
                                f'INSERT INTO "{ts}"."person" ({col_list}) '
                                f'VALUES ({placeholders})',
                                *[row_data.get(c) for c in insert_cols],
                            )

                        person_map[src_pid]["target_person_id"] = new_pid
                        person_map[src_pid]["target_found"]     = True
                        remapper.record("person", src_pid, new_pid)
                        mapping_log.append({
                            "table": "person", "source_id": src_pid, "target_id": new_pid,
                        })
                        inserted += 1

                    total_inserted += inserted
                    total_skipped  += skipped
                    yield emit("log", level="ok",
                               msg=f"person: {inserted} inserted, "
                                   f"{skipped} skipped ({cfg.person_conflict})")
                    continue

                # ---- ALL OTHER TABLES --------------------------------------
                all_src_rows = await src.fetch(
                    f'SELECT * FROM "{ss}"."{table_name}"'
                )

                tgt_person_ids = [
                    p["target_person_id"]
                    for p in person_map.values()
                    if p["target_person_id"] is not None
                ]
                tgt_dedup_keys, non_pk_dedup = (
                    await get_tgt_dedup_keys(
                        tgt, ts, table_name, pfk, dedup_cols, tgt_cols, tgt_person_ids
                    )
                    if cfg.dedup_enabled
                    else (set(), [])
                )

                inserted = skipped = 0

                for row in all_src_rows:
                    src_pid = row[pfk]
                    if src_pid not in person_map:
                        continue
                    tgt_pid = person_map[src_pid]["target_person_id"]
                    if tgt_pid is None:
                        skipped += 1
                        continue

                    row_data = {**dict(row), pfk: tgt_pid}

                    # Remap all declared foreign keys via IDRemapper
                    remapper.apply_row(row_data, fk_remaps)

                    # Dedup check (uses remapped person FK value)
                    if cfg.dedup_enabled and non_pk_dedup:
                        key = tuple(
                            [tgt_pid] + [row_data.get(c) for c in non_pk_dedup]
                        )
                        if key in tgt_dedup_keys:
                            skipped += 1
                            continue

                    # Assign new surrogate PK and register in remapper
                    if self_pk and self_pk in row_data:
                        src_self_id = dict(row)[self_pk]   # original src value
                        if cfg.id_strategy == "preserve":
                            new_self_id = src_self_id
                        elif cfg.id_strategy == "offset":
                            new_self_id = src_self_id + cfg.id_offset
                        else:
                            await pk_counter.init_table(tgt, ts, table_name, self_pk)
                            new_self_id = pk_counter.next(table_name)

                        row_data[self_pk] = new_self_id
                        remapper.record(table_name, src_self_id, new_self_id)
                        mapping_log.append({
                            "table":     table_name,
                            "source_id": src_self_id,
                            "target_id": new_self_id,
                        })

                    insert_cols  = [c for c in common_cols if c in row_data]
                    col_list     = ", ".join(f'"{c}"' for c in insert_cols)
                    placeholders = ", ".join(
                        f"${i+1}" for i in range(len(insert_cols))
                    )

                    if not cfg.dry_run:
                        try:
                            await tgt.execute(
                                f'INSERT INTO "{ts}"."{table_name}" '
                                f'({col_list}) VALUES ({placeholders})',
                                *[row_data.get(c) for c in insert_cols],
                            )
                            if cfg.dedup_enabled and non_pk_dedup:
                                tgt_dedup_keys.add(
                                    tuple(
                                        [tgt_pid] + [row_data.get(c) for c in non_pk_dedup]
                                    )
                                )
                        except asyncpg.UniqueViolationError:
                            skipped += 1
                            continue

                    inserted += 1

                total_inserted += inserted
                total_skipped  += skipped
                yield emit("log", level="ok",
                           msg=f"{table_name}: {inserted} "
                               f"{'would be ' if cfg.dry_run else ''}inserted, "
                               f"{skipped} skipped")

            yield emit("log", level="ok",
                       msg=f"{'Dry run' if cfg.dry_run else 'Merge'} complete — "
                           f"{total_inserted} rows, {total_skipped} skipped, "
                           f"{total_conflicts} person conflicts")
            yield emit("summary",
                       inserted=total_inserted,
                       skipped=total_skipped,
                       conflicts=total_conflicts,
                       mapping_count=len(mapping_log),
                       mapping=mapping_log,
                       dry_run=cfg.dry_run)

            if cfg.dry_run:
                raise Exception("__dry_run_rollback__")

    except Exception as e:
        if "__dry_run_rollback__" in str(e):
            yield emit("log", level="info",
                       msg="Dry run — transaction rolled back, no data written")
        else:
            yield emit("error", msg=f"Merge error: {e}")
    finally:
        try:
            await src.close()
            await tgt.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="OMOP Merge Tool", version="1.2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/api/tables")
async def list_tables():
    return {
        name: {k: v for k, v in m.items() if k != "insert_order"}
        for name, m in OMOP_TABLES.items()
    }


@app.post("/api/test-connection")
async def test_connection(req: ConnectionTestRequest):
    try:
        conn = await open_conn(req.config)
        ok = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata "
            "WHERE schema_name=$1)",
            req.config.schema_name,
        )
        await conn.close()
        if not ok:
            return {"ok": False, "error": f"Schema '{req.config.schema_name}' not found"}
        return {"ok": True}
    except HTTPException as e:
        return {"ok": False, "error": e.detail}


@app.post("/api/scan")
async def scan(req: ScanRequest):
    return StreamingResponse(diff_scan(req), media_type="application/x-ndjson")


@app.post("/api/merge")
async def merge(cfg: MergeConfig):
    return StreamingResponse(run_merge(cfg), media_type="application/x-ndjson")


# Serve the frontend SPA
# uncomment for dockerised version
# app.mount("/", StaticFiles(directory="/app/frontend", html=True), name="frontend")
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")