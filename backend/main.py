"""
OMOP Patient Data Merge Tool - FastAPI Backend v1.5.5
Appends patient data from a source PostgreSQL OMOP database to a target.
Patients are discovered automatically by diffing the two databases.
"""

import json
import re
import sys
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
    # v1.4: scope which patients participate in the merge.
    #   "existing_and_new" (default): import rows for patients already in
    #     target AND create new person records for source-only patients.
    #   "existing_only": only import rows for patients whose
    #     person_source_value already matches a row in target. Source-only
    #     patients and all their clinical data are skipped entirely; the
    #     person table is also skipped when running in this mode.
    patient_scope: str = "existing_and_new"
    # v1.5: cap the number of source patients to scan/merge. None = no
    # limit (full DB). Useful for fast iteration on huge tables —
    # combined with the per-table `WHERE person_id = ANY(...)` pushdown
    # below, sampling 100 patients of a 169M-row measurement returns in
    # seconds rather than minutes.
    patient_limit: int | None = None

class ConnectionTestRequest(BaseModel):
    config: DBConfig

class ScanRequest(BaseModel):
    source: DBConfig
    target: DBConfig
    tables: list[str]
    patient_scope: str = "existing_and_new"
    patient_limit: int | None = None


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
# Streaming / batching tunables
# ---------------------------------------------------------------------------
# These control the trade-off between memory usage and number of round-trips
# to PostgreSQL. They are deliberately conservative so the tool comfortably
# fits in a few hundred MB of RAM even on huge OMOP databases. Raise them
# if you have lots of free memory and want more throughput.

FETCH_BATCH         = 5_000   # source rows pulled per cursor round-trip
INSERT_BATCH        = 1_000   # rows accumulated before executemany flush
DEDUP_FETCH_BATCH   = 10_000  # target dedup rows pulled per cursor round-trip
MAPPING_LOG_CAP     = 50_000  # row-level mapping kept in RAM; beyond this we
                              # store only a per-table summary
PATIENT_PAYLOAD_CAP = 10_000  # patients-with-new-data sent inline in
                              # scan_complete; UI shows truncated + a flag
PERSON_AUDIT_CAP    = 500_000 # person-identity audit rows shipped in summary
                              # for CSV export. Each row is small (~80 bytes
                              # serialized) so 500K ≈ 40MB JSON. Plenty for
                              # most OMOP sites; bump if you have more
                              # patients and your gateway can handle it.


async def iter_rows(conn, query: str, *args, batch_size: int = FETCH_BATCH):
    """
    Stream rows from `query` via a server-side cursor.

    Must be called inside a transaction (asyncpg requirement). Yields
    `Record` objects one at a time but only `batch_size` are held in
    memory at once because `cursor.fetch(batch_size)` is what physically
    talks to the server.

    Cancellation safety
    -------------------
    If the consumer abandons this generator mid-stream (e.g. the merge
    loop raises MergeRowError and Python tears the `async for` down with
    GeneratorExit), the cursor still has unfetched batches on the wire.
    asyncpg's transaction __aexit__ then issues ROLLBACK on top of that
    pending state and crashes with "got result for unknown protocol
    state 3". Worse, that crash propagates *out* of the generator,
    masking the original MergeRowError and breaking the StreamingResponse
    so the frontend never sees an error event at all.

    We can't use `async with conn.transaction()` because its __aexit__
    runs BEFORE any except clause we put around it — that's the order
    Python defines for context managers inside try blocks. Instead we
    drive the transaction manually so we can detect mid-stream
    cancellation (GeneratorExit) and forcibly terminate the connection
    *before* asyncpg ever tries to issue ROLLBACK on the dirty protocol
    state. The merge endpoint's outer finally block tolerates a
    terminated connection, and the original upstream exception (e.g.
    MergeRowError) then propagates cleanly to the structured error
    handler.

    This is the workhorse of the v1.3 memory pass: replacing
    `await conn.fetch(...)` with `async for r in iter_rows(...)` turns a
    full-table materialisation into a constant-memory stream.
    """
    tx = conn.transaction()
    await tx.start()
    cancelled = False
    try:
        cur = await conn.cursor(query, *args)
        while True:
            batch = await cur.fetch(batch_size)
            if not batch:
                break
            for r in batch:
                try:
                    yield r
                except GeneratorExit:
                    # Caught HERE rather than around the whole `async with`
                    # so we can act before any cleanup statement hits the
                    # wire on top of the half-drained cursor.
                    cancelled = True
                    raise
    finally:
        if cancelled:
            # Don't even try to ROLLBACK — terminate is synchronous and
            # safe regardless of protocol state.
            try:
                conn.terminate()
            except Exception:
                pass
        else:
            # Normal path: clean rollback (read-only tx, so this is just
            # bookkeeping). Defensive in case the server closed on us.
            try:
                await tx.rollback()
            except Exception:
                pass


class MergeRowError(Exception):
    """
    Raised when an insert fails for reasons other than a unique-violation
    (e.g. value-too-long, NOT NULL violation, FK violation).

    Carries enough context for the frontend to display:
      - the table being merged
      - the SQL that was executed
      - the column → value pairs of the offending row (with long values
        truncated so the JSON payload stays small)
      - the underlying PG sqlstate / message / detail / column_name
        when asyncpg surfaces them
    """

    def __init__(self, table: str, sql: str, cols: list[str], values: tuple,
                 cause: Exception):
        self.table = table
        self.sql = sql
        self.cols = cols
        self.values = values
        self.cause = cause
        # Pull whatever asyncpg gives us — these attributes are present on
        # asyncpg.PostgresError but not on every Exception.
        self.sqlstate     = getattr(cause, "sqlstate", None)
        self.pg_message   = getattr(cause, "message", str(cause))
        self.pg_detail    = getattr(cause, "detail", None)
        self.pg_column    = getattr(cause, "column_name", None)
        self.pg_table     = getattr(cause, "table_name", None)
        self.pg_constraint = getattr(cause, "constraint_name", None)
        super().__init__(self.pg_message)

    def to_dict(self, value_truncate: int = 200) -> dict:
        """JSON-safe dict for the SSE error event."""
        def short(v):
            if v is None:
                return None
            s = str(v)
            return s if len(s) <= value_truncate else s[:value_truncate] + "…"

        row_repr = {c: short(v) for c, v in zip(self.cols, self.values)}

        # For varchar-length errors PostgreSQL almost never tells us which
        # column overflowed. Detect it ourselves by scanning the row for
        # values longer than the declared width in the PG message.
        # Example messages we want to handle:
        #   "value too long for type character varying(60)"
        #   "value too long for type varchar(50)"
        oversize_columns: list[dict] = []
        max_len = None
        msg = self.pg_message or ""
        m = re.search(r"character varying\((\d+)\)|varchar\((\d+)\)", msg)
        if m:
            max_len = int(m.group(1) or m.group(2))
            for c, v in zip(self.cols, self.values):
                if v is None:
                    continue
                vlen = len(str(v))
                if vlen > max_len:
                    oversize_columns.append({
                        "column":      c,
                        "length":      vlen,
                        "max_length":  max_len,
                        # Keep a slightly longer preview here so the user
                        # can see roughly what's there even after the
                        # generic short() truncation above.
                        "preview":     short(v),
                    })

        # Surface the most-likely offending column at the top of the
        # frontend display. PG's column_name takes priority when set;
        # otherwise we use the first oversize column we detected.
        likely_col = self.pg_column
        if not likely_col and oversize_columns:
            likely_col = oversize_columns[0]["column"]

        return {
            "table":            self.table,
            "sqlstate":         self.sqlstate,
            "pg_message":       self.pg_message,
            "pg_detail":        self.pg_detail,
            "pg_column":        self.pg_column,
            "pg_table":         self.pg_table,
            "pg_constraint":    self.pg_constraint,
            "likely_column":    likely_col,
            "oversize_columns": oversize_columns,
            "sql":              self.sql,
            "row":              row_repr,
        }


class BatchInserter:
    """
    Accumulates rows for one table and flushes them with `executemany`
    every INSERT_BATCH rows.

    Error handling
    --------------
    * UniqueViolationError → fall back to per-row inserts, count collisions
      as "skipped". Preserves v1.2 dedup behaviour.
    * Any other PostgresError → fall back to per-row inserts to find the
      exact failing row, then raise MergeRowError with full context
      (table, SQL, column→value mapping, sqlstate, PG message).
    """

    def __init__(self, conn, schema: str, table: str, cols: list[str]):
        self.conn = conn
        self.schema = schema
        self.table = table
        self.cols = cols
        col_list     = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
        self.sql = (
            f'INSERT INTO "{schema}"."{table}" ({col_list}) '
            f'VALUES ({placeholders})'
        )
        self._buf: list[tuple] = []
        self.inserted = 0
        self.skipped = 0

    async def add(self, values: tuple):
        self._buf.append(values)
        if len(self._buf) >= INSERT_BATCH:
            await self._flush()

    async def _flush(self):
        if not self._buf:
            return
        buf = self._buf
        self._buf = []   # clear early so a re-raise doesn't double-flush

        # First try the whole batch in one round-trip — fast path.
        # We need a savepoint here because if executemany fails the outer
        # transaction enters an aborted state and every subsequent
        # statement (including the per-row retries below) would fail with
        # "current transaction is aborted". A savepoint lets us roll back
        # just the failed batch and then probe row-by-row.
        sp_name = f"bi_{id(buf):x}"
        await self.conn.execute(f"SAVEPOINT {sp_name}")
        try:
            await self.conn.executemany(self.sql, buf)
            await self.conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            self.inserted += len(buf)
            return
        except asyncpg.PostgresError:
            await self.conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            await self.conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            # Fall through to per-row to identify exactly which row(s) failed.

        for vals in buf:
            row_sp = f"bi_row_{id(vals):x}"
            await self.conn.execute(f"SAVEPOINT {row_sp}")
            try:
                await self.conn.execute(self.sql, *vals)
                await self.conn.execute(f"RELEASE SAVEPOINT {row_sp}")
                self.inserted += 1
            except asyncpg.UniqueViolationError:
                await self.conn.execute(f"ROLLBACK TO SAVEPOINT {row_sp}")
                await self.conn.execute(f"RELEASE SAVEPOINT {row_sp}")
                self.skipped += 1
            except asyncpg.PostgresError as e:
                # Roll back the savepoint cleanly so the outer transaction
                # isn't in an aborted state when our caller's exception
                # handler runs (it may need to do its own ROLLBACK).
                # Wrap in try/except since the connection might already
                # be in a bad state and we don't want a cleanup failure
                # to mask the real error.
                try:
                    await self.conn.execute(f"ROLLBACK TO SAVEPOINT {row_sp}")
                    await self.conn.execute(f"RELEASE SAVEPOINT {row_sp}")
                except Exception:
                    pass
                raise MergeRowError(
                    table=self.table, sql=self.sql,
                    cols=self.cols, values=vals, cause=e,
                ) from e

    async def flush(self) -> tuple[int, int]:
        await self._flush()
        return self.inserted, self.skipped


async def exec_with_context(conn, table: str, sql: str,
                            cols: list[str], values: tuple):
    """
    Run a single statement and, on PostgresError, re-raise as
    MergeRowError so the merge endpoint can emit rich error context.

    Use for statements that don't go through BatchInserter — e.g. the
    person-table INSERT and the demographics-upsert UPDATE, which both
    use connection.execute() directly and would otherwise lose row
    context on failure.
    """
    try:
        await conn.execute(sql, *values)
    except asyncpg.PostgresError as e:
        raise MergeRowError(
            table=table, sql=sql, cols=cols, values=values, cause=e,
        ) from e


class CappedMappingLog:
    """
    Bounded replacement for the unbounded `mapping_log: list[dict]`.

    For small tables (< MAPPING_LOG_CAP entries across the whole run) it
    behaves identically to a list — every (table, src_id, tgt_id) triple
    is retained. Once the cap is hit we stop appending entries for the
    table that caused the overflow and only track per-table counters.
    A summary is written into `summaries` so the export step can still
    show what happened.
    """

    def __init__(self, cap: int = MAPPING_LOG_CAP):
        self.cap = cap
        self.entries: list[dict] = []
        self.summaries: dict[str, dict] = {}   # table -> {count, min, max}
        self._truncated_tables: set[str] = set()

    def add(self, table: str, src_id, tgt_id):
        s = self.summaries.setdefault(
            table, {"count": 0, "min_target": tgt_id, "max_target": tgt_id}
        )
        s["count"] += 1
        if tgt_id is not None:
            if s["min_target"] is None or tgt_id < s["min_target"]:
                s["min_target"] = tgt_id
            if s["max_target"] is None or tgt_id > s["max_target"]:
                s["max_target"] = tgt_id

        if len(self.entries) < self.cap:
            self.entries.append(
                {"table": table, "source_id": src_id, "target_id": tgt_id}
            )
        else:
            self._truncated_tables.add(table)

    def truncated(self) -> bool:
        return bool(self._truncated_tables)

    def truncated_tables(self) -> list[str]:
        return sorted(self._truncated_tables)


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

# JavaScript Number is IEEE 754 double precision: only ints with absolute
# value < 2^53 round-trip through JSON.parse without precision loss.
# OMOP databases that use BIGINT person_ids — especially hash-derived ones
# that span the full int64 range — silently lose their last few digits
# when serialized as JSON ints and parsed in the browser.
#
# Solution: any int outside this range gets serialized as a JSON string.
# CSV export, error panel, ID mapping table, and the audit all then
# round-trip these values losslessly. Small ints (counts, percentages,
# concept_ids, etc.) keep their numeric type so frontend display logic
# is unaffected.
JS_SAFE_INT_MAX =  (1 << 53) - 1   # 9_007_199_254_740_991
JS_SAFE_INT_MIN = -(1 << 53) + 1   # -9_007_199_254_740_991


def _stringify_unsafe_ints(obj):
    """
    Recursively walk a JSON-bound structure and replace any int outside
    the JS-safe range with its string form. Booleans (which are ints in
    Python) are left alone. Other types pass through unchanged — actual
    type conversion happens in json.dumps itself.
    """
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        if obj > JS_SAFE_INT_MAX or obj < JS_SAFE_INT_MIN:
            return str(obj)
        return obj
    if isinstance(obj, dict):
        return {k: _stringify_unsafe_ints(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_stringify_unsafe_ints(v) for v in obj]
    return obj


def emit(type_: str, **kwargs) -> str:
    payload = {"type": type_, "ts": datetime.now().isoformat(), **kwargs}
    return json.dumps(_stringify_unsafe_ints(payload), default=str) + "\n"


# ---------------------------------------------------------------------------
# Person identity map
# ---------------------------------------------------------------------------

async def build_person_map(src, tgt, ss, ts, limit: int | None = None,
                            limit_to_existing: bool = False):
    """
    Returns person_map: source_person_id -> {
        source_value: str | None,
        target_person_id: int | None,   # None = not yet in target
        target_found: bool,
    }

    Patients are matched across DBs by person_source_value.

    Sampling
    --------
    If `limit` is set, only the first `limit` source patients (ordered by
    person_id ascending) are returned in the map. Everything downstream
    is keyed on this map, so a small map ⇒ small scan and small merge.
    Use this for fast iteration on large databases.

    If `limit_to_existing` is True, sampling picks the first `limit`
    source patients whose person_source_value also exists in the target.
    This is useful with patient_scope="existing_only" where source-only
    patients would otherwise be sampled but then skipped, wasting the
    sample budget.

    FIX (v1.2): previously `sv = r["person_id"]` caused every patient to be
    keyed on their numeric ID rather than their source value, so cross-DB
    matching always failed silently.
    """
    # Always fetch the target person table fully — we need it as the
    # match index. It's small (one row per patient, two columns) so this
    # is fine even at scale.
    tgt_persons = await tgt.fetch(
        f'SELECT person_id, person_source_value FROM "{ts}"."person"'
    )
    tgt_by_sv = {
        r["person_source_value"]: r["person_id"]
        for r in tgt_persons
        if r["person_source_value"]
    }

    # Build the source query. Order by person_id for deterministic
    # sampling — important so repeated dry-runs produce identical scan
    # output. When limit_to_existing is set, push the filter into SQL so
    # the LIMIT applies after the join, not before.
    if limit is not None and limit_to_existing and tgt_by_sv:
        # Only consider source patients whose source_value is in target.
        # asyncpg parameter binding handles the ANY($1) array push.
        src_persons = await src.fetch(
            f'SELECT person_id, person_source_value '
            f'FROM "{ss}"."person" '
            f'WHERE person_source_value = ANY($1::text[]) '
            f'ORDER BY person_id LIMIT $2',
            list(tgt_by_sv.keys()), limit,
        )
    elif limit is not None:
        src_persons = await src.fetch(
            f'SELECT person_id, person_source_value FROM "{ss}"."person" '
            f'ORDER BY person_id LIMIT $1',
            limit,
        )
    else:
        src_persons = await src.fetch(
            f'SELECT person_id, person_source_value FROM "{ss}"."person"'
        )

    person_map = {}
    for r in src_persons:
        sv = r["person_source_value"]   # FIX: was r["person_id"]
        tgt_pid = tgt_by_sv.get(sv) if sv else None
        # match_type explains how this source patient relates to target.
        # Set initially during build; the person-table merge step flips
        # 'unmatched' to 'inserted_new' once a new target row is created.
        if not sv:
            match_type = "unmatched_no_source_value"
        elif tgt_pid is not None:
            match_type = "matched_existing"
        else:
            match_type = "unmatched"
        person_map[r["person_id"]] = {
            "source_value":     sv,
            "target_person_id": tgt_pid,
            "target_found":     tgt_pid is not None,   # FIX: key was missing
            "match_type":       match_type,
        }
    return person_map


# ---------------------------------------------------------------------------
# Dedup helper
# ---------------------------------------------------------------------------

async def get_tgt_dedup_keys(tgt, ts, table_name, pfk, dedup_cols, tgt_cols, tgt_person_ids):
    """
    Build the set of existing target dedup keys for a table.

    In v1.3 the rows are pulled via a server-side cursor in batches of
    DEDUP_FETCH_BATCH so peak memory is bounded by the final size of the
    set rather than 2× (rows + set). Each tuple in the set is small
    (person_id + a couple of dates / concept_ids) so even tens of
    millions of entries fit in well under a GB.
    """
    non_pk_dedup = [c for c in dedup_cols if c != pfk and c in tgt_cols]
    if not non_pk_dedup or not tgt_person_ids:
        return set(), non_pk_dedup
    col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
    keys: set = set()
    sql = (
        f'SELECT "{pfk}", {col_list} FROM "{ts}"."{table_name}" '
        f'WHERE "{pfk}" = ANY($1::bigint[])'
    )
    async for r in iter_rows(tgt, sql, tgt_person_ids, batch_size=DEDUP_FETCH_BATCH):
        keys.add(tuple([r[pfk]] + [r[c] for c in non_pk_dedup]))
    return keys, non_pk_dedup


async def get_admin_dedup_keys(tgt, ts, table_name, dedup_cols, tgt_cols):
    """
    Build the set of existing target dedup keys for an admin/reference
    table (provider, care_site) which has no person_fk.

    Streamed in batches so even a target with a million providers does
    not blow up memory.
    """
    non_pk_dedup = [c for c in dedup_cols if c in tgt_cols]
    if not non_pk_dedup:
        return set(), non_pk_dedup
    col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
    keys: set = set()
    sql = f'SELECT {col_list} FROM "{ts}"."{table_name}"'
    async for r in iter_rows(tgt, sql, batch_size=DEDUP_FETCH_BATCH):
        keys.add(tuple(r[c] for c in non_pk_dedup))
    return keys, non_pk_dedup


def build_clinical_source_query(schema: str, table: str, pfk: str,
                                 person_ids: list[int] | None):
    """
    Return (sql, args) for streaming a clinical table from the source.

    When `person_ids` is None we scan the whole table — no filter, full
    table cursor. When it's a list, we push a `WHERE person_id = ANY(...)`
    filter into SQL so the database does the work and the cursor only
    streams matching rows. This is the chokepoint that makes sample
    mode actually fast on huge clinical tables: on a 169M-row
    measurement with a 100-patient sample, we go from streaming 169M
    rows to streaming a few thousand.

    `person_ids` should be small (≤ ~10k) — for very large lists asyncpg
    will round-trip the array as text and PostgreSQL's ANY-array plan is
    not necessarily faster than a full scan + hash filter. We don't push
    the filter for unbounded sets; that path is for sample/cohort mode.

    The cast is `bigint[]` rather than `int[]` because some sites use
    BIGINT person_ids (often hash-derived, sometimes negative) that
    overflow int32. `bigint[]` accepts both 32- and 64-bit values.
    """
    if person_ids is None:
        return f'SELECT * FROM "{schema}"."{table}"', ()
    return (
        f'SELECT * FROM "{schema}"."{table}" '
        f'WHERE "{pfk}" = ANY($1::bigint[])',
        (person_ids,),
    )


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
        existing_only = (req.patient_scope == "existing_only")
        limit = req.patient_limit if req.patient_limit and req.patient_limit > 0 else None

        if limit is not None:
            yield emit("log", level="info",
                       msg=f"Sample mode: scanning first {limit:,} patients only "
                           f"(deterministic by person_id)")

        yield emit("log", level="info",
                   msg="Building person identity map via person_source_value…")
        person_map = await build_person_map(
            src, tgt, ss, ts,
            limit=limit,
            limit_to_existing=existing_only,
        )

        # FIX: use "target_found" key (was KeyError in original)
        new_patients = sum(1 for p in person_map.values() if not p["target_found"])
        total_src = len(person_map)
        scope_msg = (
            " — existing-patients-only mode: source-only patients will be ignored"
            if existing_only else ""
        )
        sample_msg = f" (sampled from full source)" if limit is not None else ""
        yield emit("log", level="ok",
                   msg=f"{total_src} patients in scope{sample_msg} — "
                       f"{new_patients} new, "
                       f"{total_src - new_patients} already in target{scope_msg}")

        tables_to_scan = sorted(
            [t for t in req.tables if t in OMOP_TABLES],
            key=lambda t: OMOP_TABLES[t]["insert_order"],
        )
        # In existing-only mode the person table is meaningless — drop it
        # from the scan so the UI doesn't show "X new persons would be added".
        if existing_only and "person" in tables_to_scan:
            tables_to_scan = [t for t in tables_to_scan if t != "person"]
            yield emit("log", level="info",
                       msg="person table excluded from scan (existing-only mode)")

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
                tgt_cols = await get_columns(tgt, ts, table_name)
                tgt_keys, non_pk_dedup = await get_admin_dedup_keys(
                    tgt, ts, table_name, dedup_cols, tgt_cols
                )
                new_rows_count = 0
                # Stream the source — admin tables are usually small but
                # care_site can run into the millions on big sites.
                async for row in iter_rows(
                    src, f'SELECT * FROM "{ss}"."{table_name}"'
                ):
                    if not non_pk_dedup:
                        new_rows_count += 1
                        continue
                    key = tuple(row[c] for c in non_pk_dedup)
                    if key not in tgt_keys:
                        new_rows_count += 1
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

            new_rows_count = 0
            affected_patients: set[int] = set()

            # Stream rather than fetch — clinical tables are the hot spot:
            # measurement / observation can have tens of millions of rows.
            # When sampling is active, push the patient filter into SQL
            # so the cursor only delivers in-scope rows; this is the win
            # that turns a 169M-row scan into a few-thousand-row scan.
            sample_ids = list(person_map.keys()) if limit is not None else None
            sql, args = build_clinical_source_query(ss, table_name, pfk, sample_ids)
            async for row in iter_rows(src, sql, *args):
                src_pid = row[pfk]
                if src_pid not in person_map:
                    continue
                info = person_map[src_pid]
                tgt_pid = info["target_person_id"] if info["target_found"] else None

                # In existing-only mode, skip patients who only exist in
                # the source — their clinical rows are not imported.
                if existing_only and tgt_pid is None:
                    continue

                if tgt_pid is not None and non_pk_dedup:
                    # `Record` supports r[col] directly — no need for dict(row).
                    key = tuple([tgt_pid] + [row[c] for c in non_pk_dedup])
                    if key in tgt_keys:
                        continue

                new_rows_count += 1
                affected_patients.add(src_pid)
                bucket = patient_summary.setdefault(src_pid, {})
                bucket[table_name] = bucket.get(table_name, 0) + 1

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

        total_patients = len(patients_with_new_data)
        truncated = total_patients > PATIENT_PAYLOAD_CAP
        payload_patients = patients_with_new_data[:PATIENT_PAYLOAD_CAP]

        total_new_rows = sum(t["new_rows"] for t in table_totals.values())
        yield emit("scan_complete",
                   total_patients_with_new_data=total_patients,
                   total_new_rows=total_new_rows,
                   table_totals=table_totals,
                   patients=payload_patients,
                   patients_truncated=truncated,
                   patients_shown=len(payload_patients))
        suffix = (
            f" (showing top {PATIENT_PAYLOAD_CAP:,} in preview)"
            if truncated else ""
        )
        yield emit("log", level="ok",
                   msg=f"Scan complete — {total_patients} patients "
                       f"have new data, {total_new_rows} total rows "
                       f"to import{suffix}")

    except Exception as e:
        print(f"ERROR {e}")
        yield emit("error", msg=f"Scan error: {e}")
    finally:
        # Tolerate already-terminated connections (iter_rows force-kills
        # the source connection on mid-stream cancellation).
        for c in (src, tgt):
            try:
                await c.close()
            except Exception:
                pass


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
    existing_only = (cfg.patient_scope == "existing_only")
    limit = cfg.patient_limit if cfg.patient_limit and cfg.patient_limit > 0 else None

    if limit is not None:
        yield emit("log", level="warn",
                   msg=f"Sample mode: only the first {limit:,} patients will "
                       f"be merged. Disable sampling for a full run.")

    yield emit("log", level="info", msg="Building person identity map…")
    person_map = await build_person_map(
        src, tgt, ss, ts,
        limit=limit,
        limit_to_existing=existing_only,
    )

    if existing_only:
        yield emit("log", level="info",
                   msg="Existing-patients-only mode: source-only patients "
                       "and the person table will be skipped")

    tables_to_run = sorted(
        [t for t in cfg.tables if t in OMOP_TABLES],
        key=lambda t: OMOP_TABLES[t]["insert_order"],
    )
    if existing_only and "person" in tables_to_run:
        tables_to_run = [t for t in tables_to_run if t != "person"]
        yield emit("log", level="info",
                   msg="person table excluded from merge (existing-only mode)")

    total_inserted = total_skipped = total_conflicts = 0
    mapping_log = CappedMappingLog()
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
                # `common_cols` is the intersection by NAME, not by index.
                # All INSERT statements built downstream enumerate this list
                # explicitly — `INSERT INTO tbl (a, b, c) VALUES ($1, $2, $3)`
                # — and pull values via `row_data.get(col)`, so source and
                # target column ordering can differ without any effect.
                # Source-only columns are silently dropped; target-only
                # columns get the target's default value (NULL or otherwise).
                common_cols = [c for c in src_cols if c in tgt_cols]

                # Surface schema skew so OMOP version mismatches don't go
                # unnoticed. Common when source and target are different
                # CDM versions (e.g. v5.3 vs v5.4) — useful to confirm
                # the column drift matches what you expect.
                src_only = [c for c in src_cols if c not in tgt_cols]
                tgt_only = [c for c in tgt_cols if c not in src_cols]
                if src_only:
                    yield emit("log", level="warn",
                               msg=f"{table_name}: source has columns not "
                                   f"in target — will be dropped: "
                                   f"{', '.join(src_only)}")
                if tgt_only:
                    yield emit("log", level="info",
                               msg=f"{table_name}: target has columns not "
                                   f"in source — will use target defaults: "
                                   f"{', '.join(tgt_only)}")

                # ---- ADMIN TABLES (no person FK: care_site, provider) ------
                if table_name in ADMIN_TABLES:
                    # Build dedup key set from target — streamed in v1.3.
                    tgt_admin_keys, non_pk_dedup = await get_admin_dedup_keys(
                        tgt, ts, table_name, dedup_cols, tgt_cols
                    )

                    insert_cols = list(common_cols)
                    inserter = (
                        BatchInserter(tgt, ts, table_name, insert_cols)
                        if not cfg.dry_run else None
                    )
                    inserted = skipped = 0

                    # Stream source rows — care_site can be huge in some EHRs.
                    async for row in iter_rows(
                        src, f'SELECT * FROM "{ss}"."{table_name}"'
                    ):
                        # Dedup check using Record directly (no dict copy).
                        if non_pk_dedup:
                            key = tuple(row[c] for c in non_pk_dedup)
                            if key in tgt_admin_keys:
                                skipped += 1
                                continue

                        # We mutate FK / PK columns so a dict is necessary,
                        # but only one allocation per kept row (vs. the v1.2
                        # code which called dict(row) speculatively for every
                        # row including those we then skipped).
                        row_data = dict(row)
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
                            mapping_log.add(table_name, src_self_id, new_self_id)

                        if non_pk_dedup:
                            # Block dups within this run too.
                            tgt_admin_keys.add(
                                tuple(row_data.get(c) for c in non_pk_dedup)
                            )

                        if cfg.dry_run:
                            inserted += 1
                        else:
                            await inserter.add(
                                tuple(row_data.get(c) for c in insert_cols)
                            )

                    if inserter is not None:
                        ins, skp = await inserter.flush()
                        inserted += ins
                        skipped  += skp

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
                                    upd_sql = (
                                        f'UPDATE "{ts}"."person" SET {set_clause} '
                                        f'WHERE person_id=$1'
                                    )
                                    await exec_with_context(
                                        tgt, "person", upd_sql,
                                        ["person_id"] + upd_cols,
                                        tuple([tgt_pid] + [row_data[c] for c in upd_cols]),
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
                            ins_sql = (
                                f'INSERT INTO "{ts}"."person" ({col_list}) '
                                f'VALUES ({placeholders})'
                            )
                            await exec_with_context(
                                tgt, "person", ins_sql, insert_cols,
                                tuple(row_data.get(c) for c in insert_cols),
                            )

                        person_map[src_pid]["target_person_id"] = new_pid
                        person_map[src_pid]["target_found"]     = True
                        person_map[src_pid]["match_type"]       = "inserted_new"
                        remapper.record("person", src_pid, new_pid)
                        mapping_log.add("person", src_pid, new_pid)
                        inserted += 1

                    total_inserted += inserted
                    total_skipped  += skipped
                    yield emit("log", level="ok",
                               msg=f"person: {inserted} inserted, "
                                   f"{skipped} skipped ({cfg.person_conflict})")
                    continue

                # ---- ALL OTHER TABLES --------------------------------------
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

                # Pre-init the PK counter once (instead of inside the loop).
                if self_pk and cfg.id_strategy == "auto":
                    await pk_counter.init_table(tgt, ts, table_name, self_pk)

                insert_cols = list(common_cols)
                inserter = (
                    BatchInserter(tgt, ts, table_name, insert_cols)
                    if not cfg.dry_run else None
                )

                inserted = skipped = 0

                # Stream source rows via cursor — this is the v1.3 hot path.
                # Tables like measurement / observation can be 50M+ rows;
                # fetching them all into Python `Record` objects was the
                # primary cause of the OOM observed in v1.2.
                # When sampling is active, push the patient filter into
                # SQL (v1.5) so we don't even pull rows we'll discard.
                sample_ids = list(person_map.keys()) if limit is not None else None
                sql, args = build_clinical_source_query(
                    ss, table_name, pfk, sample_ids
                )
                async for row in iter_rows(src, sql, *args):
                    src_pid = row[pfk]
                    if src_pid not in person_map:
                        continue
                    tgt_pid = person_map[src_pid]["target_person_id"]
                    if tgt_pid is None:
                        skipped += 1
                        continue

                    # Build a mutable copy so we can apply FK remaps and
                    # the new surrogate PK before insert.
                    row_data = dict(row)
                    row_data[pfk] = tgt_pid

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

                    # Assign new surrogate PK and register in remapper.
                    # Read original src value directly from Record — no need
                    # for the v1.2 dict(row)[self_pk] copy.
                    if self_pk and self_pk in row_data:
                        src_self_id = row[self_pk]
                        if cfg.id_strategy == "preserve":
                            new_self_id = src_self_id
                        elif cfg.id_strategy == "offset":
                            new_self_id = src_self_id + cfg.id_offset
                        else:
                            new_self_id = pk_counter.next(table_name)

                        row_data[self_pk] = new_self_id
                        remapper.record(table_name, src_self_id, new_self_id)
                        mapping_log.add(table_name, src_self_id, new_self_id)

                    # NOTE on dedup semantics (changed in v1.5.5):
                    # `tgt_dedup_keys` only contains rows that existed in
                    # target *before* this run. We deliberately do NOT
                    # add this row's key to the set after inserting,
                    # because clinical OMOP data legitimately has many
                    # rows sharing (person_id, concept_id, date) — daily
                    # vitals, repeated lab panels, ICU monitoring, etc.
                    # MIMIC-IV-on-OMOP is a canonical example. Within-
                    # run dedup was silently dropping ~96% of such rows.
                    # Now: source rows can repeat freely, but anything
                    # that target already had stays protected.

                    if cfg.dry_run:
                        inserted += 1
                    else:
                        await inserter.add(
                            tuple(row_data.get(c) for c in insert_cols)
                        )

                if inserter is not None:
                    ins, skp = await inserter.flush()
                    inserted += ins
                    skipped  += skp

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
            if mapping_log.truncated():
                yield emit("log", level="warn",
                           msg=f"Mapping log truncated for: "
                               f"{', '.join(mapping_log.truncated_tables())} "
                               f"(>{MAPPING_LOG_CAP:,} entries — see per-table summary)")

            # Build person identity audit. One entry per source patient with
            # the source_value used for matching, plus a match_type so the
            # CSV reader can verify "source patient X linked to target
            # patient Y because their person_source_value both equal Z."
            # Capped at PERSON_AUDIT_CAP so very large source DBs don't
            # produce a multi-GB SSE payload — the toggle to disable the
            # cap is on the roadmap if anyone needs it.
            person_audit: list[dict] = []
            audit_counts = {
                "matched_existing": 0,
                "inserted_new": 0,
                "source_only_skipped": 0,
                "unmatched_no_source_value": 0,
                "unmatched": 0,
            }
            for src_pid, info in person_map.items():
                mt = info.get("match_type", "unmatched")
                # In existing_only mode, an unmatched patient was deliberately
                # skipped — re-label so the audit is unambiguous.
                if mt == "unmatched" and existing_only:
                    mt = "source_only_skipped"
                audit_counts[mt] = audit_counts.get(mt, 0) + 1
                if len(person_audit) < PERSON_AUDIT_CAP:
                    person_audit.append({
                        "source_person_id":   src_pid,
                        "target_person_id":   info["target_person_id"],
                        "person_source_value": info["source_value"],
                        "match_type":         mt,
                    })

            audit_truncated = len(person_map) > PERSON_AUDIT_CAP
            if audit_truncated:
                yield emit("log", level="warn",
                           msg=f"Person audit truncated to {PERSON_AUDIT_CAP:,} "
                               f"of {len(person_map):,} patients in export")

            yield emit("summary",
                       inserted=total_inserted,
                       skipped=total_skipped,
                       conflicts=total_conflicts,
                       mapping_count=sum(s["count"] for s in mapping_log.summaries.values()),
                       mapping=mapping_log.entries,
                       mapping_summaries=mapping_log.summaries,
                       mapping_truncated=mapping_log.truncated(),
                       person_audit=person_audit,
                       person_audit_counts=audit_counts,
                       person_audit_total=len(person_map),
                       person_audit_truncated=audit_truncated,
                       dry_run=cfg.dry_run)

            if cfg.dry_run:
                raise Exception("__dry_run_rollback__")

    except Exception as e:
        if "__dry_run_rollback__" in str(e):
            yield emit("log", level="info",
                       msg="Dry run — transaction rolled back, no data written")
        else:
            # Unwrap exception chain in case the transaction __aexit__ or
            # some other cleanup path wrapped our MergeRowError. Walk both
            # __cause__ and __context__ to find it.
            row_err = None
            cur = e
            seen = set()
            while cur is not None and id(cur) not in seen:
                seen.add(id(cur))
                if isinstance(cur, MergeRowError):
                    row_err = cur
                    break
                cur = cur.__cause__ or cur.__context__

            if row_err is not None:
                # Always log to stderr so it's visible in the backend
                # console — helpful when debugging frontend display.
                import traceback
                print(
                    f"\n=== MergeRowError in {row_err.table} ===\n"
                    f"  sqlstate: {row_err.sqlstate}\n"
                    f"  message:  {row_err.pg_message}\n"
                    f"  column:   {row_err.pg_column}\n",
                    file=sys.stderr, flush=True,
                )

                err = row_err.to_dict()
                headline = f"{err['table']}: {err['pg_message']}"
                # Prefer PG's column name; fall back to detected oversize.
                hint_col = err["pg_column"] or err["likely_column"]
                if hint_col:
                    headline += f" (column: {hint_col})"
                if err["oversize_columns"]:
                    over = err["oversize_columns"][0]
                    headline += (
                        f" — value is {over['length']} chars, "
                        f"target accepts {over['max_length']}"
                    )
                yield emit("log", level="err", msg=f"Merge error — {headline}")
                yield emit("error",
                           msg=f"Merge error in {err['table']}: {err['pg_message']}",
                           error_kind="row_insert",
                           table=err["table"],
                           sqlstate=err["sqlstate"],
                           pg_message=err["pg_message"],
                           pg_detail=err["pg_detail"],
                           pg_column=err["pg_column"],
                           pg_constraint=err["pg_constraint"],
                           likely_column=err["likely_column"],
                           oversize_columns=err["oversize_columns"],
                           sql=err["sql"],
                           row=err["row"])
            else:
                # Unknown failure — still try to give the table at least.
                import traceback
                print(
                    f"\n=== Unhandled merge error ===\n"
                    f"{traceback.format_exc()}",
                    file=sys.stderr, flush=True,
                )
                yield emit("log", level="err", msg=f"Merge error: {e}")
                yield emit("error",
                           msg=f"Merge error: {e}",
                           error_kind="unknown",
                           exception_type=type(e).__name__)
    finally:
        try:
            await src.close()
            await tgt.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="OMOP Merge Tool", version="1.5.5")
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