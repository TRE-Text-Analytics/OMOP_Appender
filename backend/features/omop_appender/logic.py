"""
Core logic for the OMOP merge feature.

Contains:
  - DB connection helpers
  - Streaming cursor helper (iter_rows)
  - MergeRowError, BatchInserter, exec_with_context
  - CappedMappingLog, PKCounter, IDRemapper
  - SSE/NDJSON emit helper
  - Person identity map builder
  - Dedup helpers
  - diff_scan  (scan endpoint generator)
  - run_merge  (merge endpoint generator)
"""

import json
import re
import sys
from array import array
from datetime import datetime
from typing import AsyncGenerator

import asyncpg
from fastapi import HTTPException

from .constants import (
    ADMIN_TABLES,
    DEDUP_FETCH_BATCH,
    FETCH_BATCH,
    INSERT_BATCH,
    JS_SAFE_INT_MAX,
    JS_SAFE_INT_MIN,
    MAPPING_LOG_CAP,
    OMOP_TABLES,
    PATIENT_PAYLOAD_CAP,
    PERSON_AUDIT_CAP,
)
from .models import DBConfig, MergeConfig, ScanRequest


# array("q") is a signed 64-bit buffer; some sources type OMOP ids as wide
# NUMERIC and generate values beyond bigint range (> 2^63-1). Such an id can
# never be a *target* value (target columns are int/bigint), but it can appear
# as a *source* lookup key, which we only ever hold in memory and resolve via
# the remapper — so we keep the compact array where values fit and fall back to
# a plain Python list (arbitrary precision) only for the column that overflows.
_Q_MIN = -(1 << 63)
_Q_MAX = (1 << 63) - 1


def _append_id(container, value: int):
    """
    Append ``value`` to ``container``, which starts as an ``array("q")`` for
    memory efficiency. If ``value`` exceeds signed 64-bit range and the
    container is still an array, upgrade it to a plain ``list`` first. Returns
    the container to use going forward (a new list object after an upgrade,
    otherwise the same object).
    """
    if type(container) is array and not (_Q_MIN <= value <= _Q_MAX):
        container = list(container)
    container.append(value)
    return container


def _id_array_param(ids):
    """
    Build the ``(cast, values)`` pair for an ``id_col = ANY($1::<cast>)`` filter.

    OMOP ids usually fit a signed 64-bit integer, but two things break a naive
    ``$1::bigint[]`` cast:
      * NUMERIC id columns come back from asyncpg as ``Decimal`` objects, and
      * some sites use hash-derived ids that exceed the int64 range entirely
        (a 19-digit id can be up to ~9.9e18, past the ~9.2e18 int64 ceiling),
        which must therefore live in a NUMERIC column.

    So: coerce each id to ``int`` where possible and use ``bigint[]`` (which is
    index-friendly for the common bigint person_id column) when every value
    fits signed 64-bit; fall back to ``numeric[]`` only when one does not. The
    values are passed through in a form asyncpg can encode for the chosen cast
    (plain ``int`` for bigint[]; ``int``/``Decimal`` for numeric[]).
    """
    out = []
    needs_numeric = False
    for v in ids:
        try:
            iv = int(v)
        except (TypeError, ValueError):
            # Non-integral (shouldn't happen for ids) — let numeric[] carry it.
            needs_numeric = True
            out.append(v)
            continue
        if _Q_MIN <= iv <= _Q_MAX:
            out.append(iv)
        else:
            needs_numeric = True
            out.append(v)   # keep original value for numeric encoding
    return ("numeric[]" if needs_numeric else "bigint[]"), out


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
# Streaming cursor helper
# ---------------------------------------------------------------------------

async def iter_rows(conn, query: str, *args, batch_size: int = FETCH_BATCH):
    """Stream rows from a query via a server-side cursor, in constant memory.

    Only ``batch_size`` rows are held at once — ``cursor.fetch(batch_size)`` is
    what physically round-trips to the server — so a full-table scan never
    materialises the whole result set in Python. Must be called inside a
    transaction (an asyncpg requirement for cursors); this function opens and
    closes one itself.

    Args:
        conn: An asyncpg connection.
        query: The SQL to execute.
        *args: Positional query parameters.
        batch_size: Rows fetched from the server per round-trip.

    Yields:
        asyncpg ``Record`` objects, one at a time.

    Note:
        The transaction is driven manually rather than with
        ``async with conn.transaction()`` to stay correct when the consumer
        abandons the generator mid-stream (e.g. the merge loop raises
        ``MergeRowError`` and Python tears the ``async for`` down with
        ``GeneratorExit``). At that point the cursor still has unfetched
        batches on the wire; letting asyncpg's transaction exit issue a
        ROLLBACK on top of that dirty protocol state crashes with "got result
        for unknown protocol state 3", which would mask the original exception
        and break the streaming response. A context manager's ``__aexit__``
        runs before any surrounding ``except``, so instead the connection is
        forcibly terminated on ``GeneratorExit`` before any cleanup statement
        reaches the wire; the caller's outer ``finally`` tolerates a
        terminated connection and the original exception propagates cleanly.
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


# ---------------------------------------------------------------------------
# Error types
# ---------------------------------------------------------------------------

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
        self.sqlstate      = getattr(cause, "sqlstate", None)
        self.pg_message    = getattr(cause, "message", str(cause))
        self.pg_detail     = getattr(cause, "detail", None)
        self.pg_column     = getattr(cause, "column_name", None)
        self.pg_table      = getattr(cause, "table_name", None)
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
                        "column":     c,
                        "length":     vlen,
                        "max_length": max_len,
                        "preview":    short(v),
                    })

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


# ---------------------------------------------------------------------------
# Batch inserter
# ---------------------------------------------------------------------------

class BatchInserter:
    """Buffers rows for one table and flushes them with ``executemany``.

    Rows accumulate until ``INSERT_BATCH`` is reached (or ``flush()`` is called)
    and are then sent in a single round-trip. On error the batch is retried
    row-by-row inside a savepoint to isolate the offending row:

    * ``UniqueViolationError`` on a row is treated as a duplicate — counted as
      "skipped" rather than raised, so a pre-existing target row is a no-op.
    * Any other ``PostgresError`` is re-raised as ``MergeRowError`` with full
      context (table, SQL, column-to-value mapping, sqlstate, PG message).

    Attributes:
        inserted: Count of rows successfully inserted.
        skipped: Count of rows skipped as duplicates.
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
    person-table INSERT and the demographics-upsert UPDATE.
    """
    try:
        await conn.execute(sql, *values)
    except asyncpg.PostgresError as e:
        raise MergeRowError(
            table=table, sql=sql, cols=cols, values=values, cause=e,
        ) from e


# ---------------------------------------------------------------------------
# Supporting data structures
# ---------------------------------------------------------------------------

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

def _stringify_unsafe_ints(obj):
    """
    Recursively walk a JSON-bound structure and replace any int outside
    the JS-safe range with its string form. Booleans (which are ints in
    Python) are left alone. Other types pass through unchanged.
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
    """Build the source-to-target patient identity map.

    Patients are matched across the two databases by ``person_source_value``.
    Everything downstream is keyed on the returned map, so sampling here bounds
    the size of the whole scan and merge.

    Args:
        src: Source database connection.
        tgt: Target database connection.
        ss: Source schema name.
        ts: Target schema name.
        limit: If set, keep only the first ``limit`` source patients (ordered
            by ``person_id``), for sample runs.
        limit_to_existing: When sampling, restrict the sample to source
            patients whose ``person_source_value`` also exists in the target.

    Returns:
        A dict mapping each source ``person_id`` to::

            {
                "source_value": str | None,
                "target_person_id": int | None,   # None if not yet in target
                "target_found": bool,
                "match_type": str,
            }
    """
    tgt_persons = await tgt.fetch(
        f'SELECT person_id, person_source_value FROM "{ts}"."person"'
    )
    tgt_by_sv = {
        r["person_source_value"]: r["person_id"]
        for r in tgt_persons
        if r["person_source_value"]
    }

    if limit is not None and limit_to_existing and tgt_by_sv:
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
        sv = r["person_source_value"]
        tgt_pid = tgt_by_sv.get(sv) if sv else None
        if not sv:
            match_type = "unmatched_no_source_value"
        elif tgt_pid is not None:
            match_type = "matched_existing"
        else:
            match_type = "unmatched"
        person_map[r["person_id"]] = {
            "source_value":     sv,
            "target_person_id": tgt_pid,
            "target_found":     tgt_pid is not None,
            "match_type":       match_type,
        }
    return person_map


# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------

async def get_tgt_dedup_keys(tgt, ts, table_name, pfk, dedup_cols, tgt_cols, tgt_person_ids):
    """
    Build the set of existing target dedup keys for a table.

    Rows are pulled via a server-side cursor in batches of
    DEDUP_FETCH_BATCH so peak memory is bounded by the final size of the
    set rather than 2× (rows + set).
    """
    non_pk_dedup = [c for c in dedup_cols if c != pfk and c in tgt_cols]
    if not non_pk_dedup or not tgt_person_ids:
        return set(), non_pk_dedup
    col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
    keys: set = set()
    cast, id_params = _id_array_param(tgt_person_ids)
    sql = (
        f'SELECT "{pfk}", {col_list} FROM "{ts}"."{table_name}" '
        f'WHERE "{pfk}" = ANY($1::{cast})'
    )
    async for r in iter_rows(tgt, sql, id_params, batch_size=DEDUP_FETCH_BATCH):
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
    mode actually fast on huge clinical tables.

    The array cast is chosen by _id_array_param: bigint[] when every id fits
    signed 64-bit (the common, index-friendly case), numeric[] when a
    hash-derived id exceeds it or the column is NUMERIC (asyncpg returns those
    as Decimal). int[] is never used because BIGINT person_ids overflow int32.
    """
    if person_ids is None:
        return f'SELECT * FROM "{schema}"."{table}"', ()
    cast, id_params = _id_array_param(person_ids)
    return (
        f'SELECT * FROM "{schema}"."{table}" '
        f'WHERE "{pfk}" = ANY($1::{cast})',
        (id_params,),
    )


# ---------------------------------------------------------------------------
# Self-referencing FK second pass
# ---------------------------------------------------------------------------

async def apply_deferred_selfrefs(
    tgt, ts, table_name, self_pk, self_remaps, deferred, remapper,
    batch=20_000,
):
    """Resolve self-referencing FK columns in a second pass after insertion.

    Some tables reference another row of themselves, e.g.
    ``visit_occurrence.preceding_visit_occurrence_id`` and
    ``visit_detail.parent_visit_detail_id`` /
    ``preceding_visit_detail_id``. Because the source cursor yields rows in
    arbitrary order, a referenced row may not be inserted yet when the
    referencing row is processed, so remapping such a column inline would
    resolve forward references to NULL. Carrying the raw source id instead
    risks overflowing a narrower target column and aborting the insert.

    To avoid both, the insert pass writes these columns as NULL and stashes
    their source values in ``deferred``. By the time this runs the remapper
    holds the complete source-to-target id map for the table, so every
    in-scope reference resolves regardless of insertion order. References
    whose target row was not part of this merge (out of scope, filtered by
    sampling, or dedup-skipped) remain NULL, which is spec-valid.

    Each FK column is updated in batches of ``batch`` rows with one set-based
    statement — ``UPDATE ... SET col = d.tgt FROM unnest($1, $2) AS
    d(self_id, tgt) WHERE self_pk = d.self_id`` — giving ``n / batch``
    round-trips instead of one UPDATE per row.

    Args:
        tgt: Target database connection.
        ts: Target schema name.
        table_name: Table being updated.
        self_pk: The table's surrogate primary key column.
        self_remaps: Mapping of ``{self_referencing_column: id_map_name}``.
        deferred: ``{column: (self_ids, src_refs)}`` where each value is a pair
            of parallel ``array('q')`` buffers populated during the insert
            pass. Packed arrays keep even a few million deferred rows well
            within a bounded memory footprint.
        remapper: The populated ``IDRemapper`` for resolving references.
        batch: Rows per UPDATE round-trip.

    Yields:
        Human-readable progress strings for the caller to surface on the
        stream, then a final ``("__done__", remapped, left_null)`` sentinel
        tuple with the resolved and left-NULL counts.
    """
    remapped = 0
    left_null = 0

    for col, map_name in self_remaps.items():
        self_ids, src_refs = deferred.get(col, (None, None))
        n = 0 if self_ids is None else len(self_ids)
        if n == 0:
            continue

        sql = (
            f'UPDATE "{ts}"."{table_name}" AS v '
            f'SET "{col}" = d.tgt '
            f'FROM unnest($1::bigint[], $2::bigint[]) AS d(self_id, tgt) '
            f'WHERE v."{self_pk}" = d.self_id'
        )

        col_remapped = 0
        for start in range(0, n, batch):
            end = min(start + batch, n)
            ids_batch: list[int] = []
            tgt_batch: list[int] = []
            for i in range(start, end):
                tgt_ref = remapper.remap(map_name, src_refs[i])
                if tgt_ref is None:
                    left_null += 1
                    continue
                ids_batch.append(int(self_ids[i]))
                tgt_batch.append(int(tgt_ref))

            if not ids_batch:
                continue

            sp = f"sr_{start:x}"
            await tgt.execute(f"SAVEPOINT {sp}")
            try:
                await tgt.execute(sql, ids_batch, tgt_batch)
                await tgt.execute(f"RELEASE SAVEPOINT {sp}")
            except asyncpg.PostgresError as e:
                try:
                    await tgt.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    await tgt.execute(f"RELEASE SAVEPOINT {sp}")
                except Exception:
                    pass
                # A set-based UPDATE can't name the single offending row, so
                # surface the first pair of the failed batch as representative
                # context — enough for the structured error handler to render.
                raise MergeRowError(
                    table=table_name, sql=sql,
                    cols=[self_pk, col],
                    values=(ids_batch[0], tgt_batch[0]),
                    cause=e,
                ) from e

            remapped += len(ids_batch)
            col_remapped += len(ids_batch)
            yield (f"{table_name}.{col}: {col_remapped:,}/{n:,} "
                   f"self-references resolved…")

    yield ("__done__", remapped, left_null)


# ---------------------------------------------------------------------------
# Scan endpoint generator
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

            if table_name in ADMIN_TABLES:
                tgt_cols = await get_columns(tgt, ts, table_name)
                tgt_keys, non_pk_dedup = await get_admin_dedup_keys(
                    tgt, ts, table_name, dedup_cols, tgt_cols
                )
                new_rows_count = 0
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

            # Parent-scoped child tables (e.g. note_nlp). The scan is read-only
            # and has no id remapper, so the parent FK can't be resolved here;
            # we give an APPROXIMATE count by deduping on the non-parent key
            # columns only. The merge does the exact, parent-scoped dedup.
            if OMOP_TABLES[table_name].get("parent_fk"):
                parent_fk = OMOP_TABLES[table_name]["parent_fk"]
                tgt_cols = await get_columns(tgt, ts, table_name)
                scan_dedup = [c for c in dedup_cols
                              if c != parent_fk and c in tgt_cols]
                tgt_keys = set()
                if scan_dedup:
                    col_list = ", ".join(f'"{c}"' for c in scan_dedup)
                    async for r in iter_rows(
                        tgt, f'SELECT {col_list} FROM "{ts}"."{table_name}"',
                        batch_size=DEDUP_FETCH_BATCH,
                    ):
                        tgt_keys.add(tuple(r[c] for c in scan_dedup))
                new_rows_count = 0
                async for row in iter_rows(
                    src, f'SELECT * FROM "{ss}"."{table_name}"'
                ):
                    if scan_dedup:
                        key = tuple(row[c] for c in scan_dedup)
                        if key in tgt_keys:
                            continue
                    new_rows_count += 1
                table_totals[table_name] = {
                    "new_rows": new_rows_count, "affected_patients": 0,
                    "approximate": True,
                }
                yield emit("table_scan", table=table_name,
                           new_rows=new_rows_count, affected_patients=0)
                yield emit("log", level="info",
                           msg=f"{table_name}: ~{new_rows_count} new rows "
                               f"(approximate — scoped via {parent_fk} at merge time)")
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

            sample_ids = list(person_map.keys()) if limit is not None else None
            sql, args = build_clinical_source_query(ss, table_name, pfk, sample_ids)
            async for row in iter_rows(src, sql, *args):
                src_pid = row[pfk]
                if src_pid not in person_map:
                    continue
                info = person_map[src_pid]
                tgt_pid = info["target_person_id"] if info["target_found"] else None

                if existing_only and tgt_pid is None:
                    continue

                if tgt_pid is not None and non_pk_dedup:
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
        for c in (src, tgt):
            try:
                await c.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Merge endpoint generator
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
                meta       = OMOP_TABLES[table_name]
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
                    tgt_admin_keys, non_pk_dedup = await get_admin_dedup_keys(
                        tgt, ts, table_name, dedup_cols, tgt_cols
                    )
                    insert_cols = list(common_cols)
                    inserter = (
                        BatchInserter(tgt, ts, table_name, insert_cols)
                        if not cfg.dry_run else None
                    )
                    inserted = skipped = 0

                    async for row in iter_rows(
                        src, f'SELECT * FROM "{ss}"."{table_name}"'
                    ):
                        if non_pk_dedup:
                            key = tuple(row[c] for c in non_pk_dedup)
                            if key in tgt_admin_keys:
                                skipped += 1
                                continue

                        row_data = dict(row)
                        remapper.apply_row(row_data, fk_remaps)

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

                # ---- PARENT-SCOPED CHILD TABLES ----------------------------
                # Tables with no person_fk that hang off another table by a FK
                # rather than off the patient — e.g. note_nlp -> note. They are
                # scoped THROUGH their parent: a row is kept only if its parent
                # FK remaps to a row imported/matched earlier in this run (the
                # parent table has a higher-priority insert_order). A row whose
                # parent did not map (out of scope, or a parent that was
                # dedup-skipped and so never recorded) is skipped rather than
                # inserted with a dangling NULL FK.
                parent_fk = meta.get("parent_fk")
                if parent_fk:
                    parent_map = fk_remaps.get(parent_fk)
                    if parent_map is None:
                        yield emit("log", level="warn",
                                   msg=f"{table_name}: parent_fk '{parent_fk}' "
                                       f"has no fk_remaps entry — skipping table")
                        continue

                    # Target dedup keys (already in target id space). The key
                    # may include the parent FK; the target already stores it as
                    # a target id, and the source side is remapped below, so the
                    # two compare in the same space.
                    non_pk_dedup = (
                        [c for c in dedup_cols if c in tgt_cols]
                        if cfg.dedup_enabled else []
                    )
                    tgt_keys: set = set()
                    if non_pk_dedup:
                        col_list = ", ".join(f'"{c}"' for c in non_pk_dedup)
                        async for r in iter_rows(
                            tgt,
                            f'SELECT {col_list} FROM "{ts}"."{table_name}"',
                            batch_size=DEDUP_FETCH_BATCH,
                        ):
                            tgt_keys.add(tuple(r[c] for c in non_pk_dedup))

                    if self_pk and cfg.id_strategy == "auto":
                        await pk_counter.init_table(tgt, ts, table_name, self_pk)

                    insert_cols = list(common_cols)
                    inserter = (
                        BatchInserter(tgt, ts, table_name, insert_cols)
                        if not cfg.dry_run else None
                    )
                    inserted = skipped = 0

                    async for row in iter_rows(
                        src, f'SELECT * FROM "{ss}"."{table_name}"'
                    ):
                        row_data = dict(row)
                        remapper.apply_row(row_data, fk_remaps)

                        # Scope via the parent: unmapped parent -> skip (no orphan).
                        if row_data.get(parent_fk) is None:
                            skipped += 1
                            continue

                        if non_pk_dedup:
                            key = tuple(row_data.get(c) for c in non_pk_dedup)
                            if key in tgt_keys:
                                skipped += 1
                                continue

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
                                   f"{skipped} skipped (scoped via {parent_fk})")
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

                if self_pk and cfg.id_strategy == "auto":
                    await pk_counter.init_table(tgt, ts, table_name, self_pk)

                insert_cols = list(common_cols)

                # Self-referencing FK columns (map target == this table) are
                # deferred to a post-insert UPDATE pass — see
                # apply_deferred_selfrefs for why. Everything else is remapped
                # inline as before.
                self_remaps   = {c: m for c, m in fk_remaps.items()
                                 if m == table_name and c in insert_cols}
                inline_remaps = {c: m for c, m in fk_remaps.items()
                                 if m != table_name}
                # Per self-ref column: two parallel int64 arrays,
                # (target self_pk id, source ref value). 8 bytes each vs the
                # ~250 bytes/row a list-of-dicts costs — keeps a multi-million
                # row visit table's second-pass buffer under ~100 MB instead
                # of tipping the process into swap.
                deferred_selfref = {
                    c: (array("q"), array("q")) for c in self_remaps
                }

                inserter = (
                    BatchInserter(tgt, ts, table_name, insert_cols)
                    if not cfg.dry_run else None
                )
                inserted = skipped = 0
                staged = 0

                sample_ids = list(person_map.keys()) if limit is not None else None
                sql, args = build_clinical_source_query(
                    ss, table_name, pfk, sample_ids
                )
                scanned = 0
                async for row in iter_rows(src, sql, *args):
                    scanned += 1
                    if scanned % 250_000 == 0:
                        yield emit("log", level="info",
                                   msg=f"{table_name}: {scanned:,} source rows "
                                       f"scanned, {staged:,} staged so far…")
                    src_pid = row[pfk]
                    if src_pid not in person_map:
                        continue
                    tgt_pid = person_map[src_pid]["target_person_id"]
                    if tgt_pid is None:
                        skipped += 1
                        continue

                    row_data = dict(row)
                    row_data[pfk] = tgt_pid
                    remapper.apply_row(row_data, inline_remaps)

                    # Stash self-ref source values and NULL the column for the
                    # insert; resolved in the second pass once the full
                    # source->target map for this table exists.
                    self_src = {}
                    for col in self_remaps:
                        if col in row_data:
                            v = row_data[col]
                            row_data[col] = None
                            if v is not None:
                                self_src[col] = v

                    if cfg.dedup_enabled and non_pk_dedup:
                        key = tuple(
                            [tgt_pid] + [row_data.get(c) for c in non_pk_dedup]
                        )
                        if key in tgt_dedup_keys:
                            skipped += 1
                            continue

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

                        # Only worth stashing for a live run — dry runs insert
                        # nothing, so there is no row to UPDATE afterwards.
                        if self_src and not cfg.dry_run:
                            for col, v in self_src.items():
                                ids_arr, refs_arr = deferred_selfref[col]
                                # Coerce NUMERIC->Decimal to int, and upgrade
                                # the int64 array to a list if an id exceeds
                                # bigint range (see _append_id). The remapper
                                # is already int-tolerant on lookup, including
                                # for values beyond 2^63.
                                ids_arr  = _append_id(ids_arr,  int(new_self_id))
                                refs_arr = _append_id(refs_arr, int(v))
                                deferred_selfref[col] = (ids_arr, refs_arr)

                    # Dedup semantics: `tgt_dedup_keys` holds only rows that
                    # existed in the target before this run, and a row's key is
                    # deliberately NOT added to the set after inserting. Clinical
                    # OMOP data legitimately contains many rows sharing a coarse
                    # key such as (person_id, concept_id, date) — daily vitals,
                    # repeated lab panels, ICU monitoring — so collapsing within
                    # a single run would drop genuine records. Dedup therefore
                    # protects against re-importing what the target already had,
                    # not against repeats within one source extract.

                    if cfg.dry_run:
                        inserted += 1
                        staged += 1
                    else:
                        staged += 1
                        await inserter.add(
                            tuple(row_data.get(c) for c in insert_cols)
                        )

                if inserter is not None:
                    ins, skp = await inserter.flush()
                    inserted += ins
                    skipped  += skp

                # Second pass: resolve self-referencing FKs now that this
                # table's full source->target id map is known. The helper is
                # a generator so its progress can be streamed — the insert
                # phase and this phase are otherwise silent within a table.
                if self_remaps and not cfg.dry_run and any(
                    len(ids) for ids, _ in deferred_selfref.values()
                ):
                    remapped = left_null = 0
                    async for item in apply_deferred_selfrefs(
                        tgt, ts, table_name, self_pk,
                        self_remaps, deferred_selfref, remapper,
                    ):
                        if isinstance(item, tuple) and item and item[0] == "__done__":
                            _, remapped, left_null = item
                        else:
                            yield emit("log", level="info", msg=item)
                    if remapped or left_null:
                        detail = f"{remapped:,} resolved"
                        if left_null:
                            detail += f", {left_null:,} left NULL (referent out of scope)"
                        yield emit("log", level="ok",
                                   msg=f"{table_name}: self-referencing FKs — {detail}")

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
                if mt == "unmatched" and existing_only:
                    mt = "source_only_skipped"
                audit_counts[mt] = audit_counts.get(mt, 0) + 1
                if len(person_audit) < PERSON_AUDIT_CAP:
                    person_audit.append({
                        "source_person_id":    src_pid,
                        "target_person_id":    info["target_person_id"],
                        "person_source_value": info["source_value"],
                        "match_type":          mt,
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