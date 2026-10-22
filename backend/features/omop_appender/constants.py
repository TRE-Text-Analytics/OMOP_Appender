"""
OMOP table metadata and runtime tunables for the merge feature.

Table metadata is NOT defined here any more. It lives in an external YAML
file — omop_appender_config.yaml in this folder — which is the single source
of truth, so the table definitions are transparent and tunable without editing
code. That file documents every field (person_fk, self_pk, fk_remaps,
insert_order, dedup_cols, …) and the reasoning behind each dedup key.

This module loads and VALIDATES that file at import time and exposes
OMOP_TABLES and ADMIN_TABLES exactly as before, so the rest of the package
(logic.py, router.py) is unchanged. Because the file is authoritative, a
missing or malformed file is a hard error at startup rather than a silent
fallback — there is no in-code copy to fall back to.

The streaming / batching tunables stay in code (bottom of this file): they are
process-level knobs, not per-table configuration.

Requires PyYAML (`pip install pyyaml`).
"""

import os
import sys
import yaml


# ---------------------------------------------------------------------------
# Table metadata — loaded from omop_appender_config.yaml
# ---------------------------------------------------------------------------
# The config lives beside the feature's Python files (this omop_appender
# folder), not at the server's working directory — so it is found no matter
# where the app is launched from. An absolute OMOP_APPENDER_CONFIG env var
# overrides this (e.g. a per-deployment config elsewhere on disk).
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "omop_appender_config.yaml"
)

# Required keys for every table entry, and the types each accepts. Validation
# is strict because this file is authoritative: a missing or mistyped field can
# break referential integrity (a wrong person_fk / insert_order / fk_remaps),
# not merely dedup, so we refuse to start rather than merge with bad metadata.
#   None-or-str : person_fk, self_pk, visit_fk   (a column name, or null)
#   str         : label, domain, description
#   int         : insert_order
#   dict[str,str]: fk_remaps   (column_name -> id-map name)
#   list[str]   : dedup_cols   (non-empty)
_STR_FIELDS      = ("label", "domain", "description")
_NULLABLE_STR    = ("person_fk", "self_pk", "visit_fk")
_REQUIRED_FIELDS = _STR_FIELDS + _NULLABLE_STR + (
    "fk_remaps", "dedup_cols", "insert_order",
)
# Optional fields a table may also declare:
#   parent_fk  A column (also listed in fk_remaps) by which a non-per-patient
#              child table is scoped through its parent — e.g. note_nlp.note_id
#              -> note. Rows whose parent FK does not remap are skipped at merge
#              time instead of being inserted with a dangling FK.
_OPTIONAL_FIELDS = ("parent_fk",)


def _fail(msg: str):
    raise RuntimeError(f"OMOP Appender: {msg}")


def _validate_table(name: str, meta) -> None:
    """Validate one table entry's shape and types, raising on any problem."""
    if not isinstance(meta, dict):
        _fail(f"table '{name}' must be a mapping, got {type(meta).__name__}")

    missing = [k for k in _REQUIRED_FIELDS if k not in meta]
    if missing:
        _fail(f"table '{name}' is missing required field(s): {', '.join(missing)}")
    allowed = _REQUIRED_FIELDS + _OPTIONAL_FIELDS
    unknown = [k for k in meta if k not in allowed]
    if unknown:
        _fail(f"table '{name}' has unknown field(s): {', '.join(unknown)}")

    for k in _STR_FIELDS:
        if not isinstance(meta[k], str):
            _fail(f"table '{name}' field '{k}' must be a string")
    for k in _NULLABLE_STR:
        if not (meta[k] is None or isinstance(meta[k], str)):
            _fail(f"table '{name}' field '{k}' must be a column name (string) or null")

    # bool is a subclass of int — exclude it explicitly.
    if not isinstance(meta["insert_order"], int) or isinstance(meta["insert_order"], bool):
        _fail(f"table '{name}' field 'insert_order' must be an integer")

    fkr = meta["fk_remaps"]
    if not isinstance(fkr, dict) or not all(
        isinstance(col, str) and isinstance(mapname, str)
        for col, mapname in fkr.items()
    ):
        _fail(f"table '{name}' field 'fk_remaps' must be a mapping of "
              f"column name -> id-map name (all strings)")

    dc = meta["dedup_cols"]
    if not isinstance(dc, list) or not dc or not all(isinstance(c, str) for c in dc):
        _fail(f"table '{name}' field 'dedup_cols' must be a non-empty list of "
              f"column-name strings (got {dc!r})")

    pfk = meta.get("parent_fk")
    if pfk is not None:
        if not isinstance(pfk, str):
            _fail(f"table '{name}' field 'parent_fk' must be a column-name string")
        if pfk not in fkr:
            _fail(f"table '{name}' field 'parent_fk' ('{pfk}') must also appear in "
                  f"fk_remaps so it can be remapped to the target parent row")


def load_omop_tables(path=None) -> dict:
    """Load and validate the table metadata from the YAML config.

    Args:
        path: Explicit config path. Defaults to the ``OMOP_APPENDER_CONFIG``
            environment variable, then ``DEFAULT_CONFIG_PATH``.

    Returns:
        The ``{table_name: metadata}`` mapping.

    Raises:
        RuntimeError: If the file is absent, unreadable, not valid YAML, or
            structurally invalid. There is deliberately no in-code fallback,
            so any problem fails loudly at startup rather than merging with
            incomplete metadata.
    """
    cfg_path = path or os.environ.get("OMOP_APPENDER_CONFIG", DEFAULT_CONFIG_PATH)

    try:
        with open(cfg_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except FileNotFoundError:
        _fail(f"table config not found at '{cfg_path}'. This file is the single "
              f"source of truth for table metadata and is required to start. "
              f"Restore omop_appender_config.yaml in the omop_appender folder, "
              f"or set OMOP_APPENDER_CONFIG to point at a copy.")
    except OSError as e:
        _fail(f"could not read table config '{cfg_path}': {e}")
    except yaml.YAMLError as e:
        _fail(f"table config '{cfg_path}' is not valid YAML: {e}")

    if not isinstance(raw, dict) or "tables" not in raw:
        _fail(f"table config '{cfg_path}' must be a mapping with a top-level "
              f"'tables:' section")

    tables = raw["tables"]
    if not isinstance(tables, dict) or not tables:
        _fail(f"'tables' in '{cfg_path}' must be a non-empty mapping of "
              f"table name -> metadata")

    for name, meta in tables.items():
        _validate_table(name, meta)

    return dict(tables)


OMOP_TABLES = load_omop_tables()

# Tables that have no person_fk AND are not parent-scoped child tables — i.e.
# true admin/reference tables (care_site, provider). A parent-scoped child such
# as note_nlp also has no person_fk but must NOT take the admin path (that path
# cannot remap its parent FK), so it is excluded here and handled by its own
# branch in logic.py.
ADMIN_TABLES = {
    name for name, m in OMOP_TABLES.items()
    if m["person_fk"] is None and not m.get("parent_fk")
}

print(
    f"OMOP Appender: loaded {len(OMOP_TABLES)} table definitions from config "
    f"({os.environ.get('OMOP_APPENDER_CONFIG', DEFAULT_CONFIG_PATH)})",
    file=sys.stderr, flush=True,
)


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

# JavaScript Number is IEEE 754 double precision: only ints with absolute
# value < 2^53 round-trip through JSON.parse without precision loss.
JS_SAFE_INT_MAX =  (1 << 53) - 1   # 9_007_199_254_740_991
JS_SAFE_INT_MIN = -(1 << 53) + 1   # -9_007_199_254_740_991