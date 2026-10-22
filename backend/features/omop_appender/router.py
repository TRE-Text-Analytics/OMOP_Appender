"""
FastAPI router for the OMOP merge feature.

Registers four routes under /api/omop-appender/:
  GET  /api/omop-appender/tables          — list all known OMOP tables and their metadata
  POST /api/omop-appender/test-connection — verify a DB connection and schema existence
  POST /api/omop-appender/scan            — stream a diff scan (NDJSON)
  POST /api/omop-appender/merge           — stream a merge run (NDJSON)
"""

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from .constants import OMOP_TABLES
from .logic import diff_scan, open_conn, run_merge
from .models import ConnectionTestRequest, MergeConfig, ScanRequest

router = APIRouter()


@router.get("/api/omop-appender/tables")
async def list_tables():
    return {
        name: {k: v for k, v in m.items() if k != "insert_order"}
        for name, m in OMOP_TABLES.items()
    }


@router.post("/api/omop-appender/test-connection")
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
    except Exception as e:
        return {"ok": False, "error": str(e)}


@router.post("/api/omop-appender/scan")
async def scan(req: ScanRequest):
    return StreamingResponse(diff_scan(req), media_type="application/x-ndjson")


@router.post("/api/omop-appender/merge")
async def merge(cfg: MergeConfig):
    return StreamingResponse(run_merge(cfg), media_type="application/x-ndjson")
