"""
Pydantic models for the OMOP merge feature.
"""

from pydantic import BaseModel


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
    patient_scope: str = "existing_and_new"
    patient_limit: int | None = None


class ConnectionTestRequest(BaseModel):
    config: DBConfig


class ScanRequest(BaseModel):
    source: DBConfig
    target: DBConfig
    tables: list[str]
    patient_scope: str = "existing_and_new"
    patient_limit: int | None = None
