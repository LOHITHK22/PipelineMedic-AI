from __future__ import annotations

from pydantic import BaseModel

from app.db.base import SessionLocal
from app.db.models import SchemaVersion
from app.detectors.schema_drift import diff_schemas
from app.models.schemas import ToolRiskLevel
from app.tools.base import BaseTool


class GetDatabaseSchemaInput(BaseModel):
    table_name: str


class GetDatabaseSchemaOutput(BaseModel):
    table_name: str
    columns: list[dict]
    error: str | None = None


class GetDatabaseSchemaTool(BaseTool):
    name = "get_database_schema"
    risk_level = ToolRiskLevel.LOW
    input_model = GetDatabaseSchemaInput
    output_model = GetDatabaseSchemaOutput

    def _execute(self, tool_input: GetDatabaseSchemaInput) -> GetDatabaseSchemaOutput:
        from sqlalchemy import inspect
        from app.db.base import engine

        try:
            inspector = inspect(engine)
            cols = inspector.get_columns(tool_input.table_name)
            columns = [{"name": c["name"], "type": str(c["type"]), "nullable": c["nullable"]} for c in cols]
            return GetDatabaseSchemaOutput(table_name=tool_input.table_name, columns=columns)
        except Exception as e:
            return GetDatabaseSchemaOutput(table_name=tool_input.table_name, columns=[], error=str(e))


class GetSchemaVersionsInput(BaseModel):
    subject: str


class GetSchemaVersionsOutput(BaseModel):
    subject: str
    versions: list[dict]


class GetSchemaVersionsTool(BaseTool):
    name = "get_schema_versions"
    risk_level = ToolRiskLevel.LOW
    input_model = GetSchemaVersionsInput
    output_model = GetSchemaVersionsOutput

    def _execute(self, tool_input: GetSchemaVersionsInput) -> GetSchemaVersionsOutput:
        db = SessionLocal()
        try:
            rows = (
                db.query(SchemaVersion)
                .filter(SchemaVersion.subject == tool_input.subject)
                .order_by(SchemaVersion.version.asc())
                .all()
            )
            versions = [{"version": r.version, "schema": r.schema_json, "created_at": str(r.created_at)} for r in rows]
            return GetSchemaVersionsOutput(subject=tool_input.subject, versions=versions)
        finally:
            db.close()


class CompareSchemaVersionsInput(BaseModel):
    subject: str
    old_version: int
    new_version: int


class CompareSchemaVersionsOutput(BaseModel):
    subject: str
    diff: dict
    error: str | None = None


class CompareSchemaVersionsTool(BaseTool):
    name = "compare_schema_versions"
    risk_level = ToolRiskLevel.LOW
    input_model = CompareSchemaVersionsInput
    output_model = CompareSchemaVersionsOutput

    def _execute(self, tool_input: CompareSchemaVersionsInput) -> CompareSchemaVersionsOutput:
        db = SessionLocal()
        try:
            old = db.query(SchemaVersion).filter_by(subject=tool_input.subject, version=tool_input.old_version).first()
            new = db.query(SchemaVersion).filter_by(subject=tool_input.subject, version=tool_input.new_version).first()
            if not old or not new:
                return CompareSchemaVersionsOutput(subject=tool_input.subject, diff={}, error="version not found")
            diff = diff_schemas(tool_input.subject, old.schema_json, new.schema_json, tool_input.old_version, tool_input.new_version)
            return CompareSchemaVersionsOutput(subject=tool_input.subject, diff=diff.model_dump())
        finally:
            db.close()
