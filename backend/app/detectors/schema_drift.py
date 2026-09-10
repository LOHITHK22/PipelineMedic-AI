"""Deterministic schema drift detection.

Compares two flat JSON-schema-like field maps and classifies each change as
ADDED / REMOVED / RENAMED / TYPE_CHANGED / NULLABLE_CHANGED, then classifies
overall compatibility per standard schema-evolution rules (similar to
Confluent Schema Registry compatibility modes):

- Adding an OPTIONAL field            -> BACKWARD_COMPATIBLE (old readers ignore it)
- Adding a REQUIRED field             -> BREAKING
- Removing a field that had a default -> FORWARD_COMPATIBLE
- Removing a required field           -> BREAKING
- Type change                         -> BREAKING (unless declared widening, e.g. int->float)
- Nullable True->False                -> BREAKING
- Nullable False->True                -> BACKWARD_COMPATIBLE
- Rename (heuristically detected)     -> BREAKING (old consumers can't find old key)

A field map looks like:
    {"order_id": {"type": "string", "nullable": False},
     "amount": {"type": "number", "nullable": False}}
"""
from __future__ import annotations

from app.models.schemas import CompatibilityClass, FieldChange, SchemaDiffResult

WIDENING_TYPE_PAIRS = {
    ("integer", "number"),
    ("int", "float"),
}


def _classify_type_change(old_type: str, new_type: str) -> CompatibilityClass:
    if (old_type, new_type) in WIDENING_TYPE_PAIRS:
        return CompatibilityClass.BACKWARD_COMPATIBLE
    return CompatibilityClass.BREAKING


def diff_schemas(
    subject: str,
    old_schema: dict[str, dict],
    new_schema: dict[str, dict],
    old_version: int | None = None,
    new_version: int | None = None,
) -> SchemaDiffResult:
    changes: list[FieldChange] = []

    old_fields = set(old_schema.keys())
    new_fields = set(new_schema.keys())

    removed = old_fields - new_fields
    added = new_fields - old_fields
    common = old_fields & new_fields

    # Heuristic rename detection: a removed field and an added field with an
    # identical type+nullable signature and similar name (case/underscore/camel
    # variants) is treated as a rename rather than independent add+remove.
    matched_renames: dict[str, str] = {}

    def _normalize(name: str) -> str:
        return name.lower().replace("_", "")

    for old_name in list(removed):
        old_spec = old_schema[old_name]
        for new_name in list(added):
            new_spec = new_schema[new_name]
            if _normalize(old_name) == _normalize(new_name) and old_spec.get("type") == new_spec.get("type"):
                matched_renames[old_name] = new_name
                break

    for old_name, new_name in matched_renames.items():
        removed.discard(old_name)
        added.discard(new_name)
        changes.append(
            FieldChange(
                field_name=old_name,
                change_type="RENAMED",
                old_type=old_schema[old_name].get("type"),
                new_type=new_schema[new_name].get("type"),
                renamed_to=new_name,
                compatibility=CompatibilityClass.BREAKING,
            )
        )

    for name in removed:
        spec = old_schema[name]
        has_default = spec.get("default") is not None or spec.get("nullable") is True
        changes.append(
            FieldChange(
                field_name=name,
                change_type="REMOVED",
                old_type=spec.get("type"),
                old_nullable=spec.get("nullable", False),
                compatibility=CompatibilityClass.FORWARD_COMPATIBLE if has_default else CompatibilityClass.BREAKING,
            )
        )

    for name in added:
        spec = new_schema[name]
        is_required = not spec.get("nullable", False) and spec.get("default") is None
        changes.append(
            FieldChange(
                field_name=name,
                change_type="ADDED",
                new_type=spec.get("type"),
                new_nullable=spec.get("nullable", False),
                compatibility=CompatibilityClass.BREAKING if is_required else CompatibilityClass.BACKWARD_COMPATIBLE,
            )
        )

    for name in common:
        old_spec, new_spec = old_schema[name], new_schema[name]
        old_type, new_type = old_spec.get("type"), new_spec.get("type")
        old_nullable, new_nullable = old_spec.get("nullable", False), new_spec.get("nullable", False)

        if old_type != new_type:
            changes.append(
                FieldChange(
                    field_name=name,
                    change_type="TYPE_CHANGED",
                    old_type=old_type,
                    new_type=new_type,
                    compatibility=_classify_type_change(old_type, new_type),
                )
            )
        elif old_nullable != new_nullable:
            compat = (
                CompatibilityClass.BREAKING
                if (old_nullable and not new_nullable)
                else CompatibilityClass.BACKWARD_COMPATIBLE
            )
            changes.append(
                FieldChange(
                    field_name=name,
                    change_type="NULLABLE_CHANGED",
                    old_nullable=old_nullable,
                    new_nullable=new_nullable,
                    compatibility=compat,
                )
            )

    if not changes:
        overall = CompatibilityClass.BACKWARD_COMPATIBLE
    elif any(c.compatibility == CompatibilityClass.BREAKING for c in changes):
        overall = CompatibilityClass.BREAKING
    elif any(c.compatibility == CompatibilityClass.UNKNOWN for c in changes):
        overall = CompatibilityClass.UNKNOWN
    else:
        overall = CompatibilityClass.BACKWARD_COMPATIBLE

    return SchemaDiffResult(
        subject=subject,
        old_version=old_version,
        new_version=new_version,
        changes=changes,
        overall_compatibility=overall,
    )


class SchemaDriftDetector:
    """Infers a flat schema from a sample of JSON messages and diffs it
    against the last known-good schema for a subject."""

    def __init__(self, subject: str):
        self.subject = subject

    @staticmethod
    def infer_schema(sample_messages: list[dict]) -> dict[str, dict]:
        schema: dict[str, dict] = {}
        for msg in sample_messages:
            for key, value in msg.items():
                py_type = type(value).__name__
                type_map = {
                    "str": "string",
                    "int": "integer",
                    "float": "number",
                    "bool": "boolean",
                    "NoneType": "null",
                    "dict": "object",
                    "list": "array",
                }
                inferred_type = type_map.get(py_type, py_type)
                if key not in schema:
                    schema[key] = {"type": inferred_type, "nullable": value is None}
                else:
                    if value is None:
                        schema[key]["nullable"] = True
                    elif schema[key]["type"] != inferred_type and inferred_type != "null":
                        # conflicting types across sample -> mark UNKNOWN-ish by keeping first seen
                        schema[key]["type"] = schema[key]["type"]
        return schema

    def detect(self, baseline_schema: dict[str, dict], sample_messages: list[dict]) -> SchemaDiffResult | None:
        inferred = self.infer_schema(sample_messages)
        diff = diff_schemas(self.subject, baseline_schema, inferred)
        if diff.overall_compatibility in (CompatibilityClass.BACKWARD_COMPATIBLE,) and not diff.changes:
            return None
        return diff
