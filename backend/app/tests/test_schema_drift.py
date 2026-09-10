from app.detectors.schema_drift import diff_schemas
from app.models.schemas import CompatibilityClass


BASELINE = {
    "event_id": {"type": "string", "nullable": False},
    "order_id": {"type": "string", "nullable": False},
    "customer_id": {"type": "string", "nullable": False},
    "amount": {"type": "number", "nullable": False},
    "currency": {"type": "string", "nullable": False},
    "event_time": {"type": "string", "nullable": False},
}


def test_no_change_is_backward_compatible():
    diff = diff_schemas("orders.raw", BASELINE, dict(BASELINE))
    assert diff.overall_compatibility == CompatibilityClass.BACKWARD_COMPATIBLE
    assert diff.changes == []


def test_rename_is_breaking():
    new_schema = dict(BASELINE)
    del new_schema["customer_id"]
    new_schema["customerId"] = {"type": "string", "nullable": False}
    diff = diff_schemas("orders.raw", BASELINE, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BREAKING
    rename_changes = [c for c in diff.changes if c.change_type == "RENAMED"]
    assert len(rename_changes) == 1
    assert rename_changes[0].field_name == "customer_id"
    assert rename_changes[0].renamed_to == "customerId"


def test_type_narrowing_is_breaking():
    new_schema = dict(BASELINE)
    new_schema["amount"] = {"type": "string", "nullable": False}
    diff = diff_schemas("orders.raw", BASELINE, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BREAKING
    type_changes = [c for c in diff.changes if c.change_type == "TYPE_CHANGED"]
    assert type_changes[0].compatibility == CompatibilityClass.BREAKING


def test_type_widening_is_backward_compatible():
    new_schema = dict(BASELINE)
    new_schema["amount"] = {"type": "number", "nullable": False}
    old_schema = dict(BASELINE)
    old_schema["amount"] = {"type": "integer", "nullable": False}
    diff = diff_schemas("orders.raw", old_schema, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BACKWARD_COMPATIBLE


def test_adding_optional_field_is_backward_compatible():
    new_schema = dict(BASELINE)
    new_schema["discount_code"] = {"type": "string", "nullable": True}
    diff = diff_schemas("orders.raw", BASELINE, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BACKWARD_COMPATIBLE


def test_adding_required_field_is_breaking():
    new_schema = dict(BASELINE)
    new_schema["tax_id"] = {"type": "string", "nullable": False}
    diff = diff_schemas("orders.raw", BASELINE, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BREAKING


def test_removing_required_field_is_breaking():
    new_schema = dict(BASELINE)
    del new_schema["currency"]
    diff = diff_schemas("orders.raw", BASELINE, new_schema)
    assert diff.overall_compatibility == CompatibilityClass.BREAKING


def test_nullable_tightening_is_breaking():
    old_schema = dict(BASELINE)
    old_schema["currency"] = {"type": "string", "nullable": True}
    new_schema = dict(BASELINE)
    new_schema["currency"] = {"type": "string", "nullable": False}
    diff = diff_schemas("orders.raw", old_schema, new_schema)
    nullable_changes = [c for c in diff.changes if c.change_type == "NULLABLE_CHANGED"]
    assert nullable_changes[0].compatibility == CompatibilityClass.BREAKING
