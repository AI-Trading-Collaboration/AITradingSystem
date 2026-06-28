from __future__ import annotations

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.minimal_forward_diagnostic import (
    FORBIDDEN_FIELDS,
    validate_minimal_forward_diagnostic_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = PROJECT_ROOT / "config" / "research" / "minimal_forward_diagnostic_policy.yaml"
SCHEMA_PATH = (
    PROJECT_ROOT / "config" / "research" / "minimal_forward_diagnostic_log_schema.yaml"
)


def test_minimal_forward_diagnostic_policy_stays_disabled() -> None:
    policy = _load(POLICY_PATH)
    schema = _load(SCHEMA_PATH)
    result = validate_minimal_forward_diagnostic_policy(policy, schema)

    assert result["status"] == "PASS"
    assert result["enabled"] is False
    assert result["requires_owner_approval"] is True
    assert FORBIDDEN_FIELDS <= set(policy["blocked_fields"])
    assert FORBIDDEN_FIELDS <= set(schema["forbidden_fields"])
    assert not (FORBIDDEN_FIELDS & set(policy["allowed_fields"]))


def test_minimal_forward_diagnostic_rejects_policy_that_allows_weights() -> None:
    policy = _load(POLICY_PATH)
    schema = _load(SCHEMA_PATH)
    policy = dict(policy)
    policy["allowed_fields"] = [*policy["allowed_fields"], "target_weights"]

    result = validate_minimal_forward_diagnostic_policy(policy, schema)

    assert result["status"] == "FAIL"
    assert "allowed_fields_include_forbidden_fields" in result["issues"]


def _load(path):
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
