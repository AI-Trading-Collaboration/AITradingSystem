from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from math import nan
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.us_equity_special_closure_policy import (
    DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH,
    US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH,
    load_us_equity_special_closure_policy,
)


def _base_policy_payload() -> dict[str, Any]:
    payload = yaml.safe_load(
        DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.read_text(encoding="utf-8")
    )
    assert isinstance(payload, dict)
    return payload


def _write_policy(tmp_path: Path, payload: object) -> Path:
    policy_path = tmp_path / "special_closures.yaml"
    policy_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return policy_path


def test_reviewed_policy_exposes_exact_receipt_binding_metadata() -> None:
    policy_path = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.resolve()
    policy = load_us_equity_special_closure_policy(policy_path)

    assert US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH.as_posix() == (
        "config/data/us_equity_special_closure_registry.yaml"
    )
    assert policy.policy_id == "us_equity_special_closure_registry"
    assert policy.policy_version == "1.0.0"
    assert policy.schema_version == "us_equity_special_closure_registry.v1"
    assert policy.status == "reviewed_active"
    assert policy.calendar_id == "XNYS"
    assert policy.path == policy_path
    assert policy.sha256 == sha256(policy_path.read_bytes()).hexdigest()
    assert policy.closures[0].source.publisher == "New York Stock Exchange"
    assert policy.closures[0].source.url.startswith("https://www.nyse.com/")


def test_policy_rejects_duplicate_calendar_date(tmp_path: Path) -> None:
    payload = _base_policy_payload()
    payload["closures"].append(deepcopy(payload["closures"][0]))

    with pytest.raises(ValueError, match="duplicate calendar_id/date"):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", "unknown.v2", "unsupported.*schema_version"),
        ("policy_id", "unknown_policy", "unknown.*policy_id"),
        ("policy_version", "version-one", "policy_version must be semantic"),
        ("status", "draft", "reviewed_active"),
        ("calendar_id", "CBOE", "unknown.*calendar_id"),
    ],
)
def test_policy_rejects_unknown_governance_values(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    payload = _base_policy_payload()
    payload[field] = value

    with pytest.raises(ValueError, match=message):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))


def test_policy_rejects_unknown_fields(tmp_path: Path) -> None:
    payload = _base_policy_payload()
    payload["silent_override"] = True

    with pytest.raises(ValueError, match="unknown fields: silent_override"):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))


def test_policy_rejects_duplicate_yaml_mapping_keys(tmp_path: Path) -> None:
    policy_path = tmp_path / "duplicate_key.yaml"
    policy_text = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.read_text(encoding="utf-8")
    policy_path.write_text(
        policy_text.replace(
            "status: reviewed_active",
            "status: reviewed_active\nstatus: reviewed_active",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as caught:
        load_us_equity_special_closure_policy(policy_path)
    assert str(caught.value) == (
        f"unable to load US equity special-closure policy: {policy_path.resolve()}"
    )


@pytest.mark.parametrize(
    "policy_text",
    [
        "closures: [",
        'value: !!python/object/apply:os.system ["echo hi"]\n',
        "? [a, b]\n: value\n",
        "schema_version: &root\n  self: *root\n",
    ],
)
def test_policy_preserves_wrapped_yaml_parse_failures(
    tmp_path: Path,
    policy_text: str,
) -> None:
    policy_path = tmp_path / "invalid.yaml"
    policy_path.write_text(policy_text, encoding="utf-8")

    with pytest.raises(ValueError) as caught:
        load_us_equity_special_closure_policy(policy_path)
    assert str(caught.value) == (
        f"unable to load US equity special-closure policy: {policy_path.resolve()}"
    )


def test_policy_preserves_yaml_merge_flattening(tmp_path: Path) -> None:
    policy_text = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.read_text(encoding="utf-8")
    merged_text = policy_text.replace(
        "source_requirements:\n"
        "  accepted_source_classes:\n"
        "    - official_exchange_notice\n"
        "  accepted_https_hosts:\n"
        "    - www.nyse.com\n",
        "source_requirements:\n"
        "  <<: &source_requirements\n"
        "    accepted_source_classes:\n"
        "      - official_exchange_notice\n"
        "    accepted_https_hosts:\n"
        "      - www.nyse.com\n",
    )
    assert merged_text != policy_text
    policy_path = tmp_path / "merged.yaml"
    policy_path.write_text(merged_text, encoding="utf-8")

    policy = load_us_equity_special_closure_policy(policy_path)

    assert policy.accepted_source_classes == ("official_exchange_notice",)
    assert policy.accepted_https_hosts == ("www.nyse.com",)


def test_policy_preserves_hashable_non_string_key_parse_boundary(tmp_path: Path) -> None:
    policy_text = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.read_text(encoding="utf-8")
    policy_path = tmp_path / "non_string_key.yaml"
    policy_path.write_text(
        policy_text.replace(
            "source_requirements:\n",
            "source_requirements:\n  1: accepted_by_yaml_loader\n",
        ),
        encoding="utf-8",
    )

    with pytest.raises(TypeError):
        load_us_equity_special_closure_policy(policy_path)


def test_policy_preserves_non_finite_semantic_validation_boundary(tmp_path: Path) -> None:
    payload = _base_policy_payload()
    payload["rationale"] = nan

    with pytest.raises(ValueError, match="rationale must be a non-empty string"):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))

    payload["rationale"] = "1e999"
    policy = load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))
    assert policy.rationale == "1e999"


def test_policy_preserves_recursive_sequence_semantic_boundary(tmp_path: Path) -> None:
    policy_text = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH.read_text(encoding="utf-8")
    prefix, separator, _ = policy_text.partition("closures:")
    assert separator
    policy_path = tmp_path / "recursive_sequence.yaml"
    policy_path.write_text(
        f"{prefix}closures: &closures\n  - *closures\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="closure must be a mapping"):
        load_us_equity_special_closure_policy(policy_path)


def test_policy_preserves_read_and_utf8_failure_contract(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(ValueError) as caught:
        load_us_equity_special_closure_policy(missing)
    assert str(caught.value) == (
        f"unable to load US equity special-closure policy: {missing.resolve()}"
    )

    invalid_utf8 = tmp_path / "invalid_utf8.yaml"
    invalid_utf8.write_bytes(b"\xff")
    with pytest.raises(ValueError) as caught:
        load_us_equity_special_closure_policy(invalid_utf8)
    assert str(caught.value) == (
        f"unable to load US equity special-closure policy: {invalid_utf8.resolve()}"
    )


def test_policy_rejects_unknown_closure_type(tmp_path: Path) -> None:
    payload = _base_policy_payload()
    payload["closures"][0]["closure_type"] = "EARLY_CLOSE"

    with pytest.raises(ValueError, match="unknown special-closure closure_type"):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))


def test_policy_rejects_authoritative_source_tamper(tmp_path: Path) -> None:
    payload = _base_policy_payload()
    payload["closures"][0]["source"]["url"] = "https://unreviewed.example/pretend-closure.pdf"

    with pytest.raises(ValueError, match="accepted authoritative HTTPS source"):
        load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))


def test_policy_hash_exposes_valid_byte_drift_for_receipt_verifier(tmp_path: Path) -> None:
    original = load_us_equity_special_closure_policy(DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH)
    payload = _base_policy_payload()
    payload["rationale"] = f"{payload['rationale']} Reviewed wording change."
    drifted = load_us_equity_special_closure_policy(_write_policy(tmp_path, payload))

    assert drifted.policy_id == original.policy_id
    assert drifted.policy_version == original.policy_version
    assert drifted.sha256 != original.sha256


def test_policy_rejects_malformed_yaml(tmp_path: Path) -> None:
    policy_path = tmp_path / "malformed.yaml"
    policy_path.write_text("closures: [", encoding="utf-8")

    with pytest.raises(ValueError, match="unable to load"):
        load_us_equity_special_closure_policy(policy_path)
