from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ai_trading_system import first_layer_foundational_falsification_execution as v1
from ai_trading_system import first_layer_foundational_falsification_execution_v2 as subject

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap_row() -> dict[str, object]:
    return {
        "block_length_sessions": 21,
        "percentile_2_5": -4.0,
        "percentile_50": 2.0,
        "percentile_97_5": 8.0,
        "probability_excess_less_than_or_equal_to_zero": 0.25,
        "replicates": 10_000,
        "random_seed": 2555,
    }


def test_bootstrap_adapter_projects_only_strict_reducer_fields() -> None:
    row = _bootstrap_row()

    interval = subject.project_bootstrap_interval(row)

    assert tuple(interval.model_dump(mode="python")) == subject.BOOTSTRAP_INTERVAL_FIELDS
    assert "replicates" not in interval.model_dump(mode="python")
    assert "random_seed" not in interval.model_dump(mode="python")
    assert row["replicates"] == 10_000
    assert row["random_seed"] == 2555


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (lambda row: row.pop("percentile_50"), "F1_SCHEMA_ADAPTER_INPUT_DRIFT"),
        (lambda row: row.__setitem__("unexpected", 1), "F1_SCHEMA_ADAPTER_INPUT_DRIFT"),
        (lambda row: row.__setitem__("replicates", 9_999), "F1_IDENTITY_MISMATCH"),
        (lambda row: row.__setitem__("random_seed", 7), "F1_IDENTITY_MISMATCH"),
    ],
)
def test_bootstrap_adapter_fails_closed_on_input_or_audit_drift(
    mutation: object, expected_code: str
) -> None:
    row = _bootstrap_row()
    mutation(row)  # type: ignore[operator]

    with pytest.raises(subject.FoundationalFalsificationExecutionError) as exc_info:
        subject.project_bootstrap_interval(row)

    assert exc_info.value.code == expected_code


def test_v1_executor_is_byte_identical() -> None:
    path = ROOT / "src/ai_trading_system/first_layer_foundational_falsification_execution.py"

    assert hashlib.sha256(path.read_bytes()).hexdigest() == subject.V1_MODULE_SHA256


def test_v2_reuses_frozen_v1_research_identity() -> None:
    assert subject.REQUESTED_START is v1.REQUESTED_START
    assert subject.REQUESTED_END is v1.REQUESTED_END
    assert subject.EXPECTED_SESSIONS == v1.EXPECTED_SESSIONS == 1202
    assert subject.EXPECTED_INTERVALS == v1.EXPECTED_INTERVALS == 1201
    assert subject.RECONCILIATION_TOLERANCE == v1.RECONCILIATION_TOLERANCE == 1e-8
    assert subject.DIAGNOSTIC_IDS is v1.DIAGNOSTIC_IDS
    assert subject.INPUT_ROLES is v1.INPUT_ROLES
    assert subject.EXPECTED_COUNTERS is v1.EXPECTED_COUNTERS
    assert subject.AUTHORIZATION_STATE == "EXACT_PREAUTHORIZED"
    assert subject.DEFAULT_MANIFEST_PATH != v1.DEFAULT_MANIFEST_PATH
    assert subject.DEFAULT_AUTHORIZATION_PATH != v1.DEFAULT_AUTHORIZATION_PATH
    assert subject.DEFAULT_OUTPUT_DIR != v1.DEFAULT_OUTPUT_DIR
