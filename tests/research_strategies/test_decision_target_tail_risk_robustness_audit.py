from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from ai_trading_system.contracts import CanonicalStatus
from ai_trading_system.research_framework import resolve_experiment_spec
from ai_trading_system.research_framework.plugins import (
    decision_target_tail_risk_robustness_audit as audit,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/research/decision_target_tail_risk_robustness_audit_v1.yaml"
SPEC_PATH = (
    PROJECT_ROOT / "config/research/experiments/decision_target_tail_risk_robustness_audit.yaml"
)
RESULT_PATH = (
    PROJECT_ROOT
    / "outputs/research_strategies/decision_target_tail_risk_robustness_audit"
    / "decision_target_tail_risk_robustness_audit.json"
)
AS_OF = date(2026, 7, 24)


def test_frozen_protocol_and_experiment_are_manual_research_only() -> None:
    spec = resolve_experiment_spec(SPEC_PATH).value
    policy = safe_load_yaml_path(POLICY_PATH)

    assert spec.task_register_id == (
        "TRADING-2462_TAIL_RISK_CAPABILITY_ROBUSTNESS_FALSIFICATION_AUDIT"
    )
    assert spec.data_quality_required is True
    assert spec.investment_facing_envelope is False
    assert spec.manual_review_required is True
    assert spec.production_effect.value == "none"
    assert spec.broker_action == "none"
    assert spec.canonical_status(audit.READY_STATUS) is CanonicalStatus.PASS
    assert spec.canonical_status(audit.BLOCKED_STATUS) is CanonicalStatus.BLOCKED
    assert policy["protocol_frozen_before_detailed_result_rows"] is True
    assert policy["safety"]["risk_overlay_created"] is False
    assert policy["safety"]["target_weights_generated"] is False
    assert policy["safety"]["qld_used_as_signal"] is False


def test_frozen_policy_tamper_fails_closed() -> None:
    policy = copy.deepcopy(safe_load_yaml_path(POLICY_PATH))
    policy["placebo_gate"]["replicate_count"] = 9
    payload = audit.build_tail_risk_robustness_payload(
        {
            "audit_policy": policy,
            "source_requirement_text": (
                "TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER"
            ),
            "requirement_text": (
                "TRADING-2462_TAIL_RISK_CAPABILITY_ROBUSTNESS_FALSIFICATION_AUDIT "
                "owner_decision:TRADING-2462:2026-07-27:"
                "approve_tail_risk_capability_robustness_falsification_audit_v1"
            ),
        },
        as_of=AS_OF,
    )

    assert payload["status"] == audit.BLOCKED_STATUS
    assert "AUDIT_FROZEN_POLICY_COMMITMENT_INVALID" in payload["strict_validation_errors"]
    assert "AUDIT_PLACEBO_POLICY_INVALID" in payload["strict_validation_errors"]
    assert payload["data_quality_evidence"]["status"] == "FAIL"
    assert payload["risk_overlay_created"] is False
    assert payload["target_weights_generated"] is False
    assert payload["qld_used_as_signal"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_decision_precedence_is_fail_closed() -> None:
    variants = [
        {
            "variant_id": "FEATURE_LAG_1",
            "aggregate": {"passing_primary_horizons": ["5d", "10d"]},
        },
        {
            "variant_id": "EMBARGO_40",
            "aggregate": {"passing_primary_horizons": ["1d", "10d"]},
        },
    ]
    placebo = {"rows": [{"horizon_id": "10d", "horizon_pass": True}]}

    insufficient = audit._decision(
        exact_reconstruction_pass=True,
        mandatory_variants_pass=True,
        fold_influence_pass=True,
        regime_pass=True,
        calibration_pass=True,
        placebo_pass=True,
        evaluability_errors=["REGIME_STRATUM_NOT_EVALUABLE"],
        variant_results=variants,
        placebo=placebo,
    )
    assert insufficient["decision_status"] == "INSUFFICIENT_ROBUSTNESS_EVIDENCE"

    falsified = audit._decision(
        exact_reconstruction_pass=True,
        mandatory_variants_pass=True,
        fold_influence_pass=True,
        regime_pass=True,
        calibration_pass=True,
        placebo_pass=True,
        evaluability_errors=[],
        variant_results=variants,
        placebo={"rows": [{"horizon_id": "10d", "horizon_pass": False}]},
    )
    assert falsified["decision_status"] == "TAIL_RISK_CAPABILITY_FALSIFIED"

    robust = audit._decision(
        exact_reconstruction_pass=True,
        mandatory_variants_pass=True,
        fold_influence_pass=True,
        regime_pass=True,
        calibration_pass=True,
        placebo_pass=True,
        evaluability_errors=[],
        variant_results=variants,
        placebo=placebo,
    )
    assert robust["decision_status"] == "TAIL_RISK_CAPABILITY_ROBUST"
    assert robust["decision_value_audit_authorized"] is False
    assert robust["risk_overlay_authorized"] is False


def test_block_permutation_is_deterministic_and_preserves_values() -> None:
    values = np.arange(43, dtype=np.float64)
    first = audit._block_permute(
        values,
        block_sessions=10,
        rng=np.random.default_rng(2462),
    )
    second = audit._block_permute(
        values,
        block_sessions=10,
        rng=np.random.default_rng(2462),
    )

    assert np.array_equal(first, second)
    assert sorted(first.tolist()) == values.tolist()
    assert not np.array_equal(first, values)


def test_real_artifact_discloses_non_actionable_insufficient_evidence() -> None:
    if not RESULT_PATH.is_file():
        pytest.skip("local governed historical evidence is intentionally untracked")
    payload = json.loads(RESULT_PATH.read_text(encoding="utf-8"))

    assert payload["status"] == audit.READY_STATUS
    assert payload["evaluation"]["gate_summary"]["exact_reconstruction_pass"] is True
    assert payload["decision"]["decision_status"] == "INSUFFICIENT_ROBUSTNESS_EVIDENCE"
    assert payload["evaluation"]["gate_summary"]["evaluability_errors"] == [
        "REGIME_STRATUM_NOT_EVALUABLE",
        "EVENT_CALIBRATION_NOT_EVALUABLE",
    ]
    assert payload["decision"]["decision_value_audit_authorized"] is False
    assert payload["decision"]["risk_overlay_authorized"] is False
    assert payload["target_weights_generated"] is False
    assert payload["qld_used_as_signal"] is False
