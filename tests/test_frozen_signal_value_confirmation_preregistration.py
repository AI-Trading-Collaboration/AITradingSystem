from __future__ import annotations

import copy
import hashlib
import shutil
from pathlib import Path

import pytest
import yaml

from ai_trading_system.research_quality.frozen_signal_value_confirmation_preregistration import (
    FrozenSignalValueConfirmationPreregistrationPolicy,
    FrozenSignalValuePreregistrationError,
    PreregistrationActionRequest,
    assert_preregistration_action_allowed,
    load_frozen_signal_value_confirmation_preregistration,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = (
    PROJECT_ROOT
    / "config/research/frozen_signal_value_confirmation_preregistration_v1.yaml"
)


def _payload() -> dict[str, object]:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_preregistration_loads_exact_result_blind_draft_and_authorities() -> None:
    loaded = load_frozen_signal_value_confirmation_preregistration()
    policy = loaded.policy

    assert policy.policy_version == "1.0.0-draft.1"
    assert policy.policy_status == "DRAFT_OWNER_REVIEW_REQUIRED"
    assert policy.owner_review.result_blind_draft is True
    assert policy.owner_review.execution_activation_allowed is False
    assert policy.research_question.question_id == "SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2"
    assert policy.research_question.option_data_used is False
    assert policy.signal_identity.expected_signal_sessions == 1202
    assert policy.signal_identity.expected_return_intervals == 1201
    assert policy.candidate_implementation.state_to_target_qqq_weight == {
        "constructive": 1.0,
        "defensive": 0.0,
        "neutral": 0.0,
        "risk_off": 0.0,
        "risk_on": 1.0,
    }
    assert policy.primary_comparator.comparator_id == (
        "EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH"
    )
    assert policy.cost_model.one_way_cost_bps == 5.0
    assert policy.primary_metric.retain_threshold_strictly_greater_than == 0.0
    assert policy.falsification_guard.retain_threshold_less_than_or_equal_to == 0.0
    assert policy.verdict_reducer.precedence == ("INSUFFICIENT", "REJECT", "RETAIN")
    assert loaded.policy_file_sha256 == hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
    assert len(loaded.authority_observations) == 4
    assert all(item.identity_verified for item in loaded.authority_observations)
    assert all(item.semantics_verified for item in loaded.authority_observations)


def test_preregistration_is_non_executable_and_future_envelope_is_only_a_request() -> None:
    policy = load_frozen_signal_value_confirmation_preregistration().policy

    assert policy.future_run_envelope.status == "SPECIFICATION_ONLY_NOT_AUTHORIZED"
    assert policy.future_run_envelope.activation_requires_new_exact_owner_authorization is True
    assert policy.future_run_envelope.proposed_maxima_after_activation.quantconnect_actions == 0
    assert policy.future_run_envelope.proposed_maxima_after_activation.option_backtests == 0
    assert policy.safety.policy_or_threshold_finally_approved is False
    assert policy.safety.outcome_access_authorized is False
    assert policy.safety.market_data_read_authorized is False
    assert policy.safety.dq_authorized is False
    assert policy.safety.local_signal_value_confirmation_authorized is False
    assert policy.safety.backtest_authorized is False
    assert policy.safety.orders == policy.safety.fills == policy.safety.positions == 0
    assert_preregistration_action_allowed(PreregistrationActionRequest())

    with pytest.raises(
        FrozenSignalValuePreregistrationError,
        match="PREREGISTRATION_ACTION_NOT_AUTHORIZED",
    ):
        assert_preregistration_action_allowed(
            PreregistrationActionRequest(run_dq=True, run_signal_value_confirmation=True)
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["candidate_implementation"][
                "state_to_target_qqq_weight"
            ].__setitem__("risk_off", 1.0),
            "signal-to-QQQ exposure mapping drifted",
        ),
        (
            lambda payload: payload["primary_comparator"].__setitem__(
                "outcome_dependent_fit_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["primary_metric"].__setitem__(
                "retain_threshold_strictly_greater_than", 0.01
            ),
            "primary metric RETAIN threshold drifted",
        ),
        (
            lambda payload: payload["verdict_reducer"]["precedence"].reverse(),
            "verdict precedence drifted",
        ),
        (
            lambda payload: payload["safety"].__setitem__("dq_authorized", True),
            "Input should be False",
        ),
        (
            lambda payload: payload.__setitem__("unreviewed_field", True),
            "Extra inputs are not permitted",
        ),
    ],
)
def test_preregistration_fails_closed_on_economic_or_safety_drift(
    mutate: object, match: str
) -> None:
    payload = copy.deepcopy(_payload())
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        FrozenSignalValueConfirmationPreregistrationPolicy.model_validate(payload)


def test_preregistration_loader_rejects_authority_byte_drift(tmp_path: Path) -> None:
    policy_payload = _payload()
    policy_copy = (
        tmp_path
        / "config/research/frozen_signal_value_confirmation_preregistration_v1.yaml"
    )
    policy_copy.parent.mkdir(parents=True)
    shutil.copy2(POLICY_PATH, policy_copy)
    for authority in policy_payload["authorities"]:
        source = PROJECT_ROOT / authority["path"]
        destination = tmp_path / authority["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    drifted = tmp_path / policy_payload["authorities"][0]["path"]
    drifted.write_text(drifted.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(
        FrozenSignalValuePreregistrationError,
        match="authority file SHA-256 mismatch: EVIDENCE_FIRST_PORTFOLIO",
    ):
        load_frozen_signal_value_confirmation_preregistration(
            policy_path=policy_copy,
            project_root=tmp_path,
        )
