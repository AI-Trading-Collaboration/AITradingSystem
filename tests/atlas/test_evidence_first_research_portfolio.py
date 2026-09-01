from __future__ import annotations

import copy
import hashlib
import shutil
from pathlib import Path

import pytest
import yaml

from ai_trading_system.atlas.cited_query_renderer import _verdict_label_zh
from ai_trading_system.contracts.evidence_first_research_portfolio import (
    EvidenceFirstPortfolioError,
    EvidenceFirstResearchPortfolio,
    EvidenceState,
    P0AdmissionClass,
    load_projected_evidence_first_research_portfolio,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/research/evidence_first_research_portfolio_v1.yaml"


def _payload() -> dict[str, object]:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _policy() -> EvidenceFirstResearchPortfolio:
    return EvidenceFirstResearchPortfolio.from_yaml_bytes(POLICY_PATH.read_bytes())


def _projected_policy() -> EvidenceFirstResearchPortfolio:
    return load_projected_evidence_first_research_portfolio(repository_root=PROJECT_ROOT)


def test_evidence_first_portfolio_roundtrips_and_freezes_primary_question() -> None:
    policy = _policy()

    assert policy.question_id == "SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2"
    assert policy.current_verdict is EvidenceState.UNRESOLVED
    assert policy.next_experiment_id == "FROZEN_SIGNAL_VALUE_CONFIRMATION"
    assert policy.historical_window_role == "REUSED_DEVELOPMENT_CONFIRMATION"
    assert (
        EvidenceFirstResearchPortfolio.from_dict(policy.to_dict()).canonical_bytes
        == policy.canonical_bytes
    )
    assert policy.content_sha256 == hashlib.sha256(policy.canonical_bytes).hexdigest()
    assert hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest() == (
        "2df617dc247e509cb94799dda10e4c75ed1e8f1fc069a47466267125e39b8a05"
    )


def test_evidence_first_portfolio_makes_empirical_work_the_next_ready_p0() -> None:
    policy = _projected_policy()

    assert policy.allowed_p0_classes == tuple(P0AdmissionClass)
    assert policy.next_p0_when_ready is P0AdmissionClass.EMPIRICAL_EVIDENCE
    assert policy.no_automatic_successor is True
    assert policy.required_p0_fields == EvidenceFirstResearchPortfolio.REQUIRED_P0_FIELDS
    assert tuple(item.state for item in policy.evidence_ladder) == (
        EvidenceState.READY,
        EvidenceState.READY,
        EvidenceState.READY,
        EvidenceState.RETAIN,
        EvidenceState.NOT_RUN,
        EvidenceState.NOT_ESTABLISHED,
        EvidenceState.NOT_ELIGIBLE,
    )


def test_terminal_projection_verifies_admission_and_preserves_frozen_base() -> None:
    base = _policy()
    projected = _projected_policy()

    assert base.current_verdict is EvidenceState.UNRESOLVED
    assert projected.current_verdict is EvidenceState.RETAIN
    assert (
        projected.next_experiment_id
        == "OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON"
    )
    assert projected.evidence_ladder[3].state is EvidenceState.RETAIN
    assert "+13.745976956735603 个百分点" in projected.evidence_ladder[3].explanation_zh
    assert projected.empirical_run_authorized is False
    assert projected.quantconnect_action_authorized is False
    assert projected.production_effect == "none"
    assert projected.broker_action == "none"


def test_terminal_projection_fails_closed_on_bound_aggregate_tamper(tmp_path: Path) -> None:
    config_dir = tmp_path / "config/research"
    config_dir.mkdir(parents=True)
    for name in (
        "evidence_first_research_portfolio_v1.yaml",
        "frozen_signal_value_confirmation_result_admission_v1.yaml",
        "frozen_signal_value_confirmation_run_authorization_v1.yaml",
    ):
        shutil.copy2(PROJECT_ROOT / "config/research" / name, config_dir / name)
    evidence_dir = tmp_path / "inputs/research/frozen_signal_value_confirmation_v1"
    shutil.copytree(
        PROJECT_ROOT / "inputs/research/frozen_signal_value_confirmation_v1", evidence_dir
    )
    aggregate_path = evidence_dir / "aggregate_result.json"
    aggregate_path.write_text(
        aggregate_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
    )

    with pytest.raises(
        EvidenceFirstPortfolioError,
        match="EVIDENCE_FIRST_RESULT_BINDING_DRIFT:evidence_binding:AGGREGATE_RESULT",
    ):
        load_projected_evidence_first_research_portfolio(repository_root=tmp_path)


@pytest.mark.parametrize(
    ("verdict", "next_experiment", "label_zh"),
    [
        (EvidenceState.UNRESOLVED, "FROZEN_SIGNAL_VALUE_CONFIRMATION", "尚未判定。"),
        (
            EvidenceState.RETAIN,
            "OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON",
            "保留。",
        ),
        (EvidenceState.REJECT, "OPTIONS_IMPLEMENTATION_P0_CLOSED", "拒绝。"),
        (
            EvidenceState.INSUFFICIENT,
            "EXPLICIT_PROSPECTIVE_EVIDENCE_ONLY",
            "证据不足。",
        ),
    ],
)
def test_evidence_first_portfolio_supports_exact_terminal_verdict_transitions(
    verdict: EvidenceState,
    next_experiment: str,
    label_zh: str,
) -> None:
    payload = copy.deepcopy(_payload())
    payload["primary_evidence_question"]["current_verdict"] = verdict.value
    payload["primary_evidence_question"]["next_experiment_id"] = next_experiment
    payload["evidence_ladder"][3]["state"] = verdict.value

    policy = EvidenceFirstResearchPortfolio.from_dict(payload)

    assert policy.current_verdict is verdict
    assert policy.next_experiment_id == next_experiment
    assert policy.evidence_ladder[3].state is verdict
    assert _verdict_label_zh(verdict) == label_zh


def test_evidence_first_portfolio_rejects_terminal_verdict_action_mismatch() -> None:
    payload = copy.deepcopy(_payload())
    payload["primary_evidence_question"]["current_verdict"] = "RETAIN"
    payload["evidence_ladder"][3]["state"] = "RETAIN"
    payload["primary_evidence_question"][
        "next_experiment_id"
    ] = "FROZEN_SIGNAL_VALUE_CONFIRMATION"

    with pytest.raises(EvidenceFirstPortfolioError, match="EVIDENCE_FIRST_NEXT_EXPERIMENT_INVALID"):
        EvidenceFirstResearchPortfolio.from_dict(payload)


def test_evidence_first_portfolio_rejects_terminal_verdict_ladder_mismatch() -> None:
    payload = copy.deepcopy(_payload())
    payload["primary_evidence_question"]["current_verdict"] = "RETAIN"
    payload["primary_evidence_question"][
        "next_experiment_id"
    ] = "OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON"
    payload["evidence_ladder"][3]["state"] = "UNRESOLVED"

    with pytest.raises(EvidenceFirstPortfolioError, match="EVIDENCE_FIRST_LADDER_STATE_INVALID"):
        EvidenceFirstResearchPortfolio.from_dict(payload)


def test_evidence_first_portfolio_freezes_reader_and_safety_boundaries() -> None:
    policy = _policy()

    assert policy.l0_sections == EvidenceFirstResearchPortfolio.L0_SECTIONS
    assert {"task_id", "contract_id", "sha256", "receipt", "manifest", "full_ledger"} <= set(
        policy.l0_forbidden_payloads
    )
    assert policy.empirical_run_authorized is False
    assert policy.quantconnect_action_authorized is False
    assert policy.external_provider_action_authorized is False
    assert policy.investment_conclusion_generated is False
    assert policy.production_effect == "none"
    assert policy.broker_action == "none"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda payload: payload["primary_evidence_question"].__setitem__(
                "current_verdict", "READY"
            ),
            "EVIDENCE_FIRST_CURRENT_VERDICT_INVALID",
        ),
        (
            lambda payload: payload["p0_admission"].__setitem__("no_automatic_successor", False),
            "EVIDENCE_FIRST_P0_FIELDS_INVALID",
        ),
        (
            lambda payload: payload["reader_entry"]["l0_sections"].reverse(),
            "EVIDENCE_FIRST_READER_L0_INVALID",
        ),
        (
            lambda payload: payload["safety"].__setitem__("quantconnect_action_authorized", True),
            "EVIDENCE_FIRST_SAFETY_INVALID",
        ),
    ],
)
def test_evidence_first_portfolio_fails_closed_on_priority_or_safety_drift(
    mutate: object, reason: str
) -> None:
    payload = copy.deepcopy(_payload())
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(EvidenceFirstPortfolioError, match=reason):
        EvidenceFirstResearchPortfolio.from_dict(payload)
