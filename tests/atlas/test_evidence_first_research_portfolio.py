from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from ai_trading_system.contracts.evidence_first_research_portfolio import (
    EvidenceFirstPortfolioError,
    EvidenceFirstResearchPortfolio,
    EvidenceState,
    P0AdmissionClass,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = PROJECT_ROOT / "config/research/evidence_first_research_portfolio_v1.yaml"


def _payload() -> dict[str, object]:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _policy() -> EvidenceFirstResearchPortfolio:
    return EvidenceFirstResearchPortfolio.from_yaml_bytes(POLICY_PATH.read_bytes())


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


def test_evidence_first_portfolio_makes_empirical_work_the_next_ready_p0() -> None:
    policy = _policy()

    assert policy.allowed_p0_classes == tuple(P0AdmissionClass)
    assert policy.next_p0_when_ready is P0AdmissionClass.EMPIRICAL_EVIDENCE
    assert policy.no_automatic_successor is True
    assert policy.required_p0_fields == EvidenceFirstResearchPortfolio.REQUIRED_P0_FIELDS
    assert tuple(item.state for item in policy.evidence_ladder) == (
        EvidenceState.READY,
        EvidenceState.READY,
        EvidenceState.READY,
        EvidenceState.UNRESOLVED,
        EvidenceState.NOT_RUN,
        EvidenceState.NOT_ESTABLISHED,
        EvidenceState.NOT_ELIGIBLE,
    )


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
