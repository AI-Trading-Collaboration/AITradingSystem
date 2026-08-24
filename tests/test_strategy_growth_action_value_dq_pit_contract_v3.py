from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_dq_pit_contract import SessionEvaluation
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v3 import (
    ContractObservationV3,
    EvidenceIdentity,
    RunAuthorityV3,
    SessionContributorManifest,
    StrategyGrowthActionValueDqPitContractV3,
    aggregate_session_v3,
    aggregate_window_v3,
    build_run_authority_v3,
    contributor_manifest_lf_sha256,
    evaluate_contract_semantics_v3,
    evaluate_contract_v3,
    expected_prior_session_v3,
    load_strategy_growth_action_value_dq_pit_contract_v3,
    validate_run_authority_v3,
)
from ai_trading_system.trading_calendar import us_equity_market_session

CONFIG_PATH = Path(
    "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml"
)
EXPECTED_FILE_SHA256 = "b84d8d3dbe2dded761e989c623469607c386297e59d61207bb478d3054523c2e"
EXPECTED_CANONICAL_SHA256 = "9140e68dce070ca5cd421fe05ab480c9d2d330fd21a7f7c6cff0bda0b00aca8b"
EXPECTED_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"


def _target_sessions() -> tuple[date, ...]:
    current = date(2021, 2, 22)
    end = date(2025, 12, 2)
    values: list[date] = []
    while current <= end:
        if us_equity_market_session(current).is_trading_day:
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


TARGET_SESSIONS = _target_sessions()


def _identity(*, provider: str = "provider-v1") -> EvidenceIdentity:
    return EvidenceIdentity(
        provider=provider,
        engine="engine-v1",
        exchange_calendar="XNYS-v1",
        symbol_mapping="mapping-v1",
        normalization="raw-v1",
        repository_code_sha="code-sha-v1",
        source_evidence="source-evidence-v1",
        aggregate_manifest="aggregate-manifest-v1",
    )


def _contract_id(session: date) -> str:
    return f"QQQ-{session.isoformat()}-C-TEST"


def _manifests(*, empty_session: date | None = None) -> tuple[SessionContributorManifest, ...]:
    values: list[SessionContributorManifest] = []
    for session in TARGET_SESSIONS:
        ids = () if session == empty_session else (_contract_id(session),)
        values.append(
            SessionContributorManifest(
                session_date=session,
                expected_contract_ids=ids,
                contract_ids_lf_sha256=contributor_manifest_lf_sha256(ids),
            )
        )
    return tuple(values)


def _authority(
    *,
    empty_session: date | None = None,
    evidence_scope: str = "SYNTHETIC_CONTRACT_TEST_ONLY",
) -> RunAuthorityV3:
    return build_run_authority_v3(
        load_strategy_growth_action_value_dq_pit_contract_v3().contract,
        target_sessions=TARGET_SESSIONS,
        pre_window_prior_session=date(2021, 2, 19),
        expected_identity=_identity(),
        contributor_manifests=_manifests(empty_session=empty_session),
        evidence_scope=evidence_scope,  # type: ignore[arg-type]
    )


def _observation(session: date = TARGET_SESSIONS[0], **overrides: object) -> ContractObservationV3:
    prior = date(2021, 2, 19) if session == TARGET_SESSIONS[0] else TARGET_SESSIONS[
        TARGET_SESSIONS.index(session) - 1
    ]
    values: dict[str, object] = {
        "contract_id": _contract_id(session),
        "session_date": session,
        "source_date": session,
        "quote_source_date": session,
        "volume_source_date": session,
        "open_interest_session_date": prior,
        "quote_end_utc": datetime.combine(session, datetime.min.time(), tzinfo=UTC).replace(
            hour=19, minute=59
        ),
        "decision_as_of_utc": datetime.combine(
            session, datetime.min.time(), tzinfo=UTC
        ).replace(hour=20),
        "quote_available_at_utc": datetime.combine(
            session, datetime.min.time(), tzinfo=UTC
        ).replace(hour=19, minute=59, second=1),
        "volume_available_at_utc": datetime.combine(
            session, datetime.min.time(), tzinfo=UTC
        ).replace(hour=19, minute=59, second=2),
        "open_interest_available_at_utc": datetime.combine(
            session, datetime.min.time(), tzinfo=UTC
        ).replace(hour=13),
        "bid": Decimal("1.00"),
        "ask": Decimal("1.10"),
        "open_interest": 10,
        "volume": 1,
        "volume_semantics": "DECISION_AS_OF_CUMULATIVE_SESSION_VOLUME",
        "actual_identity": _identity(),
        "evidence_scope": "SYNTHETIC_CONTRACT_TEST_ONLY",
    }
    values.update(overrides)
    return ContractObservationV3(**values)  # type: ignore[arg-type]


def test_loads_v3_and_preserves_v2_as_immutable_predecessor() -> None:
    result = load_strategy_growth_action_value_dq_pit_contract_v3()

    assert result.contract_file_sha256 == EXPECTED_FILE_SHA256
    assert result.contract_canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.contract.contract_version == "3.0.0-draft.1"
    assert result.predecessor.contract_file_sha256 == (
        "c9c74d5da0819f206ae59543dcab34a2f1f920687fd4bf646da49a4eabbbd327"
    )
    assert result.predecessor.contract_canonical_sha256 == (
        "94e99dea15f0c62756f87230a7706d575b24e4c193db7bd4673ef2bb44427843"
    )
    assert result.contract.review_state.non_executable_pilot_values_freeze_ready is True
    assert result.contract.review_state.executable_authority is False


def test_run_authority_binds_exact_target_inventory_and_separate_prior() -> None:
    authority = _authority()

    validate_run_authority_v3(authority)
    assert len(authority.payload.target_sessions) == 1202
    assert authority.payload.target_sessions[0] == date(2021, 2, 22)
    assert authority.payload.target_sessions[-1] == date(2025, 12, 2)
    assert authority.payload.pre_window_prior_session == date(2021, 2, 19)
    assert authority.payload.contract_canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert expected_prior_session_v3(TARGET_SESSIONS[0], authority) == date(2021, 2, 19)
    assert expected_prior_session_v3(TARGET_SESSIONS[1], authority) == TARGET_SESSIONS[0]


@pytest.mark.parametrize(
    ("sessions", "prior", "match"),
    [
        (TARGET_SESSIONS[1:], date(2021, 2, 19), "1202"),
        ((date(2021, 2, 19), *TARGET_SESSIONS), date(2021, 2, 19), "1202"),
        (TARGET_SESSIONS, date(2021, 2, 18), "pre-window prior"),
    ],
)
def test_run_authority_rejects_1201_1203_or_wrong_prior(
    sessions: tuple[date, ...], prior: date, match: str
) -> None:
    manifests = _manifests()
    if sessions != TARGET_SESSIONS:
        manifests = tuple(
            SessionContributorManifest(
                session_date=session,
                expected_contract_ids=(_contract_id(session),),
                contract_ids_lf_sha256=contributor_manifest_lf_sha256((_contract_id(session),)),
            )
            for session in sessions
        )
    with pytest.raises(ValueError, match=match):
        build_run_authority_v3(
            load_strategy_growth_action_value_dq_pit_contract_v3().contract,
            target_sessions=sessions,
            pre_window_prior_session=prior,
            expected_identity=_identity(),
            contributor_manifests=manifests,
            evidence_scope="SYNTHETIC_CONTRACT_TEST_ONLY",
        )


def test_first_target_session_semantics_and_numeric_boundary_pass() -> None:
    contract = load_strategy_growth_action_value_dq_pit_contract_v3().contract
    authority = _authority()

    semantic = evaluate_contract_semantics_v3(_observation(), authority=authority)
    evaluation = evaluate_contract_v3(contract, _observation(), authority=authority)

    assert semantic.status == "READY_FOR_NUMERIC_CHECK"
    assert semantic.quote_age_seconds == Decimal(60)
    assert evaluation.status == "PASS"


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"actual_identity": _identity(provider="wrong-provider")}, "EVIDENCE_IDENTITY_MISMATCH"),
        ({"open_interest_session_date": date(2021, 2, 18)}, "OPEN_INTEREST_SESSION_DATE_MISMATCH"),
        ({"bid": None}, "SINGLE_SIDED_PROVIDER_QUOTE"),
    ],
)
def test_excluded_identity_pit_and_quote_invalid_are_not_hidden(
    overrides: dict[str, object], reason: str
) -> None:
    observation = _observation(
        contributing_contract=False,
        exclusion_reason="OUTSIDE_REVIEWED_MONEYNESS_BUCKET",
        **overrides,
    )

    result = evaluate_contract_semantics_v3(observation, authority=_authority())

    assert result.status == "INVALID"
    assert reason in result.reasons


def test_invalid_excluded_row_propagates_to_session_terminal() -> None:
    authority = _authority()
    contract = load_strategy_growth_action_value_dq_pit_contract_v3().contract
    valid = evaluate_contract_v3(contract, _observation(), authority=authority)
    invalid_excluded = evaluate_contract_v3(
        contract,
        _observation(
            contract_id="EXCLUDED-INVALID",
            contributing_contract=False,
            exclusion_reason="OUTSIDE_REVIEWED_MONEYNESS_BUCKET",
            actual_identity=_identity(provider="wrong-provider"),
        ),
        authority=authority,
    )

    result = aggregate_session_v3(
        TARGET_SESSIONS[0], (valid, invalid_excluded), authority=authority
    )

    assert invalid_excluded.status == "INVALID"
    assert result.status == "INVALID"
    assert "ANY_CONTRACT_INVALID_INCLUDING_EXCLUDED_ROW" in result.reasons


def test_zero_expected_is_fail_but_expected_nonempty_zero_observed_is_invalid() -> None:
    session = TARGET_SESSIONS[0]
    zero_expected = aggregate_session_v3(
        session, (), authority=_authority(empty_session=session)
    )
    missing_observed = aggregate_session_v3(session, (), authority=_authority())

    assert zero_expected.status == "FAIL"
    assert zero_expected.reasons == ("ZERO_EXPECTED_CONTRIBUTORS",)
    assert missing_observed.status == "INVALID"
    assert "EXPECTED_NONEMPTY_ZERO_OBSERVED_CONTRIBUTORS" in missing_observed.reasons


def test_exact_1202_of_1202_session_window_can_global_pass() -> None:
    sessions = tuple(
        SessionEvaluation(
            session,
            "PASS",
            1,
            0,
            (("PASS", 1),),
            ("EXACT_MANIFEST_ALL_CONTRIBUTING_CONTRACTS_PASS",),
        )
        for session in TARGET_SESSIONS
    )

    result = aggregate_window_v3(sessions, authority=_authority())

    assert result.status == "GLOBAL_PASS"
    assert result.observed_session_count == 1202
    assert result.expected_session_count == 1202


def test_real_evidence_cannot_consume_non_executable_numeric_pilot() -> None:
    authority = _authority(evidence_scope="REAL_EVIDENCE")
    observation = _observation(evidence_scope="REAL_EVIDENCE")

    result = evaluate_contract_v3(
        load_strategy_growth_action_value_dq_pit_contract_v3().contract,
        observation,
        authority=authority,
    )

    assert result.status == "AUTHORITY_UNAVAILABLE"
    assert result.reasons == ("REAL_EVIDENCE_NUMERIC_AUTHORITY_UNAVAILABLE",)


def test_authority_and_threshold_tamper_fail_closed() -> None:
    authority = replace(_authority(), canonical_sha256="0" * 64)
    semantic = evaluate_contract_semantics_v3(_observation(), authority=authority)
    assert semantic.status == "INVALID"
    assert semantic.reasons[0].startswith("RUN_AUTHORITY_INVALID:")

    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    payload["numeric_policy"]["max_quote_age_seconds"]["value"] = 121
    with pytest.raises(ValueError, match="numeric pilot policy drifted"):
        StrategyGrowthActionValueDqPitContractV3.model_validate(payload)


def test_canonical_replay_and_inventory_identity_are_stable() -> None:
    result = load_strategy_growth_action_value_dq_pit_contract_v3()
    replay = StrategyGrowthActionValueDqPitContractV3.model_validate_json(
        result.contract.canonical_bytes
    )

    assert replay == result.contract
    assert result.contract.scope_binding.target_session_inventory_lf_sha256 == (
        EXPECTED_INVENTORY_SHA256
    )
