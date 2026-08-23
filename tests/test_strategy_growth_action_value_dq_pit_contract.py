from __future__ import annotations

import copy
import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_dq_pit_contract import (
    ContractEvaluation,
    ContractObservation,
    SessionEvaluation,
    StrategyGrowthActionValueDqPitContract,
    StrategyGrowthActionValueDqPitContractError,
    StrategyGrowthActionValueDqPitContractLoadResult,
    SyntheticNumericThresholds,
    WindowEvaluation,
    aggregate_session,
    aggregate_window,
    evaluate_authority_state,
    evaluate_contract,
    evaluate_contract_semantics,
    load_strategy_growth_action_value_dq_pit_contract,
    session_inventory_lf_sha256,
    synthetic_thresholds_from_intent,
)

CONTRACT_FILE_SHA256 = "a60b6c71e492aacac31d8fc9a4f4d406659679c6a1f88ac9e53664d49134d138"
CONTRACT_CANONICAL_SHA256 = "d7c6bfe8fcb8123be6b8d6f87c5ba72a90db3c5ac50af041d1b3f5eefcc32f68"
V2_FILE_SHA256 = "bbb2e0ade108213269c3c9524b465836518457d932a6344887e6d8afb89ae620"
V2_CANONICAL_SHA256 = "b978e952c4767756025fc01b17f8694004e720a5bb44aa5dde893628a4d9c199"


def _loaded() -> StrategyGrowthActionValueDqPitContractLoadResult:
    return load_strategy_growth_action_value_dq_pit_contract()


def _observation(**changes: object) -> ContractObservation:
    session = date(2025, 1, 3)
    prior = date(2025, 1, 2)
    decision = datetime(2025, 1, 3, 16, 2, tzinfo=UTC)
    base = ContractObservation(
        contract_id="QQQ-2025-01-17-C-500",
        session_date=session,
        source_date=session,
        quote_source_date=session,
        volume_source_date=session,
        expected_prior_session_date=prior,
        open_interest_session_date=prior,
        quote_end_utc=decision - timedelta(seconds=120),
        decision_as_of_utc=decision,
        available_at_utc=decision,
        bid=Decimal("0.90"),
        ask=Decimal("1.10"),
        open_interest=10,
        volume=1,
        provider_status="EXACT",
        engine_status="EXACT",
        exchange_calendar_status="EXACT",
        symbol_mapping_status="EXACT",
        normalization_status="EXACT",
        repository_code_sha_status="EXACT",
        source_evidence_status="EXACT",
        aggregate_manifest_status="EXACT",
        evidence_scope="SYNTHETIC_CONTRACT_TEST_ONLY",
    )
    return replace(base, **cast(Any, changes))


def _thresholds() -> SyntheticNumericThresholds:
    return synthetic_thresholds_from_intent(
        _loaded().contract, confirmation="SYNTHETIC_CONTRACT_VALIDATION_ONLY"
    )


def _contract_terminal(
    status: str,
    *,
    session: date = date(2025, 1, 3),
    contributing: bool = True,
) -> ContractEvaluation:
    return ContractEvaluation(
        contract_id=f"contract-{status}",
        session_date=session,
        contributing_contract=contributing,
        status=status,  # type: ignore[arg-type]
        reasons=(status,),
        quote_age_seconds=None,
        relative_spread=None,
    )


def _session_terminal(session: date, status: str) -> SessionEvaluation:
    return SessionEvaluation(
        session_date=session,
        status=status,  # type: ignore[arg-type]
        contributing_contract_count=1,
        excluded_contract_count=0,
        terminal_counts=((status, 1),),
        reasons=(status,),
    )


def test_contract_loads_exact_identity_and_preserves_v2_consumer_bytes() -> None:
    loaded = _loaded()

    assert loaded.contract_file_sha256 == CONTRACT_FILE_SHA256
    assert loaded.contract_canonical_sha256 == CONTRACT_CANONICAL_SHA256
    assert loaded.contract.consumer_binding.file_sha256 == V2_FILE_SHA256
    assert loaded.contract.consumer_binding.canonical_sha256 == V2_CANONICAL_SHA256
    consumer = PROJECT_ROOT / loaded.contract.consumer_binding.path
    assert hashlib.sha256(consumer.read_bytes()).hexdigest() == V2_FILE_SHA256


def test_contract_is_complete_draft_without_numeric_or_execution_authority() -> None:
    contract = _loaded().contract

    assert contract.status == "DRAFT_COMPLETE_PENDING_OWNER_AND_INDEPENDENT_REVIEW"
    assert contract.review_state.owner_exact_approval == "NOT_PROVIDED"
    assert contract.review_state.independent_review == "NOT_PERFORMED"
    assert contract.numeric_authority.authority_unavailable_outcome == "INSUFFICIENT"
    assert contract.required_serial_contract_fields == (
        "QUOTE_AGE_CLOCK_AND_TIMESTAMP_DIRECTION",
        "RELATIVE_SPREAD_DENOMINATOR_AND_ZERO_DENOMINATOR",
        "CONTRACT_TO_SESSION_AGGREGATION",
        "MISSING_AND_UNKNOWN_TERMINAL_MAPPING",
        "EXACT_SOURCE_DATE_AND_PIT_RULE",
        "GLOBAL_DQ_TERMINAL_ORDER",
    )
    assert evaluate_authority_state(contract) == "AUTHORITY_UNAVAILABLE"
    assert all(
        value is False
        for name, value in contract.safety.model_dump().items()
        if name not in {"production_effect", "broker_action"}
    )
    assert contract.safety.production_effect == "none"
    assert contract.safety.broker_action == "none"


def test_contract_canonical_replay_rejects_duplicate_and_reformatted_json() -> None:
    contract = _loaded().contract

    assert StrategyGrowthActionValueDqPitContract.from_json_bytes(contract.canonical_bytes) == (
        contract
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        StrategyGrowthActionValueDqPitContract.from_json_bytes(b'{"x":1,"x":2}')
    with pytest.raises(ValueError, match="not canonical JSON bytes"):
        StrategyGrowthActionValueDqPitContract.from_json_bytes(
            contract.canonical_bytes.replace(b": ", b":", 1)
        )


def test_strict_yaml_duplicate_and_unknown_fields_fail_closed(tmp_path: Path) -> None:
    source = (
        PROJECT_ROOT
        / "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v1.yaml"
    ).read_text(encoding="utf-8")
    duplicate = tmp_path / "duplicate.yaml"
    duplicate.write_text(
        source.replace(
            "schema_version: strategy_growth_action_value_canonical_dq_pit_contract.v1\n",
            "schema_version: strategy_growth_action_value_canonical_dq_pit_contract.v1\n"
            "schema_version: strategy_growth_action_value_canonical_dq_pit_contract.v1\n",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(StrategyGrowthActionValueDqPitContractError, match="DUPLICATE_KEY"):
        load_strategy_growth_action_value_dq_pit_contract(
            contract_path=Path("duplicate.yaml"), project_root=tmp_path
        )

    payload = copy.deepcopy(_loaded().contract.model_dump(mode="python"))
    payload["unreviewed_field"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        StrategyGrowthActionValueDqPitContract.model_validate(payload)


def test_loader_rejects_contract_path_outside_reviewed_root(tmp_path: Path) -> None:
    contract_path = (
        PROJECT_ROOT
        / "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v1.yaml"
    )
    with pytest.raises(StrategyGrowthActionValueDqPitContractError, match="escapes"):
        load_strategy_growth_action_value_dq_pit_contract(
            contract_path=contract_path, project_root=tmp_path
        )


def test_source_binding_and_terminal_order_drift_fail_closed() -> None:
    payload = copy.deepcopy(_loaded().contract.model_dump(mode="python"))
    payload["source_authority_bindings"][0]["file_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="source authority bindings drifted"):
        StrategyGrowthActionValueDqPitContract.model_validate(payload)

    payload = copy.deepcopy(_loaded().contract.model_dump(mode="python"))
    payload["window_terminal"]["precedence"] = list(
        reversed(payload["window_terminal"]["precedence"])
    )
    with pytest.raises(ValidationError, match="global precedence drifted"):
        StrategyGrowthActionValueDqPitContract.model_validate(payload)


def test_exact_synthetic_numeric_boundaries_pass_without_rounding() -> None:
    result = evaluate_contract(_observation(), thresholds=_thresholds())

    assert result.status == "PASS"
    assert result.quote_age_seconds == Decimal(120)
    assert result.relative_spread == Decimal("0.20")


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {"quote_end_utc": datetime(2025, 1, 3, 15, 59, 59, tzinfo=UTC)},
            "QUOTE_AGE_ABOVE_MAXIMUM",
        ),
        ({"bid": Decimal("0.79"), "ask": Decimal("1.21")}, "RELATIVE_SPREAD_ABOVE_MAXIMUM"),
        ({"open_interest": 9}, "OPEN_INTEREST_BELOW_MINIMUM"),
        ({"volume": 0}, "VOLUME_BELOW_MINIMUM"),
    ],
)
def test_synthetic_numeric_threshold_misses_fail(changes: dict[str, object], reason: str) -> None:
    result = evaluate_contract(_observation(**changes), thresholds=_thresholds())

    assert result.status == "FAIL"
    assert reason in result.reasons


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        (
            {"quote_end_utc": datetime(2025, 1, 3, 16, 2, 1, tzinfo=UTC)},
            "QUOTE_END_AFTER_DECISION_AS_OF",
        ),
        ({"quote_end_utc": datetime(2025, 1, 3, 16, 0)}, "QUOTE_END_UTC_NAIVE_OR_NON_UTC"),
        ({"source_date": date(2025, 1, 2)}, "SOURCE_DATE_MISMATCH"),
        ({"quote_source_date": date(2025, 1, 2)}, "QUOTE_SOURCE_DATE_MISMATCH"),
        ({"volume_source_date": date(2025, 1, 2)}, "VOLUME_SOURCE_DATE_MISMATCH"),
        ({"open_interest_session_date": date(2024, 12, 31)}, "OPEN_INTEREST_SESSION_DATE_MISMATCH"),
        (
            {"available_at_utc": datetime(2025, 1, 3, 16, 2, 1, tzinfo=UTC)},
            "AVAILABLE_AT_AFTER_DECISION_AS_OF",
        ),
        ({"provider_status": "MISMATCH"}, "IDENTITY_MISMATCH"),
        ({"bid": Decimal("1.10"), "ask": Decimal("0.90")}, "QUOTE_DOMAIN_INVALID"),
    ],
)
def test_semantic_pit_and_identity_violations_are_invalid(
    changes: dict[str, object], reason: str
) -> None:
    result = evaluate_contract_semantics(_observation(**changes))

    assert result.status == "INVALID"
    assert reason in result.reasons


@pytest.mark.parametrize(
    "changes",
    [
        {"quote_end_utc": None},
        {"available_at_utc": None},
        {"bid": None},
        {"open_interest": None},
        {"expected_prior_session_date": None},
        {"source_evidence_status": "UNKNOWN"},
    ],
)
def test_missing_or_unknown_required_semantics_never_pass(changes: dict[str, object]) -> None:
    result = evaluate_contract(_observation(**changes), thresholds=_thresholds())

    assert result.status == "UNKNOWN"


def test_invalid_precedes_unknown() -> None:
    result = evaluate_contract_semantics(_observation(source_date=date(2025, 1, 2), bid=None))

    assert result.status == "INVALID"
    assert "SOURCE_DATE_MISMATCH" in result.reasons
    assert "BID_OR_ASK_MISSING" in result.reasons


def test_real_evidence_has_no_executable_numeric_authority() -> None:
    real = _observation(evidence_scope="REAL_EVIDENCE")

    assert evaluate_contract(real).status == "AUTHORITY_UNAVAILABLE"
    prohibited = evaluate_contract(real, thresholds=_thresholds())
    assert prohibited.status == "INVALID"
    assert prohibited.reasons == ("SYNTHETIC_THRESHOLDS_PROHIBITED_FOR_REAL_EVIDENCE",)


def test_synthetic_threshold_helper_requires_exact_test_only_confirmation() -> None:
    with pytest.raises(
        StrategyGrowthActionValueDqPitContractError, match="SYNTHETIC_THRESHOLD_SCOPE_REJECTED"
    ):
        synthetic_thresholds_from_intent(_loaded().contract, confirmation="USE_FOR_RESEARCH")


def test_noncontributing_contract_requires_reason_and_is_excluded() -> None:
    excluded = evaluate_contract(
        _observation(contributing_contract=False, exclusion_reason="OUTSIDE_REVIEWED_MANIFEST")
    )
    invalid = evaluate_contract(_observation(contributing_contract=False, exclusion_reason=None))

    assert excluded.status == "EXCLUDED"
    assert excluded.contributing_contract is False
    assert invalid.status == "INVALID"


def test_session_zero_contributors_and_manifest_identity_fail_closed() -> None:
    session = date(2025, 1, 3)
    excluded = _contract_terminal("EXCLUDED", contributing=False)

    assert (
        aggregate_session(session, (excluded,), contribution_manifest_status="EXACT").status
        == "FAIL"
    )
    assert (
        aggregate_session(session, (excluded,), contribution_manifest_status="UNKNOWN").status
        == "INSUFFICIENT"
    )
    assert (
        aggregate_session(session, (excluded,), contribution_manifest_status="MISMATCH").status
        == "INVALID"
    )


def test_session_precedence_is_invalid_fail_insufficient_pass() -> None:
    session = date(2025, 1, 3)

    assert (
        aggregate_session(
            session,
            (_contract_terminal("PASS"), _contract_terminal("AUTHORITY_UNAVAILABLE")),
            contribution_manifest_status="EXACT",
        ).status
        == "INSUFFICIENT"
    )
    assert (
        aggregate_session(
            session,
            (_contract_terminal("UNKNOWN"), _contract_terminal("FAIL")),
            contribution_manifest_status="EXACT",
        ).status
        == "FAIL"
    )
    assert (
        aggregate_session(
            session,
            (_contract_terminal("FAIL"), _contract_terminal("INVALID")),
            contribution_manifest_status="EXACT",
        ).status
        == "INVALID"
    )
    assert (
        aggregate_session(
            session,
            (_contract_terminal("PASS"), _contract_terminal("PASS")),
            contribution_manifest_status="EXACT",
        ).status
        == "PASS"
    )


def test_session_rejects_contract_from_another_session() -> None:
    result = aggregate_session(
        date(2025, 1, 3),
        (_contract_terminal("PASS", session=date(2025, 1, 6)),),
        contribution_manifest_status="EXACT",
    )
    assert result.status == "INVALID"


def test_session_inventory_hash_uses_ordered_iso_dates_and_terminal_newline() -> None:
    sessions = (date(2025, 1, 2), date(2025, 1, 3))
    expected = hashlib.sha256(b"2025-01-02\n2025-01-03\n").hexdigest()

    assert session_inventory_lf_sha256(sessions) == expected
    assert session_inventory_lf_sha256(tuple(reversed(sessions))) != expected


def test_window_exact_session_set_and_precedence() -> None:
    expected = (date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6))
    inventory_sha = session_inventory_lf_sha256(expected)

    def aggregate(statuses: tuple[str, ...]) -> WindowEvaluation:
        return aggregate_window(
            tuple(
                _session_terminal(session, status)
                for session, status in zip(expected, statuses, strict=True)
            ),
            expected_sessions=expected,
            expected_session_count=3,
            expected_session_inventory_lf_sha256=inventory_sha,
        )

    assert aggregate(("PASS", "PASS", "PASS")).status == "GLOBAL_PASS"
    assert aggregate(("PASS", "PASS", "INSUFFICIENT")).status == "GLOBAL_INSUFFICIENT"
    assert aggregate(("PASS", "INSUFFICIENT", "FAIL")).status == "GLOBAL_FAIL"
    assert aggregate(("FAIL", "FAIL", "INVALID")).status == "GLOBAL_INVALID"


def test_window_duplicate_missing_unexpected_and_inventory_drift_are_global_invalid() -> None:
    expected = (date(2025, 1, 2), date(2025, 1, 3))
    inventory_sha = session_inventory_lf_sha256(expected)
    first = _session_terminal(expected[0], "PASS")

    duplicate = aggregate_window(
        (first, first),
        expected_sessions=expected,
        expected_session_count=2,
        expected_session_inventory_lf_sha256=inventory_sha,
    )
    missing = aggregate_window(
        (first,),
        expected_sessions=expected,
        expected_session_count=2,
        expected_session_inventory_lf_sha256=inventory_sha,
    )
    wrong_inventory = aggregate_window(
        (_session_terminal(expected[0], "PASS"), _session_terminal(expected[1], "PASS")),
        expected_sessions=expected,
        expected_session_count=2,
        expected_session_inventory_lf_sha256="0" * 64,
    )

    assert duplicate.status == "GLOBAL_INVALID"
    assert "OBSERVED_SESSION_DUPLICATE" in duplicate.reasons
    assert missing.status == "GLOBAL_INVALID"
    assert "OBSERVED_SESSION_SET_MISMATCH" in missing.reasons
    assert wrong_inventory.status == "GLOBAL_INVALID"
    assert "EXPECTED_SESSION_INVENTORY_SHA256_MISMATCH" in wrong_inventory.reasons
