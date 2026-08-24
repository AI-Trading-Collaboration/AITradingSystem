from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v2 import (
    ContractEvaluationV2,
    ContractObservationV2,
    EvidenceIdentity,
    SessionContributorManifest,
    StrategyGrowthActionValueDqPitContractV2,
    SyntheticNumericThresholdsV2,
    aggregate_session_v2,
    contributor_manifest_lf_sha256,
    evaluate_contract_semantics_v2,
    evaluate_contract_v2,
    in_run_control_action,
    load_strategy_growth_action_value_dq_pit_contract_v2,
    pre_run_authority_state,
)

EXPECTED_FILE_SHA256 = "c9c74d5da0819f206ae59543dcab34a2f1f920687fd4bf646da49a4eabbbd327"
EXPECTED_CANONICAL_SHA256 = "94e99dea15f0c62756f87230a7706d575b24e4c193db7bd4673ef2bb44427843"
EXPECTED_SESSIONS = (date(2021, 2, 22), date(2021, 2, 23))


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


def _observation(**overrides: object) -> ContractObservationV2:
    values: dict[str, object] = {
        "contract_id": "QQQ-20210223-C-300",
        "session_date": EXPECTED_SESSIONS[1],
        "source_date": EXPECTED_SESSIONS[1],
        "quote_source_date": EXPECTED_SESSIONS[1],
        "volume_source_date": EXPECTED_SESSIONS[1],
        "open_interest_session_date": EXPECTED_SESSIONS[0],
        "quote_end_utc": datetime(2021, 2, 23, 19, 59, tzinfo=UTC),
        "decision_as_of_utc": datetime(2021, 2, 23, 20, 0, tzinfo=UTC),
        "quote_available_at_utc": datetime(2021, 2, 23, 19, 59, 1, tzinfo=UTC),
        "volume_available_at_utc": datetime(2021, 2, 23, 19, 59, 2, tzinfo=UTC),
        "open_interest_available_at_utc": datetime(2021, 2, 23, 13, 0, tzinfo=UTC),
        "bid": Decimal("1.00"),
        "ask": Decimal("1.10"),
        "open_interest": 10,
        "volume": 1,
        "volume_semantics": "DECISION_AS_OF_CUMULATIVE_SESSION_VOLUME",
        "actual_identity": _identity(),
        "evidence_scope": "SYNTHETIC_CONTRACT_TEST_ONLY",
    }
    values.update(overrides)
    return ContractObservationV2(**values)  # type: ignore[arg-type]


def _evaluate(
    observation: ContractObservationV2,
    *,
    thresholds: SyntheticNumericThresholdsV2 | None = None,
) -> ContractEvaluationV2:
    return evaluate_contract_v2(
        load_strategy_growth_action_value_dq_pit_contract_v2().contract,
        observation,
        expected_sessions=EXPECTED_SESSIONS,
        expected_identity=_identity(),
        synthetic_thresholds=thresholds,
    )


def test_loads_versioned_non_executable_successor() -> None:
    result = load_strategy_growth_action_value_dq_pit_contract_v2()

    assert result.contract_file_sha256 == EXPECTED_FILE_SHA256
    assert result.contract_canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.contract.contract_version == "2.0.0-draft.1"
    assert result.contract.review_state.executable_authority is False
    assert result.contract.numeric_policy.executable is False
    assert result.contract.safety.dq_run_authorized is False
    assert result.contract.safety.cache_read_authorized is False
    assert result.predecessor.contract.contract_version == "1.0.0-draft.1"


def test_numeric_pilot_values_preserve_rationale_and_review_boundary() -> None:
    policy = load_strategy_growth_action_value_dq_pit_contract_v2().contract.numeric_policy

    assert policy.state == "NON_EXECUTABLE_PILOT_POLICY_PENDING_REVIEW"
    assert policy.numeric_check_order == (
        "max_quote_age_seconds",
        "max_relative_spread",
        "min_open_interest",
        "min_volume",
    )
    assert policy.max_quote_age_seconds.value == Decimal(120)
    assert policy.max_relative_spread.value == Decimal("0.20")
    assert policy.min_open_interest.value == Decimal(10)
    assert policy.min_volume.value == Decimal(1)
    for item in (
        policy.max_quote_age_seconds,
        policy.max_relative_spread,
        policy.min_open_interest,
        policy.min_volume,
    ):
        assert item.review_disposition == "INSUFFICIENT_EVIDENCE_TO_APPROVE"
        assert item.primary_window_result_visible_when_selected is False
        assert item.execution_liquidity_authority is False
        assert item.rationale and item.known_risk and item.policy_source


def test_valid_semantics_and_synthetic_boundary_pass() -> None:
    semantic = evaluate_contract_semantics_v2(
        _observation(),
        expected_sessions=EXPECTED_SESSIONS,
        expected_identity=_identity(),
    )
    evaluation = _evaluate(_observation(), thresholds=SyntheticNumericThresholdsV2())

    assert semantic.status == "READY_FOR_NUMERIC_CHECK"
    assert semantic.quote_age_seconds == Decimal(60)
    assert evaluation.status == "PASS"


@pytest.mark.parametrize(
    ("overrides", "status", "reason"),
    [
        ({"quote_end_utc": None}, "UNKNOWN", "QUOTE_END_UTC_MISSING"),
        ({"bid": None}, "INVALID", "SINGLE_SIDED_PROVIDER_QUOTE"),
        ({"bid": None, "ask": None}, "UNKNOWN", "BID_AND_ASK_MISSING"),
        ({"bid": 1.0}, "INVALID", "BID_OR_ASK_NOT_DECIMAL"),
        (
            {"quote_source_date": date(2021, 2, 22)},
            "INVALID",
            "QUOTE_END_SOURCE_DATE_MISMATCH",
        ),
        (
            {"open_interest_session_date": date(2021, 2, 23)},
            "INVALID",
            "OPEN_INTEREST_SESSION_DATE_MISMATCH",
        ),
        (
            {"volume_semantics": "END_OF_DAY_FINAL_VOLUME"},
            "INVALID",
            "VOLUME_SEMANTICS_LOOKAHEAD_OR_REVISION_INVALID",
        ),
        (
            {"actual_identity": _identity(provider="provider-v2")},
            "INVALID",
            "EVIDENCE_IDENTITY_MISMATCH",
        ),
    ],
)
def test_semantic_boundaries_are_typed_and_fail_closed(
    overrides: dict[str, object], status: str, reason: str
) -> None:
    result = evaluate_contract_semantics_v2(
        _observation(**overrides),
        expected_sessions=EXPECTED_SESSIONS,
        expected_identity=_identity(),
    )

    assert result.status == status
    assert reason in result.reasons


def test_real_evidence_cannot_use_synthetic_thresholds() -> None:
    result = _evaluate(
        _observation(evidence_scope="REAL_EVIDENCE"),
        thresholds=SyntheticNumericThresholdsV2(),
    )

    assert result.status == "INVALID"
    assert result.reasons == ("SYNTHETIC_THRESHOLDS_PROHIBITED_FOR_REAL_EVIDENCE",)


def test_draft_without_synthetic_thresholds_has_no_numeric_authority() -> None:
    result = _evaluate(_observation())

    assert result.status == "AUTHORITY_UNAVAILABLE"
    assert result.reasons == ("NUMERIC_PILOT_POLICY_NOT_EXECUTABLE",)
    assert (
        pre_run_authority_state(load_strategy_growth_action_value_dq_pit_contract_v2().contract)
        == "INSUFFICIENT_EVIDENCE_TO_RUN_DQ"
    )


def test_numeric_failures_are_collected_in_fixed_order() -> None:
    result = _evaluate(
        _observation(
            quote_end_utc=datetime(2021, 2, 23, 19, 55, tzinfo=UTC),
            bid=Decimal("1.00"),
            ask=Decimal("1.50"),
            open_interest=9,
            volume=0,
        ),
        thresholds=SyntheticNumericThresholdsV2(),
    )

    assert result.status == "FAIL"
    assert result.reasons == (
        "QUOTE_AGE_ABOVE_MAXIMUM",
        "RELATIVE_SPREAD_ABOVE_MAXIMUM",
        "OPEN_INTEREST_BELOW_MINIMUM",
        "VOLUME_BELOW_MINIMUM",
    )
    assert in_run_control_action(result) == "CONTINUE_FIXED_INVENTORY"


def test_contributor_manifest_enforces_exact_unique_contract_set() -> None:
    first = _evaluate(_observation(), thresholds=SyntheticNumericThresholdsV2())
    duplicate = replace(first)
    contract_ids = (first.contract_id,)
    manifest = SessionContributorManifest(
        session_date=EXPECTED_SESSIONS[1],
        expected_contract_ids=contract_ids,
        contract_ids_lf_sha256=contributor_manifest_lf_sha256(contract_ids),
    )

    valid = aggregate_session_v2(EXPECTED_SESSIONS[1], (first,), manifest=manifest)
    invalid = aggregate_session_v2(EXPECTED_SESSIONS[1], (first, duplicate), manifest=manifest)

    assert valid.status == "PASS"
    assert invalid.status == "INVALID"
    assert "DUPLICATE_CONTRACT_ID" in invalid.reasons


def test_invalid_excluded_row_propagates_to_session() -> None:
    valid = _evaluate(_observation(), thresholds=SyntheticNumericThresholdsV2())
    invalid_excluded = _evaluate(
        _observation(
            contract_id="EXCLUDED-WITHOUT-REASON",
            contributing_contract=False,
            exclusion_reason=None,
        ),
        thresholds=SyntheticNumericThresholdsV2(),
    )
    ids = (valid.contract_id,)
    manifest = SessionContributorManifest(
        session_date=EXPECTED_SESSIONS[1],
        expected_contract_ids=ids,
        contract_ids_lf_sha256=contributor_manifest_lf_sha256(ids),
    )

    result = aggregate_session_v2(
        EXPECTED_SESSIONS[1], (valid, invalid_excluded), manifest=manifest
    )

    assert invalid_excluded.status == "INVALID"
    assert result.status == "INVALID"
    assert in_run_control_action(invalid_excluded) == "HARD_STOP_INVALID"


def test_contract_rejects_unknown_fields_and_executable_tamper() -> None:
    raw = yaml.safe_load(
        Path(
            "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml"
        ).read_text(encoding="utf-8")
    )
    raw["unexpected"] = True
    with pytest.raises(ValueError, match="extra"):
        StrategyGrowthActionValueDqPitContractV2.model_validate(raw)

    clean = yaml.safe_load(
        Path(
            "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml"
        ).read_text(encoding="utf-8")
    )
    clean["numeric_policy"]["executable"] = True
    with pytest.raises(ValueError, match="executable"):
        StrategyGrowthActionValueDqPitContractV2.model_validate(clean)


def test_canonical_json_replay_is_stable() -> None:
    contract = load_strategy_growth_action_value_dq_pit_contract_v2().contract
    replay = StrategyGrowthActionValueDqPitContractV2.model_validate_json(contract.canonical_bytes)

    assert replay == contract
    assert json.loads(contract.canonical_bytes)["contract_version"] == "2.0.0-draft.1"


def test_predecessor_hash_drift_is_rejected() -> None:
    source = Path("config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml")
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["predecessor_binding"]["file_sha256"] = "0" * 64

    with pytest.raises(ValueError, match="predecessor_binding.file_sha256"):
        StrategyGrowthActionValueDqPitContractV2.model_validate(payload)
