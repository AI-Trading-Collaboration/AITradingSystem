from __future__ import annotations

import copy
import hashlib
import shutil
from pathlib import Path

import pytest
import yaml

from ai_trading_system.first_layer_foundational_falsification_contract import (
    BootstrapInterval,
    ContractActionRequest,
    FoundationalDiagnosticSummary,
    FoundationalFalsificationContractError,
    FoundationalFalsificationPolicy,
    LeaveOneYearOutResult,
    assert_contract_action_allowed,
    load_foundational_falsification_contract,
    reduce_foundational_falsification_status,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = (
    PROJECT_ROOT
    / "config/research/first_layer_composer_v2_foundational_falsification_preregistration_v1.yaml"
)


def _payload() -> dict[str, object]:
    payload = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _bootstrap(
    block_length: int,
    *,
    lower: float = 1.0,
    median: float = 8.0,
    upper: float = 16.0,
) -> BootstrapInterval:
    return BootstrapInterval.model_validate(
        {
            "block_length_sessions": block_length,
            "percentile_2_5": lower,
            "percentile_50": median,
            "percentile_97_5": upper,
            "probability_excess_less_than_or_equal_to_zero": 0.01,
        }
    )


def _leave_one_year_out(*, year_2022_excess: float = 5.0) -> tuple[LeaveOneYearOutResult, ...]:
    return tuple(
        LeaveOneYearOutResult(
            calendar_year=year,
            paired_excess_percentage_points=(year_2022_excess if year == 2022 else 6.0),
        )
        for year in (2021, 2022, 2023, 2024, 2025)
    )


def _complete_summary(**updates: object) -> FoundationalDiagnosticSummary:
    payload: dict[str, object] = {
        "identity_issues": (),
        "completed_diagnostic_ids": (
            "POLICY_CONSUMPTION_INVENTORY",
            "CALENDAR_YEAR_ATTRIBUTION",
            "CONTIGUOUS_EPISODE_ATTRIBUTION",
            "LEAVE_ONE_CALENDAR_YEAR_OUT",
            "PAIRED_MOVING_BLOCK_BOOTSTRAP",
            "COST_SENSITIVITY",
            "SGOV_CARRY_SENSITIVITY",
            "STATE_TRANSITION_ATTRIBUTION",
            "SELECTION_HISTORY_INVENTORY",
            "SOURCE_REVISION_DIFF",
        ),
        "policy_consumption_matches_contract": True,
        "source_revision_status": "MATCHED",
        "primary_paired_excess_percentage_points": 10.0,
        "bootstrap_intervals": (_bootstrap(21), _bootstrap(63)),
        "leave_one_calendar_year_out": _leave_one_year_out(),
    }
    payload.update(updates)
    return FoundationalDiagnosticSummary.model_validate(payload)


def test_contract_loads_exact_result_blind_policy_and_authorities() -> None:
    loaded = load_foundational_falsification_contract()
    policy = loaded.policy

    assert policy.policy_status == "OWNER_DIRECTED_RESULT_BLIND_CONTRACT"
    assert policy.known_result_boundary.result_visibility == "PARTIAL_PREEXISTING"
    assert policy.known_result_boundary.historical_window_role == (
        "REUSED_DEVELOPMENT_CONFIRMATION"
    )
    assert policy.primary_identity.requested_start.isoformat() == "2021-02-22"
    assert policy.primary_identity.evaluated_end.isoformat() == "2025-12-02"
    assert policy.primary_identity.expected_signal_sessions == 1202
    assert policy.primary_identity.expected_return_intervals == 1201
    assert policy.primary_identity.primary_one_way_cost_bps == 5.0
    assert policy.bootstrap_contract.block_lengths_sessions == (21, 63)
    assert policy.bootstrap_contract.random_seed == 2555
    assert policy.bootstrap_contract.replicates_per_block_length == 10000
    assert policy.sensitivity_contract.diagnostic_one_way_cost_bps == (10.0, 15.0, 20.0)
    assert policy.reducer.precedence == ("INVALID", "FAIL", "INSUFFICIENT", "PASS")
    assert loaded.policy_file_sha256 == hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
    assert len(loaded.authority_observations) == 7
    assert all(item.identity_verified for item in loaded.authority_observations)
    assert all(item.semantics_verified for item in loaded.authority_observations)


def test_contract_is_non_executable_and_keeps_options_and_production_closed() -> None:
    policy = load_foundational_falsification_contract().policy

    assert policy.successor_run_envelope.status == "SPECIFICATION_ONLY_NOT_AUTHORIZED_BY_F0"
    assert policy.successor_run_envelope.requires_new_canonical_task is True
    assert policy.successor_run_envelope.proposed_maxima.canonical_dq_runs == 1
    assert policy.successor_run_envelope.proposed_maxima.data_downloads == 0
    assert policy.successor_run_envelope.proposed_maxima.quantconnect_actions == 0
    assert policy.safety.empirical_diagnostic_access_authorized is False
    assert policy.safety.options_wave_b_authorized is False
    assert policy.safety.options_wave_c_authorized is False
    assert policy.safety.production_allowed is False
    assert policy.safety.orders == policy.safety.fills == policy.safety.positions == 0
    assert_contract_action_allowed(ContractActionRequest())

    with pytest.raises(
        FoundationalFalsificationContractError,
        match="FOUNDATIONAL_F0_ACTION_NOT_AUTHORIZED",
    ):
        assert_contract_action_allowed(
            ContractActionRequest(read_market_data=True, run_dq=True, options_wave_b=True)
        )


def test_policy_consumption_inventory_matches_current_static_sources() -> None:
    policy = load_foundational_falsification_contract().policy
    inventory = {
        item.field_id: item.expected_status for item in policy.policy_consumption_inventory.entries
    }
    forecast_source = (
        PROJECT_ROOT / "src/ai_trading_system/first_layer_operational_forecast.py"
    ).read_text(encoding="utf-8")
    label_source = (
        PROJECT_ROOT / "src/ai_trading_system/upper_state_label_feature_reset.py"
    ).read_text(encoding="utf-8")

    assert inventory["threshold_selection.positive_score_quantile"] == ("DECLARED_AND_CONSUMED")
    assert "positive_score_quantile" in forecast_source
    assert "positive_sample_floor" in forecast_source
    for field in (
        "negative_score_quantile",
        "min_predicted_share",
        "max_predicted_share",
    ):
        assert field not in forecast_source
        assert inventory[f"threshold_selection.{field}"] == "DECLARED_NOT_CONSUMED"
    for field in ("missed_upside_penalty", "net_of_cost_penalty", "tqqq_penalty"):
        assert field not in label_source
        assert inventory[f"score_weights.{field}"] == "DECLARED_NOT_CONSUMED"
    assert "penalty_per_weight" in label_source
    assert inventory["tqqq.penalty_per_weight"] == "DECLARED_AND_CONSUMED"
    assert policy.policy_consumption_inventory.old_model_wiring_change_allowed is False


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["primary_identity"].__setitem__(
                "requested_start", "2022-12-01"
            ),
            "primary requested/evaluated window drifted",
        ),
        (
            lambda payload: payload["bootstrap_contract"]["block_lengths_sessions"].reverse(),
            "bootstrap block lengths drifted",
        ),
        (
            lambda payload: payload["bootstrap_contract"].__setitem__("random_seed", 7),
            "bootstrap seed or replicate budget drifted",
        ),
        (
            lambda payload: payload["sensitivity_contract"]["diagnostic_one_way_cost_bps"].append(
                25.0
            ),
            "diagnostic cost grid drifted",
        ),
        (
            lambda payload: payload["diagnostic_inventory"].pop(),
            "diagnostic inventory or order drifted",
        ),
        (
            lambda payload: payload["reducer"]["precedence"].reverse(),
            "foundational reducer precedence drifted",
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
def test_contract_rejects_policy_or_safety_drift(mutate: object, match: str) -> None:
    payload = copy.deepcopy(_payload())
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        FoundationalFalsificationPolicy.model_validate(payload)


def test_loader_rejects_bound_source_byte_drift(tmp_path: Path) -> None:
    payload = _payload()
    policy_copy = tmp_path / (
        "config/research/first_layer_composer_v2_foundational_falsification_preregistration_v1.yaml"
    )
    policy_copy.parent.mkdir(parents=True)
    shutil.copy2(POLICY_PATH, policy_copy)
    for binding in payload["authority_bindings"]:
        source = PROJECT_ROOT / binding["path"]
        destination = tmp_path / binding["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    drifted = tmp_path / "src/ai_trading_system/first_layer_operational_forecast.py"
    drifted.write_text(drifted.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(
        FoundationalFalsificationContractError,
        match="authority file SHA-256 mismatch: FIRST_LAYER_OPERATIONAL_FORECAST_SOURCE",
    ):
        load_foundational_falsification_contract(
            policy_path=policy_copy,
            project_root=tmp_path,
        )


def test_reducer_pass_is_narrow_and_does_not_authorize_wave_c_or_production() -> None:
    policy = load_foundational_falsification_contract().policy
    decision = reduce_foundational_falsification_status(_complete_summary(), policy=policy)

    assert decision.status == "PASS"
    assert decision.conclusion == "FOUNDATIONAL_NARROW_SIGNAL_VALUE_SUPPORTED"
    assert decision.qqq_options_wave_b == "OWNER_REVIEW_REQUIRED"
    assert decision.qqq_options_wave_c == "NOT_AUTHORIZED"
    assert decision.production_allowed is False


def test_reducer_invalid_precedes_observed_fail() -> None:
    policy = load_foundational_falsification_contract().policy
    summary = _complete_summary(
        identity_issues=("DQ_OR_PIT_NOT_PASS",),
        primary_paired_excess_percentage_points=-1.0,
    )

    decision = reduce_foundational_falsification_status(summary, policy=policy)

    assert decision.status == "INVALID"
    assert decision.reason_codes == ("DQ_OR_PIT_NOT_PASS",)


def test_reducer_fail_precedes_incomplete_diagnostics() -> None:
    policy = load_foundational_falsification_contract().policy
    summary = FoundationalDiagnosticSummary(
        primary_paired_excess_percentage_points=0.0,
    )

    decision = reduce_foundational_falsification_status(summary, policy=policy)

    assert decision.status == "FAIL"
    assert decision.qqq_options_wave_b == "STOP"
    assert decision.qqq_options_wave_c == "NOT_AUTHORIZED"


def test_reducer_fails_when_a_bootstrap_upper_bound_is_nonpositive() -> None:
    policy = load_foundational_falsification_contract().policy
    summary = _complete_summary(
        bootstrap_intervals=(
            _bootstrap(21, lower=-4.0, median=-2.0, upper=0.0),
            _bootstrap(63),
        )
    )

    decision = reduce_foundational_falsification_status(summary, policy=policy)

    assert decision.status == "FAIL"
    assert "ANY_BOOTSTRAP_97_5_PERCENTILE_LESS_THAN_OR_EQUAL_TO_ZERO" in (decision.reason_codes)


@pytest.mark.parametrize(
    "summary",
    [
        FoundationalDiagnosticSummary(),
        _complete_summary(
            bootstrap_intervals=(
                _bootstrap(21, lower=0.0, median=5.0, upper=10.0),
                _bootstrap(63),
            )
        ),
        _complete_summary(leave_one_calendar_year_out=_leave_one_year_out(year_2022_excess=0.0)),
    ],
)
def test_reducer_keeps_incomplete_or_fragile_evidence_insufficient(
    summary: FoundationalDiagnosticSummary,
) -> None:
    policy = load_foundational_falsification_contract().policy

    decision = reduce_foundational_falsification_status(summary, policy=policy)

    assert decision.status == "INSUFFICIENT"
    assert decision.qqq_options_wave_b == "HOLD"
    assert decision.qqq_options_wave_c == "NOT_AUTHORIZED"
    assert decision.production_allowed is False


def test_result_schema_rejects_unknown_axis_extra_field_and_unordered_interval() -> None:
    with pytest.raises(ValueError, match="unknown id"):
        FoundationalDiagnosticSummary(completed_diagnostic_ids=("UNKNOWN",))
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        FoundationalDiagnosticSummary.model_validate({"unexpected": True})
    with pytest.raises(ValueError, match="bootstrap percentiles must be ordered"):
        _bootstrap(21, lower=4.0, median=2.0, upper=3.0)
