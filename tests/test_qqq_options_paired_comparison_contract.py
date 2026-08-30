from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from ai_trading_system.qqq_options_research.paired_comparison_contract import (
    DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH,
    PairedComparisonContract,
    PairedComparisonContractError,
    load_paired_comparison_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH
    payload = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    assert isinstance(payload, dict)
    return payload


def test_contract_loads_at_owner_freeze_boundary_without_external_authority() -> None:
    result = load_paired_comparison_contract()

    assert result.terminal == (
        "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST"
    )
    assert result.file_sha256 == hashlib.sha256(result.path.read_bytes()).hexdigest()
    assert result.canonical_sha256 == result.contract.canonical_sha256
    assert result.contract.status == "STATIC_CONTRACT_READY_OWNER_EXACT_FREEZE_REQUIRED"
    assert result.contract.scope == "NON_EXECUTABLE_DATA_RESEARCH"


def test_frozen_direction_signal_and_option_policy_remain_unchanged() -> None:
    inherited = load_paired_comparison_contract().contract.frozen_inheritance

    assert inherited.direction_signal_id == "first_layer_composer_v2:trend_state"
    assert inherited.direction_model_change_allowed is False
    assert inherited.source_signal_states == (
        "risk_on",
        "constructive",
        "neutral",
        "defensive",
        "risk_off",
    )
    assert inherited.option_actions == ("LONG_CALL", "LONG_CALL", "FLAT", "FLAT", "FLAT")
    assert inherited.mapping_frozen is True
    assert inherited.long_put_in_baseline is False
    assert inherited.option_policy_slot_count == 37
    assert inherited.option_policy_mutation_allowed is False


def test_existing_result_is_diagnostic_only_and_cannot_select_the_successor() -> None:
    result = load_paired_comparison_contract().contract.existing_result

    assert result.backtest_id == "f2879a3cee7ec4e0b68b4f943aafd1f8"
    assert result.authorization_state == "RETROSPECTIVELY_REVIEWED"
    assert result.technical_validation_state == "PASS_EXPORT_SAFE_AGGREGATE_ONLY"
    assert result.evidence_role == "CAPABILITY_AND_DIAGNOSTIC_EVIDENCE_ONLY"
    assert result.paired_comparator_outcome == "INSUFFICIENT_PLATFORM_EVIDENCE"
    assert result.can_select_comparator is False
    assert result.can_select_normalization is False
    assert result.can_select_window is False
    assert result.can_change_baseline is False


def test_primary_comparator_is_same_signal_fully_funded_qqq_cash_ledger() -> None:
    comparator = load_paired_comparison_contract().contract.primary_comparator

    assert comparator.implementation_id == "UNDERLYING_IMPLEMENTATION"
    assert comparator.method == "SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT"
    assert str(comparator.initial_cash_usd) == "100000.00"
    assert comparator.long_call_exposure == "UNLEVERED_LONG_QQQ"
    assert comparator.flat_exposure == "ZERO_RETURN_CASH"
    assert comparator.same_signal_package_required is True
    assert comparator.same_mapping_required is True
    assert comparator.same_effective_session_required is True
    assert comparator.same_event_clock_required is True
    assert comparator.virtual_ledger_only is True
    assert comparator.order_submission_allowed is False
    assert comparator.no_eligible_contract_treatment == "RETAIN_UNDERLYING_SIGNAL_EXPOSURE"
    assert comparator.margin_allowed is False
    assert comparator.leverage_allowed is False


def test_common_capital_primary_and_capital_at_risk_time_secondary_are_distinct() -> None:
    contract = load_paired_comparison_contract().contract

    assert contract.primary_estimand.view_id == "COMMON_CAPITAL_ACCOUNT_VIEW"
    assert contract.primary_estimand.optionized_initial_cash_usd == (
        contract.primary_estimand.underlying_initial_cash_usd
    )
    assert contract.primary_estimand.headline_metric == (
        "OPTIONIZED_NET_RETURN_MINUS_UNDERLYING_IMPLEMENTATION_NET_RETURN"
    )
    assert contract.primary_estimand.negative_result_is_valid is True
    assert contract.primary_estimand.parameter_change_or_retry_on_failure_allowed is False
    assert contract.secondary_view.view_id == "CAPITAL_AT_RISK_TIME_VIEW"
    assert contract.secondary_view.may_override_primary is False
    assert contract.secondary_view.conflicting_direction_terminal == (
        "MIXED_IMPLEMENTATION_TRADEOFF"
    )


def test_comparator_multiplicity_and_diagnostics_are_exactly_bounded() -> None:
    diagnostics = load_paired_comparison_contract().contract.diagnostics

    assert tuple(row.diagnostic_id for row in diagnostics.named) == (
        "SGOV_CARRY_COMPARATOR",
        "QQQ_BUY_AND_HOLD",
    )
    assert diagnostics.maximum_primary_comparators == 1
    assert diagnostics.maximum_named_diagnostics == 2
    assert diagnostics.post_result_addition_allowed is False
    assert diagnostics.legacy_one_share_role == "EVENT_CLOCK_AND_QUOTE_PATH_DIAGNOSTIC"
    assert diagnostics.realized_delta_without_continuous_platform_evidence == (
        "INSUFFICIENT_PLATFORM_EVIDENCE"
    )
    assert diagnostics.local_delta_reconstruction_allowed is False


def test_export_surface_contains_identity_event_account_risk_and_comparator_evidence() -> None:
    fields = load_paired_comparison_contract().contract.export_safe_fields

    assert "COMPARATOR_CONTRACT_CANONICAL_SHA256" in fields.identity
    assert "MANIFEST_REPLAY_STATUS" in fields.dq_signal
    assert "EVENT_RECONCILIATION_STATUS" in fields.events
    assert "UNDERLYING_MAX_DRAWDOWN" in fields.accounts
    assert "PREMIUM_AT_RISK_HOLDING_TIME" in fields.risk
    assert "PRIMARY_RETURN_DELTA" in fields.comparator
    assert fields.raw_option_rows_allowed is False
    assert fields.complete_chain_allowed is False
    assert fields.contract_identifiers_allowed is False
    assert fields.contract_quote_history_allowed is False
    assert fields.local_option_repricing_input_allowed is False


def test_five_calendar_partitions_cover_only_the_frozen_primary_window() -> None:
    calendar = load_paired_comparison_contract().contract.calendar_diagnostics

    assert tuple(row.partition_id for row in calendar.partitions) == (
        "PRIMARY_WINDOW_CALENDAR_2021",
        "CALENDAR_2022",
        "CALENDAR_2023",
        "CALENDAR_2024",
        "PRIMARY_WINDOW_CALENDAR_2025",
    )
    assert str(calendar.partitions[0].start) == "2021-02-22"
    assert str(calendar.partitions[-1].end) == "2025-12-02"
    assert calendar.exact_once_required is True
    assert calendar.independent_backtests is False
    assert calendar.refit_or_policy_reselection_allowed is False
    assert calendar.post_result_window_addition_allowed is False


def test_falsification_matrix_has_exactly_16_axes_and_fail_closed_precedence() -> None:
    falsification = load_paired_comparison_contract().contract.falsification

    assert len(falsification.axes) == 16
    assert falsification.terminal_precedence == ("INVALID", "FAIL", "INSUFFICIENT", "PASS")
    assert falsification.allowed_statuses == ("PASS", "FAIL", "INSUFFICIENT", "INVALID")
    assert falsification.axes[-2].axis_id == "PRIMARY_IMPLEMENTATION_ESTIMAND"
    assert falsification.axes[-2].fail_when == (
        "RETURN_DELTA_NONPOSITIVE_WITH_VALID_COMPLETE_EVIDENCE"
    )
    assert falsification.axes[-1].axis_id == "EXTERNAL_AUTHORIZATION"
    assert falsification.missing_unknown_or_not_evaluated_can_pass is False
    assert falsification.fail_action == "STOP_AND_REPORT_NO_PARAMETER_CHANGE_NO_RETRY"
    assert falsification.insufficient_action == "RETAIN_GAP_NO_LOCAL_SUBSTITUTE"
    assert falsification.invalid_action == "QUARANTINE_NO_ADMISSION"


def test_safety_keeps_every_follow_on_and_external_action_closed() -> None:
    safety = load_paired_comparison_contract().contract.safety

    assert safety.static_contract_authorized is True
    assert safety.owner_exact_frozen is False
    assert safety.qc_exporter_implementation_authorized is False
    assert safety.local_result_admission_implementation_authorized is False
    assert safety.run_manifest_generation_authorized is False
    assert safety.real_dq_authorized is False
    assert safety.quantconnect_save_authorized is False
    assert safety.quantconnect_build_authorized is False
    assert safety.quantconnect_backtest_authorized is False
    assert safety.quantconnect_retry_authorized is False
    assert safety.provider_query_or_purchase_authorized is False
    assert safety.raw_option_payload_download_or_export_allowed is False
    assert (safety.orders_outside_qc_simulation, safety.fills_outside_qc_simulation) == (0, 0)
    assert safety.positions_outside_qc_simulation == 0
    assert safety.investment_conclusion_generated is False
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["existing_result"].__setitem__(
                "can_select_comparator", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["primary_comparator"].__setitem__(
                "method", "NORMALIZED_ONE_SHARE_QQQ_QUOTE_LEDGER"
            ),
            "SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT",
        ),
        (
            lambda payload: payload["primary_estimand"].__setitem__(
                "underlying_initial_cash_usd", "1.00"
            ),
            "common-capital primary view drifted",
        ),
        (
            lambda payload: payload["secondary_view"].__setitem__(
                "may_override_primary", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["diagnostics"]["named"].append(
                {
                    "diagnostic_id": "QQQ_BUY_AND_HOLD",
                    "role": "CONTEXT_ONLY_NOT_PRIMARY_PASS_FAIL",
                    "preregistered": True,
                }
            ),
            "named diagnostic surface drifted",
        ),
        (
            lambda payload: payload["export_safe_fields"]["risk"].pop(),
            "export-safe field surface drifted",
        ),
        (
            lambda payload: payload["calendar_diagnostics"]["partitions"].pop(),
            "calendar diagnostic partitions drifted",
        ),
        (
            lambda payload: payload["falsification"]["axes"][0].__setitem__(
                "pass_when", "ALWAYS"
            ),
            "16-axis falsification matrix drifted",
        ),
        (
            lambda payload: payload["safety"].__setitem__(
                "quantconnect_backtest_authorized", True
            ),
            "Input should be False",
        ),
    ],
)
def test_model_rejects_result_leakage_comparator_axis_export_or_authority_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_payload())
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        PairedComparisonContract.model_validate(payload)


def test_model_rejects_missing_and_extra_fields() -> None:
    missing = deepcopy(_payload())
    del missing["primary_estimand"]
    with pytest.raises(ValueError, match="Field required"):
        PairedComparisonContract.model_validate(missing)

    extra = deepcopy(_payload())
    extra["executable"] = True
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        PairedComparisonContract.model_validate(extra)


def test_loader_rejects_immutable_authority_hash_drift() -> None:
    payload = deepcopy(_payload())
    payload["authority_bindings"][0]["file_sha256"] = "0" * 64

    with TemporaryDirectory(dir=PROJECT_ROOT / "outputs") as directory:
        path = Path(directory) / "contract.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(PairedComparisonContractError, match="file SHA-256 mismatch"):
            load_paired_comparison_contract(path=path)


def test_loader_rejects_path_escape_and_symlink() -> None:
    with TemporaryDirectory() as directory:
        outside = Path(directory) / "outside.yaml"
        outside.write_text("schema_version: invalid\n", encoding="utf-8")
        with pytest.raises(PairedComparisonContractError, match="escapes project root"):
            load_paired_comparison_contract(path=outside)

    with TemporaryDirectory(dir=PROJECT_ROOT / "outputs") as directory:
        link = Path(directory) / "contract-link.yaml"
        target = PROJECT_ROOT / DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH
        try:
            link.symlink_to(target)
        except OSError as exc:
            pytest.skip(f"symlink creation unavailable on this platform: {exc}")
        assert os.path.islink(link)
        with pytest.raises(PairedComparisonContractError, match="symlink"):
            load_paired_comparison_contract(path=link)


def test_canonical_round_trip_preserves_non_executable_contract() -> None:
    contract = load_paired_comparison_contract().contract
    replay = PairedComparisonContract.model_validate(json.loads(contract.canonical_bytes))

    assert replay.canonical_bytes == contract.canonical_bytes
    assert replay.canonical_sha256 == contract.canonical_sha256
    text = contract.canonical_bytes.decode("utf-8")
    assert '"owner_exact_frozen": false' in text
    assert '"quantconnect_backtest_authorized": false' in text
    assert '"paired_comparator_outcome": "INSUFFICIENT_PLATFORM_EVIDENCE"' in text
    assert '"orders_outside_qc_simulation": 0' in text
