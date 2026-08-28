from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from ai_trading_system.qqq_options_research.frozen_signal_implementation_retest_contract import (
    DEFAULT_FROZEN_SIGNAL_RETEST_CONTRACT_PATH,
    FrozenSignalImplementationRetestContract,
    FrozenSignalImplementationRetestContractError,
    load_frozen_signal_implementation_retest_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, object]:
    path = PROJECT_ROOT / DEFAULT_FROZEN_SIGNAL_RETEST_CONTRACT_PATH
    return load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))


def test_contract_loads_with_static_ready_and_backtest_blocked_terminal() -> None:
    result = load_frozen_signal_implementation_retest_contract()

    assert result.terminal == "OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST"
    assert result.file_sha256 == hashlib.sha256(result.path.read_bytes()).hexdigest()
    assert result.canonical_sha256 == result.contract.canonical_sha256
    assert result.contract.status == ("STATIC_CONTRACT_READY_OWNER_EXACT_POLICY_FREEZE_REQUIRED")
    assert result.contract.scope == "NON_EXECUTABLE_DATA_RESEARCH"


def test_exact_primary_window_and_chain_coverage_are_bound_without_return_claim() -> None:
    contract = load_frozen_signal_implementation_retest_contract().contract

    assert str(contract.research_scope.requested_start) == "2021-02-22"
    assert str(contract.research_scope.evaluated_end) == "2025-12-02"
    assert contract.research_scope.expected_session_count == 1202
    assert contract.qc_chain_coverage.observed_session_count == 1202
    assert contract.qc_chain_coverage.unresolved_session_count == 0
    assert contract.qc_chain_coverage.recovered_contract_count == 6496
    assert contract.qc_chain_coverage.proves_strategy_return is False


def test_local_and_quantconnect_responsibilities_are_separate() -> None:
    split = load_frozen_signal_implementation_retest_contract().contract.responsibility_split

    assert "FREEZE_EXISTING_SIGNAL_IDENTITY" in split.local
    assert "ADMIT_EXPORT_SAFE_RESULT_EVIDENCE" in split.local
    assert "CALCULATE_CASH_EQUITY_AND_PNL" in split.quantconnect
    assert split.local_option_repricing_allowed is False
    assert split.quantconnect_direction_signal_allowed is False


def test_five_external_sources_are_optional_and_never_baseline_blockers() -> None:
    overlays = load_frozen_signal_implementation_retest_contract().contract.optional_overlays

    assert tuple(row.provider for row in overlays) == (
        "FMP_SPY_QQQ",
        "CBOE_VIX",
        "FED_SCHEDULE",
        "BLS_SCHEDULE",
        "BEA_SCHEDULE",
    )
    assert all(row.role == "OPTIONAL_RESULT_BLIND_RISK_OVERLAY" for row in overlays)
    assert all(row.mandatory_for_baseline is False for row in overlays)
    assert all(row.direction_signal_allowed is False for row in overlays)
    assert all(row.missing_blocks_baseline is False for row in overlays)


def test_signal_mapping_stays_explicitly_unresolved_without_invented_rows() -> None:
    mapping = load_frozen_signal_implementation_retest_contract().contract.signal_mapping

    assert mapping.source_signal_artifact_status == "MISSING_EXACT_RETAINED_PACKAGE"
    assert mapping.source_signal_enum_status == "UNBOUND"
    assert mapping.target_option_signal_enum == ("LONG_CALL", "LONG_PUT", "FLAT")
    assert mapping.mapping_status == "UNKNOWN_REQUIRES_OWNER_REVIEW"
    assert mapping.mapping_rows == ()
    assert mapping.defensive_or_sgov_mapping == "UNKNOWN_REQUIRES_OWNER_REVIEW"
    assert mapping.option_or_result_input_allowed is False
    assert mapping.missing_source_signal_terminal == "INVALID"


def test_all_execution_policy_axes_remain_owner_blocked() -> None:
    gates = load_frozen_signal_implementation_retest_contract().contract.policy_gates

    assert gates.selection_frozen is False
    assert gates.execution_frozen is False
    assert gates.accounting_frozen is False
    assert gates.lifecycle_frozen is False
    assert gates.quantconnect_engine_defaults_allowed is False


def test_paired_comparator_requires_the_same_signal_and_separate_overlay_lane() -> None:
    comparator = load_frozen_signal_implementation_retest_contract().contract.paired_comparator

    assert comparator.same_frozen_signal_identity_required is True
    assert comparator.required_implementations == (
        "UNDERLYING_IMPLEMENTATION",
        "OPTIONIZED_IMPLEMENTATION",
    )
    assert comparator.optional_overlay_is_separate_lane is True
    assert comparator.result_blind_parameter_freeze_required is True


def test_export_contract_rejects_raw_payload_and_local_substitute_pnl() -> None:
    result = load_frozen_signal_implementation_retest_contract().contract.export_safe_result

    assert "LEAN_VERSION" in result.required_fields
    assert "FEE_SLIPPAGE_CASH_EQUITY_RETURN_DRAWDOWN" in result.required_fields
    assert result.raw_option_rows_allowed is False
    assert result.complete_chain_export_allowed is False
    assert result.contract_quote_history_export_allowed is False
    assert result.local_substitute_pnl_allowed is False


def test_stop_taxonomy_keeps_missing_and_external_authority_fail_closed() -> None:
    stop = load_frozen_signal_implementation_retest_contract().contract.stop_policy

    assert "MISSING_FROZEN_SIGNAL_IDENTITY" in stop.reason_codes
    assert "UNREVIEWED_SIGNAL_MAPPING" in stop.reason_codes
    assert "OPTION_ALPHA_LEAKAGE" in stop.reason_codes
    assert "EXTERNAL_RUN_NOT_AUTHORIZED" in stop.reason_codes
    assert stop.terminal_precedence == ("INVALID", "FAIL", "INSUFFICIENT", "PASS")
    assert stop.missing_unknown_or_not_evaluated_can_pass is False
    assert stop.cross_date_fallback_allowed is False


def test_safety_keeps_all_external_and_execution_counters_closed() -> None:
    safety = load_frozen_signal_implementation_retest_contract().contract.safety

    assert safety.static_contract_authorized is True
    assert safety.signal_mapping_frozen is False
    assert safety.executable_policy_frozen is False
    assert safety.manifest_generation_authorized is False
    assert safety.real_dq_authorized is False
    assert safety.qc_backtest_authorized is False
    assert safety.qc_project_mutation_authorized is False
    assert safety.provider_query_authorized is False
    assert (safety.orders, safety.fills, safety.positions) == (0, 0, 0)
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["optional_overlays"][0].__setitem__(
                "mandatory_for_baseline", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["signal_mapping"]["mapping_rows"].append(
                {"source": "BULLISH", "target": "LONG_CALL"}
            ),
            "separate Owner exact-freeze successor",
        ),
        (
            lambda payload: payload["responsibility_split"].__setitem__(
                "local_option_repricing_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["policy_gates"].__setitem__(
                "quantconnect_engine_defaults_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["stop_policy"]["reason_codes"].pop(),
            "typed stop reason surface drifted",
        ),
        (
            lambda payload: payload["safety"].__setitem__("qc_backtest_authorized", True),
            "Input should be False",
        ),
    ],
)
def test_model_rejects_scope_mapping_policy_or_execution_drift(mutate: object, match: str) -> None:
    payload = deepcopy(_payload())
    mutate(payload)  # type: ignore[operator]

    with pytest.raises(ValueError, match=match):
        FrozenSignalImplementationRetestContract.model_validate(payload)


def test_loader_rejects_immutable_authority_hash_drift() -> None:
    payload = deepcopy(_payload())
    payload["authority_bindings"][0]["file_sha256"] = "0" * 64

    with TemporaryDirectory(dir=PROJECT_ROOT / "outputs") as directory:
        path = Path(directory) / "contract.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(
            FrozenSignalImplementationRetestContractError,
            match="file SHA-256 mismatch",
        ):
            load_frozen_signal_implementation_retest_contract(path=path)


def test_contract_rejects_extra_fields_and_path_escape(tmp_path: Path) -> None:
    payload = deepcopy(_payload())
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        FrozenSignalImplementationRetestContract.model_validate(payload)

    outside = tmp_path / "outside.yaml"
    outside.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(
        FrozenSignalImplementationRetestContractError,
        match="escapes project root",
    ):
        load_frozen_signal_implementation_retest_contract(path=outside)


def test_canonical_round_trip_contains_no_mapping_value_or_backtest_authority() -> None:
    contract = load_frozen_signal_implementation_retest_contract().contract
    replay = FrozenSignalImplementationRetestContract.model_validate(
        json.loads(contract.canonical_bytes)
    )

    assert replay.canonical_bytes == contract.canonical_bytes
    assert replay.canonical_sha256 == contract.canonical_sha256
    text = contract.canonical_bytes.decode("utf-8")
    assert '"mapping_rows": []' in text
    assert '"qc_backtest_authorized": false' in text
    assert '"orders": 0' in text
