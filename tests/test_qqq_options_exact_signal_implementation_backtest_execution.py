from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research.exact_signal_implementation_backtest_execution import (
    DEFAULT_PACKAGE_ROOT,
    ExactSignalImplementationBacktestExecutionError,
    build_execution_package,
    load_execution_policy,
    replay_execution_package,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _generated_main() -> str:
    return (PROJECT_ROOT / DEFAULT_PACKAGE_ROOT / "main.py").read_text(encoding="utf-8")


def test_owner_authorized_execution_policy_is_exact_and_bounded() -> None:
    loaded = load_execution_policy(project_root=PROJECT_ROOT)
    policy = loaded.payload

    assert policy["status"] == "OWNER_AUTHORIZED_SINGLE_BOUNDED_QC_DATA_RESEARCH_EXECUTION"
    assert policy["scope"] == "NON_EXECUTABLE_DATA_RESEARCH"
    assert policy["quantconnect_target"]["project_id"] == 35444189
    assert policy["quantconnect_target"]["protected_original_project_id"] == 34808569
    assert policy["research_window"] == {
        "calendar": "XNYS",
        "requested_start": "2021-02-22",
        "requested_end": "2025-12-02",
        "evaluated_start": "2021-02-22",
        "evaluated_end": "2025-12-02",
        "expected_session_count": 1202,
        "role": "PRIMARY",
    }
    assert policy["action_maxima"]["quantconnect_project_mutations"] == 1
    assert policy["action_maxima"]["quantconnect_cloud_backtests"] == 1
    assert policy["action_maxima"]["quantconnect_retries"] == 0
    assert policy["action_maxima"]["external_provider_queries"] == 0
    assert policy["action_maxima"]["raw_option_payload_exports"] == 0
    assert policy["action_maxima"]["orders_outside_qc_simulation"] == 0
    assert policy["safety"]["production_effect"] == "none"
    assert policy["safety"]["broker_action"] == "none"


def test_canonical_package_replays_exact_signal_and_main_bytes() -> None:
    receipt = replay_execution_package(project_root=PROJECT_ROOT)
    manifest = json.loads(
        (PROJECT_ROOT / DEFAULT_PACKAGE_ROOT / "execution_manifest.json").read_text(
            encoding="utf-8"
        )
    )

    assert receipt["status"] == "PASS"
    assert receipt["quantconnect_dispatch_gate"] == "PASS"
    assert receipt["daily_signal_count"] == 1202
    assert receipt["signal_transition_count"] == 83
    assert receipt["maximum_cloud_backtests"] == 1
    assert receipt["automatic_retry_allowed"] is False
    assert manifest["signal_package"]["transition_count"] == 83
    assert manifest["quantconnect_target"]["maximum_project_file_bytes"] == 32768
    assert manifest["main_py_lf_byte_count"] < 32768
    assert manifest["dispatch_count"] == 0
    generator_path = PROJECT_ROOT / manifest["offline_generator"]["path"]
    assert generator_path.is_file()


def test_transition_compression_preserves_exact_effective_state_changes() -> None:
    source = _generated_main()
    match = re.search(r"^TRANSITIONS=(.+)$", source, flags=re.MULTILINE)
    assert match is not None
    transitions = ast.literal_eval(match.group(1))

    assert len(transitions) == 83
    assert transitions[0] == ("2021-02-23", False)
    assert transitions[-1] == ("2025-10-13", False)
    assert all(action in {True, False} for _, action in transitions)
    assert all(
        left[0] < right[0]
        for left, right in zip(transitions, transitions[1:], strict=False)
    )


def test_generated_algorithm_honors_execution_and_export_boundaries() -> None:
    source = _generated_main()
    compile(source, "main.py", "exec")

    assert "fill_forward=False" in source
    assert "DefaultBrokerageModel(AccountType.CASH)" in source
    assert "class PerContractFee(FeeModel)" in source
    assert "class AdverseLimitFill(ImmediateFillModel)" in source
    assert "event.fill_price+0.01" in source
    assert "event.fill_price-0.01" in source
    assert "timedelta(minutes=5)" in source
    assert "0.45<=delta<=0.60" in source
    assert "oi>=100" in source
    assert "(ask-bid)/mid>0.20" in source
    assert "reserve>nav*0.02" in source
    assert "self._sessions_to_expiry(self._open)<=7" in source
    assert source.count("self.debug(") == 1
    assert "self.history" not in source
    assert "market_order(" not in source
    assert "liquidate(" not in source
    assert "set_holdings(" not in source
    assert "LONG_PUT" not in source
    assert ".object_store" not in source

    model_set = source.index("security.set_fee_model(PerContractFee())")
    entry_order = source.index("self._ticket=self.limit_order(symbol,1,limit")
    assert model_set < entry_order


def test_builder_is_deterministic_in_an_isolated_output(tmp_path: Path) -> None:
    first = build_execution_package(
        output_root=tmp_path / "package", project_root=PROJECT_ROOT
    )
    first_bytes = {
        item.name: item.read_bytes() for item in first.package_root.iterdir()
    }
    second = build_execution_package(
        output_root=tmp_path / "package", project_root=PROJECT_ROOT
    )
    second_bytes = {
        item.name: item.read_bytes() for item in second.package_root.iterdir()
    }

    assert first_bytes == second_bytes
    assert first.main_sha256 == second.main_sha256
    assert first.manifest_sha256 == second.manifest_sha256
    assert first.replay_receipt_sha256 == second.replay_receipt_sha256


def test_main_tamper_fails_closed(tmp_path: Path) -> None:
    built = build_execution_package(
        output_root=tmp_path / "main-tamper", project_root=PROJECT_ROOT
    )
    built.main_path.write_bytes(built.main_path.read_bytes() + b"# tamper\n")

    with pytest.raises(ExactSignalImplementationBacktestExecutionError) as caught:
        replay_execution_package(
            package_root=built.package_root, project_root=PROJECT_ROOT
        )
    assert caught.value.code == "QC_EXECUTION_MAIN_REPLAY_MISMATCH"


def test_manifest_tamper_fails_before_dispatch(tmp_path: Path) -> None:
    built = build_execution_package(
        output_root=tmp_path / "manifest-tamper", project_root=PROJECT_ROOT
    )
    manifest = json.loads(built.manifest_path.read_text(encoding="utf-8"))
    manifest["quantconnect_target"]["project_id"] = 34808569
    built.manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ExactSignalImplementationBacktestExecutionError) as caught:
        replay_execution_package(
            package_root=built.package_root, project_root=PROJECT_ROOT
        )
    assert caught.value.code == "QC_EXECUTION_MANIFEST_SEAL_INVALID"


def test_replay_receipt_tamper_fails_closed(tmp_path: Path) -> None:
    built = build_execution_package(
        output_root=tmp_path / "receipt-tamper", project_root=PROJECT_ROOT
    )
    receipt = json.loads(built.replay_receipt_path.read_text(encoding="utf-8"))
    receipt["quantconnect_dispatch_gate"] = "FAIL"
    built.replay_receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ExactSignalImplementationBacktestExecutionError) as caught:
        replay_execution_package(
            package_root=built.package_root, project_root=PROJECT_ROOT
        )
    assert caught.value.code == "QC_EXECUTION_REPLAY_RECEIPT_SEAL_INVALID"
