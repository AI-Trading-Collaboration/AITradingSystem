from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ai_trading_system.research_quality.frozen_signal_value_confirmation_execution import (
    FrozenSignalValueConfirmationExecutionError,
    calculate_candidate_primary,
    calculate_independent_replay,
    calculate_static_comparator_primary,
    load_execution_manifest,
    load_run_authorization,
    reduce_verdict,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

ROOT = Path(__file__).resolve().parents[1]
AUTHORIZATION = Path("config/research/frozen_signal_value_confirmation_run_authorization_v1.yaml")
INPUT_ROLES = (
    "signal_index",
    "real_dq_materialization_receipt",
    "signal_package_manifest_replay_receipt",
    "canonical_prices",
    "canonical_rates",
    "canonical_secondary_prices",
    "canonical_download_manifest",
    "data_quality_policy",
    "us_equity_calendar_policy",
)
RUN_ENVELOPE = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_signal_value_confirmations": 1,
    "independent_replays": 1,
    "data_downloads": 0,
    "cache_mutations": 0,
    "quantconnect_actions": 0,
    "option_backtests": 0,
    "external_provider_actions": 0,
    "orders": 0,
    "fills": 0,
    "positions": 0,
}


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def test_real_run_authorization_is_exact_and_bounded() -> None:
    loaded = load_run_authorization(project_root=ROOT)

    assert loaded.path == ROOT / AUTHORIZATION
    assert loaded.payload["status"] == "OWNER_EXACT_BOUNDED_CONFIRMATION_AUTHORIZED"
    assert loaded.payload["run_envelope"] == RUN_ENVELOPE
    assert loaded.payload["safety"]["market_data_read_authorized"] is True
    assert loaded.payload["safety"]["quantconnect_authorized"] is False
    assert loaded.payload["safety"]["option_data_use_authorized"] is False
    assert loaded.payload["safety"]["production_allowed"] is False


def test_primary_and_independent_ledgers_reconcile() -> None:
    prices = (100.0, 110.0, 99.0, 120.0, 115.0, 130.0)
    targets = (0.0, 1.0, 1.0, 0.0, 1.0)
    comparator_weight = sum(targets) / len(targets)

    candidate = calculate_candidate_primary(prices, targets)
    comparator = calculate_static_comparator_primary(prices, comparator_weight)
    replay = calculate_independent_replay(prices, targets, comparator_weight)

    assert candidate.final_value == pytest.approx(replay["candidate_final_value"], abs=1e-8)
    assert candidate.max_drawdown_magnitude_pct == pytest.approx(
        replay["candidate_max_drawdown_magnitude_pct"], abs=1e-8
    )
    assert comparator.final_value == pytest.approx(replay["comparator_final_value"], abs=1e-8)
    assert comparator.max_drawdown_magnitude_pct == pytest.approx(
        replay["comparator_max_drawdown_magnitude_pct"], abs=1e-8
    )
    assert candidate.total_cost_usd > 0.0
    assert comparator.total_cost_usd > 0.0


@pytest.mark.parametrize(
    ("gates_passed", "metric", "drawdown", "expected"),
    [
        (False, 5.0, -1.0, "INSUFFICIENT"),
        (True, None, -1.0, "INSUFFICIENT"),
        (True, 0.0, -1.0, "REJECT"),
        (True, 1.0, 0.01, "REJECT"),
        (True, 1.0, 0.0, "RETAIN"),
    ],
)
def test_reducer_precedence(
    gates_passed: bool,
    metric: float | None,
    drawdown: float | None,
    expected: str,
) -> None:
    assert (
        reduce_verdict(
            gates_passed=gates_passed,
            primary_metric_pp=metric,
            drawdown_delta_pp=drawdown,
        )
        == expected
    )


def _manifest_fixture(tmp_path: Path) -> tuple[Path, Path]:
    authorization_path = tmp_path / AUTHORIZATION
    authorization_path.parent.mkdir(parents=True)
    authorization_path.write_bytes((ROOT / AUTHORIZATION).read_bytes())
    authorization = load_run_authorization(project_root=tmp_path)

    module_path = tmp_path / "src/executor.py"
    module_path.parent.mkdir(parents=True)
    module_path.write_bytes(b"EXACT_EXECUTOR\n")
    bindings: list[dict[str, object]] = []
    for role in INPUT_ROLES:
        path = tmp_path / "inputs" / f"{role}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        content = f"{role}\n".encode()
        path.write_bytes(content)
        bindings.append(
            {
                "role": role,
                "path": path.relative_to(tmp_path).as_posix(),
                "sha256": _sha(content),
                "size_bytes": len(content),
            }
        )
    payload = {
        "schema_version": "frozen_signal_value_confirmation_execution_manifest.v1",
        "manifest_id": "frozen_signal_value_confirmation_execution_manifest_v1",
        "task_id": "TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1",
        "status": "FROZEN_READY_FOR_SINGLE_DISPATCH",
        "authorization_binding": {
            "path": AUTHORIZATION.as_posix(),
            "file_sha256": authorization.file_sha256,
            "canonical_sha256": authorization.canonical_sha256,
        },
        "code_binding": {
            "implementation_commit_sha": "1" * 40,
            "module_path": "src/executor.py",
            "module_sha256": _sha(module_path.read_bytes()),
        },
        "requested_start": "2021-02-22",
        "requested_end": "2025-12-02",
        "evaluated_start": "2021-02-22",
        "evaluated_end": "2025-12-02",
        "expected_signal_sessions": 1202,
        "expected_return_intervals": 1201,
        "run_envelope": RUN_ENVELOPE,
        "input_bindings": bindings,
        "terminal_artifact": "frozen_signal_value_confirmation_result.v1",
        "aggregate_result_only": True,
        "production_effect": "none",
        "broker_action": "none",
    }
    manifest_path = (
        tmp_path / "inputs/research/frozen_signal_value_confirmation_v1/execution_manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path, module_path


def test_execution_manifest_binds_authorization_code_and_inputs(tmp_path: Path) -> None:
    manifest_path, module_path = _manifest_fixture(tmp_path)

    loaded = load_execution_manifest(manifest_path.relative_to(tmp_path), project_root=tmp_path)

    assert tuple(item.role for item in loaded.inputs) == INPUT_ROLES
    assert loaded.payload["aggregate_result_only"] is True
    module_path.write_bytes(b"TAMPERED\n")
    with pytest.raises(
        FrozenSignalValueConfirmationExecutionError,
        match="module_sha256",
    ):
        load_execution_manifest(manifest_path.relative_to(tmp_path), project_root=tmp_path)


def test_candidate_rejects_non_binary_target() -> None:
    with pytest.raises(
        FrozenSignalValueConfirmationExecutionError,
        match="candidate target is not binary",
    ):
        calculate_candidate_primary((100.0, 101.0), (0.5,))


def test_real_aggregate_result_admission_binds_single_retain_run() -> None:
    admission_path = Path(
        "config/research/frozen_signal_value_confirmation_result_admission_v1.yaml"
    )
    payload = load_strict_yaml_text(
        (ROOT / admission_path).read_text(encoding="utf-8"),
        label=admission_path.as_posix(),
    )
    assert isinstance(payload, dict)
    assert payload["status"] == "TECHNICALLY_VALIDATED_AGGREGATE_RETAIN_ADMITTED"
    assert payload["result"]["verdict"] == "RETAIN"
    assert payload["run_accounting"] == RUN_ENVELOPE
    assert payload["safety"]["aggregate_result_only"] is True
    assert payload["safety"]["quantconnect_authorized"] is False
    for binding in payload["evidence_bindings"]:
        evidence_path = ROOT / binding["path"]
        assert evidence_path.is_file()
        assert _sha(evidence_path.read_bytes()) == binding["file_sha256"]

    result = json.loads(
        (
            ROOT / "inputs/research/frozen_signal_value_confirmation_v1/aggregate_result.json"
        ).read_text(encoding="utf-8")
    )
    assert result["actual_counters"] == RUN_ENVELOPE
    assert result["verdict"] == "RETAIN"
    assert result["gate_status"] == "PASS"
    assert result["aggregate_result_only"] is True
    assert result["raw_market_payload_exported"] is False
    assert result["raw_option_payload_exported"] is False
