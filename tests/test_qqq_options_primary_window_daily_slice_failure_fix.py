from __future__ import annotations

import hashlib
import json
import shutil
import sys
import types
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    primary_window_daily_slice_failure_fix as daily_fix,
)

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / daily_fix.DEFAULT_POLICY_PATH
PACKAGE = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2519_primary_window_daily_slice_failure_fix_v1"
)
HISTORICAL_2518 = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2518_primary_window_collector_filter_failure_fix_v1"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _isolated_root(tmp_path: Path) -> Path:
    policy_target = tmp_path / daily_fix.DEFAULT_POLICY_PATH
    policy_target.parent.mkdir(parents=True)
    shutil.copy2(POLICY, policy_target)
    package_target = tmp_path / PACKAGE.relative_to(ROOT)
    package_target.parent.mkdir(parents=True)
    shutil.copytree(PACKAGE, package_target)
    return tmp_path


def _load_qc_source(monkeypatch: pytest.MonkeyPatch) -> Any:
    imports = types.ModuleType("AlgorithmImports")
    imports.QCAlgorithm = object  # type: ignore[attr-defined]
    imports.Slice = object  # type: ignore[attr-defined]
    imports.__all__ = ["QCAlgorithm", "Slice"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "AlgorithmImports", imports)
    module = types.ModuleType("trading_2519_qc_main")
    source = (PACKAGE / "main.py").read_text(encoding="utf-8")
    exec(compile(source, str(PACKAGE / "main.py"), "exec"), module.__dict__)  # noqa: S102
    return module.QQQOptionsPrimaryWindowDerivedAggregateCollector


def _collector_for_property(cls: Any) -> tuple[Any, list[tuple[object, ...]]]:
    collector = cls()
    collector._expected_sessions = {"2021-02-22"}
    collector._seen_sessions = set()
    collector._invalid_sessions = set()
    emitted: list[tuple[object, ...]] = []
    collector._plot_pair = lambda *args: emitted.append(args)
    return collector, emitted


def _contract(*, strike: float, delta: float, open_interest: int) -> SimpleNamespace:
    return SimpleNamespace(
        bid_price=2.0,
        ask_price=2.2,
        underlying=320.0,
        strike=strike,
        expiry=datetime(2021, 3, 19),
        greeks=SimpleNamespace(delta=delta),
        open_interest=open_interest,
        volume=10,
    )


def test_policy_binds_consumed_v3_failure_and_no_further_run() -> None:
    loaded = daily_fix.load_qc_qqq_options_primary_window_daily_slice_failure_fix_policy()
    failure = loaded.policy.run_failure

    assert failure.backtest_id == "b6d711f67a47199667c8a62f86208b28"
    assert failure.processed_data_points == 38_397_482
    assert failure.observed_session_count == 0
    assert failure.invalid_session_count == 1202
    assert failure.authorization_consumed is True
    assert failure.evidence_admission_status == "FAIL"
    assert failure.dq_pit_status == "NOT_EVALUATED"
    assert loaded.policy.safety["further_cloud_run_authorized"] is False
    assert loaded.policy.safety["engine_status"] == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_package_loads_actual_result_and_canonical_failure_receipt() -> None:
    loaded = daily_fix.build_qc_qqq_options_primary_window_daily_slice_failure_fix_package()

    assert loaded.receipt.strict_admission_status == "FAIL"
    assert loaded.receipt.strict_admission_reason == "COLLECTOR_RUNTIME_TERMINAL_INCOMPLETE"
    assert loaded.receipt.local_derived_aggregate_dq_status == "NOT_EVALUATED"
    assert loaded.receipt.option_event_dq_status == "NOT_EVALUATED"
    assert loaded.receipt.selection_authorized is False
    assert loaded.receipt.further_cloud_run_authorized is False
    assert loaded.manifest.failure_receipt_content_sha256 == loaded.receipt.content_sha256
    assert tuple(item.relative_path for item in loaded.manifest.artifacts) == (
        "failure_receipt.json",
        "main.py",
        "result.json",
    )


def test_result_and_successor_exact_identities() -> None:
    assert _sha256(PACKAGE / "result.json") == (
        "30f95852fe509e5229a86bed77978f62f9756016f17c3159c5afb63b6eaa205b"
    )
    main_lf = (PACKAGE / "main.py").read_text(encoding="utf-8").replace("\r\n", "\n")
    assert hashlib.sha256(main_lf.encode()).hexdigest() == (
        "d5d8638a2e864b5182887da11d0d74a181dec2e7be41f40bc709f2e245a35261"
    )


def test_successor_uses_daily_slice_and_removes_scheduled_chain_lookup() -> None:
    source = (PACKAGE / "main.py").read_text(encoding="utf-8")

    assert "def on_data(self, data: Slice):" in source
    assert "data.option_chains.get(self._option)" in source
    assert "self._collect_session_chain(session, list(chain))" in source
    assert "self.schedule.on(" not in source
    assert "after_market_open" not in source
    assert "self.option_chain(self._option)" not in source
    assert "self._expected_sessions - self._seen_sessions" in source


def test_daily_aggregate_replay_is_input_order_invariant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cls = _load_qc_source(monkeypatch)
    contracts = [
        _contract(strike=315, delta=0.45, open_interest=100),
        _contract(strike=325, delta=0.55, open_interest=80),
    ]
    first, first_emitted = _collector_for_property(cls)
    second, second_emitted = _collector_for_property(cls)

    first._collect_session_chain(date(2021, 2, 22), contracts)
    second._collect_session_chain(date(2021, 2, 22), list(reversed(contracts)))

    assert first_emitted == second_emitted
    assert first._seen_sessions == second._seen_sessions == {"2021-02-22"}
    assert first._invalid_sessions == second._invalid_sessions == set()


def test_empty_daily_chain_fails_closed_without_emission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cls = _load_qc_source(monkeypatch)
    collector, emitted = _collector_for_property(cls)

    collector._collect_session_chain(date(2021, 2, 22), [])

    assert emitted == []
    assert collector._seen_sessions == set()
    assert collector._invalid_sessions == {"2021-02-22"}


@pytest.mark.parametrize(
    "file_name",
    ["failure_receipt.json", "package_manifest.json", "result.json", "main.py"],
)
def test_package_tamper_fails_closed(tmp_path: Path, file_name: str) -> None:
    root = _isolated_root(tmp_path)
    target = root / PACKAGE.relative_to(ROOT) / file_name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises((ValidationError, ValueError)):
        daily_fix.build_qc_qqq_options_primary_window_daily_slice_failure_fix_package(
            project_root=root
        )


def test_package_extra_file_fails_exact_inventory(tmp_path: Path) -> None:
    root = _isolated_root(tmp_path)
    (root / PACKAGE.relative_to(ROOT) / "extra.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="inventory"):
        daily_fix.build_qc_qqq_options_primary_window_daily_slice_failure_fix_package(
            project_root=root
        )


def test_failure_receipt_rejects_duplicate_json_key() -> None:
    raw = (PACKAGE / "failure_receipt.json").read_bytes()
    payload = json.loads(raw)
    duplicated = b'{"schema_version":"duplicate",' + raw[1:]
    assert payload["schema_version"].endswith("receipt.v1")

    with pytest.raises(ValueError, match="duplicate"):
        daily_fix.V3RunFailureReceipt.from_json_bytes(duplicated)


def test_failure_receipt_rejects_runtime_identity_drift() -> None:
    payload = json.loads((PACKAGE / "failure_receipt.json").read_bytes())
    payload["runtime_identity"] = "drifted"
    semantic = {key: value for key, value in payload.items() if key != "content_sha256"}
    payload["content_sha256"] = hashlib.sha256(
        json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    with pytest.raises(ValidationError, match="runtime identity"):
        daily_fix.V3RunFailureReceipt.model_validate(payload)


def test_historical_2518_authority_remains_exact() -> None:
    assert _sha256(HISTORICAL_2518 / "package_manifest.json") == (
        "a9d335f5c80a425301d25a39967b8d251d90960ef1301b55e55ac5b3380a21f7"
    )
    historical_lf = (HISTORICAL_2518 / "main.py").read_text(encoding="utf-8").replace(
        "\r\n", "\n"
    )
    assert hashlib.sha256(historical_lf.encode()).hexdigest() == (
        "064a3bba10d1599a886eb52340ba843ff19ef9caf6a0da89ac5b5119c929d49d"
    )
