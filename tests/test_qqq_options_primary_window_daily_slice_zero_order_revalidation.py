from __future__ import annotations

import hashlib
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
    primary_window_daily_slice_zero_order_revalidation as revalidation,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = revalidation.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT  # noqa: E501
LOAD_PACKAGE = (
    revalidation.load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package
)
LOAD_POLICY = (
    revalidation.load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy
)
PACKAGE = ROOT / PACKAGE_ROOT


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_qc_source(monkeypatch: pytest.MonkeyPatch) -> Any:
    imports = types.ModuleType("AlgorithmImports")
    imports.QCAlgorithm = object  # type: ignore[attr-defined]
    imports.Slice = object  # type: ignore[attr-defined]
    imports.__all__ = ["QCAlgorithm", "Slice"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "AlgorithmImports", imports)
    module = types.ModuleType("trading_2520_qc_main")
    source = (PACKAGE / "main.py").read_text(encoding="utf-8")
    exec(compile(source, str(PACKAGE / "main.py"), "exec"), module.__dict__)  # noqa: S102
    return module.QQQOptionsPrimaryWindowDerivedAggregateCollector


def _collector_for_property(cls: Any) -> tuple[Any, list[tuple[object, ...]]]:
    collector = cls()
    collector._expected_sessions = {"2021-02-22"}
    collector._seen_sessions = set()
    collector._invalid_sessions = set()
    collector._chain_sessions = set()
    collector._transport_rejected_sessions = set()
    emitted: list[tuple[object, ...]] = []
    collector._plot_pair = lambda *args: emitted.append(args)
    return collector, emitted


def _contract(*, strike: float, delta: float, open_interest: int) -> SimpleNamespace:
    return SimpleNamespace(
        bid_price=2.0,
        ask_price=2.2,
        underlying_last_price=320.0,
        strike=strike,
        expiry=datetime(2021, 3, 19),
        greeks=SimpleNamespace(delta=delta),
        open_interest=open_interest,
        volume=10,
    )


def _isolated_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = tmp_path / "package"
    shutil.copytree(PACKAGE, target)
    monkeypatch.setattr(
        revalidation,
        "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_PACKAGE_ROOT",
        target,
    )
    return target


def test_policy_records_confirmed_accessor_defect_and_unverified_cloud_hypotheses() -> None:
    loaded = (
        revalidation.load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_policy()
    )
    policy = loaded.policy

    assert tuple(item.hypothesis_id for item in policy.root_cause_hypotheses) == (
        "H1_DAILY_SLICE_DELIVERY",
        "H2_OPTION_CONTRACT_UNDERLYING_ACCESSOR",
        "H3_DAILY_TIME_FRONTIER_IDENTITY",
        "H4_TRANSPORT_AND_COVERAGE",
    )
    assert policy.root_cause_hypotheses[1].status == "CONFIRMED_OFFLINE_CODE_DEFECT"
    assert policy.predecessor.observed_session_count == 0
    assert policy.predecessor.invalid_session_count == 1202
    assert policy.safety.predecessor_v3_authorization_consumed is True
    assert policy.safety.further_cloud_run_authorized is False
    assert policy.safety.evidence_status == "FAIL"
    assert policy.safety.dq_pit_status == "NOT_EVALUATED"
    assert policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_package_is_exact_canonical_and_remains_owner_blocked() -> None:
    loaded = LOAD_PACKAGE()

    assert len(loaded.run_scope.session_ids) == 1202
    assert loaded.run_scope.session_ids[0] == date(2021, 2, 22)
    assert loaded.run_scope.session_ids[-1] == date(2025, 12, 2)
    assert loaded.proposal.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert loaded.proposal.decision == "OWNER_AUTHORIZATION_REQUIRED"
    assert loaded.proposal.maximum_orders == loaded.proposal.maximum_fills == 0
    assert loaded.manifest.external_action_performed is False
    assert loaded.manifest.evidence_status == "FAIL"
    assert loaded.manifest.dq_pit_status == "NOT_EVALUATED"
    assert loaded.investigation.confirmed_code_defects == (
        "OPTION_CONTRACT_UNDERLYING_ACCESSOR_MISMATCH",
    )
    assert tuple(item.relative_path for item in loaded.manifest.artifacts) == (
        "investigation.json",
        "main.py",
        "owner_decision_request.md",
        "proposal.json",
        "run_scope.json",
    )


def test_project_code_exact_identity_and_corrected_transport_contract() -> None:
    loaded = LOAD_PACKAGE()
    source = (PACKAGE / "main.py").read_text(encoding="utf-8")

    assert loaded.manifest.project_code_lf_sha256 == (
        "88a60874737c1e210f5a2f5ac990d14d0f4de3024a1db8f41edaddf3db6226aa"
    )
    assert _sha256(PACKAGE / "main.py") == loaded.manifest.project_code_lf_sha256
    assert "self.settings.daily_precise_end_time = True" in source
    assert "def on_data(self, data: Slice):" in source
    assert "data.option_chains.get(self._option)" in source
    assert "session = data.time.date()" in source
    assert 'self._attribute(contract, "underlying_last_price")' in source
    assert 'self._attribute(contract, "underlying")' not in source
    assert '"TRADING2520_DIAGNOSTIC"' in source
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
    assert first._transport_rejected_sessions == second._transport_rejected_sessions == set()


def test_legacy_fixture_only_underlying_accessor_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cls = _load_qc_source(monkeypatch)
    collector, emitted = _collector_for_property(cls)
    legacy = _contract(strike=320, delta=0.5, open_interest=100)
    delattr(legacy, "underlying_last_price")
    legacy.underlying = 320.0

    collector._collect_session_chain(date(2021, 2, 22), [legacy])

    assert emitted == []
    assert collector._seen_sessions == set()
    assert collector._invalid_sessions == {"2021-02-22"}
    assert collector._transport_rejected_sessions == {"2021-02-22"}


def test_missing_chain_does_not_treat_unrelated_slice_as_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cls = _load_qc_source(monkeypatch)
    collector, _ = _collector_for_property(cls)
    collector._option = "QQQ_OPTION"
    data = SimpleNamespace(
        option_chains={},
        time=datetime(2021, 2, 22, 16, 0),
    )

    collector.on_data(data)

    assert collector._invalid_sessions == set()
    assert collector._chain_sessions == set()


def test_delivered_chain_binds_session_to_slice_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cls = _load_qc_source(monkeypatch)
    collector, _ = _collector_for_property(cls)
    collector._option = "QQQ_OPTION"
    data = SimpleNamespace(
        option_chains={"QQQ_OPTION": [_contract(strike=320, delta=0.5, open_interest=100)]},
        time=datetime(2021, 2, 22, 16, 0),
    )

    collector.on_data(data)

    assert collector._chain_sessions == {"2021-02-22"}
    assert collector._seen_sessions == {"2021-02-22"}
    assert collector._invalid_sessions == set()


@pytest.mark.parametrize(
    "file_name",
    [
        "investigation.json",
        "main.py",
        "owner_decision_request.md",
        "package_manifest.json",
        "proposal.json",
        "run_scope.json",
    ],
)
def test_package_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    file_name: str,
) -> None:
    target = _isolated_package(tmp_path, monkeypatch)
    artifact = target / file_name
    artifact.write_bytes(artifact.read_bytes() + b"\n")

    with pytest.raises((ValidationError, ValueError), match="canonical|differs"):
        LOAD_PACKAGE()


def test_package_extra_file_fails_exact_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _isolated_package(tmp_path, monkeypatch)
    (target / "extra.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="inventory"):
        LOAD_PACKAGE()


def test_manifest_rejects_duplicate_json_key() -> None:
    raw = (PACKAGE / "package_manifest.json").read_bytes()
    duplicated = b'{"schema_version":"duplicate",' + raw[1:]

    with pytest.raises(ValueError, match="duplicate"):
        revalidation.DailySliceRevalidationPackageManifest.from_json_bytes(duplicated)


def test_owner_template_is_unambiguous_and_unsigned() -> None:
    text = (PACKAGE / "owner_decision_request.md").read_text(encoding="utf-8")

    assert "不构成授权" in text
    assert "<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "<PACKAGE_MANIFEST_FILE_SHA256>" in text
    assert "authorization_invalidates_after_first_run_attempt:true" in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "contract.underlying`" in text
    assert "underlying_last_price" in text


def test_predecessor_failure_and_code_remain_exact() -> None:
    policy = LOAD_POLICY().policy
    predecessor = ROOT / policy.predecessor.package_relative_path

    source = predecessor.joinpath("main.py").read_text(encoding="utf-8").replace("\r\n", "\n")
    assert hashlib.sha256(source.encode()).hexdigest() == (
        policy.predecessor.project_code_lf_sha256
    )
    assert _sha256(predecessor / "result.json") == (policy.predecessor.failed_result_file_sha256)
