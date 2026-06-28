from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.vendor_adapters import norgate_connector
from ai_trading_system.vendor_adapters.norgate_connector import (
    NorgateConnector,
    run_norgate_trial_pack,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DECISION_GATE = PROJECT_ROOT / "config" / "research" / "norgate_paid_platinum_decision_gate.yaml"


def test_norgate_trial_does_not_enable_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_missing_norgate(monkeypatch)
    final = _run_pack(tmp_path)

    assert final["promotion_allowed"] is False
    assert final["summary"]["promotion_allowed"] is False
    assert final["summary"]["first_layer_reopen_allowed"] is False
    assert final["summary"]["candidate_count"] == 0


def test_norgate_trial_does_not_enable_paper_shadow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_missing_norgate(monkeypatch)
    final = _run_pack(tmp_path)

    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"
    assert final["summary"]["paper_shadow_allowed"] is False
    assert final["summary"]["production_allowed"] is False
    assert final["summary"]["broker_action"] == "none"


def test_norgate_trial_raw_data_paths_are_gitignored() -> None:
    gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")

    for pattern in (
        "data/raw/norgate/",
        "data/vendor/norgate/",
        "data/cache/norgate/",
        "outputs/vendor/norgate_raw/",
        "*.norgate.raw.*",
    ):
        assert pattern in gitignore


def test_norgate_trial_price_limit_blocks_primary_window_model_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_missing_norgate(monkeypatch)
    final = _run_pack(tmp_path)

    assert final["summary"]["trial_price_history_limited_to_2y"] is True
    assert final["summary"]["primary_window_full_validation_requires_paid_platinum"] is True
    assert final["summary"]["model_ready_for_2021_primary_window"] is False
    assert final["summary"]["breadth_prototype_status"] == "NORGATE_BREADTH_PROTOTYPE_BLOCKED"


def test_norgate_trial_requires_owner_approval_for_paid_purchase() -> None:
    gate = _load(DECISION_GATE)

    assert gate["owner_manual_approval_before_purchase"] is True
    assert gate["purchase_allowed_without_owner_approval"] is False
    assert gate["safety_boundary"]["promotion_allowed"] is False
    assert gate["safety_boundary"]["broker_action"] == "none"


def test_norgate_trial_outputs_derived_summaries_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_missing_norgate(monkeypatch)
    final = _run_pack(tmp_path)

    assert final["summary"]["raw_data_governance_status"] == "NORGATE_TRIAL_CACHE_GOVERNANCE_PASS"
    output_root = tmp_path / "outputs"
    assert (output_root / "smoke_test_summary.json").exists()
    assert (output_root / "membership_probe_summary.json").exists()
    assert (output_root / "daily_membership_snapshot_summary.csv").exists()
    assert not list(tmp_path.rglob("*.norgate.raw.*"))
    assert not (tmp_path / "data" / "raw" / "norgate").exists()


def test_norgate_trial_integration_can_fail_closed_without_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _force_missing_norgate(monkeypatch)

    environment = NorgateConnector().inspect_environment()

    assert environment.status == "NORGATE_ENV_MISSING_PACKAGE"
    assert environment.module_present is False
    assert environment.database_available is False


def _run_pack(tmp_path: Path) -> dict[str, Any]:
    return run_norgate_trial_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
    )


def _force_missing_norgate(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_import(name: str, package: str | None = None) -> object:
        if name == "norgatedata":
            raise ImportError("forced missing norgatedata for unit test")
        return _ORIGINAL_IMPORT(name, package)

    monkeypatch.setattr(norgate_connector.importlib, "import_module", fake_import)


def _load(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    assert isinstance(payload, dict)
    return payload


_ORIGINAL_IMPORT = norgate_connector.importlib.import_module
