from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from dynamic_v3_backtest_sim_helpers import run_forward_confirmation_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_confirmation_cycle as cycle


def test_confirmation_targets_registers_only_validated_plan_targets(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    plan = fixture["confirmation_plan"]
    registry_root = tmp_path / "forward_confirmation_registry"
    registry_yaml_path = tmp_path / "registry" / "targets.yaml"
    result = cycle.register_confirmation_targets(
        confirmation_plan_id=plan["confirmation_plan_id"],
        confirmation_plan_dir=fixture["confirmation_plan_dir"],
        output_dir=registry_root,
        registry_yaml_path=registry_yaml_path,
        generated_at=datetime(2026, 7, 31, 15, tzinfo=UTC),
    )
    targets = result["targets"]

    assert result["manifest"]["targets_total"] == 1
    assert result["manifest"]["active_target_count"] == 1
    assert result["manifest"]["watch_only_target_count"] == 0
    assert [row["target_id"] for row in targets] == ["limited_adjustment_vs_no_trade"]
    assert targets[0]["source_plan_target"] == plan["confirmation_targets"]["targets"][0]
    assert all(row["auto_apply"] is False for row in targets)
    assert all(row["owner_approval_required"] is True for row in targets)
    assert registry_yaml_path.exists()
    assert (result["registry_dir"] / "confirmation_registry_input_snapshot.json").exists()

    listing = cycle.list_confirmation_targets(
        registry_id=result["registry_id"], output_dir=registry_root
    )
    assert listing["active_target_count"] == 1
    assert listing["watch_only_target_count"] == 0

    validation = cycle.validate_confirmation_targets_artifact(
        registry_id=result["registry_id"], output_dir=registry_root
    )
    assert validation["status"] == "PASS"


def test_confirmation_targets_rejects_naive_cutoff_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    with pytest.raises(cycle.DynamicV3ConfirmationCycleError, match="timezone-aware"):
        _register(fixture, tmp_path / "registry-a", tmp_path / "registry-a.yaml", naive=True)
    assert not (tmp_path / "registry-a").exists()


def test_confirmation_targets_rejects_invalid_plan_before_output(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    source = fixture["confirmation_plan"]["confirmation_plan_dir"] / "reader_brief_section.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    with pytest.raises(cycle.DynamicV3ConfirmationCycleError, match="plan validation"):
        _register(fixture, tmp_path / "registry-b", tmp_path / "registry-b.yaml")
    assert not (tmp_path / "registry-b").exists()


def test_confirmation_targets_rejects_duplicate_plan_registration(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    registry_yaml = tmp_path / "registry-c.yaml"
    _register(fixture, tmp_path / "registry-c", registry_yaml)
    with pytest.raises(cycle.DynamicV3ConfirmationCycleError, match="already registered"):
        _register(fixture, tmp_path / "registry-c2", registry_yaml)
    assert not (tmp_path / "registry-c2").exists()


@pytest.mark.parametrize(
    "artifact_name",
    [
        "confirmation_registry_manifest.json",
        "registered_targets.yaml",
        "confirmation_registry_input_snapshot.json",
        "confirmation_targets_report.md",
    ],
)
def test_confirmation_targets_validator_rejects_output_tamper(
    tmp_path: Path, monkeypatch: Any, artifact_name: str
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    result = _register(fixture, tmp_path / "registry-d", tmp_path / "registry-d.yaml")
    path = result["registry_dir"] / artifact_name
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    elif path.suffix == ".yaml":
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        payload["tampered"] = True
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    else:
        path.write_text(path.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = cycle.validate_confirmation_targets_artifact(
        registry_id=result["registry_id"], output_dir=tmp_path / "registry-d"
    )
    assert validation["status"] == "FAIL"


def test_confirmation_targets_validator_rejects_live_plan_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    result = _register(fixture, tmp_path / "registry-e", tmp_path / "registry-e.yaml")
    source = fixture["confirmation_plan"]["confirmation_plan_dir"] / "reader_brief_section.md"
    source.write_text(source.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8")
    validation = cycle.validate_confirmation_targets_artifact(
        registry_id=result["registry_id"], output_dir=tmp_path / "registry-e"
    )
    assert validation["status"] == "FAIL"


def test_confirmation_targets_validator_rejects_current_materialized_drift(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    registry_yaml = tmp_path / "registry-f.yaml"
    result = _register(fixture, tmp_path / "registry-f", registry_yaml)
    payload = yaml.safe_load(registry_yaml.read_text(encoding="utf-8"))
    payload["tampered"] = True
    registry_yaml.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    validation = cycle.validate_confirmation_targets_artifact(
        registry_id=result["registry_id"], output_dir=tmp_path / "registry-f"
    )
    assert validation["status"] == "FAIL"


def _register(
    fixture: dict[str, Any], output_dir: Path, registry_yaml_path: Path, *, naive: bool = False
) -> dict[str, Any]:
    plan = fixture["confirmation_plan"]
    generated_at = datetime(2026, 7, 31, 15) if naive else datetime(2026, 7, 31, 15, tzinfo=UTC)
    return cycle.register_confirmation_targets(
        confirmation_plan_id=plan["confirmation_plan_id"],
        confirmation_plan_dir=fixture["confirmation_plan_dir"],
        output_dir=output_dir,
        registry_yaml_path=registry_yaml_path,
        generated_at=generated_at,
    )
