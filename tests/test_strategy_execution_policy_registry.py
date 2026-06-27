from __future__ import annotations

from pathlib import Path

import yaml

from ai_trading_system.execution_semantics import (
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    REQUIRED_STRATEGY_EXECUTION_POLICY_FIELDS,
    run_strategy_execution_policy_registry_review,
)


def test_strategy_execution_policy_registry_has_required_bindings(tmp_path: Path) -> None:
    payload = run_strategy_execution_policy_registry_review(output_root=tmp_path / "outputs")
    bindings = {
        row["strategy_id"]: row for row in payload["strategy_execution_policies"]
    }

    assert payload["status"] == "EXECUTION_POLICY_REGISTRY_READY"
    assert {
        "defensive_limited_adjustment",
        "limited_adjustment",
        "no_trade",
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_v0_5_ai_trend_confirmed_only",
    } <= set(bindings)
    for strategy_id, binding in bindings.items():
        assert set(REQUIRED_STRATEGY_EXECUTION_POLICY_FIELDS) <= set(binding), strategy_id
    assert bindings["limited_adjustment"]["validation_policy"][
        "promotion_allowed_from_target_path"
    ] is False


def test_strategy_execution_policy_registry_fails_closed_without_bindings(
    tmp_path: Path,
) -> None:
    registry = yaml.safe_load(
        DEFAULT_EXECUTION_POLICY_REGISTRY_PATH.read_text(encoding="utf-8")
    )
    registry.pop("strategy_execution_policies")
    registry_path = tmp_path / "strategy_execution_policy_registry_missing_bindings.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    payload = run_strategy_execution_policy_registry_review(
        policy_registry_path=registry_path,
        output_root=tmp_path / "outputs",
    )

    assert payload["status"] == "EXECUTION_POLICY_REGISTRY_BLOCKED"
    assert payload["summary"]["strategy_binding_count"] == 0
