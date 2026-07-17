from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dynamic_v3_weight_batch_search_helpers import run_owner_research_decision_pack_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_decision as decision
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)

Validator = Callable[[], dict[str, Any]]


def _tamper_and_restore(path: Path, validate: Validator) -> None:
    original = path.read_bytes()
    try:
        path.write_bytes(original + b" ")
        assert validate()["status"] == "FAIL"
    finally:
        path.write_bytes(original)


def _tamper_snapshot(
    path: Path,
    validate: Validator,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    original = path.read_bytes()
    try:
        payload = json.loads(original.decode("utf-8"))
        mutate(payload)
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        assert validate()["status"] == "FAIL"
    finally:
        path.write_bytes(original)


@with_artifact_validation_session
def test_weight_search_decision_chain_rebuilds_all_views_and_rejects_tamper(tmp_path) -> None:
    fixture = run_owner_research_decision_pack_fixture(tmp_path)
    cluster = fixture["cluster"]
    interpretation = fixture["interpretation"]
    gate = fixture["promotion_gate"]
    plan = fixture["formal_plan"]
    dashboard = fixture["dashboard"]
    owner = fixture["owner_pack"]

    artifacts: list[tuple[Path, str, tuple[str, ...], Validator]] = [
        (
            Path(cluster["cluster_dir"]),
            "weight_candidate_cluster_input_snapshot.json",
            decision.CLUSTER_VIEWS,
            lambda: decision.validate_weight_candidate_cluster_artifact(
                cluster_id=cluster["cluster_id"],
                output_dir=tmp_path / "weight_candidate_cluster",
            ),
        ),
        (
            Path(interpretation["interpretation_dir"]),
            "weight_top_candidate_interpretation_input_snapshot.json",
            decision.INTERPRETATION_VIEWS,
            lambda: decision.validate_weight_top_candidate_interpretation_artifact(
                interpretation_id=interpretation["interpretation_id"],
                output_dir=tmp_path / "weight_top_candidate_interpretation",
            ),
        ),
        (
            Path(gate["promotion_gate_dir"]),
            "weight_method_promotion_gate_input_snapshot.json",
            decision.GATE_VIEWS,
            lambda: decision.validate_weight_method_promotion_gate_artifact(
                promotion_gate_id=gate["promotion_gate_id"],
                output_dir=tmp_path / "weight_method_promotion_gate",
            ),
        ),
        (
            Path(plan["plan_dir"]),
            "formal_method_auto_plan_input_snapshot.json",
            decision.PLAN_VIEWS,
            lambda: decision.validate_formal_method_auto_plan_artifact(
                plan_id=plan["plan_id"],
                output_dir=tmp_path / "formal_method_auto_plan",
            ),
        ),
        (
            Path(dashboard["dashboard_dir"]),
            "weight_search_dashboard_input_snapshot.json",
            decision.DASHBOARD_VIEWS,
            lambda: decision.validate_weight_search_dashboard_artifact(
                dashboard_id=dashboard["dashboard_id"],
                output_dir=tmp_path / "weight_search_dashboard",
            ),
        ),
        (
            Path(owner["owner_pack_dir"]),
            "owner_research_decision_pack_input_snapshot.json",
            decision.OWNER_VIEWS,
            lambda: decision.validate_owner_research_decision_pack_artifact(
                owner_pack_id=owner["owner_pack_id"],
                output_dir=tmp_path / "owner_research_decision_pack",
            ),
        ),
    ]

    for _root, _snapshot_name, _views, validate in artifacts:
        assert validate()["status"] == "PASS"

    for root, snapshot_name, views, validate in artifacts:
        for name in views:
            _tamper_and_restore(root / name, validate)
        _tamper_snapshot(
            root / snapshot_name,
            validate,
            lambda payload: payload.__setitem__("schema_version", "tampered.v0"),
        )

    cluster_root, cluster_snapshot, _views, cluster_validate = artifacts[0]
    _tamper_snapshot(
        cluster_root / cluster_snapshot,
        cluster_validate,
        lambda payload: payload["robustness_source"].__setitem__(
            "artifact_id", "cross-lineage-robustness"
        ),
    )

    dashboard_root, dashboard_snapshot, _views, dashboard_validate = artifacts[4]
    _tamper_snapshot(
        dashboard_root / dashboard_snapshot,
        dashboard_validate,
        lambda payload: payload["promotion_gate_source"].__setitem__(
            "artifact_id", "cross-lineage-promotion-gate"
        ),
    )

    owner_root, owner_snapshot, _views, owner_validate = artifacts[5]
    _tamper_snapshot(
        owner_root / owner_snapshot,
        owner_validate,
        lambda payload: payload["dashboard_source"].__setitem__(
            "artifact_id", "cross-lineage-dashboard"
        ),
    )

    assert plan["manifest"]["implemented"] is False
    assert owner["manifest"]["broker_action_allowed"] is False
    assert owner["manifest"]["production_effect"] == "none"
