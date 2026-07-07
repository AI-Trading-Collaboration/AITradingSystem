from __future__ import annotations

from pathlib import Path

from ai_trading_system.research_quality.growth_tilt_engine_source_traceability_remediation import (
    build_growth_tilt_source_traceability_remediation,
)


def test_source_traceability_remediation_identifies_and_updates_gap_features(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)

    payload = build_growth_tilt_source_traceability_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        project_root=tmp_path,
        report_registry=_report_registry(),
    )

    assert payload["source_traceability_gap_count"] == 7
    assert payload["source_traceability_remediated_count"] == 3
    assert payload["remaining_source_traceability_gap_count"] == 4
    assert payload["remaining_blocked_or_gap_count"] == 7
    assert payload["contract_ready_count"] == 0
    assert payload["source_traceability_remediation_completed"] is True

    records = payload["source_traceability_remediation_records"]
    assert [record["feature_id"] for record in records] == [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    status_by_feature = {
        record["feature_id"]: record["source_traceability_remediation_status"]
        for record in records
    }
    assert status_by_feature["equal_risk_baseline_weights"] == (
        "source_traceability_remediated"
    )
    assert status_by_feature["target_vol_policy"] == "source_traceability_remediated"
    assert status_by_feature["risk_on_trend_filter_context"] == (
        "source_traceability_remediated"
    )
    assert status_by_feature["trend_features"] == (
        "source_traceability_blocked_by_missing_upstream_artifact"
    )
    assert status_by_feature["volatility_inputs"] == (
        "source_traceability_blocked_by_missing_upstream_artifact"
    )
    assert status_by_feature["drawdown_features"] == (
        "source_traceability_blocked_by_missing_upstream_artifact"
    )
    assert status_by_feature["growth_tilt_engine_signal_artifact"] == (
        "source_traceability_blocked_by_missing_upstream_artifact"
    )

    equal_risk = next(
        row
        for row in payload["source_traceability_contract_metadata"]["metadata_rows"]
        if row["source_feature_id"] == "equal_risk_baseline_weights"
    )
    assert equal_risk["upstream_config_path"] == (
        "config/research/equal_risk_growth_tilt_candidate_registry.yaml"
    )
    assert equal_risk["upstream_config_key"] == "research_policy.equal_risk"
    assert equal_risk["source_snapshot_hash"].startswith("sha256:")
    assert equal_risk["fresh_market_data_required"] is False


def test_source_traceability_remediation_preserves_as_of_and_blocks_contract_ready(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)

    payload = build_growth_tilt_source_traceability_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        project_root=tmp_path,
        report_registry=_report_registry(),
    )
    rows = {
        row["feature_id"]: row
        for row in payload["updated_source_feature_mapping"]["mapping_rows"]
    }

    assert rows["volatility_inputs"]["as_of_semantics_status"] == "ready"
    assert rows["drawdown_features"]["as_of_semantics_status"] == "ready"
    assert rows["equal_risk_baseline_weights"]["source_traceability_status"] == "ready"
    assert rows["target_vol_policy"]["source_traceability_status"] == "ready"
    assert rows["risk_on_trend_filter_context"]["source_traceability_status"] == "ready"
    assert rows["equal_risk_baseline_weights"]["validity_dependency_status"] != "ready"
    assert rows["equal_risk_baseline_weights"]["pit_gate_status"] != "ready"
    assert all(row["contract_ready"] is False for row in rows.values())
    assert payload["source_traceability_remediation_validation"]["as_of_status_rollback_count"] == 0


def test_source_traceability_remediation_missing_registry_blocks_readiness(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)

    payload = build_growth_tilt_source_traceability_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        project_root=tmp_path,
        report_registry={"reports": []},
    )

    records = payload["source_traceability_remediation_records"]
    assert any(
        record["source_traceability_remediation_status"]
        == "source_traceability_blocked_by_missing_registry_entry"
        for record in records
    )
    assert payload["source_traceability_remediated_count"] == 0
    assert payload["source_traceability_remediation_completed"] is True


def test_source_traceability_remediation_missing_config_key_blocks_feature(
    tmp_path: Path,
) -> None:
    config = tmp_path / "config" / "research"
    config.mkdir(parents=True)
    (config / "equal_risk_growth_tilt_candidate_registry.yaml").write_text(
        "research_policy:\n  equal_risk: {}\nsearch_grids: {}\n",
        encoding="utf-8",
    )

    payload = build_growth_tilt_source_traceability_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        project_root=tmp_path,
        report_registry=_report_registry(),
    )

    target_vol = next(
        record
        for record in payload["source_traceability_remediation_records"]
        if record["feature_id"] == "target_vol_policy"
    )
    assert target_vol["source_traceability_remediation_status"] == (
        "source_traceability_blocked_by_missing_upstream_artifact"
    )
    assert "missing governed config key" in (
        target_vol["after"]["source_traceability_contract_metadata"][
            "traceability_blocking_reason"
        ]
    )


def _remediation_plan_result() -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED",
        "gap_count": 7,
        "ordered_remediation_items": [
            _item(
                1,
                "equal_risk_baseline_weights",
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.equal_risk",
            ),
            _item(
                2,
                "target_vol_policy",
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "search_grids.vol_target_growth_tilt",
            ),
            _item(3, "trend_features", "historical price trend / momentum windows"),
        ],
    }


def _item(order: int, feature_id: str, upstream: str) -> dict[str, object]:
    return {
        "remediation_order": order,
        "feature_id": feature_id,
        "source_feature_name": feature_id,
        "current_mapping_status": "missing_source_traceability",
        "remediation_category": "source_traceability_required",
        "missing_source_traceability": True,
        "required_upstream_artifact": upstream,
    }


def _as_of_result() -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS",
        "input_gap_count": 7,
        "contract_ready_count": 0,
        "updated_source_feature_mapping": {"mapping_rows": _mapping_rows()},
    }


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("adjusted_prices", "mapped_with_caveats", "mapped_with_caveats"),
        _row("returns", "mapped_with_caveats", "mapped_with_caveats"),
        _row(
            "volatility_inputs",
            "mapped_with_caveats",
            "missing",
            source_traceability_status="not_ready_missing_source_snapshot",
            source_ref="rolling price-derived volatility features",
            as_of_status="ready",
        ),
        _row(
            "trend_features",
            "missing_source_traceability",
            "partial",
            source_ref="historical price trend / momentum windows",
        ),
        _row(
            "drawdown_features",
            "mapped_with_caveats",
            "missing",
            source_traceability_status="not_ready_missing_source_snapshot",
            source_ref="historical drawdown windows",
            as_of_status="ready",
        ),
        _row(
            "equal_risk_baseline_weights",
            "missing_source_traceability",
            "mapped_with_caveats",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.equal_risk"
            ),
        ),
        _row(
            "target_vol_policy",
            "missing_source_traceability",
            "mapped_with_caveats",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "search_grids.vol_target_growth_tilt"
            ),
        ),
        _row(
            "risk_on_trend_filter_context",
            "mapped_with_caveats",
            "missing",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.trend_filter_rule"
            ),
        ),
        _row("execution_signal_validity_policy", "blocked_unresolved", "mapped_with_caveats"),
        _row(
            "growth_tilt_engine_signal_artifact",
            "blocked_unresolved",
            "missing",
            source_system="missing_artifact",
            source_ref="missing standalone growth_tilt_engine signal artifact",
        ),
    ]


def _row(
    feature_id: str,
    mapping_status: str,
    traceability_status: str,
    *,
    source_traceability_status: str | None = None,
    source_system: str = "derived_research_artifact",
    source_ref: str | None = None,
    as_of_status: str | None = None,
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "mapping_status": mapping_status,
        "traceability_status": traceability_status,
        "source_traceability_status": source_traceability_status,
        "source_system": source_system,
        "upstream_artifact_or_registry_reference": source_ref or f"{feature_id} source",
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": None,
        "pit_gate_status": None,
        "contract_ready": False,
    }


def _report_registry() -> dict[str, object]:
    return {
        "reports": [
            _registry_entry("growth_tilt_engine_source_feature_contract_mapping"),
            _registry_entry("growth_tilt_engine_as_of_semantics_remediation"),
            _registry_entry("growth_tilt_engine_contract_gap_remediation_plan"),
        ]
    }


def _registry_entry(report_id: str) -> dict[str, str]:
    return {"report_id": report_id, "production_effect": "none", "broker_action": "none"}


def _write_config(tmp_path: Path) -> None:
    config = tmp_path / "config" / "research"
    config.mkdir(parents=True)
    (config / "equal_risk_growth_tilt_candidate_registry.yaml").write_text(
        "\n".join(
            [
                "research_policy:",
                "  equal_risk:",
                "    enabled: true",
                "  trend_filter_rule:",
                "    enabled: true",
                "search_grids:",
                "  vol_target_growth_tilt:",
                "    enabled: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
