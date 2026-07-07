from __future__ import annotations

from ai_trading_system.research_quality import (
    growth_tilt_engine_signal_validity_dependency_remediation as validity_dependency,
)


def test_signal_validity_dependency_remediation_classifies_all_targets() -> None:
    payload = validity_dependency.build_growth_tilt_signal_validity_dependency_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        _source_traceability_result(),
    )

    assert payload["input_gap_count"] == 7
    assert payload["validity_dependency_gap_count"] == 8
    assert payload["validity_dependency_remediated_count"] == 2
    assert payload["validity_dependency_blocked_by_valid_until_window_count"] == 1
    assert payload["validity_dependency_blocked_by_source_traceability_count"] == 5
    assert payload["remaining_blocked_or_gap_count"] == 7
    assert payload["contract_ready_count"] == 0
    assert payload["signal_validity_dependency_remediation_completed"] is True

    records = payload["signal_validity_dependency_remediation_records"]
    assert [record["feature_id"] for record in records] == [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "execution_signal_validity_policy",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    status_by_feature = {
        record["feature_id"]: record["validity_dependency_remediation_status"]
        for record in records
    }
    assert status_by_feature["equal_risk_baseline_weights"] == (
        "validity_dependency_remediated"
    )
    assert status_by_feature["risk_on_trend_filter_context"] == (
        "validity_dependency_remediated"
    )
    assert status_by_feature["target_vol_policy"] == (
        "validity_dependency_blocked_by_missing_source_traceability"
    )
    assert status_by_feature["execution_signal_validity_policy"] == (
        "validity_dependency_blocked_by_valid_until_window"
    )

    execution = next(
        row
        for row in payload["signal_validity_dependency_contract_metadata"][
            "metadata_rows"
        ]
        if row["source_feature_id"] == "execution_signal_validity_policy"
    )
    assert execution["valid_until_required"] is True
    assert execution["valid_until_available"] is False
    assert execution["validity_dependency_status"] == "blocked"
    assert execution["validity_blocking_reason"] == "valid_until_window_unresolved"


def test_signal_validity_dependency_preserves_prior_dimensions_and_blocks_contract_ready() -> None:
    payload = validity_dependency.build_growth_tilt_signal_validity_dependency_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        _source_traceability_result(),
    )
    rows = {
        row["feature_id"]: row
        for row in payload["updated_source_feature_mapping"]["mapping_rows"]
    }

    assert rows["volatility_inputs"]["as_of_semantics_status"] == "ready"
    assert rows["drawdown_features"]["as_of_semantics_status"] == "ready"
    assert rows["equal_risk_baseline_weights"]["source_traceability_status"] == "ready"
    assert rows["risk_on_trend_filter_context"]["source_traceability_status"] == "ready"
    assert rows["equal_risk_baseline_weights"]["validity_dependency_status"] == "ready"
    assert rows["risk_on_trend_filter_context"]["validity_dependency_status"] == "ready"
    assert rows["execution_signal_validity_policy"]["validity_dependency_status"] == (
        "blocked"
    )
    assert rows["equal_risk_baseline_weights"]["pit_gate_status"] != "ready"
    assert all(row["contract_ready"] is False for row in rows.values())
    validation = payload["signal_validity_dependency_remediation_validation"]
    assert validation["as_of_status_rollback_count"] == 0
    assert validation["source_traceability_status_rollback_count"] == 0


def test_signal_validity_dependency_ambiguous_boundary_stays_blocked() -> None:
    source_result = _source_traceability_result()
    rows = source_result["updated_source_feature_mapping"]["mapping_rows"]
    trend = next(row for row in rows if row["feature_id"] == "trend_features")
    trend["mapping_status"] = "ambiguous_source_feature"
    trend["mapping_status_reasons"] = ["ambiguous signal boundary"]

    payload = validity_dependency.build_growth_tilt_signal_validity_dependency_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        source_result,
    )

    trend_record = next(
        record
        for record in payload["signal_validity_dependency_remediation_records"]
        if record["feature_id"] == "trend_features"
    )
    assert trend_record["validity_dependency_remediation_status"] == (
        "validity_dependency_blocked_by_ambiguous_signal_boundary"
    )
    metadata = trend_record["after"]["signal_validity_dependency_contract_metadata"]
    assert metadata["validity_dependency_status"] == "blocked"
    assert metadata["contract_ready"] is False


def test_signal_validity_dependency_valid_until_missing_cannot_be_ready() -> None:
    payload = validity_dependency.build_growth_tilt_signal_validity_dependency_remediation(
        _remediation_plan_result(),
        _as_of_result(),
        _source_traceability_result(),
    )

    execution = next(
        record
        for record in payload["signal_validity_dependency_remediation_records"]
        if record["feature_id"] == "execution_signal_validity_policy"
    )
    metadata = execution["after"]["signal_validity_dependency_contract_metadata"]
    assert metadata["valid_until_required"] is True
    assert metadata["valid_until_available"] is False
    assert metadata["validity_dependency_status"] == "blocked"
    assert execution["contract_ready"] is False
    assert (
        "validity_dependency_status"
        in execution["after"]["contract_ready_blocking_dimensions"]
    )


def _remediation_plan_result() -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY_BLOCKERS_UNRESOLVED",
        "gap_count": 7,
        "ordered_remediation_items": [
            _item(6, "execution_signal_validity_policy", "validity_dependency_required")
        ],
    }


def _item(order: int, feature_id: str, category: str) -> dict[str, object]:
    return {
        "remediation_order": order,
        "feature_id": feature_id,
        "source_feature_name": feature_id,
        "current_mapping_status": "missing_validity_dependency",
        "remediation_category": category,
        "missing_validity_dependency": True,
        "required_upstream_artifact": (
            "signal_validity_contract artifact and valid_until_window remediation result"
        ),
        "validation_requirement": "valid_until_window must be available",
    }


def _as_of_result() -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS",
        "input_gap_count": 7,
        "contract_ready_count": 0,
    }


def _source_traceability_result() -> dict[str, object]:
    rows = _mapping_rows()
    return {
        "status": (
            "GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_READY_WITH_REMAINING_BLOCKERS"
        ),
        "task_id": "TRADING-2413",
        "input_gap_count": 7,
        "source_traceability_gap_count": 7,
        "source_traceability_remediated_count": 2,
        "remaining_source_traceability_gap_count": 5,
        "remaining_blocked_or_gap_count": 7,
        "contract_ready_count": 0,
        "updated_source_feature_mapping": {
            "contract_ready_count": 0,
            "mapping_rows": rows,
        },
        "source_traceability_remediation": {
            "source_traceability_remediation_records": _source_records(rows)
        },
    }


def _source_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    feature_order = [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    row_by_feature = {row["feature_id"]: row for row in rows}
    return [
        {
            "remediation_order": index,
            "feature_id": feature_id,
            "after": row_by_feature[feature_id],
        }
        for index, feature_id in enumerate(feature_order, start=1)
    ]


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("adjusted_prices", "mapped_with_caveats", "mapped_with_caveats"),
        _row("returns", "mapped_with_caveats", "mapped_with_caveats"),
        _row(
            "volatility_inputs",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            as_of_status="ready",
            validity_status="not_assessed_in_2412",
        ),
        _row(
            "trend_features",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
        ),
        _row(
            "drawdown_features",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            as_of_status="ready",
            validity_status="not_assessed_in_2412",
        ),
        _row(
            "equal_risk_baseline_weights",
            "mapped_with_caveats",
            "ready",
            source_traceability_status="ready",
            source_traceability_remediation_status="source_traceability_remediated",
            validity_status="not_assessed_in_2413",
            source_snapshot_reference="config:equal_risk@sha256:abc",
        ),
        _row(
            "target_vol_policy",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
        ),
        _row(
            "risk_on_trend_filter_context",
            "mapped_with_caveats",
            "ready",
            source_traceability_status="ready",
            source_traceability_remediation_status="source_traceability_remediated",
            validity_status="not_assessed_in_2413",
            source_snapshot_reference="config:trend_filter@sha256:abc",
        ),
        _row(
            "execution_signal_validity_policy",
            "blocked_unresolved",
            "mapped_with_caveats",
            validity_status=None,
            validity_dependency="depends_on_valid_until_window_contract",
        ),
        _row(
            "growth_tilt_engine_signal_artifact",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
            source_system="missing_artifact",
        ),
    ]


def _row(
    feature_id: str,
    mapping_status: str,
    traceability_status: str,
    *,
    source_traceability_status: str | None = None,
    source_traceability_remediation_status: str | None = None,
    source_system: str = "derived_research_artifact",
    as_of_status: str | None = None,
    validity_status: str | None = None,
    validity_dependency: str | None = None,
    source_snapshot_reference: str | None = None,
) -> dict[str, object]:
    metadata = {
        "source_snapshot_reference": source_snapshot_reference,
        "source_snapshot_hash": "sha256:abc" if source_snapshot_reference else None,
    }
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "mapping_status": mapping_status,
        "traceability_status": traceability_status,
        "source_traceability_status": source_traceability_status,
        "source_traceability_remediation_status": source_traceability_remediation_status,
        "source_system": source_system,
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": validity_status,
        "validity_dependency": validity_dependency,
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "contract_ready": False,
        "source_traceability_contract_metadata": metadata,
    }
