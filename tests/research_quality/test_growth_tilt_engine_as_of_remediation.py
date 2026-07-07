from __future__ import annotations

from ai_trading_system.research_quality.growth_tilt_engine_as_of_remediation import (
    ALLOWED_AS_OF_REMEDIATION_STATUSES,
    build_growth_tilt_as_of_semantics_remediation,
    validate_growth_tilt_as_of_semantics_remediation,
)


def test_as_of_remediation_identifies_and_updates_missing_as_of_features() -> None:
    result = build_growth_tilt_as_of_semantics_remediation(
        _remediation_result(
            [
                _item("trend_features", "source_traceability_required", 1),
                _item("drawdown_features", "as_of_semantics_required", 4),
                _item("volatility_inputs", "as_of_semantics_required", 5),
            ],
            gap_count=3,
        ),
        _mapping_result(),
        as_of_date="2026-07-08",
    )

    assert result["as_of_gap_count"] == 2
    assert result["as_of_remediated_count"] == 2
    assert result["remaining_blocked_or_gap_count"] == 3
    assert result["contract_ready_count"] == 0
    assert result["as_of_remediation_validation"]["valid"] is True

    records = result["as_of_remediation_records"]
    assert [record["feature_id"] for record in records] == [
        "drawdown_features",
        "volatility_inputs",
    ]
    for record in records:
        metadata = record["after"]["as_of_contract_metadata"]
        assert record["as_of_remediation_status"] in ALLOWED_AS_OF_REMEDIATION_STATUSES
        assert record["as_of_remediation_status"] == "as_of_semantics_remediated"
        assert record["as_of_semantics_status_before"] == "missing"
        assert record["as_of_semantics_status_after"] == "ready"
        assert metadata["as_of_date"] == "2026-07-08"
        assert metadata["lookahead_allowed"] is False
        assert metadata["forward_window_used"] is False
        assert metadata["pit_safe"] == "unknown"
        assert metadata["contract_ready"] is False
        assert record["contract_ready"] is False


def test_as_of_remediation_does_not_mark_unrelated_dimensions_ready() -> None:
    result = build_growth_tilt_as_of_semantics_remediation(
        _remediation_result([_item("drawdown_features", "as_of_semantics_required", 4)]),
        _mapping_result(),
        as_of_date="2026-07-08",
    )

    updated_rows = {
        row["feature_id"]: row
        for row in result["updated_source_feature_mapping"]["mapping_rows"]
    }
    drawdown = updated_rows["drawdown_features"]
    trace = updated_rows["trend_features"]

    assert drawdown["as_of_semantics_status"] == "ready"
    assert drawdown["source_traceability_status"] == "not_ready_missing_source_snapshot"
    assert drawdown["validity_dependency_status"] == "not_assessed_in_2412"
    assert drawdown["pit_gate_status"] == "blocked_pending_pit_evidence"
    assert drawdown["contract_ready"] is False
    assert trace["mapping_status"] == "missing_source_traceability"
    assert trace["contract_ready"] is False


def test_as_of_remediation_enforces_no_lookahead_contract() -> None:
    result = build_growth_tilt_as_of_semantics_remediation(
        _remediation_result([_item("drawdown_features", "as_of_semantics_required", 4)]),
        _mapping_result(),
        as_of_date="2026-07-08",
    )
    records = [dict(result["as_of_remediation_records"][0])]
    records[0] = {
        **records[0],
        "after": {
            **records[0]["after"],
            "as_of_contract_metadata": {
                **records[0]["after"]["as_of_contract_metadata"],
                "lookahead_allowed": True,
            },
        },
    }

    validation = validate_growth_tilt_as_of_semantics_remediation(
        records,
        result["updated_source_feature_mapping"],
        expected_as_of_gap_count=1,
    )
    codes = {error["code"] for error in validation["errors"]}

    assert validation["valid"] is False
    assert "LOOKAHEAD_NOT_DISABLED" in codes


def test_as_of_remediation_ordering_is_deterministic() -> None:
    result = build_growth_tilt_as_of_semantics_remediation(
        _remediation_result(
            [
                _item("volatility_inputs", "as_of_semantics_required", 5),
                _item("drawdown_features", "as_of_semantics_required", 4),
            ],
            gap_count=2,
        ),
        _mapping_result(),
        as_of_date="2026-07-08",
    )

    assert [
        (record["remediation_order"], record["feature_id"])
        for record in result["as_of_remediation_records"]
    ] == [(1, "drawdown_features"), (2, "volatility_inputs")]


def _remediation_result(
    items: list[dict[str, object]],
    *,
    gap_count: int | None = None,
) -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_READY",
        "gap_count": gap_count if gap_count is not None else len(items),
        "ordered_remediation_items": items,
    }


def _item(
    feature_id: str,
    category: str,
    order: int,
) -> dict[str, object]:
    return {
        "remediation_order": order,
        "feature_id": feature_id,
        "source_feature_name": feature_id,
        "current_mapping_status": (
            "missing_as_of_semantics"
            if category == "as_of_semantics_required"
            else "missing_source_traceability"
        ),
        "remediation_category": category,
        "missing_as_of_semantics": category == "as_of_semantics_required",
        "required_upstream_artifact": f"source:{feature_id}",
    }


def _mapping_result() -> dict[str, object]:
    rows = [
        _row("trend_features", "missing_source_traceability"),
        _row("drawdown_features", "missing_as_of_semantics"),
        _row("volatility_inputs", "missing_as_of_semantics"),
    ]
    return {
        "task_id": "TRADING-2410",
        "status": "GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY",
        "source_feature_contract_mapping": {"mapping_rows": rows},
    }


def _row(feature_id: str, mapping_status: str) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "feature_type": "TECHNICAL_FEATURES",
        "mapping_status": mapping_status,
        "mapping_status_reasons": [mapping_status],
        "as_of_semantics": "missing",
        "traceability_status": "missing",
        "validity_dependency": "none_identified_in_2410",
        "pit_eligibility": "APPROXIMATE_PIT",
        "upstream_artifact_or_registry_reference": f"source:{feature_id}",
        "contract_payload": {
            "lookback_window": f"{feature_id}_lookback",
            "forward_window_used": False,
        },
    }
