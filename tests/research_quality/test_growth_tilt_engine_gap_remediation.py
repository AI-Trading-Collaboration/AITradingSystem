from __future__ import annotations

from ai_trading_system.research_quality.growth_tilt_engine_gap_remediation import (
    ALLOWED_REMEDIATION_CATEGORIES,
    build_growth_tilt_contract_gap_remediation_plan,
    validate_growth_tilt_gap_remediation_plan,
)


def test_gap_remediation_plan_inventory_and_safety() -> None:
    mapping_result = _mapping_result(
        [
            _row("ready_reference", "mapped_with_caveats"),
            _row("trace_gap", "missing_source_traceability"),
            _row("as_of_gap", "missing_as_of_semantics"),
            _row("validity_gap", "missing_validity_dependency"),
            _row("ambiguous_gap", "ambiguous_source_feature"),
            _row(
                "blocked_signal_artifact",
                "blocked_unresolved",
                feature_type="SIGNAL_ARTIFACT_CONTRACT",
            ),
        ],
        blocked_or_gap_count=5,
    )

    plan = build_growth_tilt_contract_gap_remediation_plan(mapping_result)
    items = plan["ordered_remediation_items"]
    categories = {item["remediation_category"] for item in items}

    assert plan["gap_count"] == 5
    assert [item["feature_id"] for item in items] == [
        "trace_gap",
        "as_of_gap",
        "validity_gap",
        "ambiguous_gap",
        "blocked_signal_artifact",
    ]
    assert categories <= set(ALLOWED_REMEDIATION_CATEGORIES)
    assert "ready_reference" not in {item["feature_id"] for item in items}
    assert plan["remediation_plan_validation"]["valid"] is True
    assert plan["remediation_plan_validation"]["unclassified_remediation_item_count"] == 0
    assert plan["remediation_plan_validation"]["silent_gap_resolution_count"] == 0
    assert plan["remediation_plan_validation"]["silent_blocker_downgrade_count"] == 0
    assert plan["growth_tilt_engine_blocker_resolved"] is False
    assert plan["growth_tilt_engine_blocker_downgraded"] is False
    assert plan["valid_until_window_blocker_resolved"] is False
    assert plan["valid_until_window_blocker_downgraded"] is False
    assert plan["candidate_search_enabled"] is False
    assert plan["production_enabled"] is False
    assert plan["broker_enabled"] is False
    for item in items:
        assert item["blocks_contract_ready"] is True
        assert item["blocks_pit_gate"] is True
        assert item["can_be_implemented_without_fresh_market_data"] is True
        assert item["gap_resolved_in_2411"] is False
        assert item["blocker_downgraded_in_2411"] is False
        assert item["remediation_action"]
        assert item["validation_requirement"]


def test_gap_remediation_ordering_is_deterministic() -> None:
    mapping_result = _mapping_result(
        [
            _row("z_blocked", "blocked_unresolved"),
            _row("b_as_of", "missing_as_of_semantics"),
            _row("a_trace", "missing_source_traceability"),
            _row("c_validity", "missing_validity_dependency"),
            _row("d_ambiguous", "ambiguous_source_feature"),
        ],
        blocked_or_gap_count=5,
    )

    plan = build_growth_tilt_contract_gap_remediation_plan(mapping_result)
    items = plan["ordered_remediation_items"]

    assert [item["remediation_order"] for item in items] == [1, 2, 3, 4, 5]
    assert [
        (item["feature_id"], item["remediation_category"])
        for item in items
    ] == [
        ("a_trace", "source_traceability_required"),
        ("b_as_of", "as_of_semantics_required"),
        ("c_validity", "validity_dependency_required"),
        ("d_ambiguous", "ambiguous_feature_boundary_requires_owner_review"),
        ("z_blocked", "blocked_pending_prior_remediation"),
    ]


def test_gap_remediation_validator_rejects_silent_resolution_or_downgrade() -> None:
    plan = build_growth_tilt_contract_gap_remediation_plan(
        _mapping_result(
            [
                _row("trace_gap", "missing_source_traceability"),
                _row("as_of_gap", "missing_as_of_semantics"),
            ],
            blocked_or_gap_count=2,
        )
    )
    items = [dict(item) for item in plan["ordered_remediation_items"]]
    items[0]["gap_resolved_in_2411"] = True
    items[1]["blocker_downgraded_in_2411"] = True

    validation = validate_growth_tilt_gap_remediation_plan(
        items,
        expected_gap_count=2,
    )
    codes = {error["code"] for error in validation["errors"]}

    assert validation["valid"] is False
    assert "SILENT_GAP_RESOLUTION" in codes
    assert "SILENT_BLOCKER_DOWNGRADE" in codes


def test_gap_remediation_validator_detects_count_mismatch_and_unclassified() -> None:
    validation = validate_growth_tilt_gap_remediation_plan(
        [
            {
                "remediation_order": 1,
                "feature_id": "bad_item",
                "remediation_category": "unknown_category",
                "remediation_action": "document bad item",
                "required_upstream_artifact": "artifact",
                "required_code_doc_config_change": "docs",
                "validation_requirement": "validator",
                "dependency_ordering": "phase",
                "blocker_impact": "blocks readiness",
                "gap_resolved_in_2411": False,
                "blocker_downgraded_in_2411": False,
                "blocks_contract_ready": True,
            }
        ],
        expected_gap_count=2,
    )
    codes = {error["code"] for error in validation["errors"]}

    assert validation["valid"] is False
    assert "UNCLASSIFIED_REMEDIATION_ITEM" in codes
    assert "GAP_COUNT_MISMATCH" in codes


def _mapping_result(
    rows: list[dict[str, object]],
    *,
    blocked_or_gap_count: int,
) -> dict[str, object]:
    return {
        "status": "GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_READY",
        "known_source_feature_count": len(rows),
        "blocked_or_gap_count": blocked_or_gap_count,
        "source_feature_contract_mapping": {"mapping_rows": rows},
        "contract_mapping_validation": {
            "blocked_or_gap_count": blocked_or_gap_count,
            "unclassified_feature_count": 0,
            "valid": True,
        },
    }


def _row(
    feature_id: str,
    mapping_status: str,
    *,
    feature_type: str = "TECHNICAL_FEATURES",
    pit_eligibility: str = "UNKNOWN_OR_APPROXIMATE_PIT",
    upstream_reference: str = "outputs/research_strategies/source.json",
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id.replace("_", " "),
        "feature_type": feature_type,
        "mapping_status": mapping_status,
        "upstream_artifact_or_registry_reference": upstream_reference,
        "as_of_semantics": "missing source cutoff",
        "source_snapshot_requirement": "source manifest required",
        "source_system": "prior_artifact",
        "traceability_status": mapping_status,
        "validity_dependency": (
            "valid_until_window" if "validity" in feature_id else "none"
        ),
        "pit_eligibility": pit_eligibility,
        "mapping_status_reasons": [mapping_status],
    }
