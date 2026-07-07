from __future__ import annotations

from ai_trading_system.research_quality.growth_tilt_engine_contract_mapping import (
    ALLOWED_MAPPING_STATUSES,
    build_growth_tilt_feature_mapping_row,
    build_growth_tilt_source_feature_contract_mapping,
    validate_growth_tilt_feature_mapping,
)


def test_growth_tilt_mapping_contract_ready_path() -> None:
    row = build_growth_tilt_feature_mapping_row(
        _source_feature(
            feature_id="ready_price_feature",
            pit_status="TRUE_PIT",
            pit_confidence="HIGH",
        )
    )

    assert row["mapping_status"] == "mapped_contract_ready"
    assert row["contract_validation_result"]["valid"] is True
    assert row["pit_eligibility"] == "TRUE_PIT"


def test_growth_tilt_mapping_missing_as_of_path() -> None:
    row = build_growth_tilt_feature_mapping_row(
        _source_feature(
            feature_id="missing_as_of_feature",
            as_of_handling="missing source cutoff and signal-time observation",
            recommended_action="missing as-of semantics must be remediated",
        )
    )

    assert row["mapping_status"] == "missing_as_of_semantics"
    assert row["blocking_reason_if_unresolved"] == ["as-of semantics are missing"]


def test_growth_tilt_mapping_ambiguous_path() -> None:
    row = build_growth_tilt_feature_mapping_row(
        _source_feature(
            feature_id="ambiguous_signal_artifact",
            feature_type="SIGNAL_ARTIFACT_CONTRACT",
            source_config_or_artifact="TBD signal artifact registry",
            pit_status="APPROXIMATE_PIT",
            pit_confidence="MEDIUM",
        )
    )

    assert row["mapping_status"] == "ambiguous_source_feature"
    assert row["contract_validation_result"]["valid"] is True


def test_growth_tilt_mapping_blocked_path() -> None:
    row = build_growth_tilt_feature_mapping_row(
        _source_feature(
            feature_id="blocked_validity_policy",
            feature_type="VALIDITY_POLICY",
            pit_status="UNKNOWN_OR_APPROXIMATE_PIT",
            pit_confidence="LOW",
            severity="BLOCKING",
            recommended_action="valid_until dependency is unresolved",
        )
    )

    assert row["mapping_status"] == "blocked_unresolved"
    assert row["contract_validation_result"]["valid"] is True
    assert "remains BLOCKING" in row["blocking_reason_if_unresolved"][0]


def test_growth_tilt_mapping_build_has_no_unclassified_features() -> None:
    mapping = build_growth_tilt_source_feature_contract_mapping(
        [
            _source_feature("ready_price_feature", pit_status="TRUE_PIT", pit_confidence="HIGH"),
            _source_feature(
                "missing_as_of_feature",
                as_of_handling="missing source cutoff",
            ),
            _source_feature(
                "ambiguous_signal_artifact",
                feature_type="SIGNAL_ARTIFACT_CONTRACT",
                source_config_or_artifact="TBD signal artifact registry",
            ),
            _source_feature(
                "blocked_validity_policy",
                feature_type="VALIDITY_POLICY",
                pit_status="UNKNOWN_OR_APPROXIMATE_PIT",
                pit_confidence="LOW",
                severity="BLOCKING",
                recommended_action="valid_until dependency remains unresolved",
            ),
        ]
    )

    validation = mapping["contract_mapping_validation"]
    statuses = {row["mapping_status"] for row in mapping["mapping_rows"]}
    assert statuses <= set(ALLOWED_MAPPING_STATUSES)
    assert validation["valid"] is True
    assert validation["unclassified_feature_count"] == 0
    assert mapping["blockers_resolved"] is False
    assert mapping["blockers_downgraded"] is False


def test_growth_tilt_mapping_validator_rejects_ready_without_true_pit() -> None:
    result = validate_growth_tilt_feature_mapping(
        [
            {
                "feature_id": "bad_ready_feature",
                "mapping_status": "mapped_contract_ready",
                "contract_payload": {"pit_status": "APPROXIMATE_PIT"},
                "contract_validation_result": {"valid": True},
            }
        ]
    )

    assert result["valid"] is False
    assert any(
        error["code"] == "READY_STATUS_WITHOUT_PIT_ELIGIBILITY"
        for error in result["errors"]
    )


def _source_feature(
    feature_id: str = "source_feature",
    *,
    feature_type: str = "TECHNICAL_FEATURES",
    source_config_or_artifact: str = "config/research/test_feature.yaml",
    as_of_handling: str = "explicit as-of timestamp at signal construction",
    generated_at_handling: str = "explicit generated_at from pipeline run",
    pit_status: str = "APPROXIMATE_PIT",
    pit_confidence: str = "MEDIUM",
    severity: str = "MATERIAL",
    recommended_action: str = "record source snapshot before promotion review",
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_type": feature_type,
        "source_config_or_artifact": source_config_or_artifact,
        "as_of_handling": as_of_handling,
        "generated_at_handling": generated_at_handling,
        "lookback_window": 63,
        "forward_window_used": "none",
        "pit_status": pit_status,
        "pit_confidence": pit_confidence,
        "revision_or_backfill_risk": "LOW",
        "severity": severity,
        "recommended_action": recommended_action,
        "used_by_growth_tilt_engine": True,
    }
