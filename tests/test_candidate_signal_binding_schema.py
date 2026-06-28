from __future__ import annotations

from ai_trading_system.candidate_signal_binding_schema import (
    REQUIRED_PROVENANCE_FIELDS,
    REQUIRED_SIGNAL_FIELDS,
    candidate_bound_prediction_artifact_contract_dict,
    candidate_bound_signal_series_contract_dict,
    candidate_signal_binding_schema_dict,
)


def test_candidate_signal_binding_schema_declares_required_fields() -> None:
    schema = candidate_signal_binding_schema_dict()

    for field in (
        "candidate_id",
        "source_artifact_hash",
        "as_of_timestamp",
        "decision_timestamp",
        "horizon",
        "signal_spec_version",
        "prediction_schema_version",
        "provenance",
    ):
        assert field in schema["required_fields"]
        assert field in REQUIRED_SIGNAL_FIELDS

    for field in (
        "regeneration_mode",
        "pit_policy",
        "candidate_binding_method",
        "source_schema_status",
        "promotion_eligible",
    ):
        assert field in schema["provenance"]["required_fields"]
        assert field in REQUIRED_PROVENANCE_FIELDS

    assert schema["safety_boundary"][
        "schema_migration_poc_requires_promotion_eligible_false"
    ] is True


def test_candidate_bound_contracts_are_fail_closed() -> None:
    signal_contract = candidate_bound_signal_series_contract_dict()
    prediction_contract = candidate_bound_prediction_artifact_contract_dict()

    assert signal_contract["artifact_type"] == "candidate_bound_signal_series"
    assert signal_contract["file_format"] == "csv"
    assert "provenance" in signal_contract["required_columns"]
    assert "promotion_allowed" in signal_contract["required_columns"]
    assert prediction_contract["artifact_type"] == "candidate_bound_prediction_artifact"
    assert "prediction_records" in prediction_contract["required_top_level_fields"]
    assert any(
        "promotion_eligible" in rule and "false" in rule
        for rule in prediction_contract["validation_rules"]
    )
