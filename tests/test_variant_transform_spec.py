from __future__ import annotations

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_variant_transform_spec_validates_and_writes_catalog(tmp_path) -> None:
    config_validation = system_target.validate_weight_variant_transform_spec_config()
    assert config_validation["status"] == "PASS"

    result = system_target.build_variant_transform_spec_report(
        output_dir=tmp_path / "variant_transform_spec",
    )

    manifest = result["manifest"]
    catalog = result["transform_type_catalog"]
    transform_types = {row["type"] for row in catalog["transform_types"]}

    assert manifest["status"] == "PASS"
    assert set(system_target.DEFAULT_TRANSFORM_TYPES).issubset(transform_types)
    assert all(row["required_fields"] for row in catalog["transform_types"])
    assert manifest["experiment_only"] is True
    assert manifest["not_formal_research_method"] is True
    assert manifest["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"

    validation = system_target.validate_variant_transform_spec_artifact(
        spec_id=result["spec_id"],
        output_dir=tmp_path / "variant_transform_spec",
    )
    assert validation["status"] == "PASS"
