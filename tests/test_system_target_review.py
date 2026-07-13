from __future__ import annotations

from datetime import UTC, datetime

import pytest
from dynamic_v3_system_target_helpers import (
    TARGET_AS_OF,
    report_index_for_review_fixture,
    run_review_fixture,
    write_model_target_config,
    write_target_source_artifacts,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_system_target_review_pack_and_reader_brief_summary(tmp_path) -> None:
    fixture = run_review_fixture(tmp_path)
    review = fixture["review"]
    decision = review["system_target_decision"]

    assert decision["decision_status"] == "CONTINUE_OBSERVATION"
    assert decision["recommended_research_method"] == "limited_adjustment"
    assert decision["broker_action_allowed"] is False
    assert decision["broker_action_taken"] is False
    assert decision["not_official_target_weights"] is True

    validation = system_target.validate_system_target_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "system_target_review",
    )
    assert validation["status"] == "PASS"

    summary = reader_brief._etf_dynamic_v3_system_target_summary(
        report_index_for_review_fixture(fixture)
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["review_id"] == review["review_id"]
    assert summary["recommended_research_method"] == "limited_adjustment"
    assert summary["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert summary["broker_action_allowed"] is False
    assert summary["not_official_target_weights"] is True
    assert summary["safety_status"].startswith("research_target_only=true")


def test_system_target_review_rejects_cross_lineage_target(tmp_path) -> None:
    fixture = run_review_fixture(tmp_path)
    alternate_root = tmp_path / "alternate"
    source_dirs = write_target_source_artifacts(alternate_root)
    alternate_target = system_target.generate_model_target(
        config_path=write_model_target_config(alternate_root),
        as_of=TARGET_AS_OF,
        output_dir=alternate_root / "model_target",
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
        **source_dirs,
    )

    with pytest.raises(ValueError, match="Target→Paper lineage mismatch"):
        system_target.build_system_target_review_pack(
            target_id=alternate_target["target_id"],
            paper_shadow_id=fixture["paper"]["paper_shadow_id"],
            performance_id=fixture["performance"]["performance_id"],
            model_target_dir=alternate_root / "model_target",
            paper_shadow_dir=tmp_path / "paper_shadow",
            performance_dir=tmp_path / "paper_shadow_performance",
            output_dir=tmp_path / "cross_lineage_review",
            generated_at=datetime(2026, 1, 8, 2, tzinfo=UTC),
        )
