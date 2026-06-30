from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_risk_appetite_current_form_archive_only(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    output_dir = tmp_path / "out"
    run_scope_narrowed_candidate_generators_regenerate(
        scope_review_dir=fixture["scope_review_dir"],
        refined_generator_dir=fixture["refined_generator_dir"],
        refined_validation_dir=fixture["refined_validation_dir"],
        include_candidates=(
            "baseline_plus_trend_structure_refined_confidence_v1,"
            "volatility_regime_refined_confidence_v1"
        ),
        archive_candidates="risk_appetite_refined_confidence_v1",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="scope_narrowed_regeneration",
        docs_root=tmp_path / "docs",
    )
    archive_path = (
        output_dir
        / "risk_appetite_archive"
        / "risk_appetite_current_form_archive_record.json"
    )
    record = json.loads(archive_path.read_text(encoding="utf-8"))

    assert record["candidate_id"] == "risk_appetite_refined_confidence_v1"
    assert record["archive_scope"] == "current_form"
    assert record["future_reopen_policy"]["not_permanent_concept_rejection"] is True
    assert record["risk_appetite_regenerated"] is False
    assert record["broker_action"] == "none"
    assert not (output_dir / "risk_appetite_scope_narrowed_v1").exists()
