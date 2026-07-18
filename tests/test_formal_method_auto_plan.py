from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_formal_method_auto_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_formal_method_auto_plan_never_implements_or_applies_methods(tmp_path) -> None:
    fixture = run_formal_method_auto_plan_fixture(tmp_path)
    plan = fixture["formal_plan"]

    assert plan["manifest"]["status"] in {"PLAN_READY", "SKIPPED_NO_PROMOTED_CANDIDATE"}
    assert plan["manifest"]["implemented"] is False
    assert plan["manifest"]["auto_apply"] is False
    assert plan["manifest"]["production_effect"] == "none"
    assert set(plan["validation_plan"]["stage_status"].values()) <= {
        "READY_AFTER_IMPLEMENTATION",
        "SKIPPED_NOT_IMPLEMENTED",
    }

    validation = weight_search.validate_formal_method_auto_plan_artifact(
        plan_id=plan["plan_id"],
        output_dir=tmp_path / "formal_method_auto_plan",
    )
    assert validation["status"] == "PASS"
