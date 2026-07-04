from __future__ import annotations

from pathlib import Path

from high_intensity_continue_observe_fixtures import (
    build_high_intensity_continue_observe_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_continue_forward_observe_decision import (
    run_high_intensity_risk_cap_continue_forward_observe_decision,
)

__all__ = [
    "build_high_intensity_runtime_integration_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_runtime_integration_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_continue_observe_fixture(tmp_path)
    continue_decision_dir = tmp_path / "continue_decision"
    continue_docs_root = tmp_path / "continue_docs"
    run_high_intensity_risk_cap_continue_forward_observe_decision(
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        output_dir=continue_decision_dir,
        docs_root=continue_docs_root,
    )
    return {
        **fixture,
        "continue_decision_dir": continue_decision_dir,
        "continue_docs_root": continue_docs_root,
    }
