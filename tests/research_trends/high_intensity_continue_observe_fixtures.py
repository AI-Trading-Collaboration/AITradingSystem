from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    run_high_intensity_risk_cap_forward_outcome_review,
)

__all__ = [
    "build_high_intensity_continue_observe_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_continue_observe_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    forward_outcome_review_dir = tmp_path / "forward_outcome_review"
    forward_docs_root = tmp_path / "forward_docs"
    run_high_intensity_risk_cap_forward_outcome_review(
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        output_dir=forward_outcome_review_dir,
        docs_root=forward_docs_root,
    )
    return {
        **fixture,
        "forward_outcome_review_dir": forward_outcome_review_dir,
        "forward_docs_root": forward_docs_root,
    }
