from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.reports.research_governance_summary import (
    PROMOTION_STATUS_BLOCKED_MISSING,
    build_research_governance_summary_payload,
    render_research_governance_summary_markdown,
)


def test_research_governance_summary_trading_engine_validation_path(
    tmp_path: Path,
) -> None:
    payload = build_research_governance_summary_payload(
        as_of=date(2026, 5, 4),
        project_root=tmp_path,
    )
    markdown = render_research_governance_summary_markdown(payload)

    assert payload["report_type"] == "research_governance_summary"
    assert payload["promotion_status"] == PROMOTION_STATUS_BLOCKED_MISSING
    assert payload["production_effect"] == "none"
    assert payload["manual_review_queue"]
    assert "## SEC PIT Research Status" in markdown
