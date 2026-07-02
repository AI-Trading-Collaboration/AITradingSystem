from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_pit_caveat_acceptance_report,
)


def test_pit_caveat_acceptance_allows_research_dry_run_only() -> None:
    report = build_dynamic_dry_run_pit_caveat_acceptance_report(
        wrapper_rows=[dry_run_wrapper_row()],
        pit_caveat={
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "known_at_caveats": ["native intraday known-at timestamp missing"],
            "lookahead_risk": "MITIGATED_BY_NEXT_SESSION_POLICY",
            "revision_risk": "SOURCE_ARTIFACT_VERSION_DEPENDENT",
        },
        known_at_report={"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
        risk_cap_alignment={
            "alignment_warnings": [
                "source remediation alignment blockers carried forward for 2331"
            ]
        },
    )

    assert report["pit_caveat_accepted"] is True
    assert report["strict_pit_ready"] is False
    assert report["acceptance_status"] == (
        "PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_DRY_RUN_WITH_WARNINGS"
    )
    assert "research_only_dynamic_dry_run" in report["allowed_usage"]
    assert "production" in report["blocked_usage"]
    assert report["promotion_allowed"] is False


def test_pit_caveat_acceptance_blocks_when_no_caveat_policy_is_available() -> None:
    row = dry_run_wrapper_row()
    row["pit_policy"] = ""

    report = build_dynamic_dry_run_pit_caveat_acceptance_report(
        wrapper_rows=[row],
        pit_caveat={"strict_pit_ready": False, "pit_approximation_ready": False},
        known_at_report={},
        risk_cap_alignment={},
    )

    assert report["pit_caveat_accepted"] is False
    assert report["acceptance_status"] == "PIT_CAVEAT_NOT_ACCEPTED"
