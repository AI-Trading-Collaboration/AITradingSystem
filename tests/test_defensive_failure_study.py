from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_failure_study_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_research import (
    validate_defensive_failure_study_artifact,
)


def test_defensive_failure_study_ranks_failures_without_auto_mitigation(tmp_path):
    fixture = run_failure_study_fixture(tmp_path)
    study = fixture["defensive_failure_study"]
    ranked = study["failure_cases_ranked"]
    ideas = study["failure_mitigation_ideas"]["ideas"]

    assert ranked
    assert ranked[0]["risk_asset_exposure_delta"] is None
    assert ranked[0]["likely_failure_reason"] == "late_de_risking"
    assert ranked[0]["failure_severity"] in {"HIGH", "MEDIUM", "LOW"}
    assert all(idea["auto_apply"] is False for idea in ideas)
    assert study["failure_mitigation_ideas"]["policy_change_allowed"] is False
    assert study["manifest"]["production_effect"] == "none"

    validation = validate_defensive_failure_study_artifact(
        failure_study_id=study["failure_study_id"],
        output_dir=fixture["defensive_failure_study_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
