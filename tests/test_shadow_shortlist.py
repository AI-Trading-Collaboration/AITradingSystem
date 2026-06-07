from __future__ import annotations

from pathlib import Path

from dynamic_v3_position_readiness_helpers import cluster_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_shadow_shortlist_monitoring_pack,
    validate_shadow_shortlist_artifact,
)


def test_shadow_shortlist_builds_monitoring_requirements(tmp_path: Path) -> None:
    fixture = cluster_fixture(tmp_path)
    result = build_shadow_shortlist_monitoring_pack(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        cluster_id=fixture["cluster"]["cluster_id"],
        shortlist_dir=tmp_path / "shortlist",
        cluster_dir=tmp_path / "candidate_cluster",
        output_dir=tmp_path / "shadow_shortlist",
    )

    assert result["manifest"]["shadow_candidate_count"] >= 1
    assert all(row["monitoring_requirements"]["min_days"] == 30 for row in result["candidates"])
    assert result["manifest"]["production_candidate_generated"] is False
    assert (
        validate_shadow_shortlist_artifact(
            shadow_shortlist_id=result["shadow_shortlist_id"],
            output_dir=tmp_path / "shadow_shortlist",
        )["status"]
        == "PASS"
    )
