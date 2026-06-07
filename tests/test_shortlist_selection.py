from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_position_readiness_helpers import observe_pool_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_shadow_shortlist,
    validate_shortlist_artifact,
)


def test_shortlist_excludes_hard_fail_candidates_and_scores(tmp_path: Path) -> None:
    fixture = observe_pool_fixture(tmp_path)
    pool_id = fixture["pool"]["pool_id"]
    pool_dir = tmp_path / "observe_pool" / pool_id
    rows = [
        json.loads(line)
        for line in (pool_dir / "observe_candidates.jsonl").read_text(encoding="utf-8").splitlines()
        if line
    ]
    bad = dict(rows[0])
    bad["candidate_id"] = "hard_fail_candidate"
    bad["score"] = 999
    bad["real_evaluation_artifact_path"] = ""
    bad["evidence_status"] = {**bad["evidence_status"], "data_quality": "FAIL"}
    rows.append(bad)
    (pool_dir / "observe_candidates.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )

    result = build_shadow_shortlist(
        observe_pool_id=pool_id,
        target_size=4,
        max_size=5,
        min_size=2,
        observe_pool_dir=tmp_path / "observe_pool",
        output_dir=tmp_path / "shortlist",
    )

    selected_ids = {row["candidate_id"] for row in result["candidates"]}
    rejected = {
        row["candidate_id"]: row["rejection_reasons"] for row in result["rejected_candidates"]
    }
    assert "hard_fail_candidate" not in selected_ids
    assert "data_quality_fail" in rejected["hard_fail_candidate"]
    assert all(row["selection_reasons"] for row in result["candidates"])
    assert all("performance" in row["shortlist_score_breakdown"] for row in result["candidates"])
    assert (
        validate_shortlist_artifact(
            shortlist_id=result["shortlist_id"],
            output_dir=tmp_path / "shortlist",
        )["status"]
        == "PASS"
    )
