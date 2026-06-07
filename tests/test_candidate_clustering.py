from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_position_readiness_helpers import shortlist_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_candidate_clustering,
    validate_candidate_cluster_artifact,
)


def test_candidate_clustering_selects_representatives(tmp_path: Path) -> None:
    fixture = shortlist_fixture(tmp_path)
    result = run_candidate_clustering(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        shortlist_dir=tmp_path / "shortlist",
        output_dir=tmp_path / "candidate_cluster",
    )

    assert result["manifest"]["cluster_count"] >= 1
    assert result["manifest"]["representative_count"] >= 1
    assert (result["cluster_dir"] / "parameter_similarity_matrix.csv").exists()
    assert (result["cluster_dir"] / "weight_path_similarity_matrix.csv").exists()
    assert (
        validate_candidate_cluster_artifact(
            cluster_id=result["cluster_id"],
            output_dir=tmp_path / "candidate_cluster",
        )["status"]
        == "PASS"
    )


def test_candidate_clustering_marks_incomplete_weight_path(tmp_path: Path) -> None:
    fixture = shortlist_fixture(tmp_path)
    shortlist_dir = tmp_path / "shortlist" / fixture["shortlist"]["shortlist_id"]
    rows = [
        json.loads(line)
        for line in (shortlist_dir / "shortlist_candidates.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line
    ]
    rows[0]["real_evaluation_artifact_path"] = ""
    rows[0]["weight_path_metadata"] = {}
    (shortlist_dir / "shortlist_candidates.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )

    result = run_candidate_clustering(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        shortlist_dir=tmp_path / "shortlist",
        output_dir=tmp_path / "candidate_cluster",
    )

    assert result["manifest"]["weight_path_similarity_status"] == "INCOMPLETE"
