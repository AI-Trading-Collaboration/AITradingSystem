from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_candidate_cluster_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_candidate_cluster_groups_representative_variants(tmp_path) -> None:
    fixture = run_weight_candidate_cluster_fixture(tmp_path)
    cluster = fixture["cluster"]

    assert cluster["manifest"]["status"] == "PASS"
    assert cluster["candidate_clusters"]["clusters"]
    assert cluster["cluster_representatives"]["representatives"]
    assert cluster["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_weight_candidate_cluster_artifact(
        cluster_id=cluster["cluster_id"],
        output_dir=tmp_path / "weight_candidate_cluster",
    )
    assert validation["status"] == "PASS"
