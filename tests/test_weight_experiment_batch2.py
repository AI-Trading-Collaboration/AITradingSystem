from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_experiment_batch2_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_experiment_batch2_generates_broad_research_matrix(tmp_path) -> None:
    fixture = run_weight_experiment_batch2_fixture(tmp_path)
    matrix = fixture["matrix"]

    assert matrix["manifest"]["status"] == "PASS"
    assert 50 <= matrix["manifest"]["variant_count"] <= 80
    assert len(matrix["family_coverage"]["families_covered"]) >= 8
    assert all(row["experiment_only"] is True for row in matrix["variant_specs"])
    assert all(row["not_formal_research_method"] is True for row in matrix["variant_specs"])

    validation = weight_search.validate_weight_experiment_batch2_artifact(
        matrix_id=matrix["matrix_id"],
        output_dir=tmp_path / "weight_experiment_batch2",
    )
    assert validation["status"] == "PASS"
