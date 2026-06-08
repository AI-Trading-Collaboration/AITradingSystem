from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_historical_replay_helpers import (
    build_replay_inventory,
    prepare_replay_test_environment,
    report_index_for_dynamic_v3,
    write_owner_reviews,
    write_replay_daily_advisory,
)

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    run_backfill_outcome,
    run_historical_paper_sim,
    run_historical_replay,
    run_replay_performance_review,
    validate_replay_performance_review_artifact,
)
from ai_trading_system.reports import reader_brief


def test_replay_performance_review_feeds_reader_brief_without_promotion(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    paths = prepare_replay_test_environment(tmp_path, monkeypatch)
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="first",
        as_of="2026-06-03",
        target_weights={"QQQ": 0.45, "SMH": 0.30, "SOXX": 0.10, "CASH": 0.15},
    )
    write_replay_daily_advisory(
        paths["daily_advisory_dir"],
        daily_advisory_id="second",
        as_of="2026-06-10",
        target_weights={"QQQ": 0.40, "SMH": 0.35, "SOXX": 0.10, "CASH": 0.15},
    )
    write_owner_reviews(paths["owner_review_dir"], ["first", "second"])
    inventory = build_replay_inventory(paths, start=date(2026, 6, 1), end=date(2026, 6, 30))
    replay = run_historical_replay(
        inventory_id=inventory["inventory_id"],
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    backfill = run_backfill_outcome(
        replay_id=replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    sim = run_historical_paper_sim(
        replay_id=replay["replay_id"],
        variant="limited_adjustment",
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    review = run_replay_performance_review(
        backfill_id=backfill["backfill_id"],
        sim_id=sim["sim_id"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        output_dir=paths["performance_review_dir"],
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )

    assert review["manifest"]["production_effect"] == "none"
    assert review["manifest"]["automatic_candidate_promotion"] is False
    assert review["manifest"]["baseline_config_mutated"] is False
    assert review["calibration_recommendations"]["automatic_config_update"] is False
    assert review["calibration_recommendations"]["recommendations"][0][
        "requires_owner_approval"
    ] is True
    assert "Dynamic Rescue Historical Replay Performance" in (
        review["review_dir"] / "reader_brief_section.md"
    ).read_text(encoding="utf-8")

    validation = validate_replay_performance_review_artifact(
        review_id=review["review_id"],
        output_dir=paths["performance_review_dir"],
    )
    assert validation["status"] == "PASS"

    report_index = report_index_for_dynamic_v3(
        paths,
        {
            "inventory": inventory,
            "replay": replay,
            "backfill": backfill,
            "sim": sim,
            "review": review,
        },
    )
    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)
    assert summary["replay_performance_review_id"] == review["review_id"]
    assert summary["backfilled_outcome_id"] == backfill["backfill_id"]
    assert summary["replay_performance_available_outcome_count"] == review["manifest"][
        "available_outcome_count"
    ]
    assert summary["production_candidate_generated"] is False
    assert summary["automatic_candidate_promotion"] is False
    assert summary["shadow_enrollment_allowed"] is False

    replay_only_index = {
        "reports": [
            row
            for row in report_index["reports"]
            if row["report_id"] != "etf_dynamic_v3_parameter_sweep_leaderboard"
        ],
    }
    replay_only_summary = reader_brief._etf_dynamic_v3_parameter_research_summary(
        replay_only_index
    )
    assert replay_only_summary["availability"] == "PARTIAL"
    assert replay_only_summary["replay_inventory_id"] == inventory["inventory_id"]
    assert replay_only_summary["historical_replay_id"] == replay["replay_id"]
    assert replay_only_summary["backfilled_outcome_id"] == backfill["backfill_id"]
    assert replay_only_summary["replay_performance_review_id"] == review["review_id"]
    assert replay_only_summary["production_candidate_generated"] is False
    assert replay_only_summary["automatic_candidate_promotion"] is False
    assert replay_only_summary["shadow_enrollment_allowed"] is False
    assert replay_only_summary["safety_status"].startswith("observe_only=true")

    html = reader_brief.render_reader_brief_html(
        {
            "as_of": "2026-07-15",
            "status": "OK",
            "production_effect": "none",
            "etf_dynamic_v3_parameter_research": summary,
        }
    )
    assert "Dynamic Rescue Historical Replay Performance" in html
    assert review["review_id"] in html
