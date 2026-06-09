from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    paper_snapshot_path,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_historical_replay as replay
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)


def prepare_replay_test_environment(tmp_path: Path, monkeypatch: Any) -> dict[str, Path]:
    monkeypatch.setattr(replay, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    prices_path, rates_path = write_market_cache(
        tmp_path / "market_cache",
        start="2026-05-25",
        end="2026-07-31",
    )
    prices = pd.read_csv(prices_path)
    if "symbol" not in prices.columns:
        prices["symbol"] = prices["ticker"]
    prices.to_csv(prices_path, index=False)
    snapshot_path = paper_snapshot_path(tmp_path)
    config_path = paper_config_path(tmp_path, snapshot_path=snapshot_path)
    return {
        "daily_advisory_dir": tmp_path / "position_advisory_daily",
        "owner_review_dir": tmp_path / "owner_review_journal",
        "paper_portfolio_dir": tmp_path / "paper_portfolio",
        "shadow_monitor_run_dir": tmp_path / "shadow_monitor_runs",
        "consensus_drift_dir": tmp_path / "consensus_drift",
        "inventory_dir": tmp_path / "replay_inventory",
        "historical_replay_dir": tmp_path / "historical_replay",
        "backfill_dir": tmp_path / "backfilled_outcome",
        "paper_sim_dir": tmp_path / "historical_paper_sim",
        "performance_review_dir": tmp_path / "replay_performance_review",
        "diagnosis_dir": tmp_path / "replay_diagnosis",
        "backfill_repair_dir": tmp_path / "backfill_repair",
        "variant_comparison_dir": tmp_path / "variant_comparison",
        "rule_calibration_dir": tmp_path / "rule_calibration",
        "replay_forward_bridge_dir": tmp_path / "replay_forward_bridge",
        "prices_path": prices_path,
        "rates_path": rates_path,
        "config_path": config_path,
    }


def write_replay_daily_advisory(
    daily_root: Path,
    *,
    daily_advisory_id: str,
    as_of: str,
    generated_at: str | None = None,
    current_weights: dict[str, float] | None = None,
    target_weights: dict[str, float] | None = None,
    recommended_action: str = "manual_review",
) -> Path:
    advisory_dir = daily_root / daily_advisory_id
    advisory_dir.mkdir(parents=True, exist_ok=True)
    current = current_weights or {"QQQ": 0.50, "SMH": 0.20, "SOXX": 0.10, "CASH": 0.20}
    target = target_weights
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_daily_manifest",
        "daily_advisory_id": daily_advisory_id,
        "source_shadow_monitor_run_id": f"monitor-{daily_advisory_id}",
        "as_of": as_of,
        "generated_at": generated_at or f"{as_of}T12:00:00+00:00",
        "status": "PASS",
        "mode": "SNAPSHOT_DELTA",
        "recommended_action": recommended_action,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    actions = {
        "schema_version": SCHEMA_VERSION,
        "mode": "SNAPSHOT_DELTA",
        "recommended_action": recommended_action,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
    }
    (advisory_dir / "daily_advisory_manifest.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )
    (advisory_dir / "daily_advisory_actions.json").write_text(
        json.dumps(actions, sort_keys=True),
        encoding="utf-8",
    )
    if target is not None:
        (advisory_dir / "daily_candidate_targets.jsonl").write_text(
            json.dumps(
                {
                    "daily_advisory_id": daily_advisory_id,
                    "candidate_id": f"candidate-{daily_advisory_id}",
                    "target_weights": target,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "median_target_weight": weight,
                    "mean_target_weight": weight,
                }
                for symbol, weight in target.items()
            ]
        ).to_csv(advisory_dir / "daily_consensus_weights.csv", index=False)
    deltas = {
        symbol: round(
            float((target or current).get(symbol, 0.0)) - float(current.get(symbol, 0.0)),
            6,
        )
        for symbol in sorted(set(current) | set(target or {}))
    }
    (advisory_dir / "daily_position_deltas.jsonl").write_text(
        json.dumps(
            {
                "daily_advisory_id": daily_advisory_id,
                "candidate_id": f"candidate-{daily_advisory_id}",
                "current_weights": current,
                "target_weights": target or {},
                "deltas": deltas,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return advisory_dir


def write_owner_reviews(owner_root: Path, daily_advisory_ids: list[str]) -> None:
    owner_root.mkdir(parents=True, exist_ok=True)
    rows = []
    for index, daily_advisory_id in enumerate(daily_advisory_ids):
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "review_id": f"review-{daily_advisory_id}",
                "daily_advisory_id": daily_advisory_id,
                "as_of": "2026-06-03",
                "recommended_action": "manual_review",
                "owner_decision": "paper_adjustment",
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "paper_action": {"enabled": True, "notes": "paper only"},
                "created_at": datetime(2026, 6, 3 + index, tzinfo=UTC).isoformat(),
                "updated_at": datetime(2026, 6, 3 + index, tzinfo=UTC).isoformat(),
                "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
            }
        )
    owner_root.joinpath("owner_review_journal.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    if rows:
        owner_root.joinpath("latest_owner_review.json").write_text(
            json.dumps(rows[-1], sort_keys=True),
            encoding="utf-8",
        )


def write_paper_action(
    paper_root: Path,
    *,
    daily_advisory_id: str,
    after_weights: dict[str, float],
) -> None:
    paper_dir = paper_root / "paper-1"
    paper_dir.mkdir(parents=True, exist_ok=True)
    paper_dir.joinpath("paper_action_ledger.jsonl").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "paper_portfolio_id": "paper-1",
                "daily_advisory_id": daily_advisory_id,
                "created_at": "2026-06-03T12:00:00+00:00",
                "before_weights": {"QQQ": 0.50, "SMH": 0.20, "SOXX": 0.10, "CASH": 0.20},
                "after_weights": after_weights,
                "broker_action_taken": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def build_replay_inventory(paths: dict[str, Path], *, start: date, end: date) -> dict[str, Any]:
    return replay.build_replay_inventory(
        start=start,
        end=end,
        output_dir=paths["inventory_dir"],
        daily_advisory_dir=paths["daily_advisory_dir"],
        shadow_monitor_run_dir=paths["shadow_monitor_run_dir"],
        consensus_drift_dir=paths["consensus_drift_dir"],
        owner_review_dir=paths["owner_review_dir"],
        paper_portfolio_dir=paths["paper_portfolio_dir"],
        prices_path=paths["prices_path"],
        config_path=paths["config_path"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )


def build_replay_review_chain(
    paths: dict[str, Path],
    *,
    backfill_generated_at: datetime = datetime(2026, 6, 4, tzinfo=UTC),
    chain_generated_at: datetime = datetime(2026, 7, 20, tzinfo=UTC),
) -> dict[str, Any]:
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
    historical_replay = replay.run_historical_replay(
        inventory_id=inventory["inventory_id"],
        inventory_dir=paths["inventory_dir"],
        output_dir=paths["historical_replay_dir"],
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )
    backfill = replay.run_backfill_outcome(
        replay_id=historical_replay["replay_id"],
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["backfill_dir"],
        prices_path=paths["prices_path"],
        rates_path=paths["rates_path"],
        config_path=paths["config_path"],
        enforce_data_quality_gate=False,
        generated_at=backfill_generated_at,
    )
    sim = replay.run_historical_paper_sim(
        replay_id=historical_replay["replay_id"],
        variant="limited_adjustment",
        replay_dir=paths["historical_replay_dir"],
        output_dir=paths["paper_sim_dir"],
        prices_path=paths["prices_path"],
        generated_at=chain_generated_at,
    )
    review = replay.run_replay_performance_review(
        backfill_id=backfill["backfill_id"],
        sim_id=sim["sim_id"],
        backfill_dir=paths["backfill_dir"],
        sim_dir=paths["paper_sim_dir"],
        output_dir=paths["performance_review_dir"],
        generated_at=chain_generated_at,
    )
    return {
        "inventory": inventory,
        "replay": historical_replay,
        "backfill": backfill,
        "sim": sim,
        "review": review,
    }


def build_minimal_leaderboard(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "PASS",
        "candidate_count": 1,
        "evaluator_mode": "tiny_fixture_proxy",
        "evaluator_version": "test",
        "metrics_source": "test",
        "not_for_investment_decision": True,
        "production_candidate_generated": False,
        "top_eligible_candidates": [
            {"candidate_id": "candidate-a", "gate": "observe", "score": 1.0}
        ],
        "most_common_reject_reasons": [],
        "recommended_next_actions": ["continue_manual_review"],
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def report_index_for_dynamic_v3(
    paths: dict[str, Path],
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    leaderboard_path = build_minimal_leaderboard(paths["inventory_dir"] / "leaderboard.json")
    reports = [
        {
            "report_id": "etf_dynamic_v3_parameter_sweep_leaderboard",
            "latest_artifact_path": str(leaderboard_path),
        },
        {
            "report_id": "etf_dynamic_v3_replay_inventory",
            "latest_artifact_path": str(
                artifacts["inventory"]["inventory_dir"] / "replay_inventory_report.md"
            ),
        },
        {
            "report_id": "etf_dynamic_v3_historical_replay",
            "latest_artifact_path": str(
                artifacts["replay"]["replay_dir"] / "replay_action_summary.json"
            ),
        },
        {
            "report_id": "etf_dynamic_v3_backfilled_outcome",
            "latest_artifact_path": str(
                artifacts["backfill"]["backfill_dir"] / "variant_performance_summary.json"
            ),
        },
        {
            "report_id": "etf_dynamic_v3_historical_paper_sim",
            "latest_artifact_path": str(
                artifacts["sim"]["sim_dir"] / "historical_paper_sim_report.md"
            ),
        },
        {
            "report_id": "etf_dynamic_v3_replay_performance_review",
            "latest_artifact_path": str(
                artifacts["review"]["review_dir"] / "reader_brief_section.md"
            ),
        },
    ]
    if "bridge" in artifacts:
        reports.append(
            {
                "report_id": "etf_dynamic_v3_replay_forward_bridge",
                "latest_artifact_path": str(
                    artifacts["bridge"]["bridge_dir"] / "reader_brief_section.md"
                ),
            }
        )
    return {
        "reports": reports,
    }
