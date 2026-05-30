from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    run_portfolio_candidate_review,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    load_portfolio_candidate_tracking_config,
    run_portfolio_candidate_tracking,
    validate_portfolio_candidate_tracking_payload,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture


def test_default_portfolio_candidate_tracking_config_loads() -> None:
    config = load_portfolio_candidate_tracking_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert set(config["eligible_review_status"]) == {
        "watch",
        "approved_for_shadow_candidate",
    }
    assert config["safety"]["production_write_allowed"] is False


@pytest.mark.parametrize("decision", ["watch", "approved_for_shadow_candidate"])
def test_tracking_starts_from_eligible_review_decision(
    tmp_path: Path,
    decision: str,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    baseline_before = sha256_file(fixture["baseline_path"])
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision=decision,
        as_of=fixture["as_of"],
        reason="Track in shadow mode.",
        config_path=review_config,
    )

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert run.daily_state_path.exists()
    assert run.active_state_path is not None and run.active_state_path.exists()
    assert validate_portfolio_candidate_tracking_payload(run.payload) == []
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["metadata"]["manual_review_required"] is True
    assert run.payload["metadata"]["auto_promotion"] is False
    assert run.payload["candidate"]["review_status"] == decision
    assert run.payload["candidate"]["tracking_status"] == "active_tracking"
    assert run.payload["data_gate"]["status"] == "OK"
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert run.payload["safety"]["production_write_allowed"] is False
    assert sha256_file(fixture["baseline_path"]) == baseline_before

    state = json.loads(run.active_state_path.read_text(encoding="utf-8"))
    assert state["active_candidates"][0]["profile_name"] == run.payload["candidate"][
        "profile_name"
    ]
    assert state["active_candidates"][0]["status"] == "active_tracking"


@pytest.mark.parametrize("decision", ["rejected", "needs_more_data"])
def test_ineligible_review_decision_cannot_start_tracking(
    tmp_path: Path,
    decision: str,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision=decision,
        as_of=fixture["as_of"],
        reason="Do not track.",
        config_path=review_config,
    )

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert run.payload["metadata"]["status"] == "NOT_STARTED"
    assert run.payload["candidate"]["tracking_status"] == "not_started"
    assert "review_status_not_eligible" in run.payload["date_roll_forward"][
        "block_reasons"
    ][0]
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False


def test_roll_forward_latest_valid_review_decision_and_degraded_data(
    tmp_path: Path,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    next_day = fixture["as_of"] + timedelta(days=1)

    run = run_portfolio_candidate_tracking(
        as_of=next_day,
        config_path=tracking_config,
    )

    assert run.payload["candidate"]["tracking_status"] == "degraded_tracking"
    assert run.payload["date_resolution"]["tracking_date"] == next_day.isoformat()
    assert run.payload["date_resolution"]["review_decision_date"] == fixture[
        "as_of"
    ].isoformat()
    assert run.payload["date_resolution"]["effective_data_date"] == fixture[
        "as_of"
    ].isoformat()
    assert run.payload["date_resolution"]["roll_forward_status"] == "ROLLED_FORWARD"
    assert "rolled forward" in run.payload["date_resolution"]["reason"].lower()


def test_production_hash_change_blocks_tracking(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    fixture["baseline_path"].write_text(
        fixture["baseline_path"].read_text(encoding="utf-8") + "\n# mutation\n",
        encoding="utf-8",
    )

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert run.payload["candidate"]["tracking_status"] == "tracking_blocked"
    assert run.payload["date_resolution"]["roll_forward_status"] == "BLOCKED"
    assert "production_config_hash_changed" in run.payload["date_roll_forward"][
        "block_reasons"
    ]
    assert run.payload["safety"]["production_config_modified"] is True


def test_tracking_state_uses_previous_observation_for_daily_delta(tmp_path: Path) -> None:
    fixture, candidates_run, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    first = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    payload = json.loads(candidates_run.json_path.read_text(encoding="utf-8"))
    payload["baseline"]["performance"]["cumulative_return"] += 0.01
    for profile in payload["profiles"]:
        if profile["profile_name"] == first.payload["candidate"]["profile_name"]:
            profile["performance"]["cumulative_return"] += 0.02
    candidates_run.json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    second = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert second.payload["tracking_metrics"]["candidate"]["daily_return"] > 0.0
    assert second.payload["tracking_metrics"]["excess_vs_baseline"]["daily_return"] > 0.0


def test_shadow_backtest_references_tracking_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    tracking_run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert decision["shadow_candidate_tracking"] == "active_tracking"
    assert decision["supporting_artifacts"]["portfolio_candidate_tracking"] == str(
        tracking_run.json_path
    )
    assert "tracked in shadow mode" in decision["reason"].lower()


def _write_portfolio_candidate_tracking_config(
    tmp_path: Path,
    shadow_config_path: object,
) -> Path:
    config_path = tmp_path / "config" / "portfolio" / "portfolio_candidate_tracking.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "portfolio-candidate-tracking-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test tracking",
                "intended_effect": "test tracking",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "input": {
                    "portfolio_candidate_reviews_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidate_reviews"
                    ),
                    "portfolio_candidates_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidates"
                    ),
                    "portfolio_sensitivity_dir": str(
                        tmp_path / "artifacts" / "portfolio_sensitivity"
                    ),
                    "market_data_freshness_dir": str(
                        tmp_path / "artifacts" / "data_freshness"
                    ),
                    "market_data_refresh_dir": str(
                        tmp_path / "artifacts" / "data_refresh"
                    ),
                    "signal_snapshot_dir": str(tmp_path / "artifacts" / "signal_snapshots"),
                    "backtest_snapshot_dir": str(
                        tmp_path / "artifacts" / "backtest_snapshots"
                    ),
                    "price_cache_registry_path": str(
                        tmp_path / "artifacts" / "data_registry" / "price_cache_registry.json"
                    ),
                    "shadow_backtest_dir": str(tmp_path / "artifacts" / "shadow_backtest"),
                    "shadow_backtest_dry_run_dir": str(
                        tmp_path
                        / "outputs"
                        / "dry_runs"
                        / "shadow_backtest"
                        / "shadow_backtest"
                    ),
                    "production_parameters_path": str(
                        tmp_path / "config" / "parameters" / "production" / "current.yaml"
                    ),
                    "shadow_backtest_config_path": str(shadow_config_path),
                },
                "output": {
                    "portfolio_candidate_tracking_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidate_tracking"
                    ),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                    "dry_run_dir": str(
                        tmp_path / "outputs" / "dry_runs" / "portfolio_candidate_tracking"
                    ),
                },
                "eligible_review_status": ["watch", "approved_for_shadow_candidate"],
                "ineligible_review_status": [
                    "pending_review",
                    "rejected",
                    "needs_more_data",
                ],
                "shadow_candidate_status": [
                    "not_started",
                    "active_tracking",
                    "tracking_blocked",
                    "degraded_tracking",
                    "watch",
                    "paused",
                    "retired",
                ],
                "safety": {
                    "production_write_allowed": False,
                    "candidate_promotion_enabled": False,
                    "candidate_production_promotion_allowed": False,
                    "data_quality_gate_lowered": False,
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
