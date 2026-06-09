from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    load_portfolio_candidate_review_config,
    run_portfolio_candidate_review,
    validate_portfolio_candidate_review_decision_payload,
)
from ai_trading_system.trading_engine.portfolio_candidates import run_portfolio_candidates
from trading_engine.test_portfolio_candidates import (
    _candidate_fixture,
    _write_portfolio_candidate_config,
)


def test_default_portfolio_candidate_review_config_loads() -> None:
    config = load_portfolio_candidate_review_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert set(config["review_status"]) >= {
        "pending_review",
        "approved_for_shadow_candidate",
        "rejected",
        "watch",
        "needs_more_data",
    }
    assert config["safety"]["production_write_allowed"] is False


def test_portfolio_candidate_review_generates_pending_package(tmp_path: Path) -> None:
    fixture, candidates_run, review_config = _review_fixture(tmp_path)
    baseline_before = sha256_file(fixture["baseline_path"])

    run = run_portfolio_candidate_review(
        as_of=fixture["as_of"],
        config_path=review_config,
        generated_at=datetime(2026, 1, 20, 9, 0, tzinfo=UTC),
    )

    assert run.package_json_path.exists()
    assert run.package_markdown_path.exists()
    assert run.decision_json_path.exists()
    assert run.decision_markdown_path.exists()
    assert run.package_payload["metadata"]["status"] == "PENDING_REVIEW"
    assert run.decision_payload["decision"]["status"] == "pending_review"
    assert run.package_payload["candidate"]["source_artifact"] == str(
        candidates_run.recommended_candidate_path
    )
    assert run.package_payload["candidate"]["candidate_hash"]
    assert run.package_payload["current_production"]["sha256"] == baseline_before
    assert run.package_payload["current_production"]["modified"] is False
    assert run.package_payload["evidence_summary"]["data_gate"] == "OK"
    assert run.package_payload["safety"]["production_write_allowed"] is False
    assert run.package_payload["safety"]["candidate_promotion_enabled"] is False
    assert sha256_file(fixture["baseline_path"]) == baseline_before


@pytest.mark.parametrize(
    "decision",
    ["watch", "rejected", "needs_more_data", "approved_for_shadow_candidate"],
)
def test_portfolio_candidate_review_decision_statuses(
    tmp_path: Path,
    decision: str,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    baseline_before = sha256_file(fixture["baseline_path"])
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)

    run = decide_portfolio_candidate(
        decision=decision,
        as_of=fixture["as_of"],
        reviewer="manual",
        reason="Unit test decision.",
        config_path=review_config,
    )

    assert run.decision_payload["decision"]["status"] == decision
    assert run.decision_payload["decision"]["reviewer"] == "manual"
    assert run.decision_payload["decision"]["production_write_allowed"] is False
    assert run.decision_payload["metadata"]["production_effect"] == "none"
    assert run.decision_payload["metadata"]["manual_review_required"] is True
    assert run.decision_payload["metadata"]["auto_promotion"] is False
    assert run.decision_payload["safety"]["candidate_production_promotion_allowed"] is False
    assert validate_portfolio_candidate_review_decision_payload(run.decision_payload) == []
    assert sha256_file(fixture["baseline_path"]) == baseline_before


def test_signal_limited_prevents_production_approval_but_allows_shadow_candidate(
    tmp_path: Path,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)

    run = decide_portfolio_candidate(
        decision="approved_for_shadow_candidate",
        as_of=fixture["as_of"],
        reason="Approved for shadow tracking only.",
        config_path=review_config,
    )

    assert run.decision_payload["decision"]["status"] == "approved_for_shadow_candidate"
    assert run.decision_payload["evidence_summary"]["signal_snapshot_status"] == "LIMITED"
    assert run.decision_payload["safety"]["candidate_production_promotion_allowed"] is False
    assert run.decision_payload["safety"]["candidate_promotion_enabled"] is False


def test_data_gate_not_ok_prevents_shadow_candidate_approval(tmp_path: Path) -> None:
    fixture, candidates_run, review_config = _review_fixture(tmp_path)
    payload = json.loads(candidates_run.json_path.read_text(encoding="utf-8"))
    payload["data_gate"]["status"] = "FAILED"
    candidates_run.json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)

    run = decide_portfolio_candidate(
        decision="approved_for_shadow_candidate",
        as_of=fixture["as_of"],
        config_path=review_config,
    )

    assert run.decision_payload["decision"]["status"] == "rejected"
    assert "data_gate_not_ok" in run.decision_payload["hard_rejections"]
    assert run.decision_payload["decision"]["requested_status"] == ("approved_for_shadow_candidate")


def test_production_modified_prevents_shadow_candidate_approval(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    fixture["baseline_path"].write_text(
        fixture["baseline_path"].read_text(encoding="utf-8") + "\n# test mutation\n",
        encoding="utf-8",
    )

    run = decide_portfolio_candidate(
        decision="approved_for_shadow_candidate",
        as_of=fixture["as_of"],
        config_path=review_config,
    )

    assert run.decision_payload["decision"]["status"] == "rejected"
    assert "production_config_modified" in run.decision_payload["hard_rejections"]
    assert run.decision_payload["safety"]["production_config_modified"] is True


def test_shadow_backtest_references_candidate_review_decision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decision_run = decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Signal quality remains LIMITED; continue observing.",
        config_path=review_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert "manual watch" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["portfolio_candidate_review"] == str(
        decision_run.decision_json_path
    )
    assert shadow_run.payload["safety"]["auto_promotion"] is False


def _review_fixture(tmp_path: Path) -> tuple[dict[str, object], object, Path]:
    fixture = _candidate_fixture(tmp_path)
    candidate_config = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])
    candidates_run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        profile_names=("baseline_current", "balanced_responsive"),
        config_path=candidate_config,
    )
    review_config = _write_portfolio_candidate_review_config(
        tmp_path,
        fixture["baseline_path"],
    )
    return fixture, candidates_run, review_config


def _write_portfolio_candidate_review_config(
    tmp_path: Path,
    production_path: object,
) -> Path:
    config_path = tmp_path / "config" / "portfolio" / "portfolio_candidate_review.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "portfolio-candidate-review-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test portfolio candidate review",
                "intended_effect": "test review decisions",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "input": {
                    "portfolio_candidates_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidates"
                    ),
                    "portfolio_sensitivity_dir": str(
                        tmp_path / "artifacts" / "portfolio_sensitivity"
                    ),
                    "signal_calibration_dir": str(tmp_path / "artifacts" / "signal_calibration"),
                    "signal_ablation_dir": str(tmp_path / "artifacts" / "signal_ablation"),
                    "signal_snapshot_dir": str(tmp_path / "artifacts" / "signal_snapshots"),
                    "backtest_snapshot_dir": str(tmp_path / "artifacts" / "backtest_snapshots"),
                    "price_cache_reconcile_dir": str(tmp_path / "artifacts" / "data_quality"),
                    "shadow_backtest_dir": str(tmp_path / "artifacts" / "shadow_backtest"),
                    "production_parameters_path": str(production_path),
                },
                "output": {
                    "portfolio_candidate_reviews_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidate_reviews"
                    ),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                },
                "review_status": [
                    "pending_review",
                    "approved_for_shadow_candidate",
                    "rejected",
                    "watch",
                    "needs_more_data",
                ],
                "default_decision": {
                    "status": "pending_review",
                    "reviewer": "manual",
                    "reason": "Pending manual review.",
                },
                "decision_rules": {
                    "signal_limited_production_promotion_allowed": False,
                    "approved_for_shadow_candidate_means": "shadow_tracking_only",
                    "default_recommendation_when_limited": "watch",
                    "hard_rejection": [
                        "data_gate_not_ok",
                        "production_config_modified",
                        "missing_candidate_artifact",
                        "missing_portfolio_candidates_summary",
                        "auto_promotion_true",
                        "production_effect_not_none",
                    ],
                },
                "allowed_next_steps": {
                    "pending_review": "await_manual_review",
                    "watch": "continue_shadow_tracking",
                    "approved_for_shadow_candidate": "continue_shadow_tracking",
                    "rejected": "stop_candidate_review",
                    "needs_more_data": "collect_more_evidence",
                },
                "safety": {
                    "production_write_allowed": False,
                    "candidate_promotion_enabled": False,
                    "candidate_production_promotion_allowed": False,
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
