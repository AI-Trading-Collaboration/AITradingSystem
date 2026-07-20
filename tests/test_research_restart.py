from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import research_restart as restart
from ai_trading_system import research_restart_decision as decision
from ai_trading_system.cli import app
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport


def test_r0_preflight_freezes_dual_window_and_execution_contract(tmp_path: Path) -> None:
    sources = _write_sources(tmp_path)
    payload = restart.build_research_restart_preflight(
        source_sweep_dir=sources["sweep_dir"],
        policy_path=restart.DEFAULT_RESTART_POLICY_PATH,
        primary_window_policy_path=restart.DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
        window_registry_path=restart.DEFAULT_WINDOW_REGISTRY_PATH,
        prices_path=sources["prices"],
        secondary_prices_path=sources["secondary_prices"],
        rates_path=sources["rates"],
        download_manifest_path=sources["download_manifest"],
        cost_policy_path=restart.DEFAULT_COST_POLICY_PATH,
        execution_policy_path=restart.DEFAULT_EXECUTION_POLICY_PATH,
        data_quality_report=_quality_report(sources),
    )

    assert payload["status"] == "PASS"
    assert payload["research_execution_unblocked"] is True
    assert payload["failed_check_count"] == 0
    assert payload["window_semantics"]["project_ai_cycle_start"] == "2022-12-01"
    assert payload["window_semantics"]["primary_validated_start"] == "2021-02-22"
    assert payload["research_lane"]["source_window_role"] == "legacy_comparison"
    assert payload["cost_and_execution_snapshot"]["signal_execution_lag_days"] == 1
    assert payload["production_effect"] == "none"
    assert payload["promotion_gate_allowed"] is False


def test_r0_validator_detects_source_fingerprint_drift(tmp_path: Path) -> None:
    sources = _write_sources(tmp_path)
    payload = restart.build_research_restart_preflight(
        source_sweep_dir=sources["sweep_dir"],
        policy_path=restart.DEFAULT_RESTART_POLICY_PATH,
        primary_window_policy_path=restart.DEFAULT_PRIMARY_WINDOW_POLICY_PATH,
        window_registry_path=restart.DEFAULT_WINDOW_REGISTRY_PATH,
        prices_path=sources["prices"],
        secondary_prices_path=sources["secondary_prices"],
        rates_path=sources["rates"],
        download_manifest_path=sources["download_manifest"],
        cost_policy_path=restart.DEFAULT_COST_POLICY_PATH,
        execution_policy_path=restart.DEFAULT_EXECUTION_POLICY_PATH,
        data_quality_report=_quality_report(sources),
    )
    artifact_path = tmp_path / "strategy_research_restart_preflight.json"
    markdown_path = tmp_path / "strategy_research_restart_preflight.md"
    payload["artifact_paths"] = {
        "json": str(artifact_path),
        "markdown": str(markdown_path),
    }
    restart._write_json(artifact_path, payload)
    markdown_path.write_text(restart.render_research_restart_preflight(payload), encoding="utf-8")

    assert (
        restart.validate_research_restart_preflight(artifact_path=artifact_path)["status"] == "PASS"
    )

    sources["candidate_results"].write_text(
        json.dumps({"candidate_id": "tampered"}) + "\n", encoding="utf-8"
    )
    validation = restart.validate_research_restart_preflight(artifact_path=artifact_path)
    assert validation["status"] == "FAIL"
    assert any(
        item["check_id"] == "input_fingerprints_fresh" and item["passed"] is False
        for item in validation["checks"]
    )


def test_r0_cli_commands_are_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["research", "ops", "strategy-restart-preflight", "--help"])
    assert result.exit_code == 0
    assert "--source-sweep-dir" in result.output

    validation = runner.invoke(
        app,
        ["research", "ops", "validate-strategy-restart-preflight", "--help"],
    )
    assert validation.exit_code == 0
    assert "--artifact-path" in validation.output

    r2 = runner.invoke(app, ["research", "ops", "strategy-restart-decision", "--help"])
    assert r2.exit_code == 0
    assert "--walk-forward-id" in r2.output
    assert "--robustness-id" in r2.output

    r2_validation = runner.invoke(
        app,
        ["research", "ops", "validate-strategy-restart-decision", "--help"],
    )
    assert r2_validation.exit_code == 0
    assert "--output-root" in r2_validation.output


def test_r2_decision_rules_are_ordered_and_fail_closed() -> None:
    complete = {
        "r0_hard_checks_pass": True,
        "walk_forward_contract_complete": True,
        "robustness_contract_complete": True,
        "walk_forward_negative": False,
        "robustness_negative": False,
        "legacy_only_evidence": False,
        "forward_append_only_integrity": True,
        "forward_daily_continuity": True,
        "forward_all_horizons_mature": True,
    }

    assert (
        decision._select_decision({**complete, "r0_hard_checks_pass": False})
        == "HOLD_RESEARCH_RESTART"
    )
    assert (
        decision._select_decision({**complete, "robustness_contract_complete": False})
        == "CONTINUE_EVIDENCE_CLOSURE"
    )
    assert (
        decision._select_decision({**complete, "walk_forward_negative": True})
        == "PAUSE_CANDIDATE_EXPANSION"
    )
    assert (
        decision._select_decision({**complete, "forward_all_horizons_mature": False})
        == "CONTINUE_FORWARD_MATURATION"
    )
    assert decision._select_decision(complete) == "READY_FOR_OWNER_CONTROLLED_NEXT_RESEARCH_REVIEW"


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sweep_id = "sweep_test_real"
    sweep_dir = tmp_path / sweep_id
    sweep_dir.mkdir()
    (sweep_dir / "sweep_manifest.json").write_text(
        json.dumps(
            {
                "sweep_id": sweep_id,
                "status": "completed",
                "evaluator_mode": "real_dynamic_v3_rescue",
            }
        ),
        encoding="utf-8",
    )
    (sweep_dir / "sweep_config.normalized.yaml").write_text(
        "out_of_sample:\n"
        "  enabled: true\n"
        "  holdout_start: 2024-01-01\n"
        "  holdout_end: 2026-06-05\n",
        encoding="utf-8",
    )
    candidate_results = sweep_dir / "candidate_results.jsonl"
    candidate_results.write_text(
        json.dumps({"candidate_id": "candidate_1", "status": "completed"}) + "\n",
        encoding="utf-8",
    )
    paths = {
        "sweep_dir": sweep_dir,
        "candidate_results": candidate_results,
        "prices": tmp_path / "prices.csv",
        "secondary_prices": tmp_path / "secondary_prices.csv",
        "rates": tmp_path / "rates.csv",
        "download_manifest": tmp_path / "download_manifest.csv",
    }
    for key in ("prices", "secondary_prices", "rates", "download_manifest"):
        paths[key].write_text(f"fixture={key}\n", encoding="utf-8")
    return paths


def _quality_report(paths: dict[str, Path]) -> DataQualityReport:
    return DataQualityReport(
        checked_at=datetime(2026, 7, 20, tzinfo=UTC),
        as_of=date(2026, 7, 17),
        price_summary=_summary(paths["prices"], date(2021, 2, 22), date(2026, 7, 17)),
        secondary_price_summary=_summary(
            paths["secondary_prices"], date(2021, 2, 22), date(2026, 7, 17)
        ),
        rate_summary=_summary(paths["rates"], date(2021, 2, 22), date(2026, 7, 17)),
        manifest_summary=_summary(paths["download_manifest"], date(2026, 7, 17), date(2026, 7, 17)),
        expected_price_tickers=("QQQ", "SGOV", "TQQQ"),
        expected_rate_series=("DGS3MO",),
    )


def _summary(path: Path, minimum: date, maximum: date) -> DataFileSummary:
    return DataFileSummary(
        path=path,
        exists=True,
        rows=10,
        sha256=restart._file_sha256(path),
        min_date=minimum,
        max_date=maximum,
    )
