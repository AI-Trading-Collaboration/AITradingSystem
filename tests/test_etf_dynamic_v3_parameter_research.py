from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.config import configured_price_tickers, configured_rate_series, load_universe
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DynamicV3ParameterResearchError,
    build_promotion_pack,
    build_research_index,
    build_sweep_config_validation,
    candidate_report_payload,
    data_provenance_inspect_price_cache,
    data_provenance_repair_price_manifest,
    data_provenance_validate,
    governance_report_payload,
    inspect_window_artifact,
    load_parameter_sweep_config,
    parameter_grid_candidates,
    preview_sweep_candidates,
    register_shadow_candidate,
    research_query_payload,
    run_candidate_attribution,
    run_data_audit,
    run_injection_audit,
    run_overfit_review,
    run_parameter_sweep,
    run_robustness_diagnostics,
    run_shadow_monitor,
    run_walk_forward_selection,
    run_walk_forward_validation,
    run_window_audit,
    stable_candidate_id,
    validate_artifacts_payload,
    validate_candidate_attribution_artifact,
    validate_data_audit_artifact,
    validate_injection_audit_artifact,
    validate_overfit_artifact,
    validate_parameter_governance,
    validate_promotion_pack,
    validate_robustness_artifact,
    validate_shadow_monitor_artifact,
    validate_shadow_registry,
    validate_sweep_artifact,
    validate_sweep_profiles_payload,
    validate_walk_forward_artifact,
    validate_walk_forward_selection_artifact,
    validate_weight_path_artifact,
    validate_window_audit_artifact,
    weight_path_report_payload,
    window_audit_report_payload,
)
from ai_trading_system.reports import reader_brief


def test_parameter_sweep_config_validation_and_candidate_id(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    config = load_parameter_sweep_config(config_path)
    candidates = parameter_grid_candidates(config, code_version="code", data_manifest_hash="data")

    assert build_sweep_config_validation(config_path)["status"] == "PASS"
    assert len(candidates) <= config.run.max_candidates
    assert candidates == parameter_grid_candidates(
        config,
        code_version="code",
        data_manifest_hash="data",
    )
    assert candidates[0]["candidate_id"] == stable_candidate_id(
        candidates[0]["parameters"],
        strategy_family="dynamic_v3_rescue",
        code_version="code",
        data_manifest_hash="data",
    )
    preview = preview_sweep_candidates(config_path=config_path, limit=3)
    assert [row["parameters"] for row in preview["candidates"]] == [
        row["parameters"] for row in candidates[:3]
    ]

    raw = _tiny_config()
    raw["parameter_space"] = {}
    invalid_path = tmp_path / "invalid.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(DynamicV3ParameterResearchError):
        load_parameter_sweep_config(invalid_path)


def test_tiny_sweep_resume_reports_and_validation(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    output_dir = tmp_path / "sweeps"
    result = run_parameter_sweep(config_path=config_path, output_dir=output_dir)
    sweep_id = result["sweep_id"]

    assert validate_sweep_artifact(sweep_id=sweep_id, output_dir=output_dir)["status"] == "PASS"
    sweep_dir = output_dir / sweep_id
    results = _jsonl(sweep_dir / "candidate_results.jsonl")
    errors = _jsonl(sweep_dir / "candidate_errors.jsonl")
    assert results
    assert errors
    assert any(row["gate"] != "reject" for row in results)
    assert all(row["evaluator_mode"] == "tiny_fixture_proxy" for row in results)
    assert all(row["metrics_source"] == "tiny_fixture_proxy_formula" for row in results)
    assert all(row["not_for_investment_decision"] is True for row in results)
    assert all(row["real_evaluation_artifact_path"] == "" for row in results)
    assert (sweep_dir / "leaderboard.json").exists()
    assert (sweep_dir / "sweep_report.md").exists()

    resumed = run_parameter_sweep(config_path=config_path, output_dir=output_dir, resume=sweep_id)
    assert resumed["manifest"]["completed_count"] == len(results)
    assert len(_jsonl(sweep_dir / "candidate_results.jsonl")) == len(results)

    (sweep_dir / "sweep_manifest.json").unlink()
    with (sweep_dir / "candidate_results.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(results[0], sort_keys=True) + "\n")
    resumed_without_final_manifest = run_parameter_sweep(
        config_path=config_path,
        output_dir=output_dir,
        resume=sweep_id,
    )
    assert resumed_without_final_manifest["manifest"]["completed_count"] == len(results)
    assert (sweep_dir / "sweep_manifest.json").exists()
    assert len(_jsonl(sweep_dir / "candidate_results.jsonl")) == len(results)

    candidate_id = _top_candidate_id(sweep_dir)
    report = candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=output_dir,
        write=True,
    )
    assert report["hard_gate_status"] in {"observe_only", "review_required"}
    assert (sweep_dir / "candidates" / candidate_id / "candidate_report.json").exists()


def test_walk_forward_robustness_shadow_artifacts_and_promotion_pack(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    sweep_output_dir = tmp_path / "sweeps"
    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    candidate_id = _top_candidate_id(sweep_output_dir / sweep_id)
    candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )

    wf = run_walk_forward_validation(
        sweep_id=sweep_id,
        top_n=3,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "walk_forward",
    )
    assert (
        validate_walk_forward_artifact(
            walk_forward_id=wf["walk_forward_id"],
            output_dir=tmp_path / "walk_forward",
        )["status"]
        == "PASS"
    )

    robustness = run_robustness_diagnostics(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "robustness",
    )
    assert (
        validate_robustness_artifact(
            robustness_id=robustness["robustness_id"],
            output_dir=tmp_path / "robustness",
        )["status"]
        == "PASS"
    )

    registry_path = tmp_path / DEFAULT_SHADOW_REGISTRY_PATH.name
    registered = register_shadow_candidate(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
    )
    assert registered["status"] == "PASS"
    assert (
        validate_shadow_registry(
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
        )["status"]
        == "PASS"
    )

    pack = build_promotion_pack(
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=tmp_path / "walk_forward",
        robustness_dir=tmp_path / "robustness",
        output_dir=tmp_path / "promotion",
    )
    assert pack["pack"]["status"] in {"review_required", "reject"}
    assert "tiny_fixture_not_for_investment_decision" in pack["pack"]["decision_reasons"]
    assert pack["pack"]["production_candidate_generated"] is False
    assert (
        validate_promotion_pack(
            candidate_id=candidate_id,
            output_dir=tmp_path / "promotion",
        )["status"]
        == "PASS"
    )
    assert validate_artifacts_payload(pointer_dir=tmp_path / "latest")["status"] == "PASS"


def test_dynamic_v3_parameter_research_cli_smoke(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    output_dir = tmp_path / "sweeps"
    runner = CliRunner()

    validate_result = runner.invoke(
        etf_app,
        ["dynamic-v3-rescue", "sweep-config", "validate", "--config", str(config_path)],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert validate_result.exit_code == 0, validate_result.output
    assert "status=PASS" in validate_result.output

    preview_result = runner.invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "sweep-config",
            "preview",
            "--config",
            str(config_path),
            "--limit",
            "2",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert preview_result.exit_code == 0, preview_result.output
    assert "preview_count=2" in preview_result.output

    run_result = runner.invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "sweep",
            "run",
            "--config",
            str(config_path),
            "--output",
            str(output_dir),
            "--workers",
            "1",
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert run_result.exit_code == 0, run_result.output
    sweep_id = _line_value(run_result.output, "sweep_id")

    leaderboard_result = runner.invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "sweep",
            "leaderboard",
            "--sweep-id",
            sweep_id,
            "--output",
            str(output_dir),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert leaderboard_result.exit_code == 0, leaderboard_result.output
    assert "production_candidate_generated=false" in leaderboard_result.output


def test_real_dynamic_v3_rescue_sweep_smoke_writes_real_artifacts(tmp_path: Path) -> None:
    prices_path, rates_path, as_of = _write_real_smoke_cache(tmp_path)
    config_path = _real_smoke_config_path(tmp_path, as_of)
    output_dir = tmp_path / "real_sweeps"

    result = run_parameter_sweep(
        config_path=config_path,
        output_dir=output_dir,
        workers=1,
        evaluator_mode="real_dynamic_v3_rescue",
        prices_path=prices_path,
        rates_path=rates_path,
    )
    sweep_id = result["sweep_id"]
    sweep_dir = output_dir / sweep_id
    results = _jsonl(sweep_dir / "candidate_results.jsonl")

    assert result["manifest"]["evaluator_mode"] == "real_dynamic_v3_rescue"
    assert result["manifest"]["completed_count"] <= 2
    assert results
    assert all(row["evaluator_mode"] == "real_dynamic_v3_rescue" for row in results)
    assert all(row["metrics_source"] == "real_evaluation_artifact" for row in results)
    assert all(row["not_for_investment_decision"] is False for row in results)
    assert all(Path(row["real_evaluation_artifact_path"]).exists() for row in results)
    assert {row["data_quality"]["status"] for row in results} <= {"PASS", "PASS_WITH_WARNINGS"}
    assert validate_sweep_artifact(sweep_id=sweep_id, output_dir=output_dir)["status"] == "PASS"

    first_artifact = Path(results[0]["real_evaluation_artifact_path"])
    real_payload = json.loads(first_artifact.read_text(encoding="utf-8"))
    evaluation_id = real_payload["dynamic_v3_real_evaluation_report_id"]
    assert real_payload["backtest_window"]["configured_backtest_start"] == "2022-12-01"
    assert real_payload["comparison_daily_paths"]["dynamic_candidate"]
    weight_validation = validate_weight_path_artifact(
        evaluation_id=evaluation_id,
        search_root=output_dir,
    )
    assert weight_validation["status"] == "PASS"
    assert weight_validation["attribution_completeness"] == "PARTIAL"
    weight_report = weight_path_report_payload(evaluation_id=evaluation_id, search_root=output_dir)
    assert Path(weight_report["daily_weights_path"]).exists()
    assert Path(weight_report["weight_path_metadata_path"]).exists()

    attribution = run_candidate_attribution(
        sweep_id=sweep_id,
        candidate_id=results[0]["candidate_id"],
        sweep_output_dir=output_dir,
        output_dir=tmp_path / "candidate_attribution",
    )
    assert attribution["report"]["status"] == "PARTIAL"
    assert attribution["report"]["attribution_completeness"] == "PARTIAL"

    window_audit = run_window_audit(
        as_of=pd.Timestamp("2022-12-01").date(),
        end=pd.Timestamp(as_of).date(),
        artifact_root=output_dir,
        output_dir=tmp_path / "window_audit",
    )
    assert window_audit["report"]["configured_backtest_start"] == "2022-12-01"
    assert (
        validate_window_audit_artifact(
            audit_id=window_audit["window_audit_id"],
            output_dir=tmp_path / "window_audit",
        )["status"]
        == "PASS"
    )
    assert (
        window_audit_report_payload(
            audit_id=window_audit["window_audit_id"],
            output_dir=tmp_path / "window_audit",
        )["configured_backtest_start"]
        == "2022-12-01"
    )

    first_artifact.unlink()
    validation = validate_sweep_artifact(sweep_id=sweep_id, output_dir=output_dir)
    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "real_evaluation_artifact_paths_exist"
        and check["passed"] is False
        for check in validation["checks"]
    )


def test_dynamic_v3_data_provenance_repair_and_validate(tmp_path: Path) -> None:
    prices_path, rates_path, as_of = _write_real_smoke_cache(tmp_path)

    inspect = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=tmp_path / "data_provenance",
    )
    assert inspect["status"] == "PASS_WITH_WARNINGS"
    assert inspect["prices_checksum_in_manifest"] is False

    repaired = data_provenance_repair_price_manifest(
        prices_path=prices_path,
        rates_path=rates_path,
    )
    assert repaired["provenance_status"] == "RECONSTRUCTED_MANIFEST"
    assert Path(repaired["reconstructed_manifest_path"]).exists()

    validation = data_provenance_validate(prices_path=prices_path, rates_path=rates_path)
    assert validation["status"] == "PASS_WITH_WARNINGS"
    assert validation["prices_checksum_in_manifest"] is True
    assert validation["failed_check_count"] == 0

    data_audit = run_data_audit(
        as_of=pd.Timestamp(as_of).date(),
        end=pd.Timestamp(as_of).date(),
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=tmp_path / "data_audit",
    )
    assert data_audit["report"]["prices_download_manifest_checksum_missing"] is False
    assert data_audit["report"]["provenance_status"] == "RECONSTRUCTED_MANIFEST"


def test_window_audit_detects_incomplete_actual_window(tmp_path: Path) -> None:
    artifact_path = tmp_path / "real_evaluation.json"
    artifact_path.write_text(
        json.dumps(
            {
                "report_type": "etf_dynamic_v3_real_evaluation_report",
                "status": "PASS",
                "market_regime": {"default_backtest_start": "2022-12-01"},
                "requested_range": {"start": "2022-12-01", "end": "2026-06-04"},
                "daily_path_summary": {
                    "first_signal_date": "2025-05-28",
                    "last_signal_date": "2026-05-28",
                    "row_count": 252,
                },
            }
        ),
        encoding="utf-8",
    )

    inspected = inspect_window_artifact(artifact_path=artifact_path)

    assert inspected["status"] == "INCOMPLETE"
    assert inspected["record"]["promotion_blocking"] is True
    assert "actual_evaluation_start_after_configured_backtest_start" in inspected["record"][
        "window_mismatch_reasons"
    ]


def test_dynamic_v3_stable_real_loop_artifact_contracts(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    profile_path = _profile_config_path(tmp_path, config_path)
    sweep_output_dir = tmp_path / "sweeps"

    assert validate_sweep_profiles_payload(profile_config_path=profile_path)["status"] == "PASS"
    assert validate_parameter_governance(config_path=config_path)["status"] == "PASS"

    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    candidate_id = _top_candidate_id(sweep_output_dir / sweep_id)

    attribution = run_candidate_attribution(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "candidate_attribution",
    )
    assert attribution["report"]["status"] == "INCOMPLETE"
    assert (
        validate_candidate_attribution_artifact(
            candidate_id=candidate_id,
            output_dir=tmp_path / "candidate_attribution",
        )["status"]
        == "PASS"
    )

    wf_selection = run_walk_forward_selection(
        config_path=config_path,
        profile="tiny_fixture",
        sweep_id=sweep_id,
        profile_config_path=profile_path,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "walk_forward_selection",
    )
    assert (
        validate_walk_forward_selection_artifact(
            wf_selection_id=wf_selection["wf_selection_id"],
            output_dir=tmp_path / "walk_forward_selection",
        )["status"]
        == "PASS"
    )

    overfit = run_overfit_review(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "overfit",
    )
    assert overfit["report"]["overfit_status"] in {
        "LOW_RISK",
        "REVIEW_REQUIRED",
        "HIGH_RISK",
    }
    assert (
        validate_overfit_artifact(
            overfit_id=overfit["overfit_id"],
            output_dir=tmp_path / "overfit",
        )["status"]
        == "PASS"
    )

    registry_path = tmp_path / DEFAULT_SHADOW_REGISTRY_PATH.name
    register_shadow_candidate(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
    )
    monitor = run_shadow_monitor(
        as_of=pd.Timestamp("2026-06-06").date(),
        registry_path=registry_path,
        output_dir=tmp_path / "shadow_monitor",
    )
    assert "Dynamic Rescue Shadow Monitoring" in monitor["report"]["reader_brief_section"]
    assert (
        validate_shadow_monitor_artifact(
            monitor_id=monitor["monitor_id"],
            output_dir=tmp_path / "shadow_monitor",
        )["status"]
        == "PASS"
    )

    index = build_research_index(
        sweep_output_dir=sweep_output_dir,
        shadow_registry_path=registry_path,
        output_dir=tmp_path / "index",
    )
    assert index["candidate_count"] > 0
    assert research_query_payload(
        candidate_id=candidate_id,
        output_dir=tmp_path / "index",
    )["status"] == "PASS"
    assert governance_report_payload(output_dir=tmp_path / "governance")["status"] == "PASS"


def test_dynamic_v3_data_and_injection_audit_contracts(tmp_path: Path) -> None:
    prices_path, rates_path, as_of = _write_real_smoke_cache(tmp_path)
    config_path = _real_smoke_config_path(tmp_path, as_of)

    data_audit = run_data_audit(
        as_of=pd.Timestamp(as_of).date(),
        end=pd.Timestamp(as_of).date(),
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=tmp_path / "data_audit",
    )
    assert data_audit["report"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert (
        validate_data_audit_artifact(
            data_audit_id=data_audit["data_audit_id"],
            output_dir=tmp_path / "data_audit",
        )["status"]
        == "PASS"
    )

    injection = run_injection_audit(
        config_path=config_path,
        as_of=pd.Timestamp(as_of).date(),
        end=pd.Timestamp(as_of).date(),
        max_candidates=2,
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=tmp_path / "injection_audit",
    )
    assert injection["report"]["candidate_count"] == 2
    assert (
        validate_injection_audit_artifact(
            audit_id=injection["audit_id"],
            output_dir=tmp_path / "injection_audit",
        )["status"]
        == "PASS"
    )


def test_reader_brief_dynamic_v3_parameter_research_summary(tmp_path: Path) -> None:
    leaderboard_path = tmp_path / "leaderboard.json"
    leaderboard_path.write_text(
        json.dumps(
            {
                "status": "PASS",
                "evaluator_mode": "tiny_fixture_proxy",
                "evaluator_version": "tiny_fixture_proxy_v1",
                "metrics_source": "tiny_fixture_proxy_formula",
                "not_for_investment_decision": True,
                "candidate_count": 3,
                "top_eligible_candidates": [
                    {"candidate_id": "abc123", "gate": "observe_only", "score": 0.42}
                ],
                "most_common_reject_reasons": [{"reason": "turnover_exceeds_policy", "count": 2}],
                "recommended_next_actions": ["run walk-forward"],
                "production_candidate_generated": False,
                "safety": {
                    "observe_only": True,
                    "candidate_only": True,
                    "production_effect": "none",
                    "broker_action": "none",
                    "manual_review_required": True,
                    "production_state_mutated": False,
                    "baseline_config_mutated": False,
                    "official_target_weights_mutated": False,
                    "automatic_candidate_promotion": False,
                    "auto_enrollment_without_owner_approval": False,
                    "shadow_enrollment_allowed": False,
                    "automatic_enrollment_allowed": False,
                    "owner_approval_executed": False,
                    "production_candidate_generated": False,
                },
            }
        ),
        encoding="utf-8",
    )
    promotion_path = tmp_path / "promotion_manifest.json"
    promotion_path.write_text(
        json.dumps({"status": "review_required", "production_candidate_generated": False}),
        encoding="utf-8",
    )
    evidence_path = tmp_path / "evidence_summary.json"
    evidence_path.write_text(
        json.dumps(
            {
                "backtest_window_status": "PASS",
                "weight_path_status": "PARTIAL",
                "candidate_attribution_status": "PARTIAL",
                "provenance_status": "RECONSTRUCTED_MANIFEST",
                "download_manifest_status": "PASS_WITH_WARNINGS",
                "promotion_blocking_flags": [
                    "WEIGHT_PATH_PARTIAL",
                    "DATA_PROVENANCE_INCOMPLETE",
                ],
            }
        ),
        encoding="utf-8",
    )
    report_index = {
        "reports": [
            _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
            _report_record("etf_dynamic_v3_promotion_pack", evidence_path),
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["evaluator_mode"] == "tiny_fixture_proxy"
    assert summary["not_for_investment_decision"] is True
    assert summary["top_candidate"] == "abc123"
    assert summary["promotion_status"] == "review_required"
    assert summary["backtest_window_status"] == "PASS"
    assert summary["weight_path_status"] == "PARTIAL"
    assert summary["candidate_attribution_status"] == "PARTIAL"
    assert summary["data_provenance_status"] == "RECONSTRUCTED_MANIFEST"
    assert summary["download_manifest_status"] == "PASS_WITH_WARNINGS"
    assert summary["promotion_blocking_flags"] == (
        "WEIGHT_PATH_PARTIAL, DATA_PROVENANCE_INCOMPLETE"
    )
    assert summary["promotion_manifest"] == str(promotion_path)
    assert summary["evidence_summary"] == str(evidence_path)
    assert summary["production_candidate_generated"] is False


def _tiny_config_path(tmp_path: Path) -> Path:
    path = tmp_path / "parameter_sweep_tiny.yaml"
    path.write_text(yaml.safe_dump(_tiny_config(), sort_keys=False), encoding="utf-8")
    return path


def _real_smoke_config_path(tmp_path: Path, as_of: str) -> Path:
    raw = _tiny_config()
    raw["run"]["max_candidates"] = 2
    raw["data"]["as_of"] = as_of
    raw["data"]["end"] = as_of
    raw["data"]["allow_data_quality"] = ["PASS", "PASS_WITH_WARNINGS"]
    raw["parameter_space"] = {
        "rescue_intensity": {"values": [0.50, 0.75]},
        "smooth_window_days": {"values": [5]},
        "constraint_buffer_bps": {"values": [25]},
        "turnover_penalty": {"values": [0.10]},
        "risk_off_confirmation_days": {"values": [2]},
        "rebalance_cooldown_days": {"values": [3]},
        "drawdown_guard": {"values": ["soft"]},
    }
    raw["hard_constraints"]["max_constraint_hit_rate"] = 0.35
    raw["hard_constraints"]["max_false_risk_off_delta"] = 30
    raw["hard_constraints"]["max_turnover"] = 8.0
    raw["hard_constraints"]["max_drawdown_degradation_pp"] = 0.02
    raw["hard_constraints"]["max_dynamic_vs_static_gap"] = 1.0
    raw["hard_constraints"]["allow_robustness_status"] = ["PASS", "REVIEW_REQUIRED", "FAIL"]
    raw["hard_constraints"]["noise_floor_improvement"] = 0.0
    raw["execution"]["workers"] = 1
    raw["execution"]["checkpoint_every_candidates"] = 1
    raw["execution"]["evaluator"] = "real_dynamic_v3_rescue"
    raw["execution"].pop("evaluation_mode", None)
    path = tmp_path / "parameter_sweep_real_smoke.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _profile_config_path(tmp_path: Path, config_path: Path) -> Path:
    path = tmp_path / "parameter_sweep_profiles.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "profiles": {
                    "tiny_fixture": {
                        "description": "test tiny fixture profile",
                        "config_path": str(config_path),
                        "evaluator_mode": "tiny_fixture_proxy",
                        "max_candidates": 12,
                        "workers": 1,
                        "ci_safe": True,
                        "not_for_investment_decision": True,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _tiny_config() -> dict[str, object]:
    raw = yaml.safe_load(DEFAULT_PARAMETER_SWEEP_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["run"]["max_candidates"] = 24
    raw["data"]["quality_status"] = "PASS"
    raw["data"]["manifest_hash"] = "tiny_test_manifest"
    raw["parameter_space"] = {
        "rescue_intensity": {"values": [0.50, 0.75, 1.00]},
        "smooth_window_days": {"values": [3, 10]},
        "constraint_buffer_bps": {"values": [0, 50]},
        "turnover_penalty": {"values": [0.20]},
        "risk_off_confirmation_days": {"values": [1]},
        "rebalance_cooldown_days": {"values": [10]},
        "drawdown_guard": {"values": ["hard"]},
    }
    raw["hard_constraints"]["max_drawdown_degradation_pp"] = 0.01
    raw["hard_constraints"]["max_dynamic_vs_static_gap"] = 0.21
    raw["hard_constraints"]["noise_floor_improvement"] = 0.005
    raw["walk_forward"]["min_windows"] = 2
    raw["walk_forward"]["top_n"] = 3
    return raw


def _write_real_smoke_cache(tmp_path: Path) -> tuple[Path, Path, str]:
    universe = load_universe()
    tickers = list(dict.fromkeys([*configured_price_tickers(universe), "CASH"]))
    series_ids = configured_rate_series(universe)
    dates = pd.bdate_range("2022-01-03", periods=520)
    price_rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 3.0
        drift = 0.00025 + ticker_index * 0.00001
        for day_index, row_date in enumerate(dates):
            close = base * ((1.0 + drift) ** day_index)
            price_rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "ticker": ticker,
                    "open": close,
                    "high": close * 1.001,
                    "low": close * 0.999,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000 + ticker_index,
                }
            )
    rate_rows: list[dict[str, object]] = []
    for series_index, series_id in enumerate(series_ids):
        base = 4.0 + series_index * 0.2
        for day_index, row_date in enumerate(dates):
            rate_rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "series": series_id,
                    "value": base - day_index * 0.0005,
                }
            )
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path, dates[-1].date().isoformat()


def _jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _top_candidate_id(sweep_dir: Path) -> str:
    leaderboard = json.loads((sweep_dir / "leaderboard.json").read_text(encoding="utf-8"))
    return leaderboard["top_eligible_candidates"][0]["candidate_id"]


def _line_value(output: str, key: str) -> str:
    for line in output.splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    raise AssertionError(f"{key}= not found in output:\n{output}")


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-06",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
