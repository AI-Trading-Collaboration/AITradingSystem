from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_SHADOW_REGISTRY_PATH,
    DynamicV3ParameterResearchError,
    build_promotion_pack,
    build_sweep_config_validation,
    candidate_report_payload,
    load_parameter_sweep_config,
    parameter_grid_candidates,
    preview_sweep_candidates,
    register_shadow_candidate,
    run_parameter_sweep,
    run_robustness_diagnostics,
    run_walk_forward_validation,
    stable_candidate_id,
    validate_artifacts_payload,
    validate_promotion_pack,
    validate_robustness_artifact,
    validate_shadow_registry,
    validate_sweep_artifact,
    validate_walk_forward_artifact,
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
    assert (sweep_dir / "leaderboard.json").exists()
    assert (sweep_dir / "sweep_report.md").exists()

    resumed = run_parameter_sweep(config_path=config_path, output_dir=output_dir, resume=sweep_id)
    assert resumed["manifest"]["completed_count"] == len(results)
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
    assert pack["pack"]["status"] in {"review_required", "promote_candidate"}
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


def test_reader_brief_dynamic_v3_parameter_research_summary(tmp_path: Path) -> None:
    leaderboard_path = tmp_path / "leaderboard.json"
    leaderboard_path.write_text(
        json.dumps(
            {
                "status": "PASS",
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
    report_index = {
        "reports": [
            _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
            _report_record("etf_dynamic_v3_promotion_pack", promotion_path),
        ]
    }

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["top_candidate"] == "abc123"
    assert summary["promotion_status"] == "review_required"
    assert summary["production_candidate_generated"] is False


def _tiny_config_path(tmp_path: Path) -> Path:
    path = tmp_path / "parameter_sweep_tiny.yaml"
    path.write_text(yaml.safe_dump(_tiny_config(), sort_keys=False), encoding="utf-8")
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
