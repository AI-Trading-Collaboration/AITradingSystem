from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

import ai_trading_system.etf_portfolio.dynamic_v3_parameter_research as dynamic_v3_research
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
    repair_latest_pointers_payload,
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
    scheduled_observe_payload,
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


@pytest.fixture(scope="module")
def real_smoke_cache(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path, str]:
    return _write_real_smoke_cache(tmp_path_factory.mktemp("dynamic_v3_real_smoke_cache"))


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


def test_injection_audit_selection_starts_with_ofat_matched_pairs() -> None:
    config = load_parameter_sweep_config(DEFAULT_PARAMETER_SWEEP_CONFIG_PATH)

    selected = dynamic_v3_research._select_injection_audit_candidates(
        config,
        max_candidates=8,
    )

    assert len(selected) == 8
    base = selected[0]["parameters"]
    for parameter, candidate in zip(
        dynamic_v3_research.REQUIRED_INJECTION_PARAMETERS,
        selected[1:],
        strict=True,
    ):
        changed = {name for name, value in candidate["parameters"].items() if value != base[name]}
        assert changed == {parameter}


def test_parameter_effect_summary_does_not_cross_attribute_other_axis_changes() -> None:
    parameters = {
        "rescue_intensity": 0.5,
        "smooth_window_days": 5,
        "constraint_buffer_bps": 25,
        "turnover_penalty": 0.1,
        "risk_off_confirmation_days": 2,
        "rebalance_cooldown_days": 3,
        "drawdown_guard": "soft",
    }

    def row(
        candidate_id: str,
        *,
        changes: dict[str, object] | None = None,
        config_hash: str = "config-a",
        metric_hash: str = "metric-a",
    ) -> dict[str, object]:
        return {
            "candidate_id": candidate_id,
            **parameters,
            **(changes or {}),
            "effective_real_policy_hash": config_hash,
            "effective_rescue_policy_hash": "rescue-a",
            "metric_hash": metric_hash,
            "latest_weight_hash": "weight-a",
        }

    matrix = [
        row("base"),
        row(
            "rescue-change",
            changes={"rescue_intensity": 0.75},
            config_hash="config-b",
            metric_hash="metric-b",
        ),
        row("smooth-change", changes={"smooth_window_days": 10}),
    ]

    effects = dynamic_v3_research._parameter_effect_summary(matrix, matrix)
    by_parameter = {effect["parameter"]: effect for effect in effects}

    assert by_parameter["rescue_intensity"]["effect_status"] == "EFFECTIVE"
    assert by_parameter["rescue_intensity"]["matched_pair_count"] == 1
    assert by_parameter["smooth_window_days"]["effect_status"] == "NOT_CONSUMED"
    assert by_parameter["smooth_window_days"]["config_changed_pair_count"] == 0
    assert (
        by_parameter["constraint_buffer_bps"]["effect_status"]
        == "INSUFFICIENT_MATCHED_PAIR_EVIDENCE"
    )


def test_injection_audit_validation_requires_and_accepts_complete_pair_coverage(
    tmp_path: Path,
) -> None:
    audit_id = "pair-complete"
    audit_dir = tmp_path / audit_id
    audit_dir.mkdir()
    effect_rows = [
        {
            "parameter": parameter,
            "effect_status": "NO_OBSERVED_EFFECT",
            "matched_pair_count": 1,
        }
        for parameter in dynamic_v3_research.REQUIRED_INJECTION_PARAMETERS
    ]
    (audit_dir / "injection_audit_manifest.json").write_text(
        json.dumps(
            {
                "audit_id": audit_id,
                "status": "PASS",
                "candidate_count": 8,
                "max_candidates": 8,
                "parameter_effect_pair_coverage_complete": True,
            }
        ),
        encoding="utf-8",
    )
    (audit_dir / "candidate_parameter_matrix.csv").write_text(
        "candidate_id\nbase\n",
        encoding="utf-8",
    )
    (audit_dir / "weight_path_diff_summary.json").write_text(
        json.dumps({"distinct_latest_weight_hash_count": 1}),
        encoding="utf-8",
    )
    (audit_dir / "metric_diff_summary.json").write_text("{}", encoding="utf-8")
    (audit_dir / "parameter_effect_summary.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "parameter_effect_pair_coverage_complete": True,
                "parameter_effects": effect_rows,
            }
        ),
        encoding="utf-8",
    )
    (audit_dir / "parameter_effect_report.md").write_text("# report\n", encoding="utf-8")

    validation = validate_injection_audit_artifact(audit_id=audit_id, output_dir=tmp_path)
    report = dynamic_v3_research.injection_audit_report_payload(
        audit_id=audit_id,
        output_dir=tmp_path,
    )

    assert validation["status"] == "PASS"
    assert report["parameter_effect_pair_coverage_complete"] is True
    assert report["parameter_effects"] == effect_rows


def test_injection_audit_report_downgrades_legacy_summary_without_traceback(
    tmp_path: Path,
) -> None:
    audit_id = "legacy-audit"
    audit_dir = tmp_path / audit_id
    audit_dir.mkdir()
    (audit_dir / "injection_audit_manifest.json").write_text(
        json.dumps({"audit_id": audit_id, "status": "PASS", "candidate_count": 20}),
        encoding="utf-8",
    )
    (audit_dir / "weight_path_diff_summary.json").write_text("{}", encoding="utf-8")
    (audit_dir / "metric_diff_summary.json").write_text("{}", encoding="utf-8")

    report = dynamic_v3_research.injection_audit_report_payload(
        audit_id=audit_id,
        output_dir=tmp_path,
    )

    assert report["status"] == "INCOMPLETE"
    assert report["parameter_effect_pair_coverage_complete"] is False
    assert report["parameters_without_matched_pairs"] == list(
        dynamic_v3_research.REQUIRED_INJECTION_PARAMETERS
    )
    assert report["limitations"] == ["legacy_parameter_effect_summary_missing"]


def test_weight_path_validation_derives_partial_completeness_from_files(tmp_path: Path) -> None:
    evaluation_id = "evaluation_partial"
    artifact_dir = _write_weight_path_fixture(tmp_path, evaluation_id=evaluation_id)

    validation = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=tmp_path)
    report = weight_path_report_payload(evaluation_id=evaluation_id, search_root=tmp_path)

    assert validation["status"] == "PASS"
    assert validation["declared_attribution_completeness"] == "PARTIAL"
    assert validation["observed_attribution_completeness"] == "PARTIAL"
    assert report["status"] == "PARTIAL"
    assert report["failed_check_count"] == 0
    assert Path(report["daily_weights_path"]) == artifact_dir / "daily_weights.csv"


def test_weight_path_validation_rejects_forged_complete_metadata(tmp_path: Path) -> None:
    evaluation_id = "evaluation_forged_complete"
    artifact_dir = _write_weight_path_fixture(tmp_path, evaluation_id=evaluation_id)
    metadata_path = artifact_dir / "weight_path_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["attribution_completeness"] = "COMPLETE"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    validation = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=tmp_path)
    report = weight_path_report_payload(evaluation_id=evaluation_id, search_root=tmp_path)

    assert validation["status"] == "FAIL"
    assert validation["observed_attribution_completeness"] == "PARTIAL"
    assert report["status"] == "PARTIAL"
    assert "declared_completeness_matches_observed" in {
        check["check_id"] for check in validation["checks"] if not check["passed"]
    }


def test_weight_path_validation_rejects_forged_complete_detail_level(tmp_path: Path) -> None:
    evaluation_id = "evaluation_forged_complete_detail"
    artifact_dir = _write_weight_path_fixture(tmp_path, evaluation_id=evaluation_id)
    metadata_path = artifact_dir / "weight_path_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["attribution_completeness"] = "COMPLETE"
    metadata["weight_path_detail_level"] = "complete"
    metadata["missing_fields"] = []
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    validation = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=tmp_path)

    assert validation["status"] == "FAIL"
    assert validation["observed_attribution_completeness"] == "INCOMPLETE"
    assert "complete_detail_fields_supported" in {
        check["check_id"] for check in validation["checks"] if not check["passed"]
    }


def test_weight_path_validation_rejects_invalid_weight_content(tmp_path: Path) -> None:
    evaluation_id = "evaluation_invalid_weights"
    artifact_dir = _write_weight_path_fixture(tmp_path, evaluation_id=evaluation_id)
    daily_path = artifact_dir / "daily_weights.csv"
    daily = pd.read_csv(daily_path)
    daily.loc[0, "weight"] = 0.9
    daily.to_csv(daily_path, index=False)

    validation = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=tmp_path)
    report = weight_path_report_payload(evaluation_id=evaluation_id, search_root=tmp_path)

    assert validation["status"] == "FAIL"
    assert validation["observed_attribution_completeness"] == "INCOMPLETE"
    assert report["status"] == "INCOMPLETE"
    assert "daily_weights_sum_to_one_by_date" in {
        check["check_id"] for check in validation["checks"] if not check["passed"]
    }


def test_weight_path_lookup_fails_closed_on_duplicate_evaluation_id(tmp_path: Path) -> None:
    evaluation_id = "evaluation_duplicate"
    _write_weight_path_fixture(tmp_path / "first", evaluation_id=evaluation_id)
    _write_weight_path_fixture(tmp_path / "second", evaluation_id=evaluation_id)

    validation = validate_weight_path_artifact(evaluation_id=evaluation_id, search_root=tmp_path)

    assert validation["status"] == "FAIL"
    assert validation["observed_attribution_completeness"] == "INCOMPLETE"
    with pytest.raises(DynamicV3ParameterResearchError, match="ambiguous"):
        weight_path_report_payload(evaluation_id=evaluation_id, search_root=tmp_path)


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

    resumed_with_worker_override = run_parameter_sweep(
        config_path=config_path,
        output_dir=output_dir,
        resume=sweep_id,
        workers=2,
    )
    assert resumed_with_worker_override["manifest"]["execution"]["workers"] == 2
    assert "resume worker override applied: workers=2" in (sweep_dir / "run.log").read_text(
        encoding="utf-8"
    )

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


def test_candidate_attribution_requires_explicit_candidate_report(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    sweep_output_dir = tmp_path / "sweeps"
    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    candidate_id = _top_candidate_id(sweep_output_dir / sweep_id)
    attribution_root = tmp_path / "candidate_attribution"

    with pytest.raises(DynamicV3ParameterResearchError, match="candidate report is required"):
        run_candidate_attribution(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=attribution_root,
        )

    assert not (attribution_root / candidate_id).exists()


def test_candidate_attribution_validation_recomputes_status_and_source_checksum(
    tmp_path: Path,
) -> None:
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
    attribution_root = tmp_path / "candidate_attribution"
    run_candidate_attribution(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=attribution_root,
    )
    assert (
        validate_candidate_attribution_artifact(
            candidate_id=candidate_id,
            output_dir=attribution_root,
        )["status"]
        == "PASS"
    )

    manifest_path = attribution_root / candidate_id / "attribution_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "PARTIAL"
    manifest["attribution_completeness"] = "PARTIAL"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    forged = validate_candidate_attribution_artifact(
        candidate_id=candidate_id,
        output_dir=attribution_root,
    )
    assert forged["status"] == "FAIL"
    assert "attribution_status_matches_source" in {
        check["check_id"] for check in forged["checks"] if not check["passed"]
    }

    manifest["status"] = "INCOMPLETE"
    manifest["attribution_completeness"] = "INCOMPLETE"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    candidate_report_path = (
        sweep_output_dir / sweep_id / "candidates" / candidate_id / "candidate_report.json"
    )
    candidate_report_path.write_text(
        candidate_report_path.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    stale_source = validate_candidate_attribution_artifact(
        candidate_id=candidate_id,
        output_dir=attribution_root,
    )
    assert stale_source["status"] == "FAIL"
    assert "candidate_report_checksum_matches" in {
        check["check_id"] for check in stale_source["checks"] if not check["passed"]
    }


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
    source_leaderboard = json.loads(
        (sweep_output_dir / sweep_id / "leaderboard.json").read_text(encoding="utf-8")
    )
    wf_leaderboard = json.loads(
        (tmp_path / "walk_forward" / wf["walk_forward_id"] / "wf_leaderboard.json").read_text(
            encoding="utf-8"
        )
    )
    assert wf["report"]["source_sweep_id"] == sweep_id
    assert wf["report"]["holdout_start"]
    assert wf["report"]["holdout_end"]
    assert wf["report"]["oos_summary"]["oos_recommendation"] == "continue_to_robustness"
    assert [row["candidate_id"] for row in wf_leaderboard["candidates"]] == [
        row["candidate_id"] for row in source_leaderboard["top_eligible_candidates"][:3]
    ]
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
    robustness_manifest = json.loads(
        (
            tmp_path / "robustness" / robustness["robustness_id"] / "robustness_manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert robustness_manifest["evaluator_mode"] == "tiny_fixture_proxy"
    assert robustness_manifest["metrics_source"] == "tiny_fixture_proxy_formula"
    assert robustness_manifest["not_for_investment_decision"] is True
    assert robustness_manifest["sensitivity_evidence_status"] == "TINY_FIXTURE_PROXY"

    registry_path = tmp_path / DEFAULT_SHADOW_REGISTRY_PATH.name
    registered = register_shadow_candidate(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=tmp_path / "walk_forward",
        robustness_dir=tmp_path / "robustness",
    )
    assert registered["status"] == "PASS"
    assert registered["candidate"]["observation_basis_status"] == "complete"
    assert registered["candidate"]["source_walk_forward_id"] == wf["walk_forward_id"]
    assert registered["candidate"]["source_robustness_id"] == robustness["robustness_id"]
    assert (
        validate_shadow_registry(
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
            walk_forward_dir=tmp_path / "walk_forward",
            robustness_dir=tmp_path / "robustness",
        )["status"]
        == "PASS"
    )

    window_audit_dir = tmp_path / "window_audit"
    latest_window_audit_dir = window_audit_dir / "window-audit-test"
    latest_window_audit_dir.mkdir(parents=True)
    (latest_window_audit_dir / "window_audit_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "etf_dynamic_v3_window_audit_manifest",
                "window_audit_id": "window-audit-test",
                "status": "FAIL",
                "configured_backtest_start": "2022-12-01",
                "requested_start": "2022-12-01",
                "requested_end": "2026-06-05",
                "earliest_actual_evaluation_start": "",
                "artifact_count": 2,
                "promotion_blocking_count": 2,
            }
        ),
        encoding="utf-8",
    )
    (latest_window_audit_dir / "window_audit_report.md").write_text(
        "# Window audit\n",
        encoding="utf-8",
    )

    pack = build_promotion_pack(
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=tmp_path / "walk_forward",
        robustness_dir=tmp_path / "robustness",
        window_audit_dir=window_audit_dir,
        output_dir=tmp_path / "promotion",
    )
    assert pack["pack"]["status"] in {"incomplete", "review_required", "reject"}
    assert "tiny_fixture_not_for_investment_decision" in pack["pack"]["decision_reasons"]
    assert pack["pack"]["production_candidate_generated"] is False
    evidence_summary = pack["pack"]["evidence_summary"]
    assert evidence_summary["backtest_window_status"] == "FAIL"
    assert evidence_summary["window_audit_id"] == "window-audit-test"
    assert "BACKTEST_WINDOW_INCOMPLETE" in evidence_summary["promotion_blocking_flags"]
    assert pack["pack"]["linked_artifacts"]["window_audit"].endswith("window_audit_manifest.json")
    assert (
        validate_promotion_pack(
            candidate_id=candidate_id,
            output_dir=tmp_path / "promotion",
        )["status"]
        == "PASS"
    )
    (tmp_path / "walk_forward" / wf["walk_forward_id"] / "wf_report.md").unlink()
    broken_wf_validation = validate_walk_forward_artifact(
        walk_forward_id=wf["walk_forward_id"],
        output_dir=tmp_path / "walk_forward",
    )
    assert broken_wf_validation["status"] == "FAIL"
    assert "artifact_exists:wf_report.md" in {
        check["check_id"] for check in broken_wf_validation["checks"] if not check["passed"]
    }
    pointer_dir = tmp_path / "latest"
    assert validate_artifacts_payload(pointer_dir=pointer_dir)["status"] == "FAIL"
    pointer_target = tmp_path / "pointer_target.json"
    pointer_target.write_text("{}", encoding="utf-8")
    _write_latest_pointer(pointer_dir, "latest_sweep", "sweep-test", pointer_target)
    assert validate_artifacts_payload(pointer_dir=pointer_dir)["status"] == "PASS"


def test_shadow_registry_requires_candidate_report_and_rejects_hard_reject(
    tmp_path: Path,
) -> None:
    config_path = _tiny_config_path(tmp_path)
    sweep_output_dir = tmp_path / "sweeps"
    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    sweep_dir = sweep_output_dir / sweep_id
    candidate_id = _top_candidate_id(sweep_dir)
    registry_path = tmp_path / DEFAULT_SHADOW_REGISTRY_PATH.name

    with pytest.raises(DynamicV3ParameterResearchError, match="candidate report is required"):
        register_shadow_candidate(
            sweep_id=sweep_id,
            candidate_id=candidate_id,
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
            walk_forward_dir=tmp_path / "missing_walk_forward",
            robustness_dir=tmp_path / "missing_robustness",
        )

    candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )
    registered = register_shadow_candidate(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        registry_path=registry_path,
        sweep_output_dir=sweep_output_dir,
        walk_forward_dir=tmp_path / "missing_walk_forward",
        robustness_dir=tmp_path / "missing_robustness",
    )
    assert registered["candidate"]["observation_basis_status"] == "incomplete_observation_basis"
    assert registered["candidate"]["source_walk_forward_id"] == ""
    assert registered["candidate"]["source_robustness_id"] == ""
    assert (
        validate_shadow_registry(
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
            walk_forward_dir=tmp_path / "missing_walk_forward",
            robustness_dir=tmp_path / "missing_robustness",
        )["status"]
        == "PASS"
    )

    rejected = next(
        row for row in _jsonl(sweep_dir / "candidate_results.jsonl") if row["gate"] == "reject"
    )
    rejected_candidate_id = str(rejected["candidate_id"])
    candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=rejected_candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )
    with pytest.raises(DynamicV3ParameterResearchError, match="rejected candidate"):
        register_shadow_candidate(
            sweep_id=sweep_id,
            candidate_id=rejected_candidate_id,
            registry_path=registry_path,
            sweep_output_dir=sweep_output_dir,
            walk_forward_dir=tmp_path / "missing_walk_forward",
            robustness_dir=tmp_path / "missing_robustness",
        )


def test_dynamic_v3_schedule_observe_gate_handles_due_and_pointer_failures(
    tmp_path: Path,
) -> None:
    config_path = _tiny_config_path(tmp_path)
    pointer_dir = tmp_path / "latest"
    output_dir = tmp_path / "schedule"
    generated = datetime(2026, 5, 8, 21, 0, tzinfo=UTC)

    not_due = scheduled_observe_payload(
        as_of=datetime(2026, 5, 6, tzinfo=UTC).date(),
        config_path=config_path,
        pointer_dir=pointer_dir,
        output_dir=output_dir,
        now=generated,
    )
    assert not_due["status"] == "PASS_WITH_SKIPS"
    assert not_due["due_status"] == "NOT_DUE"

    due_no_pointers = scheduled_observe_payload(
        as_of=datetime(2026, 5, 8, tzinfo=UTC).date(),
        config_path=config_path,
        pointer_dir=pointer_dir,
        output_dir=output_dir,
        now=generated,
    )
    assert due_no_pointers["status"] == "PASS_WITH_SKIPS"
    assert due_no_pointers["due_status"] == "DUE_NO_POINTERS"
    assert Path(due_no_pointers["output_artifacts"]["markdown"]).exists()

    _write_latest_pointer(pointer_dir, "latest_sweep", "missing", tmp_path / "missing.json")
    broken = scheduled_observe_payload(
        as_of=datetime(2026, 5, 8, tzinfo=UTC).date(),
        config_path=config_path,
        pointer_dir=pointer_dir,
        output_dir=output_dir,
        now=generated,
    )
    assert broken["status"] == "FAIL"
    assert broken["artifact_validation"]["status"] == "FAIL"

    pointer_target = tmp_path / "sweep_manifest.json"
    pointer_target.write_text("{}", encoding="utf-8")
    _write_latest_pointer(pointer_dir, "latest_sweep", "sweep-ok", pointer_target, now=generated)
    valid = scheduled_observe_payload(
        as_of=datetime(2026, 5, 8, tzinfo=UTC).date(),
        config_path=config_path,
        pointer_dir=pointer_dir,
        output_dir=output_dir,
        registry_path=tmp_path / "missing_registry.yaml",
        now=generated,
    )
    assert valid["status"] == "PASS"
    assert valid["artifact_validation"]["status"] == "PASS"
    assert valid["shadow_monitor"]["status"] == "SKIPPED"
    assert valid["production_candidate_generated"] is False


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


def test_real_dynamic_v3_rescue_sweep_smoke_writes_real_artifacts(
    tmp_path: Path,
    real_smoke_cache: tuple[Path, Path, str],
) -> None:
    prices_path, rates_path, as_of = real_smoke_cache
    config_path = _real_smoke_config_path(tmp_path, as_of)
    output_dir = tmp_path / "real_sweeps"

    result = run_parameter_sweep(
        config_path=config_path,
        output_dir=output_dir,
        workers=2,
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
    robustness = run_robustness_diagnostics(
        sweep_id=sweep_id,
        candidate_id=results[0]["candidate_id"],
        sweep_output_dir=output_dir,
        output_dir=tmp_path / "real_robustness",
    )
    robustness_dir = tmp_path / "real_robustness" / robustness["robustness_id"]
    robustness_manifest = json.loads(
        (robustness_dir / "robustness_manifest.json").read_text(encoding="utf-8")
    )
    robustness_diagnostics = json.loads(
        (robustness_dir / "overfit_diagnostics.json").read_text(encoding="utf-8")
    )
    sensitivity = pd.read_csv(robustness_dir / "sensitivity_matrix.csv")
    assert robustness_manifest["evaluator_mode"] == "real_dynamic_v3_rescue"
    assert robustness_manifest["metrics_source"] == "real_evaluation_artifact"
    assert robustness_manifest["data_quality"]["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert robustness_manifest["source_real_evaluation_artifact_path"] == str(first_artifact)
    assert robustness_manifest["source_real_evaluation_artifact_exists"] is True
    assert robustness_manifest["real_neighbor_count"] >= 1
    assert robustness_manifest["missing_real_neighbor_count"] == 0
    assert robustness_manifest["stress_evidence_status"] in {
        "PASS",
        "PARTIAL_REAL_STRESS_EVIDENCE",
    }
    assert robustness_manifest["regime_evidence_status"] == "PASS"
    assert robustness_diagnostics["sensitivity_evidence_status"] == "PASS"
    assert (
        robustness_diagnostics["stress_evidence_status"]
        == robustness_manifest["stress_evidence_status"]
    )
    assert robustness_diagnostics["regime_evidence_status"] == "PASS"
    assert set(sensitivity["sensitivity_evidence_source"]) == {"real_evaluation_artifact"}
    assert "tiny_fixture_proxy" not in set(sensitivity["metrics_source"].astype(str))
    robustness_validation = validate_robustness_artifact(
        robustness_id=robustness["robustness_id"],
        output_dir=tmp_path / "real_robustness",
    )
    assert robustness_validation["status"] == "PASS"
    (robustness_dir / "robustness_report.md").unlink()
    broken_robustness_validation = validate_robustness_artifact(
        robustness_id=robustness["robustness_id"],
        output_dir=tmp_path / "real_robustness",
    )
    assert broken_robustness_validation["status"] == "FAIL"
    assert "artifact_exists:robustness_report.md" in {
        check["check_id"] for check in broken_robustness_validation["checks"] if not check["passed"]
    }

    (sweep_dir / "candidate_results.jsonl").write_text(
        json.dumps(results[0], sort_keys=True) + "\n",
        encoding="utf-8",
    )
    missing_neighbor = run_robustness_diagnostics(
        sweep_id=sweep_id,
        candidate_id=results[0]["candidate_id"],
        sweep_output_dir=output_dir,
        output_dir=tmp_path / "real_robustness_missing_neighbor",
    )
    missing_manifest = json.loads(
        (
            tmp_path
            / "real_robustness_missing_neighbor"
            / missing_neighbor["robustness_id"]
            / "robustness_manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert missing_manifest["status"] == "REVIEW_REQUIRED"
    assert missing_manifest["missing_real_neighbor_count"] >= 1
    assert (
        validate_robustness_artifact(
            robustness_id=missing_neighbor["robustness_id"],
            output_dir=tmp_path / "real_robustness_missing_neighbor",
        )["status"]
        == "PASS"
    )
    (sweep_dir / "candidate_results.jsonl").write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in results) + "\n",
        encoding="utf-8",
    )

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
    assert weight_validation["declared_attribution_completeness"] == "PARTIAL"
    weight_report = weight_path_report_payload(evaluation_id=evaluation_id, search_root=output_dir)
    assert weight_report["status"] == "PARTIAL"
    assert weight_report["declared_attribution_completeness"] == "PARTIAL"
    assert Path(weight_report["daily_weights_path"]).exists()
    assert Path(weight_report["weight_path_metadata_path"]).exists()
    weight_metadata = json.loads(
        Path(weight_report["weight_path_metadata_path"]).read_text(encoding="utf-8")
    )
    assert weight_metadata["schema_version"] == 1
    assert weight_metadata["report_type"] == "etf_dynamic_v3_weight_path_metadata"
    assert weight_metadata["status"] == "PASS"
    assert weight_metadata["production_effect"] == "none"

    candidate_report_path = (
        output_dir
        / sweep_id
        / "candidates"
        / results[0]["candidate_id"]
        / "candidate_report.json"
    )
    candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=results[0]["candidate_id"],
        output_dir=output_dir,
        write=True,
    )
    candidate_report_before = candidate_report_path.read_bytes()
    attribution = run_candidate_attribution(
        sweep_id=sweep_id,
        candidate_id=results[0]["candidate_id"],
        sweep_output_dir=output_dir,
        output_dir=tmp_path / "candidate_attribution",
    )
    assert attribution["report"]["status"] == "PARTIAL"
    assert attribution["report"]["attribution_completeness"] == "PARTIAL"
    assert attribution["report"]["weight_path_observed_completeness"] == "PARTIAL"
    assert candidate_report_path.read_bytes() == candidate_report_before
    weight_delta = pd.read_csv(
        tmp_path
        / "candidate_attribution"
        / results[0]["candidate_id"]
        / "weight_path_delta.csv"
    )
    assert not weight_delta.empty
    assert {
        "as_of",
        "candidate_id",
        "source_sweep_id",
        "reference_group",
        "symbol",
        "candidate_weight",
        "baseline_weight",
        "delta",
    } <= set(weight_delta.columns)
    assert (
        weight_delta["candidate_weight"] - weight_delta["baseline_weight"]
    ).sub(weight_delta["delta"]).abs().max() <= 1e-9
    assert (
        validate_candidate_attribution_artifact(
            candidate_id=results[0]["candidate_id"],
            output_dir=tmp_path / "candidate_attribution",
        )["status"]
        == "PASS"
    )
    weight_delta.loc[0, "delta"] = float(weight_delta.loc[0, "delta"]) + 0.1
    weight_delta.to_csv(
        tmp_path
        / "candidate_attribution"
        / results[0]["candidate_id"]
        / "weight_path_delta.csv",
        index=False,
    )
    broken_delta = validate_candidate_attribution_artifact(
        candidate_id=results[0]["candidate_id"],
        output_dir=tmp_path / "candidate_attribution",
    )
    assert broken_delta["status"] == "FAIL"
    assert "weight_path_delta_matches_source" in {
        check["check_id"] for check in broken_delta["checks"] if not check["passed"]
    }

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
        check["check_id"] == "real_evaluation_artifact_paths_exist" and check["passed"] is False
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


def test_dynamic_v3_latest_pointer_ignores_external_artifact_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_dir = tmp_path / "latest"
    canonical_root = tmp_path / "canonical_dynamic_v3_root"
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_LATEST_POINTER_DIR", latest_dir)
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_DYNAMIC_V3_RESEARCH_ROOT", canonical_root)

    run_window_audit(
        as_of=pd.Timestamp("2022-12-01").date(),
        end=pd.Timestamp("2026-06-04").date(),
        artifact_root=tmp_path / "external_artifacts",
        output_dir=tmp_path / "external_window_audit",
    )

    assert not (latest_dir / "latest_window_audit.json").exists()

    result = run_window_audit(
        as_of=pd.Timestamp("2022-12-01").date(),
        end=pd.Timestamp("2026-06-04").date(),
        artifact_root=tmp_path / "canonical_artifacts",
        output_dir=canonical_root / "window_audit",
    )
    pointer = json.loads((latest_dir / "latest_window_audit.json").read_text(encoding="utf-8"))
    assert pointer["artifact_id"] == result["window_audit_id"]
    assert Path(pointer["path"]).exists()


def test_window_audit_latest_missing_target_is_structured_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_LATEST_POINTER_DIR", latest_dir)
    (latest_dir / "latest_window_audit.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "window_audit",
                "artifact_id": "missing_audit",
                "path": str(tmp_path / "missing_audit" / "window_audit_manifest.json"),
                "exists": True,
            }
        ),
        encoding="utf-8",
    )

    payload = window_audit_report_payload(latest=True, output_dir=tmp_path / "window_audit")

    assert payload["status"] == "FAIL"
    assert payload["failure_reason"] == "latest_pointer_target_missing"
    assert payload["window_audit_id"] == "missing_audit"


def test_window_audit_latest_rejects_external_pointer_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    canonical_root = tmp_path / "canonical_dynamic_v3_root"
    external_manifest = tmp_path / "external" / "window_audit_manifest.json"
    external_manifest.parent.mkdir()
    external_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "etf_dynamic_v3_window_audit_manifest",
                "window_audit_id": "external_audit",
                "status": "PASS",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_LATEST_POINTER_DIR", latest_dir)
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_DYNAMIC_V3_RESEARCH_ROOT", canonical_root)
    (latest_dir / "latest_window_audit.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "window_audit",
                "artifact_id": "external_audit",
                "path": str(external_manifest),
                "exists": True,
            }
        ),
        encoding="utf-8",
    )

    payload = window_audit_report_payload(latest=True, output_dir=canonical_root / "window_audit")

    assert payload["status"] == "FAIL"
    assert payload["failure_reason"] == "latest_pointer_target_outside_canonical_root"


def test_artifact_validation_rejects_external_default_latest_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    canonical_root = tmp_path / "canonical_dynamic_v3_root"
    external_manifest = tmp_path / "external" / "sweep_manifest.json"
    external_manifest.parent.mkdir()
    external_manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_LATEST_POINTER_DIR", latest_dir)
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_DYNAMIC_V3_RESEARCH_ROOT", canonical_root)
    (latest_dir / "latest_sweep.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "sweep",
                "artifact_id": "external_sweep",
                "path": str(external_manifest),
                "exists": True,
            }
        ),
        encoding="utf-8",
    )

    validation = validate_artifacts_payload(pointer_dir=latest_dir)

    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "latest_sweep:pointer_target_in_canonical_root"
        and check["passed"] is False
        for check in validation["checks"]
    )


def test_repair_latest_pointers_rebinds_to_canonical_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    canonical_root = tmp_path / "canonical_dynamic_v3_root"
    sweep_manifest = canonical_root / "sweeps" / "sweep_canonical" / "sweep_manifest.json"
    sweep_manifest.parent.mkdir(parents=True)
    sweep_manifest.write_text(
        json.dumps({"schema_version": 1, "sweep_id": "sweep_canonical"}),
        encoding="utf-8",
    )
    external_manifest = tmp_path / "external" / "sweep_manifest.json"
    external_manifest.parent.mkdir()
    external_manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_LATEST_POINTER_DIR", latest_dir)
    monkeypatch.setattr(dynamic_v3_research, "DEFAULT_DYNAMIC_V3_RESEARCH_ROOT", canonical_root)
    (latest_dir / "latest_sweep.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": "sweep",
                "artifact_id": "external_sweep",
                "path": str(external_manifest),
                "exists": True,
            }
        ),
        encoding="utf-8",
    )

    payload = repair_latest_pointers_payload(
        pointer_dir=latest_dir,
        artifact_root=canonical_root,
    )

    pointer = json.loads((latest_dir / "latest_sweep.json").read_text(encoding="utf-8"))
    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert pointer["artifact_id"] == "sweep_canonical"
    assert pointer["path"] == str(sweep_manifest)
    assert payload["validation"]["status"] == "PASS"


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
    assert (
        "actual_evaluation_start_after_configured_backtest_start"
        in inspected["record"]["window_mismatch_reasons"]
    )


def test_window_audit_keeps_pre_regime_actual_range_distinct_from_configured_start(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "pre_regime_evidence.json"
    artifact_path.write_text(
        json.dumps(
            {
                "report_type": "etf_dynamic_v3_real_evaluation_report",
                "status": "PASS",
                "market_regime": {"default_backtest_start": "2022-12-01"},
                "requested_range": {"start": "2021-02-22", "end": "2026-06-04"},
                "daily_path_summary": {
                    "first_signal_date": "2021-02-22",
                    "last_signal_date": "2026-06-04",
                    "row_count": 1320,
                },
            }
        ),
        encoding="utf-8",
    )

    inspected = inspect_window_artifact(artifact_path=artifact_path)
    record = inspected["record"]

    assert inspected["status"] == "PASS"
    assert record["configured_backtest_start"] == "2022-12-01"
    assert record["requested_start"] == "2021-02-22"
    assert record["actual_evaluation_start"] == "2021-02-22"
    assert record["promotion_blocking"] is False
    assert record["window_mismatch_reasons"] == []

    cli_result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "window-audit",
            "inspect-artifact",
            "--artifact-path",
            str(artifact_path),
        ],
    )
    assert cli_result.exit_code == 0
    assert "configured_backtest_start=2022-12-01" in cli_result.stdout
    assert "actual_evaluation_start=2021-02-22" in cli_result.stdout
    assert "promotion_blocking=false" in cli_result.stdout


def test_dynamic_v3_stable_real_loop_artifact_contracts(tmp_path: Path) -> None:
    config_path = _tiny_config_path(tmp_path)
    profile_path = _profile_config_path(tmp_path, config_path)
    sweep_output_dir = tmp_path / "sweeps"

    assert validate_sweep_profiles_payload(profile_config_path=profile_path)["status"] == "PASS"
    assert validate_parameter_governance(config_path=config_path)["status"] == "PASS"

    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    candidate_id = _top_candidate_id(sweep_output_dir / sweep_id)

    candidate_report_payload(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        output_dir=sweep_output_dir,
        write=True,
    )
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
    assert (
        research_query_payload(
            candidate_id=candidate_id,
            output_dir=tmp_path / "index",
        )["status"]
        == "PASS"
    )
    assert governance_report_payload(output_dir=tmp_path / "governance")["status"] == "PASS"


def test_walk_forward_and_overfit_evidence_fail_closed_on_proxy_and_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = _tiny_config_path(tmp_path)
    profile_path = _profile_config_path(tmp_path, config_path)
    sweep_output_dir = tmp_path / "sweeps"
    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    candidate_id = _top_candidate_id(sweep_output_dir / sweep_id)

    def reject_hash_evidence(*_parts: object) -> int:
        raise AssertionError("stable hash must not generate walk-forward evidence")

    monkeypatch.setattr(dynamic_v3_research, "_stable_int", reject_hash_evidence)
    selection = run_walk_forward_selection(
        config_path=config_path,
        profile="tiny_fixture",
        sweep_id=sweep_id,
        profile_config_path=profile_path,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "walk_forward_selection",
    )
    assert selection["report"]["status"] == "INCOMPLETE"
    assert selection["report"]["evidence_completeness"] == "PROXY_ONLY"
    selection_dir = selection["wf_selection_dir"]
    assert (
        validate_walk_forward_selection_artifact(
            wf_selection_id=selection["wf_selection_id"],
            output_dir=tmp_path / "walk_forward_selection",
        )["status"]
        == "PASS"
    )
    source_results_path = sweep_output_dir / sweep_id / "candidate_results.jsonl"
    source_results_bytes = source_results_path.read_bytes()
    source_results_path.write_bytes(source_results_bytes + b"\n")
    assert (
        validate_walk_forward_selection_artifact(
            wf_selection_id=selection["wf_selection_id"],
            output_dir=tmp_path / "walk_forward_selection",
        )["status"]
        == "FAIL"
    )
    source_results_path.write_bytes(source_results_bytes)
    test_rows_path = selection_dir / "test_window_results.jsonl"
    test_rows = [json.loads(line) for line in test_rows_path.read_text().splitlines()]
    test_rows[0]["candidate_id"] = "tampered_candidate"
    test_rows_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in test_rows) + "\n",
        encoding="utf-8",
    )
    assert (
        validate_walk_forward_selection_artifact(
            wf_selection_id=selection["wf_selection_id"],
            output_dir=tmp_path / "walk_forward_selection",
        )["status"]
        == "FAIL"
    )

    overfit = run_overfit_review(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "overfit",
    )
    assert overfit["report"]["overfit_status"] == "REVIEW_REQUIRED"
    assert overfit["report"]["evidence_completeness"] == "PROXY_ONLY"
    assert (
        validate_overfit_artifact(
            overfit_id=overfit["overfit_id"],
            output_dir=tmp_path / "overfit",
        )["status"]
        == "PASS"
    )
    extreme_path = overfit["overfit_dir"] / "extreme_day_dependency.json"
    extreme = json.loads(extreme_path.read_text(encoding="utf-8"))
    extreme["status"] = "PASS"
    extreme_path.write_text(json.dumps(extreme, sort_keys=True), encoding="utf-8")
    assert (
        validate_overfit_artifact(
            overfit_id=overfit["overfit_id"],
            output_dir=tmp_path / "overfit",
        )["status"]
        == "FAIL"
    )


def test_real_walk_forward_selection_uses_window_daily_paths(
    tmp_path: Path,
) -> None:
    config_path = _tiny_config_path(tmp_path)
    sweep_output_dir = tmp_path / "sweeps"
    sweep = run_parameter_sweep(config_path=config_path, output_dir=sweep_output_dir)
    sweep_id = sweep["sweep_id"]
    sweep_dir = sweep_output_dir / sweep_id
    normalized_path = sweep_dir / "sweep_config.normalized.yaml"
    normalized = yaml.safe_load(normalized_path.read_text(encoding="utf-8"))
    normalized["execution"]["evaluator"] = "real_dynamic_v3_rescue"
    normalized["execution"]["evaluation_mode"] = "real_dynamic_v3_rescue"
    normalized["data"]["quality_status"] = "PASS"
    normalized_path.write_text(
        yaml.safe_dump(normalized, sort_keys=False),
        encoding="utf-8",
    )
    config = load_parameter_sweep_config(normalized_path)
    windows = dynamic_v3_research.walk_forward_windows(config)
    path_dates = pd.bdate_range(windows[0]["train_start"], windows[-1]["test_end"])
    results_path = sweep_dir / "candidate_results.jsonl"
    rows = [
        json.loads(line)
        for line in results_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    source_rows = list(rows)
    for index, row in enumerate(source_rows, start=1):
        candidate_id = row["candidate_id"]
        report_id = f"real_eval_{candidate_id}"
        real_dir = sweep_dir / "real_evaluation" / candidate_id
        real_dir.mkdir(parents=True)
        dynamic_return = 0.00015 + index / 1_000_000
        dynamic_path = [
            {
                "signal_date": day.date().isoformat(),
                "strategy_return": dynamic_return,
                "turnover": 0.0,
                "constraints_applied_json": "[]",
                "data_quality_status": "PASS",
                "selected_regime": "risk_on",
            }
            for day in path_dates
        ]
        static_path = [
            {
                "signal_date": day.date().isoformat(),
                "strategy_return": 0.0001,
                "turnover": 0.0,
                "data_quality_status": "PASS",
            }
            for day in path_dates
        ]
        real_path = real_dir / "real_evaluation.json"
        real_path.write_text(
            json.dumps(
                {
                    "dynamic_v3_real_evaluation_report_id": report_id,
                    "source_sweep_id": sweep_id,
                    "source_sweep_candidate_id": candidate_id,
                    "comparison_daily_paths": {
                        "dynamic_candidate": dynamic_path,
                        "static_base_candidate": static_path,
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        row["evaluator_mode"] = "real_dynamic_v3_rescue"
        row["evaluator_version"] = "real_dynamic_v3_rescue_v1"
        row["metrics_source"] = "real_evaluation_artifact"
        row["not_for_investment_decision"] = False
        row["real_evaluation_artifact_path"] = str(real_path)
        row["metrics"].update(
            {
                "real_evaluation_report_id": report_id,
                "lookahead_status": "PASS",
                "weight_path_status": "PARTIAL",
            }
        )
    results_path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    manifest_path = sweep_dir / "sweep_manifest.json"
    sweep_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sweep_manifest.update(
        {
            "profile": "real_test",
            "evaluator_mode": "real_dynamic_v3_rescue",
            "not_for_investment_decision": False,
        }
    )
    manifest_path.write_text(
        json.dumps(sweep_manifest, sort_keys=True),
        encoding="utf-8",
    )
    profile_path = tmp_path / "profiles.yaml"
    profile_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "profiles": {
                    "real_test": {
                        "description": "real path selection test",
                        "config_path": str(normalized_path),
                        "evaluator_mode": "real_dynamic_v3_rescue",
                        "max_candidates": max(1, len(source_rows)),
                        "workers": 1,
                        "ci_safe": False,
                        "not_for_investment_decision": False,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    selection = run_walk_forward_selection(
        config_path=normalized_path,
        profile="real_test",
        sweep_id=sweep_id,
        profile_config_path=profile_path,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "walk_forward_selection",
    )
    assert selection["report"]["status"] == "REVIEW_REQUIRED"
    assert selection["report"]["selection_status"] == "REVIEW_REQUIRED"
    assert selection["report"]["evidence_method"] == "real_daily_path_window_v1"
    assert selection["report"]["evidence_completeness"] == "PATH_DERIVED_PARTIAL"
    train_rows = [
        json.loads(line)
        for line in (
            selection["wf_selection_dir"] / "train_window_leaderboards.jsonl"
        ).read_text(encoding="utf-8").splitlines()
    ]
    assert train_rows[0]["leaderboard"]
    assert len(train_rows[0]["leaderboard"]) == len(source_rows)
    assert all(
        row["train_metrics"]["row_count"] > 0 for row in train_rows[0]["leaderboard"]
    )
    assert all(
        row["train_metrics"]["evidence_method"] == "real_daily_path_window_v1"
        for row in train_rows[0]["leaderboard"]
    )
    assert (
        validate_walk_forward_selection_artifact(
            wf_selection_id=selection["wf_selection_id"],
            output_dir=tmp_path / "walk_forward_selection",
        )["status"]
        == "PASS"
    )

    selected_candidate = selection["report"]["summary"]["selected_candidate_count"]
    assert selected_candidate == len(windows)
    candidate_id = train_rows[0]["leaderboard"][0]["candidate_id"]
    overfit = run_overfit_review(
        sweep_id=sweep_id,
        candidate_id=candidate_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=tmp_path / "overfit",
    )
    assert overfit["report"]["evidence_completeness"] == "PATH_DERIVED_PARTIAL"
    assert overfit["report"]["overfit_status"] == "REVIEW_REQUIRED"
    assert (
        validate_overfit_artifact(
            overfit_id=overfit["overfit_id"],
            output_dir=tmp_path / "overfit",
        )["status"]
        == "PASS"
    )


def test_dynamic_v3_data_and_injection_audit_contracts(
    tmp_path: Path,
    real_smoke_cache: tuple[Path, Path, str],
) -> None:
    prices_path, rates_path, as_of = real_smoke_cache
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
        max_candidates=1,
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=tmp_path / "injection_audit",
    )
    assert injection["report"]["candidate_count"] == 1
    assert injection["report"]["status"] == "INCOMPLETE"
    assert injection["report"]["parameter_effect_pair_coverage_complete"] is False
    assert (injection["audit_dir"] / "parameter_effect_summary.json").exists()
    assert (
        validate_injection_audit_artifact(
            audit_id=injection["audit_id"],
            output_dir=tmp_path / "injection_audit",
        )["status"]
        == "FAIL"
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
    outcome_dashboard_dir = tmp_path / "outcome_dashboard" / "dashboard123"
    outcome_dashboard_dir.mkdir(parents=True)
    outcome_dashboard_path = outcome_dashboard_dir / "outcome_dashboard_manifest.json"
    outcome_dashboard_path.write_text(
        json.dumps(
            {
                "dashboard_id": "dashboard123",
                "status": "PASS_WITH_WARNINGS",
                "available_count": 11,
                "pending_count": 34,
                "insufficient_data_count": 1,
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (outcome_dashboard_dir / "outcome_availability_matrix.json").write_text(
        json.dumps({"summary": {}}),
        encoding="utf-8",
    )
    (outcome_dashboard_dir / "pending_reason_dashboard.json").write_text(
        json.dumps(
            {
                "top_pending_reasons": [{"reason": "future_window_not_reached", "count": 22}],
                "next_action": "continue_forward_tracking",
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    update_review_dir = tmp_path / "outcome_update_review" / "review123"
    update_review_dir.mkdir(parents=True)
    update_review_path = update_review_dir / "outcome_update_review_manifest.json"
    update_review_path.write_text(
        json.dumps(
            {
                "update_review_id": "review123",
                "status": "PASS",
                "ready_to_update_count": 1,
                "blocked_count": 0,
                "future_data_used_in_decision": False,
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (update_review_dir / "update_safety_checks.json").write_text(
        json.dumps({"production_effect": "none", "broker_action_taken": False}),
        encoding="utf-8",
    )
    (update_review_dir / "update_impact_preview.json").write_text(
        json.dumps({"expected_forward_available_delta": 1, "production_effect": "none"}),
        encoding="utf-8",
    )
    update_dir = tmp_path / "outcome_update" / "update123"
    update_dir.mkdir(parents=True)
    update_path = update_dir / "outcome_update_manifest.json"
    update_path.write_text(
        json.dumps(
            {
                "outcome_update_id": "update123",
                "status": "PASS",
                "updated_count": 1,
                "skipped_count": 3,
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (update_dir / "outcome_status_delta.json").write_text(
        json.dumps(
            {
                "before": {"forward_available": 0, "forward_pending": 4},
                "after": {"forward_available": 1, "forward_pending": 3},
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    refresh_dir = tmp_path / "rolling_evidence_refresh" / "refresh123"
    refresh_dir.mkdir(parents=True)
    refresh_path = refresh_dir / "rolling_refresh_manifest.json"
    refresh_path.write_text(
        json.dumps(
            {
                "refresh_id": "refresh123",
                "status": "PASS",
                "material_change": False,
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (refresh_dir / "evidence_delta_summary.json").write_text(
        json.dumps(
            {
                "before": {
                    "limited_vs_notrade_available_count": 2,
                    "consensus_target_risk": "INSUFFICIENT_DATA",
                },
                "after": {
                    "limited_vs_notrade_available_count": 3,
                    "consensus_target_risk": "INSUFFICIENT_DATA",
                },
                "material_change": False,
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    (refresh_dir / "refreshed_artifacts.json").write_text(
        json.dumps({"weekly_advisory_review_id": "weekly123", "production_effect": "none"}),
        encoding="utf-8",
    )
    trend_dir = tmp_path / "evidence_trend" / "trend123"
    trend_dir.mkdir(parents=True)
    trend_path = trend_dir / "evidence_trend_manifest.json"
    trend_path.write_text(
        json.dumps(
            {
                "trend_id": "trend123",
                "status": "PASS",
                "trend_status": "INSUFFICIENT_HISTORY",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (trend_dir / "confidence_trend_summary.json").write_text(
        json.dumps(
            {
                "trend_status": "INSUFFICIENT_HISTORY",
                "confidence_change": "NO_CHANGE",
                "next_action": "continue_tracking",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    decision_dir = tmp_path / "forward_outcome_decision" / "decision123"
    decision_dir.mkdir(parents=True)
    decision_path = decision_dir / "forward_decision_manifest.json"
    decision_path.write_text(
        json.dumps(
            {
                "decision_id": "decision123",
                "status": "PASS",
                "recommended_action": "continue_tracking",
                "rule_calibration_readiness": "NOT_READY",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (decision_dir / "forward_go_no_go_matrix.json").write_text(
        json.dumps(
            {
                "recommended_action": "continue_tracking",
                "rule_calibration_readiness": "NOT_READY",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    (decision_dir / "forward_next_actions.json").write_text(
        json.dumps(
            {
                "next_actions": [{"action": "run_next_due_scan", "target_date": "2026-06-21"}],
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    ledger_dir = tmp_path / "candidate_decision_ledger" / "ledger123"
    ledger_dir.mkdir(parents=True)
    ledger_path = ledger_dir / "candidate_decision_ledger_manifest.json"
    ledger_path.write_text(
        json.dumps(
            {
                "ledger_run_id": "ledger123",
                "record_id": "record123",
                "status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "candidate_decision_record.json").write_text(
        json.dumps(
            {
                "record_id": "record123",
                "candidate": "median_plus_regime_mismatch_filter",
                "evidence_status": "PROMISING",
                "stress_result": "STRONG",
                "mismatch_result": "IMPROVED",
                "rotation_result": "IMPROVED",
                "ab_result": "PROMISING",
                "confirmation_count": 3,
                "owner_action": "formalize_research_method",
                "final_decision": "FORMALIZE_RESEARCH_METHOD",
                "next_required_action": "start_daily_paper_shadow_runner_design",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (ledger_dir / "candidate_decision_ledger_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    threshold_dir = tmp_path / "promotion_gate_threshold_calibration" / "threshold123"
    threshold_dir.mkdir(parents=True)
    threshold_path = threshold_dir / "promotion_gate_threshold_calibration_manifest.json"
    threshold_path.write_text(
        json.dumps(
            {
                "calibration_id": "threshold123",
                "status": "PASS",
                "policy_id": "research_promotion_gate_thresholds_v1",
                "policy_version": "2026-06-15",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (threshold_dir / "promotion_gate_threshold_calibration_report.json").write_text(
        json.dumps(
            {
                "calibration_id": "threshold123",
                "status": "PASS",
                "policy_id": "research_promotion_gate_thresholds_v1",
                "policy_version": "2026-06-15",
                "current_threshold_interpretation": (
                    "FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS"
                ),
                "stress_required": "STRONG",
                "confirmation_target_minimum": 3,
                "next_required_action": "continue_with_formal_research_governance_only",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
            }
        ),
        encoding="utf-8",
    )
    (threshold_dir / "promotion_gate_threshold_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    daily_dir = tmp_path / "paper_shadow_daily" / "daily123"
    daily_dir.mkdir(parents=True)
    daily_path = daily_dir / "paper_shadow_daily_manifest.json"
    daily_path.write_text(
        json.dumps(
            {
                "observation_id": "daily123",
                "candidate": "median_plus_regime_mismatch_filter",
                "observation_date": "2026-06-12",
                "status": "PASS",
                "observation_status": "RECORDED",
                "production_effect": "none",
                "broker_action": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (daily_dir / "paper_shadow_daily_observation.json").write_text(
        json.dumps(
            {
                "observation_id": "daily123",
                "candidate": "median_plus_regime_mismatch_filter",
                "observation_date": "2026-06-12",
                "observation_status": "RECORDED",
                "daily_review": {
                    "signal_output": "OBSERVE_RISK_ON",
                    "risk_off_risk_on_state": "risk_on",
                },
                "next_required_action": "continue_daily_paper_shadow_observation",
                "manual_review_only": True,
                "observation_only": True,
                "hypothetical_weight_paper_shadow_only": True,
                "not_official_target_weights": True,
                "production_effect": "none",
                "broker_action": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "production_state_mutated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    (daily_dir / "paper_shadow_daily_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    drift_dir = tmp_path / "paper_shadow_drift_monitor" / "drift123"
    drift_dir.mkdir(parents=True)
    drift_path = drift_dir / "paper_shadow_drift_manifest.json"
    drift_path.write_text(
        json.dumps(
            {
                "monitor_id": "drift123",
                "candidate": "median_plus_regime_mismatch_filter",
                "observation_id": "daily123",
                "status": "PASS",
                "drift_severity": "NONE",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (drift_dir / "paper_shadow_drift_report.json").write_text(
        json.dumps(
            {
                "monitor_id": "drift123",
                "candidate": "median_plus_regime_mismatch_filter",
                "observation_id": "daily123",
                "drift_severity": "NONE",
                "blocking_count": 0,
                "warning_count": 0,
                "next_action": "continue_shadow",
                "manual_review_only": True,
                "read_only_monitor": True,
                "not_official_target_weights": True,
                "production_effect": "none",
                "broker_action": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "production_state_mutated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    (drift_dir / "paper_shadow_drift_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    staleness_dir = tmp_path / "evidence_staleness_monitor" / "stale123"
    staleness_dir.mkdir(parents=True)
    staleness_path = staleness_dir / "evidence_staleness_manifest.json"
    staleness_path.write_text(
        json.dumps(
            {
                "monitor_id": "stale123",
                "status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (staleness_dir / "evidence_staleness_report.json").write_text(
        json.dumps(
            {
                "requested_as_of": "2026-06-16",
                "freshness_reference_date": "2026-06-12",
                "latest_complete_market_date": "2026-06-12",
                "market_calendar_status": "TRADING_DAY",
                "evidence_freshness_status": "ACCEPTABLE",
                "coverage_status": "MANUAL_REVIEW_REQUIRED",
                "weekly_review_coverage_classification": "RECOVERY_MODE_REVIEW",
                "weekly_review_coverage_safe_for_continuation": False,
                "stale_artifacts": [],
                "blocking_artifacts": [],
                "missing_artifacts": [],
                "next_refresh_action": "continue_with_manual_freshness_note",
                "safe_to_continue_shadow": True,
                "safety_boundary_status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (staleness_dir / "evidence_staleness_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    shadow_continuation_dir = tmp_path / "shadow_continuation_readiness" / "continue123"
    shadow_continuation_dir.mkdir(parents=True)
    shadow_continuation_path = (
        shadow_continuation_dir / "shadow_continuation_readiness_manifest.json"
    )
    shadow_continuation_path.write_text(
        json.dumps(
            {
                "readiness_id": "continue123",
                "shadow_continuation_readiness": "MANUAL_REVIEW_REQUIRED",
                "safe_to_continue_shadow": False,
                "status": "MANUAL_REVIEW_REQUIRED",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (shadow_continuation_dir / "shadow_continuation_readiness_report.json").write_text(
        json.dumps(
            {
                "readiness_id": "continue123",
                "shadow_continuation_readiness": "MANUAL_REVIEW_REQUIRED",
                "safe_to_continue_shadow": False,
                "missing_artifacts": [],
                "blocking_artifacts": [],
                "stale_artifacts": [],
                "coverage_status": "MANUAL_REVIEW_REQUIRED",
                "manual_review_required": True,
                "next_required_action": (
                    "complete_full_weekly_review_or_record_manual_coverage_override"
                ),
                "data_validation_status": "PASS_WITH_WARNINGS",
                "safety_boundary_status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
            }
        ),
        encoding="utf-8",
    )
    (shadow_continuation_dir / "shadow_continuation_readiness_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    stress_dir = tmp_path / "stress_scenario_library" / "stress123"
    stress_dir.mkdir(parents=True)
    stress_path = stress_dir / "stress_scenario_manifest.json"
    stress_path.write_text(
        json.dumps(
            {
                "library_run_id": "stress123",
                "stress_scenario_library_id": "dynamic_v3_rescue_stress_scenario_library_v1",
                "status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (stress_dir / "stress_scenario_library.json").write_text(
        json.dumps(
            {
                "library_run_id": "stress123",
                "stress_scenario_library_id": "dynamic_v3_rescue_stress_scenario_library_v1",
                "scenario_count": 9,
                "required_scenarios_present": True,
                "candidate_validation_use": ("standardized_dynamic_v3_candidate_stress_validation"),
                "next_validation_action": (
                    "use_library_ids_in_next_stress_backfill_or_case_review"
                ),
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (stress_dir / "stress_scenario_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    drawdown_dir = tmp_path / "drawdown_event_casebook" / "casebook123"
    drawdown_dir.mkdir(parents=True)
    drawdown_path = drawdown_dir / "drawdown_casebook_manifest.json"
    drawdown_path.write_text(
        json.dumps(
            {
                "casebook_run_id": "casebook123",
                "drawdown_casebook_id": "dynamic_v3_rescue_drawdown_event_casebook_v1",
                "status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (drawdown_dir / "drawdown_event_casebook.json").write_text(
        json.dumps(
            {
                "casebook_run_id": "casebook123",
                "drawdown_casebook_id": "dynamic_v3_rescue_drawdown_event_casebook_v1",
                "event_count": 5,
                "worst_event": "semiconductor_pullback_2024_07",
                "regime_coverage": [
                    "risk_off",
                    "semiconductor_pullback",
                    "sideways_choppy",
                    "strong_recovery",
                    "tech_drawdown",
                ],
                "next_review_action": "use_casebook_in_next_drawdown_mismatch_review",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    (drawdown_dir / "drawdown_event_casebook_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    flip_dir = tmp_path / "flip_rotation_event_casebook" / "flip123"
    flip_dir.mkdir(parents=True)
    flip_path = flip_dir / "flip_rotation_casebook_manifest.json"
    flip_path.write_text(
        json.dumps(
            {
                "casebook_run_id": "flip123",
                "flip_rotation_casebook_id": ("dynamic_v3_rescue_flip_rotation_event_casebook_v1"),
                "status": "PASS",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
            }
        ),
        encoding="utf-8",
    )
    (flip_dir / "flip_rotation_event_casebook.json").write_text(
        json.dumps(
            {
                "casebook_run_id": "flip123",
                "flip_rotation_casebook_id": ("dynamic_v3_rescue_flip_rotation_event_casebook_v1"),
                "event_count": 5,
                "useful_flip_count": 3,
                "false_positive_count": 2,
                "dominant_trigger_signal": "high_volatility_sideways_signal",
                "next_review_action": "use_casebook_in_next_flip_rotation_review",
                "production_effect": "none",
                "broker_action_taken": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    (flip_dir / "flip_rotation_event_casebook_validation.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "failed_check_count": 0,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        ),
        encoding="utf-8",
    )
    report_index = {
        "reports": [
            _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
            _report_record("etf_dynamic_v3_promotion_pack", evidence_path),
            _report_record("etf_dynamic_v3_outcome_dashboard", outcome_dashboard_path),
            _report_record("etf_dynamic_v3_outcome_update_review", update_review_path),
            _report_record("etf_dynamic_v3_outcome_update", update_path),
            _report_record("etf_dynamic_v3_rolling_evidence_refresh", refresh_path),
            _report_record("etf_dynamic_v3_evidence_trend", trend_path),
            _report_record("etf_dynamic_v3_forward_outcome_decision", decision_path),
            _report_record(
                "etf_dynamic_v3_promotion_gate_threshold_calibration",
                threshold_path,
            ),
            _report_record("etf_dynamic_v3_paper_shadow_daily", daily_path),
            _report_record("etf_dynamic_v3_paper_shadow_drift_monitor", drift_path),
            _report_record("etf_dynamic_v3_candidate_decision_ledger", ledger_path),
            _report_record("etf_dynamic_v3_evidence_staleness_monitor", staleness_path),
            _report_record(
                "etf_dynamic_v3_shadow_continuation_readiness",
                shadow_continuation_path,
            ),
            _report_record("etf_dynamic_v3_stress_scenario_library", stress_path),
            _report_record("etf_dynamic_v3_drawdown_event_casebook", drawdown_path),
            _report_record("etf_dynamic_v3_flip_rotation_event_casebook", flip_path),
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
    assert summary["outcome_dashboard_status"] == "PASS_WITH_WARNINGS"
    assert summary["outcome_dashboard_available_count"] == 11
    assert summary["outcome_dashboard_pending_count"] == 34
    assert summary["outcome_dashboard_insufficient_count"] == 1
    assert summary["outcome_dashboard_top_pending_reason"] == "future_window_not_reached"
    assert summary["outcome_dashboard_next_action"] == "continue_forward_tracking"
    assert summary["outcome_dashboard"] == str(outcome_dashboard_path)
    assert summary["outcome_update_review_status"] == "PASS"
    assert summary["outcome_update_review_ready_count"] == 1
    assert summary["outcome_update_review_future_data_used"] is False
    assert summary["outcome_update_status"] == "PASS"
    assert summary["outcome_update_updated_count"] == 1
    assert summary["outcome_update_skipped_count"] == 3
    assert summary["outcome_update_forward_available_after"] == 1
    assert summary["rolling_evidence_refresh_status"] == "PASS"
    assert summary["rolling_limited_vs_notrade_count_before"] == 2
    assert summary["rolling_limited_vs_notrade_count_after"] == 3
    assert summary["candidate_decision_ledger_id"] == "ledger123"
    assert summary["candidate_decision_record_id"] == "record123"
    assert summary["candidate_decision_candidate"] == "median_plus_regime_mismatch_filter"
    assert summary["candidate_decision_evidence_status"] == "PROMISING"
    assert summary["candidate_decision_stress_result"] == "STRONG"
    assert summary["candidate_decision_mismatch_result"] == "IMPROVED"
    assert summary["candidate_decision_rotation_result"] == "IMPROVED"
    assert summary["candidate_decision_ab_result"] == "PROMISING"
    assert summary["candidate_decision_confirmation_count"] == 3
    assert summary["candidate_decision_owner_action"] == "formalize_research_method"
    assert summary["candidate_decision_final_decision"] == "FORMALIZE_RESEARCH_METHOD"
    assert summary["candidate_decision_next_action"] == "start_daily_paper_shadow_runner_design"
    assert summary["candidate_decision_ledger_validation_status"] == "PASS"
    assert summary["promotion_threshold_calibration_id"] == "threshold123"
    assert summary["promotion_threshold_policy_id"] == "research_promotion_gate_thresholds_v1"
    assert summary["promotion_threshold_policy_version"] == "2026-06-15"
    assert summary["promotion_threshold_status"] == "PASS"
    assert (
        summary["promotion_threshold_current_interpretation"]
        == "FORMAL_RESEARCH_READY_UNDER_PILOT_THRESHOLDS"
    )
    assert summary["promotion_threshold_stress_required"] == "STRONG"
    assert summary["promotion_threshold_confirmation_minimum"] == 3
    assert summary["promotion_threshold_validation_status"] == "PASS"
    assert (
        summary["promotion_threshold_next_action"]
        == "continue_with_formal_research_governance_only"
    )
    assert summary["paper_shadow_daily_observation_id"] == "daily123"
    assert summary["paper_shadow_daily_candidate"] == "median_plus_regime_mismatch_filter"
    assert summary["paper_shadow_daily_date"] == "2026-06-12"
    assert summary["paper_shadow_daily_status"] == "RECORDED"
    assert summary["paper_shadow_daily_signal_output"] == "OBSERVE_RISK_ON"
    assert summary["paper_shadow_daily_risk_state"] == "risk_on"
    assert summary["paper_shadow_daily_next_action"] == "continue_daily_paper_shadow_observation"
    assert summary["paper_shadow_daily_validation_status"] == "PASS"
    assert summary["paper_shadow_daily"] == str(daily_path)
    assert summary["paper_shadow_drift_monitor_id"] == "drift123"
    assert summary["paper_shadow_drift_candidate"] == "median_plus_regime_mismatch_filter"
    assert summary["paper_shadow_drift_observation_id"] == "daily123"
    assert summary["paper_shadow_drift_severity"] == "NONE"
    assert summary["paper_shadow_drift_blocking_count"] == 0
    assert summary["paper_shadow_drift_warning_count"] == 0
    assert summary["paper_shadow_drift_next_action"] == "continue_shadow"
    assert summary["paper_shadow_drift_validation_status"] == "PASS"
    assert summary["paper_shadow_drift_monitor"] == str(drift_path)
    assert summary["evidence_staleness_monitor_id"] == "stale123"
    assert summary["evidence_freshness_status"] == "ACCEPTABLE"
    assert summary["evidence_coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert summary["evidence_weekly_review_coverage_classification"] == "RECOVERY_MODE_REVIEW"
    assert summary["evidence_weekly_review_coverage_safe_for_continuation"] is False
    assert summary["evidence_requested_as_of"] == "2026-06-16"
    assert summary["evidence_freshness_reference_date"] == "2026-06-12"
    assert summary["evidence_latest_complete_market_date"] == "2026-06-12"
    assert summary["evidence_market_calendar_status"] == "TRADING_DAY"
    assert summary["evidence_stale_artifacts"] == "none"
    assert summary["evidence_blocking_artifacts"] == "none"
    assert summary["evidence_missing_artifacts"] == "none"
    assert summary["evidence_next_refresh_action"] == "continue_with_manual_freshness_note"
    assert summary["evidence_safe_to_continue_shadow"] is True
    assert summary["evidence_safety_boundary_status"] == "PASS"
    assert summary["evidence_staleness_validation_status"] == "PASS"
    assert summary["shadow_continuation_readiness_id"] == "continue123"
    assert summary["shadow_continuation_readiness"] == "MANUAL_REVIEW_REQUIRED"
    assert summary["shadow_continuation_safe_to_continue_shadow"] is False
    assert summary["shadow_continuation_missing_artifacts"] == "none"
    assert summary["shadow_continuation_blocking_artifacts"] == "none"
    assert summary["shadow_continuation_stale_artifacts"] == "none"
    assert summary["shadow_continuation_coverage_status"] == "MANUAL_REVIEW_REQUIRED"
    assert summary["shadow_continuation_manual_review_required"] is True
    assert (
        summary["shadow_continuation_next_required_action"]
        == "complete_full_weekly_review_or_record_manual_coverage_override"
    )
    assert summary["shadow_continuation_data_validation_status"] == "PASS_WITH_WARNINGS"
    assert summary["shadow_continuation_safety_boundary_status"] == "PASS"
    assert summary["shadow_continuation_validation_status"] == "PASS"
    assert summary["stress_scenario_library_run_id"] == "stress123"
    assert summary["stress_scenario_library_id"] == "dynamic_v3_rescue_stress_scenario_library_v1"
    assert summary["stress_scenario_count"] == 9
    assert summary["stress_scenario_required_present"] is True
    assert (
        summary["stress_scenario_candidate_validation_use"]
        == "standardized_dynamic_v3_candidate_stress_validation"
    )
    assert (
        summary["stress_scenario_next_action"]
        == "use_library_ids_in_next_stress_backfill_or_case_review"
    )
    assert summary["stress_scenario_validation_status"] == "PASS"
    assert summary["drawdown_casebook_run_id"] == "casebook123"
    assert summary["drawdown_casebook_id"] == "dynamic_v3_rescue_drawdown_event_casebook_v1"
    assert summary["drawdown_casebook_event_count"] == 5
    assert summary["drawdown_casebook_worst_event"] == "semiconductor_pullback_2024_07"
    assert summary["drawdown_casebook_regime_coverage"] == (
        "risk_off, semiconductor_pullback, sideways_choppy, strong_recovery, tech_drawdown"
    )
    assert (
        summary["drawdown_casebook_next_action"] == "use_casebook_in_next_drawdown_mismatch_review"
    )
    assert summary["drawdown_casebook_validation_status"] == "PASS"
    assert summary["flip_rotation_casebook_run_id"] == "flip123"
    assert (
        summary["flip_rotation_casebook_id"] == "dynamic_v3_rescue_flip_rotation_event_casebook_v1"
    )
    assert summary["flip_rotation_casebook_event_count"] == 5
    assert summary["flip_rotation_useful_count"] == 3
    assert summary["flip_rotation_false_positive_count"] == 2
    assert summary["flip_rotation_dominant_trigger"] == "high_volatility_sideways_signal"
    assert summary["flip_rotation_next_action"] == "use_casebook_in_next_flip_rotation_review"
    assert summary["flip_rotation_casebook_validation_status"] == "PASS"
    assert summary["rolling_consensus_risk_after"] == "INSUFFICIENT_DATA"
    assert summary["rolling_weekly_advisory_review_id"] == "weekly123"
    assert summary["evidence_trend_status"] == "INSUFFICIENT_HISTORY"
    assert summary["evidence_trend_confidence_change"] == "NO_CHANGE"
    assert summary["forward_outcome_decision_action"] == "continue_tracking"
    assert summary["forward_rule_calibration_readiness"] == "NOT_READY"
    assert summary["forward_next_due_scan_date"] == "2026-06-21"
    assert summary["forward_outcome_decision"] == str(decision_path)
    assert summary["production_candidate_generated"] is False


def _write_weight_path_fixture(root: Path, *, evaluation_id: str) -> Path:
    candidate_id = "candidate_fixture"
    artifact_dir = root / "evaluations" / evaluation_id
    artifact_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "date": "2023-01-03",
                "symbol": "QQQ",
                "weight": 0.6,
                "candidate_id": candidate_id,
            },
            {
                "date": "2023-01-03",
                "symbol": "SMH",
                "weight": 0.4,
                "candidate_id": candidate_id,
            },
        ]
    ).to_csv(artifact_dir / "daily_weights.csv", index=False)
    for name in ("rebalance_events", "constraint_events", "rescue_events"):
        (artifact_dir / f"{name}.json").write_text(
            json.dumps({"events": []}),
            encoding="utf-8",
        )
    pd.DataFrame(
        [
            {
                "date": "2023-01-03",
                "candidate_id": candidate_id,
                "turnover": 0.2,
            }
        ]
    ).to_csv(artifact_dir / "turnover_by_rebalance.csv", index=False)
    metadata = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_weight_path_metadata",
        "candidate_id": candidate_id,
        "evaluation_id": evaluation_id,
        "weight_path_start": "2023-01-03",
        "weight_path_end": "2023-01-03",
        "daily_weight_count": 2,
        "symbol_count": 2,
        "has_daily_weights": True,
        "has_rebalance_events": True,
        "has_constraint_events": True,
        "has_rescue_events": True,
        "has_turnover_by_rebalance": True,
        "weight_path_detail_level": "minimal",
        "attribution_completeness": "PARTIAL",
        "missing_fields": ["pre_constraint_weight"],
    }
    (artifact_dir / "weight_path_metadata.json").write_text(
        json.dumps(metadata),
        encoding="utf-8",
    )
    return artifact_dir


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
    dates = pd.bdate_range("2022-01-03", periods=360)
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


def _write_latest_pointer(
    pointer_dir: Path,
    name: str,
    artifact_id: str,
    target: Path,
    *,
    now: datetime | None = None,
) -> None:
    pointer_dir.mkdir(parents=True, exist_ok=True)
    (pointer_dir / f"{name}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifact_type": name.removeprefix("latest_"),
                "artifact_id": artifact_id,
                "path": str(target),
                "updated_at": (now or datetime.now(UTC)).isoformat(),
                "exists": target.exists(),
            }
        ),
        encoding="utf-8",
    )


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
