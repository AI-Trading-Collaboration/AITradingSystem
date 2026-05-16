from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.shadow_iteration import (
    build_shadow_iteration_report,
    write_shadow_iteration_outputs,
)


def test_shadow_iteration_writes_registry_report_and_dashboard_json(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(tmp_path)
    registry_path = tmp_path / "shadow_iteration_registry.csv"
    reports_dir = tmp_path / "reports"
    run_root = tmp_path / "shadow_iterations"

    report = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=reports_dir,
        run_output_root=run_root,
        contract_path=contract_path,
    )
    paths = write_shadow_iteration_outputs(report)

    registry = pd.read_csv(registry_path)
    assert set(registry["candidate_type"]) == {
        "weight_only",
        "gate_only",
        "weight_gate_bundle",
    }
    assert set(registry["production_effect"]) == {"none"}
    gate_row = registry.loc[registry["candidate_type"] == "gate_only"].iloc[0]
    bundle_row = registry.loc[registry["candidate_type"] == "weight_gate_bundle"].iloc[0]
    assert gate_row["promotion_status"] == "GATE_POLICY_REVIEW_ONLY"
    assert bundle_row["promotion_status"] == "DIAGNOSTIC_ONLY"

    dashboard = json.loads(paths["json_report"].read_text(encoding="utf-8"))
    assert dashboard["report_type"] == "shadow_iteration"
    assert dashboard["production_effect"] == "none"
    assert dashboard["summary"]["production_parameters_changed"] is False
    assert (
        dashboard["best_candidates"]["gate_only"][
            "is_potential_weight_iteration_candidate"
        ]
        is False
    )
    assert (
        dashboard["best_candidates"]["weight_gate_bundle"][
            "is_potential_weight_iteration_candidate"
        ]
        is False
    )
    assert dashboard["attribution"]["cap_level"]["status"] == "available"
    assert dashboard["attribution"]["position_change"]["status"] == "available"
    assert "do_not_enable_approved_hard" in dashboard["safety"]["p2_guardrails"]
    assert (
        "do_not_generate_shrinkage_production_proposal"
        in dashboard["safety"]["p2_guardrails"]
    )

    markdown = paths["markdown_report"].read_text(encoding="utf-8")
    assert "Production 参数未改变" in markdown
    assert "P2 Guardrails" in markdown
    assert "best weight-only candidate" in markdown
    assert "best gate-only candidate" in markdown
    assert "best weight-gate bundle candidate" in markdown
    assert paths["trial_cards"].exists()
    assert paths["lineage"].exists()


def test_shadow_iteration_command_does_not_modify_production_surfaces(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(tmp_path)
    protected_paths = [
        PROJECT_ROOT / "config" / "weights" / "weight_profile_current.yaml",
        PROJECT_ROOT / "config" / "scoring_rules.yaml",
        PROJECT_ROOT / "config" / "portfolio.yaml",
        PROJECT_ROOT / "data" / "processed" / "approved_calibration_overlay.json",
        PROJECT_ROOT / "data" / "processed" / "prediction_ledger.csv",
    ]
    before = {
        path: (sha256_file(path) if path.exists() else None)
        for path in protected_paths
    }

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "run-shadow-iteration",
            "--as-of",
            "2026-05-17",
            "--search-output-dir",
            str(search_output_dir),
            "--registry-path",
            str(tmp_path / "registry.csv"),
            "--reports-dir",
            str(tmp_path / "reports"),
            "--run-output-root",
            str(tmp_path / "shadow_iterations"),
            "--contract-path",
            str(contract_path),
        ],
    )

    assert result.exit_code == 0
    assert "Shadow iteration 状态" in result.output
    after = {
        path: (sha256_file(path) if path.exists() else None)
        for path in protected_paths
    }
    assert after == before


def test_register_forward_shadow_only_updates_registry_status(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(tmp_path)
    registry_path = tmp_path / "registry.csv"
    report = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    write_shadow_iteration_outputs(report)
    registry = pd.read_csv(registry_path)
    row = registry.loc[registry["candidate_type"] == "gate_only"].iloc[0]

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "register-forward-shadow",
            "--iteration-id",
            str(row["iteration_id"]),
            "--candidate-id",
            str(row["trial_id"]),
            "--as-of",
            "2026-05-18",
            "--registry-path",
            str(registry_path),
        ],
    )

    assert result.exit_code == 0, result.output
    updated = pd.read_csv(registry_path)
    updated_row = updated.loc[updated["iteration_id"] == row["iteration_id"]].iloc[0]
    assert updated_row["status"] == "FORWARD_SHADOW_ACTIVE"
    assert updated_row["production_effect"] == "none"
    assert "forward shadow 观察" in updated_row["next_action"]
    assert not (tmp_path / "config" / "weights" / "weight_profile_current.yaml").exists()
    assert not (tmp_path / "data" / "processed" / "prediction_ledger.csv").exists()


def test_shadow_iteration_promotion_contract_only_outputs_blocked_reasons(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(
        tmp_path,
        primary_driver="weight",
        available_count=8,
    )

    report = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=tmp_path / "registry.csv",
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    paths = write_shadow_iteration_outputs(report)
    dashboard = json.loads(paths["json_report"].read_text(encoding="utf-8"))

    assert dashboard["promotion_contract_check"]["status"] == "NOT_PROMOTABLE"
    assert any(
        "available_samples_below_contract_floor" in reason
        for reasons in dashboard["blocked_reasons"].values()
        for reason in reasons
    )
    assert not (tmp_path / "approved_calibration_overlay.json").exists()


def test_shadow_iteration_degrades_when_optional_attribution_is_missing(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(
        tmp_path,
        include_cap_attribution=False,
        include_position_changes=False,
    )

    report = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=tmp_path / "registry.csv",
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    paths = write_shadow_iteration_outputs(report)
    dashboard = json.loads(paths["json_report"].read_text(encoding="utf-8"))
    markdown = paths["markdown_report"].read_text(encoding="utf-8")

    assert dashboard["attribution"]["cap_level"]["status"] == "unavailable"
    assert dashboard["attribution"]["position_change"]["status"] == "unavailable"
    assert "cap-level attribution unavailable" in "\n".join(dashboard["warnings"])
    assert "position change data unavailable" in "\n".join(dashboard["warnings"])
    assert "unavailable" in markdown


def test_shadow_iteration_retires_when_primary_driver_flips_from_weight_to_gate(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(tmp_path)
    registry_path = tmp_path / "registry.csv"
    first = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    write_shadow_iteration_outputs(first)
    registry = pd.read_csv(registry_path)
    weight_mask = registry["candidate_type"] == "weight_only"
    registry.loc[weight_mask, "primary_driver"] = "weight"
    registry.to_csv(registry_path, index=False)

    second = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-18").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    write_shadow_iteration_outputs(second)
    updated = pd.read_csv(registry_path)
    row = updated.loc[updated["candidate_type"] == "weight_only"].iloc[0]

    assert row["status"] == "RETIRED"
    assert "primary_driver changed from weight to gate" in row["blocked_reasons_json"]


def test_shadow_iteration_retires_after_consecutive_missing_top_group(
    tmp_path: Path,
) -> None:
    search_output_dir, contract_path = _write_search_output(tmp_path)
    registry_path = tmp_path / "registry.csv"
    report = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-17").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    write_shadow_iteration_outputs(report)
    registry = pd.read_csv(registry_path)
    old_row = registry.iloc[0].copy()
    old_row["iteration_id"] = "old_search::missing_trial"
    old_row["trial_id"] = "missing_trial"
    old_row["status"] = "CANDIDATE"
    old_row["retirement_evidence_json"] = json.dumps(
        {"missing_top_group_count": 2},
        ensure_ascii=False,
    )
    registry = pd.concat([registry, pd.DataFrame([old_row])], ignore_index=True)
    registry.to_csv(registry_path, index=False)

    second = build_shadow_iteration_report(
        as_of=pd.Timestamp("2026-05-18").date(),
        search_output_dir=search_output_dir,
        registry_path=registry_path,
        reports_dir=tmp_path / "reports",
        run_output_root=tmp_path / "shadow_iterations",
        contract_path=contract_path,
    )
    write_shadow_iteration_outputs(second)
    updated = pd.read_csv(registry_path)
    retired = updated.loc[updated["iteration_id"] == "old_search::missing_trial"].iloc[0]

    assert retired["status"] == "RETIRED"
    assert "consecutive missing top group" in retired["blocked_reasons_json"]


def _write_search_output(
    tmp_path: Path,
    *,
    primary_driver: str = "gate",
    available_count: int = 40,
    include_cap_attribution: bool = True,
    include_position_changes: bool = True,
) -> tuple[Path, Path]:
    search_output_dir = tmp_path / "parameter_search" / "demo_search"
    search_output_dir.mkdir(parents=True)
    objective_path = tmp_path / "shadow_parameter_objective.yaml"
    search_space_path = tmp_path / "shadow_parameter_search_space.yaml"
    contract_path = tmp_path / "shadow_parameter_promotion_contract.yaml"
    objective_path.write_text(
        "version: objective_test\nstatus: validation\n",
        encoding="utf-8",
    )
    search_space_path.write_text(
        "version: search_space_test\nstatus: validation\n",
        encoding="utf-8",
    )
    contract_path.write_text(
        """
version: contract_test
status: validation
owner: system
production_effect: none
rationale: test contract
min_available_samples: 30
require_search_eligible_best: true
require_positive_excess: true
max_drawdown_degradation: 0.02
max_shadow_turnover: 1.50
gate_primary_driver_requires_cap_review: true
required_forward_shadow_available_samples: 30
owner_approval_required: true
rollback_condition_required: true
approved_hard_allowed: false
""".lstrip(),
        encoding="utf-8",
    )
    source_weights = {
        "trend": 0.25,
        "fundamentals": 0.25,
        "macro_liquidity": 0.15,
        "risk_sentiment": 0.15,
        "valuation": 0.10,
        "policy_geopolitics": 0.10,
    }
    changed_weights = {
        "trend": 0.30,
        "fundamentals": 0.25,
        "macro_liquidity": 0.15,
        "risk_sentiment": 0.15,
        "valuation": 0.05,
        "policy_geopolitics": 0.10,
    }
    rows = [
        _trial_row(
            "source_current__production_observed_gates",
            "source_current",
            "production_observed_gates",
            source_weights,
            {},
            available_count=available_count,
            objective_score=0.01,
            excess=0.0,
            weight_distance=0.0,
        ),
        _trial_row(
            "grid_weight_0001__production_observed_gates",
            "grid_weight_0001",
            "production_observed_gates",
            changed_weights,
            {},
            available_count=available_count,
            objective_score=0.20,
            excess=0.02,
            weight_distance=0.10,
        ),
        _trial_row(
            "source_current__grid_gate_0001",
            "source_current",
            "grid_gate_0001",
            source_weights,
            {"valuation": 0.70, "risk_budget": 0.70},
            available_count=available_count,
            objective_score=0.30,
            excess=0.03,
            weight_distance=0.0,
        ),
        _trial_row(
            "grid_weight_0001__grid_gate_0001",
            "grid_weight_0001",
            "grid_gate_0001",
            changed_weights,
            {"valuation": 0.70, "risk_budget": 0.70},
            available_count=available_count,
            objective_score=0.40,
            excess=0.04,
            weight_distance=0.10,
        ),
    ]
    pd.DataFrame(rows).to_csv(search_output_dir / "trials.csv", index=False)
    manifest = {
        "schema_version": 1,
        "report_type": "shadow_parameter_search",
        "production_effect": "none",
        "run_id": "demo_search",
        "generated_at": "2026-05-17T00:00:00+00:00",
        "search_window": {"start": "2026-05-01", "end": "2026-05-15"},
        "objective_path": str(objective_path),
        "search_space_path": str(search_space_path),
        "objective_checksum": sha256_file(objective_path),
        "search_space_checksum": sha256_file(search_space_path),
        "git_commit_sha": "abc123",
        "best_trial_id": "grid_weight_0001__grid_gate_0001",
        "best_diagnostic_trial_id": "grid_weight_0001__grid_gate_0001",
        "factorial_attribution": {
            "selected_trial_id": "grid_weight_0001__grid_gate_0001",
            "selected_trial_eligible": True,
            "primary_driver": primary_driver,
            "baseline_trial_id": "source_current__production_observed_gates",
            "weight_only_trial_id": "grid_weight_0001__production_observed_gates",
            "gate_only_trial_id": "source_current__grid_gate_0001",
            "combined_trial_id": "grid_weight_0001__grid_gate_0001",
            "weight_only_excess_delta": 0.02,
            "gate_only_excess_delta": 0.03,
            "combined_excess_delta": 0.04,
            "interaction_excess_delta": -0.01,
        },
        "warnings": [],
    }
    if include_cap_attribution:
        manifest["cap_attribution"] = [
            {
                "gate_id": "valuation",
                "selected_cap_value": 0.70,
                "cap_only_trial_id": "source_current__cap_only_valuation",
                "cap_only_excess_total_return": 0.024,
                "excess_delta_vs_baseline": 0.024,
                "cap_only_shadow_max_drawdown": -0.05,
                "cap_only_shadow_turnover": 0.8,
            },
            {
                "gate_id": "risk_budget",
                "selected_cap_value": 0.70,
                "cap_only_trial_id": "source_current__cap_only_risk_budget",
                "cap_only_excess_total_return": 0.006,
                "excess_delta_vs_baseline": 0.006,
                "cap_only_shadow_max_drawdown": -0.04,
                "cap_only_shadow_turnover": 0.4,
            },
        ]
    if include_position_changes:
        manifest["position_change_rows"] = [
            {
                "as_of": "2026-05-01",
                "production_position": 0.40,
                "candidate_position": 0.65,
                "position_delta": 0.25,
                "production_binding_gates": "valuation",
                "candidate_binding_gates": "valuation:40%->70%",
                "return_impact": 0.01,
            }
        ]
    (search_output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return search_output_dir, contract_path


def _trial_row(
    trial_id: str,
    weight_candidate_id: str,
    gate_candidate_id: str,
    weights: dict[str, float],
    gate_caps: dict[str, float],
    *,
    available_count: int,
    objective_score: float,
    excess: float,
    weight_distance: float,
) -> dict[str, object]:
    return {
        "trial_id": trial_id,
        "weight_candidate_id": weight_candidate_id,
        "weight_candidate_version": "v1",
        "gate_candidate_id": gate_candidate_id,
        "gate_candidate_version": "v1",
        "total_count": available_count,
        "available_count": available_count,
        "pending_count": 0,
        "missing_count": 0,
        "production_total_return": 0.05,
        "shadow_total_return": 0.05 + excess,
        "excess_total_return": excess,
        "production_max_drawdown": -0.05,
        "shadow_max_drawdown": -0.055,
        "production_turnover": 0.4,
        "shadow_turnover": 0.8,
        "shadow_beats_production_rate": 0.6,
        "weight_l1_distance_from_production": weight_distance,
        "max_single_factor_step": weight_distance,
        "gate_relaxation_distance": 0.2 if gate_caps else 0.0,
        "changed_dimension_count": len(gate_caps),
        "objective_score": objective_score,
        "eligible": available_count >= 30,
        "ineligibility_reason": ""
        if available_count >= 30
        else "available_samples_below_objective_floor",
        "target_weights_json": json.dumps(weights, sort_keys=True),
        "gate_cap_overrides_json": json.dumps(gate_caps, sort_keys=True),
    }
