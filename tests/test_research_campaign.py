from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.research_campaign import (
    DEFAULT_MODULE_REGISTRY_PATH,
    CampaignSpec,
    build_b2_campaign_e2e_compute_report,
    build_b2_campaign_full_parity_validation,
    build_b2_compute_adapter_smoke_report,
    build_b2_compute_parity_validation,
    build_b2_current_form_campaign_archive,
    build_b2_full_diagnostic_compute_report,
    build_b2_gate_compute_report,
    build_b3_signal_compute_adapter_smoke_report,
    build_campaign_b2_branch_finalization,
    build_campaign_b2_final_gate,
    build_campaign_b2_next_action_freeze,
    build_campaign_b2_owner_decision_record,
    build_campaign_b2_owner_review_packet,
    build_campaign_b2_reusable_evidence_report,
    build_campaign_evidence_budget_final_decision_drill,
    build_campaign_managed_b2_final_repeatability_run,
    build_campaign_run_next_stage_smoke_report,
    build_campaign_status_ux_report,
    build_campaign_validation_payload,
    build_case_specific_runner_deprecation_plan,
    build_evidence_budget_forced_transition_report,
    build_fast_shock_trigger_feasibility_rfc,
    build_legacy_b2_runner_deprecation_readiness,
    build_post_b2_campaign_program_snapshot,
    build_reentry_policy_design_contract,
    build_slow_drawdown_defensive_overlay_rfc,
    campaign_plan,
    classify_interaction_effect,
    evaluate_gate,
    initialize_campaign,
    load_campaign_bundle,
    load_campaign_spec,
    plan_experiment_matrix,
    run_campaign_stage,
    validate_stage_adapter_contracts,
    validate_stage_transition,
    write_campaign_control_plane_v1_validation_artifacts,
    write_campaign_state,
)

B2_SPEC = Path("docs/examples/research_campaigns/b2_risk_overlay_current_form.yaml")
B3_SPEC = Path("docs/examples/research_campaigns/b3_slow_tilt_signal_precheck.yaml")


def test_b2_and_b3_sample_specs_validate() -> None:
    for path in (B2_SPEC, B3_SPEC):
        spec = load_campaign_spec(path)
        payload = build_campaign_validation_payload(spec=spec)

        assert payload["validation_status"] == "PASS"
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["safety_boundary"]["official_target_weights"] is False


def test_stage_adapter_contract_validation_passes() -> None:
    payload = validate_stage_adapter_contracts()

    assert payload["validation_status"] == "PASS"
    assert {
        "b2-risk-overlay-audited-artifact-adapter-v1",
        "b2-risk-overlay-control-window-compute-adapter-v1",
        "b3-signal-precheck-audited-artifact-adapter-v1",
        "b3-signal-precheck-compute-adapter-v1",
    } <= set(payload["adapter_ids"])
    assert payload["adapter_run_modes"]["b2-risk-overlay-audited-artifact-adapter-v1"] == (
        "AUDITED_ARTIFACT_MODE"
    )
    assert payload["adapter_run_modes"]["b2-risk-overlay-control-window-compute-adapter-v1"] == (
        "COMPUTE_MODE"
    )
    assert "COMPUTE_MODE" in payload["run_modes"]
    assert "SIGNAL_PRECHECK" in payload["supported_stages"]
    assert payload["safety_boundary"]["official_target_weights"] is False


def test_b2_migration_represents_stage_outcome_reason_codes_and_next_actions(
    tmp_path: Path,
) -> None:
    init = initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)
    spec, state, evidence = load_campaign_bundle("b2-risk-overlay-current-form", tmp_path)
    plan = campaign_plan(campaign_id=spec.campaign_id, campaign_root=tmp_path)

    assert init["migrated_from_legacy"] is True
    assert state.current_stage == "TARGETED_EVIDENCE"
    assert state.current_outcome == "NEEDS_MORE_EVIDENCE"
    assert {
        "FAST_RISK_NOT_SUPPORTED",
        "SLOW_DRAWDOWN_SINGLE_WINDOW_ONLY",
        "REENTRY_LAG_SIGNAL_DRIVEN",
    } <= set(state.reason_codes)
    assert len(evidence) >= 6
    assert "COMPLETE_FINAL_REPEATABILITY_ROUND" in plan["allowed_next_actions"]
    assert "NARROW_ROLE" in plan["allowed_next_actions"]
    assert "RETURN_TO_DESIGN" in plan["allowed_next_actions"]
    assert "ACCESS_UNTOUCHED_HOLDOUT" in plan["blocked_actions"]


def test_b3_migration_stays_signal_only_and_does_not_require_portfolio_metrics(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B3_SPEC, campaign_root=tmp_path)
    spec, state, evidence = load_campaign_bundle("b3-slow-tilt-signal-precheck", tmp_path)
    gate = evaluate_gate(spec=spec, state=state, evidence=evidence)

    assert state.current_stage == "INPUT_PRECHECK"
    assert state.current_outcome == "MIXED"
    assert gate["decision_outcome"] == "MIXED"
    assert "PORTFOLIO_EFFECT" not in gate["required_evidence_categories"]
    assert state.safety_boundary["official_target_weights"] is False


def test_b2_stage_runner_uses_configured_compute_adapter(tmp_path: Path) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)

    payload = run_campaign_stage(
        campaign_id="b2-risk-overlay-current-form",
        requested_stage="TARGETED_EVIDENCE",
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )

    assert payload["outcome"] == "NEEDS_MORE_EVIDENCE"
    assert payload["generated_evidence_count"] == 6
    assert payload["adapter_status"] == "B2_TARGETED_EVIDENCE_COMPUTE_PASS"
    assert payload["result"]["adapter_id"] == ("b2-risk-overlay-control-window-compute-adapter-v1")
    assert payload["result"]["run_mode"] == "COMPUTE_MODE"
    assert payload["result"]["compute_performed"] is True
    assert payload["result"]["imported_evidence"] is False
    assert payload["result"]["stage"] == "TARGETED_EVIDENCE"
    assert Path(payload["result"]["json_path"]).exists()
    assert payload["safety_boundary"]["production_effect"] == "none"


def test_b3_signal_precheck_adapter_stays_signal_only_and_blocks_backfill(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B3_SPEC, campaign_root=tmp_path)

    payload = run_campaign_stage(
        campaign_id="b3-slow-tilt-signal-precheck",
        requested_stage="INPUT_PRECHECK",
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )
    plan = campaign_plan(campaign_id="b3-slow-tilt-signal-precheck", campaign_root=tmp_path)

    assert payload["outcome"] == "MIXED"
    assert payload["adapter_status"] == "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS"
    assert payload["result"]["adapter_id"] == "b3-signal-precheck-compute-adapter-v1"
    assert payload["result"]["run_mode"] == "COMPUTE_MODE"
    assert payload["result"]["stage"] == "SIGNAL_PRECHECK"
    assert payload["generated_evidence_count"] == 3
    assert "B3_MINI_BACKFILL" in plan["blocked_actions"]
    assert "CONTINUE_SIGNAL_DIRECTION_REDESIGN" in plan["allowed_next_actions"]


def test_campaign_control_plane_validation_pack_writes_expected_artifacts(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)
    initialize_campaign(spec_path=B3_SPEC, campaign_root=tmp_path)

    payload = write_campaign_control_plane_v1_validation_artifacts(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )

    assert payload["status"] == "RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS"
    expected = {
        "b2_campaign_adapter_parity_map",
        "b2_campaign_parity_validation",
        "evidence_budget_enforcement_report",
        "campaign_next_action_parity_review",
        "b2_targeted_evidence_compute_adapter",
        "b2_targeted_evidence_compute_parity",
        "b2_full_diagnostic_compute_adapter",
        "b2_gate_compute_adapter",
        "b2_campaign_e2e_compute",
        "b2_campaign_full_parity_validation",
        "campaign_run_next_stage_smoke",
        "evidence_budget_forced_transition_report",
        "campaign_evidence_budget_final_decision_drill",
        "b3_signal_compute_adapter_smoke",
        "campaign_status_plan_ux_report",
        "case_specific_runner_deprecation_plan",
        "legacy_b2_runner_deprecation_readiness",
        "campaign_b2_next_action_freeze",
        "campaign_managed_b2_final_repeatability_run",
        "campaign_b2_final_gate",
        "campaign_b2_owner_review_packet",
        "campaign_b2_branch_finalization",
        "campaign_b2_owner_decision_record",
        "b2_current_form_campaign_archive",
        "campaign_b2_reusable_evidence_report",
        "slow_drawdown_defensive_overlay_rfc",
        "reentry_policy_design_contract",
        "fast_shock_trigger_feasibility_rfc",
        "post_b2_campaign_program_snapshot",
        "campaign_control_plane_v1_validation_pack",
    }
    assert expected <= set(payload["artifacts"])
    for paths in payload["artifacts"].values():
        assert Path(paths["json_path"]).exists()
        assert Path(paths["markdown_path"]).exists()


def test_b2_compute_parity_and_run_next_smoke_pass(tmp_path: Path) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)

    smoke = build_b2_compute_adapter_smoke_report(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )
    parity = build_b2_compute_parity_validation(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
        smoke_report=smoke,
    )
    run_next = build_campaign_run_next_stage_smoke_report(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )

    assert smoke["status"] == "B2_TARGETED_EVIDENCE_COMPUTE_PASS"
    assert parity["status"] == "B2_TARGETED_EVIDENCE_COMPUTE_PARITY_PASS"
    assert run_next["status"] == "CAMPAIGN_RUN_NEXT_STAGE_SMOKE_PASS"
    assert run_next["run_result"]["adapter_status"] == "B2_TARGETED_EVIDENCE_COMPUTE_PASS"


def test_b2_full_gate_e2e_parity_budget_drill_and_legacy_readiness(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)

    output_root = tmp_path / "outputs"
    targeted = build_b2_compute_parity_validation(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    full = build_b2_full_diagnostic_compute_report(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    gate = build_b2_gate_compute_report(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    e2e = build_b2_campaign_e2e_compute_report(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    full_parity = build_b2_campaign_full_parity_validation(
        campaign_root=tmp_path,
        output_root=output_root,
        targeted_parity=targeted,
        full_compute=full,
        gate_compute=gate,
    )
    final_drill = build_campaign_evidence_budget_final_decision_drill(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    legacy = build_legacy_b2_runner_deprecation_readiness(full_parity=full_parity)

    assert full["status"] == "B2_FULL_DIAGNOSTIC_COMPUTE_PASS"
    assert full["adapter_run"]["status"] == "B2_FULL_DIAGNOSTIC_COMPLETE"
    assert gate["status"] == "B2_GATE_COMPUTE_PASS"
    assert gate["decision"] == "B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN"
    assert e2e["status"] == "B2_CAMPAIGN_E2E_COMPUTE_PASS_WITH_LIMITATIONS"
    assert e2e["stage_runs"][-1]["outcome"] == "OWNER_OVERRIDE_REQUIRED"
    assert full_parity["status"] == "B2_CAMPAIGN_FULL_PARITY_PASS_WITH_EXPLAINED_DIFFS"
    assert final_drill["status"] == "CAMPAIGN_EVIDENCE_BUDGET_FINAL_DECISION_PASS"
    assert final_drill["gate_run_result"]["outcome"] == "OWNER_OVERRIDE_REQUIRED"
    assert legacy["status"] == "LEGACY_B2_RUNNER_KEEP_COMPATIBILITY_LAYER"


def test_b2_finalization_freeze_repeatability_gate_packet_and_branch(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)

    output_root = tmp_path / "outputs"
    freeze = build_campaign_b2_next_action_freeze(campaign_root=tmp_path)
    repeatability = build_campaign_managed_b2_final_repeatability_run(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    final_gate = build_campaign_b2_final_gate(
        campaign_root=tmp_path,
        output_root=output_root,
    )
    packet = build_campaign_b2_owner_review_packet(
        campaign_root=tmp_path,
        output_root=output_root,
        final_gate=final_gate,
    )
    branch = build_campaign_b2_branch_finalization(
        campaign_root=tmp_path,
        output_root=output_root,
        final_gate=final_gate,
        owner_packet=packet,
    )

    assert freeze["status"] == "CAMPAIGN_B2_NEXT_ACTION_FREEZE_READY"
    assert {
        "COMPLETE_FINAL_REPEATABILITY_ROUND",
        "NARROW_ROLE",
        "RETURN_TO_DESIGN",
    } <= set(freeze["allowed_next_actions"])
    assert {"B4_RETEST", "B5", "B6", "V3", "PAPER_SHADOW"} <= set(freeze["blocked_actions"])
    assert repeatability["status"] == "B2_FINAL_REPEATABILITY_RUN_COMPLETE"
    assert repeatability["orchestration_entrypoint"] == "campaign run --stage next"
    assert repeatability["run_result"]["adapter_status"] == "B2_TARGETED_EVIDENCE_COMPUTE_PASS"
    assert repeatability["budget_consumed"] is True
    assert repeatability["tuning_review"]["parameter_tuning_applied"] is False
    assert final_gate["status"] == "OWNER_OVERRIDE_REQUIRED"
    assert final_gate["raw_campaign_adapter_decision"] == "OWNER_OVERRIDE_REQUIRED"
    assert packet["status"] == "B2_OWNER_REVIEW_PACKET_READY"
    assert packet["owner_decision_appended"] is False
    assert {
        "continue_narrow_b2_research",
        "return_b2_to_design",
        "reject_current_b2_form",
        "hold_for_owner_override",
    } <= {option["option_id"] for option in packet["owner_options"]}
    assert branch["status"] == "OWNER_REVIEW_REQUIRED"
    assert branch["B4_retest_allowed"] is False
    assert branch["B5_allowed"] is False
    assert branch["B6_allowed"] is False
    assert branch["v3_allowed"] is False
    assert branch["paper_shadow_allowed"] is False


def test_b2_owner_decision_archive_reusable_evidence_and_redesign_snapshot(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)
    initialize_campaign(spec_path=B3_SPEC, campaign_root=tmp_path)

    owner = build_campaign_b2_owner_decision_record(
        campaign_root=tmp_path,
        owner_action="return_to_design",
        decision_id="test-b2-owner-return-to-design",
        apply_to_campaign_state=True,
    )
    _, state, evidence = load_campaign_bundle("b2-risk-overlay-current-form", tmp_path)
    owner_decisions = [
        json.loads(line)
        for line in (tmp_path / "b2-risk-overlay-current-form" / "owner_decisions.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]

    assert owner["status"] == "B2_OWNER_DECISION_RECORDED"
    assert owner["owner_action"] == "return_to_design"
    assert owner["decision_outcome"] == "RETURNED_TO_DESIGN"
    assert owner["owner_decision_appended"] is True
    assert owner["campaign_state_updated"] is True
    assert all(value is False for value in owner["disallowed_owner_approvals"].values())
    assert state.current_stage == "ARCHIVED"
    assert state.current_outcome == "RETURNED_TO_DESIGN"
    assert "OWNER_DECISION_RETURN_TO_DESIGN" in state.reason_codes
    assert evidence
    assert len(owner_decisions) == 1

    archive = build_b2_current_form_campaign_archive(campaign_root=tmp_path)
    reusable = build_campaign_b2_reusable_evidence_report(campaign_root=tmp_path)
    slow_rfc = build_slow_drawdown_defensive_overlay_rfc(campaign_root=tmp_path)
    reentry = build_reentry_policy_design_contract(campaign_root=tmp_path)
    fast_rfc = build_fast_shock_trigger_feasibility_rfc(campaign_root=tmp_path)
    snapshot = build_post_b2_campaign_program_snapshot(
        campaign_root=tmp_path,
        owner_decision=owner,
        archive=archive,
        reusable_evidence=reusable,
        slow_drawdown_rfc=slow_rfc,
        reentry_contract=reentry,
        fast_shock_rfc=fast_rfc,
    )
    plan = campaign_plan(campaign_id="b2-risk-overlay-current-form", campaign_root=tmp_path)

    assert archive["status"] == "B2_CURRENT_FORM_RETURNED_TO_DESIGN"
    assert archive["evidence_record_count"] == len(evidence)
    assert archive["B4_retest_allowed"] is False
    assert archive["B5_allowed"] is False
    assert archive["B6_allowed"] is False
    assert archive["v3_allowed"] is False
    assert archive["paper_shadow_allowed"] is False
    assert reusable["status"] == "B2_REUSABLE_EVIDENCE_REPORT_READY"
    assert reusable["invalidated_evidence"][0]["evidence_id"] == "fast_risk_current_form"
    assert slow_rfc["status"] == "SLOW_DRAWDOWN_OVERLAY_RFC_READY"
    assert reentry["status"] == "REENTRY_POLICY_CONTRACT_READY"
    assert fast_rfc["status"] == "FAST_SHOCK_RFC_READY"
    assert snapshot["status"] == "POST_B2_CAMPAIGN_PROGRAM_SNAPSHOT_READY"
    assert snapshot["current_b2_campaign_status"] == "B2_CURRENT_FORM_RETURNED_TO_DESIGN"
    assert snapshot["B4_retest_allowed"] is False
    assert snapshot["B5_allowed"] is False
    assert snapshot["B6_allowed"] is False
    assert snapshot["v3_allowed"] is False
    assert snapshot["paper_shadow_allowed"] is False
    assert plan["allowed_next_actions"] == []
    assert "REOPEN_ARCHIVED_CAMPAIGN_WITHOUT_OWNER_ACTION" in plan["blocked_actions"]


def test_budget_forced_transition_b3_smoke_status_ux_and_deprecation_plan(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)
    initialize_campaign(spec_path=B3_SPEC, campaign_root=tmp_path)

    budget = build_evidence_budget_forced_transition_report(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )
    b3_smoke = build_b3_signal_compute_adapter_smoke_report(
        campaign_root=tmp_path,
        output_root=tmp_path / "outputs",
    )
    ux = build_campaign_status_ux_report(campaign_root=tmp_path)
    deprecation = build_case_specific_runner_deprecation_plan()

    assert budget["status"] == "EVIDENCE_BUDGET_FORCED_TRANSITION_PASS"
    assert budget["exhausted_budget_run_result"]["outcome"] == "BLOCKED"
    assert b3_smoke["status"] == "B3_SIGNAL_COMPUTE_ADAPTER_SMOKE_PASS"
    assert b3_smoke["adapter_run"]["compute_performed"] is True
    assert ux["status"] == "CAMPAIGN_STATUS_AND_PLAN_UX_PASS"
    assert ux["concise_status"]["adapter_run_mode"] == "COMPUTE_MODE"
    assert deprecation["status"] == "CASE_SPECIFIC_RUNNER_DEPRECATION_PLAN_READY"


def test_p0_mixed_allocator_cannot_masquerade_as_single_module() -> None:
    raw = _read_yaml(B2_SPEC)
    raw["module_graph"]["modules"] = ["p0-mixed-allocator"]
    raw["module_graph"]["allowed_mechanisms"] = ["p0_mixed_allocator"]
    raw["module_graph"]["forbidden_mechanisms"] = []
    spec = CampaignSpec.model_validate(raw)

    payload = build_campaign_validation_payload(spec=spec)

    issue_ids = {issue["issue_id"] for issue in payload["issues"]}
    assert payload["validation_status"] == "FAIL"
    assert "p0_mixed_allocator_not_single_module" in issue_ids
    assert "module_not_approved_for_campaign" in issue_ids


def test_signal_module_outputting_weight_is_rejected(tmp_path: Path) -> None:
    registry = _read_yaml(DEFAULT_MODULE_REGISTRY_PATH)
    registry["modules"]["slow-tilt-b3"]["output_contract"].append("target_weight")
    registry_path = tmp_path / "module_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    spec = load_campaign_spec(B3_SPEC)

    payload = build_campaign_validation_payload(
        spec=spec,
        module_registry_path=registry_path,
    )

    issue_ids = {issue["issue_id"] for issue in payload["issues"]}
    assert payload["validation_status"] == "FAIL"
    assert "signal_module_outputs_weight" in issue_ids


def test_unauthorized_holdout_access_fails_closed() -> None:
    raw = _read_yaml(B2_SPEC)
    raw["window_policy"]["holdout_access"] = "OWNER_AUTHORIZATION_REQUIRED"
    raw["owner_authorized_holdout"] = False
    spec = CampaignSpec.model_validate(raw)

    payload = build_campaign_validation_payload(spec=spec)

    issue_ids = {issue["issue_id"] for issue in payload["issues"]}
    assert payload["validation_status"] == "FAIL"
    assert "holdout_access_requires_owner_authorization" in issue_ids


def test_state_machine_blocks_illegal_jump_to_full_diagnostic() -> None:
    transition = validate_stage_transition("INPUT_PRECHECK", "FULL_DIAGNOSTIC")

    assert transition["allowed"] is False
    assert transition["reason"] == "ILLEGAL_STAGE_SKIP"


def test_experiment_matrix_generates_e_r_t_main_and_pairwise_interactions() -> None:
    raw = _read_yaml(B2_SPEC)
    raw["campaign_id"] = "interaction-matrix-fixture"
    raw["module_graph"]["modules"] = [
        "execution-control-e",
        "risk-overlay-b2",
        "slow-tilt-b3",
        "confidence-shrinkage-c",
        "regime-information-g",
    ]
    raw["module_graph"]["allowed_mechanisms"] = [
        "execution_control",
        "risk_signal",
        "exposure_scaler",
        "reentry_logic",
        "slow_tilt",
        "confidence_shrinkage",
        "regime_filter",
    ]
    raw["module_graph"]["forbidden_mechanisms"] = []
    raw["module_graph"]["allowed_interaction_order"] = 2
    spec = CampaignSpec.model_validate(raw)

    matrix = plan_experiment_matrix(spec=spec)
    experiment_ids = {item["experiment_id"] for item in matrix["experiment_matrix"]}
    blocked_modules = {item["module_id"] for item in matrix["blocked_modules"]}

    assert {"b0-static+E", "b0-static+R", "b0-static+T"} <= experiment_ids
    assert {"b0-static+E+R", "b0-static+E+T", "b0-static+R+T"} <= experiment_ids
    assert "confidence-shrinkage-c" in blocked_modules
    assert "regime-information-g" in blocked_modules


def test_evidence_budget_exhaustion_blocks_open_ended_needs_more_evidence(
    tmp_path: Path,
) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)
    spec, state, evidence = load_campaign_bundle("b2-risk-overlay-current-form", tmp_path)
    state.evidence_budget_used.needs_more_evidence_occurrences = (
        spec.evidence_budget.max_needs_more_evidence_occurrences
    )
    write_campaign_state(state, tmp_path / spec.campaign_id)

    gate = evaluate_gate(spec=spec, state=state, evidence=evidence[:1])
    plan = campaign_plan(campaign_id=spec.campaign_id, campaign_root=tmp_path)

    assert gate["decision_outcome"] == "OWNER_OVERRIDE_REQUIRED"
    assert "EMIT_OPEN_ENDED_NEEDS_MORE_EVIDENCE" in plan["blocked_actions"]
    assert "OWNER_OVERRIDE_REQUIRED_FOR_MORE_EVIDENCE" in plan["required_owner_actions"]


def test_interaction_classification_uses_governed_policy_thresholds() -> None:
    assert classify_interaction_effect(value=0.03) == "POSITIVE_SYNERGY"
    assert classify_interaction_effect(value=-0.03) == "NEGATIVE_INTERFERENCE"
    assert classify_interaction_effect(value=0.0) == "REDUNDANT"


def test_campaign_cli_validate_and_init(tmp_path: Path) -> None:
    runner = CliRunner()
    validate = runner.invoke(app, ["research", "campaign", "validate", "--spec", str(B2_SPEC)])
    assert validate.exit_code == 0, validate.output

    init = runner.invoke(
        app,
        [
            "research",
            "campaign",
            "init",
            "--spec",
            str(B2_SPEC),
            "--campaign-root",
            str(tmp_path),
        ],
    )
    assert init.exit_code == 0, init.output

    status_json = tmp_path / "status.json"
    status = runner.invoke(
        app,
        [
            "research",
            "campaign",
            "status",
            "--id",
            "b2-risk-overlay-current-form",
            "--campaign-root",
            str(tmp_path),
            "--json-output-path",
            str(status_json),
        ],
    )
    assert status.exit_code == 0, status.output
    payload = json.loads(status_json.read_text(encoding="utf-8"))
    assert payload["current_stage"] == "TARGETED_EVIDENCE"
    assert payload["production_effect"] == "none"


def _read_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))
