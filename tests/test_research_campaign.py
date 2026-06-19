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
    build_campaign_validation_payload,
    campaign_plan,
    classify_interaction_effect,
    evaluate_gate,
    initialize_campaign,
    load_campaign_bundle,
    load_campaign_spec,
    plan_experiment_matrix,
    run_campaign_stage,
    validate_stage_transition,
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


def test_migrated_stage_runner_uses_imported_audited_evidence(tmp_path: Path) -> None:
    initialize_campaign(spec_path=B2_SPEC, campaign_root=tmp_path)

    payload = run_campaign_stage(
        campaign_id="b2-risk-overlay-current-form",
        requested_stage="TARGETED_EVIDENCE",
        campaign_root=tmp_path,
    )

    assert payload["outcome"] == "NEEDS_MORE_EVIDENCE"
    assert payload["generated_evidence_count"] == 0
    assert payload["result"]["source"] == "imported_audited_evidence"
    assert payload["safety_boundary"]["production_effect"] == "none"


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
