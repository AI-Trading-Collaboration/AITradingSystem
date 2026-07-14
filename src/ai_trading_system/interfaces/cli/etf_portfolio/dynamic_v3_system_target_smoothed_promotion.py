from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_paper_shadow_primary_switch_app,
    dynamic_v3_primary_research_candidate_gate_app,
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_forward_binding_app,
    dynamic_v3_smoothed_owner_promotion_app,
    dynamic_v3_smoothed_promotion_review_app,
)


def _mapping_obj(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records_obj(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


@dynamic_v3_smoothed_promotion_review_app.command("pack")
def dynamic_v3_smoothed_promotion_review_pack_command(
    readiness_scorecard_id: Annotated[
        str,
        typer.Option("--readiness-scorecard-id", help="readiness scorecard id。"),
    ],
    owner_update_id: Annotated[
        str,
        typer.Option("--owner-update-id", help="owner update id。"),
    ],
    watch_pack_id: Annotated[
        str,
        typer.Option("--watch-pack-id", help="watch pack id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed promotion review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> None:
    """运行 TRADING-261 smoothed promotion review pack。"""
    result = system_target.build_smoothed_promotion_review_pack(
        readiness_scorecard_id=readiness_scorecard_id,
        owner_update_id=owner_update_id,
        watch_pack_id=watch_pack_id,
        output_dir=output_dir,
    )
    evidence = result["promotion_evidence_summary"]
    blocking = result["promotion_blocking_issues"]
    typer.echo(f"promotion_review_id={result['promotion_review_id']}")
    typer.echo(f"promotion_review_dir={result['promotion_review_dir']}")
    typer.echo(f"readiness_decision={evidence['readiness_decision']}")
    typer.echo(f"decision_confidence={evidence['decision_confidence']}")
    typer.echo(f"can_enter_owner_review={blocking['can_enter_owner_review']}")
    typer.echo("automatic_promotion_allowed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_promotion_review_app.command("report")
def dynamic_v3_smoothed_promotion_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest promotion review。"),
    ] = False,
    promotion_review_id: Annotated[
        str | None,
        typer.Option("--promotion-review-id", help="promotion review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed promotion review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> None:
    """展示 TRADING-261 smoothed promotion review 摘要。"""
    payload = system_target.smoothed_promotion_review_report_payload(
        promotion_review_id=promotion_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    evidence = _mapping_obj(payload.get("promotion_evidence_summary"))
    blocking = _mapping_obj(payload.get("promotion_blocking_issues"))
    typer.echo(f"promotion_review_id={payload['promotion_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"readiness_decision={evidence.get('readiness_decision')}")
    typer.echo(f"decision_confidence={evidence.get('decision_confidence')}")
    typer.echo(f"can_enter_owner_review={blocking.get('can_enter_owner_review')}")
    typer.echo(f"report_path={payload['smoothed_promotion_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-promotion-review")
def dynamic_v3_validate_smoothed_promotion_review_command(
    promotion_review_id: Annotated[
        str,
        typer.Option("--promotion-review-id", help="promotion review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed promotion review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_PROMOTION_REVIEW_DIR,
) -> None:
    """校验 TRADING-261 smoothed promotion review artifact。"""
    payload = system_target.validate_smoothed_promotion_review_artifact(
        promotion_review_id=promotion_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_primary_research_candidate_gate_app.command("run")
def dynamic_v3_primary_research_candidate_gate_run_command(
    promotion_review_id: Annotated[
        str,
        typer.Option("--promotion-review-id", help="promotion review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="primary candidate gate artifact root。"),
    ] = system_target.DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> None:
    """运行 TRADING-262 primary research candidate gate。"""
    result = system_target.run_primary_research_candidate_gate(
        promotion_review_id=promotion_review_id,
        output_dir=output_dir,
    )
    decision = result["gate_decision"]
    typer.echo(f"gate_id={result['gate_id']}")
    typer.echo(f"gate_dir={result['gate_dir']}")
    typer.echo(f"gate_decision={decision['gate_decision']}")
    typer.echo(f"decision_confidence={decision['decision_confidence']}")
    typer.echo(f"owner_approval_required={decision['owner_approval_required']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_primary_research_candidate_gate_app.command("report")
def dynamic_v3_primary_research_candidate_gate_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest primary gate。"),
    ] = False,
    gate_id: Annotated[str | None, typer.Option("--gate-id", help="gate id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="primary candidate gate artifact root。"),
    ] = system_target.DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> None:
    """展示 TRADING-262 primary gate 摘要。"""
    payload = system_target.primary_research_candidate_gate_report_payload(
        gate_id=gate_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("gate_decision"))
    typer.echo(f"gate_id={payload['gate_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"gate_decision={decision.get('gate_decision')}")
    typer.echo(f"decision_confidence={decision.get('decision_confidence')}")
    typer.echo(f"report_path={payload['primary_research_candidate_gate_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-primary-research-candidate-gate")
def dynamic_v3_validate_primary_research_candidate_gate_command(
    gate_id: Annotated[str, typer.Option("--gate-id", help="gate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="primary candidate gate artifact root。"),
    ] = system_target.DEFAULT_PRIMARY_RESEARCH_CANDIDATE_GATE_DIR,
) -> None:
    """校验 TRADING-262 primary research candidate gate artifact。"""
    payload = system_target.validate_primary_research_candidate_gate_artifact(
        gate_id=gate_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_forward_binding_app.command("run")
def dynamic_v3_smoothed_forward_binding_run_command(
    confirmation_id: Annotated[
        str,
        typer.Option("--confirmation-id", help="smoothed confirmation id。"),
    ],
    gate_id: Annotated[str, typer.Option("--gate-id", help="primary gate id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward binding artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> None:
    """运行 TRADING-263 smoothed forward confirmation binding。"""
    result = system_target.run_smoothed_forward_binding(
        confirmation_id=confirmation_id,
        gate_id=gate_id,
        output_dir=output_dir,
    )
    targets = _records_obj(result["bound_confirmation_targets"].get("targets"))
    typer.echo(f"binding_id={result['binding_id']}")
    typer.echo(f"binding_dir={result['binding_dir']}")
    typer.echo(f"bound_target_count={len(targets)}")
    typer.echo("bound_to_weekly_progress=true")
    typer.echo("auto_rule_change_allowed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_forward_binding_app.command("report")
def dynamic_v3_smoothed_forward_binding_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward binding。"),
    ] = False,
    binding_id: Annotated[str | None, typer.Option("--binding-id", help="binding id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward binding artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> None:
    """展示 TRADING-263 smoothed forward binding 摘要。"""
    payload = system_target.smoothed_forward_binding_report_payload(
        binding_id=binding_id,
        latest=latest,
        output_dir=output_dir,
    )
    targets = _records_obj(_mapping_obj(payload.get("bound_confirmation_targets")).get("targets"))
    watch_only = [row.get("target_id") for row in targets if row.get("status") == "WATCH_ONLY"]
    typer.echo(f"binding_id={payload['binding_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"bound_target_count={len(targets)}")
    typer.echo(f"watch_only_targets={','.join(str(item) for item in watch_only)}")
    typer.echo(f"report_path={payload['smoothed_forward_binding_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-forward-binding")
def dynamic_v3_validate_smoothed_forward_binding_command(
    binding_id: Annotated[str, typer.Option("--binding-id", help="binding id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed forward binding artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
) -> None:
    """校验 TRADING-263 smoothed forward binding artifact。"""
    payload = system_target.validate_smoothed_forward_binding_artifact(
        binding_id=binding_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_primary_switch_app.command("plan")
def dynamic_v3_paper_shadow_primary_switch_plan_command(
    gate_id: Annotated[str, typer.Option("--gate-id", help="primary gate id。")],
    binding_id: Annotated[str, typer.Option("--binding-id", help="binding id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow primary switch artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> None:
    """运行 TRADING-264 paper shadow primary candidate switch plan。"""
    result = system_target.build_paper_shadow_primary_switch_plan(
        gate_id=gate_id,
        binding_id=binding_id,
        output_dir=output_dir,
    )
    plan = result["primary_switch_plan"]
    safety = result["primary_switch_safety_checks"]
    typer.echo(f"switch_plan_id={result['switch_plan_id']}")
    typer.echo(f"switch_plan_dir={result['switch_plan_dir']}")
    typer.echo(f"proposed_primary_research_candidate={plan['proposed_primary_research_candidate']}")
    typer.echo(f"auto_switch={plan['auto_switch']}")
    typer.echo(f"safety_status={safety['status']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_paper_shadow_primary_switch_app.command("report")
def dynamic_v3_paper_shadow_primary_switch_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest primary switch plan。"),
    ] = False,
    switch_plan_id: Annotated[
        str | None,
        typer.Option("--switch-plan-id", help="switch plan id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow primary switch artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> None:
    """展示 TRADING-264 paper shadow primary switch plan 摘要。"""
    payload = system_target.paper_shadow_primary_switch_report_payload(
        switch_plan_id=switch_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    plan = _mapping_obj(payload.get("primary_switch_plan"))
    safety = _mapping_obj(payload.get("primary_switch_safety_checks"))
    typer.echo(f"switch_plan_id={payload['switch_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        f"proposed_primary_research_candidate={plan.get('proposed_primary_research_candidate')}"
    )
    typer.echo(f"auto_switch={plan.get('auto_switch')}")
    typer.echo(f"safety_status={safety.get('status')}")
    typer.echo(f"report_path={payload['paper_shadow_primary_switch_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-primary-switch")
def dynamic_v3_validate_paper_shadow_primary_switch_command(
    switch_plan_id: Annotated[
        str,
        typer.Option("--switch-plan-id", help="switch plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow primary switch artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
) -> None:
    """校验 TRADING-264 paper shadow primary switch artifact。"""
    payload = system_target.validate_paper_shadow_primary_switch_artifact(
        switch_plan_id=switch_plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_owner_promotion_app.command("create")
def dynamic_v3_smoothed_owner_promotion_create_command(
    promotion_review_id: Annotated[
        str,
        typer.Option("--promotion-review-id", help="promotion review id。"),
    ],
    gate_id: Annotated[str, typer.Option("--gate-id", help="primary gate id。")],
    switch_plan_id: Annotated[
        str,
        typer.Option("--switch-plan-id", help="switch plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner promotion decision artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> None:
    """创建 TRADING-265 smoothed owner promotion decision journal。"""
    result = system_target.create_smoothed_owner_promotion_decision(
        promotion_review_id=promotion_review_id,
        gate_id=gate_id,
        switch_plan_id=switch_plan_id,
        output_dir=output_dir,
    )
    decision = result["owner_promotion_decision"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"decision_dir={result['decision_dir']}")
    typer.echo(f"owner_decision={decision['owner_decision']}")
    typer.echo(f"recommended_owner_action={decision['recommended_owner_action']}")
    typer.echo("actual_switch_executed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_owner_promotion_app.command("record")
def dynamic_v3_smoothed_owner_promotion_record_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    decision: Annotated[
        str,
        typer.Option("--decision", help="owner decision。"),
    ],
    decision_reason: Annotated[
        str,
        typer.Option("--decision-reason", help="owner decision reason。"),
    ] = "",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner promotion decision artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> None:
    """记录 TRADING-265 smoothed owner promotion decision。"""
    result = system_target.record_smoothed_owner_promotion_decision(
        decision_id=decision_id,
        decision=decision,
        decision_reason=decision_reason,
        output_dir=output_dir,
    )
    owner_decision = result["owner_promotion_decision"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"owner_decision={owner_decision['owner_decision']}")
    typer.echo(f"recommended_owner_action={owner_decision['recommended_owner_action']}")
    typer.echo(
        "paper_shadow_primary_candidate_change_allowed="
        f"{owner_decision['paper_shadow_primary_candidate_change_allowed']}"
    )
    typer.echo("actual_switch_executed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_owner_promotion_app.command("report")
def dynamic_v3_smoothed_owner_promotion_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner promotion。"),
    ] = False,
    decision_id: Annotated[str | None, typer.Option("--decision-id", help="decision id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner promotion decision artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> None:
    """展示 TRADING-265 owner promotion decision 摘要。"""
    payload = system_target.smoothed_owner_promotion_report_payload(
        decision_id=decision_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("owner_promotion_decision"))
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"owner_decision={decision.get('owner_decision')}")
    typer.echo(f"recommended_owner_action={decision.get('recommended_owner_action')}")
    typer.echo(
        "paper_shadow_primary_candidate_change_allowed="
        f"{decision.get('paper_shadow_primary_candidate_change_allowed')}"
    )
    typer.echo(f"report_path={payload['smoothed_owner_promotion_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-owner-promotion")
def dynamic_v3_validate_smoothed_owner_promotion_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner promotion decision artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
) -> None:
    """校验 TRADING-265 smoothed owner promotion decision artifact。"""
    payload = system_target.validate_smoothed_owner_promotion_artifact(
        decision_id=decision_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
