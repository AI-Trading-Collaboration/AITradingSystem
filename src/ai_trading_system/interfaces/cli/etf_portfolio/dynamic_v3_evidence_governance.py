from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_RECOVERY_DIR,
    DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    DEFAULT_EVIDENCE_SUMMARY_DIR,
    DEFAULT_GATE_IMPACT_DIR,
    DEFAULT_GATE_POLICY_DIR,
    DEFAULT_OBSERVE_POOL_DIR,
    DEFAULT_REGIME_COVERAGE_DIR,
    DEFAULT_RESEARCH_DECISION_DIR,
    DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    apply_evidence_gate_policy,
    candidate_recovery_report_payload,
    evidence_diagnosis_report_payload,
    evidence_gate_policy_report_payload,
    gate_impact_report_payload,
    rebuild_observe_pool_from_recovery,
    research_decision_report_payload,
    research_decision_update_report_payload,
    run_candidate_recovery,
    run_evidence_diagnosis,
    run_gate_impact,
    run_research_decision,
    update_research_decision,
    validate_candidate_recovery_artifact,
    validate_evidence_diagnosis_artifact,
    validate_evidence_gate_policy,
    validate_gate_impact_artifact,
    validate_research_decision_artifact,
    validate_research_decision_update_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_recovery_app,
    dynamic_v3_evidence_diagnosis_app,
    dynamic_v3_gate_impact_app,
    dynamic_v3_gate_policy_app,
    dynamic_v3_observe_pool_app,
    dynamic_v3_rescue_app,
    dynamic_v3_research_decision_app,
)


@dynamic_v3_research_decision_app.command("run")
def dynamic_v3_research_decision_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="medium_real sweep id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """生成 TRADING-120 research decision pack。"""
    result = run_research_decision(sweep_id=sweep_id, output_dir=output_dir)
    manifest = result["manifest"]
    recommendation = result["recommendation"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"decision_dir={result['decision_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recommendation={recommendation['recommendation']}")
    typer.echo(f"priority={recommendation['priority']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("report")
def dynamic_v3_research_decision_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest research decision pointer。"),
    ] = False,
    decision_id: Annotated[
        str | None,
        typer.Option("--decision-id", help="decision id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """展示 TRADING-120 research decision 摘要。"""
    payload = research_decision_report_payload(
        decision_id=decision_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommendation={payload['recommendation']}")
    typer.echo(f"report_path={payload['research_decision_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-research-decision")
def dynamic_v3_validate_research_decision_command(
    decision_id: Annotated[str, typer.Option("--decision-id", help="decision id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_DIR,
) -> None:
    """校验 TRADING-120 research decision artifact。"""
    payload = validate_research_decision_artifact(decision_id=decision_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_evidence_diagnosis_app.command("run")
def dynamic_v3_evidence_diagnosis_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    summary_id: Annotated[
        str | None,
        typer.Option("--summary-id", help="optional source evidence summary id。"),
    ] = None,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Annotated[
        Path,
        typer.Option("--evidence-summary-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Annotated[
        Path,
        typer.Option("--regime-coverage-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """生成 TRADING-121 evidence blocking diagnosis。"""
    result = run_evidence_diagnosis(
        sweep_id=sweep_id,
        summary_id=summary_id,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"diagnosis_id={result['diagnosis_id']}")
    typer.echo(f"diagnosis_dir={result['diagnosis_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"usable_candidates={manifest['usable_candidates']}")
    typer.echo(f"hard_blocked_candidates={manifest['hard_blocked_candidates']}")
    typer.echo(f"soft_blocked_candidates={manifest['soft_blocked_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_evidence_diagnosis_app.command("report")
def dynamic_v3_evidence_diagnosis_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence diagnosis pointer。"),
    ] = False,
    diagnosis_id: Annotated[
        str | None,
        typer.Option("--diagnosis-id", help="diagnosis id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """展示 TRADING-121 evidence diagnosis 摘要。"""
    payload = evidence_diagnosis_report_payload(
        diagnosis_id=diagnosis_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"diagnosis_id={payload['diagnosis_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"usable_candidates={payload['usable_candidates']}")
    typer.echo(f"report_path={payload['evidence_diagnosis_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-evidence-diagnosis")
def dynamic_v3_validate_evidence_diagnosis_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
) -> None:
    """校验 TRADING-121 evidence diagnosis artifact。"""
    payload = validate_evidence_diagnosis_artifact(
        diagnosis_id=diagnosis_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_impact_app.command("run")
def dynamic_v3_gate_impact_run_command(
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """生成 TRADING-122 gate impact simulation。"""
    result = run_gate_impact(
        diagnosis_id=diagnosis_id,
        diagnosis_dir=diagnosis_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"impact_id={result['impact_id']}")
    typer.echo(f"impact_dir={result['impact_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"baseline_observe_candidates={manifest['baseline_observe_candidates']}")
    typer.echo(f"best_scenario={manifest['best_scenario']}")
    typer.echo(f"best_observe_candidates={manifest['best_observe_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_gate_impact_app.command("report")
def dynamic_v3_gate_impact_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest gate impact pointer。"),
    ] = False,
    impact_id: Annotated[str | None, typer.Option("--impact-id", help="impact id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """展示 TRADING-122 gate impact 摘要。"""
    payload = gate_impact_report_payload(impact_id=impact_id, latest=latest, output_dir=output_dir)
    typer.echo(f"impact_id={payload['impact_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"best_scenario={payload['best_scenario']}")
    typer.echo(f"best_observe_candidates={payload['best_observe_candidates']}")
    typer.echo(f"report_path={payload['gate_impact_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-gate-impact")
def dynamic_v3_validate_gate_impact_command(
    impact_id: Annotated[str, typer.Option("--impact-id", help="impact id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
) -> None:
    """校验 TRADING-122 gate impact artifact。"""
    payload = validate_gate_impact_artifact(impact_id=impact_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_policy_app.command("validate")
def dynamic_v3_gate_policy_validate_command(
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> None:
    """校验 TRADING-123 evidence gate policy。"""
    payload = validate_evidence_gate_policy(policy_path=policy_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"policy_version={payload['policy_version']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_gate_policy_app.command("report")
def dynamic_v3_gate_policy_report_command(
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
) -> None:
    """展示 TRADING-123 evidence gate policy 摘要。"""
    payload = evidence_gate_policy_report_payload(policy_path=policy_path)
    validation = payload["validation"]
    typer.echo(f"status={payload['status']}")
    typer.echo(f"policy_path={payload['policy_path']}")
    typer.echo(f"policy_version={validation['policy_version']}")
    typer.echo(f"manual_review_allowed={','.join(validation['manual_review_allowed'])}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_gate_policy_app.command("apply")
def dynamic_v3_gate_policy_apply_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    policy_path: Annotated[
        Path,
        typer.Option("--policy", "--policy-path", help="evidence gate policy YAML。"),
    ] = DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    evidence_summary_dir: Annotated[
        Path,
        typer.Option("--evidence-summary-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
    regime_coverage_dir: Annotated[
        Path,
        typer.Option("--regime-coverage-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="gate policy artifact root。"),
    ] = DEFAULT_GATE_POLICY_DIR,
) -> None:
    """应用 TRADING-123 evidence gate policy，写 calibrated candidate status。"""
    result = apply_evidence_gate_policy(
        sweep_id=sweep_id,
        policy_path=policy_path,
        sweep_output_dir=sweep_output_dir,
        evidence_summary_dir=evidence_summary_dir,
        regime_coverage_dir=regime_coverage_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"policy_run_id={result['policy_run_id']}")
    typer.echo(f"policy_dir={result['policy_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_only_candidates={manifest['observe_only_candidates']}")
    typer.echo(f"manual_review_required_candidates={manifest['manual_review_required_candidates']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_recovery_app.command("run")
def dynamic_v3_candidate_recovery_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    policy_run_id: Annotated[str, typer.Option("--policy-run-id", help="policy run id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    gate_policy_dir: Annotated[
        Path,
        typer.Option("--gate-policy-dir", help="gate policy artifact root。"),
    ] = DEFAULT_GATE_POLICY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """生成 TRADING-124 recovered observe-only candidates。"""
    result = run_candidate_recovery(
        sweep_id=sweep_id,
        policy_run_id=policy_run_id,
        sweep_output_dir=sweep_output_dir,
        gate_policy_dir=gate_policy_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"recovery_id={result['recovery_id']}")
    typer.echo(f"recovery_dir={result['recovery_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recovered_candidate_count={manifest['recovered_candidate_count']}")
    typer.echo(f"observe_only_candidate_count={manifest['observe_only_candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_recovery_app.command("report")
def dynamic_v3_candidate_recovery_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest candidate recovery pointer。"),
    ] = False,
    recovery_id: Annotated[
        str | None,
        typer.Option("--recovery-id", help="recovery id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """展示 TRADING-124 candidate recovery 摘要。"""
    payload = candidate_recovery_report_payload(
        recovery_id=recovery_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"recovery_id={payload['recovery_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recovered_candidate_count={payload['recovered_candidate_count']}")
    typer.echo(f"report_path={payload['recovery_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-recovery")
def dynamic_v3_validate_candidate_recovery_command(
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="recovery id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
) -> None:
    """校验 TRADING-124 candidate recovery artifact。"""
    payload = validate_candidate_recovery_artifact(recovery_id=recovery_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_observe_pool_app.command("rebuild")
def dynamic_v3_observe_pool_rebuild_command(
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="candidate recovery id。")],
    recovery_dir: Annotated[
        Path,
        typer.Option("--recovery-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """基于 TRADING-124 recovery artifact 重建 observe-only pool。"""
    result = rebuild_observe_pool_from_recovery(
        recovery_id=recovery_id,
        recovery_dir=recovery_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pool_id={result['pool_id']}")
    typer.echo(f"pool_dir={result['pool_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_candidate_count={manifest['observe_candidate_count']}")
    typer.echo(f"manual_review_required_count={manifest['manual_review_required_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("update")
def dynamic_v3_research_decision_update_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    diagnosis_id: Annotated[str, typer.Option("--diagnosis-id", help="diagnosis id。")],
    impact_id: Annotated[str, typer.Option("--impact-id", help="gate impact id。")],
    recovery_id: Annotated[str, typer.Option("--recovery-id", help="candidate recovery id。")],
    diagnosis_dir: Annotated[
        Path,
        typer.Option("--diagnosis-dir", help="evidence diagnosis artifact root。"),
    ] = DEFAULT_EVIDENCE_DIAGNOSIS_DIR,
    gate_impact_dir: Annotated[
        Path,
        typer.Option("--gate-impact-dir", help="gate impact artifact root。"),
    ] = DEFAULT_GATE_IMPACT_DIR,
    recovery_dir: Annotated[
        Path,
        typer.Option("--recovery-dir", help="candidate recovery artifact root。"),
    ] = DEFAULT_CANDIDATE_RECOVERY_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """生成 TRADING-125 research decision update。"""
    result = update_research_decision(
        sweep_id=sweep_id,
        diagnosis_id=diagnosis_id,
        impact_id=impact_id,
        recovery_id=recovery_id,
        diagnosis_dir=diagnosis_dir,
        gate_impact_dir=gate_impact_dir,
        recovery_dir=recovery_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    go_no_go = result["go_no_go_matrix"]
    typer.echo(f"decision_update_id={result['decision_update_id']}")
    typer.echo(f"decision_update_dir={result['decision_update_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"go_no_go={go_no_go['go_no_go']}")
    typer.echo(f"recommended_action={go_no_go['recommended_action']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_research_decision_app.command("update-report")
def dynamic_v3_research_decision_update_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest decision update pointer。"),
    ] = False,
    decision_update_id: Annotated[
        str | None,
        typer.Option("--decision-update-id", help="decision update id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """展示 TRADING-125 research decision update 摘要。"""
    payload = research_decision_update_report_payload(
        decision_update_id=decision_update_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"decision_update_id={payload['decision_update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"go_no_go={payload['go_no_go']}")
    typer.echo(f"recommended_action={payload['recommended_action']}")
    typer.echo(f"report_path={payload['research_decision_update_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-research-decision-update")
def dynamic_v3_validate_research_decision_update_command(
    decision_update_id: Annotated[
        str,
        typer.Option("--decision-update-id", help="decision update id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="research decision update artifact root。"),
    ] = DEFAULT_RESEARCH_DECISION_UPDATE_DIR,
) -> None:
    """校验 TRADING-125 research decision update artifact。"""
    payload = validate_research_decision_update_artifact(
        decision_update_id=decision_update_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
