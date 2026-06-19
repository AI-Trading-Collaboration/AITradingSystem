from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.research_campaign import (
    DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    DEFAULT_CAMPAIGN_ROOT,
    DEFAULT_GATE_POLICY_PATH,
    DEFAULT_MIGRATION_PATH,
    DEFAULT_MODULE_REGISTRY_PATH,
    DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    DEFAULT_WINDOW_POLICY_PATH,
    ResearchCampaignError,
    archive_campaign,
    build_campaign_validation_payload,
    build_case_specific_runner_deprecation_plan,
    build_owner_packet,
    build_status_payload,
    campaign_plan,
    diagnose_campaign,
    evaluate_gate,
    initialize_campaign,
    load_campaign_bundle,
    load_campaign_spec,
    run_campaign_stage,
    validate_stage_adapter_contracts,
    write_campaign_control_plane_v1_validation_artifacts,
)

console = Console()
research_app = typer.Typer(help="研究 Campaign 控制面。", no_args_is_help=True)
campaign_app = typer.Typer(
    help="Research Campaign spec、状态机、证据和 owner packet。",
    no_args_is_help=True,
)
research_app.add_typer(campaign_app, name="campaign")


@campaign_app.command("init")
def init_campaign_command(
    spec: Annotated[Path, typer.Option("--spec", help="Campaign YAML/JSON spec 路径。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    migration_path: Annotated[
        Path,
        typer.Option("--migration-config", help="历史 evidence 迁移配置路径。"),
    ] = DEFAULT_MIGRATION_PATH,
    force: Annotated[bool, typer.Option(help="覆盖已有 campaign 状态。")] = False,
) -> None:
    """创建 Campaign state、evidence store、transition audit 和 reproducibility manifest。"""
    try:
        payload = initialize_campaign(
            spec_path=spec,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            migration_path=migration_path,
            force=force,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    style = "green" if payload["validation_status"] != "FAIL" else "red"
    console.print(f"[{style}]Campaign 初始化：{payload['validation_status']}[/{style}]")
    console.print(f"Campaign：{payload['campaign_id']}")
    console.print(f"Stage：{payload['current_stage']}；Outcome：{payload['current_outcome']}")
    console.print(f"Evidence records：{payload['evidence_record_count']}")
    console.print(f"目录：{payload['campaign_dir']}")


@campaign_app.command("validate")
def validate_campaign_command(
    spec: Annotated[
        Path | None,
        typer.Option("--spec", help="Campaign YAML/JSON spec 路径。"),
    ] = None,
    campaign_id: Annotated[
        str | None,
        typer.Option("--id", help="已初始化 campaign id。"),
    ] = None,
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign spec、module boundary、holdout policy、gate policy 和 safety metadata。"""
    if bool(spec) == bool(campaign_id):
        raise typer.BadParameter("--spec 和 --id 必须且只能指定一个")
    try:
        if spec is not None:
            campaign_spec = load_campaign_spec(spec)
        else:
            campaign_spec, _, _ = load_campaign_bundle(campaign_id or "", campaign_root)
        payload = build_campaign_validation_payload(
            spec=campaign_spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation", payload["validation_status"])
    console.print(
        f"issues={payload['summary']['issue_count']}；"
        f"errors={payload['summary']['error_count']}；"
        f"warnings={payload['summary']['warning_count']}；"
        f"production_effect={payload['safety_boundary']['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validate-adapters")
def validate_campaign_adapters_command(
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign stage adapter contract、输入 artifact 和 safety metadata。"""
    try:
        payload = validate_stage_adapter_contracts(
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign adapter contract validation", payload["validation_status"])
    console.print(
        f"adapters={payload['adapter_count']}；"
        f"issues={len(payload['issues'])}；"
        f"production_effect={payload['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validation-pack")
def campaign_validation_pack_command(
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """写出 Control Plane v1 rc2 adapter/parity/budget/next-action validation pack。"""
    try:
        payload = write_campaign_control_plane_v1_validation_artifacts(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation pack", payload["status"])
    console.print(
        f"artifacts={len(payload['artifacts'])}；production_effect={payload['production_effect']}"
    )


@campaign_app.command("plan")
def plan_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 stage/outcome、预算使用、允许动作、阻断动作和推荐下一阶段。"""
    try:
        payload = campaign_plan(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_campaign_plan(payload)


@campaign_app.command("allowed-actions")
def allowed_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 允许的下一步动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "allowed_next_actions": payload["allowed_next_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Allowed actions", result["allowed_next_actions"])


@campaign_app.command("blocked-actions")
def blocked_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 被阻断的动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "blocked_actions": payload["blocked_actions"],
        "required_owner_actions": payload["required_owner_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Blocked actions", result["blocked_actions"])
    _print_action_list("Owner required", result["required_owner_actions"])


@campaign_app.command("budget")
def budget_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 evidence budget used/remaining。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "budget_status": payload["budget_status"],
        "evidence_budget_used": payload["evidence_budget_used"],
        "evidence_budget_remaining": payload["evidence_budget_remaining"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_status("Campaign budget", result["budget_status"])
    console.print(f"used={result['evidence_budget_used']}")
    console.print(f"remaining={result['evidence_budget_remaining']}")


@campaign_app.command("source-artifacts")
def source_artifacts_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 Campaign source artifact lineage。"""
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=True,
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "source_artifacts": payload["source_artifacts"],
        "adapter_runtime": payload["adapter_runtime"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Source artifacts", result["source_artifacts"])


@campaign_app.command("run")
def run_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    stage: Annotated[
        str,
        typer.Option("--stage", help="要运行的 stage，默认 next。"),
    ] = "next",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """按状态机运行允许的下一阶段；缺少计算 adapter 时 fail-closed。"""
    try:
        payload = run_campaign_stage(
            campaign_id=campaign_id,
            requested_stage=stage,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            adapter_registry_path=adapter_registry_path,
            output_root=output_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign stage run", payload["outcome"])
    console.print(f"run_id={payload['run_id']}；stage={payload['stage']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；"
        f"status={payload.get('adapter_status')}"
    )
    if payload["outcome"] == "BLOCKED":
        raise typer.Exit(code=1)


@campaign_app.command("diagnose")
def diagnose_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """聚合 campaign-level evidence matrix，并披露 best/worst/missing evidence。"""
    try:
        payload = diagnose_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign diagnosis", payload["current_outcome"])
    console.print(
        f"positive={len(payload['positive_evidence'])}；"
        f"negative={len(payload['negative_evidence'])}；"
        f"missing={len(payload['missing_required_evidence_categories'])}"
    )


@campaign_app.command("gate")
def gate_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """用配置化 gate policy 计算当前 Campaign gate 结果。"""
    try:
        spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
        payload = evaluate_gate(
            spec=spec,
            state=state,
            evidence=evidence,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign gate", payload["decision_outcome"])
    console.print(
        f"scorecard={payload['scorecard_policy']}；"
        f"missing={len(payload['missing_required_evidence_categories'])}；"
        f"blockers={len(payload['blocking_evidence_ids'])}"
    )


@campaign_app.command("status")
def status_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    view: Annotated[
        str,
        typer.Option("--view", help="concise 或 detailed。"),
    ] = "concise",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 canonical Campaign status。"""
    if view not in {"concise", "detailed"}:
        raise typer.BadParameter("--view must be concise or detailed")
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=view == "detailed",
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign status", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；evidence={payload['evidence_record_count']}")
    console.print(f"budget={payload['budget_status']}；run_mode={payload['adapter_run_mode']}")
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")


@campaign_app.command("packet")
def packet_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Owner packet 输出根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
) -> None:
    """生成 Campaign Reader Brief / owner packet，不写 owner decision。"""
    try:
        payload = build_owner_packet(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            output_root=output_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign owner packet", payload["decision"])
    console.print(f"JSON：{payload['json_path']}")
    console.print(f"Markdown：{payload['markdown_path']}")
    console.print("owner_decision_appended=false；production_effect=none")


@campaign_app.command("archive")
def archive_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    reason: Annotated[
        str,
        typer.Option("--reason", help="Archive reason。"),
    ] = "manual_archive",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
) -> None:
    """归档 Campaign，不写 owner decision、不触发 production effect。"""
    try:
        payload = archive_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            reason=reason,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign archive", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；reason={payload['archive_reason']}")


@campaign_app.command("deprecation-plan")
def deprecation_plan_campaign_command(
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出旧 B2/B3 task-specific runner 的 Campaign 替代边界。"""
    payload = build_case_specific_runner_deprecation_plan()
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign deprecation plan", payload["status"])
    for runner in payload["old_runners"]:
        console.print(
            f"{runner['old_command']} -> {runner['replacement_campaign_command']}；"
            f"parity={runner['parity_status']}；status={runner['deprecation_status']}"
        )


def _print_campaign_plan(payload: dict[str, object]) -> None:
    _print_status("Campaign plan", str(payload["current_outcome"]))
    console.print(f"stage={payload['current_stage']}；next={payload['next_recommended_stage']}")
    console.print(f"budget={payload['budget_status']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；"
        f"run_mode={payload.get('adapter_run_mode') or 'none'}"
    )
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")
    console.print(f"owner_required={', '.join(payload['required_owner_actions']) or 'none'}")


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "PROMISING"} else "yellow"
    if status in {"FAIL", "BLOCKED", "REJECTED"}:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")


def _write_json_if_requested(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _print_action_list(label: str, values: object) -> None:
    console.print(f"{label}:")
    if not values:
        console.print("- none")
        return
    for value in values:
        console.print(f"- {value}")
