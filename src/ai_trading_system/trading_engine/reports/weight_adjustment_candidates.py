from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.yaml_loader import safe_load_yaml_path

WEIGHT_ADJUSTMENT_CANDIDATE_SCHEMA_VERSION = 1
WEIGHT_ADJUSTMENT_CANDIDATE_REPORT_TYPE = "weight_adjustment_candidates"
MODE_OBSERVE_ONLY = "observe_only"
PRODUCTION_EFFECT_NONE = "none"
STATUS_LIMITED = "LIMITED"
STATUS_BLOCKED = "BLOCKED"
STATUS_OBSERVE_ONLY = "OBSERVE_ONLY"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_WEIGHT_ADJUSTMENT_CANDIDATE_POLICY_PATH = (
    REPO_ROOT / "config" / "weight_adjustment_candidate_policy.yaml"
)
DEFAULT_PARAMETER_GOVERNANCE_PATH = REPO_ROOT / "config" / "parameter_governance.yaml"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"
DEFAULT_SHADOW_PROFILES_PATH = REPO_ROOT / "config" / "weights" / "shadow_weight_profiles.yaml"

BLOCKER_MISSING_DAILY_SUMMARY = "missing_daily_decision_summary"
BLOCKER_MISSING_PAPER_SIGNAL_QUALITY = "missing_paper_signal_quality"
BLOCKER_MISSING_SHADOW_IMPACT = "missing_shadow_parameter_impact"
BLOCKER_PARAMETER_GOVERNANCE_MISSING = "parameter_governance_missing"
BLOCKER_PRODUCTION_PROFILE_MISSING = "production_profile_missing"
BLOCKER_INSUFFICIENT_SAMPLE = "insufficient_sample"
BLOCKER_LOW_DATA_QUALITY = "low_data_quality"
BLOCKER_SYNTHETIC_RATIO_HIGH = "synthetic_snapshot_ratio_too_high"
BLOCKER_CONTINUOUS_REPLAY_MISSING = "continuous_replay_missing"
BLOCKER_SHADOW_IMPACT_INSUFFICIENT = "shadow_impact_insufficient"
BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE = "paper_signal_quality_unreliable"
BLOCKER_MANUAL_APPROVAL = "manual_approval_required"
BLOCKER_SHADOW_PROFILE_MISSING = "shadow_profile_missing"
BLOCKER_WEIGHT_TOTAL_IMBALANCE = "weight_total_imbalance"

REASON_EXPLANATIONS = {
    BLOCKER_MISSING_DAILY_SUMMARY: "缺少同日 daily_decision_summary JSON。",
    BLOCKER_MISSING_PAPER_SIGNAL_QUALITY: "缺少同日 paper_signal_quality JSON。",
    BLOCKER_MISSING_SHADOW_IMPACT: "缺少同日 shadow_parameter_impact JSON。",
    BLOCKER_PARAMETER_GOVERNANCE_MISSING: "缺少 parameter_governance manifest。",
    BLOCKER_PRODUCTION_PROFILE_MISSING: "缺少 current production weight profile。",
    BLOCKER_INSUFFICIENT_SAMPLE: "paper / shadow 样本数低于 policy floor。",
    BLOCKER_LOW_DATA_QUALITY: "data gate 或 paper/shadow 质量状态不足以支持调权候选。",
    BLOCKER_SYNTHETIC_RATIO_HIGH: "synthetic snapshot ratio 高于 policy 上限。",
    BLOCKER_CONTINUOUS_REPLAY_MISSING: "缺少 continuous-portfolio replay 证据。",
    BLOCKER_SHADOW_IMPACT_INSUFFICIENT: "shadow impact 尚不足以支持权重候选复核。",
    BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE: "paper signal quality 仍不可靠。",
    BLOCKER_MANUAL_APPROVAL: "任何权重候选都必须先经人工复核。",
    BLOCKER_SHADOW_PROFILE_MISSING: "没有可参考的 existing shadow profile。",
    BLOCKER_WEIGHT_TOTAL_IMBALANCE: "候选权重和未保持 policy 要求的总权重。",
}


def default_weight_adjustment_candidates_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"weight_adjustment_candidates_{as_of.isoformat()}.json"


def build_weight_adjustment_candidates_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_ADJUSTMENT_CANDIDATE_POLICY_PATH,
    daily_decision_summary_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    parameter_governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_profiles_path: Path = DEFAULT_SHADOW_PROFILES_PATH,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_weight_adjustment_candidates_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")

    policy = _load_yaml_object(policy_path)
    thresholds = _mapping(policy.get("thresholds"))
    required_validations = _strings(policy.get("required_validations"))
    max_delta = _float_value(thresholds.get("max_single_day_weight_delta"), default=0.02)
    target_total = _float_value(thresholds.get("target_total_weight_sum"), default=1.0)
    total_tolerance = _float_value(thresholds.get("total_weight_tolerance"), default=0.000001)

    resolved_daily_summary_path = daily_decision_summary_path or (
        reports_dir / f"daily_decision_summary_{as_of.isoformat()}.json"
    )
    resolved_signal_quality_path = paper_signal_quality_path or (
        reports_dir / f"paper_signal_quality_{as_of.isoformat()}.json"
    )
    resolved_shadow_impact_path = shadow_parameter_impact_path or (
        reports_dir / f"shadow_parameter_impact_{as_of.isoformat()}.json"
    )
    selected_replay_path = replay_json_path or _select_latest_replay_path(reports_dir, as_of)

    daily_summary = _read_json_object(resolved_daily_summary_path)
    signal_quality = _read_json_object(resolved_signal_quality_path)
    shadow_impact = _read_json_object(resolved_shadow_impact_path)
    replay_payload = _read_json_object(selected_replay_path)
    parameter_governance = _load_yaml_object(parameter_governance_path)
    production_profile = _load_yaml_object(production_profile_path)
    shadow_profiles = _load_yaml_object(shadow_profiles_path)

    gate = _candidate_gate(
        daily_summary=daily_summary,
        signal_quality=signal_quality,
        shadow_impact=shadow_impact,
        replay_payload=replay_payload,
        parameter_governance=parameter_governance,
        production_profile=production_profile,
        shadow_profiles=shadow_profiles,
        thresholds=thresholds,
    )
    source_weights = _production_weights(production_profile)
    shadow_profile_records = _shadow_profile_records(shadow_profiles)
    candidates = _build_candidates(
        as_of=as_of,
        generated_at=generated,
        source_weights=source_weights,
        production_profile=production_profile,
        production_profile_path=production_profile_path,
        shadow_profiles=shadow_profile_records,
        gate=gate,
        max_delta=max_delta,
        target_total=target_total,
        total_tolerance=total_tolerance,
        required_validations=required_validations,
    )
    candidate_count = len(candidates)
    top_candidate_id = str(candidates[0]["candidate_id"]) if candidates else ""
    policy_report = _policy_report(policy, policy_path)
    payload = {
        "schema_version": WEIGHT_ADJUSTMENT_CANDIDATE_SCHEMA_VERSION,
        "report_type": WEIGHT_ADJUSTMENT_CANDIDATE_REPORT_TYPE,
        "generated_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "mode": MODE_OBSERVE_ONLY,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "status": gate["status"],
        "gate_status": gate["status"],
        "candidate_count": candidate_count,
        "top_candidate_id": top_candidate_id,
        "policy_id": policy_report["policy_id"],
        "policy_version": policy_report["version"],
        "thresholds_snapshot": policy_report["thresholds"],
        "policy": policy_report,
        "evaluation_scope": {
            "observe_only": True,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "changes_production_parameters": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "uses_single_day_pnl_to_increase_weight": False,
        },
        "safety_boundary": {
            "reads_broker_api_key": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay": False,
            "writes_production_profile": False,
            "deletes_core_risk_gate": False,
            "bypasses_data_quality_gate": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
        },
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "source_artifacts": {
            "reports_dir": str(reports_dir),
            "policy_path": _artifact_record(policy_path, reports_dir),
            "daily_decision_summary": _artifact_record(
                resolved_daily_summary_path,
                reports_dir,
                expected_report_type="daily_decision_summary",
                payload=daily_summary,
            ),
            "paper_signal_quality": _artifact_record(
                resolved_signal_quality_path,
                reports_dir,
                expected_report_type="paper_signal_quality",
                payload=signal_quality,
            ),
            "shadow_parameter_impact": _artifact_record(
                resolved_shadow_impact_path,
                reports_dir,
                expected_report_type="shadow_parameter_impact",
                payload=shadow_impact,
            ),
            "paper_trading_replay": _artifact_record(
                selected_replay_path,
                reports_dir,
                expected_report_type="paper_trading_replay",
                payload=replay_payload,
                optional=True,
            ),
            "parameter_governance": _artifact_record(parameter_governance_path, reports_dir),
            "production_profile": _artifact_record(production_profile_path, reports_dir),
            "shadow_profiles": _artifact_record(shadow_profiles_path, reports_dir, optional=True),
        },
        "input_status": gate["input_status"],
        "candidate_gate": gate,
        "summary": {
            "candidate_count": candidate_count,
            "top_candidate_id": top_candidate_id,
            "gate_status": gate["status"],
            "main_blocked_by": gate["blocked_by"][0] if gate["blocked_by"] else "none",
            "production_effect": PRODUCTION_EFFECT_NONE,
            "mode": MODE_OBSERVE_ONLY,
        },
        "candidates": candidates,
        "notes": [
            "本报告只生成 observe-only 权重调节候选，不修改 production profile。",
            "候选不会改变 daily dashboard 主结论，不触发交易，也不会绕过 data quality gate。",
            "单日 paper PnL 不会用于调高权重。",
        ],
    }
    return payload


def write_weight_adjustment_candidates_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    policy_path: Path = DEFAULT_WEIGHT_ADJUSTMENT_CANDIDATE_POLICY_PATH,
    daily_decision_summary_path: Path | None = None,
    paper_signal_quality_path: Path | None = None,
    shadow_parameter_impact_path: Path | None = None,
    replay_json_path: Path | None = None,
    parameter_governance_path: Path = DEFAULT_PARAMETER_GOVERNANCE_PATH,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    shadow_profiles_path: Path = DEFAULT_SHADOW_PROFILES_PATH,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_weight_adjustment_candidates_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        policy_path=policy_path,
        daily_decision_summary_path=daily_decision_summary_path,
        paper_signal_quality_path=paper_signal_quality_path,
        shadow_parameter_impact_path=shadow_parameter_impact_path,
        replay_json_path=replay_json_path,
        parameter_governance_path=parameter_governance_path,
        production_profile_path=production_profile_path,
        shadow_profiles_path=shadow_profiles_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated_at,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_weight_adjustment_candidates_report(payload), encoding="utf-8")
    return payload


def render_weight_adjustment_candidates_report(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    gate = _mapping(payload.get("candidate_gate"))
    thresholds = _mapping(payload.get("thresholds_snapshot"))
    candidates = _list_mappings(payload.get("candidates"))
    lines = [
        "# Weight Adjustment Candidate Generator",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- mode：`{payload.get('mode')}`",
        f"- gate_status：`{payload.get('gate_status')}`",
        "- production_effect=none",
        "- production 参数修改：无",
        "- daily dashboard 主结论影响：无",
        "- 交易触发：无",
        "",
        "## Policy Thresholds",
        "",
        "| Threshold | Value |",
        "|---|---:|",
        (
            "| max_single_day_weight_delta | "
            f"{thresholds.get('max_single_day_weight_delta', 'missing')} |"
        ),
        f"| target_total_weight_sum | {thresholds.get('target_total_weight_sum', 'missing')} |",
        f"| total_weight_tolerance | {thresholds.get('total_weight_tolerance', 'missing')} |",
        (
            "| minimum_paper_signal_sample_count | "
            f"{thresholds.get('minimum_paper_signal_sample_count', 'missing')} |"
        ),
        (
            "| minimum_shadow_sample_count | "
            f"{thresholds.get('minimum_shadow_sample_count', 'missing')} |"
        ),
        (
            "| maximum_synthetic_snapshot_ratio | "
            f"{thresholds.get('maximum_synthetic_snapshot_ratio', 'missing')} |"
        ),
        "",
        "## Gate",
        "",
        f"- candidate_count：{summary.get('candidate_count', 0)}",
        f"- top_candidate_id：`{summary.get('top_candidate_id', '')}`",
        f"- main_blocked_by：`{summary.get('main_blocked_by', 'none')}`",
        f"- blocked_by：{', '.join(_strings(gate.get('blocked_by'))) or 'none'}",
        f"- explanation：{gate.get('explanation', '')}",
        "",
        "## Candidates",
        "",
        "| candidate_id | source_profile | target_profile | blocked_by | max_abs_delta |",
        "|---|---|---|---|---:|",
    ]
    for candidate in candidates:
        source_profile = _mapping(candidate.get("source_profile"))
        target_profile = _mapping(candidate.get("target_profile"))
        deltas = [
            abs(_float_value(change.get("delta"), default=0.0))
            for change in _list_mappings(candidate.get("parameter_changes"))
        ]
        lines.append(
            "| "
            f"`{candidate.get('candidate_id')}` | "
            f"{source_profile.get('profile_id', 'missing')} | "
            f"{target_profile.get('profile_id', 'missing')} | "
            f"{', '.join(_strings(candidate.get('blocked_by'))) or 'none'} | "
            f"{max(deltas) if deltas else 0.0:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- 只生成候选，不写入 `config/weights/weight_profile_current.yaml`。",
            "- 不删除 core risk gate，不绕过 `aits validate-data`。",
            "- 不根据单日 paper PnL 调高任何权重。",
            "- 所有候选进入人工复核前均保持 blocked。",
            "",
        ]
    )
    return "\n".join(lines)


def _build_candidates(
    *,
    as_of: date,
    generated_at: datetime,
    source_weights: dict[str, float],
    production_profile: dict[str, Any],
    production_profile_path: Path,
    shadow_profiles: list[dict[str, Any]],
    gate: dict[str, Any],
    max_delta: float,
    target_total: float,
    total_tolerance: float,
    required_validations: list[str],
) -> list[dict[str, Any]]:
    if not source_weights:
        return [
            _limited_candidate(
                as_of=as_of,
                generated_at=generated_at,
                gate=gate,
                required_validations=required_validations,
                reason_code=BLOCKER_PRODUCTION_PROFILE_MISSING,
            )
        ]
    if gate["status"] == STATUS_LIMITED:
        candidate_profiles = [
            {
                "profile_id": "limited_input",
                "version": "limited",
                "target_weights": dict(source_weights),
                "metadata": {},
            }
        ]
    else:
        candidate_profiles = shadow_profiles or [
            {
                "profile_id": "missing_shadow_profile",
                "version": "missing",
                "target_weights": dict(source_weights),
                "metadata": {},
            }
        ]
    candidates: list[dict[str, Any]] = []
    for profile in candidate_profiles:
        profile_id = _string_value(profile.get("profile_id")) or "unknown_shadow_profile"
        shadow_target = _weights_from_mapping(_mapping(profile.get("target_weights")))
        if set(shadow_target) != set(source_weights):
            shadow_target = dict(source_weights)
        target_weights = _small_step_weights(
            source_weights=source_weights,
            target_weights=shadow_target,
            max_delta=max_delta,
        )
        total_weight = sum(target_weights.values())
        blocked_by = list(gate["blocked_by"])
        if abs(total_weight - target_total) > total_tolerance:
            blocked_by.append(BLOCKER_WEIGHT_TOTAL_IMBALANCE)
        blocked_by = _unique_strings(blocked_by)
        changes = [
            {
                "parameter_id": f"base_weights.{key}",
                "from": round(source_weights[key], 10),
                "to": round(target_weights[key], 10),
                "delta": round(target_weights[key] - source_weights[key], 10),
                "max_single_day_delta": max_delta,
            }
            for key in source_weights
        ]
        candidates.append(
            {
                "candidate_id": (
                    f"weight_adjustment_candidate:{as_of.isoformat()}:{_slug(profile_id)}"
                ),
                "generated_at": generated_at.isoformat(),
                "mode": MODE_OBSERVE_ONLY,
                "blocked": True,
                "gate_status": gate["status"],
                "source_profile": {
                    "profile_id": "production_current",
                    "version": _string_value(production_profile.get("version")) or "missing",
                    "path": str(production_profile_path),
                    "weights": {key: round(value, 10) for key, value in source_weights.items()},
                },
                "target_profile": {
                    "profile_id": f"{profile_id}_small_step_candidate",
                    "source_shadow_profile_id": profile_id,
                    "source_shadow_profile_version": _string_value(profile.get("version")),
                    "weights": {key: round(value, 10) for key, value in target_weights.items()},
                    "total_weight": round(total_weight, 10),
                },
                "parameter_changes": changes,
                "reason_codes": _candidate_reason_codes(gate, shadow_profiles),
                "expected_effect": {
                    "summary": "小幅靠近 existing shadow profile，用于后续人工复核。",
                    "max_single_day_weight_delta": max_delta,
                    "total_weight_after": round(total_weight, 10),
                    "keeps_total_weight_balanced": abs(total_weight - target_total)
                    <= total_tolerance,
                    "uses_single_day_pnl_to_increase_weight": False,
                    "changes_core_risk_gate": False,
                    "bypasses_data_quality_gate": False,
                },
                "risk_notes": _candidate_risk_notes(gate),
                "blocked_by": blocked_by,
                "required_validations": list(required_validations),
                "production_effect": PRODUCTION_EFFECT_NONE,
            }
        )
    return candidates


def _limited_candidate(
    *,
    as_of: date,
    generated_at: datetime,
    gate: dict[str, Any],
    required_validations: list[str],
    reason_code: str,
) -> dict[str, Any]:
    blocked_by = _unique_strings([*gate["blocked_by"], reason_code, BLOCKER_MANUAL_APPROVAL])
    return {
        "candidate_id": f"weight_adjustment_candidate:{as_of.isoformat()}:limited",
        "generated_at": generated_at.isoformat(),
        "mode": MODE_OBSERVE_ONLY,
        "blocked": True,
        "gate_status": STATUS_LIMITED,
        "source_profile": {"profile_id": "missing", "weights": {}},
        "target_profile": {"profile_id": "limited_no_change", "weights": {}},
        "parameter_changes": [],
        "reason_codes": [reason_code, "limited_input"],
        "expected_effect": {
            "summary": "输入不足，仅输出 blocked 占位候选。",
            "uses_single_day_pnl_to_increase_weight": False,
            "changes_core_risk_gate": False,
            "bypasses_data_quality_gate": False,
        },
        "risk_notes": _candidate_risk_notes(gate),
        "blocked_by": blocked_by,
        "required_validations": list(required_validations),
        "production_effect": PRODUCTION_EFFECT_NONE,
    }


def _candidate_gate(
    *,
    daily_summary: dict[str, Any],
    signal_quality: dict[str, Any],
    shadow_impact: dict[str, Any],
    replay_payload: dict[str, Any],
    parameter_governance: dict[str, Any],
    production_profile: dict[str, Any],
    shadow_profiles: dict[str, Any],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    input_status = {
        "daily_decision_summary": _valid_report(daily_summary, "daily_decision_summary"),
        "paper_signal_quality": _valid_report(signal_quality, "paper_signal_quality"),
        "shadow_parameter_impact": _valid_report(shadow_impact, "shadow_parameter_impact"),
        "paper_trading_replay": _valid_report(replay_payload, "paper_trading_replay"),
        "parameter_governance": bool(parameter_governance),
        "production_profile": bool(_production_weights(production_profile)),
        "shadow_profiles": bool(_shadow_profile_records(shadow_profiles)),
    }
    if not input_status["daily_decision_summary"]:
        blockers.append(BLOCKER_MISSING_DAILY_SUMMARY)
    if not input_status["paper_signal_quality"]:
        blockers.append(BLOCKER_MISSING_PAPER_SIGNAL_QUALITY)
    if not input_status["shadow_parameter_impact"]:
        blockers.append(BLOCKER_MISSING_SHADOW_IMPACT)
    if not input_status["parameter_governance"]:
        blockers.append(BLOCKER_PARAMETER_GOVERNANCE_MISSING)
    if not input_status["production_profile"]:
        blockers.append(BLOCKER_PRODUCTION_PROFILE_MISSING)
    if not input_status["shadow_profiles"]:
        blockers.append(BLOCKER_SHADOW_PROFILE_MISSING)

    checks = _gate_checks(
        daily_summary=daily_summary,
        signal_quality=signal_quality,
        shadow_impact=shadow_impact,
        replay_payload=replay_payload,
        thresholds=thresholds,
    )
    if not checks["sample"]["passed"]:
        blockers.append(BLOCKER_INSUFFICIENT_SAMPLE)
    if not checks["data_quality"]["passed"]:
        blockers.append(BLOCKER_LOW_DATA_QUALITY)
    if not checks["synthetic_snapshot_ratio"]["passed"]:
        blockers.append(BLOCKER_SYNTHETIC_RATIO_HIGH)
    if not checks["continuous_replay"]["passed"]:
        blockers.append(BLOCKER_CONTINUOUS_REPLAY_MISSING)
    if not checks["shadow_impact"]["passed"]:
        blockers.append(BLOCKER_SHADOW_IMPACT_INSUFFICIENT)
    if not checks["paper_signal_quality"]["passed"]:
        blockers.append(BLOCKER_PAPER_SIGNAL_QUALITY_UNRELIABLE)
    blockers.append(BLOCKER_MANUAL_APPROVAL)
    unique_blockers = _unique_strings(blockers)
    missing_required = [
        code
        for code in (
            BLOCKER_MISSING_DAILY_SUMMARY,
            BLOCKER_MISSING_PAPER_SIGNAL_QUALITY,
            BLOCKER_MISSING_SHADOW_IMPACT,
            BLOCKER_PARAMETER_GOVERNANCE_MISSING,
            BLOCKER_PRODUCTION_PROFILE_MISSING,
        )
        if code in unique_blockers
    ]
    status = STATUS_LIMITED if missing_required else STATUS_BLOCKED
    if unique_blockers == [BLOCKER_MANUAL_APPROVAL]:
        status = STATUS_BLOCKED
    return {
        "status": status,
        "blocked": bool(unique_blockers),
        "blocked_by": unique_blockers,
        "missing_required_inputs": missing_required,
        "checks": checks,
        "input_status": input_status,
        "reason_explanations": {
            code: REASON_EXPLANATIONS.get(code, code) for code in unique_blockers
        },
        "explanation": _gate_explanation(status, unique_blockers),
    }


def _gate_checks(
    *,
    daily_summary: dict[str, Any],
    signal_quality: dict[str, Any],
    shadow_impact: dict[str, Any],
    replay_payload: dict[str, Any],
    thresholds: dict[str, Any],
) -> dict[str, Any]:
    signal_summary = _mapping(signal_quality.get("summary"))
    impact_summary = _mapping(shadow_impact.get("summary"))
    impact_counts = _mapping(impact_summary.get("sample_counts"))
    signal_sample = _int_value(signal_summary.get("sample_count"), default=0)
    shadow_sample = _int_value(impact_counts.get("shadow"), default=0)
    production_sample = _int_value(impact_counts.get("production"), default=0)
    min_signal_sample = _int_value(
        thresholds.get("minimum_paper_signal_sample_count"),
        default=7,
    )
    min_shadow_sample = _int_value(thresholds.get("minimum_shadow_sample_count"), default=7)
    min_production_sample = _int_value(
        thresholds.get("minimum_production_baseline_count"),
        default=7,
    )
    synthetic_ratio = _float_value(
        signal_summary.get("synthetic_snapshot_ratio"),
        default=1.0,
    )
    max_synthetic_ratio = _float_value(
        thresholds.get("maximum_synthetic_snapshot_ratio"),
        default=0.25,
    )
    daily_data_gate = _mapping(daily_summary.get("data_gate"))
    data_gate_status = _string_value(daily_data_gate.get("status"))
    signal_status = _string_value(signal_quality.get("evaluation_status"))
    impact_status = _string_value(shadow_impact.get("impact_status"))
    continuous_from_replay = _is_continuous_replay(replay_payload)
    continuous_from_impact = bool(_mapping(shadow_impact.get("continuous_replay")).get("available"))
    sample_passed = (
        signal_sample >= min_signal_sample
        and shadow_sample >= min_shadow_sample
        and production_sample >= min_production_sample
    )
    data_quality_passed = (
        data_gate_status == "PASS"
        and signal_status not in {"LOW_DATA_QUALITY", "UNRELIABLE"}
        and impact_status not in {"LOW_DATA_QUALITY", "SHADOW_UNRELIABLE"}
        and synthetic_ratio <= max_synthetic_ratio
    )
    signal_passed = signal_status in {"OBSERVE_ONLY", "PROMISING_BUT_LIMITED"}
    impact_passed = impact_status in {"OBSERVE_ONLY", "SHADOW_PROMISING_BUT_LIMITED"}
    return {
        "sample": {
            "passed": sample_passed,
            "paper_signal_sample_count": signal_sample,
            "minimum_paper_signal_sample_count": min_signal_sample,
            "shadow_sample_count": shadow_sample,
            "minimum_shadow_sample_count": min_shadow_sample,
            "production_baseline_count": production_sample,
            "minimum_production_baseline_count": min_production_sample,
        },
        "data_quality": {
            "passed": data_quality_passed,
            "daily_data_gate_status": data_gate_status or "missing",
            "paper_signal_quality_status": signal_status or "missing",
            "shadow_impact_status": impact_status or "missing",
        },
        "synthetic_snapshot_ratio": {
            "passed": synthetic_ratio <= max_synthetic_ratio,
            "actual": synthetic_ratio,
            "maximum": max_synthetic_ratio,
        },
        "continuous_replay": {
            "passed": continuous_from_replay or continuous_from_impact,
            "from_replay_artifact": continuous_from_replay,
            "from_shadow_impact": continuous_from_impact,
        },
        "shadow_impact": {
            "passed": impact_passed,
            "impact_status": impact_status or "missing",
        },
        "paper_signal_quality": {
            "passed": signal_passed,
            "evaluation_status": signal_status or "missing",
        },
        "manual_approval": {
            "passed": False,
            "required": True,
        },
    }


def _candidate_reason_codes(
    gate: dict[str, Any],
    shadow_profiles: list[dict[str, Any]],
) -> list[str]:
    reason_codes = [
        "small_step_weight_adjustment",
        "sum_to_one_preserved",
        "data_quality_gate_required",
        "manual_review_required",
    ]
    if shadow_profiles:
        reason_codes.append("existing_shadow_profile_reference")
    if gate["status"] == STATUS_LIMITED:
        reason_codes.append("limited_input")
    return reason_codes


def _candidate_risk_notes(gate: dict[str, Any]) -> list[str]:
    notes = [
        "候选只用于观察，不改变 production 权重。",
        "不删除 core risk gate，不绕过 data quality gate。",
        "不根据单日 paper PnL 调高权重。",
    ]
    for blocker in _strings(gate.get("blocked_by")):
        explanation = REASON_EXPLANATIONS.get(blocker)
        if explanation:
            notes.append(explanation)
    return _unique_strings(notes)


def _small_step_weights(
    *,
    source_weights: dict[str, float],
    target_weights: dict[str, float],
    max_delta: float,
) -> dict[str, float]:
    positive: dict[str, float] = {}
    negative: dict[str, float] = {}
    for key, source_value in source_weights.items():
        raw_delta = target_weights.get(key, source_value) - source_value
        if raw_delta > 0:
            positive[key] = min(raw_delta, max_delta)
        elif raw_delta < 0:
            negative[key] = max(raw_delta, -max_delta)
    increase_total = sum(positive.values())
    decrease_total = -sum(negative.values())
    transfer = min(increase_total, decrease_total)
    deltas = {key: 0.0 for key in source_weights}
    if transfer > 0 and increase_total > 0 and decrease_total > 0:
        positive_scale = transfer / increase_total
        negative_scale = transfer / decrease_total
        for key, value in positive.items():
            deltas[key] = value * positive_scale
        for key, value in negative.items():
            deltas[key] = value * negative_scale
    return {key: source_weights[key] + deltas[key] for key in source_weights}


def _production_weights(profile: dict[str, Any]) -> dict[str, float]:
    return _weights_from_mapping(_mapping(profile.get("base_weights")))


def _shadow_profile_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in _list_mappings(payload.get("profiles")):
        weights = _weights_from_mapping(_mapping(record.get("target_weights")))
        if not weights:
            continue
        records.append(
            {
                "profile_id": _string_value(record.get("profile_id")) or "unknown_shadow_profile",
                "version": _string_value(record.get("version")) or "missing",
                "target_weights": weights,
                "metadata": _mapping(record.get("metadata")),
            }
        )
    return records


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _valid_report(payload: dict[str, Any], report_type: str) -> bool:
    return bool(payload) and _string_value(payload.get("report_type")) == report_type


def _is_continuous_replay(payload: dict[str, Any]) -> bool:
    return (
        _string_value(payload.get("report_type")) == "paper_trading_replay"
        and _string_value(payload.get("replay_mode")) == "continuous_portfolio"
        and bool(payload.get("portfolio_carry_forward"))
    )


def _select_latest_replay_path(reports_dir: Path, as_of: date) -> Path | None:
    selected: tuple[date, Path] | None = None
    for path in reports_dir.glob("paper_trading_replay_*.json"):
        match = re.search(
            r"paper_trading_replay_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", path.name
        )
        if not match:
            continue
        end = date.fromisoformat(match.group(2))
        if end > as_of:
            continue
        if selected is None or end > selected[0]:
            selected = (end, path)
    return selected[1] if selected else None


def _policy_report(policy: dict[str, Any], policy_path: Path) -> dict[str, Any]:
    return {
        "policy_id": _string_value(policy.get("policy_id")) or "weight_adjustment_candidate_policy",
        "version": policy.get("version", "missing"),
        "status": _string_value(policy.get("status")) or "missing",
        "owner": _string_value(policy.get("owner")) or "missing",
        "production_effect": _string_value(policy.get("production_effect")) or "none",
        "path": str(policy_path),
        "rationale": _string_value(policy.get("rationale")),
        "intended_effect": _string_value(policy.get("intended_effect")),
        "validation_evidence": _string_value(policy.get("validation_evidence")),
        "review_condition": _string_value(policy.get("review_condition")),
        "thresholds": dict(_mapping(policy.get("thresholds"))),
    }


def _artifact_record(
    path: Path | None,
    base_dir: Path,
    *,
    expected_report_type: str | None = None,
    payload: dict[str, Any] | None = None,
    optional: bool = False,
) -> dict[str, Any]:
    if path is None:
        return {
            "path": "",
            "exists": False,
            "optional": optional,
            "href": "",
            "checksum_sha256": "",
            "valid": optional,
        }
    exists = path.exists()
    checksum = _sha256(path) if exists and path.is_file() else ""
    actual_type = _string_value((payload or {}).get("report_type"))
    valid = exists and (expected_report_type is None or actual_type == expected_report_type)
    if optional and not exists:
        valid = True
    return {
        "path": str(path),
        "exists": exists,
        "optional": optional,
        "href": _report_href(path, base_dir),
        "checksum_sha256": checksum,
        "expected_report_type": expected_report_type or "",
        "actual_report_type": actual_type,
        "valid": valid,
    }


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = safe_load_yaml_path(path) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _unique_strings(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_value(value: Any, *, default: float) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _int_value(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return slug.strip("_") or "candidate"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_href(path: Path, base_dir: Path) -> str:
    try:
        rel = path.relative_to(base_dir)
    except ValueError:
        return path.as_posix()
    return rel.as_posix()


def _gate_explanation(status: str, blockers: list[str]) -> str:
    if status == STATUS_LIMITED:
        return "关键输入缺失，仅输出 blocked candidate 占位记录。"
    if blockers:
        return "候选已被 gate 阻断：" + ", ".join(blockers)
    return "候选仍为 observe-only，不具备生产效果。"
