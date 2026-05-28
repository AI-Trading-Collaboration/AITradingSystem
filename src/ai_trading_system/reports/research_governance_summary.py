from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "research_governance_summary"
PRODUCTION_EFFECT = "none"

GROUP_PRODUCTION_ACTIVE = "Production-active"
GROUP_APPROVED_INACTIVE = "Approved but inactive"
GROUP_SHADOW_OBSERVE = "Shadow observe-only"
GROUP_CANDIDATE_RESEARCH = "Candidate / research-only"
GROUP_BLOCKED = "Blocked / insufficient data"
GROUP_ROLLBACK_WARNING = "Rollback / warning"

GROUP_ORDER = (
    GROUP_PRODUCTION_ACTIVE,
    GROUP_APPROVED_INACTIVE,
    GROUP_SHADOW_OBSERVE,
    GROUP_CANDIDATE_RESEARCH,
    GROUP_BLOCKED,
    GROUP_ROLLBACK_WARNING,
)

BLOCKED_STATUS_TOKENS = (
    "BLOCKED",
    "INSUFFICIENT",
    "MISSING",
    "LOW_DATA_QUALITY",
    "UNAVAILABLE",
    "SOURCE_UNAVAILABLE",
    "FAILED",
    "FAIL",
    "NOT_PROMOTABLE",
)
ROLLBACK_STATUS_TOKENS = ("ROLLBACK", "WARNING")


@dataclass(frozen=True)
class GovernanceArtifactSpec:
    artifact_id: str
    title: str
    source_task: str
    directory: Path
    filename_prefix: str
    filename_suffix: str
    default_group: str
    expected: bool = True


def default_research_governance_summary_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_governance_summary_{as_of.isoformat()}.json"


def default_research_governance_summary_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_governance_summary_{as_of.isoformat()}.md"


def default_research_governance_specs(
    project_root: Path = PROJECT_ROOT,
) -> tuple[GovernanceArtifactSpec, ...]:
    reports_dir = project_root / "outputs" / "reports"
    backtests_dir = project_root / "outputs" / "backtests"
    return (
        GovernanceArtifactSpec(
            "parameter_governance",
            "Parameter Governance",
            "aits feedback evaluate-parameter-governance",
            reports_dir,
            "parameter_governance_",
            ".json",
            GROUP_PRODUCTION_ACTIVE,
        ),
        GovernanceArtifactSpec(
            "backtest_daily",
            "Daily Score Backtest",
            "aits backtest",
            backtests_dir,
            "backtest_",
            ".md",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "backtest_robustness",
            "Backtest Robustness",
            "aits backtest --robustness",
            backtests_dir,
            "backtest_robustness_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "gate_event_attribution",
            "Gate Event Attribution",
            "aits backtest gate-event-attribution",
            backtests_dir,
            "gate_event_attribution_",
            ".md",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_evaluation",
            "SEC PIT Evaluation",
            "aits sec-pit evaluate",
            project_root / "outputs" / "sec_pit_evaluation",
            "sec_pit_evaluation_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_baseline_comparison",
            "SEC PIT Baseline Comparison",
            "aits sec-pit compare-baseline",
            project_root / "outputs" / "sec_pit_baseline_comparison",
            "sec_pit_baseline_comparison_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_real_run_diagnostics",
            "SEC PIT Real Run Diagnostics",
            "aits sec-pit diagnose-run",
            project_root / "outputs" / "sec_pit_diagnostics",
            "sec_pit_real_run_diagnostics_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_candidate_review",
            "SEC PIT Candidate Review",
            "aits sec-pit review-candidates",
            project_root / "outputs" / "sec_pit_candidate_review",
            "sec_pit_candidate_review_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "sec_pit_baseline_coverage",
            "SEC PIT Baseline Coverage",
            "aits sec-pit audit-baseline-coverage",
            project_root / "outputs" / "sec_pit_baseline_coverage",
            "sec_pit_baseline_coverage_summary_",
            ".json",
            GROUP_BLOCKED,
        ),
        GovernanceArtifactSpec(
            "sec_pit_shadow_observe",
            "SEC PIT Shadow Observe",
            "aits sec-pit shadow-observe",
            project_root / "outputs" / "sec_pit_shadow_observe",
            "sec_pit_shadow_observe_summary_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "sec_pit_shadow_monitor",
            "SEC PIT Shadow Monitor",
            "aits sec-pit shadow-monitor",
            project_root / "outputs" / "sec_pit_shadow_monitor",
            "sec_pit_shadow_monitor_summary_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "shadow_parameter_impact",
            "Shadow Parameter Impact",
            "python scripts/run_shadow_parameter_impact.py",
            reports_dir,
            "shadow_parameter_impact_",
            ".json",
            GROUP_SHADOW_OBSERVE,
        ),
        GovernanceArtifactSpec(
            "weight_adjustment_candidates",
            "Weight Adjustment Candidates",
            "python scripts/run_weight_adjustment_candidates.py",
            reports_dir,
            "weight_adjustment_candidates_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "weight_candidate_evaluation",
            "Weight Candidate Evaluation",
            "python scripts/run_weight_candidate_evaluation.py",
            reports_dir,
            "weight_candidate_evaluation_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "weight_promotion_gate",
            "Weight Promotion Gate",
            "python scripts/run_weight_promotion_gate.py",
            reports_dir,
            "weight_promotion_gate_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
        GovernanceArtifactSpec(
            "daily_weight_adjustment_summary",
            "Daily Weight Adjustment Summary",
            "python scripts/run_daily_weight_adjustment.py",
            reports_dir,
            "daily_weight_adjustment_summary_",
            ".json",
            GROUP_CANDIDATE_RESEARCH,
        ),
    )


def build_research_governance_summary_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    specs: tuple[GovernanceArtifactSpec, ...] | None = None,
) -> dict[str, Any]:
    resolved_specs = specs or default_research_governance_specs(project_root)
    cards = [_card_from_spec(spec, as_of) for spec in resolved_specs]
    groups = [
        {
            "group": group,
            "count": len([card for card in cards if card["group"] == group]),
            "cards": [card for card in cards if card["group"] == group],
        }
        for group in GROUP_ORDER
    ]
    missing = [card for card in cards if card["availability"] == "MISSING"]
    limited = [card for card in cards if card["availability"] == "LIMITED"]
    warning_cards = [
        card
        for card in cards
        if card["group"] == GROUP_ROLLBACK_WARNING or card["production_effect_risk"] is True
    ]
    status = "PASS"
    if missing or limited or warning_cards:
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": {
            "regime_id": "ai_after_chatgpt",
            "start_date": "2022-12-01",
            "anchor_date": "2022-11-30",
        },
        "summary": {
            "card_count": len(cards),
            "missing_count": len(missing),
            "limited_count": len(limited),
            "warning_count": len(warning_cards),
            "production_effect_risk_count": len(
                [card for card in cards if card["production_effect_risk"] is True]
            ),
            "manual_review_required_count": len(
                [card for card in cards if card["manual_review_required"] is True]
            ),
            "groups": {item["group"]: item["count"] for item in groups},
        },
        "groups": groups,
        "cards": cards,
        "warnings": _warnings(cards),
        "methodology": {
            "collector_mode": "read_existing_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
            "group_order": list(GROUP_ORDER),
        },
    }


def write_research_governance_summary_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_research_governance_summary_report(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_research_governance_summary_markdown(payload), encoding="utf-8")
    return output_path


def render_research_governance_summary_markdown(payload: Mapping[str, Any]) -> str:
    as_of = _text(payload.get("as_of"), "UNKNOWN")
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Governance Summary {as_of}",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        "- 市场阶段：`ai_after_chatgpt`",
        "- production_effect=none；本报告只读汇总已有 research / shadow / governance artifact，"
        "不运行上游任务、不修改 production scoring、weights、position gates 或 trading 行为。",
        f"- 卡片数：{_text(summary.get('card_count'), '0')}；"
        f"缺失：{_text(summary.get('missing_count'), '0')}；"
        f"限制：{_text(summary.get('limited_count'), '0')}；"
        f"警告：{_text(summary.get('warning_count'), '0')}",
        "",
    ]
    for group in _records(payload.get("groups")):
        cards = _records(group.get("cards"))
        if not cards:
            continue
        lines.extend(
            [
                f"## {group['group']}",
                "",
                "| Artifact | Status | Candidate | Production effect | "
                "Manual review | Next action | Source |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for card in cards:
            lines.append(
                "| "
                f"{_text(card.get('title'))} | "
                f"{_text(card.get('artifact_status'), 'UNKNOWN')} | "
                f"`{_text(card.get('candidate_id'), 'UNKNOWN')}` | "
                f"`{_text(card.get('production_effect'), 'UNKNOWN')}` | "
                f"{_text(card.get('manual_review_required'), 'UNKNOWN')} | "
                f"{_text(card.get('next_action'), 'review_source_artifact')} | "
                f"`{Path(_text(card.get('path'))).name if card.get('path') else 'MISSING'}` |"
            )
        lines.append("")
    lines.extend(
        [
            "## 审计边界",
            "",
            "- `Production-active` 只表示该 artifact 描述生产参数或生产 profile 状态；"
            "本报告本身仍固定 `production_effect=none`。",
            "- `Shadow observe-only`、`Candidate / research-only`、`Blocked / insufficient data` "
            "均不得被解读为 production promotion。",
            "- 缺少 artifact 时只标记 `MISSING`，不补造 research 结论。",
        ]
    )
    return "\n".join(lines)


def _card_from_spec(spec: GovernanceArtifactSpec, as_of: date) -> dict[str, Any]:
    path = _latest_artifact_path(spec.directory, spec.filename_prefix, spec.filename_suffix, as_of)
    exists = path.exists()
    payload = _read_json_or_empty(path) if exists and spec.filename_suffix == ".json" else {}
    availability = "AVAILABLE" if exists else "MISSING"
    if exists and spec.filename_suffix == ".json" and not payload:
        availability = "LIMITED"
    status = _artifact_status(payload, exists=exists)
    production_effect = _production_effect(payload, spec)
    manual_review_required = _manual_review_required(payload, spec)
    candidate_id = _candidate_id(payload, spec)
    group = _card_group(
        default_group=spec.default_group,
        status=status,
        payload=payload,
        exists=exists,
    )
    return {
        "card_id": spec.artifact_id,
        "title": spec.title,
        "group": group,
        "source_task": spec.source_task,
        "path": str(path),
        "exists": exists,
        "availability": availability,
        "artifact_status": status,
        "artifact_date": (
            parsed_date.isoformat()
            if (parsed_date := _date_from_path(path, spec.filename_prefix)) is not None
            else None
        ),
        "candidate_id": candidate_id,
        "production_effect": production_effect,
        "production_effect_risk": production_effect not in {"", "none", "read_only", "advisory"},
        "manual_review_required": manual_review_required,
        "evidence_summary": _evidence_summary(payload, spec, exists=exists),
        "next_review_date": _first_text(
            payload,
            "next_review_date",
            "next_review_after",
            "review_after",
        ),
        "next_action": _next_action(payload, status=status, exists=exists),
        "risk_flags": _risk_flags(payload, status=status, production_effect=production_effect),
        "source_artifacts": _source_artifacts(payload),
    }


def _latest_artifact_path(directory: Path, prefix: str, suffix: str, as_of: date) -> Path:
    default_path = directory / f"{prefix}{as_of.isoformat()}{suffix}"
    if not directory.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in directory.glob(f"{prefix}*{suffix}"):
        if (
            prefix == "backtest_"
            and suffix == ".md"
            and not re.fullmatch(
                r"backtest_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.md",
                path.name,
            )
        ):
            continue
        parsed = _date_from_path(path, prefix)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _date_from_path(path: Path, prefix: str) -> date | None:
    raw = path.stem.removeprefix(prefix)
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", raw)
    if not matches:
        return None
    try:
        return date.fromisoformat(matches[-1])
    except ValueError:
        return None


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _artifact_status(payload: Mapping[str, Any], *, exists: bool) -> str:
    if not exists:
        return "MISSING"
    for key in (
        "status",
        "gate_status",
        "evaluation_status",
        "promotion_gate_status",
        "impact_status",
        "shadow_status",
        "monitor_status",
        "monitoring_status",
        "recommendation",
        "readiness",
    ):
        value = _text(payload.get(key))
        if value:
            return value
    return "AVAILABLE"


def _production_effect(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> str:
    if not payload:
        return "none" if spec.filename_suffix != ".json" else ""
    safety = _mapping(payload.get("safety"))
    execution = _mapping(payload.get("execution_boundary"))
    return _first_non_empty(
        payload.get("production_effect"),
        safety.get("production_effect"),
        execution.get("production_effect"),
        "none",
    )


def _manual_review_required(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> bool:
    if not payload:
        return spec.default_group in {
            GROUP_CANDIDATE_RESEARCH,
            GROUP_SHADOW_OBSERVE,
            GROUP_BLOCKED,
        }
    for key in ("manual_review_required", "manual_review_only", "requires_manual_review"):
        if key in payload:
            return bool(payload.get(key))
    if _records(payload.get("required_manual_review_items")):
        return True
    return spec.default_group in {GROUP_CANDIDATE_RESEARCH, GROUP_SHADOW_OBSERVE}


def _candidate_id(payload: Mapping[str, Any], spec: GovernanceArtifactSpec) -> str:
    for key in (
        "candidate_id",
        "top_candidate_id",
        "selected_trial_id",
        "candidate_feature",
        "feature",
        "profile_id",
    ):
        value = _text(payload.get(key))
        if value:
            return value
    candidate = _mapping(payload.get("candidate"))
    profile = _mapping(payload.get("target_profile"))
    return _first_non_empty(
        candidate.get("candidate_id"),
        profile.get("profile_id"),
        spec.artifact_id,
    )


def _card_group(
    *,
    default_group: str,
    status: str,
    payload: Mapping[str, Any],
    exists: bool,
) -> str:
    upper_status = status.upper()
    if not exists or any(token in upper_status for token in BLOCKED_STATUS_TOKENS):
        return GROUP_BLOCKED
    if any(token in upper_status for token in ROLLBACK_STATUS_TOKENS):
        return GROUP_ROLLBACK_WARNING
    recommendation = _text(payload.get("recommendation")).upper()
    if "ROLLBACK" in recommendation or "WARNING" in recommendation:
        return GROUP_ROLLBACK_WARNING
    return default_group


def _evidence_summary(
    payload: Mapping[str, Any],
    spec: GovernanceArtifactSpec,
    *,
    exists: bool,
) -> str:
    if not exists:
        return "artifact missing"
    for key in (
        "summary",
        "evidence_summary",
        "recommendation",
        "main_blocked_by",
        "main_blocked_reason",
        "monitoring_status_reason",
        "status_reason",
        "primary",
        "readiness",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, Mapping):
            compact = _compact_mapping(value)
            if compact:
                return compact
    return f"{spec.title} artifact available"


def _next_action(payload: Mapping[str, Any], *, status: str, exists: bool) -> str:
    if not exists:
        return "generate_or_review_source_artifact"
    for key in ("next_action", "recommended_next_action", "recommendation", "next_step"):
        value = _text(payload.get(key))
        if value:
            return value
    required = _records(payload.get("required_manual_review_items"))
    if required:
        return "manual_review_required"
    if any(token in status.upper() for token in BLOCKED_STATUS_TOKENS):
        return "resolve_blocker_before_promotion"
    return "review_source_artifact"


def _risk_flags(
    payload: Mapping[str, Any],
    *,
    status: str,
    production_effect: str,
) -> list[str]:
    flags: list[str] = []
    if production_effect not in {"", "none", "read_only", "advisory"}:
        flags.append("production_effect_not_none")
    if any(token in status.upper() for token in BLOCKED_STATUS_TOKENS):
        flags.append("blocked_or_insufficient_data")
    if any(token in status.upper() for token in ROLLBACK_STATUS_TOKENS):
        flags.append("rollback_or_warning")
    for key in ("blocked_by", "warnings", "risk_flags"):
        value = payload.get(key)
        if isinstance(value, list):
            flags.extend(str(item) for item in value if item not in {None, ""})
        elif isinstance(value, str) and value:
            flags.append(value)
    return list(dict.fromkeys(flags))


def _source_artifacts(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("source_artifacts") or payload.get("source_inputs") or {}
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    if isinstance(raw, Mapping):
        return [
            {"artifact_id": str(key), **dict(value)}
            for key, value in raw.items()
            if isinstance(value, Mapping)
        ]
    return []


def _warnings(cards: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for card in cards:
        if card["availability"] == "MISSING":
            warnings.append(f"{card['card_id']}_missing:{card['path']}")
        if card["availability"] == "LIMITED":
            warnings.append(f"{card['card_id']}_limited:{card['path']}")
        if card["group"] == GROUP_ROLLBACK_WARNING:
            warnings.append(f"{card['card_id']}_rollback_or_warning:{card['artifact_status']}")
        if card["production_effect_risk"] is True:
            warnings.append(
                f"{card['card_id']}_production_effect_not_none:{card['production_effect']}"
            )
    return warnings


def _first_text(payload: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(payload.get(key))
        if value:
            return value
    return ""


def _first_non_empty(*values: object) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _compact_mapping(value: Mapping[str, Any]) -> str:
    items = []
    for key, raw in value.items():
        if isinstance(raw, (dict, list)):
            continue
        text = _text(raw)
        if text:
            items.append(f"{key}={text}")
        if len(items) >= 4:
            break
    return "；".join(items)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)
