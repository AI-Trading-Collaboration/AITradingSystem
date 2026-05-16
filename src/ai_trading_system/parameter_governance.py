from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.parameter_candidates import (
    DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
)

SCHEMA_VERSION = 1
DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "parameter_governance.yaml"
)
DEFAULT_PARAMETER_GOVERNANCE_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"

SOURCE_LEVELS = frozenset(
    {
        "owner_policy",
        "empirical_calibrated",
        "pilot_prior",
        "validation_shadow",
        "temporary_baseline",
        "invariant",
    }
)
ALLOWED_ACTIONS = frozenset(
    {
        "KEEP_CURRENT",
        "COLLECT_MORE_EVIDENCE",
        "PREPARE_FORWARD_SHADOW",
        "OWNER_DECISION_REQUIRED",
        "BLOCKED_BY_DATA",
        "BLOCKED_BY_POLICY",
    }
)
CONFIG_ACTIONS = frozenset(
    {
        "keep_current",
        "collect_more_evidence",
        "prepare_forward_shadow",
        "owner_decision_required",
        "blocked_by_data",
        "blocked_by_policy",
    }
)


@dataclass(frozen=True)
class ParameterGovernanceEntry:
    parameter_id: str
    surface: str
    config_path: Path
    source_level: str
    owner: str
    status: str
    rationale: str
    validation_evidence: tuple[str, ...]
    review_after: str
    exit_condition: str
    production_effect: str
    candidate_categories: tuple[str, ...]
    allowed_actions: tuple[str, ...]
    requires_owner_quantitative_input_for_production: bool
    allows_shadow_without_owner_quantitative_input: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "parameter_id": self.parameter_id,
            "surface": self.surface,
            "config_path": str(self.config_path),
            "source_level": self.source_level,
            "owner": self.owner,
            "status": self.status,
            "rationale": self.rationale,
            "validation_evidence": list(self.validation_evidence),
            "review_after": self.review_after,
            "exit_condition": self.exit_condition,
            "production_effect": self.production_effect,
            "candidate_categories": list(self.candidate_categories),
            "allowed_actions": list(self.allowed_actions),
            "requires_owner_quantitative_input_for_production": (
                self.requires_owner_quantitative_input_for_production
            ),
            "allows_shadow_without_owner_quantitative_input": (
                self.allows_shadow_without_owner_quantitative_input
            ),
        }


@dataclass(frozen=True)
class ParameterGovernanceManifest:
    version: str
    status: str
    owner: str
    market_regime_id: str
    owner_quantitative_input_status: str
    production_effect: str
    rationale: str
    validation: str
    review_after: str
    actions: dict[str, str]
    parameters: tuple[ParameterGovernanceEntry, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "owner": self.owner,
            "market_regime_id": self.market_regime_id,
            "owner_quantitative_input_status": self.owner_quantitative_input_status,
            "production_effect": self.production_effect,
            "rationale": self.rationale,
            "validation": self.validation,
            "review_after": self.review_after,
            "actions": self.actions,
            "parameters": [entry.to_dict() for entry in self.parameters],
        }


@dataclass(frozen=True)
class ParameterGovernanceParameterReport:
    parameter_id: str
    surface: str
    config_path: Path
    config_exists: bool
    config_sha256: str | None
    source_level: str
    manifest_status: str
    owner: str
    owner_quantitative_input_status: str
    candidate_count: int
    candidate_status_counts: dict[str, int]
    veto_reason_counts: dict[str, int]
    action: str
    action_reason: str
    constraints: tuple[str, ...]
    production_effect: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "parameter_id": self.parameter_id,
            "surface": self.surface,
            "config_path": str(self.config_path),
            "config_exists": self.config_exists,
            "config_sha256": self.config_sha256,
            "source_level": self.source_level,
            "manifest_status": self.manifest_status,
            "owner": self.owner,
            "owner_quantitative_input_status": self.owner_quantitative_input_status,
            "candidate_count": self.candidate_count,
            "candidate_status_counts": self.candidate_status_counts,
            "veto_reason_counts": self.veto_reason_counts,
            "action": self.action,
            "action_reason": self.action_reason,
            "constraints": list(self.constraints),
            "production_effect": self.production_effect,
        }


@dataclass(frozen=True)
class ParameterGovernanceReport:
    as_of: date
    generated_at: datetime
    manifest_path: Path
    candidate_ledger_path: Path
    manifest: ParameterGovernanceManifest
    candidate_ledger_status: str
    candidate_evaluation_mode: str
    candidate_count: int
    trial_count: int
    action_counts: dict[str, int]
    parameters: tuple[ParameterGovernanceParameterReport, ...]
    warnings: tuple[str, ...]

    @property
    def status(self) -> str:
        if self._blocking_policy_count:
            return "FAIL"
        if self.warnings or any(
            item.action in {
                "BLOCKED_BY_DATA",
                "BLOCKED_BY_POLICY",
                "OWNER_DECISION_REQUIRED",
            }
            for item in self.parameters
        ):
            return "PASS_WITH_LIMITATIONS"
        return "PASS"

    @property
    def production_effect(self) -> str:
        return "none"

    @property
    def _blocking_policy_count(self) -> int:
        return sum(1 for item in self.parameters if item.action == "BLOCKED_BY_POLICY")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "parameter_governance",
            "status": self.status,
            "production_effect": self.production_effect,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "manifest_path": str(self.manifest_path),
            "candidate_ledger_path": str(self.candidate_ledger_path),
            "manifest": self.manifest.to_dict(),
            "manifest_version": self.manifest.version,
            "manifest_status": self.manifest.status,
            "owner_quantitative_input_status": (
                self.manifest.owner_quantitative_input_status
            ),
            "candidate_ledger_status": self.candidate_ledger_status,
            "candidate_evaluation_mode": self.candidate_evaluation_mode,
            "candidate_count": self.candidate_count,
            "trial_count": self.trial_count,
            "parameter_count": len(self.parameters),
            "action_counts": self.action_counts,
            "warnings": list(self.warnings),
            "parameters": [item.to_dict() for item in self.parameters],
        }


def default_parameter_governance_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"parameter_governance_{as_of.isoformat()}.md"


def default_parameter_governance_summary_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"parameter_governance_{as_of.isoformat()}.json"


def load_parameter_governance_manifest(
    path: Path = DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
) -> ParameterGovernanceManifest:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"parameter governance manifest must be a mapping: {path}")
    actions = _string_mapping(raw, "actions")
    unknown_actions = set(actions) - CONFIG_ACTIONS
    if unknown_actions:
        raise ValueError(
            "parameter governance manifest has unknown actions: "
            + ", ".join(sorted(unknown_actions))
        )
    parameters = _entries(raw, path)
    manifest = ParameterGovernanceManifest(
        version=_required_string(raw, "version"),
        status=_required_string(raw, "status"),
        owner=_required_string(raw, "owner"),
        market_regime_id=str(raw.get("market_regime_id") or "ai_after_chatgpt"),
        owner_quantitative_input_status=_required_string(
            raw,
            "owner_quantitative_input_status",
        ),
        production_effect=_required_string(raw, "production_effect"),
        rationale=_required_string(raw, "rationale"),
        validation=_required_string(raw, "validation"),
        review_after=_required_string(raw, "review_after"),
        actions=actions,
        parameters=parameters,
    )
    _validate_manifest(manifest, path)
    return manifest


def build_parameter_governance_report(
    *,
    as_of: date,
    manifest_path: Path = DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
    candidate_ledger_path: Path = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    generated_at: datetime | None = None,
) -> ParameterGovernanceReport:
    manifest = load_parameter_governance_manifest(manifest_path)
    generated = generated_at or datetime.now(tz=UTC)
    candidate_payload, candidate_warnings = _load_candidate_ledger(candidate_ledger_path)
    candidates = _candidate_items(candidate_payload)
    candidate_evaluation_mode = str(candidate_payload.get("evaluation_mode") or "strict")
    parameters = tuple(
        _parameter_report(
            entry,
            manifest=manifest,
            candidates=candidates,
            candidate_evaluation_mode=candidate_evaluation_mode,
        )
        for entry in manifest.parameters
    )
    action_counts: dict[str, int] = {}
    for parameter in parameters:
        action_counts[parameter.action] = action_counts.get(parameter.action, 0) + 1
    warnings = list(candidate_warnings)
    if manifest.owner_quantitative_input_status != "available":
        warnings.append(
            "owner 暂无可量化配置输入；本报告只能给出候选治理动作，不能写生产参数。"
        )
    if not candidates:
        warnings.append("parameter candidate ledger 没有候选记录；参数建议只能保持观察。")
    if candidate_evaluation_mode == "flow_validation":
        warnings.append(
            "parameter candidate ledger 使用 flow_validation 放宽门禁；"
            "治理建议仅用于验证 shadow 接线，不可进入 owner approval 或 production。"
        )
    return ParameterGovernanceReport(
        as_of=as_of,
        generated_at=generated,
        manifest_path=manifest_path,
        candidate_ledger_path=candidate_ledger_path,
        manifest=manifest,
        candidate_ledger_status=str(candidate_payload.get("status") or "NOT_CONNECTED"),
        candidate_evaluation_mode=candidate_evaluation_mode,
        candidate_count=int(candidate_payload.get("candidate_count") or len(candidates)),
        trial_count=int(candidate_payload.get("trial_count") or 0),
        action_counts=dict(sorted(action_counts.items())),
        parameters=parameters,
        warnings=tuple(dict.fromkeys(warnings)),
    )


def render_parameter_governance_report(report: ParameterGovernanceReport) -> str:
    lines = [
        "# 参数配置治理报告",
        "",
        f"- 状态：{report.status}",
        "- production_effect：none",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 复核日期：{report.as_of.isoformat()}",
        f"- Manifest：`{report.manifest_path}`",
        f"- Manifest version：{report.manifest.version}（status={report.manifest.status}）",
        f"- 市场阶段：{report.manifest.market_regime_id}",
        f"- Owner quantitative input：{report.manifest.owner_quantitative_input_status}",
        f"- Candidate ledger：`{report.candidate_ledger_path}`",
        f"- Candidate ledger status：{report.candidate_ledger_status}",
        f"- Candidate evaluation mode：{report.candidate_evaluation_mode}",
        f"- Trial / candidate：{report.trial_count} / {report.candidate_count}",
        f"- Action 分布：{_format_counts(report.action_counts)}",
        "",
        "## 方法边界",
        "",
        "- 本报告只读评估参数治理状态，不修改 scoring、backtest、sample policy、"
        "overlay 或 rule card。",
        "- owner 暂缺量化输入时，不把任何建议写成生产参数；"
        "可推进的也只能是 candidate/shadow 准备。",
        "- 所有 production 参数变化仍需要 replay、forward shadow、owner approval 和回滚条件。",
        "",
        "## 参数面",
        "",
        "| 参数面 | Source level | 配置 | Candidate | Action | 原因 | 约束 |",
        "|---|---|---|---:|---|---|---|",
    ]
    for item in report.parameters:
        lines.append(_parameter_row(item))
    lines.extend(
        [
            "",
            "## 阻断与下一步",
            "",
        ]
    )
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 当前没有额外 warning；仍保持 production_effect=none。")
    lines.append(_next_action(report))
    return "\n".join(lines).rstrip() + "\n"


def write_parameter_governance_report(
    report: ParameterGovernanceReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_parameter_governance_report(report), encoding="utf-8")
    return output_path


def write_parameter_governance_summary(
    report: ParameterGovernanceReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _entries(
    raw: dict[str, Any],
    manifest_path: Path,
) -> tuple[ParameterGovernanceEntry, ...]:
    raw_entries = raw.get("parameters")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError(f"parameter governance manifest requires parameters: {manifest_path}")
    entries = tuple(_entry(item, manifest_path) for item in raw_entries)
    duplicate_ids = _duplicates(entry.parameter_id for entry in entries)
    if duplicate_ids:
        raise ValueError(
            "parameter governance parameter_id must be unique: "
            + ", ".join(duplicate_ids)
        )
    return entries


def _entry(
    raw: Any,
    manifest_path: Path,
) -> ParameterGovernanceEntry:
    if not isinstance(raw, dict):
        raise ValueError(f"parameter governance entry must be a mapping: {manifest_path}")
    source_level = _required_string(raw, "source_level")
    if source_level not in SOURCE_LEVELS:
        raise ValueError(f"unknown source_level for parameter governance: {source_level}")
    allowed_actions = _string_tuple(raw, "allowed_actions")
    unknown_actions = {item for item in allowed_actions if item not in CONFIG_ACTIONS}
    if unknown_actions:
        raise ValueError(
            "parameter governance entry has unknown allowed_actions: "
            + ", ".join(sorted(unknown_actions))
        )
    return ParameterGovernanceEntry(
        parameter_id=_required_string(raw, "parameter_id"),
        surface=_required_string(raw, "surface"),
        config_path=_project_path(_required_string(raw, "config_path")),
        source_level=source_level,
        owner=_required_string(raw, "owner"),
        status=_required_string(raw, "status"),
        rationale=_required_string(raw, "rationale"),
        validation_evidence=_string_tuple(raw, "validation_evidence"),
        review_after=_required_string(raw, "review_after"),
        exit_condition=_required_string(raw, "exit_condition"),
        production_effect=_required_string(raw, "production_effect"),
        candidate_categories=_string_tuple(raw, "candidate_categories", required=False),
        allowed_actions=allowed_actions,
        requires_owner_quantitative_input_for_production=bool(
            raw.get("requires_owner_quantitative_input_for_production", True)
        ),
        allows_shadow_without_owner_quantitative_input=bool(
            raw.get("allows_shadow_without_owner_quantitative_input", False)
        ),
    )


def _validate_manifest(manifest: ParameterGovernanceManifest, path: Path) -> None:
    if manifest.production_effect != "none":
        raise ValueError(f"parameter governance manifest production_effect must be none: {path}")
    for entry in manifest.parameters:
        if entry.production_effect != "none":
            raise ValueError(
                f"parameter governance entry production_effect must be none: {entry.parameter_id}"
            )
        if not entry.validation_evidence:
            raise ValueError(
                f"parameter governance entry missing validation_evidence: {entry.parameter_id}"
            )
        if not entry.allowed_actions:
            raise ValueError(
                f"parameter governance entry missing allowed_actions: {entry.parameter_id}"
            )


def _parameter_report(
    entry: ParameterGovernanceEntry,
    *,
    manifest: ParameterGovernanceManifest,
    candidates: tuple[dict[str, Any], ...],
    candidate_evaluation_mode: str,
) -> ParameterGovernanceParameterReport:
    matching = tuple(
        candidate
        for candidate in candidates
        if str(candidate.get("category") or "") in set(entry.candidate_categories)
    )
    candidate_status_counts = _status_counts(matching)
    veto_reason_counts = _veto_counts(matching)
    config_exists = entry.config_path.exists()
    action, reason, constraints = _action_for_entry(
        entry,
        manifest=manifest,
        candidates=matching,
        candidate_status_counts=candidate_status_counts,
        veto_reason_counts=veto_reason_counts,
        config_exists=config_exists,
        candidate_evaluation_mode=candidate_evaluation_mode,
    )
    return ParameterGovernanceParameterReport(
        parameter_id=entry.parameter_id,
        surface=entry.surface,
        config_path=entry.config_path,
        config_exists=config_exists,
        config_sha256=_file_sha256(entry.config_path) if config_exists else None,
        source_level=entry.source_level,
        manifest_status=entry.status,
        owner=entry.owner,
        owner_quantitative_input_status=manifest.owner_quantitative_input_status,
        candidate_count=len(matching),
        candidate_status_counts=candidate_status_counts,
        veto_reason_counts=veto_reason_counts,
        action=action,
        action_reason=reason,
        constraints=constraints,
        production_effect=entry.production_effect,
    )


def _action_for_entry(
    entry: ParameterGovernanceEntry,
    *,
    manifest: ParameterGovernanceManifest,
    candidates: tuple[dict[str, Any], ...],
    candidate_status_counts: dict[str, int],
    veto_reason_counts: dict[str, int],
    config_exists: bool,
    candidate_evaluation_mode: str,
) -> tuple[str, str, tuple[str, ...]]:
    constraints: list[str] = []
    allowed = {_action_token(item) for item in entry.allowed_actions}
    owner_unavailable = manifest.owner_quantitative_input_status != "available"
    if owner_unavailable and entry.requires_owner_quantitative_input_for_production:
        constraints.append("owner_quantitative_input_unavailable_for_production")
    if not config_exists:
        return _guarded_action(
            "BLOCKED_BY_POLICY",
            "配置路径不存在，不能评估参数治理。",
            allowed=allowed,
            constraints=constraints,
        )
    if entry.source_level == "invariant":
        return _guarded_action(
            "KEEP_CURRENT",
            "manifest 声明该参数面为 invariant，不由市场反馈自动调参。",
            allowed=allowed,
            constraints=constraints,
        )
    if not entry.candidate_categories:
        action = (
            "COLLECT_MORE_EVIDENCE"
            if entry.source_level == "temporary_baseline"
            else "KEEP_CURRENT"
        )
        reason = (
            "该参数面当前没有 parameter candidate 类别；先按 manifest 继续观察。"
        )
        return _guarded_action(action, reason, allowed=allowed, constraints=constraints)
    if not candidates:
        return _guarded_action(
            "COLLECT_MORE_EVIDENCE",
            "当前 candidate ledger 没有覆盖该参数面。",
            allowed=allowed,
            constraints=constraints,
        )
    if (
        candidate_evaluation_mode == "flow_validation"
        and candidate_status_counts.get("READY_FOR_FORWARD_SHADOW", 0)
    ):
        constraints.append("flow_validation_only_no_production")
        if owner_unavailable and not entry.allows_shadow_without_owner_quantitative_input:
            return _guarded_action(
                "OWNER_DECISION_REQUIRED",
                "flow validation 候选可验证 shadow 接线，但该参数面要求 owner 先给出量化输入。",
                allowed=allowed,
                constraints=constraints,
            )
        return _guarded_action(
            "PREPARE_FORWARD_SHADOW",
            (
                "flow validation 模式存在候选进入 validation-only shadow；"
                "仅用于接线验证，不改 production。"
            ),
            allowed=allowed,
            constraints=constraints,
        )
    if _has_data_block(veto_reason_counts) or _all_statuses_start_with(
        candidate_status_counts,
        "BLOCKED_BY_DATA",
    ):
        return _guarded_action(
            "BLOCKED_BY_DATA",
            "候选被数据质量、数据可信度、coverage 或 placeholder 证据阻断。",
            allowed=allowed,
            constraints=constraints,
        )
    if candidate_status_counts.get("NEEDS_MATERIALITY_POLICY", 0):
        return _guarded_action(
            "BLOCKED_BY_POLICY",
            "候选缺 materiality policy，不能进入 shadow 或 owner review。",
            allowed=allowed,
            constraints=constraints,
        )
    if candidate_status_counts.get("READY_FOR_FORWARD_SHADOW", 0):
        if owner_unavailable and not entry.allows_shadow_without_owner_quantitative_input:
            return _guarded_action(
                "OWNER_DECISION_REQUIRED",
                "候选可进入 shadow 讨论，但该参数面要求 owner 先给出量化输入。",
                allowed=allowed,
                constraints=constraints,
            )
        return _guarded_action(
            "PREPARE_FORWARD_SHADOW",
            "存在未被高级证据阻断的候选；只能准备 forward shadow，不改 production。",
            allowed=allowed,
            constraints=constraints,
        )
    if candidate_status_counts.get("READY_FOR_OWNER_REVIEW", 0):
        return _guarded_action(
            "OWNER_DECISION_REQUIRED",
            "存在候选进入 owner review；当前不能由系统代填量化配置。",
            allowed=allowed,
            constraints=constraints,
        )
    if any(
        candidate_status_counts.get(status, 0)
        for status in (
            "READY_FOR_AS_IF_REPLAY",
            "RISK_REVIEW",
            "MATERIAL_RISK_REVIEW",
            "BLOCKED_BY_OOS",
            "BLOCKED_BY_RANDOM_BASELINE",
        )
    ):
        return _guarded_action(
            "COLLECT_MORE_EVIDENCE",
            "候选仍需补 replay、OOS、random/benchmark 或风险复核证据。",
            allowed=allowed,
            constraints=constraints,
        )
    if candidate_status_counts and set(candidate_status_counts) <= {"OBSERVE_ONLY"}:
        return _guarded_action(
            "KEEP_CURRENT",
            "候选收益变化未达到 material 阈值；保持当前配置并继续观察。",
            allowed=allowed,
            constraints=constraints,
        )
    return _guarded_action(
        "COLLECT_MORE_EVIDENCE",
        "候选状态未达到 shadow/owner 条件；继续积累证据。",
        allowed=allowed,
        constraints=constraints,
    )


def _guarded_action(
    desired: str,
    reason: str,
    *,
    allowed: set[str],
    constraints: list[str],
) -> tuple[str, str, tuple[str, ...]]:
    if desired not in allowed:
        constraints.append(f"manifest_disallows_{desired.lower()}")
        return (
            "BLOCKED_BY_POLICY",
            f"manifest 未允许建议动作 {desired}；先补治理边界。",
            tuple(dict.fromkeys(constraints)),
        )
    return desired, reason, tuple(dict.fromkeys(constraints))


def _load_candidate_ledger(path: Path) -> tuple[dict[str, Any], tuple[str, ...]]:
    if not path.exists():
        return (
            {
                "status": "NOT_CONNECTED",
                "candidate_count": 0,
                "trial_count": 0,
                "candidates": [],
            },
            (f"parameter candidate ledger 不存在：{path}",),
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"parameter candidate ledger must contain an object: {path}")
    warnings = payload.get("warnings", [])
    if isinstance(warnings, list):
        return payload, tuple(str(item) for item in warnings if str(item).strip())
    if isinstance(warnings, str) and warnings.strip():
        return payload, (warnings,)
    return payload, ()


def _candidate_items(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_items = payload.get("candidates", [])
    if not isinstance(raw_items, list):
        return ()
    return tuple(item for item in raw_items if isinstance(item, dict))


def _status_counts(candidates: tuple[dict[str, Any], ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        status = str(candidate.get("recommendation_status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _veto_counts(candidates: tuple[dict[str, Any], ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        raw_reasons = candidate.get("veto_reasons", [])
        if not isinstance(raw_reasons, list):
            continue
        for reason in raw_reasons:
            token = str(reason)
            if not token.strip():
                continue
            counts[token] = counts.get(token, 0) + 1
    return dict(sorted(counts.items()))


def _has_data_block(veto_reason_counts: dict[str, int]) -> bool:
    return any(reason.startswith("data_") for reason in veto_reason_counts)


def _all_statuses_start_with(counts: dict[str, int], prefix: str) -> bool:
    return bool(counts) and all(status.startswith(prefix) for status in counts)


def _project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"parameter governance manifest missing required string: {key}")
    return value.strip()


def _string_tuple(
    payload: dict[str, Any],
    key: str,
    *,
    required: bool = True,
) -> tuple[str, ...]:
    raw = payload.get(key)
    if raw is None and not required:
        return ()
    if not isinstance(raw, list):
        raise ValueError(f"parameter governance manifest list required: {key}")
    values = tuple(str(item).strip() for item in raw if str(item).strip())
    if required and not values:
        raise ValueError(f"parameter governance manifest list must not be empty: {key}")
    return values


def _string_mapping(payload: dict[str, Any], key: str) -> dict[str, str]:
    raw = payload.get(key)
    if not isinstance(raw, dict):
        raise ValueError(f"parameter governance manifest mapping required: {key}")
    return {str(item_key): str(item_value) for item_key, item_value in raw.items()}


def _duplicates(values: Any) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "无"
    return "，".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _parameter_row(item: ParameterGovernanceParameterReport) -> str:
    constraints = "无" if not item.constraints else "<br/>".join(item.constraints)
    status_counts = _format_counts(item.candidate_status_counts)
    config_label = item.config_path.name if item.config_exists else "MISSING"
    return (
        f"| `{item.parameter_id}` | {item.source_level} | `{config_label}` | "
        f"{item.candidate_count} ({status_counts}) | {item.action} | "
        f"{item.action_reason} | {constraints} |"
    )


def _next_action(report: ParameterGovernanceReport) -> str:
    if report.status == "FAIL":
        return "- 下一步：先修复 manifest/policy 阻断，再运行参数治理报告。"
    if report.action_counts.get("BLOCKED_BY_DATA", 0):
        return "- 下一步：优先修复 candidate ledger 中的数据可信度、coverage 或 benchmark 阻断。"
    if report.action_counts.get("PREPARE_FORWARD_SHADOW", 0):
        return "- 下一步：为未阻断候选准备 shadow 假设卡和 rollback 条件，不改 production。"
    if report.action_counts.get("OWNER_DECISION_REQUIRED", 0):
        return "- 下一步：等待 owner 给出量化 policy 或明确拒绝；系统不得代填参数。"
    return "- 下一步：继续运行 feedback/outcome/robustness 闭环，保持当前配置。"


def _action_token(config_action: str) -> str:
    return config_action.upper()
