from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import DEFAULT_EXECUTION_POLICY_CONFIG_PATH

ExecutionPolicyStatus = Literal["production", "candidate", "retired"]
ExecutionActionId = Literal[
    "maintain",
    "small_increase",
    "no_new_position",
    "reduce_to_target_range",
    "wait_manual_review",
    "observe_only",
]

REQUIRED_ACTION_IDS: frozenset[str] = frozenset(
    {
        "maintain",
        "small_increase",
        "no_new_position",
        "reduce_to_target_range",
        "wait_manual_review",
        "observe_only",
    }
)


class ExecutionPolicyIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class ExecutionAction(BaseModel):
    action_id: ExecutionActionId
    label: str = Field(min_length=1)
    description: str = Field(min_length=1)
    allowed_in_reports: bool = True


class ExecutionPolicy(BaseModel):
    policy_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1, pattern=r"^v[0-9]+([.][0-9]+)*$")
    status: ExecutionPolicyStatus
    owner: str = Field(min_length=1)
    description: str = Field(min_length=1)
    minimum_rebalance_delta: float = Field(ge=0, le=1)
    small_increase_delta: float = Field(ge=0, le=1)
    reduce_delta: float = Field(ge=0, le=1)
    low_confidence_levels: list[str] = Field(default_factory=list)
    manual_review_gate_ids: list[str] = Field(default_factory=list)
    no_new_position_gate_ids: list[str] = Field(default_factory=list)
    cooldown_trading_days: int = Field(ge=0)
    actions: list[ExecutionAction] = Field(min_length=1)
    last_reviewed_at: date
    next_review_due: date

    @model_validator(mode="after")
    def validate_thresholds_and_dates(self) -> Self:
        if self.next_review_due < self.last_reviewed_at:
            raise ValueError("next_review_due must not be before last_reviewed_at")
        if self.small_increase_delta < self.minimum_rebalance_delta:
            raise ValueError("small_increase_delta must be >= minimum_rebalance_delta")
        if self.reduce_delta < self.minimum_rebalance_delta:
            raise ValueError("reduce_delta must be >= minimum_rebalance_delta")
        return self


@dataclass(frozen=True)
class ExecutionPolicyIssue:
    severity: ExecutionPolicyIssueSeverity
    code: str
    message: str
    action_id: str | None = None


@dataclass(frozen=True)
class ExecutionPolicyStore:
    input_path: Path
    policy: ExecutionPolicy | None
    load_errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ExecutionPolicyValidationReport:
    as_of: date
    store: ExecutionPolicyStore
    issues: tuple[ExecutionPolicyIssue, ...]

    @property
    def action_count(self) -> int:
        if self.store.policy is None:
            return 0
        return len(self.store.policy.actions)

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == ExecutionPolicyIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == ExecutionPolicyIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class ExecutionAdvisory:
    policy_id: str
    policy_version: str
    action_id: str
    label: str
    target_band: tuple[float, float]
    previous_band: tuple[float, float] | None
    delta_midpoint: float | None
    reasons: tuple[str, ...]
    constraints: tuple[str, ...]
    production_effect: str = "none"


def load_execution_policy(
    input_path: Path | str = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
) -> ExecutionPolicyStore:
    path = Path(input_path)
    if not path.exists():
        return ExecutionPolicyStore(
            input_path=path,
            policy=None,
            load_errors=(f"文件不存在：{path}",),
        )
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
    except (OSError, yaml.YAMLError) as exc:
        return ExecutionPolicyStore(input_path=path, policy=None, load_errors=(str(exc),))

    try:
        policy = ExecutionPolicy.model_validate(raw)
    except ValidationError as exc:
        return ExecutionPolicyStore(
            input_path=path,
            policy=None,
            load_errors=(_compact_validation_error(exc),),
        )
    return ExecutionPolicyStore(input_path=path, policy=policy)


def validate_execution_policy(
    store: ExecutionPolicyStore,
    *,
    as_of: date,
) -> ExecutionPolicyValidationReport:
    issues: list[ExecutionPolicyIssue] = []
    for error in store.load_errors:
        issues.append(
            ExecutionPolicyIssue(
                severity=ExecutionPolicyIssueSeverity.ERROR,
                code="execution_policy_load_error",
                message=error,
            )
        )
    policy = store.policy
    if policy is not None:
        _check_action_registry(policy, issues)
        if policy.next_review_due < as_of:
            issues.append(
                ExecutionPolicyIssue(
                    severity=ExecutionPolicyIssueSeverity.WARNING,
                    code="execution_policy_review_overdue",
                    message=(
                        "execution policy 已超过 next_review_due："
                        f"{policy.next_review_due.isoformat()}"
                    ),
                )
            )
    return ExecutionPolicyValidationReport(as_of=as_of, store=store, issues=tuple(issues))


def build_execution_advisory(
    *,
    policy: ExecutionPolicy,
    current_band: tuple[float, float],
    previous_band: tuple[float, float] | None,
    confidence_level: str,
    triggered_gate_ids: tuple[str, ...],
    report_status: str,
) -> ExecutionAdvisory:
    action_by_id = {action.action_id: action for action in policy.actions}
    constraints: list[str] = []
    reasons: list[str] = []
    current_mid = _midpoint(current_band)
    previous_mid = None if previous_band is None else _midpoint(previous_band)
    delta_midpoint = None if previous_mid is None else current_mid - previous_mid
    triggered_gate_set = set(triggered_gate_ids) - {"score_model"}
    manual_review_gates = sorted(triggered_gate_set & set(policy.manual_review_gate_ids))
    no_new_position_gates = sorted(triggered_gate_set & set(policy.no_new_position_gate_ids))

    if previous_band is None:
        action_id: ExecutionActionId = "observe_only"
        reasons.append("缺少上一期最终仓位区间，先建立执行基线。")
    elif confidence_level in set(policy.low_confidence_levels):
        action_id = "wait_manual_review"
        reasons.append(f"判断置信度为 {confidence_level}，需要人工复核。")
    elif manual_review_gates:
        action_id = "wait_manual_review"
        reasons.append(f"触发需人工复核 gate：{', '.join(manual_review_gates)}。")
    elif delta_midpoint is not None and delta_midpoint <= -policy.reduce_delta:
        action_id = "reduce_to_target_range"
        reasons.append("最终目标区间较上一期明显下移，应复核是否降至目标范围。")
    elif no_new_position_gates and delta_midpoint is not None and delta_midpoint > 0:
        action_id = "no_new_position"
        reasons.append(
            f"目标区间上移但触发禁止新增仓位 gate：{', '.join(no_new_position_gates)}。"
        )
    elif delta_midpoint is not None and delta_midpoint >= policy.small_increase_delta:
        action_id = "small_increase"
        reasons.append("最终目标区间较上一期明显上移，可进入小幅加仓复核。")
    elif delta_midpoint is not None and abs(delta_midpoint) < policy.minimum_rebalance_delta:
        action_id = "maintain"
        reasons.append("最终目标区间变化小于最小再平衡阈值。")
    else:
        action_id = "maintain"
        reasons.append("未满足减仓、加仓或等待复核条件，维持原执行节奏。")

    if report_status != "PASS":
        constraints.append(f"报告状态为 {report_status}，执行含义需要降级解释。")
    if no_new_position_gates:
        constraints.append(f"禁止主动加仓 gate：{', '.join(no_new_position_gates)}。")
    if manual_review_gates:
        constraints.append(f"人工复核 gate：{', '.join(manual_review_gates)}。")
    if policy.cooldown_trading_days:
        constraints.append(f"冷却期规则：{policy.cooldown_trading_days} 个交易日。")

    action = action_by_id[action_id]
    return ExecutionAdvisory(
        policy_id=policy.policy_id,
        policy_version=policy.version,
        action_id=action.action_id,
        label=action.label,
        target_band=current_band,
        previous_band=previous_band,
        delta_midpoint=delta_midpoint,
        reasons=tuple(reasons),
        constraints=tuple(constraints),
    )


def render_execution_advisory_section(
    advisory: ExecutionAdvisory,
    *,
    validation_status: str | None = None,
    validation_report_path: Path | None = None,
) -> str:
    previous = (
        "无上一期基线"
        if advisory.previous_band is None
        else f"{advisory.previous_band[0]:.0%}-{advisory.previous_band[1]:.0%}"
    )
    delta = "NA" if advisory.delta_midpoint is None else f"{advisory.delta_midpoint:+.0%}"
    lines = [
        "## 执行建议",
        "",
        f"- 执行动作：{advisory.label}（`{advisory.action_id}`）",
        f"- 执行政策：`{advisory.policy_id}` `{advisory.policy_version}`",
        f"- 执行政策校验：{validation_status or '未记录'}",
        (
            "- 目标区间（股票风险资产内）："
            f"{advisory.target_band[0]:.0%}-{advisory.target_band[1]:.0%}"
        ),
        f"- 上一期最终区间：{previous}",
        f"- 区间中点变化：{delta}",
        f"- 生产影响：{advisory.production_effect}",
        "",
        "### 动作原因",
        "",
    ]
    lines.extend(f"- {reason}" for reason in advisory.reasons)
    lines.extend(["", "### 执行约束", ""])
    if advisory.constraints:
        lines.extend(f"- {constraint}" for constraint in advisory.constraints)
    else:
        lines.append("- 未触发额外执行约束。")
    if validation_report_path is not None:
        lines.extend(["", f"- 执行政策校验报告：`{validation_report_path}`"])
    lines.extend(
        [
            "",
            "本节是 advisory execution policy 的解释输出，不是自动交易指令；"
            "正式执行仍需人工复核账户约束、流动性、税费和事件窗口。",
        ]
    )
    return "\n".join(lines)


def lookup_execution_action(
    input_path: Path | str,
    action_id: str,
) -> ExecutionAction:
    store = load_execution_policy(input_path)
    if store.load_errors:
        raise ValueError("; ".join(store.load_errors))
    if store.policy is None:
        raise ValueError("execution policy 为空。")
    for action in store.policy.actions:
        if action.action_id == action_id:
            return action
    raise KeyError(f"未找到 execution action：{action_id}")


def render_execution_action_lookup(action: ExecutionAction) -> str:
    return "\n".join(
        [
            f"# {action.label}",
            "",
            f"- Action ID：`{action.action_id}`",
            f"- Reports allowed：{action.allowed_in_reports}",
            f"- Description：{action.description}",
        ]
    )


def default_execution_policy_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"execution_policy_{as_of.isoformat()}.md"


def write_execution_policy_report(
    report: ExecutionPolicyValidationReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_execution_policy_report(report), encoding="utf-8")
    return output_path


def render_execution_policy_report(report: ExecutionPolicyValidationReport) -> str:
    policy = report.store.policy
    lines = [
        "# Advisory Execution Policy 校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.store.input_path}`",
        f"- 动作数量：{report.action_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "- 治理边界：本政策只约束报告中的 advisory action language，"
        "不直接执行交易，也不改变 production scoring 或 position_gate。",
    ]
    if policy is not None:
        lines.extend(
            [
                f"- Policy：`{policy.policy_id}` `{policy.version}`",
                f"- 最小再平衡阈值：{policy.minimum_rebalance_delta:.0%}",
                f"- 小幅加仓阈值：{policy.small_increase_delta:.0%}",
                f"- 减仓阈值：{policy.reduce_delta:.0%}",
                f"- 冷却期：{policy.cooldown_trading_days} 个交易日",
                f"- 下次复核：{policy.next_review_due.isoformat()}",
                "",
                "## Advisory Action Taxonomy",
                "",
                "| Action | Label | Reports | Description |",
                "|---|---|---|---|",
            ]
        )
        for action in policy.actions:
            lines.append(
                "| "
                f"`{action.action_id}` | "
                f"{_escape_markdown_table(action.label)} | "
                f"{action.allowed_in_reports} | "
                f"{_escape_markdown_table(action.description)} |"
            )
    lines.extend(_issue_section(report.issues))
    return "\n".join(lines).rstrip() + "\n"


def _check_action_registry(
    policy: ExecutionPolicy,
    issues: list[ExecutionPolicyIssue],
) -> None:
    seen: set[str] = set()
    duplicate_ids: set[str] = set()
    for action in policy.actions:
        if action.action_id in seen:
            duplicate_ids.add(action.action_id)
        seen.add(action.action_id)
        if not action.allowed_in_reports:
            issues.append(
                ExecutionPolicyIssue(
                    severity=ExecutionPolicyIssueSeverity.WARNING,
                    code="action_not_allowed_in_reports",
                    action_id=action.action_id,
                    message="action taxonomy 中存在不允许出现在报告中的动作。",
                )
            )
    for action_id in sorted(duplicate_ids):
        issues.append(
            ExecutionPolicyIssue(
                severity=ExecutionPolicyIssueSeverity.ERROR,
                code="duplicate_execution_action_id",
                action_id=action_id,
                message="execution action id 必须唯一。",
            )
        )
    missing = REQUIRED_ACTION_IDS - seen
    if missing:
        issues.append(
            ExecutionPolicyIssue(
                severity=ExecutionPolicyIssueSeverity.ERROR,
                code="missing_required_execution_actions",
                message="缺少必需 advisory action："
                + ", ".join(sorted(missing)),
            )
        )


def _midpoint(band: tuple[float, float]) -> float:
    return (band[0] + band[1]) / 2.0


def _issue_section(issues: tuple[ExecutionPolicyIssue, ...]) -> list[str]:
    lines = ["", "## 校验事项", ""]
    if not issues:
        lines.append("未发现错误或警告。")
        return lines
    lines.extend(["| Severity | Code | Action | Message |", "|---|---|---|---|"])
    for issue in issues:
        lines.append(
            "| "
            f"{issue.severity.value} | "
            f"`{issue.code}` | "
            f"{issue.action_id or ''} | "
            f"{_escape_markdown_table(issue.message)} |"
        )
    return lines


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _compact_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error.get("loc", ())) or "<root>"
        parts.append(f"{location}: {error.get('msg', '')}")
    return "; ".join(parts)
