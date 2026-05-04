from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_RULE_CARDS_PATH = PROJECT_ROOT / "config" / "rule_cards.yaml"

RuleCardType = Literal[
    "scoring",
    "position_gate",
    "source_policy",
    "risk_event",
    "valuation_gate",
    "thesis_state",
    "data_quality",
    "feedback_loop",
]
RuleCardStatus = Literal["production", "candidate", "retired"]
ApprovalStatus = Literal[
    "baseline_recorded",
    "approved",
    "pending_approval",
    "rejected",
]
ValidationStatus = Literal[
    "baseline_tested",
    "replay_passed",
    "shadow_passed",
    "pending_validation",
    "failed",
]


class RuleGovernanceIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class RuleCardApproval(BaseModel):
    approval_status: ApprovalStatus
    approved_by: str = ""
    approved_at: date | None = None
    rationale: str = ""


class RuleCardValidation(BaseModel):
    validation_status: ValidationStatus
    validation_refs: list[str] = Field(default_factory=list)
    sample_limitations: list[str] = Field(default_factory=list)


class RuleCardRollback(BaseModel):
    procedure: str = Field(min_length=1)
    trigger_conditions: list[str] = Field(min_length=1)


class RuleCard(BaseModel):
    rule_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    rule_name: str = Field(min_length=1)
    rule_type: RuleCardType
    status: RuleCardStatus
    version: str = Field(min_length=1, pattern=r"^v[0-9]+([.][0-9]+)*$")
    owner: str = Field(min_length=1)
    applies_to: list[str] = Field(min_length=1)
    source_config_paths: list[str] = Field(min_length=1)
    description: str = Field(min_length=1)
    production_since: date | None = None
    retired_at: date | None = None
    linked_rule_experiment: str = ""
    approval: RuleCardApproval
    validation: RuleCardValidation
    rollback: RuleCardRollback
    known_limitations: list[str] = Field(default_factory=list)
    last_reviewed_at: date
    next_review_due: date

    @model_validator(mode="after")
    def validate_lifecycle_fields(self) -> Self:
        if self.status == "production" and self.production_since is None:
            raise ValueError("production rule card requires production_since")
        if self.status == "retired" and self.retired_at is None:
            raise ValueError("retired rule card requires retired_at")
        if self.status == "candidate" and not self.linked_rule_experiment:
            raise ValueError("candidate rule card requires linked_rule_experiment")
        if self.next_review_due < self.last_reviewed_at:
            raise ValueError("next_review_due must not be before last_reviewed_at")
        return self


@dataclass(frozen=True)
class RuleGovernanceIssue:
    severity: RuleGovernanceIssueSeverity
    code: str
    message: str
    rule_id: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class RuleCardStore:
    input_path: Path
    cards: tuple[RuleCard, ...]
    load_errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RuleGovernanceReport:
    as_of: date
    store: RuleCardStore
    issues: tuple[RuleGovernanceIssue, ...]

    @property
    def card_count(self) -> int:
        return len(self.store.cards)

    @property
    def production_count(self) -> int:
        return sum(1 for card in self.store.cards if card.status == "production")

    @property
    def candidate_count(self) -> int:
        return sum(1 for card in self.store.cards if card.status == "candidate")

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == RuleGovernanceIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == RuleGovernanceIssueSeverity.WARNING
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


def load_rule_card_store(input_path: Path | str = DEFAULT_RULE_CARDS_PATH) -> RuleCardStore:
    path = Path(input_path)
    if not path.exists():
        return RuleCardStore(input_path=path, cards=(), load_errors=(f"文件不存在：{path}",))
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
    except (OSError, yaml.YAMLError) as exc:
        return RuleCardStore(input_path=path, cards=(), load_errors=(str(exc),))

    cards: list[RuleCard] = []
    errors: list[str] = []
    for index, raw_card in enumerate(_raw_cards(raw), start=1):
        try:
            cards.append(RuleCard.model_validate(raw_card))
        except ValidationError as exc:
            errors.append(f"card #{index}: {_compact_validation_error(exc)}")
    return RuleCardStore(input_path=path, cards=tuple(cards), load_errors=tuple(errors))


def validate_rule_card_store(
    store: RuleCardStore,
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
) -> RuleGovernanceReport:
    issues: list[RuleGovernanceIssue] = []
    for error in store.load_errors:
        issues.append(
            RuleGovernanceIssue(
                severity=RuleGovernanceIssueSeverity.ERROR,
                code="rule_card_load_error",
                path=store.input_path,
                message=error,
            )
        )
    _check_duplicate_rule_ids(store.cards, issues, store.input_path)
    for card in store.cards:
        _check_rule_card(card, as_of=as_of, project_root=project_root, issues=issues)
    return RuleGovernanceReport(as_of=as_of, store=store, issues=tuple(issues))


def lookup_rule_card(input_path: Path, rule_id: str) -> RuleCard:
    store = load_rule_card_store(input_path)
    if store.load_errors:
        raise ValueError("; ".join(store.load_errors))
    for card in store.cards:
        if card.rule_id == rule_id:
            return card
    raise KeyError(f"rule card not found: {rule_id}")


def render_rule_governance_report(report: RuleGovernanceReport) -> str:
    status_counts = Counter(card.status for card in report.store.cards)
    type_counts = Counter(card.rule_type for card in report.store.cards)
    lines = [
        "# 规则治理校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.store.input_path}`",
        f"- Rule card 数量：{report.card_count}",
        f"- Production：{report.production_count}",
        f"- Candidate：{report.candidate_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 生命周期摘要",
        "",
        "| 状态 | 数量 |",
        "|---|---:|",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f"| {status} | {count} |")

    lines.extend(["", "## 类型摘要", "", "| 类型 | 数量 |", "|---|---:|"])
    for rule_type, count in sorted(type_counts.items()):
        lines.append(f"| {rule_type} | {count} |")

    lines.extend(
        [
            "",
            "## Rule Cards",
            "",
            "| Rule | Type | Status | Version | Owner | Approval | Validation | Next review |",
            "|---|---|---|---|---|---|---|---|",
        ]
    )
    for card in sorted(report.store.cards, key=lambda item: item.rule_id):
        lines.append(
            "| "
            f"`{card.rule_id}` | "
            f"{card.rule_type} | "
            f"{card.status} | "
            f"{card.version} | "
            f"{_escape_markdown_table(card.owner)} | "
            f"{card.approval.approval_status} | "
            f"{card.validation.validation_status} | "
            f"{card.next_review_due.isoformat()} |"
        )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Rule | 说明 |", "|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.rule_id or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- rule card registry 记录生产规则、候选规则和退役规则的生命周期。",
            "- `baseline_recorded` 只表示把已有 production 行为纳入审计台账；"
            "未来规则变更仍需 replay/shadow 和 owner 批准。",
            "- candidate rule card 必须链接 `rule_experiment`，不得直接影响 production。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_rule_governance_report(
    report: RuleGovernanceReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_rule_governance_report(report), encoding="utf-8")
    return output_path


def default_rule_governance_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"rule_governance_{as_of.isoformat()}.md"


def render_rule_card_lookup(card: RuleCard) -> str:
    lines = [
        f"# {card.rule_id}",
        "",
        f"- 名称：{card.rule_name}",
        f"- 类型：{card.rule_type}",
        f"- 状态：{card.status}",
        f"- 版本：{card.version}",
        f"- Owner：{card.owner}",
        f"- 适用范围：{', '.join(card.applies_to)}",
        f"- 来源配置：{', '.join(card.source_config_paths)}",
        f"- Approval：{card.approval.approval_status}",
        f"- Validation：{card.validation.validation_status}",
        f"- Last reviewed：{card.last_reviewed_at.isoformat()}",
        f"- Next review：{card.next_review_due.isoformat()}",
        f"- Rollback：{card.rollback.procedure}",
    ]
    if card.linked_rule_experiment:
        lines.append(f"- Rule experiment：`{card.linked_rule_experiment}`")
    return "\n".join(lines) + "\n"


def _raw_cards(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "cards" in raw:
        cards = raw["cards"]
        return cards if isinstance(cards, list) else [cards]
    return [raw]


def _check_duplicate_rule_ids(
    cards: tuple[RuleCard, ...],
    issues: list[RuleGovernanceIssue],
    path: Path,
) -> None:
    counts = Counter(card.rule_id for card in cards)
    for rule_id, count in counts.items():
        if count > 1:
            issues.append(
                RuleGovernanceIssue(
                    severity=RuleGovernanceIssueSeverity.ERROR,
                    code="duplicate_rule_card_id",
                    rule_id=rule_id,
                    path=path,
                    message="rule_id 重复，无法稳定追踪规则生命周期。",
                )
            )


def _check_rule_card(
    card: RuleCard,
    *,
    as_of: date,
    project_root: Path,
    issues: list[RuleGovernanceIssue],
) -> None:
    if card.last_reviewed_at > as_of:
        issues.append(
            RuleGovernanceIssue(
                severity=RuleGovernanceIssueSeverity.ERROR,
                code="rule_card_reviewed_in_future",
                rule_id=card.rule_id,
                message="last_reviewed_at 晚于评估日期。",
            )
        )
    if card.next_review_due < as_of and card.status != "retired":
        issues.append(
            RuleGovernanceIssue(
                severity=RuleGovernanceIssueSeverity.WARNING,
                code="rule_card_review_overdue",
                rule_id=card.rule_id,
                message="rule card 已超过 next_review_due，需要复核。",
            )
        )
    if card.status == "production":
        if card.approval.approval_status not in {"approved", "baseline_recorded"}:
            issues.append(
                RuleGovernanceIssue(
                    severity=RuleGovernanceIssueSeverity.ERROR,
                    code="production_rule_not_approved",
                    rule_id=card.rule_id,
                    message="production rule card 必须 approved 或 baseline_recorded。",
                )
            )
        if not card.validation.validation_refs:
            issues.append(
                RuleGovernanceIssue(
                    severity=RuleGovernanceIssueSeverity.ERROR,
                    code="production_rule_missing_validation_refs",
                    rule_id=card.rule_id,
                    message="production rule card 必须记录验证引用。",
                )
            )
    if card.status == "candidate" and card.approval.approval_status == "approved":
        issues.append(
            RuleGovernanceIssue(
                severity=RuleGovernanceIssueSeverity.WARNING,
                code="candidate_rule_approved_but_not_promoted",
                rule_id=card.rule_id,
                message="candidate 已批准但未升级 production，需要明确下一步。",
            )
        )
    for raw_path in card.source_config_paths:
        resolved = project_root / raw_path
        if not resolved.exists():
            issues.append(
                RuleGovernanceIssue(
                    severity=RuleGovernanceIssueSeverity.WARNING,
                    code="rule_card_source_path_missing",
                    rule_id=card.rule_id,
                    message=f"来源配置路径不存在：{raw_path}",
                )
            )


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "rule card schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _severity_label(severity: RuleGovernanceIssueSeverity) -> str:
    if severity == RuleGovernanceIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
