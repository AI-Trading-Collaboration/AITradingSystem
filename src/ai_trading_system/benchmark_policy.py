from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import (
    DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    PROJECT_ROOT,
)

BenchmarkRole = Literal[
    "broad_market_beta",
    "nasdaq_growth_beta",
    "semiconductor_beta",
    "semiconductor_cross_check",
    "ai_theme_beta",
    "defensive_or_cash_proxy",
    "custom",
]
BenchmarkPolicyStatus = Literal["production", "candidate", "retired"]
BenchmarkInstrumentType = Literal["etf", "index", "single_stock", "basket"]
CustomBasketStatus = Literal["planned", "candidate", "production", "retired"]

ROLE_LABELS = {
    "broad_market_beta": "广义美股 Beta",
    "nasdaq_growth_beta": "纳指/成长股 Beta",
    "semiconductor_beta": "半导体主题 Beta",
    "semiconductor_cross_check": "半导体交叉验证",
    "ai_theme_beta": "AI 主题 Basket",
    "defensive_or_cash_proxy": "防御或现金代理",
    "custom": "自定义口径",
}


class BenchmarkPolicyIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class BenchmarkInstrument(BaseModel):
    benchmark_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    ticker: str = Field(min_length=1, pattern=r"^[A-Za-z0-9.^:-]+$")
    name: str = Field(min_length=1)
    instrument_type: BenchmarkInstrumentType
    role: BenchmarkRole
    default_ai_proxy_eligible: bool = False
    default_benchmark: bool = False
    source_config_paths: list[str] = Field(default_factory=list)
    use_cases: list[str] = Field(min_length=1)
    interpretation: str = Field(min_length=1)
    limitations: list[str] = Field(default_factory=list)


class CustomAIBasket(BaseModel):
    basket_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    name: str = Field(min_length=1)
    status: CustomBasketStatus
    description: str = Field(min_length=1)
    point_in_time_lifecycle: bool = False
    lifecycle_path: str = ""
    weighting_method: str = ""
    rebalance_frequency: str = ""
    source_config_paths: list[str] = Field(default_factory=list)
    available_from: date | None = None
    limitations: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_production_readiness(self) -> Self:
        if self.status in {"candidate", "production"}:
            if not self.point_in_time_lifecycle:
                raise ValueError("candidate/production custom AI basket requires PIT lifecycle")
            if not self.lifecycle_path:
                raise ValueError("candidate/production custom AI basket requires lifecycle_path")
            if not self.weighting_method:
                raise ValueError("candidate/production custom AI basket requires weighting_method")
            if not self.rebalance_frequency:
                raise ValueError(
                    "candidate/production custom AI basket requires rebalance_frequency"
                )
        if self.status == "production" and self.available_from is None:
            raise ValueError("production custom AI basket requires available_from")
        return self


class BenchmarkPolicy(BaseModel):
    policy_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1, pattern=r"^v[0-9]+([.][0-9]+)*$")
    status: BenchmarkPolicyStatus
    owner: str = Field(min_length=1)
    description: str = Field(min_length=1)
    default_ai_proxy: str = Field(min_length=1)
    default_benchmarks: list[str] = Field(min_length=1)
    minimum_roles: list[BenchmarkRole] = Field(default_factory=list)
    instruments: list[BenchmarkInstrument] = Field(min_length=1)
    custom_ai_baskets: list[CustomAIBasket] = Field(default_factory=list)
    last_reviewed_at: date
    next_review_due: date

    @model_validator(mode="after")
    def validate_review_dates(self) -> Self:
        if self.next_review_due < self.last_reviewed_at:
            raise ValueError("next_review_due must not be before last_reviewed_at")
        return self


@dataclass(frozen=True)
class BenchmarkPolicyIssue:
    severity: BenchmarkPolicyIssueSeverity
    code: str
    message: str
    subject: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class BenchmarkPolicyStore:
    input_path: Path
    policy: BenchmarkPolicy | None
    load_errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class BenchmarkPolicyReport:
    as_of: date
    store: BenchmarkPolicyStore
    issues: tuple[BenchmarkPolicyIssue, ...]
    selected_strategy_ticker: str | None = None
    selected_benchmark_tickers: tuple[str, ...] | None = None

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == BenchmarkPolicyIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == BenchmarkPolicyIssueSeverity.WARNING
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

    @property
    def instrument_count(self) -> int:
        if self.store.policy is None:
            return 0
        return len(self.store.policy.instruments)

    @property
    def custom_basket_count(self) -> int:
        if self.store.policy is None:
            return 0
        return len(self.store.policy.custom_ai_baskets)


def load_benchmark_policy(
    input_path: Path | str = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
) -> BenchmarkPolicyStore:
    path = Path(input_path)
    if not path.exists():
        return BenchmarkPolicyStore(
            input_path=path,
            policy=None,
            load_errors=(f"文件不存在：{path}",),
        )
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
    except (OSError, yaml.YAMLError) as exc:
        return BenchmarkPolicyStore(input_path=path, policy=None, load_errors=(str(exc),))

    try:
        policy = BenchmarkPolicy.model_validate(raw)
    except ValidationError as exc:
        return BenchmarkPolicyStore(
            input_path=path,
            policy=None,
            load_errors=(_compact_validation_error(exc),),
        )
    return BenchmarkPolicyStore(input_path=path, policy=policy)


def validate_benchmark_policy(
    store: BenchmarkPolicyStore,
    *,
    as_of: date,
    selected_strategy_ticker: str | None = None,
    selected_benchmark_tickers: tuple[str, ...] | None = None,
    project_root: Path = PROJECT_ROOT,
) -> BenchmarkPolicyReport:
    issues: list[BenchmarkPolicyIssue] = []
    for error in store.load_errors:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.ERROR,
                code="benchmark_policy_load_error",
                path=store.input_path,
                message=error,
            )
        )
    policy = store.policy
    if policy is not None:
        _check_duplicate_instruments(policy, issues)
        _check_duplicate_custom_baskets(policy, issues)
        _check_default_selection(policy, issues)
        _check_source_paths(policy, project_root, issues)
        _check_custom_baskets(policy, project_root, issues)
        _check_review_due(policy, as_of, issues)
        _check_selected_tickers(
            policy,
            selected_strategy_ticker=selected_strategy_ticker,
            selected_benchmark_tickers=selected_benchmark_tickers,
            issues=issues,
        )
    return BenchmarkPolicyReport(
        as_of=as_of,
        store=store,
        issues=tuple(issues),
        selected_strategy_ticker=selected_strategy_ticker,
        selected_benchmark_tickers=selected_benchmark_tickers,
    )


def default_benchmark_policy_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"benchmark_policy_{as_of.isoformat()}.md"


def write_benchmark_policy_report(report: BenchmarkPolicyReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_benchmark_policy_report(report), encoding="utf-8")
    return output_path


def lookup_benchmark_policy_entry(
    input_path: Path | str,
    entry_id: str,
) -> BenchmarkInstrument | CustomAIBasket:
    store = load_benchmark_policy(input_path)
    if store.load_errors:
        raise ValueError("; ".join(store.load_errors))
    if store.policy is None:
        raise KeyError(f"benchmark policy not found: {input_path}")
    normalized = entry_id.upper()
    for instrument in store.policy.instruments:
        if instrument.benchmark_id == entry_id or instrument.ticker.upper() == normalized:
            return instrument
    for basket in store.policy.custom_ai_baskets:
        if basket.basket_id == entry_id:
            return basket
    raise KeyError(f"benchmark policy entry not found: {entry_id}")


def render_benchmark_policy_lookup(entry: BenchmarkInstrument | CustomAIBasket) -> str:
    if isinstance(entry, BenchmarkInstrument):
        lines = [
            f"Benchmark：{entry.benchmark_id}",
            f"Ticker：{entry.ticker}",
            f"名称：{entry.name}",
            f"角色：{_role_label(entry.role)}",
            f"类型：{entry.instrument_type}",
            f"默认 AI proxy 候选：{entry.default_ai_proxy_eligible}",
            f"默认基准：{entry.default_benchmark}",
            f"解释：{entry.interpretation}",
            f"适用场景：{'; '.join(entry.use_cases)}",
            f"限制：{'; '.join(entry.limitations) if entry.limitations else '无'}",
        ]
        return "\n".join(lines) + "\n"

    lines = [
        f"Custom AI basket：{entry.basket_id}",
        f"名称：{entry.name}",
        f"状态：{entry.status}",
        f"说明：{entry.description}",
        f"Point-in-time lifecycle：{entry.point_in_time_lifecycle}",
        f"Lifecycle path：{entry.lifecycle_path or '未配置'}",
        f"权重方法：{entry.weighting_method or '未配置'}",
        f"再平衡频率：{entry.rebalance_frequency or '未配置'}",
        f"限制：{'; '.join(entry.limitations) if entry.limitations else '无'}",
    ]
    return "\n".join(lines) + "\n"


def render_benchmark_policy_report(report: BenchmarkPolicyReport) -> str:
    policy = report.store.policy
    lines = [
        "# Benchmark Policy 校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 输入路径：`{report.store.input_path}`",
        f"- 已登记 benchmark 数量：{report.instrument_count}",
        f"- 已登记 custom AI basket 数量：{report.custom_basket_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
    ]
    if policy is not None:
        role_counts = Counter(instrument.role for instrument in policy.instruments)
        lines.extend(
            [
                f"- Policy：`{policy.policy_id}` `{policy.version}`",
                f"- 默认 AI proxy：{policy.default_ai_proxy}",
                f"- 默认基准：{', '.join(policy.default_benchmarks)}",
                f"- 下次复核：{policy.next_review_due.isoformat()}",
                "",
                "## 角色覆盖",
                "",
                "| 角色 | 数量 |",
                "|---|---:|",
            ]
        )
        for role, count in sorted(role_counts.items()):
            lines.append(f"| {_role_label(role)} | {count} |")

        lines.extend(
            [
                "",
                "## Benchmark Registry",
                "",
                "| ID | Ticker | 角色 | 默认基准 | AI proxy 候选 | 解释边界 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for instrument in sorted(policy.instruments, key=lambda item: item.benchmark_id):
            lines.append(
                "| "
                f"`{instrument.benchmark_id}` | "
                f"{instrument.ticker} | "
                f"{_role_label(instrument.role)} | "
                f"{instrument.default_benchmark} | "
                f"{instrument.default_ai_proxy_eligible} | "
                f"{_escape_markdown_table(instrument.interpretation)} |"
            )

        lines.extend(
            [
                "",
                "## Custom AI Baskets",
                "",
                "| Basket | 状态 | PIT lifecycle | Lifecycle path | 权重方法 | 限制 |",
                "|---|---|---|---|---|---|",
            ]
        )
        if policy.custom_ai_baskets:
            for basket in sorted(policy.custom_ai_baskets, key=lambda item: item.basket_id):
                lines.append(
                    "| "
                    f"`{basket.basket_id}` | "
                    f"{basket.status} | "
                    f"{basket.point_in_time_lifecycle} | "
                    f"`{basket.lifecycle_path or '未配置'}` | "
                    f"{_escape_markdown_table(basket.weighting_method or '未配置')} | "
                    f"{_escape_markdown_table('; '.join(basket.limitations) or '无')} |"
                )
        else:
            lines.append("| 无 | NA | NA | NA | NA | NA |")

        selected_section = render_benchmark_policy_summary_section(report)
        if selected_section:
            lines.extend(["", selected_section.rstrip()])

    lines.extend(_issue_section(report.issues))
    return "\n".join(lines).rstrip() + "\n"


def render_benchmark_policy_summary_section(report: BenchmarkPolicyReport | None) -> str:
    if report is None:
        return ""
    policy = report.store.policy
    if policy is None:
        return "\n".join(
            [
                "## 基准政策与解释边界",
                "",
                f"- 基准政策状态：{report.status}",
                "- 解释边界：benchmark policy 未通过加载，不能审计本次 proxy/benchmark 口径。",
            ]
        )
    strategy_ticker = report.selected_strategy_ticker or policy.default_ai_proxy
    benchmark_tickers = report.selected_benchmark_tickers or tuple(policy.default_benchmarks)
    instruments_by_ticker = _instruments_by_ticker(policy)

    lines = [
        "## 基准政策与解释边界",
        "",
        f"- 基准政策状态：{report.status}",
        f"- Policy：`{policy.policy_id}` `{policy.version}`",
        f"- 策略代理标的：{strategy_ticker}",
        f"- 对比基准：{', '.join(benchmark_tickers)}",
        "- 解释边界：动态系统收益必须同时和广义市场、成长股、半导体主题 "
        "benchmark 对比；`SMH` 可作为当前 AI proxy，但不能单独代表完整 AI 产业链。",
        "",
        "| 口径 | Ticker / Basket | 角色 | 可解释内容 | 限制 |",
        "|---|---|---|---|---|",
    ]
    strategy_entry = instruments_by_ticker.get(strategy_ticker.upper())
    lines.append(
        _selection_row(
            label="AI proxy",
            identifier=strategy_ticker,
            instrument=strategy_entry,
        )
    )
    for ticker in benchmark_tickers:
        lines.append(
            _selection_row(
                label="Benchmark",
                identifier=ticker,
                instrument=instruments_by_ticker.get(ticker.upper()),
            )
        )
    warning_lines = [
        f"- {issue.code}：{issue.message}"
        for issue in report.issues
        if issue.severity == BenchmarkPolicyIssueSeverity.WARNING
    ]
    if warning_lines:
        lines.extend(["", "### 基准政策警告", "", *warning_lines])
    return "\n".join(lines)


def _check_duplicate_instruments(
    policy: BenchmarkPolicy,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    id_counts = Counter(instrument.benchmark_id for instrument in policy.instruments)
    ticker_counts = Counter(instrument.ticker.upper() for instrument in policy.instruments)
    for benchmark_id, count in id_counts.items():
        if count > 1:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="duplicate_benchmark_id",
                    subject=benchmark_id,
                    message="benchmark_id 必须唯一。",
                )
            )
    for ticker, count in ticker_counts.items():
        if count > 1:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="duplicate_benchmark_ticker",
                    subject=ticker,
                    message="ticker 在 benchmark registry 中必须唯一。",
                )
            )


def _check_duplicate_custom_baskets(
    policy: BenchmarkPolicy,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    counts = Counter(basket.basket_id for basket in policy.custom_ai_baskets)
    for basket_id, count in counts.items():
        if count > 1:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="duplicate_custom_basket_id",
                    subject=basket_id,
                    message="custom AI basket id 必须唯一。",
                )
            )


def _check_default_selection(
    policy: BenchmarkPolicy,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    by_ticker = _instruments_by_ticker(policy)
    basket_ids = {basket.basket_id for basket in policy.custom_ai_baskets}
    default_proxy = policy.default_ai_proxy.upper()
    if default_proxy not in by_ticker and policy.default_ai_proxy not in basket_ids:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.ERROR,
                code="default_ai_proxy_not_registered",
                subject=policy.default_ai_proxy,
                message="default_ai_proxy 必须登记在 instruments 或 custom_ai_baskets 中。",
            )
        )
    elif default_proxy in by_ticker and not by_ticker[default_proxy].default_ai_proxy_eligible:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.WARNING,
                code="default_ai_proxy_not_marked_eligible",
                subject=policy.default_ai_proxy,
                message="default_ai_proxy 未标记 default_ai_proxy_eligible。",
            )
        )
    for ticker in policy.default_benchmarks:
        if ticker.upper() not in by_ticker:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="default_benchmark_not_registered",
                    subject=ticker,
                    message="default_benchmarks 中的 ticker 必须登记在 instruments 中。",
                )
            )
    default_roles = {
        by_ticker[ticker.upper()].role
        for ticker in policy.default_benchmarks
        if ticker.upper() in by_ticker
    }
    missing_roles = set(policy.minimum_roles) - default_roles
    if missing_roles:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.WARNING,
                code="default_benchmark_role_coverage_incomplete",
                message=(
                    "默认基准未覆盖最低要求角色："
                    + ", ".join(_role_label(role) for role in sorted(missing_roles))
                ),
            )
        )


def _check_source_paths(
    policy: BenchmarkPolicy,
    project_root: Path,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    for instrument in policy.instruments:
        for raw_path in instrument.source_config_paths:
            resolved = _resolve_project_path(project_root, raw_path)
            if not resolved.exists():
                issues.append(
                    BenchmarkPolicyIssue(
                        severity=BenchmarkPolicyIssueSeverity.WARNING,
                        code="benchmark_source_path_missing",
                        subject=instrument.benchmark_id,
                        path=resolved,
                        message=f"登记的 source_config_path 不存在：{raw_path}",
                    )
                )
    for basket in policy.custom_ai_baskets:
        for raw_path in basket.source_config_paths:
            resolved = _resolve_project_path(project_root, raw_path)
            if not resolved.exists():
                issues.append(
                    BenchmarkPolicyIssue(
                        severity=BenchmarkPolicyIssueSeverity.WARNING,
                        code="custom_basket_source_path_missing",
                        subject=basket.basket_id,
                        path=resolved,
                        message=f"登记的 source_config_path 不存在：{raw_path}",
                    )
                )


def _check_custom_baskets(
    policy: BenchmarkPolicy,
    project_root: Path,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    for basket in policy.custom_ai_baskets:
        if basket.status == "planned":
            continue
        if basket.lifecycle_path:
            resolved = _resolve_project_path(project_root, basket.lifecycle_path)
            if not resolved.exists():
                issues.append(
                    BenchmarkPolicyIssue(
                        severity=BenchmarkPolicyIssueSeverity.ERROR,
                        code="custom_basket_lifecycle_missing",
                        subject=basket.basket_id,
                        path=resolved,
                        message="candidate/production custom AI basket 的 lifecycle_path 不存在。",
                    )
                )


def _check_review_due(
    policy: BenchmarkPolicy,
    as_of: date,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    if policy.next_review_due < as_of:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.WARNING,
                code="benchmark_policy_review_overdue",
                subject=policy.policy_id,
                message=(
                    "benchmark policy 已超过 next_review_due："
                    f"{policy.next_review_due.isoformat()}"
                ),
            )
        )


def _check_selected_tickers(
    policy: BenchmarkPolicy,
    *,
    selected_strategy_ticker: str | None,
    selected_benchmark_tickers: tuple[str, ...] | None,
    issues: list[BenchmarkPolicyIssue],
) -> None:
    by_ticker = _instruments_by_ticker(policy)
    basket_ids = {basket.basket_id for basket in policy.custom_ai_baskets}
    if selected_strategy_ticker:
        normalized_strategy = selected_strategy_ticker.upper()
        if normalized_strategy not in by_ticker and selected_strategy_ticker not in basket_ids:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="selected_ai_proxy_not_registered",
                    subject=selected_strategy_ticker,
                    message="本次选择的 strategy_ticker 未登记在 benchmark policy 中。",
                )
            )
        elif normalized_strategy in by_ticker:
            instrument = by_ticker[normalized_strategy]
            if not instrument.default_ai_proxy_eligible and instrument.role != "ai_theme_beta":
                issues.append(
                    BenchmarkPolicyIssue(
                        severity=BenchmarkPolicyIssueSeverity.WARNING,
                        code="selected_ai_proxy_not_eligible",
                        subject=selected_strategy_ticker,
                        message="本次选择的 strategy_ticker 未标记为默认 AI proxy 候选。",
                    )
                )
    if selected_benchmark_tickers is None:
        return
    selected_roles: set[BenchmarkRole] = set()
    for ticker in selected_benchmark_tickers:
        normalized = ticker.upper()
        instrument = by_ticker.get(normalized)
        if instrument is None:
            issues.append(
                BenchmarkPolicyIssue(
                    severity=BenchmarkPolicyIssueSeverity.ERROR,
                    code="selected_benchmark_not_registered",
                    subject=ticker,
                    message="本次选择的 benchmark ticker 未登记在 benchmark policy 中。",
                )
            )
            continue
        selected_roles.add(instrument.role)
    missing_roles = set(policy.minimum_roles) - selected_roles
    if selected_benchmark_tickers and missing_roles:
        issues.append(
            BenchmarkPolicyIssue(
                severity=BenchmarkPolicyIssueSeverity.WARNING,
                code="selected_benchmark_role_coverage_incomplete",
                message=(
                    "本次 benchmark 未覆盖最低建议角色："
                    + ", ".join(_role_label(role) for role in sorted(missing_roles))
                ),
            )
        )


def _instruments_by_ticker(policy: BenchmarkPolicy) -> dict[str, BenchmarkInstrument]:
    return {instrument.ticker.upper(): instrument for instrument in policy.instruments}


def _selection_row(
    *,
    label: str,
    identifier: str,
    instrument: BenchmarkInstrument | None,
) -> str:
    if instrument is None:
        return f"| {label} | {identifier} | 未登记 | 无法审计 | 需先补充 benchmark policy |"
    limitations = "; ".join(instrument.limitations) if instrument.limitations else "无"
    return (
        "| "
        f"{label} | "
        f"{instrument.ticker} | "
        f"{_role_label(instrument.role)} | "
        f"{_escape_markdown_table(instrument.interpretation)} | "
        f"{_escape_markdown_table(limitations)} |"
    )


def _issue_section(issues: tuple[BenchmarkPolicyIssue, ...]) -> list[str]:
    lines = ["", "## 校验事项", ""]
    if not issues:
        lines.append("未发现错误或警告。")
        return lines
    lines.extend(
        [
            "| Severity | Code | Subject | Message |",
            "|---|---|---|---|",
        ]
    )
    for issue in issues:
        subject = issue.subject or (str(issue.path) if issue.path is not None else "")
        lines.append(
            "| "
            f"{issue.severity.value} | "
            f"`{issue.code}` | "
            f"{_escape_markdown_table(subject)} | "
            f"{_escape_markdown_table(issue.message)} |"
        )
    return lines


def _resolve_project_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project_root / path


def _role_label(role: str) -> str:
    return ROLE_LABELS.get(role, role)


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _compact_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error.get("loc", ())) or "<root>"
        parts.append(f"{location}: {error.get('msg', '')}")
    return "; ".join(parts)
