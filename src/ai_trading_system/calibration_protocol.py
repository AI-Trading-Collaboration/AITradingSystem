from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml

from ai_trading_system.config import PROJECT_ROOT

CalibrationProtocolSeverity = Literal["ERROR", "WARNING"]

REQUIRED_PROTOCOL_FIELDS = (
    "protocol_id",
    "experiment_id",
    "git_commit",
    "feature_version",
    "prompt_version",
    "model_version",
    "data_snapshot_hash",
    "yaml_hash",
    "cost_model_version",
    "execution_assumption_version",
    "market_regime",
    "date_range",
    "label_horizon",
    "train_validation_test_scheme",
    "purge_days",
    "embargo_days",
    "objective_version",
    "benchmark_set",
    "parameter_family_scope",
    "parameter_search_space_hash",
    "number_of_trials",
    "approval_owner",
)
DISALLOWED_PARAMETER_SCOPES = frozenset({"all", "combined", "global", "full_system"})
ALLOWED_PARAMETER_SCOPES = frozenset(
    {
        "signal_weights",
        "gate_rules",
        "execution_parameters",
        "risk_gate",
        "event_attribution",
        "soft_gate_overlay",
    }
)
REQUIRED_BENCHMARKS = frozenset({"current_production_model"})
RECOMMENDED_BENCHMARKS = frozenset(
    {
        "buy_and_hold",
        "manual_weight_signal",
        "equal_weight_signal",
        "trend_only_model",
        "risk_parity_baseline",
        "no_gate_model",
    }
)
AI_AFTER_CHATGPT_START = date(2022, 12, 1)
PLACEHOLDER_TOKENS = ("REPLACE", "YYYY", "TODO")


@dataclass(frozen=True)
class CalibrationProtocolIssue:
    severity: CalibrationProtocolSeverity
    code: str
    field: str
    message: str
    recommendation: str


@dataclass(frozen=True)
class CalibrationProtocolReport:
    manifest_path: Path
    as_of: date
    manifest: Mapping[str, Any]
    issues: tuple[CalibrationProtocolIssue, ...]

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

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


def load_calibration_protocol_manifest(path: Path | str) -> dict[str, Any]:
    input_path = Path(path)
    raw = yaml.safe_load(input_path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("calibration protocol manifest must contain a mapping")
    return dict(raw)


def validate_calibration_protocol_manifest(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path,
    as_of: date,
) -> CalibrationProtocolReport:
    issues: list[CalibrationProtocolIssue] = []
    _append_required_field_issues(issues, manifest)
    _append_placeholder_issues(issues, manifest)
    _append_date_range_issues(issues, manifest)
    _append_horizon_and_embargo_issues(issues, manifest)
    _append_scheme_issues(issues, manifest)
    _append_objective_issues(issues, manifest)
    _append_benchmark_issues(issues, manifest)
    _append_parameter_scope_issues(issues, manifest)
    _append_trial_issues(issues, manifest)
    _append_production_boundary_issues(issues, manifest)
    return CalibrationProtocolReport(
        manifest_path=manifest_path,
        as_of=as_of,
        manifest=manifest,
        issues=tuple(issues),
    )


def render_calibration_protocol_report(report: CalibrationProtocolReport) -> str:
    lines = [
        "# 调权防过拟合协议校验报告",
        "",
        f"- 状态：{report.status}",
        f"- 校验日期：{report.as_of.isoformat()}",
        f"- Manifest：`{report.manifest_path}`",
        "- production_effect：none",
        "- 边界：本报告只校验调权实验协议，不批准 overlay、不修改 "
        "production scoring、position_gate 或回测仓位。",
        "",
        "## Manifest 摘要",
        "",
        "| 字段 | 值 |",
        "|---|---|",
    ]
    for field in REQUIRED_PROTOCOL_FIELDS:
        lines.append(f"| `{field}` | {_format_manifest_value(report.manifest.get(field))} |")
    lines.extend(
        [
            "",
            "## 校验结果",
            "",
        ]
    )
    if report.issues:
        lines.extend(
            [
                "| Severity | Code | Field | 说明 | 建议 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity} | `{issue.code}` | `{issue.field}` | "
                f"{issue.message} | {issue.recommendation} |"
            )
    else:
        lines.append("未发现错误或警告。")
    lines.extend(
        [
            "",
            "## 必须保留的治理边界",
            "",
            "- 通过本校验只表示 protocol manifest 可用于后续实验，"
            "不表示权重候选可进入 production。",
            "- 后续仍需 OOS 聚合、参数邻域稳定性、成本压力、DSR/PBO "
            "或同等多重测试折扣、forward shadow 和 owner/rule approval。",
            "- 使用 `ai_after_chatgpt` 主结论时，默认结论窗口不得把 "
            "2022-12-01 之前历史当作 AI-cycle 结论依据。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_calibration_protocol_report(
    report: CalibrationProtocolReport,
    output_path: Path | str,
) -> Path:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_calibration_protocol_report(report), encoding="utf-8")
    return destination


def default_calibration_protocol_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"calibration_protocol_{as_of.isoformat()}.md"


def _append_required_field_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    for field in REQUIRED_PROTOCOL_FIELDS:
        value = manifest.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            _issue(
                issues,
                "ERROR",
                "missing_required_field",
                field,
                f"`{field}` 缺失或为空。",
                "先冻结完整 protocol manifest，再运行任何调权实验。",
            )


def _append_placeholder_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    for field, value in manifest.items():
        for text in _nested_strings(value):
            if any(token in text.upper() for token in PLACEHOLDER_TOKENS):
                _issue(
                    issues,
                    "ERROR",
                    "placeholder_value",
                    str(field),
                    f"`{field}` 仍包含模板占位符。",
                    "替换为本次实验的真实 commit、hash、日期或引用后再校验。",
                )
                break


def _append_date_range_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    start, end = _date_range(manifest.get("date_range"))
    if start is None or end is None:
        if "date_range" in manifest:
            _issue(
                issues,
                "ERROR",
                "invalid_date_range",
                "date_range",
                "`date_range` 必须提供可解析的 start/end。",
                "使用 `{start: YYYY-MM-DD, end: YYYY-MM-DD}`。",
            )
        return
    if start > end:
        _issue(
            issues,
            "ERROR",
            "date_range_reversed",
            "date_range",
            "`date_range.start` 晚于 `date_range.end`。",
            "修正实验窗口后重新校验。",
        )
    if str(manifest.get("market_regime", "")).strip() == "ai_after_chatgpt":
        if start < AI_AFTER_CHATGPT_START:
            _issue(
                issues,
                "WARNING",
                "pre_ai_regime_primary_range",
                "date_range",
                "主实验窗口早于 2022-12-01。",
                "如使用早期历史，只能标为 warm-up、压力测试或 regime 对照。",
            )


def _append_horizon_and_embargo_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    horizon = _positive_int(manifest.get("label_horizon"))
    purge_days = _non_negative_int(manifest.get("purge_days"))
    embargo_days = _non_negative_int(manifest.get("embargo_days"))
    if "label_horizon" in manifest and horizon is None:
        _issue(
            issues,
            "ERROR",
            "invalid_label_horizon",
            "label_horizon",
            "`label_horizon` 必须是正整数或类似 `20D` 的交易日字符串。",
            "按调权标签窗口写入整数交易日。",
        )
    for field, value in (("purge_days", purge_days), ("embargo_days", embargo_days)):
        if field in manifest and value is None:
            _issue(
                issues,
                "ERROR",
                "invalid_non_negative_integer",
                field,
                f"`{field}` 必须是非负整数。",
                "按标签窗口重叠风险设置 purging / embargo。",
            )
    if horizon is not None and horizon > 1 and purge_days == 0 and embargo_days == 0:
        _issue(
            issues,
            "ERROR",
            "missing_purge_or_embargo",
            "purge_days,embargo_days",
            "多日标签窗口没有设置 purging 或 embargo。",
            "至少设置一个正数，避免训练/测试标签窗口重叠。",
        )


def _append_scheme_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    scheme = str(manifest.get("train_validation_test_scheme", "")).lower()
    if not scheme:
        return
    if "nested" not in scheme or "walk" not in scheme:
        _issue(
            issues,
            "ERROR",
            "non_nested_walk_forward_scheme",
            "train_validation_test_scheme",
            "`train_validation_test_scheme` 未声明 nested walk-forward。",
            "内层只调参，外层 test window 只看一次。",
        )


def _append_objective_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    objective = str(manifest.get("objective_version", "")).lower()
    if not objective:
        return
    banned_fragments = ("sharpe_only", "return_only", "cagr_only", "max_return")
    if any(fragment in objective for fragment in banned_fragments):
        _issue(
            issues,
            "ERROR",
            "single_metric_objective",
            "objective_version",
            "调权目标看起来只优化收益、CAGR 或 Sharpe。",
            "使用包含回撤、换手、尾部损失、稳定性和复杂度惩罚的多目标 objective。",
        )


def _append_benchmark_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    benchmarks = _string_set(manifest.get("benchmark_set"))
    if "benchmark_set" in manifest and not benchmarks:
        _issue(
            issues,
            "ERROR",
            "empty_benchmark_set",
            "benchmark_set",
            "`benchmark_set` 为空或不可解析。",
            "至少包含 current_production_model 和一个外部/消融基准。",
        )
        return
    missing_required = REQUIRED_BENCHMARKS - benchmarks
    if missing_required:
        _issue(
            issues,
            "ERROR",
            "missing_required_benchmark",
            "benchmark_set",
            "缺少必须比较的 production 基准："
            + ", ".join(sorted(missing_required)),
            "候选权重必须显式战胜当前生产模型后才可继续。",
        )
    if not benchmarks.intersection(RECOMMENDED_BENCHMARKS):
        _issue(
            issues,
            "WARNING",
            "benchmark_set_too_narrow",
            "benchmark_set",
            "未发现 buy-and-hold、trend-only、no-gate 或等价消融基准。",
            "补充外部基准和消融基准，避免只和空仓或单一生产模型比较。",
        )


def _append_parameter_scope_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    scope = str(manifest.get("parameter_family_scope", "")).strip().lower()
    if not scope:
        return
    if scope in DISALLOWED_PARAMETER_SCOPES:
        _issue(
            issues,
            "ERROR",
            "disallowed_global_parameter_scope",
            "parameter_family_scope",
            "参数范围试图把信号、gate 和执行参数一起全局调优。",
            "按 signal_weights -> gate_rules -> execution_parameters 分层推进。",
        )
    elif scope not in ALLOWED_PARAMETER_SCOPES:
        _issue(
            issues,
            "WARNING",
            "unknown_parameter_family_scope",
            "parameter_family_scope",
            f"未识别的参数分层 `{scope}`。",
            "确认它不会把 alpha、risk gate 和执行纪律混在同一轮调优。",
        )


def _append_trial_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    trials = _positive_int(manifest.get("number_of_trials"))
    if "number_of_trials" in manifest and trials is None:
        _issue(
            issues,
            "ERROR",
            "invalid_number_of_trials",
            "number_of_trials",
            "`number_of_trials` 必须是正整数。",
            "记录所有候选和被拒绝参数组合，不能只保留最终选择。",
        )
        return
    if trials is not None and trials > 1:
        adjustment = str(
            manifest.get("multiple_testing_adjustment")
            or manifest.get("pbo_dsr_method")
            or ""
        ).strip()
        if not adjustment:
            _issue(
                issues,
                "WARNING",
                "missing_multiple_testing_adjustment",
                "multiple_testing_adjustment",
                "多次 trial 未声明 DSR/PBO 或等价多重测试折扣。",
                "补充多重测试折扣方法和试验登记表引用。",
            )
    if manifest.get("parameter_search_space_hash") and not manifest.get("trial_registry_ref"):
        _issue(
            issues,
            "WARNING",
            "missing_trial_registry_ref",
            "trial_registry_ref",
            "已声明搜索空间 hash，但没有 trial registry 引用。",
            "保存所有 trial 的参数、状态和拒绝原因，便于复核 selection bias。",
        )


def _append_production_boundary_issues(
    issues: list[CalibrationProtocolIssue],
    manifest: Mapping[str, Any],
) -> None:
    production_effect = str(manifest.get("production_effect", "none")).strip().lower()
    if production_effect not in {"", "none", "report_only"}:
        _issue(
            issues,
            "ERROR",
            "production_effect_not_allowed",
            "production_effect",
            "`production_effect` 不是 none/report_only。",
            "调权协议校验不得直接改变 production overlay、scoring 或 position gate。",
        )


def _issue(
    issues: list[CalibrationProtocolIssue],
    severity: CalibrationProtocolSeverity,
    code: str,
    field: str,
    message: str,
    recommendation: str,
) -> None:
    issues.append(
        CalibrationProtocolIssue(
            severity=severity,
            code=code,
            field=field,
            message=message,
            recommendation=recommendation,
        )
    )


def _format_manifest_value(value: Any) -> str:
    if value is None or value == "":
        return "未提供"
    if isinstance(value, Mapping):
        return ", ".join(f"{key}={item}" for key, item in value.items()) or "未提供"
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value) or "未提供"
    return str(value)


def _date_range(value: Any) -> tuple[date | None, date | None]:
    if isinstance(value, Mapping):
        return _date_or_none(value.get("start")), _date_or_none(value.get("end"))
    if isinstance(value, (list, tuple)) and len(value) == 2:
        return _date_or_none(value[0]), _date_or_none(value[1])
    if isinstance(value, str) and "/" in value:
        start, end = value.split("/", 1)
        return _date_or_none(start.strip()), _date_or_none(end.strip())
    return None, None


def _date_or_none(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _positive_int(value: Any) -> int | None:
    parsed = _int_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _non_negative_int(value: Any) -> int | None:
    parsed = _int_or_none(value)
    if parsed is None or parsed < 0:
        return None
    return parsed


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().upper()
        if text.endswith("D"):
            text = text[:-1]
        if text.isdigit():
            return int(text)
    return None


def _string_set(value: Any) -> set[str]:
    if isinstance(value, str):
        return {item.strip() for item in value.split(",") if item.strip()}
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    return set()


def _nested_strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_nested_strings(item))
        return tuple(strings)
    if isinstance(value, (list, tuple, set)):
        strings = []
        for item in value:
            strings.extend(_nested_strings(item))
        return tuple(strings)
    return ()


DEFAULT_CALIBRATION_PROTOCOL_PATH = (
    PROJECT_ROOT / "config" / "weights" / "calibration_protocol.yaml"
)
