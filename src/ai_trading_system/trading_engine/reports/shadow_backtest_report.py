from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SHADOW_BACKTEST_REPORT_TYPE = "shadow_parameter_backtest"
SHADOW_BACKTEST_SCHEMA_VERSION = 1


def default_shadow_backtest_output_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_shadow_backtest_summary_json_path(output_root: Path, as_of: date) -> Path:
    return default_shadow_backtest_output_dir(output_root, as_of) / "shadow_backtest_summary.json"


def default_shadow_backtest_summary_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_shadow_backtest_output_dir(output_root, as_of) / "shadow_backtest_summary.md"


def write_shadow_backtest_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_shadow_backtest_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_shadow_backtest_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    baseline = _mapping(payload.get("baseline_result"))
    candidate = _mapping(payload.get("candidate_result"))
    comparison = _mapping(payload.get("relative_comparison"))
    decision = _mapping(payload.get("promotion_decision"))
    data_quality = _mapping(payload.get("data_quality"))
    lines = [
        "# Shadow Parameter Backtest Summary",
        "",
        "## 1. Run Metadata",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- generated_at: `{metadata.get('generated_at', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- market_regime: `{metadata.get('market_regime', 'UNKNOWN')}`",
        f"- date_range: `{metadata.get('date_range', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        "",
        "## 2. Executive Summary",
        "",
        f"- 数据质量状态：`{data_quality.get('status', 'UNKNOWN')}`",
        f"- Baseline version：`{metadata.get('baseline_parameter_version', 'UNKNOWN')}`",
        f"- Candidate version：`{metadata.get('candidate_parameter_version', 'UNKNOWN')}`",
        f"- Promotion status：`{decision.get('status', 'UNKNOWN')}`",
        f"- 结论：{decision.get('reason', 'No decision reason provided.')}",
        "",
        "## 3. Baseline vs Candidate",
        "",
        "| Metric | Baseline | Candidate | Delta |",
        "|---|---:|---:|---:|",
    ]
    for key in ("annualized_return", "max_drawdown", "sharpe_ratio", "turnover"):
        lines.append(
            "| "
            f"`{key}` | "
            f"{_format_metric(baseline.get(key))} | "
            f"{_format_metric(candidate.get(key))} | "
            f"{_format_metric(comparison.get(f'{key}_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 4. Walk-forward Results",
            "",
            "| Window | Train | Validation | Baseline Return | Candidate Return | Status |",
            "|---|---|---|---:|---:|---|",
        ]
    )
    for window in _records(payload.get("walk_forward_windows")):
        baseline_metrics = _mapping(window.get("baseline_metrics"))
        candidate_metrics = _mapping(window.get("candidate_metrics"))
        lines.append(
            "| "
            f"`{window.get('window_id', '')}` | "
            f"{window.get('train_start', '')} to {window.get('train_end', '')} | "
            f"{window.get('validation_start', '')} to {window.get('validation_end', '')} | "
            f"{_format_metric(baseline_metrics.get('annualized_return'))} | "
            f"{_format_metric(candidate_metrics.get('annualized_return'))} | "
            f"`{window.get('status', 'UNKNOWN')}` |"
        )
    lines.extend(
        [
            "",
            "## 5. Parameter Changes",
            "",
            "| Parameter | Baseline | Candidate | Delta | Reason | Risk |",
            "|---|---:|---:|---:|---|---|",
        ]
    )
    changes = _records(payload.get("parameter_changes"))
    if not changes:
        lines.append("| none |  |  |  | No parameter change selected. |  |")
    for change in changes:
        lines.append(
            "| "
            f"`{change.get('name', '')}` | "
            f"{_format_metric(change.get('baseline'))} | "
            f"{_format_metric(change.get('candidate'))} | "
            f"{_format_metric(change.get('delta'))} | "
            f"{_escape_table(change.get('reason', ''))} | "
            f"{_escape_table(change.get('risk', ''))} |"
        )
    lines.extend(
        [
            "",
            "## 6. Attribution",
            "",
            f"- Passing windows ratio：`{payload.get('passing_windows_ratio', 0.0)}`",
            f"- Overfitting risk：`{payload.get('overfitting_risk', 'UNKNOWN')}`",
            "- Hard gates are evaluated but not tuned in v0.1.",
            "",
            "## 7. Risk & Overfitting Warnings",
            "",
        ]
    )
    warnings = [str(item) for item in payload.get("warnings", []) if str(item)]
    lines.extend([f"- {warning}" for warning in warnings] or ["- 未发现额外警告。"])
    lines.extend(
        [
            "",
            "## 8. Promotion Decision",
            "",
            f"- status：`{decision.get('status', 'UNKNOWN')}`",
            f"- reason：{decision.get('reason', '')}",
            f"- hard_rejections：`{', '.join(decision.get('hard_rejections', [])) or 'none'}`",
            "",
            "## 9. Manual Review Checklist",
            "",
        ]
    )
    review_items = [str(item) for item in decision.get("manual_review_items", []) if str(item)]
    lines.extend([f"- {item}" for item in review_items] or ["- 无额外人工复核项。"])
    lines.extend(
        [
            "",
            "## 10. Input / Output Artifacts",
            "",
        ]
    )
    for section_name in ("input_artifacts", "output_artifacts"):
        lines.append(f"### {section_name}")
        for key, value in _mapping(payload.get(section_name)).items():
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def latest_shadow_backtest_summary_path(output_root: Path) -> Path | None:
    candidates = sorted(output_root.glob("*/shadow_backtest_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def load_shadow_backtest_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_shadow_backtest_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != SHADOW_BACKTEST_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != SHADOW_BACKTEST_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in (
        "run_id",
        "generated_at",
        "production_effect",
        "manual_review_required",
        "auto_promotion",
        "baseline_parameter_version",
        "candidate_parameter_version",
    ):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if not isinstance(payload.get("walk_forward_windows"), list):
        issues.append("walk_forward_windows must be a list")
    if not isinstance(payload.get("parameter_changes"), list):
        issues.append("parameter_changes must be a list")
    if not isinstance(payload.get("promotion_decision"), dict):
        issues.append("promotion_decision must be an object")
    return issues


def reports_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"shadow_parameter_backtest_{as_of.isoformat()}.json",
        reports_dir / f"shadow_parameter_backtest_{as_of.isoformat()}.md",
    )


def write_shadow_backtest_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    json_path, markdown_path = reports_alias_paths(reports_dir, as_of)
    return write_shadow_backtest_summary(payload, json_path, markdown_path)


def default_formal_shadow_backtest_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "shadow_backtest"


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _format_metric(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"


def _escape_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
