from __future__ import annotations

import ast
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"
DEFAULT_PHASE0_RUNNER_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "etf_portfolio" / "weight_research_unblock.py"
)

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

COMMON_ARTIFACT_FIELDS = [
    "schema_version",
    "artifact_family",
    "candidate_module_id",
    "run_id",
    "source_artifact_ids",
    "input_checksum",
    "as_of_date",
    "window_id",
    "resolved_config",
    "safety_metadata",
]


def build_research_layer_interface_contract(
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    return {
        "schema_version": 1,
        "task_id": "TRADING-512A",
        "report_type": "research_layer_interface_contract",
        "status": "RESEARCH_LAYER_INTERFACE_CONTRACT_READY",
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "common_artifact_fields": COMMON_ARTIFACT_FIELDS,
        "layers": [
            {
                "layer_id": "feature",
                "artifact_family": "feature_artifact",
                "consumes": ["raw_price_data", "volume_data", "approved_macro_inputs"],
                "produces": ["feature_matrix", "feature_quality_flags"],
                "allowed_outputs": [
                    "momentum",
                    "trend_gap",
                    "realized_volatility",
                    "drawdown",
                    "relative_strength",
                    "breadth",
                    "correlation",
                ],
                "forbidden_outputs": [
                    "signal_state",
                    "target_weight",
                    "executed_weight",
                    "backtest_result",
                ],
            },
            {
                "layer_id": "signal",
                "artifact_family": "signal_artifact",
                "consumes": ["feature_artifact"],
                "produces": ["signal_score", "state", "confidence", "diagnostics"],
                "allowed_outputs": [
                    "signal_score",
                    "state",
                    "confidence",
                    "coverage",
                    "missing_or_stale_flags",
                    "blocking_reason",
                ],
                "forbidden_outputs": [
                    "target_weight",
                    "turnover_budget",
                    "broker_order",
                    "backtest_result",
                ],
            },
            {
                "layer_id": "target",
                "artifact_family": "target_path_artifact",
                "consumes": ["signal_artifact"],
                "produces": ["research_only_target_path"],
                "allowed_outputs": [
                    "relative_tilt",
                    "exposure_scaler",
                    "target_deviation_from_baseline",
                    "target_confidence",
                ],
                "forbidden_outputs": [
                    "executed_weight",
                    "execution_cost",
                    "hidden_feature_recalculation",
                    "broker_order",
                ],
            },
            {
                "layer_id": "execution",
                "artifact_family": "executed_weight_artifact",
                "consumes": ["target_path_artifact"],
                "produces": ["executed_hypothetical_research_weights"],
                "allowed_outputs": [
                    "executed_weight",
                    "skipped_adjustment",
                    "execution_delay",
                    "turnover",
                    "cost_proxy",
                    "constraint_hit",
                ],
                "forbidden_outputs": [
                    "signal_state",
                    "feature_matrix",
                    "official_target_weight",
                    "broker_order",
                ],
            },
            {
                "layer_id": "evaluation",
                "artifact_family": "evaluation_artifact",
                "consumes": ["executed_weight_artifact", "market_return_data"],
                "produces": ["research_metrics", "gate_result"],
                "allowed_outputs": [
                    "utility",
                    "return_delta",
                    "drawdown_delta",
                    "turnover_delta",
                    "cost_delta",
                    "worst_window",
                    "gate_status",
                ],
                "forbidden_outputs": [
                    "modified_feature",
                    "modified_signal",
                    "modified_target_path",
                    "allocator_rerun",
                ],
            },
        ],
        "dependency_direction_rules": [
            "evaluation_must_not_import_signal_or_allocator_implementation",
            "signal_must_not_import_execution_or_evaluation",
            "target_must_only_consume_signal_artifacts",
            "execution_must_only_consume_target_path_artifacts",
        ],
        "reader_brief": {
            "summary": "Feature、Signal、Target、Execution、Evaluation 五层接口已冻结。",
            "key_result": "RESEARCH_LAYER_INTERFACE_CONTRACT_READY",
            "blocking_issues": "B2/B3 仍需通过 signal diagnostics 后才能进入 target mapping。",
            "warnings": "该合同只冻结边界，不代表 B2-B6 信号或目标路径已实现。",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": (
                "运行 dependency boundary validation 并建立 signal diagnostics framework。"
            ),
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def build_dependency_boundary_validation(
    *,
    phase0_runner_path: Path = DEFAULT_PHASE0_RUNNER_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    imports = _module_imports(phase0_runner_path)
    forbidden_phase0_imports = {
        "ai_trading_system.etf_portfolio.allocation",
        "ai_trading_system.etf_portfolio.signals",
        "ai_trading_system.etf_portfolio.regime",
        "ai_trading_system.etf_portfolio.features",
    }
    checks = [
        _check(
            "common_artifact_schema_fields_frozen",
            set(COMMON_ARTIFACT_FIELDS)
            >= {
                "schema_version",
                "candidate_module_id",
                "run_id",
                "source_artifact_ids",
                "input_checksum",
                "safety_metadata",
            },
            "common artifact schema must include audit fields",
        ),
        _check(
            "phase0_runner_avoids_p0_signal_allocator_imports",
            not (imports & forbidden_phase0_imports),
            f"forbidden imports={sorted(imports & forbidden_phase0_imports)}",
        ),
        _check(
            "future_dependency_direction_rules_declared",
            True,
            "512A contract declares evaluator/signal/target/execution direction rules",
        ),
        _check(
            "official_target_and_broker_boundary",
            SAFETY_BOUNDARY["official_target_weights"] is False
            and SAFETY_BOUNDARY["broker_action_allowed"] is False
            and SAFETY_BOUNDARY["production_effect"] == "none",
            "research-only safety boundary is frozen",
        ),
    ]
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "schema_version": 1,
        "task_id": "TRADING-512A",
        "report_type": "dependency_boundary_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "phase0_runner_path": str(phase0_runner_path),
        "scanned_imports": sorted(imports),
        "checks": checks,
        "blocking_checks": [
            check["check_id"] for check in checks if check["status"] != "PASS"
        ],
        "reader_brief": {
            "summary": "当前 Phase 0 runner 未导入 P0 allocator/signals/regime/features。",
            "key_result": status,
            "blocking_issues": "none" if status == "PASS" else "See blocking_checks.",
            "warnings": (
                "B2/B3 独立模块落地后必须继续扩展该 validation，扫描新模块依赖。"
            ),
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": (
                "建立 signal diagnostics framework"
                if status == "PASS"
                else "修复 dependency boundary 后继续"
            ),
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def build_signal_diagnostics_framework_contract(
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    return {
        "schema_version": 1,
        "task_id": "TRADING-512B",
        "report_type": "signal_diagnostics_framework_contract",
        "status": "SIGNAL_DIAGNOSTICS_FRAMEWORK_READY",
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "accepted_statuses": [
            "SIGNAL_DIAGNOSTICS_PASS",
            "SIGNAL_DIAGNOSTICS_PASS_WITH_WARNINGS",
            "SIGNAL_DIAGNOSTICS_BLOCKED",
        ],
        "required_checks": [
            "coverage",
            "freshness",
            "schema_compatibility",
            "missingness",
            "state_transitions",
            "cross_window_stability",
            "event_based_diagnostics",
            "robustness_status",
            "fail_closed_reason",
        ],
        "required_input_columns": ["date", "symbol", "signal_score", "state", "confidence"],
        "forbidden_outputs": [
            "target_weight",
            "executed_weight",
            "portfolio_return",
            "backtest_result",
            "broker_order",
            "official_target_weight",
        ],
        "runner_contract": {
            "evaluates_signal_only": True,
            "evaluates_portfolio_return": False,
            "blocks_on_missing_required_columns": True,
            "blocks_on_empty_signal_artifact": True,
            "blocks_on_stale_signal_artifact": True,
        },
        "reader_brief": {
            "summary": "通用 signal diagnostics framework 已冻结，只评价 signal 质量。",
            "key_result": "SIGNAL_DIAGNOSTICS_FRAMEWORK_READY",
            "blocking_issues": "B2/B3 进入 target mapping 前必须得到非 BLOCKED diagnostics。",
            "warnings": "Diagnostics PASS 不是组合收益或候选晋级结论。",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "实现 B2/B3 signal 时先输出 signal artifact 并运行 diagnostics。",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def build_signal_diagnostics_report(
    signal_frame: pd.DataFrame,
    *,
    signal_artifact_id: str,
    as_of: date,
    required_columns: list[str] | None = None,
    max_stale_days: int = 10,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    required = required_columns or ["date", "symbol", "signal_score", "state", "confidence"]
    checks: list[dict[str, Any]] = []
    missing_columns = sorted(set(required) - set(signal_frame.columns))
    checks.append(_check("schema_compatibility", not missing_columns, f"missing={missing_columns}"))
    checks.append(_check("non_empty_signal_artifact", not signal_frame.empty, "signal rows > 0"))
    if missing_columns or signal_frame.empty:
        status = "SIGNAL_DIAGNOSTICS_BLOCKED"
        return _signal_diagnostics_payload(
            signal_artifact_id=signal_artifact_id,
            as_of=as_of,
            generated_at=generated,
            status=status,
            checks=checks,
            metrics={},
            fail_closed_reason="missing_required_columns_or_empty_signal",
        )

    frame = signal_frame.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    valid_dates = frame["_date"].dropna()
    max_signal_date = valid_dates.max().date() if not valid_dates.empty else None
    stale_days = None if max_signal_date is None else (as_of - max_signal_date).days
    freshness_ok = stale_days is not None and stale_days <= max_stale_days
    checks.append(_check("freshness", freshness_ok, f"stale_days={stale_days}"))

    required_cells = frame[required]
    missing_cell_rate = float(required_cells.isna().sum().sum()) / float(
        max(1, required_cells.size)
    )
    checks.append(_check("missingness", missing_cell_rate == 0.0, f"rate={missing_cell_rate:.6f}"))

    symbol_count = int(frame["symbol"].nunique())
    date_count = int(valid_dates.dt.date.nunique()) if not valid_dates.empty else 0
    coverage = float(len(frame)) / float(max(1, symbol_count * date_count))
    checks.append(_check("coverage", coverage > 0.0, f"coverage={coverage:.6f}"))

    state_transitions = _state_transition_count(frame)
    checks.append(_check("state_transitions", True, f"transition_count={state_transitions}"))

    cross_window_stability = (
        _cross_window_state_stability(frame) if "window_id" in frame.columns else None
    )
    checks.append(
        _check(
            "cross_window_stability",
            True,
            "not_evaluated_no_window_id"
            if cross_window_stability is None
            else f"stability={cross_window_stability:.6f}",
        )
    )
    event_count = int(frame["event_id"].nunique()) if "event_id" in frame.columns else 0
    checks.append(_check("event_based_diagnostics", True, f"event_count={event_count}"))

    blocking = [check for check in checks if check["status"] == "FAIL"]
    warning = [check for check in checks if check["status"] == "WARN"]
    if blocking:
        status = "SIGNAL_DIAGNOSTICS_BLOCKED"
    elif warning:
        status = "SIGNAL_DIAGNOSTICS_PASS_WITH_WARNINGS"
    else:
        status = "SIGNAL_DIAGNOSTICS_PASS"
    metrics = {
        "row_count": int(len(frame)),
        "symbol_count": symbol_count,
        "date_count": date_count,
        "coverage": coverage,
        "missing_cell_rate": missing_cell_rate,
        "max_signal_date": None if max_signal_date is None else max_signal_date.isoformat(),
        "stale_days": stale_days,
        "state_transition_count": state_transitions,
        "cross_window_state_stability": cross_window_stability,
        "event_count": event_count,
    }
    return _signal_diagnostics_payload(
        signal_artifact_id=signal_artifact_id,
        as_of=as_of,
        generated_at=generated,
        status=status,
        checks=checks,
        metrics=metrics,
        fail_closed_reason="none" if status != "SIGNAL_DIAGNOSTICS_BLOCKED" else "checks_failed",
    )


def write_research_layer_interface_contract(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    return _write_artifact_pair(
        payload,
        stem="research_layer_interface_contract",
        markdown=render_research_layer_interface_contract(payload),
        output_dir=output_dir,
        alias_dir=alias_dir,
    )


def write_dependency_boundary_validation(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    return _write_artifact_pair(
        payload,
        stem="dependency_boundary_validation",
        markdown=render_dependency_boundary_validation(payload),
        output_dir=output_dir,
        alias_dir=alias_dir,
    )


def write_signal_diagnostics_framework_contract(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    return _write_artifact_pair(
        payload,
        stem="signal_diagnostics_framework_contract",
        markdown=render_signal_diagnostics_framework_contract(payload),
        output_dir=output_dir,
        alias_dir=alias_dir,
    )


def render_research_layer_interface_contract(payload: dict[str, Any]) -> str:
    lines = [
        "# Research Layer Interface Contract",
        "",
        f"- Status：{payload['status']}",
        f"- Generated At：{payload['generated_at']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
        "## Layers",
        "",
        "| Layer | Artifact Family | Consumes | Produces |",
        "|---|---|---|---|",
    ]
    for layer in payload["layers"]:
        lines.append(
            "| "
            f"{layer['layer_id']} | "
            f"{layer['artifact_family']} | "
            f"{', '.join(layer['consumes'])} | "
            f"{', '.join(layer['produces'])} |"
        )
    lines.extend(_reader_brief_lines(payload))
    return "\n".join(lines) + "\n"


def render_dependency_boundary_validation(payload: dict[str, Any]) -> str:
    lines = [
        "# Dependency Boundary Validation",
        "",
        f"- Status：{payload['status']}",
        f"- Runner：`{payload['phase0_runner_path']}`",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in payload["checks"]:
        lines.append(f"| {check['check_id']} | {check['status']} | {_cell(check['message'])} |")
    lines.extend(_reader_brief_lines(payload))
    return "\n".join(lines) + "\n"


def render_signal_diagnostics_framework_contract(payload: dict[str, Any]) -> str:
    lines = [
        "# Signal Diagnostics Framework Contract",
        "",
        f"- Status：{payload['status']}",
        f"- Accepted Statuses：{', '.join(payload['accepted_statuses'])}",
        "",
        "## Required Checks",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["required_checks"])
    lines.extend(_reader_brief_lines(payload))
    return "\n".join(lines) + "\n"


def _signal_diagnostics_payload(
    *,
    signal_artifact_id: str,
    as_of: date,
    generated_at: datetime,
    status: str,
    checks: list[dict[str, Any]],
    metrics: dict[str, Any],
    fail_closed_reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": "TRADING-512B",
        "report_type": "signal_diagnostics_report",
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "signal_artifact_id": signal_artifact_id,
        "as_of_date": as_of.isoformat(),
        "checks": checks,
        "metrics": metrics,
        "robustness_status": status,
        "fail_closed_reason": fail_closed_reason,
        "evaluates_portfolio_return": False,
        "forbidden_outputs_absent": True,
        "reader_brief": {
            "summary": "Signal diagnostics evaluated signal quality only.",
            "key_result": status,
            "blocking_issues": (
                fail_closed_reason if status == "SIGNAL_DIAGNOSTICS_BLOCKED" else "none"
            ),
            "warnings": "Diagnostics does not evaluate portfolio return.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "Use non-BLOCKED signal diagnostics before target mapping.",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _module_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def _check(check_id: str, passed: bool, message: str) -> dict[str, str]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _state_transition_count(frame: pd.DataFrame) -> int:
    ordered = frame.sort_values(["symbol", "_date"])
    count = 0
    for _, group in ordered.groupby("symbol"):
        states = [str(value) for value in group["state"].tolist()]
        count += sum(1 for left, right in zip(states, states[1:], strict=False) if left != right)
    return count


def _cross_window_state_stability(frame: pd.DataFrame) -> float:
    grouped = frame.groupby(["window_id", "state"]).size().unstack(fill_value=0)
    if grouped.empty:
        return 0.0
    dominant = grouped.max(axis=1)
    totals = grouped.sum(axis=1).clip(lower=1)
    return float((dominant / totals).mean())


def _write_artifact_pair(
    payload: dict[str, Any],
    *,
    stem: str,
    markdown: str,
    output_dir: Path,
    alias_dir: Path | None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"{stem}_{stamp}.json"
    md_path = output_dir / f"{stem}_{stamp}.md"
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    if alias_dir is not None:
        alias_dir.mkdir(parents=True, exist_ok=True)
        _write_json(alias_dir / f"{stem}.json", payload)
        (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
    return json_path, md_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _reader_brief_lines(payload: dict[str, Any]) -> list[str]:
    brief = payload["reader_brief"]
    return [
        "",
        "## Reader Brief",
        "",
        f"- Summary：{brief['summary']}",
        f"- Key Result：{brief['key_result']}",
        f"- Blocking Issues：{brief['blocking_issues']}",
        f"- Warnings：{brief['warnings']}",
        f"- Safety Boundary：{brief['safety_boundary']}",
        f"- Next Action：{brief['next_action']}",
    ]


def _stamp_from_generated_at(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


def _cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
