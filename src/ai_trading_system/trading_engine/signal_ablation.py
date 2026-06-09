from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
    PortfolioSimulationResult,
    simulate_parameter_portfolio,
)
from ai_trading_system.trading_engine.backtesting.walk_forward import (
    generate_walk_forward_windows,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    load_signal_ablation_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ProductionParameters,
    ShadowBacktestConfig,
    SignalAblationConfig,
    SignalAblationThresholds,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    NEUTRAL_FALLBACK_SIGNALS,
    PRICE_DERIVED_SIGNALS,
    PROXY_SIGNALS,
    REQUIRED_SIGNALS,
    load_signal_snapshot_payload,
    signal_snapshot_frames,
    signal_snapshot_summary,
)

SIGNAL_ABLATION_SCHEMA_VERSION = 1
SIGNAL_ABLATION_REPORT_TYPE = "signal_ablation"
SIGNAL_ABLATION_ALIAS_REPORT_TYPE = "signal_ablation_report"

ContributionClass = Literal[
    "positive",
    "negative",
    "neutral",
    "unstable",
    "insufficient_data",
]
ContributionDiagnosticStatus = Literal[
    "VALID",
    "BELOW_THRESHOLD",
    "NO_SCORE_IMPACT",
    "NO_PORTFOLIO_IMPACT",
    "NOT_USED_IN_SCORE",
    "FALLBACK_SIGNAL",
    "INSUFFICIENT_DATA",
    "IMPLEMENTATION_WARNING",
]

DELTA_METRICS: tuple[str, ...] = (
    "cumulative_return",
    "annualized_return",
    "max_drawdown",
    "volatility",
    "downside_volatility",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "turnover",
    "number_of_rebalances",
    "drawdown_reduction_ratio",
    "missed_upside_rate",
    "false_risk_alert_rate",
)

PRIMARY_DELTA_KEYS: tuple[str, ...] = (
    "annualized_return_delta",
    "max_drawdown_delta",
    "sharpe_ratio_delta",
    "turnover_delta",
    "drawdown_reduction_delta",
)


@dataclass(frozen=True)
class SignalAblationRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path


def default_signal_ablation_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "signal_ablation"


def default_signal_ablation_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_signal_ablation_json_path(output_root: Path, as_of: date) -> Path:
    return default_signal_ablation_dir(output_root, as_of) / "signal_ablation_summary.json"


def default_signal_ablation_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_signal_ablation_dir(output_root, as_of) / "signal_ablation_summary.md"


def latest_signal_ablation_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_signal_ablation_root()
    candidates = sorted(root.glob("*/signal_ablation_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"signal_ablation_{as_of.isoformat()}.json",
        reports_dir / f"signal_ablation_{as_of.isoformat()}.md",
    )


def run_signal_ablation(
    *,
    as_of: date | None = None,
    signals: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
    dry_run: bool = False,
    mode: str | None = None,
    generated_at: datetime | None = None,
) -> SignalAblationRun:
    payload = build_signal_ablation_payload(
        as_of=as_of,
        signals=signals,
        config_path=config_path,
        dry_run=dry_run,
        mode=mode,
        generated_at=generated_at,
    )
    resolved_as_of = _payload_date(payload)
    root = _output_root(load_signal_ablation_config(config_path), dry_run=dry_run)
    json_path = default_signal_ablation_json_path(root, resolved_as_of)
    markdown_path = default_signal_ablation_markdown_path(root, resolved_as_of)
    write_signal_ablation_report(payload, json_path, markdown_path)
    return SignalAblationRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
    )


def build_signal_ablation_payload(
    *,
    as_of: date | None = None,
    signals: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
    dry_run: bool = False,
    mode: str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_signal_ablation_config(resolved_config_path)
    shadow_config_path = resolve_project_path(config.shadow_backtest_config_path)
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    prices_path = resolve_project_path(shadow_config.data.prices_path)
    prices = shadow_backtest_module._read_prices(prices_path)
    resolved_as_of = as_of or shadow_backtest_module._latest_price_date(prices) or generated.date()
    requested_signals = _requested_signals(signals)
    selected_mode = mode or config.default_mode
    if selected_mode != "remove_one_signal":
        return _unsupported_mode_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            mode=selected_mode,
        )

    baseline_path = resolve_project_path(shadow_config.baseline_parameters_path)
    try:
        baseline = load_production_parameters(baseline_path)
    except (OSError, ValueError) as exc:
        return _failure_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            status="FAILED",
            reason=f"production baseline unavailable: {exc}",
        )

    data_quality_report_path = default_quality_report_path(
        _data_quality_report_dir(shadow_config, dry_run=dry_run),
        resolved_as_of,
    )
    data_quality_report = shadow_backtest_module._run_data_quality_gate(
        config=shadow_config,
        baseline=baseline,
        as_of=resolved_as_of,
        report_path=data_quality_report_path,
    )
    diagnostic_output_root = _diagnostic_output_root(shadow_config, dry_run=dry_run)
    diagnostic_run = run_backtest_input_diagnostics(
        as_of=resolved_as_of,
        config_path=shadow_config_path,
        output_root=diagnostic_output_root,
        generated_at=datetime(
            resolved_as_of.year,
            resolved_as_of.month,
            resolved_as_of.day,
            tzinfo=UTC,
        ),
    )
    data_quality_status, warnings = shadow_backtest_module._shadow_data_quality_status(
        config=shadow_config,
        baseline=baseline,
        prices=prices,
        as_of=resolved_as_of,
        data_quality_report=data_quality_report,
    )
    data_quality_status, warnings = shadow_backtest_module._merge_backtest_input_diagnostic_status(
        data_quality_status,
        warnings,
        diagnostic_run.payload,
    )
    backtest_mode = _diagnostic_backtest_mode(diagnostic_run.payload)
    signal_snapshot_path = _signal_snapshot_path_from_diagnostics(diagnostic_run.payload)
    signal_snapshot_payload = (
        load_signal_snapshot_payload(signal_snapshot_path)
        if signal_snapshot_path is not None
        else {}
    )
    snapshot_summary = signal_snapshot_summary(signal_snapshot_payload)
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            shadow_config=shadow_config,
            baseline=baseline,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            diagnostic_path=diagnostic_run.json_path,
            diagnostic_payload=diagnostic_run.payload,
            signal_snapshot_path=signal_snapshot_path,
            signal_snapshot_summary=snapshot_summary,
            requested_signals=requested_signals,
            mode=selected_mode,
            warnings=warnings,
        )
    if not signal_snapshot_payload:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            shadow_config=shadow_config,
            baseline=baseline,
            data_quality_status="FAILED",
            data_quality_report_path=data_quality_report_path,
            diagnostic_path=diagnostic_run.json_path,
            diagnostic_payload=diagnostic_run.payload,
            signal_snapshot_path=signal_snapshot_path,
            signal_snapshot_summary=snapshot_summary,
            requested_signals=requested_signals,
            mode=selected_mode,
            warnings=[*warnings, "signal snapshot missing; ablation not run"],
        )

    signal_frames = signal_snapshot_frames(signal_snapshot_payload)
    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, resolved_as_of)
    windows = generate_walk_forward_windows(trading_dates, shadow_config.walk_forward)
    full_start = shadow_backtest_module._full_result_start(windows, trading_dates, shadow_config)
    full_end = resolved_as_of
    baseline_result = simulate_parameter_portfolio(
        prices,
        baseline,
        baseline.weights,
        shadow_config.transaction_cost,
        start=full_start,
        end=full_end,
        signal_frames=signal_frames,
    )
    baseline_by_window = {
        window.window_id: simulate_parameter_portfolio(
            prices,
            baseline,
            baseline.weights,
            shadow_config.transaction_cost,
            start=window.validation_start,
            end=window.validation_end,
            signal_frames=signal_frames,
        )
        for window in windows
    }
    contributions = [
        _signal_contribution(
            signal=signal,
            baseline=baseline,
            shadow_config=shadow_config,
            prices=prices,
            signal_frames=signal_frames,
            signal_snapshot_payload=signal_snapshot_payload,
            baseline_result=baseline_result,
            baseline_by_window=baseline_by_window,
            windows=windows,
            full_start=full_start,
            full_end=full_end,
            thresholds=config.thresholds,
            min_walk_forward_windows=config.stability.min_walk_forward_windows,
            score_delta_epsilon=config.diagnostics.score_delta_epsilon,
            portfolio_weight_delta_epsilon=config.diagnostics.portfolio_weight_delta_epsilon,
            non_neutral_value_epsilon=config.diagnostics.non_neutral_value_epsilon,
        )
        for signal in requested_signals
    ]
    diagnostics = _diagnostics_summary(contributions)
    summary = _summary(contributions, snapshot_summary, diagnostics)
    status = _metadata_status(
        data_quality_status=data_quality_status,
        snapshot_status=str(snapshot_summary.get("status", "UNKNOWN")),
    )
    baseline_source = _baseline_source_path(shadow_config, resolved_as_of, dry_run=dry_run)
    return {
        "schema_version": SIGNAL_ABLATION_SCHEMA_VERSION,
        "report_type": SIGNAL_ABLATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-ablation-{resolved_as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": status,
            "backtest_mode": backtest_mode,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "market_regime": shadow_config.market_regime.id,
            "market_regime_anchor": shadow_config.market_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": shadow_config.market_regime.anchor_event,
            "requested_date_range": {
                "start": full_start.isoformat(),
                "end": full_end.isoformat(),
            },
            "ablation_mode": selected_mode,
            "config_path": str(resolved_config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(
                resolved_config_path,
                shadow_config_path,
                signal_snapshot_path,
            ),
        },
        "input_artifacts": {
            "signal_ablation_config": str(resolved_config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "baseline_parameters": str(baseline_path),
            "prices": str(prices_path),
            "data_quality_report": str(data_quality_report_path),
            "backtest_input_diagnostics": str(diagnostic_run.json_path),
            "signal_snapshot": "" if signal_snapshot_path is None else str(signal_snapshot_path),
        },
        "baseline": {
            "source": str(baseline_source) if baseline_source.exists() else "computed_in_run",
            "metrics": baseline_result.metrics.to_dict(),
        },
        "data_quality": {
            "status": data_quality_status,
            "validate_data_status": data_quality_report.status,
            "quality_report_path": str(data_quality_report_path),
            "diagnostic_report": str(diagnostic_run.json_path),
            "backtest_mode": backtest_mode,
            "signal_snapshot_status": snapshot_summary.get("status", "UNKNOWN"),
            "can_run_shadow_backtest": _diagnostic_can_run_shadow_backtest(diagnostic_run.payload),
            "can_promote_candidate": _diagnostic_can_promote_candidate(diagnostic_run.payload),
        },
        "signal_snapshot": {
            "path": "" if signal_snapshot_path is None else str(signal_snapshot_path),
            **snapshot_summary,
        },
        "policy": {
            "version": config.version,
            "thresholds": config.thresholds.model_dump(mode="json"),
            "min_walk_forward_windows": config.stability.min_walk_forward_windows,
            "diagnostics": config.diagnostics.model_dump(mode="json"),
            "promotion_effect": "manual_review_only",
        },
        "diagnostics": diagnostics,
        "signal_contributions": contributions,
        "summary": summary,
        "warnings": list(dict.fromkeys(warnings)),
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "candidate_promotion_triggered": False,
            "broker_action": False,
            "trading_action": False,
        },
    }


def write_signal_ablation_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_signal_ablation_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_signal_ablation_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": SIGNAL_ABLATION_ALIAS_REPORT_TYPE,
        "source_report_type": SIGNAL_ABLATION_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_signal_ablation_report(alias_payload, json_path, markdown_path)


def render_signal_ablation_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    baseline = _mapping(payload.get("baseline"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    summary = _mapping(payload.get("summary"))
    diagnostics = _mapping(payload.get("diagnostics"))
    lines = [
        "# Signal Ablation Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- backtest_mode: `{metadata.get('backtest_mode', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- positive_signals: `{', '.join(_strings(summary.get('positive_signals'))) or 'none'}`",
        f"- negative_signals: `{', '.join(_strings(summary.get('negative_signals'))) or 'none'}`",
        f"- fallback_signals: `{', '.join(_strings(summary.get('fallback_signals'))) or 'none'}`",
        (
            "- can_support_candidate_promotion: "
            f"`{summary.get('can_support_candidate_promotion', False)}`"
        ),
        f"- reason: {summary.get('reason', '')}",
        "",
        "## 2. Baseline Backtest",
        "",
        f"- source: `{baseline.get('source', 'UNKNOWN')}`",
        f"- cumulative_return: `{_format_metric(baseline_metrics.get('cumulative_return'))}`",
        f"- annualized_return: `{_format_metric(baseline_metrics.get('annualized_return'))}`",
        f"- max_drawdown: `{_format_metric(baseline_metrics.get('max_drawdown'))}`",
        f"- sharpe_ratio: `{_format_metric(baseline_metrics.get('sharpe_ratio'))}`",
        f"- turnover: `{_format_metric(baseline_metrics.get('turnover'))}`",
        "",
        "## Implementation Diagnostics",
        "",
        (
            "- all_real_signals_used_in_score: "
            f"`{diagnostics.get('all_real_signals_used_in_score', False)}`"
        ),
        (
            "- all_ablation_runs_changed_scores: "
            f"`{diagnostics.get('all_ablation_runs_changed_scores', False)}`"
        ),
        (
            "- any_score_to_portfolio_disconnect: "
            f"`{diagnostics.get('any_score_to_portfolio_disconnect', False)}`"
        ),
        (
            "- implementation_warnings: "
            f"`{len(_strings(diagnostics.get('implementation_warnings')))}`"
        ),
        "",
        "## Signal Usage Diagnostics",
        "",
        (
            "| Signal | In Snapshot | In Weights | Effective Weight | "
            "Non-neutral Ratio | Used in Score |"
        ),
        "|---|---:|---:|---:|---:|---:|",
    ]
    contributions = _records(payload.get("signal_contributions"))
    for contribution in contributions:
        usage = _mapping(contribution.get("signal_usage_diagnostics"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"{usage.get('present_in_snapshot', False)} | "
            f"{usage.get('present_in_weights', False)} | "
            f"{_format_metric(usage.get('effective_weight'))} | "
            f"{_format_metric(usage.get('non_neutral_value_ratio'))} | "
            f"{usage.get('used_in_score_calculation', False)} |"
        )
    lines.extend(
        [
            "",
            "## Score Impact Diagnostics",
            "",
            (
                "| Signal | Mean Abs Score Delta | Max Abs Score Delta | "
                "Affected Asset Days | Affected Ratio |"
            ),
            "|---|---:|---:|---:|---:|",
        ]
    )
    for contribution in contributions:
        impact = _mapping(contribution.get("score_impact"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"{_format_metric(impact.get('mean_abs_score_delta'))} | "
            f"{_format_metric(impact.get('max_abs_score_delta'))} | "
            f"{impact.get('affected_asset_days', 0)} | "
            f"{_format_metric(impact.get('affected_asset_day_ratio'))} |"
        )
    lines.extend(
        [
            "",
            "## Portfolio Impact Diagnostics",
            "",
            (
                "| Signal | Mean Abs Weight Delta | Max Abs Weight Delta | "
                "Changed Days | Change Ratio |"
            ),
            "|---|---:|---:|---:|---:|",
        ]
    )
    for contribution in contributions:
        impact = _mapping(contribution.get("portfolio_impact"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"{_format_metric(impact.get('mean_abs_weight_delta'))} | "
            f"{_format_metric(impact.get('max_abs_weight_delta'))} | "
            f"{impact.get('rebalance_days_changed', 0)} | "
            f"{_format_metric(impact.get('rebalance_change_ratio'))} |"
        )
    lines.extend(
        [
            "",
            "## Threshold Diagnostics",
            "",
            (
                "| Signal | Sharpe Delta | Sharpe Passed | Return Delta | "
                "Return Passed | Drawdown Delta | Drawdown Passed |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for contribution in contributions:
        threshold = _mapping(contribution.get("threshold_diagnostics"))
        sharpe = _mapping(threshold.get("sharpe_delta"))
        annualized = _mapping(threshold.get("annualized_return_delta"))
        drawdown = _mapping(threshold.get("max_drawdown_delta"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"{_format_metric(sharpe.get('value'))} | "
            f"{sharpe.get('passed', False)} | "
            f"{_format_metric(annualized.get('value'))} | "
            f"{annualized.get('passed', False)} | "
            f"{_format_metric(drawdown.get('value'))} | "
            f"{drawdown.get('passed', False)} |"
        )
    lines.extend(
        [
            "",
            "## Why No Promotion-credit Signals?",
            "",
            f"- {summary.get('no_promotion_credit_reason', '')}",
            "",
            "## 3. Signal Contribution Ranking",
            "",
            (
                "| Signal | Quality | Class | Diagnostic Status | Promotion Credit | "
                "Sharpe Delta | Return Delta | Drawdown Delta |"
            ),
            "|---|---|---|---|---|---:|---:|---:|",
        ]
    )
    for contribution in sorted(contributions, key=_ranking_key):
        delta = _mapping(contribution.get("remove_one_signal_delta"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"`{contribution.get('quality', '')}` | "
            f"`{contribution.get('contribution_class', '')}` | "
            f"`{contribution.get('diagnostic_status', '')}` | "
            f"`{contribution.get('promotion_credit_allowed', False)}` | "
            f"{_format_metric(delta.get('sharpe_ratio_delta'))} | "
            f"{_format_metric(delta.get('annualized_return_delta'))} | "
            f"{_format_metric(delta.get('max_drawdown_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 4. Per-signal Ablation Results",
            "",
        ]
    )
    for contribution in contributions:
        delta = _mapping(contribution.get("remove_one_signal_delta"))
        lines.extend(
            [
                f"### `{contribution.get('signal', '')}`",
                "",
                f"- quality: `{contribution.get('quality', '')}`",
                f"- status: `{contribution.get('signal_status', '')}`",
                f"- contribution_class: `{contribution.get('contribution_class', '')}`",
                f"- diagnostic_status: `{contribution.get('diagnostic_status', '')}`",
                f"- classification_reason: {contribution.get('classification_reason', '')}",
                (
                    "- promotion_credit_allowed: "
                    f"`{contribution.get('promotion_credit_allowed', False)}`"
                ),
                (
                    "- annualized_return_delta: "
                    f"`{_format_metric(delta.get('annualized_return_delta'))}`"
                ),
                f"- sharpe_ratio_delta: `{_format_metric(delta.get('sharpe_ratio_delta'))}`",
                f"- max_drawdown_delta: `{_format_metric(delta.get('max_drawdown_delta'))}`",
                f"- turnover_delta: `{_format_metric(delta.get('turnover_delta'))}`",
                "",
            ]
        )
    lines.extend(
        [
            "## 5. Walk-forward Stability",
            "",
            "| Signal | Positive | Neutral | Negative | Stability Ratio |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for contribution in contributions:
        stability = _mapping(contribution.get("window_stability"))
        lines.append(
            "| "
            f"`{contribution.get('signal', '')}` | "
            f"{stability.get('positive_windows', 0)} | "
            f"{stability.get('neutral_windows', 0)} | "
            f"{stability.get('negative_windows', 0)} | "
            f"{_format_metric(stability.get('stability_ratio'))} |"
        )
    lines.extend(
        [
            "",
            "## 6. Proxy and Fallback Signal Warnings",
            "",
        ]
    )
    warning_lines = []
    for contribution in contributions:
        for warning in _strings(contribution.get("warnings")):
            warning_lines.append(f"- `{contribution.get('signal', '')}`: {warning}")
    lines.extend(warning_lines or ["- No proxy/fallback warning beyond current status."])
    lines.extend(
        [
            "",
            "## 7. Promotion Eligibility Impact",
            "",
            (
                "- can_support_candidate_promotion: "
                f"`{summary.get('can_support_candidate_promotion', False)}`"
            ),
            (
                "- promotion_credit_signals: "
                f"`{', '.join(_strings(summary.get('promotion_credit_signals'))) or 'none'}`"
            ),
            f"- reason: {summary.get('reason', '')}",
            "",
            "## 8. Manual Review Checklist",
            "",
            "- Review positive price-derived signals before changing weights.",
            "- Treat proxy and fallback contribution as risk evidence, not promotion credit.",
            "- Keep candidate promotion disabled while signal snapshot quality remains LIMITED.",
            "",
            "## 9. Input / Output Artifacts",
            "",
        ]
    )
    for section in ("input_artifacts", "output_artifacts"):
        artifacts = _mapping(payload.get(section))
        if not artifacts:
            continue
        lines.append(f"### {section}")
        for key, value in artifacts.items():
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_signal_ablation_diagnostics(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    diagnostics = _mapping(payload.get("diagnostics"))
    lines = [
        "Signal Ablation Diagnostics",
        "",
        f"status: {metadata.get('status', 'UNKNOWN')}",
        f"backtest_mode: {metadata.get('backtest_mode', 'UNKNOWN')}",
        (
            "real_signals_used_in_score: "
            f"{_yes_no(diagnostics.get('all_real_signals_used_in_score'))}"
        ),
        (
            "ablation_changed_scores: "
            f"{_yes_no(diagnostics.get('all_ablation_runs_changed_scores'))}"
        ),
        (
            "score_to_portfolio_disconnect: "
            f"{_yes_no(diagnostics.get('any_score_to_portfolio_disconnect'))}"
        ),
        ("no_promotion_credit_reason: " f"{summary.get('no_promotion_credit_reason', '')}"),
        "",
    ]
    for contribution in _records(payload.get("signal_contributions")):
        usage = _mapping(contribution.get("signal_usage_diagnostics"))
        score_impact = _mapping(contribution.get("score_impact"))
        portfolio_impact = _mapping(contribution.get("portfolio_impact"))
        lines.extend(
            [
                f"{contribution.get('signal', 'UNKNOWN')}:",
                ("  used_in_score: " f"{_yes_no(usage.get('used_in_score_calculation'))}"),
                f"  effective_weight: {_format_metric(usage.get('effective_weight'))}",
                (
                    "  non_neutral_value_ratio: "
                    f"{_format_metric(usage.get('non_neutral_value_ratio'))}"
                ),
                (
                    "  score_impact: "
                    f"{_impact_label(score_impact.get('max_abs_score_delta'))}"
                    f" (max={_format_metric(score_impact.get('max_abs_score_delta'))})"
                ),
                (
                    "  portfolio_impact: "
                    f"{_impact_label(portfolio_impact.get('max_abs_weight_delta'))}"
                    f" (max={_format_metric(portfolio_impact.get('max_abs_weight_delta'))})"
                ),
                f"  classification: {contribution.get('contribution_class', 'UNKNOWN')}",
                f"  diagnostic_status: {contribution.get('diagnostic_status', 'UNKNOWN')}",
                f"  reason: {contribution.get('classification_reason', '')}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def load_signal_ablation_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_signal_ablation_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != SIGNAL_ABLATION_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != SIGNAL_ABLATION_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if not isinstance(payload.get("baseline"), dict):
        issues.append("baseline must be an object")
    if not isinstance(payload.get("signal_contributions"), list):
        issues.append("signal_contributions must be a list")
    for item in _records(payload.get("signal_contributions")):
        signal = str(item.get("signal") or "")
        if not signal:
            issues.append("signal_contribution missing signal")
        if item.get("contribution_class") not in {
            "positive",
            "negative",
            "neutral",
            "unstable",
            "insufficient_data",
        }:
            issues.append(f"{signal} has unsupported contribution_class")
        if not item.get("classification_reason"):
            issues.append(f"{signal} missing classification_reason")
        if item.get("diagnostic_status") not in {
            "VALID",
            "BELOW_THRESHOLD",
            "NO_SCORE_IMPACT",
            "NO_PORTFOLIO_IMPACT",
            "NOT_USED_IN_SCORE",
            "FALLBACK_SIGNAL",
            "INSUFFICIENT_DATA",
            "IMPLEMENTATION_WARNING",
        }:
            issues.append(f"{signal} has unsupported diagnostic_status")
        for key in (
            "signal_usage_diagnostics",
            "score_impact",
            "portfolio_impact",
            "threshold_diagnostics",
        ):
            if not isinstance(item.get(key), dict):
                issues.append(f"{signal} missing {key}")
        if item.get("quality") == "neutral_fallback" and item.get("promotion_credit_allowed"):
            issues.append(f"{signal} fallback cannot receive promotion credit")
    diagnostics = _mapping(payload.get("diagnostics"))
    if payload.get("signal_contributions") and not diagnostics:
        issues.append("diagnostics must be present")
    if diagnostics and diagnostics.get("classification_reasons_present") is not True:
        issues.append("classification_reasons_present must be true")
    summary = _mapping(payload.get("summary"))
    if summary.get("can_support_candidate_promotion") is not False:
        issues.append("can_support_candidate_promotion must remain false in v0.1")
    if payload.get("signal_contributions") and not summary.get("no_promotion_credit_reason"):
        issues.append("summary.no_promotion_credit_reason must be present")
    safety = _mapping(payload.get("safety"))
    if safety.get("candidate_promotion_triggered") not in {False, None}:
        issues.append("candidate_promotion_triggered must be false")
    return issues


def signal_ablation_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("signal-ablation-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer signal ablation date from {source_path}") from exc


def classify_ablation_delta(
    delta: dict[str, float],
    thresholds: SignalAblationThresholds,
) -> ContributionClass:
    annualized_delta = float(delta.get("annualized_return_delta", 0.0))
    sharpe_delta = float(delta.get("sharpe_ratio_delta", 0.0))
    max_drawdown_delta = float(delta.get("max_drawdown_delta", 0.0))
    turnover_delta = float(delta.get("turnover_delta", 0.0))
    if (
        abs(annualized_delta) <= thresholds.annualized_return_noise_band
        and abs(sharpe_delta) <= thresholds.sharpe_noise_band
        and abs(max_drawdown_delta) <= thresholds.max_drawdown_noise_band
        and abs(turnover_delta) <= thresholds.turnover_noise_band
    ):
        return "neutral"
    if sharpe_delta < -thresholds.sharpe_noise_band and (
        annualized_delta < -thresholds.annualized_return_noise_band
        or max_drawdown_delta < -thresholds.max_drawdown_noise_band
    ):
        return "positive"
    if sharpe_delta > thresholds.sharpe_noise_band and (
        max_drawdown_delta > thresholds.max_drawdown_noise_band
        or annualized_delta >= -thresholds.annualized_return_noise_band
    ):
        return "negative"
    return "neutral"


def _signal_contribution(
    *,
    signal: str,
    baseline: ProductionParameters,
    shadow_config: ShadowBacktestConfig,
    prices: pd.DataFrame,
    signal_frames: dict[str, pd.DataFrame],
    signal_snapshot_payload: dict[str, Any],
    baseline_result: PortfolioSimulationResult,
    baseline_by_window: dict[str, PortfolioSimulationResult],
    windows: tuple[Any, ...],
    full_start: date,
    full_end: date,
    thresholds: SignalAblationThresholds,
    min_walk_forward_windows: int,
    score_delta_epsilon: float,
    portfolio_weight_delta_epsilon: float,
    non_neutral_value_epsilon: float,
) -> dict[str, Any]:
    ablated_weights = _remove_one_signal_weights(baseline.weights, signal)
    ablated_result = simulate_parameter_portfolio(
        prices,
        baseline,
        ablated_weights,
        shadow_config.transaction_cost,
        start=full_start,
        end=full_end,
        signal_frames=signal_frames,
    )
    delta = _delta_metrics(baseline_result.metrics.to_dict(), ablated_result.metrics.to_dict())
    window_rows: list[dict[str, Any]] = []
    for window in windows:
        window_baseline = baseline_by_window[window.window_id]
        window_ablation = simulate_parameter_portfolio(
            prices,
            baseline,
            ablated_weights,
            shadow_config.transaction_cost,
            start=window.validation_start,
            end=window.validation_end,
            signal_frames=signal_frames,
        )
        window_delta = _delta_metrics(
            window_baseline.metrics.to_dict(),
            window_ablation.metrics.to_dict(),
        )
        window_class = classify_ablation_delta(window_delta, thresholds)
        window_rows.append(
            {
                **window.to_dict(),
                "class": window_class,
                "baseline_metrics": window_baseline.metrics.to_dict(),
                "ablation_metrics": window_ablation.metrics.to_dict(),
                "annualized_return_delta": window_delta["annualized_return_delta"],
                "sharpe_ratio_delta": window_delta["sharpe_ratio_delta"],
                "max_drawdown_delta": window_delta["max_drawdown_delta"],
                "turnover_delta": window_delta["turnover_delta"],
                "drawdown_reduction_delta": window_delta["drawdown_reduction_delta"],
            }
        )
    overall_class = _overall_class(
        delta=delta,
        window_rows=window_rows,
        thresholds=thresholds,
        min_walk_forward_windows=min_walk_forward_windows,
    )
    snapshot_item = _mapping(_mapping(signal_snapshot_payload.get("signals")).get(signal))
    quality = str(snapshot_item.get("quality") or _quality_for_signal(signal))
    status = str(snapshot_item.get("status") or "MISSING")
    usage_diagnostics = _signal_usage_diagnostics(
        signal=signal,
        baseline=baseline,
        signal_frames=signal_frames,
        signal_snapshot_payload=signal_snapshot_payload,
        baseline_result=baseline_result,
        full_start=full_start,
        full_end=full_end,
        non_neutral_value_epsilon=non_neutral_value_epsilon,
    )
    score_impact = _score_impact(
        baseline_result,
        ablated_result,
        score_delta_epsilon=score_delta_epsilon,
    )
    portfolio_impact = _portfolio_impact(
        baseline_result,
        ablated_result,
        portfolio_weight_delta_epsilon=portfolio_weight_delta_epsilon,
    )
    threshold_diagnostics = _threshold_diagnostics(delta, thresholds)
    diagnostic_status = _diagnostic_status(
        signal=signal,
        quality=quality,
        contribution_class=overall_class,
        usage_diagnostics=usage_diagnostics,
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
        score_delta_epsilon=score_delta_epsilon,
        portfolio_weight_delta_epsilon=portfolio_weight_delta_epsilon,
    )
    classification_reason = _classification_reason(
        signal=signal,
        quality=quality,
        contribution_class=overall_class,
        diagnostic_status=diagnostic_status,
        delta=delta,
        thresholds=thresholds,
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
    )
    warnings = _signal_warnings(
        signal=signal,
        quality=quality,
        contribution_class=overall_class,
        usage_diagnostics=usage_diagnostics,
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
        diagnostic_status=diagnostic_status,
        score_delta_epsilon=score_delta_epsilon,
        portfolio_weight_delta_epsilon=portfolio_weight_delta_epsilon,
    )
    promotion_credit_allowed = (
        quality == "price_derived" and overall_class == "positive" and diagnostic_status == "VALID"
    )
    return {
        "signal": signal,
        "quality": quality,
        "signal_status": status,
        "mode": "remove_one_signal",
        "contribution_class": overall_class,
        "classification_reason": classification_reason,
        "diagnostic_status": diagnostic_status,
        "promotion_credit_allowed": promotion_credit_allowed,
        "effective_weight": float(baseline.weights.get(signal, 0.0)),
        "baseline_weight": float(baseline.weights.get(signal, 0.0)),
        "ablation_weights": ablated_weights,
        "used_in_score_calculation": usage_diagnostics["used_in_score_calculation"],
        "signal_usage_diagnostics": usage_diagnostics,
        "score_impact": score_impact,
        "portfolio_impact": portfolio_impact,
        "threshold_diagnostics": threshold_diagnostics,
        "baseline_metrics": baseline_result.metrics.to_dict(),
        "ablation_metrics": ablated_result.metrics.to_dict(),
        "remove_one_signal_delta": {key: delta[key] for key in PRIMARY_DELTA_KEYS},
        "all_delta_metrics": delta,
        "window_stability": _window_stability(window_rows),
        "windows": window_rows,
        "warnings": warnings,
    }


def _remove_one_signal_weights(weights: dict[str, float], signal: str) -> dict[str, float]:
    remaining_total = sum(value for key, value in weights.items() if key != signal)
    if remaining_total <= 0.0:
        return {key: 0.0 for key in weights}
    return {
        key: 0.0 if key == signal else round(float(value) / remaining_total, 12)
        for key, value in weights.items()
    }


def _delta_metrics(
    baseline_metrics: dict[str, Any],
    ablation_metrics: dict[str, Any],
) -> dict[str, float]:
    delta = {
        f"{key}_delta": _as_float(ablation_metrics.get(key)) - _as_float(baseline_metrics.get(key))
        for key in DELTA_METRICS
    }
    delta["drawdown_reduction_delta"] = delta.pop("drawdown_reduction_ratio_delta")
    return {key: round(value, 12) for key, value in delta.items()}


def _signal_usage_diagnostics(
    *,
    signal: str,
    baseline: ProductionParameters,
    signal_frames: dict[str, pd.DataFrame],
    signal_snapshot_payload: dict[str, Any],
    baseline_result: PortfolioSimulationResult,
    full_start: date,
    full_end: date,
    non_neutral_value_epsilon: float,
) -> dict[str, Any]:
    snapshot_signals = _mapping(signal_snapshot_payload.get("signals"))
    present_in_snapshot = signal in snapshot_signals
    effective_weight = float(baseline.weights.get(signal, 0.0))
    present_in_weights = signal in baseline.weights
    frame = signal_frames.get(signal)
    non_neutral_ratio = _non_neutral_value_ratio(
        frame,
        start=full_start,
        end=full_end,
        epsilon=non_neutral_value_epsilon,
    )
    used_in_score = _signal_used_in_score_rows(baseline_result.score_rows, signal)
    return {
        "present_in_snapshot": present_in_snapshot,
        "present_in_weights": present_in_weights,
        "effective_weight": effective_weight,
        "non_neutral_value_ratio": non_neutral_ratio,
        "used_in_score_calculation": used_in_score,
    }


def _non_neutral_value_ratio(
    frame: pd.DataFrame | None,
    *,
    start: date,
    end: date,
    epsilon: float,
) -> float:
    if frame is None or frame.empty:
        return 0.0
    dated = frame.copy()
    dated.index = pd.to_datetime(dated.index, errors="coerce")
    dated = dated.loc[dated.index.notna()]
    dated = dated.loc[(dated.index.date >= start) & (dated.index.date <= end)]
    values = pd.to_numeric(dated.stack(), errors="coerce").dropna()
    if values.empty:
        return 0.0
    changed = (values - 0.5).abs() > epsilon
    return round(float(changed.mean()), 6)


def _signal_used_in_score_rows(score_rows: tuple[dict[str, object], ...], signal: str) -> bool:
    for row in score_rows:
        contributions = row.get("signal_contributions")
        signal_values = row.get("signal_values")
        if isinstance(contributions, dict) and signal in contributions:
            return True
        if isinstance(signal_values, dict) and signal in signal_values:
            return True
    return False


def _score_impact(
    baseline_result: PortfolioSimulationResult,
    ablated_result: PortfolioSimulationResult,
    *,
    score_delta_epsilon: float,
) -> dict[str, Any]:
    baseline_scores = _score_by_asset_day(baseline_result.score_rows)
    ablated_scores = _score_by_asset_day(ablated_result.score_rows)
    keys = sorted(set(baseline_scores) & set(ablated_scores))
    deltas = [abs(ablated_scores[key] - baseline_scores[key]) for key in keys]
    affected = [delta for delta in deltas if delta > score_delta_epsilon]
    return {
        "mean_abs_score_delta": _mean(deltas),
        "max_abs_score_delta": round(max(deltas), 12) if deltas else 0.0,
        "affected_asset_days": len(affected),
        "affected_asset_day_ratio": 0.0 if not deltas else round(len(affected) / len(deltas), 6),
        "asset_day_count": len(deltas),
    }


def _score_by_asset_day(score_rows: tuple[dict[str, object], ...]) -> dict[tuple[str, str], float]:
    scores: dict[tuple[str, str], float] = {}
    for row in score_rows:
        signal_date = str(row.get("date") or "")
        asset = str(row.get("asset") or "")
        if not signal_date or not asset:
            continue
        scores[(signal_date, asset)] = _as_float(row.get("composite_score"))
    return scores


def _portfolio_impact(
    baseline_result: PortfolioSimulationResult,
    ablated_result: PortfolioSimulationResult,
    *,
    portfolio_weight_delta_epsilon: float,
) -> dict[str, Any]:
    baseline_rows = _daily_rows_by_date(baseline_result.daily_rows)
    ablated_rows = _daily_rows_by_date(ablated_result.daily_rows)
    dates = sorted(set(baseline_rows) & set(ablated_rows))
    weight_deltas: list[float] = []
    changed_days = 0
    for signal_date in dates:
        baseline_weights = _row_portfolio_weights(baseline_rows[signal_date])
        ablated_weights = _row_portfolio_weights(ablated_rows[signal_date])
        assets = sorted(set(baseline_weights) | set(ablated_weights))
        day_deltas = [
            abs(ablated_weights.get(asset, 0.0) - baseline_weights.get(asset, 0.0))
            for asset in assets
        ]
        weight_deltas.extend(day_deltas)
        turnover_delta = abs(
            _as_float(ablated_rows[signal_date].get("turnover"))
            - _as_float(baseline_rows[signal_date].get("turnover"))
        )
        if any(delta > portfolio_weight_delta_epsilon for delta in day_deltas) or (
            turnover_delta > portfolio_weight_delta_epsilon
        ):
            changed_days += 1
    return {
        "mean_abs_weight_delta": _mean(weight_deltas),
        "max_abs_weight_delta": round(max(weight_deltas), 12) if weight_deltas else 0.0,
        "rebalance_days_changed": changed_days,
        "rebalance_change_ratio": 0.0 if not dates else round(changed_days / len(dates), 6),
        "portfolio_day_count": len(dates),
    }


def _daily_rows_by_date(
    daily_rows: tuple[dict[str, object], ...],
) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for row in daily_rows:
        signal_date = str(row.get("date") or "")
        if signal_date:
            rows[signal_date] = row
    return rows


def _row_portfolio_weights(row: dict[str, object]) -> dict[str, float]:
    weights = row.get("portfolio_weights")
    if isinstance(weights, dict):
        return {str(asset): _as_float(value) for asset, value in weights.items()}
    return {"risk_asset_exposure": _as_float(row.get("risk_asset_exposure"))}


def _threshold_diagnostics(
    delta: dict[str, float],
    thresholds: SignalAblationThresholds,
) -> dict[str, dict[str, Any]]:
    pairs = {
        "sharpe_delta": (delta.get("sharpe_ratio_delta", 0.0), thresholds.sharpe_noise_band),
        "annualized_return_delta": (
            delta.get("annualized_return_delta", 0.0),
            thresholds.annualized_return_noise_band,
        ),
        "max_drawdown_delta": (
            delta.get("max_drawdown_delta", 0.0),
            thresholds.max_drawdown_noise_band,
        ),
        "turnover_delta": (delta.get("turnover_delta", 0.0), thresholds.turnover_noise_band),
    }
    return {
        key: {
            "value": round(float(value), 12),
            "threshold": float(threshold),
            "passed": abs(float(value)) > float(threshold),
        }
        for key, (value, threshold) in pairs.items()
    }


def _diagnostic_status(
    *,
    signal: str,
    quality: str,
    contribution_class: ContributionClass,
    usage_diagnostics: dict[str, Any],
    score_impact: dict[str, Any],
    portfolio_impact: dict[str, Any],
    score_delta_epsilon: float,
    portfolio_weight_delta_epsilon: float,
) -> ContributionDiagnosticStatus:
    if signal in NEUTRAL_FALLBACK_SIGNALS or quality == "neutral_fallback":
        return "FALLBACK_SIGNAL"
    if contribution_class == "insufficient_data":
        return "INSUFFICIENT_DATA"
    if (
        usage_diagnostics.get("present_in_snapshot") is True
        and usage_diagnostics.get("present_in_weights") is False
    ):
        return "IMPLEMENTATION_WARNING"
    if usage_diagnostics.get("used_in_score_calculation") is not True:
        return "NOT_USED_IN_SCORE"
    if _as_float(score_impact.get("max_abs_score_delta")) <= score_delta_epsilon:
        return "NO_SCORE_IMPACT"
    if _as_float(portfolio_impact.get("max_abs_weight_delta")) <= portfolio_weight_delta_epsilon:
        return "NO_PORTFOLIO_IMPACT"
    if contribution_class == "neutral":
        return "BELOW_THRESHOLD"
    return "VALID"


def _classification_reason(
    *,
    signal: str,
    quality: str,
    contribution_class: ContributionClass,
    diagnostic_status: ContributionDiagnosticStatus,
    delta: dict[str, float],
    thresholds: SignalAblationThresholds,
    score_impact: dict[str, Any],
    portfolio_impact: dict[str, Any],
) -> str:
    if diagnostic_status == "FALLBACK_SIGNAL":
        return "Signal is neutral fallback and cannot be used as promotion evidence."
    if diagnostic_status == "NOT_USED_IN_SCORE":
        return "Signal exists in snapshot but is not used in score calculation."
    if diagnostic_status == "NO_SCORE_IMPACT":
        return (
            "Ablation did not materially change scores. This may indicate neutral signal "
            "values, zero effective weight, or score calculation integration issue."
        )
    if diagnostic_status == "NO_PORTFOLIO_IMPACT":
        return (
            "Scores changed but portfolio weights did not materially change. Portfolio "
            "construction may be too insensitive or thresholds are too wide."
        )
    if diagnostic_status == "INSUFFICIENT_DATA":
        return "Not enough walk-forward windows are available for stable classification."
    sharpe_delta = _as_float(delta.get("sharpe_ratio_delta"))
    return_delta = _as_float(delta.get("annualized_return_delta"))
    drawdown_delta = _as_float(delta.get("max_drawdown_delta"))
    if contribution_class == "positive":
        return (
            f"Removing {signal} reduced Sharpe by {abs(sharpe_delta):.4f} and "
            "worsened return or drawdown beyond configured noise bands."
        )
    if contribution_class == "negative":
        return (
            f"Removing {signal} improved Sharpe by {abs(sharpe_delta):.4f} without "
            "a material return penalty or with better drawdown."
        )
    if contribution_class == "unstable":
        return "Walk-forward windows show both positive and negative contribution signs."
    if diagnostic_status == "BELOW_THRESHOLD":
        return (
            f"Removing {signal} changed Sharpe by {sharpe_delta:.4f}, annualized return "
            f"by {return_delta:.4f}, and max drawdown by {drawdown_delta:.4f}; at least "
            f"one measured effect is below the configured noise bands "
            f"({thresholds.sharpe_noise_band:.4f} Sharpe, "
            f"{thresholds.annualized_return_noise_band:.4f} return, "
            f"{thresholds.max_drawdown_noise_band:.4f} drawdown)."
        )
    return (
        f"Removing {signal} is classified as {contribution_class}; "
        f"score impact max={_format_metric(score_impact.get('max_abs_score_delta'))}, "
        f"portfolio impact max={_format_metric(portfolio_impact.get('max_abs_weight_delta'))}."
    )


def _mean(values: list[float]) -> float:
    return 0.0 if not values else round(sum(values) / len(values), 12)


def _overall_class(
    *,
    delta: dict[str, float],
    window_rows: list[dict[str, Any]],
    thresholds: SignalAblationThresholds,
    min_walk_forward_windows: int,
) -> ContributionClass:
    if len(window_rows) < min_walk_forward_windows:
        return "insufficient_data"
    classes = [str(row.get("class")) for row in window_rows]
    if "positive" in classes and "negative" in classes:
        return "unstable"
    return classify_ablation_delta(delta, thresholds)


def _window_stability(window_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "positive_windows": sum(1 for row in window_rows if row.get("class") == "positive"),
        "neutral_windows": sum(1 for row in window_rows if row.get("class") == "neutral"),
        "negative_windows": sum(1 for row in window_rows if row.get("class") == "negative"),
        "unstable_windows": sum(1 for row in window_rows if row.get("class") == "unstable"),
        "insufficient_data_windows": sum(
            1 for row in window_rows if row.get("class") == "insufficient_data"
        ),
    }
    total = len(window_rows)
    dominant = max(counts.values()) if counts else 0
    return {
        **counts,
        "window_count": total,
        "stability_ratio": 0.0 if total <= 0 else round(dominant / total, 6),
    }


def _signal_warnings(
    *,
    signal: str,
    quality: str,
    contribution_class: ContributionClass,
    usage_diagnostics: dict[str, Any],
    score_impact: dict[str, Any],
    portfolio_impact: dict[str, Any],
    diagnostic_status: ContributionDiagnosticStatus,
    score_delta_epsilon: float,
    portfolio_weight_delta_epsilon: float,
) -> list[str]:
    warnings: list[str] = []
    if signal in PROXY_SIGNALS or quality in {"proxy_or_neutral", "price_proxy"}:
        warnings.append("Proxy signal cannot be used as positive promotion evidence.")
    if signal in NEUTRAL_FALLBACK_SIGNALS or quality == "neutral_fallback":
        warnings.append("Neutral fallback signal cannot be used as promotion evidence.")
        if contribution_class in {"positive", "negative", "unstable"}:
            warnings.append(
                "Neutral fallback ablation effect is suspicious because weight "
                "redistribution, not real signal information, may drive the result."
            )
    if (
        usage_diagnostics.get("present_in_snapshot") is True
        and usage_diagnostics.get("used_in_score_calculation") is not True
    ):
        warnings.append("Signal exists in snapshot but is not used in score calculation.")
    if diagnostic_status == "IMPLEMENTATION_WARNING":
        warnings.append("Signal ablation diagnostic detected a possible implementation issue.")
    if _as_float(score_impact.get("max_abs_score_delta")) <= score_delta_epsilon:
        warnings.append(
            "Ablation did not materially change scores. This may indicate neutral signal "
            "values, zero effective weight, or score calculation integration issue."
        )
    if (
        _as_float(score_impact.get("max_abs_score_delta")) > score_delta_epsilon
        and _as_float(portfolio_impact.get("max_abs_weight_delta"))
        <= portfolio_weight_delta_epsilon
    ):
        warnings.append(
            "Scores changed but portfolio weights did not materially change. Portfolio "
            "construction may be too insensitive or thresholds are too wide."
        )
    return warnings


def _diagnostics_summary(contributions: list[dict[str, Any]]) -> dict[str, Any]:
    real_contributions = [item for item in contributions if item.get("quality") == "price_derived"]
    implementation_warnings = [
        f"{item.get('signal')}: {warning}"
        for item in contributions
        for warning in _strings(item.get("warnings"))
        if "implementation" in warning.lower() or "not used in score calculation" in warning.lower()
    ]
    return {
        "all_real_signals_used_in_score": bool(real_contributions)
        and all(item.get("used_in_score_calculation") is True for item in real_contributions),
        "all_ablation_runs_changed_scores": bool(contributions)
        and all(
            _as_float(_mapping(item.get("score_impact")).get("max_abs_score_delta")) > 0.0
            for item in contributions
        ),
        "any_score_to_portfolio_disconnect": any(
            item.get("diagnostic_status") == "NO_PORTFOLIO_IMPACT" for item in contributions
        ),
        "implementation_warnings": implementation_warnings,
        "classification_reasons_present": bool(contributions)
        and all(bool(str(item.get("classification_reason") or "")) for item in contributions),
        "no_promotion_credit_reason": _no_promotion_credit_reason(contributions),
    }


def _no_promotion_credit_reason(contributions: list[dict[str, Any]]) -> str:
    if any(item.get("promotion_credit_allowed") is True for item in contributions):
        return "Promotion-credit signals are present but still require manual review."
    real_contributions = [item for item in contributions if item.get("quality") == "price_derived"]
    if not real_contributions:
        return "No promotion-credit signals because no price-derived real signals were evaluated."
    statuses = {str(item.get("diagnostic_status")) for item in real_contributions}
    classes = {str(item.get("contribution_class")) for item in real_contributions}
    if statuses & {"NOT_USED_IN_SCORE", "IMPLEMENTATION_WARNING"}:
        return (
            "No promotion-credit signals because at least one real signal is not "
            "reliably used in score calculation."
        )
    if "NO_SCORE_IMPACT" in statuses:
        return (
            "No promotion-credit signals because ablation did not materially affect "
            "real-signal scores."
        )
    if "NO_PORTFOLIO_IMPACT" in statuses:
        return (
            "No promotion-credit signals because ablation did not materially affect "
            "portfolio weights."
        )
    if statuses <= {"BELOW_THRESHOLD"} or classes <= {"neutral"}:
        return (
            "No promotion-credit signals because all real signal contributions are "
            "below threshold."
        )
    if "INSUFFICIENT_DATA" in statuses:
        return "No promotion-credit signals because walk-forward evidence is insufficient."
    return (
        "No promotion-credit signals because no price-derived signal met the positive "
        "contribution rule."
    )


def _summary(
    contributions: list[dict[str, Any]],
    snapshot_summary: dict[str, Any],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    by_class = {
        name: [
            str(item.get("signal"))
            for item in contributions
            if item.get("contribution_class") == name
        ]
        for name in (
            "positive",
            "negative",
            "neutral",
            "unstable",
            "insufficient_data",
        )
    }
    fallback = sorted(
        dict.fromkeys(
            [
                *_strings(snapshot_summary.get("neutral_fallback_signals")),
                *[
                    str(item.get("signal"))
                    for item in contributions
                    if item.get("quality") == "neutral_fallback"
                ],
            ]
        )
    )
    promotion_credit = sorted(
        str(item.get("signal"))
        for item in contributions
        if item.get("promotion_credit_allowed") is True
    )
    snapshot_status = str(snapshot_summary.get("status") or "UNKNOWN")
    return {
        "positive_signals": by_class["positive"],
        "negative_signals": by_class["negative"],
        "neutral_signals": by_class["neutral"],
        "unstable_signals": by_class["unstable"],
        "insufficient_data_signals": by_class["insufficient_data"],
        "fallback_signals": fallback,
        "proxy_signals": _strings(snapshot_summary.get("proxy_signals")),
        "promotion_credit_signals": promotion_credit,
        "can_support_candidate_promotion": False,
        "no_promotion_credit_reason": diagnostics.get("no_promotion_credit_reason", ""),
        "reason": (
            "Signal snapshot quality is LIMITED and fallback/proxy signals remain present."
            if snapshot_status == "LIMITED"
            else "Ablation is manual-review-only in v0.1 and cannot auto-enable promotion."
        ),
    }


def _blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: SignalAblationConfig,
    config_path: Path,
    shadow_config_path: Path,
    shadow_config: ShadowBacktestConfig,
    baseline: ProductionParameters,
    data_quality_status: str,
    data_quality_report_path: Path,
    diagnostic_path: Path,
    diagnostic_payload: dict[str, Any],
    signal_snapshot_path: Path | None,
    signal_snapshot_summary: dict[str, Any],
    requested_signals: tuple[str, ...],
    mode: str,
    warnings: list[str],
) -> dict[str, Any]:
    status = "INSUFFICIENT_DATA" if data_quality_status == "INSUFFICIENT_DATA" else "FAILED"
    return {
        "schema_version": SIGNAL_ABLATION_SCHEMA_VERSION,
        "report_type": SIGNAL_ABLATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-ablation-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": status,
            "backtest_mode": _diagnostic_backtest_mode(diagnostic_payload),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": False,
            "market_regime": shadow_config.market_regime.id,
            "market_regime_anchor": shadow_config.market_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": shadow_config.market_regime.anchor_event,
            "ablation_mode": mode,
            "config_path": str(config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
            "code_version": git_commit_sha() or "unknown",
        },
        "input_artifacts": {
            "signal_ablation_config": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "baseline_parameters": str(
                resolve_project_path(shadow_config.baseline_parameters_path)
            ),
            "data_quality_report": str(data_quality_report_path),
            "backtest_input_diagnostics": str(diagnostic_path),
            "signal_snapshot": "" if signal_snapshot_path is None else str(signal_snapshot_path),
        },
        "baseline": {"source": "not_computed", "metrics": {}},
        "data_quality": {
            "status": data_quality_status,
            "diagnostic_report": str(diagnostic_path),
            "signal_snapshot_status": signal_snapshot_summary.get("status", "MISSING"),
            "requested_signals": list(requested_signals),
        },
        "signal_snapshot": {
            "path": "" if signal_snapshot_path is None else str(signal_snapshot_path),
            **signal_snapshot_summary,
        },
        "policy": {
            "version": config.version,
            "thresholds": config.thresholds.model_dump(mode="json"),
            "min_walk_forward_windows": config.stability.min_walk_forward_windows,
        },
        "signal_contributions": [],
        "summary": {
            "positive_signals": [],
            "negative_signals": [],
            "neutral_signals": [],
            "unstable_signals": [],
            "fallback_signals": _strings(signal_snapshot_summary.get("neutral_fallback_signals")),
            "promotion_credit_signals": [],
            "can_support_candidate_promotion": False,
            "reason": "Signal ablation was not run because required input quality is blocked.",
        },
        "warnings": warnings,
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "candidate_promotion_triggered": False,
            "broker_action": False,
            "trading_action": False,
        },
    }


def _failure_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: SignalAblationConfig,
    config_path: Path,
    shadow_config_path: Path,
    status: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": SIGNAL_ABLATION_SCHEMA_VERSION,
        "report_type": SIGNAL_ABLATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-ablation-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": status,
            "backtest_mode": "blocked",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "config_path": str(config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
        },
        "baseline": {"source": "not_computed", "metrics": {}},
        "signal_contributions": [],
        "summary": {
            "positive_signals": [],
            "negative_signals": [],
            "neutral_signals": [],
            "unstable_signals": [],
            "fallback_signals": [],
            "promotion_credit_signals": [],
            "can_support_candidate_promotion": False,
            "reason": reason,
        },
        "warnings": [reason],
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "candidate_promotion_triggered": False,
        },
    }


def _unsupported_mode_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: SignalAblationConfig,
    config_path: Path,
    shadow_config_path: Path,
    mode: str,
) -> dict[str, Any]:
    return _failure_payload(
        as_of=as_of,
        generated_at=generated_at,
        config=config,
        config_path=config_path,
        shadow_config_path=shadow_config_path,
        status="FAILED",
        reason=f"unsupported ablation mode in v0.1: {mode}",
    )


def _requested_signals(signals: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if signals is None:
        return REQUIRED_SIGNALS
    requested = tuple(dict.fromkeys(str(signal) for signal in signals if str(signal)))
    unknown = [signal for signal in requested if signal not in REQUIRED_SIGNALS]
    if unknown:
        raise ValueError("unknown required signal(s): " + ", ".join(unknown))
    return requested


def _metadata_status(*, data_quality_status: str, snapshot_status: str) -> str:
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return data_quality_status
    if snapshot_status == "OK":
        return "OK"
    if snapshot_status == "LIMITED":
        return "LIMITED"
    return "FAILED"


def _quality_for_signal(signal: str) -> str:
    if signal in PRICE_DERIVED_SIGNALS:
        return "price_derived"
    if signal in PROXY_SIGNALS:
        return "proxy_or_neutral"
    if signal in NEUTRAL_FALLBACK_SIGNALS:
        return "neutral_fallback"
    return "missing"


def _signal_snapshot_path_from_diagnostics(payload: dict[str, Any]) -> Path | None:
    checks = _mapping(payload.get("checks"))
    signal_snapshots = _mapping(checks.get("signal_snapshots"))
    for path_text in _strings(signal_snapshots.get("snapshot_files")):
        path = Path(path_text)
        if path.name == "signal_snapshot.json" and path.exists():
            return path
    return None


def _diagnostic_backtest_mode(payload: dict[str, Any]) -> str:
    return str(_mapping(payload.get("summary")).get("backtest_mode") or "blocked")


def _diagnostic_can_run_shadow_backtest(payload: dict[str, Any]) -> bool:
    return bool(_mapping(payload.get("summary")).get("can_run_shadow_backtest"))


def _diagnostic_can_promote_candidate(payload: dict[str, Any]) -> bool:
    return bool(_mapping(payload.get("summary")).get("can_promote_candidate"))


def _baseline_source_path(
    shadow_config: ShadowBacktestConfig,
    as_of: date,
    *,
    dry_run: bool,
) -> Path:
    if dry_run:
        return (
            PROJECT_ROOT
            / "outputs"
            / "dry_runs"
            / "shadow_backtest"
            / "shadow_backtest"
            / as_of.isoformat()
            / "shadow_backtest_summary.json"
        )
    return (
        resolve_project_path(shadow_config.output.shadow_backtest_dir)
        / as_of.isoformat()
        / "shadow_backtest_summary.json"
    )


def _data_quality_report_dir(shadow_config: ShadowBacktestConfig, *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "signal_ablation" / "reports"
    return resolve_project_path(shadow_config.data.data_quality_report_dir)


def _diagnostic_output_root(shadow_config: ShadowBacktestConfig, *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "signal_ablation"
    return resolve_project_path(shadow_config.output.shadow_backtest_dir).parent


def _output_root(config: SignalAblationConfig, *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "signal_ablation"
    return resolve_project_path(config.output.signal_ablation_dir)


def _payload_date(payload: dict[str, Any]) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("signal-ablation-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        return datetime.now(tz=UTC).date()


def _config_hash(
    config_path: Path,
    shadow_config_path: Path,
    signal_snapshot_path: Path | None,
) -> str:
    digest = sha256()
    for path in (config_path, shadow_config_path, signal_snapshot_path):
        if path is None or not path.exists() or not path.is_file():
            continue
        digest.update(str(path).encode("utf-8"))
        digest.update(sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


def _ranking_key(contribution: dict[str, Any]) -> tuple[int, float]:
    rank = {
        "positive": 0,
        "negative": 1,
        "unstable": 2,
        "neutral": 3,
        "insufficient_data": 4,
    }.get(str(contribution.get("contribution_class")), 5)
    delta = _mapping(contribution.get("remove_one_signal_delta"))
    return rank, _as_float(delta.get("sharpe_ratio_delta"))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []


def _as_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if pd.isna(number):
        return 0.0
    return float(number)


def _format_metric(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"


def _yes_no(value: object) -> str:
    return "yes" if value is True else "no"


def _impact_label(value: object) -> str:
    return "non-zero" if abs(_as_float(value)) > 0.0 else "none"
