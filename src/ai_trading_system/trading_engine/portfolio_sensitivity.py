from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.backtesting.metrics import calculate_portfolio_metrics
from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
    DEFENSIVE_ASSETS,
    _apply_asset_cap,
    _feature_frame,
    _prepare_price_panel,
    _risk_budget_for_score,
)
from ai_trading_system.trading_engine.backtesting.transaction_cost import (
    calculate_transaction_cost,
)
from ai_trading_system.trading_engine.backtesting.walk_forward import (
    WalkForwardWindow,
    generate_walk_forward_windows,
)
from ai_trading_system.trading_engine.data_registry_consistency import (
    portfolio_sensitivity_data_gate_context,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ProductionParameters,
    ShadowBacktestConfig,
)
from ai_trading_system.trading_engine.signal_calibration import (
    latest_signal_calibration_path_on_or_before,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    signal_snapshot_frames,
    signal_snapshot_summary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PORTFOLIO_SENSITIVITY_SCHEMA_VERSION = 1
PORTFOLIO_SENSITIVITY_REPORT_TYPE = "portfolio_sensitivity"
PORTFOLIO_SENSITIVITY_ALIAS_REPORT_TYPE = "portfolio_sensitivity_report"
DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_sensitivity_profiles.yaml"
)


@dataclass(frozen=True)
class PortfolioSensitivityRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    recommended_profile_path: Path


@dataclass(frozen=True)
class SensitivitySimulation:
    profile_name: str
    profile_config: dict[str, Any]
    target: pd.DataFrame
    actual: pd.DataFrame
    score: pd.DataFrame
    adjusted_score: pd.DataFrame
    daily_rows: tuple[dict[str, object], ...]
    score_rows: tuple[dict[str, object], ...]
    metrics: dict[str, float | int]
    gross_turnover: float
    transaction_cost_drag: float
    rebalance_opportunity_days: int
    rebalance_days: int
    constraint_binding: dict[str, Any]


def default_portfolio_sensitivity_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_sensitivity"


def default_portfolio_sensitivity_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_sensitivity_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_sensitivity_dir(output_root, as_of)
        / "portfolio_sensitivity_summary.json"
    )


def default_portfolio_sensitivity_markdown_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_sensitivity_dir(output_root, as_of)
        / "portfolio_sensitivity_summary.md"
    )


def default_recommended_portfolio_sensitivity_profile_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_sensitivity_dir(output_root, as_of)
        / "recommended_portfolio_sensitivity_profile.yaml"
    )


def latest_portfolio_sensitivity_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_portfolio_sensitivity_root()
    candidates = sorted(root.glob("*/portfolio_sensitivity_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_sensitivity_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_sensitivity_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_sensitivity_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_sensitivity_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_sensitivity_{as_of.isoformat()}.md",
    )


def load_portfolio_sensitivity_config(
    path: Path | str = DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio sensitivity config must be a mapping: {path}")
    _validate_portfolio_sensitivity_config(payload)
    return payload


def run_portfolio_sensitivity(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> PortfolioSensitivityRun:
    config = load_portfolio_sensitivity_config(config_path)
    root = _output_root(config, dry_run=dry_run)
    payload = build_portfolio_sensitivity_payload(
        as_of=as_of,
        profile_names=profile_names,
        config_path=config_path,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
    )
    resolved_as_of = portfolio_sensitivity_payload_date(
        payload,
        default_portfolio_sensitivity_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_portfolio_sensitivity_json_path(root, resolved_as_of)
    markdown_path = default_portfolio_sensitivity_markdown_path(root, resolved_as_of)
    recommended_path = default_recommended_portfolio_sensitivity_profile_path(
        root,
        resolved_as_of,
    )
    write_portfolio_sensitivity_report(payload, json_path, markdown_path)
    write_recommended_portfolio_sensitivity_profile(payload, recommended_path)
    return PortfolioSensitivityRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        recommended_profile_path=recommended_path,
    )


def build_portfolio_sensitivity_payload(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
    output_root: Path | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_portfolio_sensitivity_config(resolved_config_path)
    shadow_config_path = resolve_project_path(
        str(config.get("shadow_backtest_config_path") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    prices_path = resolve_project_path(shadow_config.data.prices_path)
    prices = shadow_backtest_module._read_prices(prices_path)
    data_gate = portfolio_sensitivity_data_gate_context(
        as_of=as_of,
        config_path=shadow_config_path,
        output_root=_backtest_artifact_root(shadow_config),
    )
    manifest_as_of = _manifest_date_from_data_gate(data_gate)
    resolved_as_of = (
        as_of
        or manifest_as_of
        or shadow_backtest_module._latest_price_date(prices)
        or generated.date()
    )
    resolved_output_root = output_root or _output_root(config, dry_run=dry_run)
    profile_map = _mapping(config.get("profiles"))
    selected_profiles = _selected_profiles(profile_map, profile_names)
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
            reason=f"production baseline unavailable: {exc}",
            dry_run=dry_run,
        )

    data_quality_as_of = resolved_as_of
    latest_price_as_of = shadow_backtest_module._latest_price_date(prices)
    if as_of is None and latest_price_as_of is not None and latest_price_as_of > data_quality_as_of:
        data_quality_as_of = latest_price_as_of
    data_quality_report_path = default_quality_report_path(
        _data_quality_report_dir(config, shadow_config, dry_run=dry_run),
        data_quality_as_of,
    )
    data_quality_report = shadow_backtest_module._run_data_quality_gate(
        config=shadow_config,
        baseline=baseline,
        as_of=data_quality_as_of,
        report_path=data_quality_report_path,
        backtest_manifest_path=_manifest_path_from_data_gate(data_gate),
    )
    data_quality_status, warnings = shadow_backtest_module._shadow_data_quality_status(
        config=shadow_config,
        baseline=baseline,
        prices=prices,
        as_of=data_quality_as_of,
        data_quality_report=data_quality_report,
    )
    if data_gate.get("status") == "FAILED":
        error_code = str(data_gate.get("error_code") or "DATA_GATE_FAILED")
        reason = str(
            data_gate.get("reason")
            or "Portfolio sensitivity was not run because data registry consistency failed."
        )
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            data_quality_status="FAILED",
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[*warnings, f"data registry gate failed: {error_code}"],
            dry_run=dry_run,
            reason=reason,
            data_gate=data_gate,
        )
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=list(warnings),
            dry_run=dry_run,
            reason="Portfolio sensitivity was not run because data quality is blocked.",
            data_gate=data_gate,
        )

    signal_snapshot_path = _signal_snapshot_path(shadow_config, resolved_as_of)
    signal_snapshot_payload = load_signal_snapshot_payload(signal_snapshot_path)
    if not signal_snapshot_payload:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            data_quality_status="LIMITED",
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[*warnings, "signal snapshot missing; portfolio sensitivity not run"],
            dry_run=dry_run,
            reason="Signal snapshot is missing; score-to-weight diagnostics need snapshot input.",
            data_gate=data_gate,
        )

    signal_frames = signal_snapshot_frames(signal_snapshot_payload)
    snapshot_summary = signal_snapshot_summary(signal_snapshot_payload)
    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, resolved_as_of)
    windows = generate_walk_forward_windows(trading_dates, shadow_config.walk_forward)
    full_start = shadow_backtest_module._full_result_start(windows, trading_dates, shadow_config)
    full_end = resolved_as_of
    simulations = {
        name: _simulate_sensitivity_profile(
            profile_name=name,
            profile_config=_mapping(profile_map.get(name)),
            prices=prices,
            baseline=baseline,
            shadow_config=shadow_config,
            signal_frames=signal_frames,
            start=full_start,
            end=full_end,
        )
        for name in selected_profiles
    }
    baseline_profile_name = (
        "baseline_v0_1" if "baseline_v0_1" in simulations else selected_profiles[0]
    )
    baseline_simulation = simulations[baseline_profile_name]
    diagnostics_config = _mapping(config.get("diagnostics"))
    profile_results = [
        _profile_payload(
            simulation=simulations[name],
            baseline_simulation=baseline_simulation,
            baseline=baseline,
            diagnostics_config=diagnostics_config,
            windows=windows,
        )
        for name in selected_profiles
    ]
    ranking = _rank_profiles(profile_results, config)
    diagnosis = _diagnosis(profile_results, ranking, diagnostics_config)
    calibration_path = latest_signal_calibration_path_on_or_before(resolved_as_of)
    shadow_summary_path = _latest_shadow_backtest_supporting_path(resolved_as_of)
    input_artifacts = {
        "portfolio_sensitivity_profiles": str(resolved_config_path),
        "shadow_backtest_config": str(shadow_config_path),
        "baseline_parameters": str(baseline_path),
        "prices": str(prices_path),
        "data_quality_report": str(data_quality_report_path),
        "signal_snapshot": str(signal_snapshot_path),
        "signal_calibration": "" if calibration_path is None else str(calibration_path),
        "shadow_backtest": "" if shadow_summary_path is None else str(shadow_summary_path),
    }
    output_artifacts = {
        "summary_json": str(
            default_portfolio_sensitivity_json_path(resolved_output_root, resolved_as_of)
        ),
        "summary_markdown": str(
            default_portfolio_sensitivity_markdown_path(resolved_output_root, resolved_as_of)
        ),
        "recommended_profile": str(
            default_recommended_portfolio_sensitivity_profile_path(
                resolved_output_root,
                resolved_as_of,
            )
        ),
    }
    return {
        "schema_version": PORTFOLIO_SENSITIVITY_SCHEMA_VERSION,
        "report_type": PORTFOLIO_SENSITIVITY_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-sensitivity-{resolved_as_of.isoformat()}",
            "generated_at": generated.isoformat(),
            "status": "LIMITED",
            "backtest_mode": "full_signal_backtest_limited",
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
            "profile_count": len(profile_results),
            "config_path": str(resolved_config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(
                resolved_config_path,
                shadow_config_path,
                baseline_path,
                signal_snapshot_path,
            ),
        },
        "inputs": input_artifacts,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
        "data_quality": {
            "status": data_quality_status,
            "validate_data_status": data_quality_report.status,
            "quality_report_path": str(data_quality_report_path),
            "signal_snapshot_status": snapshot_summary.get("status", "UNKNOWN"),
            "can_promote_candidate": False,
            "error_count": data_quality_report.error_count,
            "warning_count": data_quality_report.warning_count,
        },
        "data_gate": data_gate,
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": diagnostics_config,
            "ranking": _mapping(config.get("ranking")),
            "promotion_effect": "manual_review_only",
        },
        "fallback_signal_dilution": _fallback_signal_dilution(
            snapshot_summary,
            baseline,
            diagnostics_config,
        ),
        "profiles": profile_results,
        "ranking": ranking,
        "diagnosis": diagnosis,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Signal snapshot quality remains LIMITED; portfolio sensitivity diagnostics "
                "are advisory only and cannot enable candidate promotion."
            ),
        },
        "warnings": list(dict.fromkeys(warnings)),
        "safety": _safety_payload(),
    }


def write_portfolio_sensitivity_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_portfolio_sensitivity_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_portfolio_sensitivity_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": PORTFOLIO_SENSITIVITY_ALIAS_REPORT_TYPE,
        "source_report_type": PORTFOLIO_SENSITIVITY_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_portfolio_sensitivity_report(alias_payload, json_path, markdown_path)


def write_recommended_portfolio_sensitivity_profile(
    payload: dict[str, Any],
    output_path: Path,
) -> Path:
    ranking = _mapping(payload.get("ranking"))
    best_profile = str(ranking.get("best_profile") or "")
    profile = next(
        (
            item
            for item in _records(payload.get("profiles"))
            if str(item.get("profile_name")) == best_profile
        ),
        {},
    )
    recommended = {
        "version": "recommended_portfolio_sensitivity_profile_v0_1",
        "profile_name": best_profile,
        "source_portfolio_sensitivity": _mapping(payload.get("metadata")).get("run_id"),
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "profile_config": _mapping(profile.get("profile_config")),
        "ranking_metrics": _mapping(profile.get("ranking_metrics")),
        "reason": ranking.get("reason", ""),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(recommended, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return output_path


def render_portfolio_sensitivity_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    ranking = _mapping(payload.get("ranking"))
    diagnosis = _mapping(payload.get("diagnosis"))
    promotion = _mapping(payload.get("promotion_impact"))
    data_gate = _mapping(payload.get("data_gate"))
    profiles = _records(payload.get("profiles"))
    lines = [
        "# Portfolio Sensitivity Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- data_gate: `{data_gate.get('status', 'UNKNOWN')}`",
        f"- data_registry_consistency: `{data_gate.get('data_registry_consistency', 'UNKNOWN')}`",
        f"- latest_resolution: `{data_gate.get('latest_resolution_status', 'UNKNOWN')}`",
        f"- best_profile: `{ranking.get('best_profile', 'UNKNOWN')}`",
        f"- primary_bottleneck: `{diagnosis.get('primary_bottleneck', 'UNKNOWN')}`",
        (
            "- portfolio_is_too_insensitive: "
            f"`{diagnosis.get('portfolio_is_too_insensitive', False)}`"
        ),
        "",
        "## 2. Profiles Tested",
        "",
        "| Profile | Status | Description |",
        "|---|---|---|",
    ]
    for profile in profiles:
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"`{profile.get('status', '')}` | "
            f"{profile.get('description', '')} |"
        )
    lines.extend(
        [
            "",
            "## 3. Primary Bottleneck Diagnosis",
            "",
            f"- primary_bottleneck: `{diagnosis.get('primary_bottleneck', 'UNKNOWN')}`",
            f"- secondary_bottleneck: `{diagnosis.get('secondary_bottleneck', 'UNKNOWN')}`",
            (
                "- portfolio_is_too_insensitive: "
                f"`{diagnosis.get('portfolio_is_too_insensitive', False)}`"
            ),
            f"- reason: {diagnosis.get('reason', '')}",
            "",
            "## 4. Score Dispersion",
            "",
            "| Profile | Mean | Std | P05 | P50 | P95 | Neutral Ratio | Warning |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("score_dispersion"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(item.get('mean'))} | "
            f"{_format_metric(item.get('std'))} | "
            f"{_format_metric(item.get('p05'))} | "
            f"{_format_metric(item.get('p50'))} | "
            f"{_format_metric(item.get('p95'))} | "
            f"{_format_metric(item.get('neutral_score_ratio_0_45_to_0_55'))} | "
            f"{item.get('warning', '')} |"
        )
    lines.extend(
        [
            "",
            "## 5. Score-to-Target-Weight Transmission",
            "",
            (
                "| Profile | Mean Abs Target Delta | Max Abs Target Delta | "
                "Changed Asset-days | Correlation |"
            ),
            "|---|---:|---:|---:|---:|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("score_to_target_weight"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(item.get('mean_abs_target_weight_delta'))} | "
            f"{_format_metric(item.get('max_abs_target_weight_delta'))} | "
            f"{_format_metric(item.get('asset_days_with_weight_change_ratio'))} | "
            f"{_format_metric(item.get('score_weight_correlation'))} |"
        )
    lines.extend(
        [
            "",
            "## 6. Target-to-Actual-Weight Transmission",
            "",
            (
                "| Profile | Mean Abs Actual Delta | Rebalance Days | Opportunity Days | "
                "Suppression Ratio | Warning |"
            ),
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("target_to_actual_weight"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(item.get('mean_abs_actual_weight_delta'))} | "
            f"{item.get('rebalance_days', 0)} | "
            f"{item.get('rebalance_opportunity_days', 0)} | "
            f"{_format_metric(item.get('rebalance_suppression_ratio'))} | "
            f"{item.get('warning', '')} |"
        )
    lines.extend(
        [
            "",
            "## 7. Constraint Binding Diagnostics",
            "",
            (
                "| Profile | Single Asset Days | Sector Days | Cash Floor Days | "
                "Most Constrained Assets | Warning |"
            ),
            "|---|---:|---:|---:|---|---|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("constraint_binding"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{item.get('single_asset_cap_binding_days', 0)} | "
            f"{item.get('sector_cap_binding_days', 0)} | "
            f"{item.get('cash_floor_binding_days', 0)} | "
            f"{', '.join(_strings(item.get('most_constrained_assets'))) or 'none'} | "
            f"{item.get('warning', '')} |"
        )
    lines.extend(
        [
            "",
            "## 8. Turnover and Cost Impact",
            "",
            "| Profile | Turnover | Delta vs Baseline | Estimated Cost Drag | Warning |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("turnover_cost_impact"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(item.get('turnover'))} | "
            f"{_format_metric(item.get('turnover_delta_vs_baseline'))} | "
            f"{_format_metric(item.get('estimated_cost_drag'))} | "
            f"{item.get('warning', '')} |"
        )
    lines.extend(
        [
            "",
            "## 9. Backtest Performance by Profile",
            "",
            "| Profile | Annualized Return | Max Drawdown | Sharpe | Sortino | Calmar | Turnover |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for profile in profiles:
        item = _mapping(profile.get("performance"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(item.get('annualized_return'))} | "
            f"{_format_metric(item.get('max_drawdown'))} | "
            f"{_format_metric(item.get('sharpe_ratio'))} | "
            f"{_format_metric(item.get('sortino_ratio'))} | "
            f"{_format_metric(item.get('calmar_ratio'))} | "
            f"{_format_metric(item.get('turnover'))} |"
        )
    lines.extend(
        [
            "",
            "## 10. Recommended Portfolio Sensitivity Profile",
            "",
            f"- best_profile: `{ranking.get('best_profile', 'UNKNOWN')}`",
            f"- reason: {ranking.get('reason', '')}",
            "",
            "## 11. Promotion Eligibility Impact",
            "",
            (
                "- can_support_candidate_promotion: "
                f"`{promotion.get('can_support_candidate_promotion', False)}`"
            ),
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 12. Manual Review Checklist",
            "",
            (
                "- Review whether lower rebalance threshold improves score-to-actual-weight "
                "transmission without excessive turnover."
            ),
            (
                "- Review whether position or sector caps bind frequently enough to suppress "
                "signal expression."
            ),
            "- Keep candidate promotion disabled while signal snapshot quality remains LIMITED.",
            (
                "- Treat the recommended profile as advisory only; do not write production "
                "parameters without manual review."
            ),
            "",
            "## 13. Input / Output Artifacts",
            "",
        ]
    )
    for section in ("inputs", "output_artifacts"):
        artifacts = _mapping(payload.get(section))
        if not artifacts:
            continue
        lines.append(f"### {section}")
        for key, value in artifacts.items():
            if value:
                lines.append(f"- `{key}`: `{value}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def load_portfolio_sensitivity_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_portfolio_sensitivity_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_SENSITIVITY_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != PORTFOLIO_SENSITIVITY_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if not isinstance(payload.get("profiles"), list):
        issues.append("profiles must be a list")
    for profile in _records(payload.get("profiles")):
        name = str(profile.get("profile_name") or "")
        if not name:
            issues.append("profile missing profile_name")
        for key in (
            "score_dispersion",
            "score_to_target_weight",
            "target_to_actual_weight",
            "constraint_binding",
            "turnover_cost_impact",
            "performance",
            "ranking_metrics",
        ):
            if not isinstance(profile.get(key), dict):
                issues.append(f"{name} missing {key}")
    ranking = _mapping(payload.get("ranking"))
    if payload.get("profiles") and not ranking.get("best_profile"):
        issues.append("ranking.best_profile must be present")
    promotion = _mapping(payload.get("promotion_impact"))
    if promotion.get("can_support_candidate_promotion") is not False:
        issues.append("sensitivity must not support candidate promotion")
    safety = _mapping(payload.get("safety"))
    if safety.get("production_parameters_modified") not in {False, None}:
        issues.append("production_parameters_modified must be false")
    if safety.get("candidate_promotion_triggered") not in {False, None}:
        issues.append("candidate_promotion_triggered must be false")
    return issues


def portfolio_sensitivity_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("portfolio-sensitivity-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer portfolio sensitivity date from {source_path}") from exc


def _simulate_sensitivity_profile(
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    prices: pd.DataFrame,
    baseline: ProductionParameters,
    shadow_config: ShadowBacktestConfig,
    signal_frames: dict[str, pd.DataFrame],
    start: date,
    end: date,
    signal_weights: dict[str, float] | None = None,
) -> SensitivitySimulation:
    price_panel = _prepare_price_panel(prices, baseline.flattened_asset_universe())
    if price_panel.empty:
        return _empty_simulation(profile_name, profile_config)
    price_panel = price_panel.loc[
        (price_panel.index.date >= start) & (price_panel.index.date <= end)
    ].copy()
    if len(price_panel.index) < 3:
        return _empty_simulation(profile_name, profile_config)
    returns = price_panel.pct_change().fillna(0.0)
    features = _feature_frame(price_panel, returns, signal_frames=signal_frames)
    tradable_assets = [
        asset for asset in baseline.flattened_asset_universe() if asset not in DEFENSIVE_ASSETS
    ]
    defensive_assets = [asset for asset in ("SGOV", "CASH") if asset in price_panel.columns]
    effective_signal_weights = signal_weights or baseline.weights
    composite, contribution_frames = _composite_scores(
        features,
        weights=effective_signal_weights,
        tradable_assets=tradable_assets,
    )
    multiplier = _float_value(profile_config.get("score_sensitivity_multiplier"), default=1.0)
    adjusted_score = (0.5 + (composite - 0.5) * multiplier).clip(0.0, 1.0)
    target, constraint_binding = _target_weights_from_scores(
        adjusted_score,
        baseline=baseline,
        profile_config=profile_config,
        tradable_assets=tradable_assets,
        defensive_assets=defensive_assets,
    )
    actual, rebalance_info = _actual_weights_from_target(
        target,
        threshold=_float_value(profile_config.get("rebalance_threshold"), default=0.05),
    )
    turnover = actual.diff().abs().sum(axis=1).fillna(actual.abs().sum(axis=1))
    shifted_actual = actual.shift(1).fillna(0.0)
    gross_returns = (
        shifted_actual * returns.reindex(columns=shifted_actual.columns).fillna(0.0)
    ).sum(axis=1)
    costs = turnover.apply(
        lambda value: calculate_transaction_cost(float(value), shadow_config.transaction_cost)
    )
    strategy_returns = gross_returns - costs
    equity = (1.0 + strategy_returns).cumprod()
    benchmark_returns = returns["QQQ"] if "QQQ" in returns.columns else gross_returns
    exposure = shifted_actual.loc[
        :,
        [asset for asset in tradable_assets if asset in shifted_actual.columns],
    ]
    exposure_series = (
        exposure.sum(axis=1) if not exposure.empty else pd.Series(0.0, index=returns.index)
    )
    metrics = calculate_portfolio_metrics(
        strategy_returns,
        equity,
        turnover,
        benchmark_returns=benchmark_returns,
        exposure=exposure_series,
    ).to_dict()
    daily_rows = _daily_rows(
        strategy_returns=strategy_returns,
        gross_returns=gross_returns,
        costs=costs,
        turnover=turnover,
        equity=equity,
        target=target,
        actual=actual,
        exposure=exposure_series,
        daily_score=adjusted_score.mean(axis=1).fillna(0.0),
    )
    score_rows = _score_rows(
        composite=composite,
        adjusted_score=adjusted_score,
        contribution_frames=contribution_frames,
        weights=effective_signal_weights,
        tradable_assets=tradable_assets,
    )
    return SensitivitySimulation(
        profile_name=profile_name,
        profile_config=profile_config,
        target=target,
        actual=actual,
        score=composite,
        adjusted_score=adjusted_score,
        daily_rows=tuple(daily_rows),
        score_rows=tuple(score_rows),
        metrics=metrics,
        gross_turnover=float(turnover.sum()),
        transaction_cost_drag=float(costs.sum()),
        rebalance_opportunity_days=int(rebalance_info["rebalance_opportunity_days"]),
        rebalance_days=int(rebalance_info["rebalance_days"]),
        constraint_binding=constraint_binding,
    )


def _profile_payload(
    *,
    simulation: SensitivitySimulation,
    baseline_simulation: SensitivitySimulation,
    baseline: ProductionParameters,
    diagnostics_config: dict[str, Any],
    windows: tuple[WalkForwardWindow, ...],
) -> dict[str, Any]:
    score_dispersion = score_dispersion_diagnostics(
        simulation.adjusted_score,
        diagnostics_config,
    )
    score_to_target = score_to_target_weight_diagnostics(
        simulation.adjusted_score,
        simulation.target,
        diagnostics_config,
    )
    target_to_actual = target_to_actual_weight_diagnostics(
        simulation.actual,
        simulation.rebalance_days,
        simulation.rebalance_opportunity_days,
        diagnostics_config,
    )
    constraint_binding = constraint_binding_diagnostics(
        simulation.constraint_binding,
        total_days=len(simulation.target.index),
        diagnostics_config=diagnostics_config,
    )
    turnover_cost = turnover_cost_impact(
        simulation,
        baseline_simulation,
        diagnostics_config,
    )
    performance = _performance_payload(simulation.metrics)
    ranking_metrics = _ranking_metrics(
        simulation=simulation,
        baseline_simulation=baseline_simulation,
        score_to_target=score_to_target,
        target_to_actual=target_to_actual,
        constraint_binding=constraint_binding,
        windows=windows,
        diagnostics_config=diagnostics_config,
    )
    warnings = [
        str(item)
        for item in (
            score_dispersion.get("warning"),
            score_to_target.get("warning"),
            target_to_actual.get("warning"),
            constraint_binding.get("warning"),
            turnover_cost.get("warning"),
        )
        if str(item)
    ]
    return {
        "profile_name": simulation.profile_name,
        "description": simulation.profile_config.get("description", ""),
        "profile_config": simulation.profile_config,
        "status": "LIMITED",
        "score_dispersion": score_dispersion,
        "score_to_target_weight": score_to_target,
        "target_to_actual_weight": target_to_actual,
        "constraint_binding": constraint_binding,
        "turnover_cost_impact": turnover_cost,
        "performance": performance,
        "ranking_metrics": ranking_metrics,
        "warnings": list(dict.fromkeys(warnings)),
        "audit": {
            "score_row_count": len(simulation.score_rows),
            "daily_row_count": len(simulation.daily_rows),
            "production_position_limits": baseline.position_limits.model_dump(mode="json"),
        },
    }


def score_dispersion_diagnostics(
    adjusted_score: pd.DataFrame,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    values = _stack_values(adjusted_score)
    neutral_band = _mapping(diagnostics_config.get("neutral_band"))
    lower = _float_value(neutral_band.get("lower"), default=0.45)
    upper = _float_value(neutral_band.get("upper"), default=0.55)
    warning_ratio = _float_value(
        diagnostics_config.get("neutral_compression_warning_ratio"),
        default=0.50,
    )
    min_std = _float_value(diagnostics_config.get("min_score_std"), default=0.03)
    if values.empty:
        return {
            "mean": 0.0,
            "std": 0.0,
            "p05": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "neutral_score_ratio_0_45_to_0_55": 0.0,
            "warning": "Scores are unavailable and cannot drive allocation changes.",
        }
    neutral_ratio = float(((values >= lower) & (values <= upper)).mean())
    std = float(values.std(ddof=0))
    warning = ""
    if neutral_ratio >= warning_ratio or std < min_std:
        warning = (
            "Scores are compressed around neutral and may not drive meaningful allocation "
            "changes."
        )
    return {
        "mean": _round_float(values.mean()),
        "std": _round_float(std),
        "p05": _round_float(values.quantile(0.05)),
        "p50": _round_float(values.quantile(0.50)),
        "p95": _round_float(values.quantile(0.95)),
        "neutral_score_ratio_0_45_to_0_55": _round_float(neutral_ratio),
        "warning": warning,
    }


def score_to_target_weight_diagnostics(
    adjusted_score: pd.DataFrame,
    target: pd.DataFrame,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    tradable_columns = [column for column in adjusted_score.columns if column in target.columns]
    target_tradable = target.reindex(columns=tradable_columns).fillna(0.0)
    target_delta = target_tradable.diff().abs().iloc[1:]
    epsilon = _float_value(diagnostics_config.get("weight_change_epsilon"), default=0.000001)
    min_delta = _float_value(
        diagnostics_config.get("min_score_to_target_delta"),
        default=0.005,
    )
    if target_delta.empty:
        mean_delta = 0.0
        max_delta = 0.0
        changed_ratio = 0.0
    else:
        mean_delta = float(target_delta.stack().mean())
        max_delta = float(target_delta.stack().max())
        changed_ratio = float((target_delta > epsilon).stack().mean())
    score_values = _stack_values(adjusted_score.reindex(columns=tradable_columns))
    weight_values = _stack_values(target_tradable)
    correlation = _correlation(score_values, weight_values)
    warning = ""
    if mean_delta < min_delta:
        warning = "Score changes are weakly transmitted into target weights."
    return {
        "mean_abs_target_weight_delta": _round_float(mean_delta),
        "max_abs_target_weight_delta": _round_float(max_delta),
        "asset_days_with_weight_change_ratio": _round_float(changed_ratio),
        "score_weight_correlation": _round_float(correlation),
        "warning": warning,
    }


def target_to_actual_weight_diagnostics(
    actual: pd.DataFrame,
    rebalance_days: int,
    rebalance_opportunity_days: int,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    actual_delta = actual.diff().abs().iloc[1:]
    if actual_delta.empty:
        mean_delta = 0.0
        max_delta = 0.0
    else:
        mean_delta = float(actual_delta.stack().mean())
        max_delta = float(actual_delta.stack().max())
    suppression_ratio = (
        0.0
        if rebalance_opportunity_days <= 0
        else 1.0 - min(rebalance_days / rebalance_opportunity_days, 1.0)
    )
    threshold = _float_value(
        diagnostics_config.get("rebalance_suppression_warning_ratio"),
        default=0.70,
    )
    warning = ""
    if suppression_ratio >= threshold and rebalance_opportunity_days > 0:
        warning = "Rebalance threshold may be suppressing signal-driven allocation changes."
    return {
        "mean_abs_actual_weight_delta": _round_float(mean_delta),
        "max_abs_actual_weight_delta": _round_float(max_delta),
        "rebalance_days": int(rebalance_days),
        "rebalance_opportunity_days": int(rebalance_opportunity_days),
        "rebalance_suppression_ratio": _round_float(suppression_ratio),
        "warning": warning,
    }


def constraint_binding_diagnostics(
    binding: dict[str, Any],
    *,
    total_days: int,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    total = max(total_days, 1)
    threshold = _float_value(
        diagnostics_config.get("constraint_binding_warning_ratio"),
        default=0.25,
    )
    single_days = _int_value(binding.get("single_asset_cap_binding_days"))
    sector_days = _int_value(binding.get("sector_cap_binding_days"))
    cash_days = _int_value(binding.get("cash_floor_binding_days"))
    most_constrained = _strings(binding.get("most_constrained_assets"))
    ratio = max(single_days / total, sector_days / total, cash_days / total)
    warning = ""
    if ratio >= threshold:
        warning = (
            "Position, sector, or cash constraints frequently bind and may suppress signal "
            "expression."
        )
    return {
        "single_asset_cap_binding_days": single_days,
        "sector_cap_binding_days": sector_days,
        "cash_floor_binding_days": cash_days,
        "single_asset_cap_binding_ratio": _round_float(single_days / total),
        "sector_cap_binding_ratio": _round_float(sector_days / total),
        "cash_floor_binding_ratio": _round_float(cash_days / total),
        "most_constrained_assets": most_constrained,
        "warning": warning,
    }


def turnover_cost_impact(
    simulation: SensitivitySimulation,
    baseline_simulation: SensitivitySimulation,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    turnover = _float_value(simulation.metrics.get("turnover"))
    baseline_turnover = _float_value(baseline_simulation.metrics.get("turnover"))
    delta = turnover - baseline_turnover
    warning_delta = _float_value(
        diagnostics_config.get("turnover_warning_delta_vs_baseline"),
        default=0.20,
    )
    warning = ""
    if delta > warning_delta:
        warning = "Higher sensitivity improves signal expression but increases turnover."
    return {
        "turnover": _round_float(turnover),
        "turnover_delta_vs_baseline": _round_float(delta),
        "estimated_cost_drag": _round_float(simulation.transaction_cost_drag),
        "warning": warning,
    }


def _target_weights_from_scores(
    adjusted_score: pd.DataFrame,
    *,
    baseline: ProductionParameters,
    profile_config: dict[str, Any],
    tradable_assets: list[str],
    defensive_assets: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = tradable_assets + defensive_assets
    target = pd.DataFrame(0.0, index=adjusted_score.index, columns=columns)
    risk_budget = adjusted_score.mean(axis=1).fillna(0.0).apply(_risk_budget_for_score)
    method = str(profile_config.get("score_to_weight_method") or "linear")
    scores = adjusted_score.reindex(columns=tradable_assets).fillna(0.5)
    if method == "softmax":
        temperature = max(
            _float_value(profile_config.get("softmax_temperature"), default=1.0),
            1e-9,
        )
        shifted = scores.sub(scores.max(axis=1), axis=0) / temperature
        raw = np.exp(shifted).replace([np.inf, -np.inf], np.nan)
        row_sums = raw.sum(axis=1).replace(0.0, np.nan)
        allocations = raw.div(row_sums, axis=0).fillna(0.0).mul(risk_budget, axis=0)
    else:
        positive = scores.clip(lower=0.0)
        row_sums = positive.sum(axis=1).replace(0.0, np.nan)
        allocations = positive.div(row_sums, axis=0).fillna(0.0).mul(risk_budget, axis=0)
    max_single = _float_value(profile_config.get("max_single_asset_weight"), default=0.30)
    max_sector = _float_value(profile_config.get("max_sector_weight"), default=0.60)
    min_cash = _float_value(profile_config.get("min_cash_weight"), default=0.05)
    single_binding_mask = allocations.max(axis=1) > max_single + 1e-12
    single_asset_counts = (allocations > max_single + 1e-12).sum(axis=0)
    capped = _apply_asset_cap(allocations, max_single)
    capped, sector_binding = _apply_sector_cap(
        capped,
        baseline=baseline,
        cap=max_sector,
        tradable_assets=tradable_assets,
    )
    used_before_cash = capped.sum(axis=1)
    cash_floor_mask = used_before_cash > (1.0 - min_cash) + 1e-12
    if cash_floor_mask.any():
        scale = (1.0 - min_cash) / used_before_cash.where(cash_floor_mask)
        capped.loc[cash_floor_mask, tradable_assets] = capped.loc[
            cash_floor_mask,
            tradable_assets,
        ].mul(scale.loc[cash_floor_mask], axis=0)
    target.loc[:, tradable_assets] = capped.reindex(columns=tradable_assets).fillna(0.0)
    used = target.loc[:, tradable_assets].sum(axis=1)
    residual = (1.0 - used).clip(lower=0.0)
    defensive = _primary_defensive_asset(defensive_assets)
    if defensive is not None:
        target.loc[:, defensive] = residual
    constrained_assets = Counter(
        {
            str(asset): int(count)
            for asset, count in single_asset_counts.items()
            if int(count) > 0
        }
    )
    constrained_assets.update(sector_binding["asset_counts"])
    return target.fillna(0.0), {
        "single_asset_cap_binding_days": int(single_binding_mask.sum()),
        "sector_cap_binding_days": int(sector_binding["sector_cap_binding_days"]),
        "cash_floor_binding_days": int(cash_floor_mask.sum()),
        "most_constrained_assets": [
            asset for asset, _count in constrained_assets.most_common(5)
        ],
    }


def _apply_sector_cap(
    allocations: pd.DataFrame,
    *,
    baseline: ProductionParameters,
    cap: float,
    tradable_assets: list[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    capped = allocations.copy()
    binding_days: set[pd.Timestamp] = set()
    asset_counts: Counter[str] = Counter()
    sector_assets: dict[str, list[str]] = {}
    for asset in tradable_assets:
        sector_assets.setdefault(baseline.sector_for_asset(asset), []).append(asset)
    for assets in sector_assets.values():
        columns = [asset for asset in assets if asset in capped.columns]
        if not columns:
            continue
        sector_sum = capped.loc[:, columns].sum(axis=1)
        mask = sector_sum > cap + 1e-12
        if not mask.any():
            continue
        binding_days.update(pd.Timestamp(index) for index in capped.index[mask])
        scale = cap / sector_sum.where(mask)
        capped.loc[mask, columns] = capped.loc[mask, columns].mul(scale.loc[mask], axis=0)
        for asset in columns:
            asset_counts[asset] += int(mask.sum())
    return capped, {
        "sector_cap_binding_days": len(binding_days),
        "asset_counts": asset_counts,
    }


def _actual_weights_from_target(
    target: pd.DataFrame,
    *,
    threshold: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    rows: list[pd.Series] = []
    current = pd.Series(0.0, index=target.columns)
    rebalance_days = 0
    opportunity_days = 0
    first = True
    for _timestamp, target_row in target.iterrows():
        diff = (target_row - current).abs()
        max_diff = float(diff.max()) if not diff.empty else 0.0
        if max_diff > 1e-12:
            if not first:
                opportunity_days += 1
            if first or max_diff >= threshold:
                current = target_row.copy()
                if not first:
                    rebalance_days += 1
        rows.append(current.copy())
        first = False
    actual = pd.DataFrame(rows, index=target.index, columns=target.columns).fillna(0.0)
    return actual, {
        "rebalance_days": rebalance_days,
        "rebalance_opportunity_days": opportunity_days,
    }


def _composite_scores(
    features: dict[str, pd.DataFrame],
    *,
    weights: dict[str, float],
    tradable_assets: list[str],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    index = next(iter(features.values())).index
    composite = pd.DataFrame(0.0, index=index, columns=tradable_assets)
    contribution_frames: dict[str, pd.DataFrame] = {}
    for signal, weight in weights.items():
        frame = features.get(signal)
        if frame is None:
            continue
        weighted = frame.reindex(columns=tradable_assets).fillna(0.5) * float(weight)
        contribution_frames[signal] = weighted
        composite = composite.add(weighted, fill_value=0.0)
    return composite.clip(0.0, 1.0), contribution_frames


def _rank_profiles(profiles: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    weights = _mapping(_mapping(config.get("ranking")).get("weights"))
    ranked: list[dict[str, Any]] = []
    for profile in profiles:
        metrics = _mapping(profile.get("ranking_metrics"))
        score = _ranking_score(metrics, weights)
        ranked.append(
            {
                "profile_name": profile.get("profile_name", ""),
                "ranking_score": score,
                "ranking_metrics": metrics,
            }
        )
    ranked = sorted(
        ranked,
        key=lambda item: (
            -_float_value(item.get("ranking_score")),
            str(item.get("profile_name")),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    best_profile = str(ranked[0]["profile_name"]) if ranked else ""
    best = next(
        (profile for profile in profiles if profile.get("profile_name") == best_profile),
        {},
    )
    return {
        "best_profile": best_profile,
        "reason": _ranking_reason(best),
        "profiles": ranked,
    }


def _ranking_score(metrics: dict[str, Any], weights: dict[str, Any]) -> float:
    score = 0.0
    for key, raw_weight in weights.items():
        value = _float_value(metrics.get(key))
        score += _float_value(raw_weight) * value
    return round(score, 12)


def _ranking_reason(profile: dict[str, Any]) -> str:
    if not profile:
        return "No profile could be ranked."
    metrics = _mapping(profile.get("ranking_metrics"))
    actual = _float_value(metrics.get("actual_rebalance_effectiveness"))
    turnover_penalty = _float_value(metrics.get("turnover_penalty"))
    constraint_penalty = _float_value(metrics.get("constraint_binding_penalty"))
    if actual > 0.0 and turnover_penalty < 0.35 and constraint_penalty < 0.50:
        return (
            "Improved score-to-actual-weight transmission with acceptable turnover and "
            "constraint binding."
        )
    return (
        "Best profile is advisory only; review score transmission, turnover, and binding "
        "constraints before any manual portfolio-construction change."
    )


def _diagnosis(
    profiles: list[dict[str, Any]],
    ranking: dict[str, Any],
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    baseline = next(
        (profile for profile in profiles if profile.get("profile_name") == "baseline_v0_1"),
        profiles[0] if profiles else {},
    )
    candidates: list[tuple[str, float]] = []
    score_dispersion = _mapping(baseline.get("score_dispersion"))
    neutral_ratio = _float_value(score_dispersion.get("neutral_score_ratio_0_45_to_0_55"))
    neutral_threshold = _float_value(
        diagnostics_config.get("neutral_compression_warning_ratio"),
        default=0.50,
    )
    if neutral_ratio >= neutral_threshold:
        candidates.append(("score_compression", neutral_ratio))
    score_to_target = _mapping(baseline.get("score_to_target_weight"))
    min_delta = _float_value(
        diagnostics_config.get("min_score_to_target_delta"),
        default=0.005,
    )
    target_delta = _float_value(score_to_target.get("mean_abs_target_weight_delta"))
    if target_delta < min_delta:
        candidates.append(("score_to_weight_mapping", min_delta - target_delta))
    target_to_actual = _mapping(baseline.get("target_to_actual_weight"))
    suppression = _float_value(target_to_actual.get("rebalance_suppression_ratio"))
    suppression_threshold = _float_value(
        diagnostics_config.get("rebalance_suppression_warning_ratio"),
        default=0.70,
    )
    if suppression >= suppression_threshold:
        candidates.append(("rebalance_threshold", suppression))
    constraint = _mapping(baseline.get("constraint_binding"))
    constraint_ratio = max(
        _float_value(constraint.get("single_asset_cap_binding_ratio")),
        _float_value(constraint.get("sector_cap_binding_ratio")),
        _float_value(constraint.get("cash_floor_binding_ratio")),
    )
    constraint_threshold = _float_value(
        diagnostics_config.get("constraint_binding_warning_ratio"),
        default=0.25,
    )
    if constraint_ratio >= constraint_threshold:
        candidates.append(("position_or_sector_constraints", constraint_ratio))
    if candidates:
        ordered = sorted(candidates, key=lambda item: (-item[1], item[0]))
        primary = ordered[0][0]
        secondary = ordered[1][0] if len(ordered) > 1 else "none"
    else:
        primary = "none"
        secondary = "none"
    best_profile = str(ranking.get("best_profile") or "")
    best = next(
        (profile for profile in profiles if profile.get("profile_name") == best_profile),
        {},
    )
    best_metrics = _mapping(best.get("ranking_metrics"))
    too_insensitive = primary in {
        "rebalance_threshold",
        "score_to_weight_mapping",
        "position_or_sector_constraints",
    } and _float_value(best_metrics.get("actual_rebalance_effectiveness")) > 0.0
    if primary == "none":
        reason = (
            "Portfolio sensitivity diagnostics did not find a major score-to-weight "
            "transmission bottleneck."
        )
    else:
        reason = (
            f"Primary bottleneck appears to be {primary}; latest best profile is "
            f"{best_profile or 'UNKNOWN'}."
        )
    return {
        "primary_bottleneck": primary,
        "secondary_bottleneck": secondary,
        "portfolio_is_too_insensitive": bool(too_insensitive),
        "reason": reason,
    }


def _ranking_metrics(
    *,
    simulation: SensitivitySimulation,
    baseline_simulation: SensitivitySimulation,
    score_to_target: dict[str, Any],
    target_to_actual: dict[str, Any],
    constraint_binding: dict[str, Any],
    windows: tuple[WalkForwardWindow, ...],
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    metrics = simulation.metrics
    baseline_metrics = baseline_simulation.metrics
    turnover_delta = _float_value(metrics.get("turnover")) - _float_value(
        baseline_metrics.get("turnover")
    )
    constraint_penalty = max(
        _float_value(constraint_binding.get("single_asset_cap_binding_ratio")),
        _float_value(constraint_binding.get("sector_cap_binding_ratio")),
        _float_value(constraint_binding.get("cash_floor_binding_ratio")),
    )
    suppression = _float_value(target_to_actual.get("rebalance_suppression_ratio"))
    return {
        "score_to_weight_effectiveness": _round_float(
            _float_value(score_to_target.get("mean_abs_target_weight_delta"))
            * max(_float_value(score_to_target.get("score_weight_correlation")), 0.0)
        ),
        "actual_rebalance_effectiveness": _round_float(1.0 - suppression),
        "improvement_in_sharpe": _round_float(
            _float_value(metrics.get("sharpe_ratio"))
            - _float_value(baseline_metrics.get("sharpe_ratio"))
        ),
        "improvement_in_max_drawdown": _round_float(
            _float_value(metrics.get("max_drawdown"))
            - _float_value(baseline_metrics.get("max_drawdown"))
        ),
        "turnover_penalty": _round_float(max(turnover_delta, 0.0)),
        "constraint_binding_penalty": _round_float(constraint_penalty),
        "stability_across_walk_forward_windows": _round_float(
            _walk_forward_stability(
                simulation.actual,
                windows=windows,
                diagnostics_config=diagnostics_config,
            )
        ),
    }


def _walk_forward_stability(
    actual: pd.DataFrame,
    *,
    windows: tuple[WalkForwardWindow, ...],
    diagnostics_config: dict[str, Any],
) -> float:
    if not windows or actual.empty:
        return 0.0
    threshold = _float_value(
        diagnostics_config.get("rebalance_suppression_warning_ratio"),
        default=0.70,
    )
    passing = 0
    evaluated = 0
    for window in windows:
        frame = actual.loc[
            (actual.index.date >= window.validation_start)
            & (actual.index.date <= window.validation_end)
        ]
        if len(frame.index) < 2:
            continue
        delta = frame.diff().abs().sum(axis=1).iloc[1:]
        opportunity = int((delta > 1e-12).sum())
        if opportunity <= 0:
            continue
        suppression = 1.0 - min(opportunity / max(len(delta), 1), 1.0)
        passing += int(suppression < threshold)
        evaluated += 1
    return 0.0 if evaluated == 0 else passing / evaluated


def _fallback_signal_dilution(
    snapshot_summary: dict[str, Any],
    baseline: ProductionParameters,
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    fallback_signals = sorted(
        dict.fromkeys(
            [
                *_strings(snapshot_summary.get("proxy_signals")),
                *_strings(snapshot_summary.get("neutral_fallback_signals")),
            ]
        )
    )
    real_signals = _strings(snapshot_summary.get("real_signals"))
    fallback_weight = sum(_float_value(baseline.weights.get(signal)) for signal in fallback_signals)
    real_weight = sum(_float_value(baseline.weights.get(signal)) for signal in real_signals)
    threshold = _float_value(
        diagnostics_config.get("fallback_weight_warning_ratio"),
        default=0.30,
    )
    warning = ""
    if fallback_weight >= threshold:
        warning = "Fallback or proxy signals still carry material configured weight."
    return {
        "real_signals": real_signals,
        "fallback_or_proxy_signals": fallback_signals,
        "real_signal_weight": _round_float(real_weight),
        "fallback_or_proxy_signal_weight": _round_float(fallback_weight),
        "warning": warning,
    }


def _performance_payload(metrics: dict[str, float | int]) -> dict[str, float]:
    return {
        "annualized_return": _round_float(metrics.get("annualized_return")),
        "max_drawdown": _round_float(metrics.get("max_drawdown")),
        "sharpe_ratio": _round_float(metrics.get("sharpe_ratio")),
        "sortino_ratio": _round_float(metrics.get("sortino_ratio")),
        "calmar_ratio": _round_float(metrics.get("calmar_ratio")),
        "turnover": _round_float(metrics.get("turnover")),
    }


def _daily_rows(
    *,
    strategy_returns: pd.Series,
    gross_returns: pd.Series,
    costs: pd.Series,
    turnover: pd.Series,
    equity: pd.Series,
    target: pd.DataFrame,
    actual: pd.DataFrame,
    exposure: pd.Series,
    daily_score: pd.Series,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for timestamp in strategy_returns.index:
        rows.append(
            {
                "date": pd.Timestamp(timestamp).date().isoformat(),
                "strategy_return": float(strategy_returns.loc[timestamp]),
                "gross_return": float(gross_returns.loc[timestamp]),
                "transaction_cost": float(costs.loc[timestamp]),
                "turnover": float(turnover.loc[timestamp]),
                "equity": float(equity.loc[timestamp]),
                "risk_asset_exposure": float(exposure.loc[timestamp]),
                "average_composite_score": float(daily_score.loc[timestamp]),
                "target_weights": {
                    str(asset): _round_float(target.loc[timestamp, asset])
                    for asset in target.columns
                },
                "portfolio_weights": {
                    str(asset): _round_float(actual.loc[timestamp, asset])
                    for asset in actual.columns
                },
            }
        )
    return rows


def _score_rows(
    *,
    composite: pd.DataFrame,
    adjusted_score: pd.DataFrame,
    contribution_frames: dict[str, pd.DataFrame],
    weights: dict[str, float],
    tradable_assets: list[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for timestamp in composite.index:
        signal_date = pd.Timestamp(timestamp).date().isoformat()
        for asset in tradable_assets:
            contributions = {
                signal: _round_float(frame.loc[timestamp, asset])
                for signal, frame in contribution_frames.items()
                if asset in frame.columns
            }
            signal_values = {
                signal: _signal_value_from_contribution(value, weights.get(signal, 0.0))
                for signal, value in contributions.items()
            }
            rows.append(
                {
                    "date": signal_date,
                    "asset": asset,
                    "composite_score": _round_float(composite.loc[timestamp, asset]),
                    "adjusted_score": _round_float(adjusted_score.loc[timestamp, asset]),
                    "signal_values": signal_values,
                    "signal_contributions": contributions,
                }
            )
    return rows


def _empty_simulation(profile_name: str, profile_config: dict[str, Any]) -> SensitivitySimulation:
    metrics = calculate_portfolio_metrics(pd.Series(), pd.Series(), pd.Series()).to_dict()
    return SensitivitySimulation(
        profile_name=profile_name,
        profile_config=profile_config,
        target=pd.DataFrame(),
        actual=pd.DataFrame(),
        score=pd.DataFrame(),
        adjusted_score=pd.DataFrame(),
        daily_rows=(),
        score_rows=(),
        metrics=metrics,
        gross_turnover=0.0,
        transaction_cost_drag=0.0,
        rebalance_opportunity_days=0,
        rebalance_days=0,
        constraint_binding={
            "single_asset_cap_binding_days": 0,
            "sector_cap_binding_days": 0,
            "cash_floor_binding_days": 0,
            "most_constrained_assets": [],
        },
    )


def _signal_snapshot_path(shadow_config: ShadowBacktestConfig, as_of: date) -> Path:
    root = resolve_project_path(shadow_config.data.signal_snapshot_dir)
    exact = root / as_of.isoformat() / "signal_snapshot.json"
    if exact.exists():
        return exact
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/signal_snapshot.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return exact
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _latest_shadow_backtest_supporting_path(as_of: date) -> Path | None:
    exact = (
        PROJECT_ROOT
        / "artifacts"
        / "shadow_backtest"
        / as_of.isoformat()
        / "shadow_backtest_summary.json"
    )
    if exact.exists():
        return exact
    root = PROJECT_ROOT / "artifacts" / "shadow_backtest"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/shadow_backtest_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _selected_profiles(
    profile_map: dict[str, Any],
    profile_names: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if profile_names is None:
        return tuple(profile_map)
    selected = tuple(dict.fromkeys(str(name) for name in profile_names if str(name)))
    unknown = [name for name in selected if name not in profile_map]
    if unknown:
        raise ValueError("unknown portfolio sensitivity profile(s): " + ", ".join(unknown))
    if not selected:
        raise ValueError("at least one portfolio sensitivity profile is required")
    return selected


def _validate_portfolio_sensitivity_config(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != "none":
        raise ValueError("portfolio sensitivity config production_effect must be none")
    if payload.get("manual_review_required") is not True:
        raise ValueError("portfolio sensitivity config manual_review_required must be true")
    if payload.get("auto_promotion") is not False:
        raise ValueError("portfolio sensitivity config auto_promotion must be false")
    profiles = _mapping(payload.get("profiles"))
    if not profiles:
        raise ValueError("portfolio sensitivity config must define profiles")
    for name, profile in profiles.items():
        profile_map = _mapping(profile)
        for key in (
            "score_sensitivity_multiplier",
            "rebalance_threshold",
            "max_single_asset_weight",
            "max_sector_weight",
            "min_cash_weight",
            "score_to_weight_method",
        ):
            if key not in profile_map:
                raise ValueError(f"{name} missing {key}")
        method = str(profile_map.get("score_to_weight_method"))
        if method not in {"linear", "softmax"}:
            raise ValueError(f"{name} unsupported score_to_weight_method: {method}")
        if method == "softmax" and "softmax_temperature" not in profile_map:
            raise ValueError(f"{name} missing softmax_temperature")
    ranking = _mapping(payload.get("ranking"))
    if not _mapping(ranking.get("weights")):
        raise ValueError("portfolio sensitivity config missing ranking weights")


def _failure_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    reason: str,
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "schema_version": PORTFOLIO_SENSITIVITY_SCHEMA_VERSION,
        "report_type": PORTFOLIO_SENSITIVITY_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-sensitivity-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "FAILED",
            "backtest_mode": "blocked",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "config_path": str(config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
        },
        "inputs": {
            "portfolio_sensitivity_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
        },
        "input_artifacts": {
            "portfolio_sensitivity_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
        },
        "data_quality": {"status": "FAILED"},
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": _mapping(config.get("diagnostics")),
        },
        "profiles": [],
        "ranking": {"best_profile": "", "reason": reason, "profiles": []},
        "diagnosis": {
            "primary_bottleneck": "blocked",
            "secondary_bottleneck": "none",
            "portfolio_is_too_insensitive": False,
            "reason": reason,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": reason,
        },
        "warnings": [reason],
        "safety": _safety_payload(),
    }


def _blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    data_quality_status: str,
    data_quality_report_path: Path,
    selected_profiles: tuple[str, ...],
    warnings: list[str],
    dry_run: bool,
    reason: str,
    data_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": PORTFOLIO_SENSITIVITY_SCHEMA_VERSION,
        "report_type": PORTFOLIO_SENSITIVITY_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-sensitivity-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": data_quality_status,
            "backtest_mode": "blocked",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "profile_count": len(selected_profiles),
            "config_path": str(config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
        },
        "inputs": {
            "portfolio_sensitivity_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "data_quality_report": str(data_quality_report_path),
        },
        "input_artifacts": {
            "portfolio_sensitivity_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "data_quality_report": str(data_quality_report_path),
        },
        "data_quality": {
            "status": data_quality_status,
            "quality_report_path": str(data_quality_report_path),
            "can_promote_candidate": False,
        },
        "data_gate": data_gate or {"status": data_quality_status},
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": _mapping(config.get("diagnostics")),
        },
        "profiles": [],
        "ranking": {"best_profile": "", "reason": reason, "profiles": []},
        "diagnosis": {
            "primary_bottleneck": "blocked",
            "secondary_bottleneck": "none",
            "portfolio_is_too_insensitive": False,
            "reason": reason,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": reason,
        },
        "warnings": list(dict.fromkeys([*warnings, reason])),
        "safety": _safety_payload(),
    }


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_sensitivity"
    output = _mapping(config.get("output"))
    return resolve_project_path(
        str(output.get("portfolio_sensitivity_dir") or default_portfolio_sensitivity_root())
    )


def _data_quality_report_dir(config: dict[str, Any], shadow_config: Any, *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_sensitivity" / "reports"
    return resolve_project_path(str(shadow_config.data.data_quality_report_dir))


def _backtest_artifact_root(shadow_config: ShadowBacktestConfig) -> Path:
    shadow_dir = resolve_project_path(str(shadow_config.output.shadow_backtest_dir))
    return shadow_dir.parent


def _manifest_path_from_data_gate(data_gate: dict[str, Any]) -> Path | None:
    path_text = str(data_gate.get("manifest") or "")
    if not path_text:
        return None
    path = Path(path_text)
    return path if path.exists() else None


def _manifest_date_from_data_gate(data_gate: dict[str, Any]) -> date | None:
    path = _manifest_path_from_data_gate(data_gate)
    if path is not None:
        try:
            return date.fromisoformat(path.parent.name)
        except ValueError:
            pass
    latest = _mapping(data_gate.get("latest_resolution"))
    raw = str(latest.get("resolved_backtest_manifest_date") or "")
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _config_hash(*paths: Path) -> str:
    digest = sha256()
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        digest.update(str(path).encode("utf-8"))
        digest.update(sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "production_parameters_modified": False,
        "candidate_promotion_triggered": False,
        "broker_action": False,
        "trading_action": False,
    }


def _primary_defensive_asset(defensive_assets: list[str]) -> str | None:
    if "SGOV" in defensive_assets:
        return "SGOV"
    if "CASH" in defensive_assets:
        return "CASH"
    return None


def _stack_values(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame.stack(), errors="coerce").dropna()


def _correlation(left: pd.Series, right: pd.Series) -> float:
    if left.empty or right.empty:
        return 0.0
    count = min(len(left), len(right))
    if count < 2:
        return 0.0
    left_values = left.iloc[:count].reset_index(drop=True)
    right_values = right.iloc[:count].reset_index(drop=True)
    value = left_values.corr(right_values)
    if pd.isna(value):
        return 0.0
    return float(value)


def _signal_value_from_contribution(contribution: float, weight: float) -> float:
    weight_float = _float_value(weight)
    if weight_float <= 0.0:
        return 0.0
    return _round_float(max(0.0, min(1.0, contribution / weight_float)))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []


def _int_value(value: object, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_value(value: object, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(number):
        return default
    return float(number)


def _round_float(value: object) -> float:
    return round(_float_value(value), 6)


def _format_metric(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"
