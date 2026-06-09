from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty
from ai_trading_system.trading_engine.data_registry_consistency import (
    portfolio_sensitivity_data_gate_context,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
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
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    SensitivitySimulation,
    _backtest_artifact_root,
    _config_hash,
    _format_metric,
    _manifest_date_from_data_gate,
    _manifest_path_from_data_gate,
    _mapping,
    _records,
    _round_float,
    _signal_snapshot_path,
    _simulate_sensitivity_profile,
    constraint_binding_diagnostics,
    latest_portfolio_sensitivity_path_on_or_before,
    load_portfolio_sensitivity_payload,
    score_to_target_weight_diagnostics,
    target_to_actual_weight_diagnostics,
)
from ai_trading_system.trading_engine.signal_ablation import classify_ablation_delta
from ai_trading_system.trading_engine.signal_calibration import (
    latest_signal_calibration_path_on_or_before,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    signal_snapshot_frames,
    signal_snapshot_summary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PORTFOLIO_CANDIDATES_SCHEMA_VERSION = 1
PORTFOLIO_CANDIDATES_REPORT_TYPE = "portfolio_candidates"
PORTFOLIO_CANDIDATES_ALIAS_REPORT_TYPE = "portfolio_candidates_report"
DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_candidate_profiles.yaml"
)


@dataclass(frozen=True)
class PortfolioCandidatesRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    recommended_candidate_path: Path


def default_portfolio_candidates_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_candidates"


def default_portfolio_candidates_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_candidates_json_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_candidates_dir(output_root, as_of) / "portfolio_candidates_summary.json"
    )


def default_portfolio_candidates_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_portfolio_candidates_dir(output_root, as_of) / "portfolio_candidates_summary.md"


def default_recommended_portfolio_candidate_path(output_root: Path, as_of: date) -> Path:
    return (
        default_portfolio_candidates_dir(output_root, as_of)
        / "recommended_portfolio_candidate.yaml"
    )


def latest_portfolio_candidates_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_portfolio_candidates_root()
    candidates = sorted(root.glob("*/portfolio_candidates_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_candidates_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_candidates_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidates_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def report_alias_paths(reports_dir: Path, as_of: date) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_candidates_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_candidates_{as_of.isoformat()}.md",
    )


def load_portfolio_candidate_config(
    path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio candidate config must be a mapping: {path}")
    _validate_portfolio_candidate_config(payload)
    return payload


def run_portfolio_candidates(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> PortfolioCandidatesRun:
    config = load_portfolio_candidate_config(config_path)
    root = _output_root(config, dry_run=dry_run)
    payload = build_portfolio_candidates_payload(
        as_of=as_of,
        profile_names=profile_names,
        config_path=config_path,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
    )
    resolved_as_of = portfolio_candidates_payload_date(
        payload,
        default_portfolio_candidates_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_portfolio_candidates_json_path(root, resolved_as_of)
    markdown_path = default_portfolio_candidates_markdown_path(root, resolved_as_of)
    recommended_path = default_recommended_portfolio_candidate_path(root, resolved_as_of)
    write_portfolio_candidates_report(payload, json_path, markdown_path)
    write_recommended_portfolio_candidate(payload, recommended_path)
    return PortfolioCandidatesRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        recommended_candidate_path=recommended_path,
    )


def build_portfolio_candidates_payload(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
    output_root: Path | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_portfolio_candidate_config(resolved_config_path)
    shadow_config_path = resolve_project_path(
        str(config.get("shadow_backtest_config_path") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    signal_ablation_config_path = resolve_project_path(
        str(config.get("signal_ablation_config_path") or DEFAULT_SIGNAL_ABLATION_CONFIG_PATH)
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    ablation_config = load_signal_ablation_config(signal_ablation_config_path)
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
    baseline_profile_name = str(config.get("baseline_profile") or "baseline_current")
    simulation_profile_names = tuple(dict.fromkeys([baseline_profile_name, *selected_profiles]))
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
            signal_ablation_config_path=signal_ablation_config_path,
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
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            signal_ablation_config_path=signal_ablation_config_path,
            data_quality_status="FAILED",
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[*warnings, f"data registry gate failed: {error_code}"],
            dry_run=dry_run,
            reason=str(
                data_gate.get("reason")
                or "Portfolio candidates were not run because data quality is blocked."
            ),
            data_gate=data_gate,
        )
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            signal_ablation_config_path=signal_ablation_config_path,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=list(warnings),
            dry_run=dry_run,
            reason="Portfolio candidates were not run because data quality is blocked.",
            data_gate=data_gate,
        )

    sensitivity_path = _latest_sensitivity_path_from_config(config, resolved_as_of)
    if sensitivity_path is None:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            signal_ablation_config_path=signal_ablation_config_path,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[
                *warnings,
                "portfolio sensitivity summary missing; run aits portfolio sensitivity --latest",
            ],
            dry_run=dry_run,
            reason=(
                "Portfolio sensitivity summary is missing; run "
                "`aits portfolio sensitivity --latest` before evaluating candidates."
            ),
            data_gate=data_gate,
        )
    sensitivity_payload = load_portfolio_sensitivity_payload(sensitivity_path)
    sensitivity_diagnosis = _mapping(sensitivity_payload.get("diagnosis"))
    sensitivity_primary = str(sensitivity_diagnosis.get("primary_bottleneck") or "UNKNOWN")
    if sensitivity_payload.get("report_type") != "portfolio_sensitivity":
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            signal_ablation_config_path=signal_ablation_config_path,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[*warnings, "portfolio sensitivity summary is unreadable"],
            dry_run=dry_run,
            reason="Portfolio sensitivity summary is unreadable; candidates were not run.",
            data_gate=data_gate,
        )
    if sensitivity_primary != "rebalance_threshold":
        warnings = [
            *warnings,
            (
                "portfolio sensitivity primary_bottleneck is "
                f"{sensitivity_primary}, not rebalance_threshold"
            ),
        ]

    signal_snapshot_path = _signal_snapshot_path(shadow_config, resolved_as_of)
    signal_snapshot_payload = load_signal_snapshot_payload(signal_snapshot_path)
    if not signal_snapshot_payload:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            signal_ablation_config_path=signal_ablation_config_path,
            data_quality_status="LIMITED",
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=[*warnings, "signal snapshot missing; portfolio candidates not run"],
            dry_run=dry_run,
            reason="Signal snapshot is missing; portfolio candidates need snapshot input.",
            data_gate=data_gate,
        )

    signal_frames = signal_snapshot_frames(signal_snapshot_payload)
    snapshot_summary = signal_snapshot_summary(signal_snapshot_payload)
    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, resolved_as_of)
    windows = shadow_backtest_module.generate_walk_forward_windows(
        trading_dates,
        shadow_config.walk_forward,
    )
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
        for name in simulation_profile_names
    }
    baseline_simulation = simulations[baseline_profile_name]
    diagnostics_config = _diagnostics_config(config)
    profile_payloads = {
        name: _profile_payload(
            simulation=simulations[name],
            baseline_simulation=baseline_simulation,
            baseline=baseline,
            shadow_config=shadow_config,
            prices=prices,
            signal_frames=signal_frames,
            signal_snapshot_payload=signal_snapshot_payload,
            snapshot_summary=snapshot_summary,
            ablation_config=ablation_config,
            start=full_start,
            end=full_end,
            diagnostics_config=diagnostics_config,
            guardrail_config=_mapping(config.get("guardrails")),
            data_gate=data_gate,
            is_baseline=name == baseline_profile_name,
        )
        for name in simulation_profile_names
    }
    ranking = _rank_profiles(profile_payloads, baseline_profile_name, config, data_gate)
    baseline_payload = _baseline_payload(profile_payloads[baseline_profile_name])
    candidate_payloads = [
        profile_payloads[name] for name in selected_profiles if name != baseline_profile_name
    ]
    best_profile = str(ranking.get("best_profile") or baseline_profile_name)
    calibration_path = latest_signal_calibration_path_on_or_before(resolved_as_of)
    shadow_summary_path = _latest_shadow_backtest_path_from_config(config, resolved_as_of)
    input_artifacts = {
        "portfolio_candidate_profiles": str(resolved_config_path),
        "shadow_backtest_config": str(shadow_config_path),
        "signal_ablation_config": str(signal_ablation_config_path),
        "baseline_parameters": str(baseline_path),
        "prices": str(prices_path),
        "data_quality_report": str(data_quality_report_path),
        "backtest_input_manifest": str(_manifest_path_from_data_gate(data_gate) or ""),
        "signal_snapshot": str(signal_snapshot_path),
        "signal_calibration": "" if calibration_path is None else str(calibration_path),
        "portfolio_sensitivity": str(sensitivity_path),
        "shadow_backtest": "" if shadow_summary_path is None else str(shadow_summary_path),
    }
    output_artifacts = {
        "summary_json": str(
            default_portfolio_candidates_json_path(resolved_output_root, resolved_as_of)
        ),
        "summary_markdown": str(
            default_portfolio_candidates_markdown_path(resolved_output_root, resolved_as_of)
        ),
        "recommended_candidate": str(
            default_recommended_portfolio_candidate_path(resolved_output_root, resolved_as_of)
        ),
    }
    return {
        "schema_version": PORTFOLIO_CANDIDATES_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATES_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-candidates-{resolved_as_of.isoformat()}",
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
            "profile_count": len(simulation_profile_names),
            "candidate_count": len(candidate_payloads),
            "config_path": str(resolved_config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(
                resolved_config_path,
                shadow_config_path,
                signal_ablation_config_path,
                baseline_path,
                signal_snapshot_path,
                sensitivity_path,
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
            "guardrails": _mapping(config.get("guardrails")),
            "ranking": _mapping(config.get("ranking")),
            "promotion_effect": "manual_review_only",
        },
        "portfolio_sensitivity_context": {
            "source_artifact": str(sensitivity_path),
            "primary_bottleneck": sensitivity_primary,
            "portfolio_is_too_insensitive": sensitivity_diagnosis.get(
                "portfolio_is_too_insensitive",
                False,
            ),
        },
        "signal_snapshot": {
            "path": str(signal_snapshot_path),
            **snapshot_summary,
        },
        "baseline": baseline_payload,
        "candidates": candidate_payloads,
        "profiles": [profile_payloads[name] for name in simulation_profile_names],
        "ranking": ranking,
        "recommended_candidate": {
            "profile_name": best_profile,
            "artifact": output_artifacts["recommended_candidate"],
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Portfolio candidate profile is advisory only. Signal snapshot quality "
                "remains LIMITED and production promotion is disabled."
            ),
        },
        "warnings": list(dict.fromkeys(warnings)),
        "safety": _safety_payload(),
    }


def write_portfolio_candidates_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_portfolio_candidates_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_portfolio_candidates_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": PORTFOLIO_CANDIDATES_ALIAS_REPORT_TYPE,
        "source_report_type": PORTFOLIO_CANDIDATES_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_portfolio_candidates_report(alias_payload, json_path, markdown_path)


def write_recommended_portfolio_candidate(payload: dict[str, Any], output_path: Path) -> Path:
    ranking = _mapping(payload.get("ranking"))
    best_profile = str(ranking.get("best_profile") or "")
    profiles = _records(payload.get("profiles"))
    profile = next(
        (item for item in profiles if str(item.get("profile_name")) == best_profile),
        {},
    )
    recommended = {
        "version": "recommended_portfolio_candidate_v0_1",
        "profile_name": best_profile,
        "source_portfolio_candidates": _mapping(payload.get("metadata")).get("run_id"),
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "profile_config": _mapping(profile.get("profile_config")),
        "ranking_score": profile.get("ranking_score", 0.0),
        "ranking_metrics": _mapping(profile.get("ranking_metrics")),
        "risk_guardrails": _mapping(profile.get("risk_guardrails")),
        "reason": ranking.get("reason", ""),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(recommended, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return output_path


def render_portfolio_candidates_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    data_gate = _mapping(payload.get("data_gate"))
    baseline = _mapping(payload.get("baseline"))
    ranking = _mapping(payload.get("ranking"))
    promotion = _mapping(payload.get("promotion_impact"))
    candidates = _records(payload.get("candidates"))
    lines = [
        "# Portfolio Construction Candidate Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- data_gate: `{data_gate.get('status', 'UNKNOWN')}`",
        f"- best_profile: `{ranking.get('best_profile', 'UNKNOWN')}`",
        f"- reason: {ranking.get('reason', '')}",
        "",
        "## 2. Data Gate and Inputs",
        "",
        f"- data_registry_consistency: `{data_gate.get('data_registry_consistency', 'UNKNOWN')}`",
        f"- latest_resolution: `{data_gate.get('latest_resolution_status', 'UNKNOWN')}`",
    ]
    for key, value in _mapping(payload.get("inputs")).items():
        if value:
            lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 3. Baseline Portfolio Construction",
            "",
            f"- profile_name: `{baseline.get('profile_name', '')}`",
            (
                "- rebalance_suppression_ratio: "
                f"`{_format_metric(_mapping(baseline.get('signal_transmission')).get('rebalance_suppression_ratio'))}`"
            ),
            (
                "- turnover: "
                f"`{_format_metric(_mapping(baseline.get('performance')).get('turnover'))}`"
            ),
            "",
            "## 4. Candidate Profiles Tested",
            "",
            "| Profile | Status | Ranking Score | Guardrail | Description |",
            "|---|---|---:|---|---|",
        ]
    )
    for candidate in candidates:
        guardrail = _mapping(candidate.get("risk_guardrails"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"`{candidate.get('status', '')}` | "
            f"{_format_metric(candidate.get('ranking_score'))} | "
            f"`{guardrail.get('guardrail_status', '')}` | "
            f"{candidate.get('description', '')} |"
        )
    lines.extend(
        [
            "",
            "## 5. Signal Transmission Comparison",
            "",
            (
                "| Profile | Score->Target Eff. | Target->Actual Eff. | "
                "Suppression | Mean Actual Delta | Rebalance Days |"
            ),
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for candidate in candidates:
        item = _mapping(candidate.get("signal_transmission"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"{_format_metric(item.get('score_to_target_weight_effectiveness'))} | "
            f"{_format_metric(item.get('target_to_actual_weight_effectiveness'))} | "
            f"{_format_metric(item.get('rebalance_suppression_ratio'))} | "
            f"{_format_metric(item.get('mean_abs_actual_weight_delta'))} | "
            f"{item.get('rebalance_days', 0)} |"
        )
    lines.extend(
        [
            "",
            "## 6. Performance and Risk Comparison",
            "",
            (
                "| Profile | Cumulative Return | Annualized Return | Max Drawdown | "
                "Volatility | Sharpe | Sortino | Calmar |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for candidate in candidates:
        item = _mapping(candidate.get("performance"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"{_format_metric(item.get('cumulative_return'))} | "
            f"{_format_metric(item.get('annualized_return'))} | "
            f"{_format_metric(item.get('max_drawdown'))} | "
            f"{_format_metric(item.get('volatility'))} | "
            f"{_format_metric(item.get('sharpe_ratio'))} | "
            f"{_format_metric(item.get('sortino_ratio'))} | "
            f"{_format_metric(item.get('calmar_ratio'))} |"
        )
    lines.extend(
        [
            "",
            "## 7. Turnover and Cost Impact",
            "",
            "| Profile | Turnover | Estimated Cost Drag | Turnover Relative Increase |",
            "|---|---:|---:|---:|",
        ]
    )
    for candidate in candidates:
        performance = _mapping(candidate.get("performance"))
        guardrail = _mapping(candidate.get("risk_guardrails"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"{_format_metric(performance.get('turnover'))} | "
            f"{_format_metric(performance.get('estimated_cost_drag'))} | "
            f"{_format_metric(guardrail.get('turnover_relative_increase'))} |"
        )
    lines.extend(
        [
            "",
            "## 8. Risk Guardrails",
            "",
            (
                "| Profile | Status | Drawdown Worse | Turnover Too High | "
                "Single Cap | Sector Cap | Cash Floor |"
            ),
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for candidate in candidates:
        item = _mapping(candidate.get("risk_guardrails"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"`{item.get('guardrail_status', '')}` | "
            f"{item.get('max_drawdown_worse_than_baseline', False)} | "
            f"{item.get('turnover_too_high', False)} | "
            f"{item.get('single_asset_cap_overused', False)} | "
            f"{item.get('sector_cap_overused', False)} | "
            f"{item.get('cash_floor_binding', False)} |"
        )
    lines.extend(
        [
            "",
            "## 9. Signal Contribution Impact",
            "",
            "| Profile | Positive | Negative | Neutral | Unstable | Promotion Credit | Reason |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for candidate in candidates:
        item = _mapping(candidate.get("signal_contribution"))
        lines.append(
            "| "
            f"`{candidate.get('profile_name', '')}` | "
            f"{item.get('positive_signals', 0)} | "
            f"{item.get('negative_signals', 0)} | "
            f"{item.get('neutral_signals', 0)} | "
            f"{item.get('unstable_signals', 0)} | "
            f"{item.get('promotion_credit_signals', 0)} | "
            f"{item.get('reason', '')} |"
        )
    lines.extend(
        [
            "",
            "## 10. Recommended Candidate Profile",
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
            "- Review whether lower rebalance thresholds improve signal transmission.",
            "- Confirm turnover and drawdown guardrails before any manual config proposal.",
            "- Keep candidate promotion disabled while signal snapshot quality remains LIMITED.",
            "- Do not write recommended candidate values into production without manual review.",
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


def load_portfolio_candidates_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_portfolio_candidates_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_CANDIDATES_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != PORTFOLIO_CANDIDATES_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    if not isinstance(payload.get("baseline"), dict):
        issues.append("baseline must be present")
    if not isinstance(payload.get("candidates"), list):
        issues.append("candidates must be a list")
    for candidate in _records(payload.get("candidates")):
        name = str(candidate.get("profile_name") or "")
        if not name:
            issues.append("candidate missing profile_name")
        for key in (
            "performance",
            "signal_transmission",
            "delta_vs_baseline",
            "risk_guardrails",
            "signal_contribution",
        ):
            if not isinstance(candidate.get(key), dict):
                issues.append(f"{name} missing {key}")
    promotion = _mapping(payload.get("promotion_impact"))
    if promotion.get("can_support_candidate_promotion") is not False:
        issues.append("portfolio candidates must not support candidate promotion")
    recommended = _mapping(payload.get("recommended_candidate"))
    if recommended.get("production_effect") not in {"none", None}:
        issues.append("recommended candidate production_effect must be none")
    if recommended.get("manual_review_required") not in {True, None}:
        issues.append("recommended candidate manual_review_required must be true")
    if recommended.get("auto_promotion") not in {False, None}:
        issues.append("recommended candidate auto_promotion must be false")
    safety = _mapping(payload.get("safety"))
    if safety.get("production_parameters_modified") not in {False, None}:
        issues.append("production_parameters_modified must be false")
    if safety.get("candidate_promotion_triggered") not in {False, None}:
        issues.append("candidate_promotion_triggered must be false")
    return issues


def portfolio_candidates_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("portfolio-candidates-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer portfolio candidates date from {source_path}") from exc


def _profile_payload(
    *,
    simulation: SensitivitySimulation,
    baseline_simulation: SensitivitySimulation,
    baseline: ProductionParameters,
    shadow_config: ShadowBacktestConfig,
    prices: pd.DataFrame,
    signal_frames: dict[str, pd.DataFrame],
    signal_snapshot_payload: dict[str, Any],
    snapshot_summary: dict[str, Any],
    ablation_config: SignalAblationConfig,
    start: date,
    end: date,
    diagnostics_config: dict[str, Any],
    guardrail_config: dict[str, Any],
    data_gate: dict[str, Any],
    is_baseline: bool,
) -> dict[str, Any]:
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
    constraint = constraint_binding_diagnostics(
        simulation.constraint_binding,
        total_days=len(simulation.target.index),
        diagnostics_config=diagnostics_config,
    )
    baseline_score_to_target = score_to_target_weight_diagnostics(
        baseline_simulation.adjusted_score,
        baseline_simulation.target,
        diagnostics_config,
    )
    baseline_target_to_actual = target_to_actual_weight_diagnostics(
        baseline_simulation.actual,
        baseline_simulation.rebalance_days,
        baseline_simulation.rebalance_opportunity_days,
        diagnostics_config,
    )
    transmission = _signal_transmission_payload(score_to_target, target_to_actual)
    baseline_transmission = _signal_transmission_payload(
        baseline_score_to_target,
        baseline_target_to_actual,
    )
    risk_guardrails = _risk_guardrails_payload(
        simulation=simulation,
        baseline_simulation=baseline_simulation,
        constraint_binding=constraint,
        guardrail_config=guardrail_config,
        data_gate=data_gate,
    )
    signal_contribution = _signal_contribution_summary(
        simulation=simulation,
        profile_config=simulation.profile_config,
        prices=prices,
        baseline=baseline,
        shadow_config=shadow_config,
        signal_frames=signal_frames,
        signal_snapshot_payload=signal_snapshot_payload,
        snapshot_summary=snapshot_summary,
        ablation_config=ablation_config,
        start=start,
        end=end,
    )
    warnings = [
        str(item)
        for item in (
            score_to_target.get("warning"),
            target_to_actual.get("warning"),
            constraint.get("warning"),
            risk_guardrails.get("warning"),
        )
        if str(item)
    ]
    if is_baseline:
        warnings = [item for item in warnings if item]
    return {
        "profile_name": simulation.profile_name,
        "description": simulation.profile_config.get("description", ""),
        "profile_config": simulation.profile_config,
        "status": "LIMITED",
        "performance": _performance_payload(simulation),
        "signal_transmission": transmission,
        "delta_vs_baseline": _delta_vs_baseline(transmission, baseline_transmission),
        "risk_guardrails": risk_guardrails,
        "signal_contribution": signal_contribution,
        "constraint_binding": constraint,
        "score_to_target_weight": score_to_target,
        "target_to_actual_weight": target_to_actual,
        "ranking_score": 0.0,
        "ranking_metrics": {},
        "hard_rejected": False,
        "hard_rejection_reasons": [],
        "warnings": list(dict.fromkeys(warnings)),
        "audit": {
            "score_row_count": len(simulation.score_rows),
            "daily_row_count": len(simulation.daily_rows),
            "production_position_limits": baseline.position_limits.model_dump(mode="json"),
            "is_baseline": is_baseline,
        },
    }


def _baseline_payload(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "profile_name": profile.get("profile_name", ""),
        "description": profile.get("description", ""),
        "profile_config": _mapping(profile.get("profile_config")),
        "performance": _mapping(profile.get("performance")),
        "signal_transmission": _mapping(profile.get("signal_transmission")),
        "risk_guardrails": _mapping(profile.get("risk_guardrails")),
    }


def _signal_transmission_payload(
    score_to_target: dict[str, Any],
    target_to_actual: dict[str, Any],
) -> dict[str, Any]:
    score_effectiveness = _float_value(score_to_target.get("mean_abs_target_weight_delta")) * max(
        _float_value(score_to_target.get("score_weight_correlation")),
        0.0,
    )
    suppression = _float_value(target_to_actual.get("rebalance_suppression_ratio"))
    return {
        "score_to_target_weight_effectiveness": _round_float(score_effectiveness),
        "target_to_actual_weight_effectiveness": _round_float(1.0 - suppression),
        "rebalance_suppression_ratio": _round_float(suppression),
        "mean_abs_actual_weight_delta": _round_float(
            target_to_actual.get("mean_abs_actual_weight_delta")
        ),
        "rebalance_days": _int_value(target_to_actual.get("rebalance_days")),
        "rebalance_opportunity_days": _int_value(
            target_to_actual.get("rebalance_opportunity_days")
        ),
    }


def _delta_vs_baseline(
    transmission: dict[str, Any],
    baseline_transmission: dict[str, Any],
) -> dict[str, Any]:
    return {
        "rebalance_suppression_ratio_delta": _round_float(
            _float_value(transmission.get("rebalance_suppression_ratio"))
            - _float_value(baseline_transmission.get("rebalance_suppression_ratio"))
        ),
        "mean_abs_actual_weight_delta_delta": _round_float(
            _float_value(transmission.get("mean_abs_actual_weight_delta"))
            - _float_value(baseline_transmission.get("mean_abs_actual_weight_delta"))
        ),
        "rebalance_days_delta": _int_value(transmission.get("rebalance_days"))
        - _int_value(baseline_transmission.get("rebalance_days")),
    }


def _performance_payload(simulation: SensitivitySimulation) -> dict[str, Any]:
    metrics = simulation.metrics
    return {
        "cumulative_return": _round_float(metrics.get("cumulative_return")),
        "annualized_return": _round_float(metrics.get("annualized_return")),
        "max_drawdown": _round_float(metrics.get("max_drawdown")),
        "volatility": _round_float(metrics.get("volatility")),
        "sharpe_ratio": _round_float(metrics.get("sharpe_ratio")),
        "sortino_ratio": _round_float(metrics.get("sortino_ratio")),
        "calmar_ratio": _round_float(metrics.get("calmar_ratio")),
        "turnover": _round_float(metrics.get("turnover")),
        "estimated_cost_drag": _round_float(simulation.transaction_cost_drag),
    }


def _risk_guardrails_payload(
    *,
    simulation: SensitivitySimulation,
    baseline_simulation: SensitivitySimulation,
    constraint_binding: dict[str, Any],
    guardrail_config: dict[str, Any],
    data_gate: dict[str, Any],
) -> dict[str, Any]:
    drawdown_limit = _float_value(guardrail_config.get("max_drawdown_worse_limit"), default=0.03)
    turnover_limit = _float_value(
        guardrail_config.get("turnover_relative_increase_limit"),
        default=0.30,
    )
    single_limit = _float_value(
        guardrail_config.get("max_single_asset_binding_ratio"),
        default=0.25,
    )
    sector_limit = _float_value(
        guardrail_config.get("max_sector_cap_binding_ratio"),
        default=0.25,
    )
    cash_limit = _float_value(
        guardrail_config.get("max_cash_floor_binding_ratio"),
        default=0.25,
    )
    baseline_drawdown = _float_value(baseline_simulation.metrics.get("max_drawdown"))
    drawdown = _float_value(simulation.metrics.get("max_drawdown"))
    drawdown_worse_by = max(0.0, baseline_drawdown - drawdown)
    baseline_turnover = _float_value(baseline_simulation.metrics.get("turnover"))
    turnover = _float_value(simulation.metrics.get("turnover"))
    turnover_relative_increase = _relative_increase(turnover, baseline_turnover)
    single_ratio = _float_value(constraint_binding.get("single_asset_cap_binding_ratio"))
    sector_ratio = _float_value(constraint_binding.get("sector_cap_binding_ratio"))
    cash_ratio = _float_value(constraint_binding.get("cash_floor_binding_ratio"))
    flags = {
        "max_drawdown_worse_than_baseline": drawdown_worse_by > drawdown_limit,
        "turnover_too_high": turnover_relative_increase > turnover_limit,
        "single_asset_cap_overused": single_ratio > single_limit,
        "sector_cap_overused": sector_ratio > sector_limit,
        "cash_floor_binding": cash_ratio > cash_limit,
        "data_gate_not_ok": data_gate.get("status") != "OK",
    }
    guardrail_status = "PASS" if not any(flags.values()) else "FAIL"
    warning = ""
    if guardrail_status == "FAIL":
        failed = [key for key, value in flags.items() if value]
        warning = "Risk guardrail breach: " + ", ".join(failed)
    return {
        **flags,
        "guardrail_status": guardrail_status,
        "max_drawdown_worse_by": _round_float(drawdown_worse_by),
        "turnover_relative_increase": _round_float(turnover_relative_increase),
        "single_asset_cap_binding_ratio": _round_float(single_ratio),
        "sector_cap_binding_ratio": _round_float(sector_ratio),
        "cash_floor_binding_ratio": _round_float(cash_ratio),
        "warning": warning,
    }


def _signal_contribution_summary(
    *,
    simulation: SensitivitySimulation,
    profile_config: dict[str, Any],
    prices: pd.DataFrame,
    baseline: ProductionParameters,
    shadow_config: ShadowBacktestConfig,
    signal_frames: dict[str, pd.DataFrame],
    signal_snapshot_payload: dict[str, Any],
    snapshot_summary: dict[str, Any],
    ablation_config: SignalAblationConfig,
    start: date,
    end: date,
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    signals_by_class: dict[str, list[str]] = {
        "positive": [],
        "negative": [],
        "neutral": [],
        "unstable": [],
    }
    promotion_credit = 0
    snapshot_status = str(snapshot_summary.get("status") or "UNKNOWN")
    signal_payloads = _mapping(signal_snapshot_payload.get("signals"))
    for signal in baseline.weights:
        ablated_weights = _remove_one_signal_weights(baseline.weights, signal)
        ablated = _simulate_sensitivity_profile(
            profile_name=f"{simulation.profile_name}__without_{signal}",
            profile_config=profile_config,
            prices=prices,
            baseline=baseline,
            shadow_config=shadow_config,
            signal_frames=signal_frames,
            start=start,
            end=end,
            signal_weights=ablated_weights,
        )
        contribution_class = classify_ablation_delta(
            _delta_metrics(simulation.metrics, ablated.metrics),
            ablation_config.thresholds,
        )
        counts[contribution_class] += 1
        signals_by_class.setdefault(contribution_class, []).append(signal)
        signal_payload = _mapping(signal_payloads.get(signal))
        if (
            snapshot_status == "OK"
            and signal_payload.get("quality") == "price_derived"
            and contribution_class == "positive"
        ):
            promotion_credit += 1
    if snapshot_status == "LIMITED":
        promotion_credit = 0
        reason = "Signal snapshot quality remains LIMITED."
    else:
        reason = "Portfolio candidate ablation is advisory only."
    return {
        "positive_signals": int(counts["positive"]),
        "negative_signals": int(counts["negative"]),
        "neutral_signals": int(counts["neutral"]),
        "unstable_signals": int(counts["unstable"]),
        "promotion_credit_signals": int(promotion_credit),
        "reason": reason,
        "signals_by_class": signals_by_class,
    }


def _rank_profiles(
    profile_payloads: dict[str, dict[str, Any]],
    baseline_profile_name: str,
    config: dict[str, Any],
    data_gate: dict[str, Any],
) -> dict[str, Any]:
    weights = _mapping(_mapping(config.get("ranking")).get("weights"))
    baseline = profile_payloads[baseline_profile_name]
    ranked: list[dict[str, Any]] = []
    for profile in profile_payloads.values():
        metrics = _ranking_metrics(profile, baseline, data_gate)
        score = _ranking_score(metrics, weights)
        hard_rejections = _hard_rejection_reasons(profile, data_gate)
        profile["ranking_metrics"] = metrics
        profile["ranking_score"] = score
        profile["hard_rejected"] = bool(hard_rejections)
        profile["hard_rejection_reasons"] = hard_rejections
        ranked.append(
            {
                "profile_name": profile.get("profile_name", ""),
                "ranking_score": score,
                "ranking_metrics": metrics,
                "hard_rejected": bool(hard_rejections),
                "hard_rejection_reasons": hard_rejections,
            }
        )
    ranked = sorted(
        ranked,
        key=lambda item: (
            bool(item.get("hard_rejected")),
            -_float_value(item.get("ranking_score")),
            str(item.get("profile_name")),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    baseline_score = _float_value(profile_payloads[baseline_profile_name].get("ranking_score"))
    candidate_rows = [
        row
        for row in ranked
        if row.get("profile_name") != baseline_profile_name and row.get("hard_rejected") is False
    ]
    if candidate_rows and _float_value(candidate_rows[0].get("ranking_score")) > baseline_score:
        best_profile = str(candidate_rows[0].get("profile_name") or "")
        reason = (
            "Improved signal transmission with acceptable turnover and no drawdown "
            "guardrail breach."
        )
    else:
        best_profile = baseline_profile_name
        reason = (
            "No candidate profile safely improved baseline after signal transmission, "
            "turnover, drawdown, and guardrail checks."
        )
    return {
        "best_profile": best_profile,
        "reason": reason,
        "profiles": ranked,
    }


def _ranking_metrics(
    profile: dict[str, Any],
    baseline: dict[str, Any],
    data_gate: dict[str, Any],
) -> dict[str, Any]:
    transmission = _mapping(profile.get("signal_transmission"))
    baseline_transmission = _mapping(baseline.get("signal_transmission"))
    performance = _mapping(profile.get("performance"))
    baseline_performance = _mapping(baseline.get("performance"))
    risk = _mapping(profile.get("risk_guardrails"))
    signal_improvement = max(
        _float_value(transmission.get("target_to_actual_weight_effectiveness"))
        - _float_value(baseline_transmission.get("target_to_actual_weight_effectiveness")),
        0.0,
    )
    sharpe_improvement = _float_value(performance.get("sharpe_ratio")) - _float_value(
        baseline_performance.get("sharpe_ratio")
    )
    drawdown_control = 1.0 - min(_float_value(risk.get("max_drawdown_worse_by")) / 0.03, 1.0)
    guardrail_pass = 1.0 if risk.get("guardrail_status") == "PASS" else 0.0
    if data_gate.get("status") != "OK":
        guardrail_pass = 0.0
    return {
        "signal_transmission_improvement": _round_float(signal_improvement),
        "sharpe_improvement": _round_float(sharpe_improvement),
        "drawdown_control": _round_float(drawdown_control),
        "turnover_penalty": _round_float(risk.get("turnover_relative_increase")),
        "guardrail_pass": _round_float(guardrail_pass),
    }


def _ranking_score(metrics: dict[str, Any], weights: dict[str, Any]) -> float:
    return _round_float(
        _float_value(weights.get("signal_transmission_improvement"), default=0.30)
        * _float_value(metrics.get("signal_transmission_improvement"))
        + _float_value(weights.get("sharpe_improvement"), default=0.20)
        * _float_value(metrics.get("sharpe_improvement"))
        + _float_value(weights.get("drawdown_control"), default=0.20)
        * _float_value(metrics.get("drawdown_control"))
        - _float_value(weights.get("turnover_penalty"), default=0.15)
        * _float_value(metrics.get("turnover_penalty"))
        + _float_value(weights.get("guardrail_pass"), default=0.15)
        * _float_value(metrics.get("guardrail_pass"))
    )


def _hard_rejection_reasons(profile: dict[str, Any], data_gate: dict[str, Any]) -> list[str]:
    risk = _mapping(profile.get("risk_guardrails"))
    reasons: list[str] = []
    if risk.get("max_drawdown_worse_than_baseline") is True:
        reasons.append("max_drawdown_worse_than_baseline_by_more_than_3pct")
    if risk.get("turnover_too_high") is True:
        reasons.append("turnover_increase_more_than_30pct")
    if data_gate.get("status") != "OK":
        reasons.append("data_gate_not_ok")
    return reasons


def _delta_metrics(
    baseline_metrics: dict[str, Any],
    ablation_metrics: dict[str, Any],
) -> dict[str, float]:
    keys = (
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
    delta = {
        f"{key}_delta": _float_value(ablation_metrics.get(key))
        - _float_value(baseline_metrics.get(key))
        for key in keys
    }
    delta["drawdown_reduction_delta"] = delta.pop("drawdown_reduction_ratio_delta")
    return {key: round(value, 12) for key, value in delta.items()}


def _remove_one_signal_weights(weights: dict[str, float], signal: str) -> dict[str, float]:
    remaining_total = sum(value for key, value in weights.items() if key != signal)
    if remaining_total <= 0.0:
        return {key: 0.0 for key in weights}
    return {
        key: 0.0 if key == signal else round(float(value) / remaining_total, 12)
        for key, value in weights.items()
    }


def _relative_increase(value: float, baseline: float) -> float:
    if baseline <= 1e-12:
        return 0.0 if value <= 1e-12 else 1.0
    return max((value - baseline) / abs(baseline), 0.0)


def _selected_profiles(
    profile_map: dict[str, Any],
    profile_names: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if profile_names is None:
        return tuple(profile_map)
    selected = tuple(dict.fromkeys(str(name) for name in profile_names if str(name)))
    unknown = [name for name in selected if name not in profile_map]
    if unknown:
        raise ValueError("unknown portfolio candidate profile(s): " + ", ".join(unknown))
    if not selected:
        raise ValueError("at least one portfolio candidate profile is required")
    return selected


def _validate_portfolio_candidate_config(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != "none":
        raise ValueError("portfolio candidate config production_effect must be none")
    if payload.get("manual_review_required") is not True:
        raise ValueError("portfolio candidate config manual_review_required must be true")
    if payload.get("auto_promotion") is not False:
        raise ValueError("portfolio candidate config auto_promotion must be false")
    profiles = _mapping(payload.get("profiles"))
    if not profiles:
        raise ValueError("portfolio candidate config must define profiles")
    baseline_profile = str(payload.get("baseline_profile") or "baseline_current")
    if baseline_profile not in profiles:
        raise ValueError("portfolio candidate baseline_profile must exist in profiles")
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
    if not _mapping(_mapping(payload.get("ranking")).get("weights")):
        raise ValueError("portfolio candidate config missing ranking weights")
    if not _mapping(payload.get("guardrails")):
        raise ValueError("portfolio candidate config missing guardrails")


def _failure_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    signal_ablation_config_path: Path,
    reason: str,
    dry_run: bool,
) -> dict[str, Any]:
    return _base_blocked_payload(
        as_of=as_of,
        generated_at=generated_at,
        config=config,
        config_path=config_path,
        shadow_config_path=shadow_config_path,
        signal_ablation_config_path=signal_ablation_config_path,
        data_quality_status="FAILED",
        data_quality_report_path=None,
        selected_profiles=(),
        warnings=[reason],
        dry_run=dry_run,
        reason=reason,
        data_gate={"status": "FAILED"},
    )


def _blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    signal_ablation_config_path: Path,
    data_quality_status: str,
    data_quality_report_path: Path,
    selected_profiles: tuple[str, ...],
    warnings: list[str],
    dry_run: bool,
    reason: str,
    data_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _base_blocked_payload(
        as_of=as_of,
        generated_at=generated_at,
        config=config,
        config_path=config_path,
        shadow_config_path=shadow_config_path,
        signal_ablation_config_path=signal_ablation_config_path,
        data_quality_status=data_quality_status,
        data_quality_report_path=data_quality_report_path,
        selected_profiles=selected_profiles,
        warnings=warnings,
        dry_run=dry_run,
        reason=reason,
        data_gate=data_gate or {"status": data_quality_status},
    )


def _base_blocked_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    signal_ablation_config_path: Path,
    data_quality_status: str,
    data_quality_report_path: Path | None,
    selected_profiles: tuple[str, ...],
    warnings: list[str],
    dry_run: bool,
    reason: str,
    data_gate: dict[str, Any],
) -> dict[str, Any]:
    inputs = {
        "portfolio_candidate_profiles": str(config_path),
        "shadow_backtest_config": str(shadow_config_path),
        "signal_ablation_config": str(signal_ablation_config_path),
    }
    if data_quality_report_path is not None:
        inputs["data_quality_report"] = str(data_quality_report_path)
    return {
        "schema_version": PORTFOLIO_CANDIDATES_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATES_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-candidates-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": data_quality_status,
            "backtest_mode": "blocked",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "profile_count": len(selected_profiles),
            "candidate_count": 0,
            "config_path": str(config_path),
            "shadow_backtest_config_path": str(shadow_config_path),
        },
        "inputs": inputs,
        "input_artifacts": inputs,
        "data_quality": {
            "status": data_quality_status,
            "quality_report_path": (
                "" if data_quality_report_path is None else str(data_quality_report_path)
            ),
            "can_promote_candidate": False,
        },
        "data_gate": data_gate,
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "guardrails": _mapping(config.get("guardrails")),
            "ranking": _mapping(config.get("ranking")),
            "promotion_effect": "manual_review_only",
        },
        "baseline": {},
        "candidates": [],
        "profiles": [],
        "ranking": {"best_profile": "", "reason": reason, "profiles": []},
        "recommended_candidate": {
            "profile_name": "",
            "artifact": "",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
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
        return PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_candidates"
    output = _mapping(config.get("output"))
    return resolve_project_path(
        str(output.get("portfolio_candidates_dir") or default_portfolio_candidates_root())
    )


def _data_quality_report_dir(
    config: dict[str, Any],
    shadow_config: ShadowBacktestConfig,
    *,
    dry_run: bool,
) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "portfolio_candidates" / "reports"
    return resolve_project_path(str(shadow_config.data.data_quality_report_dir))


def _latest_sensitivity_path_from_config(config: dict[str, Any], as_of: date) -> Path | None:
    input_config = _mapping(config.get("input"))
    raw_root = input_config.get("portfolio_sensitivity_dir")
    if raw_root:
        return latest_portfolio_sensitivity_path_on_or_before(
            as_of,
            resolve_project_path(str(raw_root)),
        )
    return latest_portfolio_sensitivity_path_on_or_before(as_of)


def _latest_shadow_backtest_path_from_config(config: dict[str, Any], as_of: date) -> Path | None:
    input_config = _mapping(config.get("input"))
    root = resolve_project_path(
        str(
            input_config.get("shadow_backtest_dir")
            or PROJECT_ROOT / "artifacts" / "shadow_backtest"
        )
    )
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
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _diagnostics_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "min_score_to_target_delta": 0.005,
        "weight_change_epsilon": 0.000001,
        "rebalance_suppression_warning_ratio": 0.70,
        "constraint_binding_warning_ratio": 0.25,
        **_mapping(config.get("diagnostics")),
    }


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
