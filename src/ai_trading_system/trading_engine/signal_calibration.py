from __future__ import annotations

import json
import math
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
from ai_trading_system.trading_engine.parameters import shadow_backtest as shadow_backtest_module
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    load_signal_ablation_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.signal_ablation import (
    _diagnostics_summary as _ablation_diagnostics_summary,
)
from ai_trading_system.trading_engine.signal_ablation import (
    _signal_contribution as _ablation_signal_contribution,
)
from ai_trading_system.trading_engine.signal_ablation import _summary as _ablation_summary
from ai_trading_system.trading_engine.signal_snapshots import (
    NEUTRAL_FALLBACK_SIGNALS,
    NEUTRAL_SIGNAL_VALUE,
    PRICE_DERIVED_SIGNALS,
    REQUIRED_SIGNALS,
    SIGNAL_SNAPSHOT_REPORT_TYPE,
    SIGNAL_SNAPSHOT_SCHEMA_VERSION,
    _build_neutral_signal,
    _coverage,
    _latest_price_date,
    _neutral_reason,
    _overall_quality,
    _panel_date_range,
    _prepare_price_panel,
    _read_prices,
    _value_rows,
    signal_snapshot_frames,
    signal_snapshot_summary,
    write_signal_snapshot,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    _config_hash as _snapshot_config_hash,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SIGNAL_CALIBRATION_SCHEMA_VERSION = 1
SIGNAL_CALIBRATION_REPORT_TYPE = "signal_calibration"
SIGNAL_CALIBRATION_ALIAS_REPORT_TYPE = "signal_calibration_report"
DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH = (
    PROJECT_ROOT / "config" / "signals" / "signal_calibration_profiles.yaml"
)


@dataclass(frozen=True)
class SignalCalibrationRun:
    as_of: date
    payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    recommended_profile_path: Path


def default_signal_calibration_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "signal_calibration"


def default_signal_calibration_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_signal_calibration_json_path(output_root: Path, as_of: date) -> Path:
    return default_signal_calibration_dir(output_root, as_of) / "signal_calibration_summary.json"


def default_signal_calibration_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_signal_calibration_dir(output_root, as_of) / "signal_calibration_summary.md"


def default_recommended_profile_path(output_root: Path, as_of: date) -> Path:
    return default_signal_calibration_dir(output_root, as_of) / "recommended_signal_profile.yaml"


def latest_signal_calibration_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_signal_calibration_root()
    candidates = sorted(root.glob("*/signal_calibration_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_signal_calibration_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_signal_calibration_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/signal_calibration_summary.json"):
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
        reports_dir / f"signal_calibration_{as_of.isoformat()}.json",
        reports_dir / f"signal_calibration_{as_of.isoformat()}.md",
    )


def load_signal_calibration_config(
    path: Path | str = DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"signal calibration config must be a mapping: {path}")
    _validate_signal_calibration_config(payload)
    return payload


def run_signal_calibration(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> SignalCalibrationRun:
    config = load_signal_calibration_config(config_path)
    root = _output_root(config, dry_run=dry_run)
    payload = build_signal_calibration_payload(
        as_of=as_of,
        profile_names=profile_names,
        config_path=config_path,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
    )
    resolved_as_of = signal_calibration_payload_date(
        payload,
        default_signal_calibration_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_signal_calibration_json_path(root, resolved_as_of)
    markdown_path = default_signal_calibration_markdown_path(root, resolved_as_of)
    recommended_path = default_recommended_profile_path(root, resolved_as_of)
    write_signal_calibration_report(payload, json_path, markdown_path)
    write_recommended_signal_profile(payload, recommended_path)
    return SignalCalibrationRun(
        as_of=resolved_as_of,
        payload=payload,
        json_path=json_path,
        markdown_path=markdown_path,
        recommended_profile_path=recommended_path,
    )


def build_signal_calibration_payload(
    *,
    as_of: date | None = None,
    profile_names: tuple[str, ...] | list[str] | None = None,
    config_path: Path | str = DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
    dry_run: bool = False,
    generated_at: datetime | None = None,
    output_root: Path | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_signal_calibration_config(resolved_config_path)
    shadow_config_path = resolve_project_path(
        str(config.get("shadow_backtest_config_path") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    ablation_config_path = resolve_project_path(
        str(config.get("signal_ablation_config_path") or DEFAULT_SIGNAL_ABLATION_CONFIG_PATH)
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    ablation_config = load_signal_ablation_config(ablation_config_path)
    prices_path = resolve_project_path(shadow_config.data.prices_path)
    prices = _read_prices(prices_path)
    resolved_as_of = as_of or _latest_price_date(prices) or generated.date()
    output_dir = default_signal_calibration_dir(
        output_root or _output_root(config, dry_run=dry_run),
        resolved_as_of,
    )
    baseline_path = resolve_project_path(shadow_config.baseline_parameters_path)
    profile_map = _mapping(config.get("profiles"))
    selected_profiles = _selected_profiles(profile_map, profile_names)

    try:
        baseline = load_production_parameters(baseline_path)
    except (OSError, ValueError) as exc:
        return _failure_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            ablation_config_path=ablation_config_path,
            reason=f"production baseline unavailable: {exc}",
            dry_run=dry_run,
        )

    data_quality_report_path = default_quality_report_path(
        _data_quality_report_dir(config, shadow_config, dry_run=dry_run),
        resolved_as_of,
    )
    data_quality_report = shadow_backtest_module._run_data_quality_gate(
        config=shadow_config,
        baseline=baseline,
        as_of=resolved_as_of,
        report_path=data_quality_report_path,
    )
    data_quality_status, warnings = shadow_backtest_module._shadow_data_quality_status(
        config=shadow_config,
        baseline=baseline,
        prices=prices,
        as_of=resolved_as_of,
        data_quality_report=data_quality_report,
    )
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return _blocked_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            ablation_config_path=ablation_config_path,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            selected_profiles=selected_profiles,
            warnings=list(warnings),
            dry_run=dry_run,
        )

    from ai_trading_system.trading_engine.backtesting.walk_forward import (
        generate_walk_forward_windows,
    )

    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, resolved_as_of)
    windows = generate_walk_forward_windows(trading_dates, shadow_config.walk_forward)
    full_start = shadow_backtest_module._full_result_start(windows, trading_dates, shadow_config)
    full_end = resolved_as_of
    profile_results = [
        _build_profile_result(
            profile_name=name,
            profile_config=_mapping(profile_map.get(name)),
            config=config,
            config_path=resolved_config_path,
            shadow_config=shadow_config,
            shadow_config_path=shadow_config_path,
            baseline=baseline,
            baseline_path=baseline_path,
            prices=prices,
            prices_path=prices_path,
            as_of=resolved_as_of,
            generated_at=generated,
            output_dir=output_dir,
            windows=windows,
            full_start=full_start,
            full_end=full_end,
            ablation_thresholds=ablation_config.thresholds,
            min_walk_forward_windows=ablation_config.stability.min_walk_forward_windows,
            score_delta_epsilon=ablation_config.diagnostics.score_delta_epsilon,
            portfolio_weight_delta_epsilon=(
                ablation_config.diagnostics.portfolio_weight_delta_epsilon
            ),
            non_neutral_value_epsilon=ablation_config.diagnostics.non_neutral_value_epsilon,
        )
        for name in selected_profiles
    ]
    ranking = _rank_profiles(profile_results, config)
    return {
        "schema_version": SIGNAL_CALIBRATION_SCHEMA_VERSION,
        "report_type": SIGNAL_CALIBRATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-calibration-{resolved_as_of.isoformat()}",
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
            "signal_ablation_config_path": str(ablation_config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(resolved_config_path, shadow_config_path),
        },
        "input_artifacts": {
            "signal_calibration_profiles": str(resolved_config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "signal_ablation_config": str(ablation_config_path),
            "baseline_parameters": str(baseline_path),
            "prices": str(prices_path),
            "data_quality_report": str(data_quality_report_path),
        },
        "data_quality": {
            "status": data_quality_status,
            "validate_data_status": data_quality_report.status,
            "quality_report_path": str(data_quality_report_path),
            "backtest_mode": "full_signal_backtest_limited",
            "can_promote_candidate": False,
            "error_count": data_quality_report.error_count,
            "warning_count": data_quality_report.warning_count,
        },
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": _mapping(config.get("diagnostics")),
            "normalization_policy": _mapping(config.get("normalization_policy")),
            "ranking": _mapping(config.get("ranking")),
            "promotion_effect": "manual_review_only",
        },
        "profiles": profile_results,
        "ranking": ranking,
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Signal snapshot quality is still LIMITED and fallback signals remain present; "
                "calibration is manual-review-only."
            ),
        },
        "warnings": list(dict.fromkeys(warnings)),
        "safety": _safety_payload(),
    }


def build_calibrated_signal_snapshot_payload(
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    config_path: Path,
    shadow_config: Any,
    baseline: Any,
    prices: pd.DataFrame,
    prices_path: Path,
    as_of: date,
    generated_at: datetime,
    calibration_config: dict[str, Any],
) -> dict[str, Any]:
    assets = tuple(asset for asset in baseline.flattened_asset_universe() if asset != "CASH")
    panel = _prepare_price_panel(prices, assets, as_of)
    signals: dict[str, Any] = {
        "trend_momentum": build_calibrated_trend_momentum_signal(
            panel,
            assets,
            profile_name=profile_name,
            profile_config=_mapping(profile_config.get("trend_momentum")),
            calibration_config=calibration_config,
        ),
        "sector_strength": build_calibrated_sector_strength_signal(
            panel,
            assets,
            profile_name=profile_name,
            profile_config=_mapping(profile_config.get("sector_strength")),
            calibration_config=calibration_config,
        ),
        "macro_liquidity": _build_neutral_signal(
            panel,
            assets,
            status="LIMITED",
            quality="proxy_or_neutral",
            method="neutral_fallback_v0_1",
            reason="Reliable PIT macro-liquidity dataset is not available in v0.1.",
        ),
    }
    for signal in NEUTRAL_FALLBACK_SIGNALS:
        signals[signal] = _build_neutral_signal(
            panel,
            assets,
            status="NEUTRAL_FALLBACK",
            quality="neutral_fallback",
            method="neutral_fallback_v0_1",
            reason=_neutral_reason(signal),
        )
    overall = _overall_quality(signals)
    data_start, data_end = _panel_date_range(panel, as_of)
    return {
        "schema_version": SIGNAL_SNAPSHOT_SCHEMA_VERSION,
        "report_type": SIGNAL_SNAPSHOT_REPORT_TYPE,
        "metadata": {
            "snapshot_id": f"signal-calibration-{profile_name}-{as_of.isoformat()}",
            "as_of": as_of.isoformat(),
            "generated_at": generated_at.isoformat(),
            "status": overall["status"],
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "price_derived_only": False,
            "calibration_profile": profile_name,
            "calibration_profile_description": profile_config.get("description", ""),
            "market_regime": shadow_config.market_regime.id,
            "market_regime_anchor": shadow_config.market_regime.anchor_date.isoformat(),
            "market_regime_anchor_event": shadow_config.market_regime.anchor_event,
            "data_range": {"start": data_start.isoformat(), "end": data_end.isoformat()},
            "asset_universe": list(assets),
            "required_signals": list(REQUIRED_SIGNALS),
            "config_path": str(config_path),
            "config_hash": _snapshot_config_hash(config_path),
            "price_data_path": str(prices_path),
            "code_version": git_commit_sha() or "unknown",
        },
        "signals": signals,
        "overall_quality": overall,
    }


def build_calibrated_trend_momentum_signal(
    panel: pd.DataFrame,
    assets: tuple[str, ...],
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    calibration_config: dict[str, Any],
) -> dict[str, Any]:
    if panel.empty:
        return _failed_signal("price data unavailable for calibrated trend_momentum")
    normalization = str(profile_config.get("normalization") or "sigmoid_zscore")
    windows = _int_sequence(profile_config.get("windows"))
    ma_pairs = _ma_pairs(profile_config.get("ma_pairs"))
    features: dict[str, pd.DataFrame] = {}
    scores: list[pd.DataFrame] = []
    for window in windows:
        feature = panel.pct_change(window)
        name = f"return_{window}d"
        features[name] = feature
        scores.append(_normalize_profile_feature(feature, normalization, calibration_config))
    for fast, slow in ma_pairs:
        fast_ma = panel.rolling(fast, min_periods=min(5, fast)).mean()
        slow_ma = panel.rolling(slow, min_periods=min(20, slow)).mean()
        cross_name = f"ma_{fast}_vs_{slow}"
        price_name = f"price_vs_{slow}ma"
        features[cross_name] = fast_ma.div(slow_ma).sub(1.0)
        features[price_name] = panel.div(slow_ma).sub(1.0)
        scores.append(
            _normalize_profile_feature(features[cross_name], normalization, calibration_config)
        )
        scores.append(
            _normalize_profile_feature(features[price_name], normalization, calibration_config)
        )
    score = _average_score_frames(scores, panel)
    score = _apply_signal_clipping(score, _mapping(profile_config.get("clipping")))
    score = score.reindex(columns=list(assets)).fillna(NEUTRAL_SIGNAL_VALUE)
    values = _value_rows(score, features)
    return {
        "status": "OK",
        "quality": "price_derived",
        "method": f"calibrated_profile:{profile_name}:trend_momentum:{normalization}",
        "coverage": _coverage(values, panel, assets),
        "profile": profile_name,
        "formula": {
            "windows": windows,
            "ma_pairs": [list(pair) for pair in ma_pairs],
            "normalization": normalization,
            "clipping": _mapping(profile_config.get("clipping")),
        },
        "values": values,
    }


def build_calibrated_sector_strength_signal(
    panel: pd.DataFrame,
    assets: tuple[str, ...],
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    calibration_config: dict[str, Any],
) -> dict[str, Any]:
    if panel.empty:
        return _failed_signal("price data unavailable for calibrated sector_strength")
    benchmark_name = str(profile_config.get("benchmark") or "QQQ")
    benchmark = panel[benchmark_name] if benchmark_name in panel.columns else panel.mean(axis=1)
    normalization = str(profile_config.get("normalization") or "sigmoid_zscore")
    windows = _int_sequence(profile_config.get("windows"))
    relative_weight = _float_value(profile_config.get("relative_weight"), default=1.0)
    features: dict[str, pd.DataFrame] = {}
    scores: list[pd.DataFrame] = []
    for window in windows:
        relative = panel.pct_change(window).sub(benchmark.pct_change(window), axis=0)
        name = f"relative_return_{window}d_vs_{benchmark_name}"
        features[name] = relative
        scores.append(_normalize_profile_feature(relative, normalization, calibration_config))
    score = _average_score_frames(scores, panel)
    if not math.isclose(relative_weight, 1.0):
        score = NEUTRAL_SIGNAL_VALUE + (score - NEUTRAL_SIGNAL_VALUE) * relative_weight
    score = score.clip(lower=0.0, upper=1.0)
    score = score.reindex(columns=list(assets)).fillna(NEUTRAL_SIGNAL_VALUE)
    values = _value_rows(score, features)
    return {
        "status": "OK",
        "quality": "price_derived",
        "method": f"calibrated_profile:{profile_name}:sector_strength:{normalization}",
        "coverage": _coverage(values, panel, assets),
        "profile": profile_name,
        "formula": {
            "benchmark": benchmark_name,
            "windows": windows,
            "normalization": normalization,
            "relative_weight": relative_weight,
        },
        "values": values,
    }


def signal_distribution_diagnostics(
    signal_frames: dict[str, pd.DataFrame],
    diagnostics_config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    neutral_band = _mapping(diagnostics_config.get("neutral_band"))
    extreme_band = _mapping(diagnostics_config.get("extreme_band"))
    neutral_lower = _float_value(neutral_band.get("lower"), default=0.45)
    neutral_upper = _float_value(neutral_band.get("upper"), default=0.55)
    extreme_lower = _float_value(extreme_band.get("lower"), default=0.20)
    extreme_upper = _float_value(extreme_band.get("upper"), default=0.80)
    warning_ratio = _float_value(
        diagnostics_config.get("neutral_compression_warning_ratio"),
        default=0.50,
    )
    diagnostics: dict[str, dict[str, Any]] = {}
    for signal in PRICE_DERIVED_SIGNALS:
        values = _signal_values(signal_frames.get(signal))
        if values.empty:
            diagnostics[signal] = {
                "mean": 0.0,
                "std": 0.0,
                "p05": 0.0,
                "p25": 0.0,
                "p50": 0.0,
                "p75": 0.0,
                "p95": 0.0,
                "neutral_ratio_0_45_to_0_55": 0.0,
                "extreme_ratio_below_0_2_or_above_0_8": 0.0,
                "warning": "Signal has no numeric values.",
            }
            continue
        neutral_ratio = float(((values >= neutral_lower) & (values <= neutral_upper)).mean())
        extreme_ratio = float(((values < extreme_lower) | (values > extreme_upper)).mean())
        item = {
            "mean": _round_float(values.mean()),
            "std": _round_float(values.std(ddof=0)),
            "p05": _round_float(values.quantile(0.05)),
            "p25": _round_float(values.quantile(0.25)),
            "p50": _round_float(values.quantile(0.50)),
            "p75": _round_float(values.quantile(0.75)),
            "p95": _round_float(values.quantile(0.95)),
            "neutral_ratio_0_45_to_0_55": round(neutral_ratio, 6),
            "extreme_ratio_below_0_2_or_above_0_8": round(extreme_ratio, 6),
        }
        if neutral_ratio >= warning_ratio:
            item["warning"] = (
                "Signal is over-compressed around neutral value and may not materially "
                "affect portfolio construction."
            )
        diagnostics[signal] = item
    return diagnostics


def signal_correlation_diagnostics(
    signal_frames: dict[str, pd.DataFrame],
    diagnostics_config: dict[str, Any],
) -> dict[str, Any]:
    thresholds = _mapping(diagnostics_config.get("correlation_thresholds"))
    high = _float_value(thresholds.get("high_correlation"), default=0.75)
    very_high = _float_value(thresholds.get("very_high_correlation"), default=0.90)
    trend = _signal_values_by_asset_day(signal_frames.get("trend_momentum"), "trend_momentum")
    sector = _signal_values_by_asset_day(signal_frames.get("sector_strength"), "sector_strength")
    joined = trend.join(sector, how="inner").dropna()
    if len(joined.index) < 2:
        return {
            "trend_momentum_vs_sector_strength": 0.0,
            "warning": "Insufficient overlapping signal observations for correlation.",
        }
    correlation = joined["trend_momentum"].corr(joined["sector_strength"])
    if pd.isna(correlation):
        correlation = 0.0
    result: dict[str, Any] = {
        "trend_momentum_vs_sector_strength": round(float(correlation), 6)
    }
    abs_correlation = abs(float(correlation))
    if abs_correlation >= very_high:
        result["warning"] = "Very high correlation may make trend and sector signals redundant."
    elif abs_correlation >= high:
        result["warning"] = "High correlation may reduce independent contribution."
    return result


def write_signal_calibration_report(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_signal_calibration_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_signal_calibration_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": SIGNAL_CALIBRATION_ALIAS_REPORT_TYPE,
        "source_report_type": SIGNAL_CALIBRATION_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_signal_calibration_report(alias_payload, json_path, markdown_path)


def write_recommended_signal_profile(payload: dict[str, Any], output_path: Path) -> Path:
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
        "version": "recommended_signal_profile_v0_1",
        "profile_name": best_profile,
        "source_signal_calibration": _mapping(payload.get("metadata")).get("run_id"),
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "profile_config": _mapping(profile.get("profile_config")),
        "ranking_metrics": _mapping(profile.get("ranking_metrics")),
        "reason": ranking.get("reason", ""),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(recommended, sort_keys=False), encoding="utf-8")
    return output_path


def render_signal_calibration_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    ranking = _mapping(payload.get("ranking"))
    promotion = _mapping(payload.get("promotion_impact"))
    profiles = _records(payload.get("profiles"))
    lines = [
        "# Signal Calibration Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- backtest_mode: `{metadata.get('backtest_mode', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- best_profile: `{ranking.get('best_profile', 'UNKNOWN')}`",
        f"- reason: {ranking.get('reason', '')}",
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
            "## 3. Profile Ranking",
            "",
            (
                "| Rank | Profile | Score | Positive Signals | Promotion Credit | "
                "Neutral Ratio | Correlation Penalty |"
            ),
            "|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in _records(ranking.get("profiles")):
        metrics = _mapping(row.get("ranking_metrics"))
        lines.append(
            "| "
            f"{row.get('rank', '')} | "
            f"`{row.get('profile_name', '')}` | "
            f"{_format_metric(row.get('ranking_score'))} | "
            f"{metrics.get('ablation_positive_signal_count', 0)} | "
            f"{metrics.get('promotion_credit_signal_count', 0)} | "
            f"{_format_metric(metrics.get('signal_neutral_ratio'))} | "
            f"{_format_metric(metrics.get('signal_correlation_penalty'))} |"
        )
    lines.extend(["", "## 4. Signal Distribution Diagnostics", ""])
    for profile in profiles:
        lines.extend([f"### `{profile.get('profile_name', '')}`", ""])
        lines.extend(
            [
                "| Signal | Mean | Std | P50 | Neutral Ratio | Extreme Ratio | Warning |",
                "|---|---:|---:|---:|---:|---:|---|",
            ]
        )
        for signal, item in _mapping(profile.get("signal_distribution")).items():
            item_map = _mapping(item)
            lines.append(
                "| "
                f"`{signal}` | "
                f"{_format_metric(item_map.get('mean'))} | "
                f"{_format_metric(item_map.get('std'))} | "
                f"{_format_metric(item_map.get('p50'))} | "
                f"{_format_metric(item_map.get('neutral_ratio_0_45_to_0_55'))} | "
                f"{_format_metric(item_map.get('extreme_ratio_below_0_2_or_above_0_8'))} | "
                f"{item_map.get('warning', '')} |"
            )
        lines.append("")
    lines.extend(["## 5. Signal Correlation Diagnostics", ""])
    for profile in profiles:
        correlation = _mapping(profile.get("signal_correlation"))
        lines.append(
            "- "
            f"`{profile.get('profile_name', '')}`: "
            f"trend_momentum_vs_sector_strength="
            f"`{_format_metric(correlation.get('trend_momentum_vs_sector_strength'))}`; "
            f"{correlation.get('warning', '')}"
        )
    lines.extend(
        [
            "",
            "## 6. Backtest Metrics by Profile",
            "",
            "| Profile | Annualized Return | Max Drawdown | Sharpe | Turnover |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for profile in profiles:
        metrics = _mapping(profile.get("metrics"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(metrics.get('annualized_return'))} | "
            f"{_format_metric(metrics.get('max_drawdown'))} | "
            f"{_format_metric(metrics.get('sharpe_ratio'))} | "
            f"{_format_metric(metrics.get('turnover'))} |"
        )
    lines.extend(
        [
            "",
            "## 7. Ablation Results by Profile",
            "",
            "| Profile | Positive | Promotion Credit | Reason |",
            "|---|---:|---:|---|",
        ]
    )
    for profile in profiles:
        ablation = _mapping(profile.get("ablation"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{ablation.get('positive_signals', 0)} | "
            f"{ablation.get('promotion_credit_signals', 0)} | "
            f"{ablation.get('reason', '')} |"
        )
    lines.extend(
        [
            "",
            "## 8. Portfolio Impact",
            "",
            "| Profile | Mean Abs Score Delta | Mean Abs Weight Delta |",
            "|---|---:|---:|",
        ]
    )
    for profile in profiles:
        metrics = _mapping(profile.get("ranking_metrics"))
        lines.append(
            "| "
            f"`{profile.get('profile_name', '')}` | "
            f"{_format_metric(metrics.get('mean_abs_score_delta'))} | "
            f"{_format_metric(metrics.get('mean_abs_portfolio_weight_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 9. Recommended Calibrated Profile",
            "",
            f"- best_profile: `{ranking.get('best_profile', 'UNKNOWN')}`",
            f"- reason: {ranking.get('reason', '')}",
            "",
            "## 10. Promotion Eligibility Impact",
            "",
            (
                "- can_support_candidate_promotion: "
                f"`{promotion.get('can_support_candidate_promotion', False)}`"
            ),
            f"- reason: {promotion.get('reason', '')}",
            "",
            "## 11. Manual Review Checklist",
            "",
            "- Review distribution compression before adopting a profile.",
            "- Review trend/sector correlation before treating both as independent evidence.",
            "- Keep candidate promotion disabled while fallback signals remain present.",
            (
                "- If score and portfolio change but contribution stays below threshold, "
                "open TRADING-053."
            ),
            "",
            "## 12. Input / Output Artifacts",
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


def load_signal_calibration_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_signal_calibration_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != SIGNAL_CALIBRATION_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != SIGNAL_CALIBRATION_REPORT_TYPE:
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
            "metrics",
            "ablation",
            "signal_distribution",
            "signal_correlation",
            "ranking_metrics",
        ):
            if not isinstance(profile.get(key), dict):
                issues.append(f"{name} missing {key}")
    ranking = _mapping(payload.get("ranking"))
    if payload.get("profiles") and not ranking.get("best_profile"):
        issues.append("ranking.best_profile must be present")
    promotion = _mapping(payload.get("promotion_impact"))
    if promotion.get("can_support_candidate_promotion") is not False:
        issues.append("calibration must not support candidate promotion")
    safety = _mapping(payload.get("safety"))
    if safety.get("production_parameters_modified") not in {False, None}:
        issues.append("production_parameters_modified must be false")
    if safety.get("candidate_promotion_triggered") not in {False, None}:
        issues.append("candidate_promotion_triggered must be false")
    return issues


def signal_calibration_payload_date(payload: dict[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("signal-calibration-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer signal calibration date from {source_path}") from exc


def _build_profile_result(
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    config: dict[str, Any],
    config_path: Path,
    shadow_config: Any,
    shadow_config_path: Path,
    baseline: Any,
    baseline_path: Path,
    prices: pd.DataFrame,
    prices_path: Path,
    as_of: date,
    generated_at: datetime,
    output_dir: Path,
    windows: tuple[Any, ...],
    full_start: date,
    full_end: date,
    ablation_thresholds: Any,
    min_walk_forward_windows: int,
    score_delta_epsilon: float,
    portfolio_weight_delta_epsilon: float,
    non_neutral_value_epsilon: float,
) -> dict[str, Any]:
    from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
        simulate_parameter_portfolio,
    )

    snapshot_payload = build_calibrated_signal_snapshot_payload(
        profile_name=profile_name,
        profile_config=profile_config,
        config_path=config_path,
        shadow_config=shadow_config,
        baseline=baseline,
        prices=prices,
        prices_path=prices_path,
        as_of=as_of,
        generated_at=generated_at,
        calibration_config=config,
    )
    profile_dir = output_dir / "profiles" / profile_name
    snapshot_json = profile_dir / "signal_snapshot.json"
    snapshot_md = profile_dir / "signal_snapshot.md"
    write_signal_snapshot(snapshot_payload, snapshot_json, snapshot_md)
    signal_frames = signal_snapshot_frames(snapshot_payload)
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
        _ablation_signal_contribution(
            signal=signal,
            baseline=baseline,
            shadow_config=shadow_config,
            prices=prices,
            signal_frames=signal_frames,
            signal_snapshot_payload=snapshot_payload,
            baseline_result=baseline_result,
            baseline_by_window=baseline_by_window,
            windows=windows,
            full_start=full_start,
            full_end=full_end,
            thresholds=ablation_thresholds,
            min_walk_forward_windows=min_walk_forward_windows,
            score_delta_epsilon=score_delta_epsilon,
            portfolio_weight_delta_epsilon=portfolio_weight_delta_epsilon,
            non_neutral_value_epsilon=non_neutral_value_epsilon,
        )
        for signal in PRICE_DERIVED_SIGNALS
    ]
    diagnostics = _ablation_diagnostics_summary(contributions)
    snapshot_summary = signal_snapshot_summary(snapshot_payload)
    ablation_summary = _ablation_summary(contributions, snapshot_summary, diagnostics)
    distribution = signal_distribution_diagnostics(
        signal_frames,
        _mapping(config.get("diagnostics")),
    )
    correlation = signal_correlation_diagnostics(
        signal_frames,
        _mapping(config.get("diagnostics")),
    )
    ranking_metrics = _ranking_metrics(
        baseline_result=baseline_result,
        contributions=contributions,
        distribution=distribution,
        correlation=correlation,
        config=config,
    )
    metrics = baseline_result.metrics.to_dict()
    positive_signals = _strings(ablation_summary.get("positive_signals"))
    promotion_credit_signals = _strings(ablation_summary.get("promotion_credit_signals"))
    return {
        "profile_name": profile_name,
        "description": profile_config.get("description", ""),
        "profile_config": profile_config,
        "status": "LIMITED",
        "backtest_mode": "full_signal_backtest_limited",
        "metrics": {
            "annualized_return": metrics.get("annualized_return", 0.0),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "sharpe_ratio": metrics.get("sharpe_ratio", 0.0),
            "turnover": metrics.get("turnover", 0.0),
        },
        "ablation": {
            "positive_signals": len(positive_signals),
            "positive_signal_names": positive_signals,
            "promotion_credit_signals": len(promotion_credit_signals),
            "promotion_credit_signal_names": promotion_credit_signals,
            "reason": ablation_summary.get("no_promotion_credit_reason")
            or ablation_summary.get("reason", ""),
            "diagnostics": diagnostics,
            "contributions": contributions,
        },
        "signal_distribution": distribution,
        "signal_correlation": correlation,
        "portfolio_impact": _portfolio_impact_summary(contributions),
        "ranking_metrics": ranking_metrics,
        "artifacts": {
            "calibrated_signal_snapshot": str(snapshot_json),
            "calibrated_signal_snapshot_md": str(snapshot_md),
        },
        "input_artifacts": {
            "baseline_parameters": str(baseline_path),
            "shadow_backtest_config": str(shadow_config_path),
            "prices": str(prices_path),
        },
    }


def _normalize_profile_feature(
    frame: pd.DataFrame,
    normalization: str,
    calibration_config: dict[str, Any],
) -> pd.DataFrame:
    policy = _mapping(calibration_config.get("normalization_policy"))
    if normalization == "minmax_rolling":
        return _normalize_minmax_rolling(frame, policy)
    if normalization == "zscore_to_unit":
        zscore = _rolling_zscore(frame, policy)
        clip = _float_value(policy.get("zscore_clip"), default=3.0)
        return (NEUTRAL_SIGNAL_VALUE + zscore.clip(-clip, clip) / (2.0 * clip)).clip(0.0, 1.0)
    if normalization == "sigmoid_zscore":
        zscore = _rolling_zscore(frame, policy)
        scale = _float_value(policy.get("zscore_scale"), default=1.0)
        return pd.DataFrame(
            1.0 / (1.0 + np.exp(-(zscore / scale))),
            index=frame.index,
            columns=frame.columns,
        ).clip(0.0, 1.0)
    raise ValueError(f"unsupported signal normalization: {normalization}")


def _normalize_minmax_rolling(frame: pd.DataFrame, policy: dict[str, Any]) -> pd.DataFrame:
    window = _int_value(policy.get("rolling_window"), default=252)
    min_periods = min(_int_value(policy.get("min_periods"), default=20), window)
    epsilon = _float_value(policy.get("minmax_epsilon"), default=1e-9)
    rolling_min = frame.rolling(window, min_periods=min_periods).min()
    rolling_max = frame.rolling(window, min_periods=min_periods).max()
    span = (rolling_max - rolling_min).where(lambda value: value.abs() > epsilon)
    normalized = (frame - rolling_min).div(span)
    return normalized.clip(0.0, 1.0).fillna(NEUTRAL_SIGNAL_VALUE)


def _rolling_zscore(frame: pd.DataFrame, policy: dict[str, Any]) -> pd.DataFrame:
    window = _int_value(policy.get("rolling_window"), default=252)
    min_periods = min(_int_value(policy.get("min_periods"), default=20), window)
    mean = frame.rolling(window, min_periods=min_periods).mean()
    std = frame.rolling(window, min_periods=min_periods).std(ddof=0).replace(0.0, np.nan)
    return frame.sub(mean).div(std).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _average_score_frames(scores: list[pd.DataFrame], panel: pd.DataFrame) -> pd.DataFrame:
    if not scores:
        return pd.DataFrame(NEUTRAL_SIGNAL_VALUE, index=panel.index, columns=panel.columns)
    total = scores[0].copy()
    for score in scores[1:]:
        total = total.add(score, fill_value=NEUTRAL_SIGNAL_VALUE)
    return (total / len(scores)).clip(0.0, 1.0)


def _apply_signal_clipping(score: pd.DataFrame, clipping: dict[str, Any]) -> pd.DataFrame:
    if not clipping:
        return score.clip(0.0, 1.0)
    lower = _float_value(clipping.get("lower"), default=0.0)
    upper = _float_value(clipping.get("upper"), default=1.0)
    return score.clip(lower=max(0.0, lower), upper=min(1.0, upper))


def _ranking_metrics(
    *,
    baseline_result: Any,
    contributions: list[dict[str, Any]],
    distribution: dict[str, dict[str, Any]],
    correlation: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    metrics = baseline_result.metrics.to_dict()
    positive_count = sum(
        1 for item in contributions if item.get("contribution_class") == "positive"
    )
    promotion_credit_count = sum(
        1 for item in contributions if item.get("promotion_credit_allowed") is True
    )
    score_delta = _mean(
        [
            _float_value(_mapping(item.get("score_impact")).get("mean_abs_score_delta"))
            for item in contributions
        ]
    )
    weight_delta = _mean(
        [
            _float_value(_mapping(item.get("portfolio_impact")).get("mean_abs_weight_delta"))
            for item in contributions
        ]
    )
    stability = _mean(
        [
            _float_value(_mapping(item.get("window_stability")).get("stability_ratio"))
            for item in contributions
        ]
    )
    neutral_ratio = _mean(
        [
            _float_value(item.get("neutral_ratio_0_45_to_0_55"))
            for item in distribution.values()
        ]
    )
    correlation_value = abs(
        _float_value(correlation.get("trend_momentum_vs_sector_strength"), default=0.0)
    )
    high = _float_value(
        _mapping(_mapping(config.get("diagnostics")).get("correlation_thresholds")).get(
            "high_correlation"
        ),
        default=0.75,
    )
    correlation_penalty = correlation_value if correlation_value >= high else 0.0
    return {
        "ablation_positive_signal_count": positive_count,
        "promotion_credit_signal_count": promotion_credit_count,
        "mean_abs_score_delta": score_delta,
        "mean_abs_portfolio_weight_delta": weight_delta,
        "annualized_return": _round_float(metrics.get("annualized_return")),
        "max_drawdown": _round_float(metrics.get("max_drawdown")),
        "sharpe_ratio": _round_float(metrics.get("sharpe_ratio")),
        "turnover": _round_float(metrics.get("turnover")),
        "walk_forward_stability_ratio": stability,
        "signal_neutral_ratio": neutral_ratio,
        "signal_correlation_penalty": round(correlation_penalty, 6),
    }


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
        weight = _float_value(raw_weight)
        if key == "max_drawdown_abs":
            value = abs(_float_value(metrics.get("max_drawdown")))
        else:
            value = _float_value(metrics.get(key))
        score += weight * value
    return round(score, 12)


def _ranking_reason(profile: dict[str, Any]) -> str:
    if not profile:
        return "No profile could be ranked."
    metrics = _mapping(profile.get("ranking_metrics"))
    positive = _int_value(metrics.get("ablation_positive_signal_count"), default=0)
    promotion_credit = _int_value(metrics.get("promotion_credit_signal_count"), default=0)
    neutral = _float_value(metrics.get("signal_neutral_ratio"), default=0.0)
    if positive > 0:
        return (
            "Best profile improved real signal contribution diagnostics, but signal quality "
            "remains LIMITED and candidate promotion stays disabled."
        )
    if neutral < 0.50 or promotion_credit > 0:
        return (
            "Best profile reduced neutral compression or improved score/portfolio impact, "
            "but signal quality remains LIMITED."
        )
    return (
        "Calibration did not find a profile with material contribution above threshold; "
        "trend/sector feature design or portfolio sensitivity may need follow-up."
    )


def _portfolio_impact_summary(contributions: list[dict[str, Any]]) -> dict[str, Any]:
    impacts = [_mapping(item.get("portfolio_impact")) for item in contributions]
    return {
        "mean_abs_weight_delta": _mean(
            [_float_value(item.get("mean_abs_weight_delta")) for item in impacts]
        ),
        "max_abs_weight_delta": max(
            [_float_value(item.get("max_abs_weight_delta")) for item in impacts] or [0.0]
        ),
        "rebalance_days_changed": sum(
            _int_value(item.get("rebalance_days_changed")) for item in impacts
        ),
    }


def _signal_values(frame: pd.DataFrame | None) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype=float)
    values = pd.to_numeric(frame.stack(), errors="coerce").dropna()
    return values.clip(0.0, 1.0)


def _signal_values_by_asset_day(frame: pd.DataFrame | None, name: str) -> pd.DataFrame:
    values = _signal_values(frame)
    if values.empty:
        return pd.DataFrame(columns=[name])
    values.index = pd.MultiIndex.from_tuples(
        [(str(index[0]), str(index[1])) for index in values.index],
        names=["date", "asset"],
    )
    return values.rename(name).to_frame()


def _selected_profiles(
    profile_map: dict[str, Any],
    profile_names: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    if profile_names is None:
        return tuple(profile_map)
    selected = tuple(dict.fromkeys(str(name) for name in profile_names if str(name)))
    unknown = [name for name in selected if name not in profile_map]
    if unknown:
        raise ValueError("unknown signal calibration profile(s): " + ", ".join(unknown))
    if not selected:
        raise ValueError("at least one signal calibration profile is required")
    return selected


def _validate_signal_calibration_config(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != "none":
        raise ValueError("signal calibration config production_effect must be none")
    if payload.get("manual_review_required") is not True:
        raise ValueError("signal calibration config manual_review_required must be true")
    if payload.get("auto_promotion") is not False:
        raise ValueError("signal calibration config auto_promotion must be false")
    profiles = _mapping(payload.get("profiles"))
    if not profiles:
        raise ValueError("signal calibration config must define profiles")
    diagnostics = _mapping(payload.get("diagnostics"))
    thresholds = _mapping(diagnostics.get("correlation_thresholds"))
    if "high_correlation" not in thresholds or "very_high_correlation" not in thresholds:
        raise ValueError("signal calibration config missing correlation thresholds")
    for name, profile in profiles.items():
        profile_map = _mapping(profile)
        if not _mapping(profile_map.get("trend_momentum")):
            raise ValueError(f"{name} missing trend_momentum profile")
        if not _mapping(profile_map.get("sector_strength")):
            raise ValueError(f"{name} missing sector_strength profile")


def _int_sequence(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(int(item) for item in value if int(item) > 0)


def _ma_pairs(value: Any) -> tuple[tuple[int, int], ...]:
    pairs: list[tuple[int, int]] = []
    if not isinstance(value, list | tuple):
        return tuple()
    for item in value:
        if not isinstance(item, list | tuple) or len(item) != 2:
            continue
        fast = int(item[0])
        slow = int(item[1])
        if fast > 0 and slow > 0:
            pairs.append((fast, slow))
    return tuple(pairs)


def _failure_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: dict[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    ablation_config_path: Path,
    reason: str,
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SIGNAL_CALIBRATION_SCHEMA_VERSION,
        "report_type": SIGNAL_CALIBRATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-calibration-{as_of.isoformat()}",
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
            "signal_ablation_config_path": str(ablation_config_path),
        },
        "input_artifacts": {
            "signal_calibration_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "signal_ablation_config": str(ablation_config_path),
        },
        "data_quality": {"status": "FAILED"},
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": _mapping(config.get("diagnostics")),
        },
        "profiles": [],
        "ranking": {"best_profile": "", "reason": reason, "profiles": []},
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
    ablation_config_path: Path,
    data_quality_status: str,
    data_quality_report_path: Path,
    selected_profiles: tuple[str, ...],
    warnings: list[str],
    dry_run: bool,
) -> dict[str, Any]:
    reason = "Signal calibration was not run because required input quality is blocked."
    return {
        "schema_version": SIGNAL_CALIBRATION_SCHEMA_VERSION,
        "report_type": SIGNAL_CALIBRATION_REPORT_TYPE,
        "metadata": {
            "run_id": f"signal-calibration-{as_of.isoformat()}",
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
            "signal_ablation_config_path": str(ablation_config_path),
        },
        "input_artifacts": {
            "signal_calibration_profiles": str(config_path),
            "shadow_backtest_config": str(shadow_config_path),
            "signal_ablation_config": str(ablation_config_path),
            "data_quality_report": str(data_quality_report_path),
        },
        "data_quality": {
            "status": data_quality_status,
            "quality_report_path": str(data_quality_report_path),
            "can_promote_candidate": False,
        },
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "diagnostics": _mapping(config.get("diagnostics")),
        },
        "profiles": [],
        "ranking": {"best_profile": "", "reason": reason, "profiles": []},
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": reason,
        },
        "warnings": list(dict.fromkeys([*warnings, reason])),
        "safety": _safety_payload(),
    }


def _failed_signal(reason: str) -> dict[str, Any]:
    return {
        "status": "FAILED",
        "quality": "missing",
        "method": "not_built",
        "coverage": 0.0,
        "reason": reason,
        "values": [],
    }


def _output_root(config: dict[str, Any], *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "signal_calibration"
    output = _mapping(config.get("output"))
    return resolve_project_path(
        str(output.get("signal_calibration_dir") or default_signal_calibration_root())
    )


def _data_quality_report_dir(config: dict[str, Any], shadow_config: Any, *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "signal_calibration" / "reports"
    return resolve_project_path(str(shadow_config.data.data_quality_report_dir))


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


def _mean(values: list[float]) -> float:
    return 0.0 if not values else round(sum(values) / len(values), 6)


def _format_metric(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"
