from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.backtesting.metrics import calculate_portfolio_metrics
from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
    DEFENSIVE_ASSETS,
    _feature_frame,
    _prepare_price_panel,
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
    DEFAULT_PRODUCTION_PARAMETERS_PATH,
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.parameter_schema import WalkForwardConfig
from ai_trading_system.trading_engine.portfolio_candidates import (
    DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    load_portfolio_candidate_config,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    SensitivitySimulation,
    _actual_weights_from_target,
    _backtest_artifact_root,
    _composite_scores,
    _manifest_date_from_data_gate,
    _manifest_path_from_data_gate,
    _signal_snapshot_path,
    _simulate_sensitivity_profile,
    _target_weights_from_scores,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    signal_snapshot_frames,
    signal_snapshot_summary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

WEIGHT_TUNING_SCHEMA_VERSION = 1
WEIGHT_TUNING_REPORT_TYPE = "weight_tuning"
WEIGHT_TUNING_ALIAS_REPORT_TYPE = "weight_tuning_report"
DEFAULT_WEIGHT_TUNING_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "parameters" / "weight_tuning_v0_1.yaml"
)


@dataclass(frozen=True)
class WeightTuningRun:
    as_of: date
    payload: dict[str, Any]
    candidates_payload: dict[str, Any]
    json_path: Path
    markdown_path: Path
    recommended_weights_path: Path
    candidates_path: Path


@dataclass(frozen=True)
class _SimulationContext:
    profile_name: str
    profile_config: dict[str, Any]
    baseline: Any
    shadow_config: Any
    returns: pd.DataFrame
    features: dict[str, pd.DataFrame]
    tradable_assets: list[str]
    defensive_assets: list[str]


def default_weight_tuning_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "weight_tuning"


def default_weight_tuning_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_weight_tuning_json_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_dir(output_root, as_of) / "weight_tuning_summary.json"


def default_weight_tuning_markdown_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_dir(output_root, as_of) / "weight_tuning_summary.md"


def default_recommended_shadow_weights_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_dir(output_root, as_of) / "recommended_shadow_weights.yaml"


def default_weight_tuning_candidates_path(output_root: Path, as_of: date) -> Path:
    return default_weight_tuning_dir(output_root, as_of) / "weight_tuning_candidates.json"


def latest_weight_tuning_path(output_root: Path | None = None) -> Path | None:
    root = output_root or default_weight_tuning_root()
    candidates = sorted(root.glob("*/weight_tuning_summary.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_weight_tuning_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_weight_tuning_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_tuning_summary.json"):
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
        reports_dir / f"weight_tuning_{as_of.isoformat()}.json",
        reports_dir / f"weight_tuning_{as_of.isoformat()}.md",
    )


def load_weight_tuning_config(
    path: Path | str = DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"weight tuning config must be a mapping: {path}")
    _validate_weight_tuning_config(payload)
    return payload


def run_weight_tuning(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
    portfolio_profile: str | None = None,
    signals: Sequence[str] | None = None,
    dry_run: bool = False,
    generated_at: datetime | None = None,
) -> WeightTuningRun:
    config = load_weight_tuning_config(config_path)
    root = _output_root(config, dry_run=dry_run)
    payload, candidates_payload = build_weight_tuning_payload(
        as_of=as_of,
        config_path=config_path,
        portfolio_profile=portfolio_profile,
        signals=signals,
        dry_run=dry_run,
        generated_at=generated_at,
        output_root=root,
    )
    resolved_as_of = weight_tuning_payload_date(
        payload,
        default_weight_tuning_json_path(root, datetime.now(tz=UTC).date()),
    )
    json_path = default_weight_tuning_json_path(root, resolved_as_of)
    markdown_path = default_weight_tuning_markdown_path(root, resolved_as_of)
    recommended_path = default_recommended_shadow_weights_path(root, resolved_as_of)
    candidates_path = default_weight_tuning_candidates_path(root, resolved_as_of)
    write_weight_tuning_summary(payload, json_path, markdown_path)
    write_weight_tuning_candidates(candidates_payload, candidates_path)
    write_recommended_shadow_weights(payload, recommended_path)
    return WeightTuningRun(
        as_of=resolved_as_of,
        payload=payload,
        candidates_payload=candidates_payload,
        json_path=json_path,
        markdown_path=markdown_path,
        recommended_weights_path=recommended_path,
        candidates_path=candidates_path,
    )


def build_weight_tuning_payload(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
    portfolio_profile: str | None = None,
    signals: Sequence[str] | None = None,
    dry_run: bool = False,
    generated_at: datetime | None = None,
    output_root: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_config_path = resolve_project_path(str(config_path))
    config = load_weight_tuning_config(resolved_config_path)
    inputs = _mapping(config.get("inputs"))
    shadow_config_path = resolve_project_path(
        str(inputs.get("shadow_backtest_config") or DEFAULT_SHADOW_BACKTEST_CONFIG_PATH)
    )
    portfolio_config_path = resolve_project_path(
        str(inputs.get("portfolio_candidate_profiles") or DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH)
    )
    shadow_config = load_shadow_backtest_config(shadow_config_path)
    portfolio_config = load_portfolio_candidate_config(portfolio_config_path)
    resolved_output_root = output_root or _output_root(config, dry_run=dry_run)
    profile_name = portfolio_profile or str(
        inputs.get("default_portfolio_profile") or "lower_rebalance_threshold_2pct"
    )
    profile_config = _portfolio_profile_config(portfolio_config, profile_name)
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
    baseline_path = resolve_project_path(
        str(inputs.get("baseline_parameters") or DEFAULT_PRODUCTION_PARAMETERS_PATH)
    )
    try:
        baseline = load_production_parameters(baseline_path)
    except (OSError, ValueError) as exc:
        return _blocked_payload_pair(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            reason=f"baseline parameters unavailable: {exc}",
            status="INSUFFICIENT_DATA",
            dry_run=dry_run,
            data_gate=data_gate,
            output_root=resolved_output_root,
        )

    data_quality_as_of = resolved_as_of
    latest_price_as_of = shadow_backtest_module._latest_price_date(prices)
    if as_of is None and latest_price_as_of is not None and latest_price_as_of > resolved_as_of:
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
    freshness = _market_data_freshness_summary(resolved_as_of, allow_latest=as_of is None)
    if data_gate.get("status") != str(inputs.get("require_data_gate") or "OK"):
        return _blocked_payload_pair(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            reason=str(data_gate.get("reason") or "data gate did not return OK"),
            status="INSUFFICIENT_DATA",
            dry_run=dry_run,
            data_gate=data_gate,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            freshness=freshness,
            warnings=warnings,
            output_root=resolved_output_root,
        )
    if data_quality_status in {"FAILED", "INSUFFICIENT_DATA"}:
        return _blocked_payload_pair(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            reason="validate-data gate failed before weight tuning.",
            status="INSUFFICIENT_DATA",
            dry_run=dry_run,
            data_gate=data_gate,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            freshness=freshness,
            warnings=warnings,
            output_root=resolved_output_root,
        )
    required_freshness = str(inputs.get("require_freshness_status") or "OK")
    if freshness.get("status") != required_freshness:
        return _blocked_payload_pair(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            reason=(
                f"market data freshness is {freshness.get('status', 'MISSING')}, "
                f"required {required_freshness}."
            ),
            status="INSUFFICIENT_DATA",
            dry_run=dry_run,
            data_gate=data_gate,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            freshness=freshness,
            warnings=warnings,
            output_root=resolved_output_root,
        )

    signal_snapshot_path = _signal_snapshot_path(shadow_config, resolved_as_of)
    signal_snapshot_payload = load_signal_snapshot_payload(signal_snapshot_path)
    snapshot_summary = signal_snapshot_summary(signal_snapshot_payload)
    allowed_snapshot_status = {
        str(item) for item in _sequence(inputs.get("allow_signal_snapshot_status"))
    }
    if not signal_snapshot_payload or snapshot_summary.get("status") not in allowed_snapshot_status:
        return _blocked_payload_pair(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            reason=(
                "signal snapshot missing or status not allowed: "
                f"{snapshot_summary.get('status', 'MISSING')}"
            ),
            status="INSUFFICIENT_DATA",
            dry_run=dry_run,
            data_gate=data_gate,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            freshness=freshness,
            warnings=warnings,
            signal_snapshot_path=signal_snapshot_path,
            signal_quality=snapshot_summary,
            output_root=resolved_output_root,
        )

    selected_signals = _selected_signals(config, signals)
    candidates, rejected_by_constraints = generate_restricted_grid_candidates(
        config,
        baseline.weights,
        selected_signals=selected_signals,
    )
    signal_frames = signal_snapshot_frames(signal_snapshot_payload)
    trading_dates = shadow_backtest_module._trading_dates(prices, baseline, resolved_as_of)
    walk_forward_config = WalkForwardConfig.model_validate(_mapping(config.get("walk_forward")))
    windows = generate_walk_forward_windows(trading_dates, walk_forward_config)
    guardrail_config = _mapping(config.get("guardrails"))
    min_windows = _int_value(guardrail_config.get("min_validation_windows"), default=3)
    if len(windows) < min_windows:
        payload = _base_payload(
            as_of=resolved_as_of,
            generated_at=generated,
            config=config,
            config_path=resolved_config_path,
            shadow_config_path=shadow_config_path,
            portfolio_config_path=portfolio_config_path,
            baseline_path=baseline_path,
            portfolio_profile=profile_name,
            data_gate=data_gate,
            data_quality_status=data_quality_status,
            data_quality_report_path=data_quality_report_path,
            freshness=freshness,
            signal_snapshot_path=signal_snapshot_path,
            signal_quality=snapshot_summary,
            dry_run=dry_run,
            output_root=resolved_output_root,
            warnings=[
                *warnings,
                f"walk-forward windows={len(windows)} below min_validation_windows={min_windows}",
            ],
            status="INSUFFICIENT_DATA",
        )
        payload["baseline"] = {"weights": dict(baseline.weights), "metrics": {}}
        payload["search"] = {
            "method": "restricted_grid_search",
            "candidates_evaluated": 0,
            "candidates_rejected_by_constraints": rejected_by_constraints,
            "candidates_rejected_by_guardrails": 0,
            "candidates_generated": len(candidates),
        }
        payload["recommended_candidate"] = _no_candidate_payload(
            dict(baseline.weights),
            "needs_more_data",
            "Walk-forward validation windows are insufficient.",
        )
        payload["walk_forward"] = {
            "windows": [window.to_dict() for window in windows],
            "non_worse_window_ratio": 0.0,
        }
        candidates_payload = _candidates_payload(payload, [])
        return payload, candidates_payload

    full_start = _full_result_start(windows, trading_dates, shadow_config.market_regime)
    full_end = resolved_as_of
    full_context = _simulation_context(
        profile_name=profile_name,
        profile_config=profile_config,
        prices=prices,
        baseline=baseline,
        shadow_config=shadow_config,
        signal_frames=signal_frames,
        start=full_start,
        end=full_end,
    )
    window_contexts = {
        window.window_id: _simulation_context(
            profile_name=f"{profile_name}__{window.window_id}",
            profile_config=profile_config,
            prices=prices,
            baseline=baseline,
            shadow_config=shadow_config,
            signal_frames=signal_frames,
            start=window.validation_start,
            end=window.validation_end,
        )
        for window in windows
    }
    baseline_full = _simulate_weight_context(
        full_context,
        weights=baseline.weights,
        profile_name=f"{profile_name}__baseline",
    )
    baseline_windows = {
        window_id: _simulate_weight_context(
            context,
            weights=baseline.weights,
            profile_name=f"{profile_name}__baseline__{window_id}",
        )
        for window_id, context in window_contexts.items()
    }
    evaluated = [
        _evaluate_candidate(
            candidate=candidate,
            baseline_full=baseline_full,
            baseline_windows=baseline_windows,
            full_context=full_context,
            window_contexts=window_contexts,
            profile_name=profile_name,
            windows=windows,
            config=config,
            data_gate=data_gate,
            freshness=freshness,
            signal_quality=snapshot_summary,
        )
        for candidate in candidates
    ]
    ranked = sorted(
        evaluated,
        key=lambda item: (
            item["guardrail_status"] != "PASS",
            -_float_value(_mapping(item.get("objective_breakdown")).get("objective_score")),
            item.get("candidate_id", ""),
        ),
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    guardrail_rejections = sum(1 for row in evaluated if row.get("guardrail_status") != "PASS")
    recommended = _recommended_candidate(ranked, baseline.weights)
    metadata_status = _metadata_status(recommended, snapshot_summary)
    payload = _base_payload(
        as_of=resolved_as_of,
        generated_at=generated,
        config=config,
        config_path=resolved_config_path,
        shadow_config_path=shadow_config_path,
        portfolio_config_path=portfolio_config_path,
        baseline_path=baseline_path,
        portfolio_profile=profile_name,
        data_gate=data_gate,
        data_quality_status=data_quality_status,
        data_quality_report_path=data_quality_report_path,
        freshness=freshness,
        signal_snapshot_path=signal_snapshot_path,
        signal_quality=snapshot_summary,
        dry_run=dry_run,
        output_root=resolved_output_root,
        warnings=warnings,
        status=metadata_status,
        requested_date_range={"start": full_start.isoformat(), "end": full_end.isoformat()},
    )
    payload["baseline"] = {
        "weights": dict(baseline.weights),
        "metrics": _metrics_payload(baseline_full),
    }
    payload["search"] = {
        "method": "restricted_grid_search",
        "candidates_evaluated": len(evaluated),
        "candidates_rejected_by_constraints": rejected_by_constraints,
        "candidates_rejected_by_guardrails": guardrail_rejections,
        "candidates_generated": len(candidates),
        "selected_signals": list(selected_signals),
    }
    summary_limit = int(config.get("summary_candidate_limit", 10))
    payload["candidate_ranking"] = [
        _candidate_ranking_row(row) for row in ranked[:summary_limit]
    ]
    payload["recommended_candidate"] = recommended
    payload["walk_forward"] = {
        "windows": list(_mapping(recommended).get("walk_forward_windows", [])),
        "non_worse_window_ratio": _mapping(recommended.get("relative_metrics")).get(
            "non_worse_walk_forward_ratio",
            0.0,
        )
        if isinstance(recommended, dict)
        else 0.0,
    }
    candidates_payload = _candidates_payload(payload, ranked)
    return payload, candidates_payload


def generate_restricted_grid_candidates(
    config: Mapping[str, Any],
    baseline_weights: Mapping[str, float],
    *,
    selected_signals: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    scope = _mapping(config.get("tuning_scope"))
    tunable = _mapping(scope.get("tunable_weights"))
    capped = _mapping(scope.get("capped_weights"))
    fixed = _mapping(scope.get("fixed_weights"))
    constraints = _mapping(config.get("constraints"))
    variable_names = [*tunable.keys(), *capped.keys()]
    selected = (
        _selected_signals(config, selected_signals)
        if selected_signals is not None
        else tuple(variable_names)
    )
    fixed_values = {name: _float_value(_mapping(item).get("value")) for name, item in fixed.items()}
    base_values = dict(baseline_weights)
    ranges: dict[str, list[float]] = {}
    for name in variable_names:
        entry = _mapping(tunable.get(name) or capped.get(name))
        if name in selected:
            ranges[name] = _range_values(entry)
        else:
            ranges[name] = [_clamp(_float_value(base_values.get(name)), entry)]
    rejected = 0
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, float], ...]] = set()
    target_sum = _float_value(constraints.get("total_weight_sum"), default=1.0)
    max_l1 = _float_value(constraints.get("max_total_l1_distance_from_baseline"), default=2.0)
    for index, values in enumerate(product(*(ranges[name] for name in variable_names)), start=1):
        raw = {name: float(value) for name, value in zip(variable_names, values, strict=True)}
        weights = _normalize_preserving_fixed(
            raw,
            fixed_values,
            target_sum=target_sum,
            normalize=bool(constraints.get("normalize_after_search", True)),
        )
        if not _candidate_within_constraints(weights, config):
            rejected += 1
            continue
        l1_distance = sum(
            abs(weights.get(name, 0.0) - float(baseline_weights.get(name, 0.0)))
            for name in set(weights) | set(baseline_weights)
        )
        if l1_distance > max_l1 + 1e-12:
            rejected += 1
            continue
        key = tuple(sorted((name, round(value, 12)) for name, value in weights.items()))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            {
                "candidate_id": f"wt-{len(candidates) + 1:04d}",
                "source_grid_index": index,
                "weights": weights,
                "l1_distance_from_baseline": round(l1_distance, 12),
                "fallback_signals_free_tuned": False,
                "raw_search_values": raw,
            }
        )
    max_candidates = _int_value(_mapping(config.get("search")).get("max_candidates"), default=0)
    if max_candidates > 0 and len(candidates) > max_candidates:
        candidates = sorted(
            candidates,
            key=lambda item: (
                _float_value(item.get("l1_distance_from_baseline")),
                str(item.get("candidate_id")),
            ),
        )
        rejected += len(candidates) - max_candidates
        candidates = candidates[:max_candidates]
    return candidates, rejected


def write_weight_tuning_summary(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(render_weight_tuning_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def write_weight_tuning_candidates(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_recommended_shadow_weights(payload: dict[str, Any], output_path: Path) -> Path:
    metadata = _mapping(payload.get("metadata"))
    recommended = _mapping(payload.get("recommended_candidate"))
    constraints = _mapping(payload.get("safety"))
    weights = _mapping(recommended.get("weights"))
    review_status = str(recommended.get("status") or "rejected")
    profile = str(_mapping(payload.get("inputs")).get("portfolio_profile") or "")
    yaml_payload = {
        "metadata": {
            "version": f"shadow-weight-candidate-{payload.get('as_of', output_path.parent.name)}",
            "source": "TRADING-059 restricted backtest weight tuning",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "signal_quality_status": _mapping(payload.get("signal_quality")).get(
                "status",
                metadata.get("status", "UNKNOWN"),
            ),
            "portfolio_profile": profile,
        },
        "weights": {key: float(value) for key, value in weights.items()},
        "constraints": {
            "fallback_signals_free_tuned": constraints.get(
                "fallback_signals_free_tuned",
                False,
            ),
            "production_write_allowed": False,
            "promotion_allowed": False,
        },
        "review": {
            "status": review_status,
            "reason": recommended.get("reason", ""),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(yaml_payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return output_path


def write_weight_tuning_report_alias(
    payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        **payload,
        "report_type": WEIGHT_TUNING_ALIAS_REPORT_TYPE,
        "source_report_type": WEIGHT_TUNING_REPORT_TYPE,
    }
    json_path, markdown_path = report_alias_paths(reports_dir, as_of)
    return write_weight_tuning_summary(alias_payload, json_path, markdown_path)


def load_weight_tuning_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_weight_tuning_payload(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != WEIGHT_TUNING_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") not in {
        WEIGHT_TUNING_REPORT_TYPE,
        WEIGHT_TUNING_ALIAS_REPORT_TYPE,
    }:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    for key in ("run_id", "generated_at", "status", "production_effect"):
        if key not in metadata:
            issues.append(f"metadata missing {key}")
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    safety = _mapping(payload.get("safety"))
    if safety.get("fallback_signals_free_tuned") is not False:
        issues.append("fallback_signals_free_tuned must be false")
    if safety.get("production_config_modified") is not False:
        issues.append("production_config_modified must be false")
    if safety.get("production_write_allowed") is not False:
        issues.append("production_write_allowed must be false")
    if not isinstance(payload.get("recommended_candidate"), dict):
        issues.append("recommended_candidate must be an object")
    if not isinstance(payload.get("search"), dict):
        issues.append("search must be an object")
    return issues


def weight_tuning_payload_date(payload: Mapping[str, Any], source_path: Path) -> date:
    metadata = _mapping(payload.get("metadata"))
    run_id = str(metadata.get("run_id") or "")
    raw_date = run_id.removeprefix("weight-tuning-")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        raise ValueError(f"cannot infer weight tuning date from {source_path}") from exc


def render_weight_tuning_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    inputs = _mapping(payload.get("inputs"))
    signal_quality = _mapping(payload.get("signal_quality"))
    baseline = _mapping(payload.get("baseline"))
    baseline_metrics = _mapping(baseline.get("metrics"))
    search = _mapping(payload.get("search"))
    recommended = _mapping(payload.get("recommended_candidate"))
    candidate_metrics = _mapping(recommended.get("metrics"))
    relative = _mapping(recommended.get("relative_metrics"))
    objective = _mapping(recommended.get("objective_breakdown"))
    guardrails = _mapping(recommended.get("guardrails"))
    walk_forward = _mapping(payload.get("walk_forward"))
    lines = [
        "# Restricted Backtest Weight Tuning Summary",
        "",
        "## 1. Executive Summary",
        "",
        f"- run_id: `{metadata.get('run_id', 'UNKNOWN')}`",
        f"- status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- recommended_status: `{recommended.get('status', 'UNKNOWN')}`",
        f"- production_effect: `{metadata.get('production_effect', 'none')}`",
        f"- manual_review_required: `{metadata.get('manual_review_required', True)}`",
        f"- auto_promotion: `{metadata.get('auto_promotion', False)}`",
        f"- reason: {recommended.get('reason', '')}",
        "",
        "## 2. Input Readiness",
        "",
        f"- baseline_parameters: `{inputs.get('baseline_parameters', '')}`",
        f"- signal_snapshot: `{inputs.get('signal_snapshot', '')}`",
        f"- backtest_input_manifest: `{inputs.get('backtest_input_manifest', '')}`",
        f"- portfolio_profile: `{inputs.get('portfolio_profile', '')}`",
        f"- data_gate: `{_mapping(payload.get('data_quality')).get('data_gate_status', '')}`",
        f"- freshness: `{_mapping(payload.get('freshness')).get('status', 'MISSING')}`",
        "",
        "## 3. Signal Quality",
        "",
        f"- status: `{signal_quality.get('status', 'UNKNOWN')}`",
        f"- real_signals: `{', '.join(_strings(signal_quality.get('real_signals'))) or 'none'}`",
        f"- proxy_signals: `{', '.join(_strings(signal_quality.get('proxy_signals'))) or 'none'}`",
        "- fallback_signals: "
        f"`{', '.join(_strings(signal_quality.get('fallback_signals'))) or 'none'}`",
        "",
        "## 4. Baseline Weights",
        "",
        "| Signal | Weight |",
        "|---|---:|",
    ]
    for key, value in sorted(_mapping(baseline.get("weights")).items()):
        lines.append(f"| `{key}` | {_format_float(value)} |")
    promotion_impact = _mapping(payload.get("promotion_impact"))
    lines.extend(
        [
            "",
            "## 5. Tuning Scope",
            "",
        ]
    )
    for key, value in _mapping(payload.get("tuning_scope")).items():
        lines.append(f"- {key}: `{', '.join(_strings(value)) or value}`")
    lines.extend(
        [
            "",
            "## 6. Search Configuration",
            "",
            f"- method: `{search.get('method', 'restricted_grid_search')}`",
            f"- candidates_evaluated: `{search.get('candidates_evaluated', 0)}`",
            "- candidates_rejected_by_constraints: "
            f"`{search.get('candidates_rejected_by_constraints', 0)}`",
            "- candidates_rejected_by_guardrails: "
            f"`{search.get('candidates_rejected_by_guardrails', 0)}`",
            "",
            "## 7. Candidate Ranking",
            "",
            "| Rank | Candidate | Status | Objective | Guardrail |",
            "|---:|---|---|---:|---|",
        ]
    )
    ranking = _records(payload.get("candidate_ranking"))
    if not ranking:
        lines.append("|  | none |  |  |  |")
    for row in ranking:
        lines.append(
            "| "
            f"{row.get('rank', '')} | "
            f"`{row.get('candidate_id', '')}` | "
            f"`{row.get('status', '')}` | "
            f"{_format_float(row.get('objective_score'))} | "
            f"`{row.get('guardrail_status', '')}` |"
        )
    lines.extend(
        [
            "",
            "## 8. Recommended Shadow Weights",
            "",
            "| Signal | Weight |",
            "|---|---:|",
        ]
    )
    for key, value in sorted(_mapping(recommended.get("weights")).items()):
        lines.append(f"| `{key}` | {_format_float(value)} |")
    lines.extend(
        [
            "",
            "## 9. Baseline vs Recommended Candidate",
            "",
            "| Metric | Baseline | Candidate | Delta |",
            "|---|---:|---:|---:|",
        ]
    )
    for key in (
        "annualized_return",
        "cumulative_return",
        "max_drawdown",
        "sharpe_ratio",
        "turnover",
    ):
        lines.append(
            "| "
            f"`{key}` | "
            f"{_format_float(baseline_metrics.get(key))} | "
            f"{_format_float(candidate_metrics.get(key))} | "
            f"{_format_float(relative.get(f'{key}_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 10. Walk-forward Validation",
            "",
            f"- non_worse_walk_forward_ratio: `{walk_forward.get('non_worse_window_ratio', 0.0)}`",
            "",
            "| Window | Validation | Status | Sharpe Delta | Return Delta |",
            "|---|---|---|---:|---:|",
        ]
    )
    for window in _records(walk_forward.get("windows")):
        rel = _mapping(window.get("relative_metrics"))
        lines.append(
            "| "
            f"`{window.get('window_id', '')}` | "
            f"{window.get('validation_start', '')} to {window.get('validation_end', '')} | "
            f"`{window.get('status', '')}` | "
            f"{_format_float(rel.get('sharpe_ratio_delta'))} | "
            f"{_format_float(rel.get('annualized_return_delta'))} |"
        )
    lines.extend(
        [
            "",
            "## 11. Guardrails",
            "",
            f"- guardrail_status: `{guardrails.get('status', 'UNKNOWN')}`",
            "- hard_rejections: "
            f"`{', '.join(_strings(guardrails.get('hard_rejections'))) or 'none'}`",
            "",
            "## 12. Overfitting Warnings",
            "",
        ]
    )
    warnings = _strings(payload.get("warnings"))
    lines.extend([f"- {warning}" for warning in warnings] or ["- 未发现额外警告。"])
    lines.extend(
        [
            "",
            "## 13. Promotion Impact",
            "",
            "- can_support_candidate_promotion: "
            f"`{promotion_impact.get('can_support_candidate_promotion', False)}`",
            f"- reason: {promotion_impact.get('reason', '')}",
            "",
            "## 14. Manual Review Checklist",
            "",
            "- 确认 data gate / freshness / signal snapshot status。",
            "- 确认 fallback signals 未自由调参。",
            "- 复核 walk-forward windows 是否足以支持 shadow-only 观察。",
            "- 确认不修改 `config/parameters/production/current.yaml`。",
            "",
            "## 15. Input / Output Artifacts",
            "",
        ]
    )
    for section_name in ("inputs", "output_artifacts"):
        lines.append(f"### {section_name}")
        for key, value in _mapping(payload.get(section_name)).items():
            lines.append(f"- `{key}`: `{value}`")
        lines.append("")
    lines.extend(
        [
            "## Objective Breakdown",
            "",
        ]
    )
    for key, value in objective.items():
        lines.append(f"- `{key}`: `{_format_float(value)}`")
    return "\n".join(lines).rstrip() + "\n"


def render_weight_tuning_explanation(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    recommended = _mapping(payload.get("recommended_candidate"))
    search = _mapping(payload.get("search"))
    guardrails = _mapping(recommended.get("guardrails"))
    relative = _mapping(recommended.get("relative_metrics"))
    return "\n".join(
        [
            f"status={metadata.get('status', 'UNKNOWN')}",
            f"recommended_status={recommended.get('status', 'UNKNOWN')}",
            f"candidates_evaluated={search.get('candidates_evaluated', 0)}",
            f"guardrail_status={guardrails.get('status', 'UNKNOWN')}",
            f"sharpe_delta={_format_float(relative.get('sharpe_ratio_delta'))}",
            f"annualized_return_delta={_format_float(relative.get('annualized_return_delta'))}",
            f"non_worse_walk_forward_ratio="
            f"{_format_float(relative.get('non_worse_walk_forward_ratio'))}",
            f"reason={recommended.get('reason', '')}",
            "production_effect=none",
            "manual_review_required=true",
            "auto_promotion=false",
        ]
    )


def _simulation_context(
    *,
    profile_name: str,
    profile_config: dict[str, Any],
    prices: pd.DataFrame,
    baseline: Any,
    shadow_config: Any,
    signal_frames: dict[str, pd.DataFrame],
    start: date,
    end: date,
) -> _SimulationContext:
    price_panel = _prepare_price_panel(prices, baseline.flattened_asset_universe())
    if not price_panel.empty:
        price_panel = price_panel.loc[
            (price_panel.index.date >= start) & (price_panel.index.date <= end)
        ].copy()
    returns = (
        price_panel.pct_change().fillna(0.0)
        if len(price_panel.index) >= 3
        else pd.DataFrame()
    )
    features = (
        _feature_frame(price_panel, returns, signal_frames=signal_frames)
        if not returns.empty
        else {}
    )
    tradable_assets = [
        asset for asset in baseline.flattened_asset_universe() if asset not in DEFENSIVE_ASSETS
    ]
    defensive_assets = [asset for asset in ("SGOV", "CASH") if asset in price_panel.columns]
    return _SimulationContext(
        profile_name=profile_name,
        profile_config=profile_config,
        baseline=baseline,
        shadow_config=shadow_config,
        returns=returns,
        features=features,
        tradable_assets=tradable_assets,
        defensive_assets=defensive_assets,
    )


def _simulate_weight_context(
    context: _SimulationContext,
    *,
    weights: Mapping[str, float],
    profile_name: str,
) -> SensitivitySimulation:
    if context.returns.empty or not context.features:
        return _simulate_sensitivity_profile(
            profile_name=profile_name,
            profile_config=context.profile_config,
            prices=pd.DataFrame(),
            baseline=context.baseline,
            shadow_config=context.shadow_config,
            signal_frames={},
            start=date(1970, 1, 1),
            end=date(1970, 1, 1),
            signal_weights=dict(weights),
        )
    composite, contribution_frames = _composite_scores(
        context.features,
        weights=dict(weights),
        tradable_assets=context.tradable_assets,
    )
    multiplier = _float_value(
        context.profile_config.get("score_sensitivity_multiplier"),
        default=1.0,
    )
    adjusted_score = (0.5 + (composite - 0.5) * multiplier).clip(0.0, 1.0)
    target, constraint_binding = _target_weights_from_scores(
        adjusted_score,
        baseline=context.baseline,
        profile_config=context.profile_config,
        tradable_assets=context.tradable_assets,
        defensive_assets=context.defensive_assets,
    )
    actual, rebalance_info = _actual_weights_from_target(
        target,
        threshold=_float_value(context.profile_config.get("rebalance_threshold"), default=0.05),
    )
    turnover = actual.diff().abs().sum(axis=1).fillna(actual.abs().sum(axis=1))
    shifted_actual = actual.shift(1).fillna(0.0)
    gross_returns = (
        shifted_actual * context.returns.reindex(columns=shifted_actual.columns).fillna(0.0)
    ).sum(axis=1)
    costs = turnover.apply(
        lambda value: calculate_transaction_cost(
            float(value),
            context.shadow_config.transaction_cost,
        )
    )
    strategy_returns = gross_returns - costs
    equity = (1.0 + strategy_returns).cumprod()
    benchmark_returns = (
        context.returns["QQQ"] if "QQQ" in context.returns.columns else gross_returns
    )
    exposure = shifted_actual.loc[
        :,
        [asset for asset in context.tradable_assets if asset in shifted_actual.columns],
    ]
    exposure_series = (
        exposure.sum(axis=1)
        if not exposure.empty
        else pd.Series(0.0, index=context.returns.index)
    )
    metrics = calculate_portfolio_metrics(
        strategy_returns,
        equity,
        turnover,
        benchmark_returns=benchmark_returns,
        exposure=exposure_series,
    ).to_dict()
    return SensitivitySimulation(
        profile_name=profile_name,
        profile_config=context.profile_config,
        target=target,
        actual=actual,
        score=composite,
        adjusted_score=adjusted_score,
        daily_rows=(),
        score_rows=(),
        metrics=metrics,
        gross_turnover=float(turnover.sum()),
        transaction_cost_drag=float(costs.sum()),
        rebalance_opportunity_days=int(rebalance_info["rebalance_opportunity_days"]),
        rebalance_days=int(rebalance_info["rebalance_days"]),
        constraint_binding=constraint_binding,
    )


def _evaluate_candidate(
    *,
    candidate: dict[str, Any],
    baseline_full: SensitivitySimulation,
    baseline_windows: dict[str, SensitivitySimulation],
    full_context: _SimulationContext,
    window_contexts: dict[str, _SimulationContext],
    profile_name: str,
    windows: Sequence[WalkForwardWindow],
    config: Mapping[str, Any],
    data_gate: Mapping[str, Any],
    freshness: Mapping[str, Any],
    signal_quality: Mapping[str, Any],
) -> dict[str, Any]:
    weights = {key: float(value) for key, value in _mapping(candidate.get("weights")).items()}
    candidate_full = _simulate_weight_context(
        full_context,
        weights=weights,
        profile_name=f"{profile_name}__{candidate['candidate_id']}",
    )
    window_payloads: list[dict[str, Any]] = []
    breakdowns: list[dict[str, float]] = []
    non_worse_count = 0
    for window in windows:
        baseline_sim = baseline_windows[window.window_id]
        candidate_sim = _simulate_weight_context(
            window_contexts[window.window_id],
            weights=weights,
            profile_name=f"{profile_name}__{candidate['candidate_id']}__{window.window_id}",
        )
        relative = _relative_metrics(baseline_sim, candidate_sim, 0.0)
        status = _window_status(relative, _mapping(config.get("guardrails")))
        if status == "non_worse":
            non_worse_count += 1
        breakdown = _objective_breakdown(
            baseline_sim,
            candidate_sim,
            _mapping(_mapping(config.get("objective")).get("ranking_weights")),
        )
        breakdowns.append(breakdown)
        window_payloads.append(
            {
                **window.to_dict(),
                "status": status,
                "baseline_metrics": _metrics_payload(baseline_sim),
                "candidate_metrics": _metrics_payload(candidate_sim),
                "relative_metrics": relative,
                "objective_breakdown": breakdown,
            }
        )
    non_worse_ratio = non_worse_count / len(windows) if windows else 0.0
    relative = _relative_metrics(baseline_full, candidate_full, non_worse_ratio)
    objective = _average_objective_breakdown(
        breakdowns,
        _mapping(_mapping(config.get("objective")).get("ranking_weights")),
    )
    guardrails = _guardrail_payload(
        relative_metrics=relative,
        config=config,
        data_gate=data_gate,
        freshness=freshness,
        validation_window_count=len(windows),
        fallback_signals_free_tuned=bool(candidate.get("fallback_signals_free_tuned")),
    )
    status, reason = _candidate_status_and_reason(
        objective,
        relative,
        guardrails,
        signal_quality,
        config,
    )
    rejection_reasons = _candidate_rejection_reasons(
        objective=objective,
        relative=relative,
        guardrails=guardrails,
        signal_quality=signal_quality,
        config=config,
    )
    return {
        "candidate_id": candidate["candidate_id"],
        "weights": weights,
        "constraint_status": "PASS",
        "metrics": _metrics_payload(candidate_full),
        "relative_metrics": relative,
        "objective_breakdown": objective,
        "guardrails": guardrails,
        "guardrail_status": guardrails["status"],
        "rejection_reasons": rejection_reasons,
        "status": status,
        "reason": reason,
        "walk_forward_windows": window_payloads,
        "walk_forward_summary": _candidate_walk_forward_summary(
            window_payloads,
            relative,
            _mapping(config.get("guardrails")),
        ),
        "l1_distance_from_baseline": candidate.get("l1_distance_from_baseline", 0.0),
        "fallback_signals_free_tuned": False,
    }


def _relative_metrics(
    baseline: SensitivitySimulation,
    candidate: SensitivitySimulation,
    non_worse_ratio: float,
) -> dict[str, float]:
    baseline_metrics = _metrics_payload(baseline)
    candidate_metrics = _metrics_payload(candidate)
    return {
        "baseline_turnover": _round_float(baseline_metrics.get("turnover", 0.0)),
        "candidate_turnover": _round_float(candidate_metrics.get("turnover", 0.0)),
        "annualized_return_delta": _round_float(
            candidate_metrics.get("annualized_return", 0.0)
            - baseline_metrics.get("annualized_return", 0.0)
        ),
        "cumulative_return_delta": _round_float(
            candidate_metrics.get("cumulative_return", 0.0)
            - baseline_metrics.get("cumulative_return", 0.0)
        ),
        "max_drawdown_delta": _round_float(
            candidate_metrics.get("max_drawdown", 0.0)
            - baseline_metrics.get("max_drawdown", 0.0)
        ),
        "sharpe_ratio_delta": _round_float(
            candidate_metrics.get("sharpe_ratio", 0.0)
            - baseline_metrics.get("sharpe_ratio", 0.0)
        ),
        "turnover_delta": _round_float(
            candidate_metrics.get("turnover", 0.0) - baseline_metrics.get("turnover", 0.0)
        ),
        "cost_drag_delta": _round_float(
            candidate_metrics.get("estimated_cost_drag", 0.0)
            - baseline_metrics.get("estimated_cost_drag", 0.0)
        ),
        "non_worse_walk_forward_ratio": _round_float(non_worse_ratio),
    }


def _objective_breakdown(
    baseline: SensitivitySimulation,
    candidate: SensitivitySimulation,
    weights: Mapping[str, Any],
) -> dict[str, float]:
    baseline_metrics = _metrics_payload(baseline)
    candidate_metrics = _metrics_payload(candidate)
    sharpe = candidate_metrics.get("sharpe_ratio", 0.0) - baseline_metrics.get(
        "sharpe_ratio",
        0.0,
    )
    drawdown = candidate_metrics.get("max_drawdown", 0.0) - baseline_metrics.get(
        "max_drawdown",
        0.0,
    )
    annualized = candidate_metrics.get("annualized_return", 0.0) - baseline_metrics.get(
        "annualized_return",
        0.0,
    )
    transmission = _signal_transmission_score(candidate) - _signal_transmission_score(baseline)
    turnover = max(
        candidate_metrics.get("turnover", 0.0) - baseline_metrics.get("turnover", 0.0),
        0.0,
    )
    objective = (
        _float_value(weights.get("sharpe_improvement"), default=0.35) * sharpe
        + _float_value(weights.get("max_drawdown_improvement"), default=0.25) * drawdown
        + _float_value(weights.get("annualized_return_improvement"), default=0.20)
        * annualized
        + _float_value(weights.get("signal_transmission_improvement"), default=0.10)
        * transmission
        - _float_value(weights.get("turnover_penalty"), default=0.10) * turnover
    )
    return {
        "sharpe_improvement_score": _round_float(sharpe),
        "drawdown_improvement_score": _round_float(drawdown),
        "return_improvement_score": _round_float(annualized),
        "signal_transmission_score": _round_float(transmission),
        "turnover_penalty_score": _round_float(turnover),
        "objective_score": _round_float(objective),
    }


def _average_objective_breakdown(
    breakdowns: Sequence[Mapping[str, Any]],
    weights: Mapping[str, Any],
) -> dict[str, float]:
    if not breakdowns:
        return {
            "sharpe_improvement_score": 0.0,
            "drawdown_improvement_score": 0.0,
            "return_improvement_score": 0.0,
            "signal_transmission_score": 0.0,
            "turnover_penalty_score": 0.0,
            "objective_score": 0.0,
        }
    averaged = {
        key: _round_float(
            sum(_float_value(item.get(key)) for item in breakdowns) / len(breakdowns)
        )
        for key in (
            "sharpe_improvement_score",
            "drawdown_improvement_score",
            "return_improvement_score",
            "signal_transmission_score",
            "turnover_penalty_score",
        )
    }
    averaged["objective_score"] = _round_float(
        _float_value(weights.get("sharpe_improvement"), default=0.35)
        * averaged["sharpe_improvement_score"]
        + _float_value(weights.get("max_drawdown_improvement"), default=0.25)
        * averaged["drawdown_improvement_score"]
        + _float_value(weights.get("annualized_return_improvement"), default=0.20)
        * averaged["return_improvement_score"]
        + _float_value(weights.get("signal_transmission_improvement"), default=0.10)
        * averaged["signal_transmission_score"]
        - _float_value(weights.get("turnover_penalty"), default=0.10)
        * averaged["turnover_penalty_score"]
    )
    return averaged


def _guardrail_payload(
    *,
    relative_metrics: Mapping[str, Any],
    config: Mapping[str, Any],
    data_gate: Mapping[str, Any],
    freshness: Mapping[str, Any],
    validation_window_count: int,
    fallback_signals_free_tuned: bool,
) -> dict[str, Any]:
    guardrails = _mapping(config.get("guardrails"))
    failed: list[str] = []
    if data_gate.get("status") != "OK":
        failed.append("data_gate_not_ok")
    if freshness.get("status") != "OK":
        failed.append("freshness_not_ok")
    if _mapping(config.get("metadata")).get("production_effect") != "none":
        failed.append("production_effect_not_none")
    if _mapping(config.get("metadata")).get("auto_promotion") is not False:
        failed.append("auto_promotion_true")
    if fallback_signals_free_tuned:
        failed.append("fallback_signal_free_tuned")
    if _float_value(relative_metrics.get("max_drawdown_delta")) < -_float_value(
        guardrails.get("max_drawdown_worse_limit"),
        default=0.02,
    ):
        failed.append("max_drawdown_worse_than_baseline_by_more_than_limit")
    if _float_value(relative_metrics.get("annualized_return_delta")) < -_float_value(
        guardrails.get("annualized_return_underperformance_limit"),
        default=0.01,
    ):
        failed.append("annualized_return_underperformance_more_than_limit")
    turnover_relative = _turnover_relative_increase(relative_metrics)
    if turnover_relative > _float_value(
        guardrails.get("turnover_relative_increase_limit"),
        default=0.30,
    ):
        failed.append("turnover_increase_more_than_limit")
    if _float_value(relative_metrics.get("non_worse_walk_forward_ratio")) < _float_value(
        guardrails.get("min_non_worse_walk_forward_ratio"),
        default=0.60,
    ):
        failed.append("non_worse_walk_forward_ratio_below_minimum")
    if validation_window_count < _int_value(guardrails.get("min_validation_windows"), default=3):
        failed.append("insufficient_validation_windows")
    return {
        "status": "FAIL" if failed else "PASS",
        "hard_rejections": failed,
        "turnover_relative_increase": _round_float(turnover_relative),
        "validation_window_count": validation_window_count,
    }


def _candidate_rejection_reasons(
    *,
    objective: Mapping[str, Any],
    relative: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    signal_quality: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    hard_rejections = _strings(guardrails.get("hard_rejections"))
    for reason in hard_rejections:
        reasons.append(_failure_reason_code(reason))
    if _float_value(relative.get("annualized_return_delta")) <= 0.0:
        reasons.append("no_return_improvement")
    if _float_value(relative.get("sharpe_ratio_delta")) <= 0.0:
        reasons.append("sharpe_not_improved")
    if _float_value(relative.get("max_drawdown_delta")) < 0.0:
        reasons.append("max_drawdown_worse")
    if _float_value(relative.get("cost_drag_delta")) > 0.0:
        reasons.append("cost_drag_too_high")
    if _float_value(objective.get("objective_score")) <= _float_value(
        _mapping(config.get("candidate_status_policy")).get("watch_objective_score_min"),
        default=0.0,
    ):
        reasons.append("risk_adjusted_performance_worse")
    if signal_quality.get("status") == "LIMITED" and guardrails.get("status") == "PASS":
        reasons.append("signal_quality_limited")
    return sorted(dict.fromkeys(reasons))


def _failure_reason_code(reason: str) -> str:
    return {
        "max_drawdown_worse_than_baseline_by_more_than_limit": "drawdown_guardrail_failed",
        "annualized_return_underperformance_more_than_limit": "return_guardrail_failed",
        "turnover_increase_more_than_limit": "turnover_guardrail_failed",
        "non_worse_walk_forward_ratio_below_minimum": "walk_forward_guardrail_failed",
        "fallback_signal_free_tuned": "fallback_free_tuning_guardrail_failed",
        "production_effect_not_none": "production_safety_guardrail_failed",
        "auto_promotion_true": "production_safety_guardrail_failed",
    }.get(reason, reason)


def _candidate_walk_forward_summary(
    windows: Sequence[Mapping[str, Any]],
    relative: Mapping[str, Any],
    guardrails: Mapping[str, Any],
) -> dict[str, Any]:
    positive = 0
    negative = 0
    worst_window = ""
    worst_score = 0.0
    for window in windows:
        window_relative = _mapping(window.get("relative_metrics"))
        return_delta = _float_value(window_relative.get("annualized_return_delta"))
        sharpe_delta = _float_value(window_relative.get("sharpe_ratio_delta"))
        score = return_delta + sharpe_delta
        if return_delta > 0.0 or sharpe_delta > 0.0:
            positive += 1
        if return_delta < 0.0 or sharpe_delta < 0.0:
            negative += 1
        if not worst_window or score < worst_score:
            worst_window = str(window.get("window_id") or "")
            worst_score = score
    count = len(windows)
    return {
        "validation_window_count": count,
        "non_worse_ratio": _round_float(relative.get("non_worse_walk_forward_ratio")),
        "non_worse_ratio_threshold": _round_float(
            guardrails.get("min_non_worse_walk_forward_ratio"),
        ),
        "positive_windows": positive,
        "negative_windows": negative,
        "unstable_windows": negative,
        "positive_window_ratio": _round_float(positive / count if count else 0.0),
        "worst_window": worst_window,
        "worst_window_status": "worse" if negative else "",
    }


def _candidate_status_and_reason(
    objective: Mapping[str, Any],
    relative: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    signal_quality: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[str, str]:
    if guardrails.get("status") != "PASS":
        return "rejected", "Candidate failed one or more hard guardrails."
    status_policy = _mapping(config.get("candidate_status_policy"))
    if _float_value(objective.get("objective_score")) <= _float_value(
        status_policy.get("watch_objective_score_min"),
        default=0.0,
    ):
        return "rejected", "Candidate did not improve the configured walk-forward objective."
    clear = (
        _float_value(relative.get("sharpe_ratio_delta"))
        >= _float_value(status_policy.get("shadow_candidate_min_sharpe_delta"), default=0.05)
        and _float_value(relative.get("annualized_return_delta"))
        >= _float_value(
            status_policy.get("shadow_candidate_min_annualized_return_delta"),
            default=0.01,
        )
        and _float_value(relative.get("non_worse_walk_forward_ratio"))
        >= _float_value(
            status_policy.get("shadow_candidate_min_non_worse_ratio"),
            default=0.60,
        )
    )
    if clear:
        return (
            "shadow_candidate_only",
            "Candidate improves the configured walk-forward objective and passes guardrails, "
            "but production promotion remains disabled.",
        )
    if signal_quality.get("status") == "LIMITED":
        return (
            "watch",
            "Candidate mildly improves baseline, but signal quality remains LIMITED.",
        )
    return "watch", "Candidate passes guardrails and should be reviewed manually."


def _recommended_candidate(
    ranked: Sequence[dict[str, Any]],
    baseline_weights: Mapping[str, float],
) -> dict[str, Any]:
    for row in ranked:
        if row.get("guardrail_status") == "PASS" and row.get("status") in {
            "watch",
            "shadow_candidate_only",
        }:
            return dict(row)
    if ranked:
        best = dict(ranked[0])
        best["status"] = "rejected"
        best["reason"] = "No candidate passed guardrails with a positive configured objective."
        return best
    return _no_candidate_payload(
        dict(baseline_weights),
        "rejected",
        "No weight tuning candidate was evaluated.",
    )


def _base_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: Mapping[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    portfolio_config_path: Path,
    baseline_path: Path,
    portfolio_profile: str,
    data_gate: Mapping[str, Any],
    data_quality_status: str = "UNKNOWN",
    data_quality_report_path: Path | None = None,
    freshness: Mapping[str, Any] | None = None,
    signal_snapshot_path: Path | None = None,
    signal_quality: Mapping[str, Any] | None = None,
    dry_run: bool = False,
    output_root: Path | None = None,
    warnings: Sequence[str] | None = None,
    status: str = "UNKNOWN",
    requested_date_range: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    root = output_root or _output_root(config, dry_run=dry_run)
    signal_quality_payload = _signal_quality_payload(signal_quality or {})
    metadata = _mapping(config.get("metadata"))
    run_id = f"weight-tuning-{as_of.isoformat()}"
    input_artifacts = {
        "baseline_parameters": str(baseline_path),
        "weight_tuning_config": str(config_path),
        "shadow_backtest_config": str(shadow_config_path),
        "portfolio_candidate_profiles": str(portfolio_config_path),
        "signal_snapshot": "" if signal_snapshot_path is None else str(signal_snapshot_path),
        "backtest_input_manifest": str(_manifest_path_from_data_gate(dict(data_gate)) or ""),
        "data_quality_report": (
            "" if data_quality_report_path is None else str(data_quality_report_path)
        ),
        "market_data_freshness": str((freshness or {}).get("source_artifact") or ""),
        "portfolio_profile": portfolio_profile,
    }
    output_artifacts = {
        "weight_tuning_summary_json": str(default_weight_tuning_json_path(root, as_of)),
        "weight_tuning_summary_md": str(default_weight_tuning_markdown_path(root, as_of)),
        "recommended_shadow_weights": str(default_recommended_shadow_weights_path(root, as_of)),
        "weight_tuning_candidates": str(default_weight_tuning_candidates_path(root, as_of)),
    }
    return {
        "schema_version": WEIGHT_TUNING_SCHEMA_VERSION,
        "report_type": WEIGHT_TUNING_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": run_id,
            "generated_at": generated_at.isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": dry_run,
            "market_regime": _mapping(config.get("market_regime")).get(
                "id",
                "ai_after_chatgpt",
            ),
            "market_regime_anchor": _mapping(config.get("market_regime")).get(
                "anchor_date",
                "2022-11-30",
            ),
            "market_regime_anchor_event": _mapping(config.get("market_regime")).get(
                "anchor_event",
                "ChatGPT public launch",
            ),
            "requested_date_range": dict(requested_date_range or {}),
            "config_path": str(config_path),
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(config_path, baseline_path),
            "policy_version": metadata.get("version", "weight-tuning-v0.1"),
        },
        "inputs": input_artifacts,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
        "data_quality": {
            "status": data_quality_status,
            "data_gate_status": data_gate.get("status", "UNKNOWN"),
            "data_gate": dict(data_gate),
            "quality_report_path": ""
            if data_quality_report_path is None
            else str(data_quality_report_path),
            "backtest_manifest": str(_manifest_path_from_data_gate(dict(data_gate)) or ""),
        },
        "freshness": dict(freshness or {"status": "MISSING"}),
        "signal_quality": signal_quality_payload,
        "tuning_scope": _tuning_scope_payload(config),
        "constraints": _mapping(config.get("constraints")),
        "objective": _mapping(config.get("objective")),
        "guardrail_policy": _mapping(config.get("guardrails")),
        "baseline": {},
        "search": {},
        "candidate_ranking": [],
        "recommended_candidate": {},
        "walk_forward": {"windows": [], "non_worse_window_ratio": 0.0},
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": (
                "Weight tuning is shadow-only. It cannot write production parameters or "
                "enable automatic promotion."
            ),
        },
        "warnings": list(dict.fromkeys(str(warning) for warning in warnings or [] if warning)),
        "safety": _safety_payload(),
    }


def _blocked_payload_pair(
    *,
    as_of: date,
    generated_at: datetime,
    config: Mapping[str, Any],
    config_path: Path,
    shadow_config_path: Path,
    portfolio_config_path: Path,
    baseline_path: Path,
    portfolio_profile: str,
    reason: str,
    status: str,
    dry_run: bool,
    data_gate: Mapping[str, Any],
    data_quality_status: str = "FAILED",
    data_quality_report_path: Path | None = None,
    freshness: Mapping[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    signal_snapshot_path: Path | None = None,
    signal_quality: Mapping[str, Any] | None = None,
    output_root: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _base_payload(
        as_of=as_of,
        generated_at=generated_at,
        config=config,
        config_path=config_path,
        shadow_config_path=shadow_config_path,
        portfolio_config_path=portfolio_config_path,
        baseline_path=baseline_path,
        portfolio_profile=portfolio_profile,
        data_gate=data_gate,
        data_quality_status=data_quality_status,
        data_quality_report_path=data_quality_report_path,
        freshness=freshness,
        signal_snapshot_path=signal_snapshot_path,
        signal_quality=signal_quality,
        dry_run=dry_run,
        output_root=output_root,
        warnings=[*(warnings or ()), reason],
        status=status,
    )
    baseline_weights = _read_baseline_weights(baseline_path)
    payload["baseline"] = {"weights": baseline_weights, "metrics": {}}
    payload["search"] = {
        "method": "restricted_grid_search",
        "candidates_evaluated": 0,
        "candidates_rejected_by_constraints": 0,
        "candidates_rejected_by_guardrails": 0,
        "candidates_generated": 0,
    }
    payload["recommended_candidate"] = _no_candidate_payload(
        baseline_weights,
        "insufficient_data",
        reason,
    )
    payload["promotion_impact"] = {
        "can_support_candidate_promotion": False,
        "reason": reason,
    }
    return payload, _candidates_payload(payload, [])


def _candidates_payload(
    summary_payload: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metadata = _mapping(summary_payload.get("metadata"))
    return {
        "schema_version": WEIGHT_TUNING_SCHEMA_VERSION,
        "report_type": "weight_tuning_candidates",
        "metadata": {
            "run_id": metadata.get("run_id"),
            "generated_at": metadata.get("generated_at"),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "summary_artifact": _mapping(summary_payload.get("output_artifacts")).get(
            "weight_tuning_summary_json",
            "",
        ),
        "candidate_count": len(candidates),
        "candidates": [dict(candidate) for candidate in candidates],
        "safety": _safety_payload(),
    }


def _candidate_ranking_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rank": row.get("rank", ""),
        "candidate_id": row.get("candidate_id", ""),
        "status": row.get("status", "UNKNOWN"),
        "constraint_status": row.get("constraint_status", "PASS"),
        "guardrail_status": row.get("guardrail_status", "UNKNOWN"),
        "rejection_reasons": _strings(row.get("rejection_reasons")),
        "objective_score": _mapping(row.get("objective_breakdown")).get("objective_score", 0.0),
        "relative_metrics": row.get("relative_metrics", {}),
        "walk_forward_summary": row.get("walk_forward_summary", {}),
        "weights": row.get("weights", {}),
        "reason": row.get("reason", ""),
    }


def _no_candidate_payload(
    weights: Mapping[str, float],
    status: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "weights": {key: float(value) for key, value in weights.items()},
        "metrics": {},
        "relative_metrics": {},
        "objective_breakdown": {
            "sharpe_improvement_score": 0.0,
            "drawdown_improvement_score": 0.0,
            "return_improvement_score": 0.0,
            "signal_transmission_score": 0.0,
            "turnover_penalty_score": 0.0,
            "objective_score": 0.0,
        },
        "guardrail_status": "FAIL",
        "guardrails": {"status": "FAIL", "hard_rejections": [status]},
        "reason": reason,
        "walk_forward_windows": [],
    }


def _metadata_status(recommended: Mapping[str, Any], signal_quality: Mapping[str, Any]) -> str:
    status = str(recommended.get("status") or "")
    if status in {"watch", "shadow_candidate_only"}:
        return str(signal_quality.get("status") or "LIMITED")
    if status in {"insufficient_data", "needs_more_data"}:
        return "INSUFFICIENT_DATA"
    return "NO_CANDIDATE"


def _window_status(relative: Mapping[str, Any], guardrails: Mapping[str, Any]) -> str:
    return_delta = _float_value(relative.get("annualized_return_delta"))
    drawdown_delta = _float_value(relative.get("max_drawdown_delta"))
    if return_delta < -_float_value(
        guardrails.get("annualized_return_underperformance_limit"),
        default=0.01,
    ):
        return "worse"
    if drawdown_delta < -_float_value(
        guardrails.get("max_drawdown_worse_limit"),
        default=0.02,
    ):
        return "worse"
    return "non_worse"


def _signal_transmission_score(simulation: SensitivitySimulation) -> float:
    actual = simulation.actual
    if actual.empty:
        return 0.0
    risk_assets = [column for column in actual.columns if column not in {"CASH", "SGOV"}]
    if not risk_assets:
        return 0.0
    dispersion = actual.loc[:, risk_assets].std(axis=1, ddof=0).mean()
    return 0.0 if pd.isna(dispersion) else float(dispersion)


def _metrics_payload(simulation: SensitivitySimulation) -> dict[str, float | int]:
    metrics = dict(simulation.metrics)
    metrics["estimated_cost_drag"] = _round_float(simulation.transaction_cost_drag)
    metrics["signal_transmission_score"] = _round_float(_signal_transmission_score(simulation))
    return {
        key: _round_float(value) if isinstance(value, float) else value
        for key, value in metrics.items()
    }


def _normalize_preserving_fixed(
    raw: Mapping[str, float],
    fixed: Mapping[str, float],
    *,
    target_sum: float,
    normalize: bool,
) -> dict[str, float]:
    fixed_total = sum(float(value) for value in fixed.values())
    residual = max(target_sum - fixed_total, 0.0)
    raw_total = sum(float(value) for value in raw.values())
    weights = dict(fixed)
    if normalize and raw_total > 0.0:
        weights.update({key: float(value) / raw_total * residual for key, value in raw.items()})
    else:
        weights.update({key: float(value) for key, value in raw.items()})
    return {key: round(float(value), 12) for key, value in weights.items()}


def _candidate_within_constraints(weights: Mapping[str, float], config: Mapping[str, Any]) -> bool:
    constraints = _mapping(config.get("constraints"))
    target_sum = _float_value(constraints.get("total_weight_sum"), default=1.0)
    if abs(sum(weights.values()) - target_sum) > 1e-9:
        return False
    if bool(constraints.get("forbid_negative_weights", True)) and any(
        value < -1e-12 for value in weights.values()
    ):
        return False
    min_single = _float_value(constraints.get("min_single_weight"), default=0.0)
    max_single = _float_value(constraints.get("max_single_weight"), default=1.0)
    if any(value < min_single - 1e-12 or value > max_single + 1e-12 for value in weights.values()):
        return False
    scope = _mapping(config.get("tuning_scope"))
    entries = {**_mapping(scope.get("tunable_weights")), **_mapping(scope.get("capped_weights"))}
    for name, entry in entries.items():
        entry_map = _mapping(entry)
        value = float(weights.get(name, 0.0))
        if value < _float_value(entry_map.get("min")) - 1e-12:
            return False
        if value > _float_value(entry_map.get("max")) + 1e-12:
            return False
    for name, entry in _mapping(scope.get("fixed_weights")).items():
        expected = _float_value(_mapping(entry).get("value"))
        if abs(float(weights.get(name, 0.0)) - expected) > 1e-12:
            return False
    return True


def _selected_signals(config: Mapping[str, Any], signals: Sequence[str] | None) -> tuple[str, ...]:
    scope = _mapping(config.get("tuning_scope"))
    tunable = set(_mapping(scope.get("tunable_weights")))
    capped = set(_mapping(scope.get("capped_weights")))
    fixed = set(_mapping(scope.get("fixed_weights")))
    allowed = tunable | capped
    if signals is None:
        return tuple(sorted(allowed))
    selected = tuple(dict.fromkeys(str(signal) for signal in signals if str(signal)))
    unknown = [signal for signal in selected if signal not in allowed | fixed]
    if unknown:
        raise ValueError("unknown tuning signal(s): " + ", ".join(unknown))
    forbidden = [signal for signal in selected if signal in fixed]
    if forbidden:
        raise ValueError("fixed fallback signals cannot be freely tuned: " + ", ".join(forbidden))
    if not selected:
        raise ValueError("at least one tuning signal is required")
    return selected


def _range_values(entry: Mapping[str, Any]) -> list[float]:
    start = _float_value(entry.get("min"))
    stop = _float_value(entry.get("max"))
    step = _float_value(entry.get("step"))
    values: list[float] = []
    current = start
    while current <= stop + 1e-12:
        values.append(round(current, 12))
        current += step
    return values


def _clamp(value: float, entry: Mapping[str, Any]) -> float:
    return min(max(value, _float_value(entry.get("min"))), _float_value(entry.get("max")))


def _turnover_relative_increase(relative: Mapping[str, Any]) -> float:
    baseline_turnover_delta = _float_value(relative.get("turnover_delta"))
    candidate_turnover = _float_value(relative.get("candidate_turnover"))
    baseline_turnover = candidate_turnover - baseline_turnover_delta
    if baseline_turnover <= 1e-12:
        return 0.0 if baseline_turnover_delta <= 1e-12 else 1.0
    return max(baseline_turnover_delta / abs(baseline_turnover), 0.0)


def _portfolio_profile_config(config: Mapping[str, Any], profile_name: str) -> dict[str, Any]:
    profiles = _mapping(config.get("profiles"))
    profile = _mapping(profiles.get(profile_name))
    if not profile:
        raise ValueError(f"unknown portfolio profile: {profile_name}")
    return profile


def _market_data_freshness_summary(as_of: date, *, allow_latest: bool = False) -> dict[str, Any]:
    path = _latest_market_data_freshness_path(as_of)
    if path is None and allow_latest:
        path = _latest_market_data_freshness_path(None)
    if path is None:
        return {"status": "MISSING", "source_artifact": ""}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "MISSING", "source_artifact": str(path)}
    freshness = _mapping(payload.get("freshness"))
    metadata = _mapping(payload.get("metadata"))
    readiness = _mapping(payload.get("tracking_readiness"))
    return {
        "status": str(freshness.get("status") or metadata.get("status") or "UNKNOWN"),
        "tracking_readiness": str(readiness.get("readiness") or ""),
        "source_artifact": str(path),
    }


def _latest_market_data_freshness_path(as_of: date | None) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_freshness"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_freshness_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if as_of is None or candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _full_result_start(
    windows: Sequence[WalkForwardWindow],
    trading_dates: Sequence[date],
    market_regime: Any,
) -> date:
    regime_start = getattr(market_regime, "default_backtest_start", date(2022, 12, 1))
    if windows:
        return max(windows[0].validation_start, regime_start)
    if trading_dates:
        return max(trading_dates[0], regime_start)
    return regime_start


def _tuning_scope_payload(config: Mapping[str, Any]) -> dict[str, list[str]]:
    scope = _mapping(config.get("tuning_scope"))
    return {
        "tunable_weights": sorted(_mapping(scope.get("tunable_weights"))),
        "capped_weights": sorted(_mapping(scope.get("capped_weights"))),
        "fixed_weights": sorted(_mapping(scope.get("fixed_weights"))),
        "forbidden_free_tuning": sorted(_mapping(scope.get("fixed_weights"))),
    }


def _signal_quality_payload(summary: Mapping[str, Any]) -> dict[str, Any]:
    fallback = [
        *_strings(summary.get("neutral_fallback_signals")),
        *[
            signal
            for signal in _strings(summary.get("proxy_signals"))
            if signal == "valuation_risk"
        ],
    ]
    return {
        "status": summary.get("status", "MISSING"),
        "real_signals": _strings(summary.get("real_signals")),
        "proxy_signals": _strings(summary.get("proxy_signals")),
        "fallback_signals": sorted(dict.fromkeys(fallback)),
        "production_promotion_allowed": False,
    }


def _safety_payload() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "fallback_signals_free_tuned": False,
        "production_write_allowed": False,
        "production_config_modified": False,
        "candidate_promotion_triggered": False,
        "broker_action": False,
        "trading_action": False,
    }


def _output_root(config: Mapping[str, Any], *, dry_run: bool) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "weight_tuning"
    output = _mapping(config.get("output"))
    return resolve_project_path(
        str(output.get("weight_tuning_dir") or default_weight_tuning_root())
    )


def _data_quality_report_dir(
    config: Mapping[str, Any],
    shadow_config: Any,
    *,
    dry_run: bool,
) -> Path:
    if dry_run:
        return PROJECT_ROOT / "outputs" / "dry_runs" / "weight_tuning" / "reports"
    output = _mapping(config.get("output"))
    if output.get("data_quality_report_dir"):
        return resolve_project_path(str(output.get("data_quality_report_dir")))
    return resolve_project_path(str(shadow_config.data.data_quality_report_dir))


def _read_baseline_weights(path: Path) -> dict[str, float]:
    try:
        baseline = load_production_parameters(path)
    except (OSError, ValueError):
        return {}
    return {key: float(value) for key, value in baseline.weights.items()}


def _config_hash(*paths: Path) -> str:
    from hashlib import sha256

    digest = sha256()
    for path in paths:
        if path.exists() and path.is_file():
            digest.update(str(path).encode("utf-8"))
            digest.update(sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


def _validate_weight_tuning_config(payload: Mapping[str, Any]) -> None:
    metadata = _mapping(payload.get("metadata"))
    if metadata.get("production_effect") != "none":
        raise ValueError("weight tuning production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        raise ValueError("weight tuning manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        raise ValueError("weight tuning auto_promotion must be false")
    scope = _mapping(payload.get("tuning_scope"))
    if not _mapping(scope.get("tunable_weights")):
        raise ValueError("weight tuning config missing tunable_weights")
    if not _mapping(scope.get("fixed_weights")):
        raise ValueError("weight tuning config missing fixed_weights")
    constraints = _mapping(payload.get("constraints"))
    if constraints.get("forbid_free_fallback_weight_tuning") is not True:
        raise ValueError("weight tuning must forbid free fallback tuning")
    if not _mapping(payload.get("guardrails")):
        raise ValueError("weight tuning config missing guardrails")


def _sequence(value: object) -> tuple[object, ...]:
    if isinstance(value, list | tuple):
        return tuple(value)
    return ()


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
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


def _round_float(value: object, *, digits: int = 12) -> float:
    return round(_float_value(value), digits)


def _format_float(value: object) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "NA"
    return f"{number:.4f}"
