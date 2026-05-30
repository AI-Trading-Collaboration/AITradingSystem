from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.backtesting.portfolio_simulator import (
    PortfolioSimulationResult,
    simulate_parameter_portfolio,
)
from ai_trading_system.trading_engine.backtesting.walk_forward import (
    WalkForwardWindow,
    generate_walk_forward_windows,
)
from ai_trading_system.trading_engine.parameters.parameter_diff import (
    ParameterChange,
    diff_parameters,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_production_parameters,
    load_promotion_rules,
    load_shadow_backtest_config,
    resolve_project_path,
)
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    DataQualityStatus,
    ProductionParameters,
    ShadowBacktestConfig,
)
from ai_trading_system.trading_engine.parameters.promotion_rules import (
    PromotionDecision,
    evaluate_promotion_decision,
)
from ai_trading_system.trading_engine.parameters.shadow_parameter_generator import (
    CandidateWeightSet,
    generate_bounded_weight_candidates,
)
from ai_trading_system.trading_engine.reports.parameter_promotion_report import (
    PARAMETER_PROMOTION_REPORT_TYPE,
    PARAMETER_PROMOTION_SCHEMA_VERSION,
    default_parameter_promotion_json_path,
    default_parameter_promotion_markdown_path,
    write_parameter_promotion_decision,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    SHADOW_BACKTEST_REPORT_TYPE,
    SHADOW_BACKTEST_SCHEMA_VERSION,
    default_shadow_backtest_summary_json_path,
    default_shadow_backtest_summary_markdown_path,
    write_shadow_backtest_summary,
)


@dataclass(frozen=True)
class ShadowBacktestArtifacts:
    summary_json: Path
    summary_markdown: Path
    shadow_parameters_json: Path
    shadow_parameters_markdown: Path
    candidate_parameters_json: Path
    candidate_parameters_markdown: Path
    promotion_json: Path
    promotion_markdown: Path

    def to_dict(self) -> dict[str, str]:
        return {
            "shadow_backtest_summary_json": str(self.summary_json),
            "shadow_backtest_summary_md": str(self.summary_markdown),
            "shadow_parameters_json": str(self.shadow_parameters_json),
            "shadow_parameters_md": str(self.shadow_parameters_markdown),
            "candidate_parameters_json": str(self.candidate_parameters_json),
            "candidate_parameters_md": str(self.candidate_parameters_markdown),
            "parameter_promotion_decision_json": str(self.promotion_json),
            "parameter_promotion_decision_md": str(self.promotion_markdown),
        }


@dataclass(frozen=True)
class ShadowBacktestRun:
    as_of: date
    payload: dict[str, Any]
    promotion_payload: dict[str, Any]
    artifacts: ShadowBacktestArtifacts | None


@dataclass(frozen=True)
class CandidateEvaluation:
    candidate: CandidateWeightSet
    objective_score: float
    windows: tuple[dict[str, Any], ...]
    passing_ratio: float


def build_shadow_backtest_summary(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: bool = False,
) -> ShadowBacktestRun:
    config = load_shadow_backtest_config(config_path)
    config_file = Path(config_path)
    prices_path = resolve_project_path(config.data.prices_path)
    prices = _read_prices(prices_path)
    resolved_as_of = as_of or _latest_price_date(prices) or date.today()
    output_roots = _output_roots(config, dry_run=dry_run)
    data_quality_report_path = default_quality_report_path(
        output_roots["data_quality_report_dir"],
        resolved_as_of,
    )
    generated_at = datetime.now(tz=UTC)
    baseline_path = resolve_project_path(config.baseline_parameters_path)
    promotion_rules_path = resolve_project_path(config.promotion_rules_path)
    input_artifacts = {
        "baseline_parameters": str(baseline_path),
        "shadow_backtest_config": str(config_file),
        "promotion_rules": str(promotion_rules_path),
        "market_data_snapshot": str(prices_path),
        "signal_snapshot": "generated_from_price_features_v0_1",
        "data_quality_report": str(data_quality_report_path),
    }
    try:
        baseline = load_production_parameters(baseline_path)
        promotion_rules = load_promotion_rules(promotion_rules_path)
    except (OSError, ValueError) as exc:
        payload = _failure_payload(
            as_of=resolved_as_of,
            generated_at=generated_at,
            config=config,
            config_path=config_file,
            input_artifacts=input_artifacts,
            reason=str(exc),
        )
        promotion_payload = _promotion_payload_from_summary(payload)
        return ShadowBacktestRun(resolved_as_of, payload, promotion_payload, artifacts=None)

    data_quality_report = _run_data_quality_gate(
        config=config,
        baseline=baseline,
        as_of=resolved_as_of,
        report_path=data_quality_report_path,
    )
    from ai_trading_system.trading_engine.backtest_input_diagnostics import (
        run_backtest_input_diagnostics,
    )

    diagnostic_run = run_backtest_input_diagnostics(
        as_of=resolved_as_of,
        config_path=config_file,
        output_root=resolve_project_path(config.output.shadow_backtest_dir).parent,
        generated_at=datetime(
            resolved_as_of.year,
            resolved_as_of.month,
            resolved_as_of.day,
            tzinfo=UTC,
        ),
    )
    input_artifacts["backtest_input_diagnostic"] = str(diagnostic_run.json_path)
    input_artifacts["backtest_input_manifest"] = str(diagnostic_run.manifest_path)
    backtest_mode = _diagnostic_backtest_mode(diagnostic_run.payload)
    signal_snapshot_path = _signal_snapshot_path_from_diagnostics(diagnostic_run.payload)
    from ai_trading_system.trading_engine.signal_snapshots import (
        load_signal_snapshot_payload,
        signal_snapshot_frames,
    )

    signal_snapshot_payload = (
        load_signal_snapshot_payload(signal_snapshot_path)
        if signal_snapshot_path is not None
        and backtest_mode in {"full_signal_backtest_limited", "full_signal_backtest"}
        else {}
    )
    signal_frames = (
        signal_snapshot_frames(signal_snapshot_payload) if signal_snapshot_payload else None
    )
    if signal_snapshot_path is not None:
        input_artifacts["signal_snapshot"] = str(signal_snapshot_path)
    quality_status, quality_warnings = _shadow_data_quality_status(
        config=config,
        baseline=baseline,
        prices=prices,
        as_of=resolved_as_of,
        data_quality_report=data_quality_report,
    )
    quality_status, quality_warnings = _merge_backtest_input_diagnostic_status(
        quality_status,
        quality_warnings,
        diagnostic_run.payload,
    )
    trading_dates = _trading_dates(prices, baseline, resolved_as_of)
    windows = generate_walk_forward_windows(trading_dates, config.walk_forward)
    if not windows and quality_status == "OK":
        quality_status = "INSUFFICIENT_DATA"
    candidates = generate_bounded_weight_candidates(baseline, config.search)
    evaluation = _select_candidate(
        prices=prices,
        baseline=baseline,
        candidates=candidates,
        windows=windows,
        config=config,
        signal_frames=signal_frames,
    )
    selected_weights = (
        evaluation.candidate.weights if evaluation is not None else dict(baseline.weights)
    )
    window_payloads = evaluation.windows if evaluation is not None else tuple()
    full_start = _full_result_start(windows, trading_dates, config)
    full_end = resolved_as_of
    baseline_result = simulate_parameter_portfolio(
        prices,
        baseline,
        baseline.weights,
        config.transaction_cost,
        start=full_start,
        end=full_end,
        signal_frames=signal_frames,
    )
    candidate_result = simulate_parameter_portfolio(
        prices,
        baseline,
        selected_weights,
        config.transaction_cost,
        start=full_start,
        end=full_end,
        signal_frames=signal_frames,
    )
    candidate_version = (
        evaluation.candidate.version if evaluation is not None else "no-shadow-candidate"
    )
    parameter_changes = _parameter_changes(
        baseline=baseline,
        candidate_weights=selected_weights,
        candidate_result=candidate_result,
        baseline_result=baseline_result,
        windows=window_payloads,
    )
    promotion_decision = evaluate_promotion_decision(
        rules=promotion_rules,
        baseline_result=baseline_result.metrics.to_dict(),
        candidate_result=candidate_result.metrics.to_dict(),
        walk_forward_windows=window_payloads,
        parameter_changes=parameter_changes,
        data_quality_status=quality_status,
        missing_required_input_artifacts=_missing_required_input_artifacts(input_artifacts),
    )
    promotion_decision = _apply_backtest_mode_promotion_constraints(
        promotion_decision,
        backtest_mode=backtest_mode,
    )
    passing_ratio = evaluation.passing_ratio if evaluation is not None else 0.0
    payload = _summary_payload(
        as_of=resolved_as_of,
        generated_at=generated_at,
        config=config,
        config_path=config_file,
        baseline=baseline,
        candidate_version=candidate_version,
        candidate_weights=selected_weights,
        baseline_result=baseline_result,
        candidate_result=candidate_result,
        parameter_changes=parameter_changes,
        windows=window_payloads,
        promotion_decision=promotion_decision,
        data_quality_status=quality_status,
        data_quality_report=data_quality_report,
        data_quality_report_path=data_quality_report_path,
        backtest_input_diagnostics=diagnostic_run.payload,
        backtest_input_diagnostics_path=diagnostic_run.json_path,
        input_artifacts=input_artifacts,
        output_artifacts={},
        signal_snapshot_payload=signal_snapshot_payload,
        passing_ratio=passing_ratio,
        warnings=tuple(quality_warnings),
        dry_run=dry_run,
    )
    promotion_payload = _promotion_payload_from_summary(payload)
    return ShadowBacktestRun(resolved_as_of, payload, promotion_payload, artifacts=None)


def run_shadow_parameter_backtest(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: bool = False,
) -> ShadowBacktestRun:
    run = build_shadow_backtest_summary(as_of=as_of, config_path=config_path, dry_run=dry_run)
    config = load_shadow_backtest_config(config_path)
    roots = _output_roots(config, dry_run=dry_run)
    artifacts = _write_artifacts(run.payload, run.promotion_payload, roots, run.as_of)
    payload = dict(run.payload)
    payload["output_artifacts"] = artifacts.to_dict()
    _rewrite_summary_with_outputs(payload, run.promotion_payload, artifacts)
    return ShadowBacktestRun(
        as_of=run.as_of,
        payload=payload,
        promotion_payload=run.promotion_payload,
        artifacts=artifacts,
    )


def _run_data_quality_gate(
    *,
    config: ShadowBacktestConfig,
    baseline: ProductionParameters,
    as_of: date,
    report_path: Path,
    backtest_manifest_path: Path | None = None,
) -> DataQualityReport:
    quality_config = load_data_quality()
    universe = load_universe()
    expected_tickers = [asset for asset in baseline.flattened_asset_universe() if asset != "CASH"]
    report = validate_data_cache(
        resolve_project_path(config.data.prices_path),
        resolve_project_path(config.data.rates_path),
        expected_tickers,
        configured_rate_series(universe),
        quality_config,
        as_of,
        manifest_path=resolve_project_path(config.data.download_manifest_path),
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=resolve_project_path(config.data.secondary_prices_path),
        require_secondary_prices=False,
    )
    write_data_quality_report(report, report_path)
    return report


def _select_candidate(
    *,
    prices: pd.DataFrame,
    baseline: ProductionParameters,
    candidates: tuple[CandidateWeightSet, ...],
    windows: tuple[WalkForwardWindow, ...],
    config: ShadowBacktestConfig,
    signal_frames: dict[str, pd.DataFrame] | None = None,
) -> CandidateEvaluation | None:
    if not candidates or not windows:
        return None
    baseline_by_window = {
        window.window_id: simulate_parameter_portfolio(
            prices,
            baseline,
            baseline.weights,
            config.transaction_cost,
            start=window.validation_start,
            end=window.validation_end,
            signal_frames=signal_frames,
        )
        for window in windows
    }
    evaluations: list[CandidateEvaluation] = []
    for candidate in candidates:
        window_payloads: list[dict[str, Any]] = []
        objective = 0.0
        pass_count = 0
        for window in windows:
            baseline_result = baseline_by_window[window.window_id]
            candidate_result = simulate_parameter_portfolio(
                prices,
                baseline,
                candidate.weights,
                config.transaction_cost,
                start=window.validation_start,
                end=window.validation_end,
                signal_frames=signal_frames,
            )
            status = _window_status(baseline_result, candidate_result)
            if status == "PASS":
                pass_count += 1
            objective += _window_objective(baseline_result, candidate_result)
            window_payloads.append(
                {
                    **window.to_dict(),
                    "baseline_metrics": baseline_result.metrics.to_dict(),
                    "candidate_metrics": candidate_result.metrics.to_dict(),
                    "status": status,
                }
            )
        passing_ratio = pass_count / len(windows)
        objective -= candidate.l1_change * 0.02
        evaluations.append(
            CandidateEvaluation(
                candidate=candidate,
                objective_score=objective,
                windows=tuple(window_payloads),
                passing_ratio=passing_ratio,
            )
        )
    return max(evaluations, key=lambda item: (item.objective_score, item.passing_ratio))


def _summary_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: ShadowBacktestConfig,
    config_path: Path,
    baseline: ProductionParameters,
    candidate_version: str,
    candidate_weights: dict[str, float],
    baseline_result: PortfolioSimulationResult,
    candidate_result: PortfolioSimulationResult,
    parameter_changes: tuple[ParameterChange, ...],
    windows: tuple[dict[str, Any], ...],
    promotion_decision: PromotionDecision,
    data_quality_status: DataQualityStatus,
    data_quality_report: DataQualityReport,
    data_quality_report_path: Path,
    backtest_input_diagnostics: dict[str, Any],
    backtest_input_diagnostics_path: Path,
    input_artifacts: dict[str, str],
    output_artifacts: dict[str, str],
    signal_snapshot_payload: dict[str, Any],
    passing_ratio: float,
    warnings: tuple[str, ...],
    dry_run: bool,
) -> dict[str, Any]:
    baseline_metrics = baseline_result.metrics.to_dict()
    candidate_metrics = candidate_result.metrics.to_dict()
    date_range = _date_range_from_rows(baseline_result.daily_rows)
    metadata_status = (
        "OK"
        if data_quality_status == "OK"
        else "LIMITED"
        if data_quality_status == "LIMITED"
        else "DEGRADED"
    )
    metadata = {
        "run_id": f"shadow-backtest-{as_of.isoformat()}",
        "generated_at": generated_at.isoformat(),
        "status": metadata_status,
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "observe_only": True,
        "dry_run": dry_run,
        "backtest_mode": _diagnostic_backtest_mode(backtest_input_diagnostics),
        "code_version": git_commit_sha() or "unknown",
        "git_worktree_dirty": git_worktree_dirty(),
        "config_hash": _config_hash(config_path, input_artifacts),
        "market_regime": config.market_regime.id,
        "market_regime_anchor": config.market_regime.anchor_date.isoformat(),
        "market_regime_anchor_event": config.market_regime.anchor_event,
        "date_range": date_range,
        "baseline_parameter_version": baseline.version,
        "candidate_parameter_version": candidate_version,
        "baseline_parameters_path": input_artifacts["baseline_parameters"],
    }
    return {
        "schema_version": SHADOW_BACKTEST_SCHEMA_VERSION,
        "report_type": SHADOW_BACKTEST_REPORT_TYPE,
        "metadata": metadata,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
        "data_quality": {
            "status": data_quality_status,
            "overall_status": _diagnostic_overall_status(backtest_input_diagnostics),
            "price_data_status": _diagnostic_check_status(
                backtest_input_diagnostics,
                "price_data",
            ),
            "signal_snapshots_status": _diagnostic_check_status(
                backtest_input_diagnostics,
                "signal_snapshots",
            ),
            "backtest_mode": _diagnostic_backtest_mode(backtest_input_diagnostics),
            "validate_data_status": data_quality_report.status,
            "quality_report_path": str(data_quality_report_path),
            "diagnostic_report": str(backtest_input_diagnostics_path),
            "blocking_errors": _diagnostic_blocking_errors(backtest_input_diagnostics),
            "blocking_reasons": _diagnostic_blocking_reasons(backtest_input_diagnostics),
            "can_run_shadow_backtest": _diagnostic_can_run_shadow_backtest(
                backtest_input_diagnostics
            ),
            "can_promote_candidate": _diagnostic_can_promote_candidate(backtest_input_diagnostics),
            "error_count": data_quality_report.error_count,
            "warning_count": data_quality_report.warning_count,
        },
        "point_in_time_status": dict(config.point_in_time_status),
        "baseline_parameters": {
            "version": baseline.version,
            "weights": dict(baseline.weights),
            "hard_gates": baseline.hard_gates,
            "position_limits": baseline.position_limits.model_dump(mode="json"),
        },
        "shadow_parameters": {
            "version": candidate_version,
            "weights": dict(candidate_weights),
            "hard_gates": baseline.hard_gates,
            "position_limits": baseline.position_limits.model_dump(mode="json"),
            "mode": "observe_only",
            "backtest_mode": _diagnostic_backtest_mode(backtest_input_diagnostics),
            "promotion_eligible": _diagnostic_can_promote_candidate(backtest_input_diagnostics),
        },
        "candidate_parameters": {
            "version": candidate_version,
            "weights": dict(candidate_weights),
            "hard_gates": baseline.hard_gates,
            "position_limits": baseline.position_limits.model_dump(mode="json"),
            "backtest_mode": _diagnostic_backtest_mode(backtest_input_diagnostics),
            "promotion_eligible": _diagnostic_can_promote_candidate(backtest_input_diagnostics),
        },
        "baseline_result": baseline_metrics,
        "candidate_result": candidate_metrics,
        "relative_comparison": _relative_comparison(baseline_metrics, candidate_metrics),
        "parameter_changes": [change.to_dict() for change in parameter_changes],
        "walk_forward_windows": list(windows),
        "score_calculation": _score_calculation_payload(
            backtest_mode=_diagnostic_backtest_mode(backtest_input_diagnostics),
            weights=candidate_weights,
            signal_snapshot_payload=signal_snapshot_payload,
            signal_snapshot_path=input_artifacts.get("signal_snapshot", ""),
        ),
        "score_attribution": {
            "mode": _diagnostic_backtest_mode(backtest_input_diagnostics),
            "row_count": len(candidate_result.score_rows),
            "rows": list(candidate_result.score_rows),
        },
        "parameter_contribution_summary": dict(
            candidate_result.parameter_contribution_summary or {}
        ),
        "passing_windows_ratio": passing_ratio,
        "overfitting_risk": _overfitting_risk(passing_ratio, len(windows)),
        "promotion_decision": _promotion_decision_payload(
            promotion_decision,
            as_of=as_of,
            backtest_mode=_diagnostic_backtest_mode(backtest_input_diagnostics),
        ),
        "promotion_constraints": _promotion_constraints_for_mode(
            _diagnostic_backtest_mode(backtest_input_diagnostics)
        ),
        "warnings": list(warnings),
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "broker_action": False,
            "trading_action": False,
        },
    }


def _failure_payload(
    *,
    as_of: date,
    generated_at: datetime,
    config: ShadowBacktestConfig,
    config_path: Path,
    input_artifacts: dict[str, str],
    reason: str,
) -> dict[str, Any]:
    decision = PromotionDecision(
        status="rejected",
        reason=f"Shadow backtest failed before evaluation: {reason}",
        hard_rejections=("missing_required_input_artifacts",),
        manual_review_items=("fix_shadow_backtest_inputs",),
        criteria_results={},
    )
    return {
        "schema_version": SHADOW_BACKTEST_SCHEMA_VERSION,
        "report_type": SHADOW_BACKTEST_REPORT_TYPE,
        "metadata": {
            "run_id": f"shadow-backtest-{as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "FAILED",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": False,
            "code_version": git_commit_sha() or "unknown",
            "git_worktree_dirty": git_worktree_dirty(),
            "config_hash": _config_hash(config_path, input_artifacts),
            "market_regime": config.market_regime.id,
            "date_range": "",
            "baseline_parameter_version": "MISSING",
            "candidate_parameter_version": "MISSING",
        },
        "input_artifacts": input_artifacts,
        "output_artifacts": {},
        "data_quality": {"status": "FAILED"},
        "point_in_time_status": dict(config.point_in_time_status),
        "baseline_result": {},
        "candidate_result": {},
        "relative_comparison": {},
        "parameter_changes": [],
        "walk_forward_windows": [],
        "passing_windows_ratio": 0.0,
        "overfitting_risk": "HIGH",
        "promotion_decision": decision.to_dict(),
        "warnings": [reason],
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_parameters_modified": False,
            "broker_action": False,
            "trading_action": False,
        },
    }


def _promotion_payload_from_summary(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(payload.get("metadata") or {})
    return {
        "schema_version": PARAMETER_PROMOTION_SCHEMA_VERSION,
        "report_type": PARAMETER_PROMOTION_REPORT_TYPE,
        "metadata": {
            "run_id": metadata.get("run_id"),
            "generated_at": metadata.get("generated_at"),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "baseline_parameter_version": metadata.get("baseline_parameter_version"),
            "candidate_parameter_version": metadata.get("candidate_parameter_version"),
        },
        "promotion_decision": payload.get("promotion_decision", {}),
        "source_shadow_backtest": metadata.get("run_id"),
        "safety": payload.get("safety", {}),
    }


def _promotion_decision_payload(
    decision: PromotionDecision,
    *,
    as_of: date,
    backtest_mode: str,
) -> dict[str, object]:
    payload = decision.to_dict()
    ablation_path = (
        PROJECT_ROOT
        / "artifacts"
        / "signal_ablation"
        / as_of.isoformat()
        / "signal_ablation_summary.json"
    )
    supporting = payload.get("supporting_artifacts")
    if not isinstance(supporting, dict):
        supporting = {}
    if ablation_path.exists():
        supporting["signal_ablation"] = str(ablation_path)
    calibration_path = _latest_signal_calibration_supporting_path(as_of)
    if calibration_path is not None:
        supporting["signal_calibration"] = str(calibration_path)
    sensitivity_path = _latest_portfolio_sensitivity_supporting_path(as_of)
    if sensitivity_path is not None:
        supporting["portfolio_sensitivity"] = str(sensitivity_path)
    candidates_path = _latest_portfolio_candidates_supporting_path(as_of)
    if candidates_path is not None:
        supporting["portfolio_candidates"] = str(candidates_path)
    review_path = _latest_portfolio_candidate_review_supporting_path(as_of)
    if review_path is not None:
        supporting["portfolio_candidate_review"] = str(review_path)
    tracking_path = _latest_portfolio_candidate_tracking_supporting_path(as_of)
    if tracking_path is not None:
        supporting["portfolio_candidate_tracking"] = str(tracking_path)
    tracking_review_path = _latest_portfolio_tracking_review_supporting_path(as_of)
    if tracking_review_path is not None:
        supporting["portfolio_tracking_review"] = str(tracking_review_path)
    freshness_path = _latest_market_data_freshness_supporting_path(as_of)
    if freshness_path is not None:
        supporting["market_data_freshness"] = str(freshness_path)
    refresh_path = _latest_market_data_refresh_supporting_path(as_of)
    if refresh_path is not None:
        supporting["market_data_refresh"] = str(refresh_path)
    if supporting:
        payload["supporting_artifacts"] = supporting
    if backtest_mode == "full_signal_backtest_limited":
        reason = str(payload.get("reason") or "")
        if ablation_path.exists() and "signal ablation" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Signal ablation summary is available as supporting evidence, "
                "but candidate promotion remains disabled while signal quality is limited "
                "and fallback signals remain present."
            )
            reason = str(payload.get("reason") or "")
        if calibration_path is not None and "signal calibration" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Signal calibration summary is available as supporting evidence, "
                "but it does not enable candidate promotion while signal quality remains "
                "LIMITED and fallback signals remain present."
            )
            reason = str(payload.get("reason") or "")
    reason = str(payload.get("reason") or "")
    if sensitivity_path is not None and "portfolio sensitivity" not in reason.lower():
        payload["reason"] = (
            reason.rstrip(".")
            + ". Portfolio sensitivity summary is available as supporting evidence, "
            "but it is advisory only and cannot enable candidate promotion."
        )
        reason = str(payload.get("reason") or "")
    if candidates_path is not None and "portfolio candidate" not in reason.lower():
        payload["reason"] = (
            reason.rstrip(".")
            + ". Portfolio candidate profiles improved or evaluated signal transmission, "
            "but signal snapshot quality remains LIMITED and candidate promotion remains "
            "disabled."
        )
        reason = str(payload.get("reason") or "")
    review_status = _portfolio_candidate_review_status(review_path)
    if review_status == "watch" and "manual watch" not in reason.lower():
        payload["reason"] = (
            reason.rstrip(".")
            + ". Portfolio candidate is under manual watch. Candidate improves "
            "responsiveness but signal quality remains LIMITED."
        )
        reason = str(payload.get("reason") or "")
    elif review_status == "approved_for_shadow_candidate":
        payload["shadow_candidate_status"] = "approved_for_tracking"
        if "shadow tracking only" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Manual review approved the portfolio profile for shadow tracking only. "
                "Production promotion remains disabled because signal quality is LIMITED."
            )
            reason = str(payload.get("reason") or "")
    tracking_status = _portfolio_candidate_tracking_status(tracking_path)
    if tracking_status == "active_tracking":
        payload["shadow_candidate_tracking"] = "active_tracking"
        if "tracked in shadow mode" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Portfolio candidate is being tracked in shadow mode. Production "
                "promotion remains disabled because signal quality is LIMITED."
            )
    elif tracking_status == "degraded_tracking":
        payload["shadow_candidate_tracking"] = "degraded_tracking"
        if "tracking is degraded" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Portfolio candidate tracking is degraded due to latest data "
                "roll-forward. Production promotion remains disabled."
            )
    elif tracking_status == "tracking_blocked":
        payload["shadow_candidate_tracking"] = "tracking_blocked"
        if "tracking is blocked" not in reason.lower():
            payload["reason"] = (
                reason.rstrip(".")
                + ". Portfolio candidate tracking is blocked, so it cannot support "
                "production promotion."
            )
            reason = str(payload.get("reason") or "")
    tracking_review = _portfolio_tracking_review_details(tracking_review_path)
    tracking_review_status = str(tracking_review.get("recommendation") or "")
    if tracking_review_status:
        payload["portfolio_tracking_review_recommendation"] = tracking_review_status
        payload["portfolio_tracking_review_tracking_days"] = tracking_review.get(
            "tracking_days",
            0,
        )
        payload["portfolio_tracking_review_stage"] = tracking_review.get("stage", "")
        if "tracking review" not in reason.lower():
            tracking_days = int(tracking_review.get("tracking_days") or 0)
            min_short = int(tracking_review.get("min_days_for_short_review") or 5)
            stage = str(tracking_review.get("stage") or "")
            if tracking_review_status == "needs_more_data" and tracking_days < min_short:
                review_reason = (
                    "Shadow candidate tracking is active, but only "
                    f"{tracking_days} tracking {_day_label(tracking_days)} "
                    f"{_is_are(tracking_days)} available. At least {min_short} "
                    "tracking days are required before short-window review."
                )
            elif stage == "short_window_review":
                review_reason = (
                    "Shadow candidate has entered short-window review, but "
                    "production promotion remains disabled because signal quality is "
                    "LIMITED and manual promotion gate has not been passed."
                )
            elif stage == "extended_review_ready":
                review_reason = (
                    "Shadow candidate has reached the extended review window, but "
                    "production promotion remains disabled unless a separate manual "
                    "promotion gate is passed."
                )
            else:
                review_reason = (
                    f"Shadow candidate tracking review is {tracking_review_status}; "
                    "this review is advisory only and cannot enable production promotion."
                )
            payload["reason"] = (
                reason.rstrip(".")
                + ". "
                + review_reason
            )
            reason = str(payload.get("reason") or "")
    freshness_status = _market_data_freshness_status(freshness_path)
    if freshness_status and "market data freshness" not in reason.lower():
        payload["reason"] = (
            reason.rstrip(".")
            + f". Market data freshness is {freshness_status}; it is supporting "
            "readiness evidence only and does not enable production promotion."
        )
        reason = str(payload.get("reason") or "")
    refresh_status = _market_data_refresh_status(refresh_path)
    if refresh_status and "market data refresh" not in reason.lower():
        payload["reason"] = (
            reason.rstrip(".")
            + f". Market data refresh is {refresh_status}; production promotion "
            "remains disabled."
        )
    return payload


def _latest_signal_calibration_supporting_path(as_of: date) -> Path | None:
    exact_path = (
        PROJECT_ROOT
        / "artifacts"
        / "signal_calibration"
        / as_of.isoformat()
        / "signal_calibration_summary.json"
    )
    if exact_path.exists():
        return exact_path
    root = PROJECT_ROOT / "artifacts" / "signal_calibration"
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


def _latest_portfolio_sensitivity_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_sensitivity"
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
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_candidates_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidates"
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


def _latest_portfolio_candidate_review_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_reviews"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_review_decision.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_candidate_tracking_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_tracking"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_tracking_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_tracking_review_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_tracking_reviews"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_tracking_review_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_market_data_freshness_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_freshness"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_freshness_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_market_data_refresh_supporting_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_refresh"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_refresh_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _portfolio_candidate_review_status(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    decision = payload.get("decision") if isinstance(payload, dict) else None
    if not isinstance(decision, dict):
        return ""
    return str(decision.get("status") or "")


def _portfolio_candidate_tracking_status(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    candidate = payload.get("candidate") if isinstance(payload, dict) else None
    if not isinstance(candidate, dict):
        return ""
    return str(candidate.get("tracking_status") or "")


def _portfolio_tracking_review_recommendation(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    recommendation = payload.get("recommendation") if isinstance(payload, dict) else None
    if not isinstance(recommendation, dict):
        return ""
    return str(recommendation.get("status") or "")


def _portfolio_tracking_review_details(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    recommendation = (
        payload.get("recommendation") if isinstance(payload.get("recommendation"), dict) else {}
    )
    tracking_window = (
        payload.get("tracking_window") if isinstance(payload.get("tracking_window"), dict) else {}
    )
    return {
        "recommendation": str(recommendation.get("status") or ""),
        "tracking_days": tracking_window.get(
            "tracking_days",
            candidate.get("tracking_days", 0),
        ),
        "stage": str(tracking_window.get("stage") or ""),
        "min_days_for_short_review": tracking_window.get("min_days_for_short_review", 5),
        "min_days_for_extended_review": tracking_window.get("min_days_for_extended_review", 20),
    }


def _market_data_freshness_status(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    freshness = payload.get("freshness") if isinstance(payload, dict) else None
    if isinstance(freshness, dict):
        return str(freshness.get("status") or "")
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if isinstance(metadata, dict):
        return str(metadata.get("status") or "")
    return ""


def _market_data_refresh_status(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if isinstance(metadata, dict):
        return str(metadata.get("status") or "")
    return ""


def _write_artifacts(
    payload: dict[str, Any],
    promotion_payload: dict[str, Any],
    roots: dict[str, Path],
    as_of: date,
) -> ShadowBacktestArtifacts:
    summary_json = default_shadow_backtest_summary_json_path(roots["shadow_backtest_dir"], as_of)
    summary_md = default_shadow_backtest_summary_markdown_path(roots["shadow_backtest_dir"], as_of)
    shadow_json = roots["shadow_parameters_dir"] / as_of.isoformat() / "shadow_parameters.json"
    shadow_md = roots["shadow_parameters_dir"] / as_of.isoformat() / "shadow_parameters.md"
    candidate_json = (
        roots["candidate_parameters_dir"] / as_of.isoformat() / "candidate_parameters.json"
    )
    candidate_md = roots["candidate_parameters_dir"] / as_of.isoformat() / "candidate_parameters.md"
    promotion_json = default_parameter_promotion_json_path(roots["parameter_promotion_dir"], as_of)
    promotion_md = default_parameter_promotion_markdown_path(
        roots["parameter_promotion_dir"],
        as_of,
    )
    write_shadow_backtest_summary(payload, summary_json, summary_md)
    _write_parameter_snapshot(payload, shadow_json, shadow_md, snapshot_key="shadow_parameters")
    _write_parameter_snapshot(
        payload,
        candidate_json,
        candidate_md,
        snapshot_key="candidate_parameters",
    )
    write_parameter_promotion_decision(promotion_payload, promotion_json, promotion_md)
    return ShadowBacktestArtifacts(
        summary_json=summary_json,
        summary_markdown=summary_md,
        shadow_parameters_json=shadow_json,
        shadow_parameters_markdown=shadow_md,
        candidate_parameters_json=candidate_json,
        candidate_parameters_markdown=candidate_md,
        promotion_json=promotion_json,
        promotion_markdown=promotion_md,
    )


def _rewrite_summary_with_outputs(
    payload: dict[str, Any],
    promotion_payload: dict[str, Any],
    artifacts: ShadowBacktestArtifacts,
) -> None:
    write_shadow_backtest_summary(payload, artifacts.summary_json, artifacts.summary_markdown)
    promotion_payload["source_artifacts"] = {"shadow_backtest_summary": str(artifacts.summary_json)}
    write_parameter_promotion_decision(
        promotion_payload,
        artifacts.promotion_json,
        artifacts.promotion_markdown,
    )


def _write_parameter_snapshot(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
    *,
    snapshot_key: str,
) -> None:
    snapshot = {
        "schema_version": 1,
        "report_type": snapshot_key,
        "metadata": payload.get("metadata", {}),
        "parameters": payload.get(snapshot_key, {}),
        "parameter_changes": payload.get("parameter_changes", []),
        "promotion_decision": payload.get("promotion_decision", {}),
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(_render_parameter_snapshot_markdown(snapshot), encoding="utf-8")


def _render_parameter_snapshot_markdown(snapshot: dict[str, Any]) -> str:
    metadata = snapshot.get("metadata") if isinstance(snapshot.get("metadata"), dict) else {}
    parameters = snapshot.get("parameters") if isinstance(snapshot.get("parameters"), dict) else {}
    weights = parameters.get("weights") if isinstance(parameters.get("weights"), dict) else {}
    lines = [
        f"# {snapshot.get('report_type', 'parameter_snapshot')}",
        "",
        f"- version：`{parameters.get('version', 'UNKNOWN')}`",
        f"- source run：`{metadata.get('run_id', 'UNKNOWN')}`",
        "- production_effect：`none`",
        "- manual_review_required：`true`",
        "- auto_promotion：`false`",
        "",
        "## Weights",
        "",
        "| Parameter | Weight |",
        "|---|---:|",
    ]
    for key, value in sorted(weights.items()):
        lines.append(f"| `{key}` | {float(value):.4f} |")
    return "\n".join(lines).rstrip() + "\n"


def _window_status(
    baseline_result: PortfolioSimulationResult,
    candidate_result: PortfolioSimulationResult,
) -> str:
    baseline = baseline_result.metrics
    candidate = candidate_result.metrics
    if (
        candidate.annualized_return >= baseline.annualized_return
        and candidate.max_drawdown >= baseline.max_drawdown
    ):
        return "PASS"
    if (
        candidate.sharpe_ratio > baseline.sharpe_ratio
        and candidate.turnover <= baseline.turnover * 1.2
    ):
        return "WATCH"
    return "FAIL"


def _window_objective(
    baseline_result: PortfolioSimulationResult,
    candidate_result: PortfolioSimulationResult,
) -> float:
    baseline = baseline_result.metrics
    candidate = candidate_result.metrics
    return (
        candidate.annualized_return
        - baseline.annualized_return
        + 0.05 * (candidate.sharpe_ratio - baseline.sharpe_ratio)
        + 0.5 * (candidate.max_drawdown - baseline.max_drawdown)
        - 0.01 * max(0.0, candidate.turnover - baseline.turnover)
    )


def _parameter_changes(
    *,
    baseline: ProductionParameters,
    candidate_weights: dict[str, float],
    candidate_result: PortfolioSimulationResult,
    baseline_result: PortfolioSimulationResult,
    windows: tuple[dict[str, Any], ...],
) -> tuple[ParameterChange, ...]:
    improved_metrics, worsened_metrics = _metric_direction(
        baseline_result.metrics.to_dict(),
        candidate_result.metrics.to_dict(),
    )
    passing_windows = tuple(
        str(window.get("window_id")) for window in windows if str(window.get("status")) == "PASS"
    )
    reasons: dict[str, str] = {}
    source_windows: dict[str, tuple[str, ...]] = {}
    improved: dict[str, tuple[str, ...]] = {}
    worsened: dict[str, tuple[str, ...]] = {}
    annualized_delta = (
        candidate_result.metrics.annualized_return - baseline_result.metrics.annualized_return
    )
    sharpe_delta = candidate_result.metrics.sharpe_ratio - baseline_result.metrics.sharpe_ratio
    for key, candidate_value in candidate_weights.items():
        baseline_value = baseline.weights.get(key, 0.0)
        if abs(candidate_value - baseline_value) <= 1e-12:
            continue
        reasons[key] = (
            "Bounded grid search selected this change after walk-forward validation; "
            f"passing_windows={len(passing_windows)}, "
            f"annualized_return_delta={annualized_delta:.4f}, "
            f"sharpe_delta={sharpe_delta:.4f}."
        )
        source_windows[key] = passing_windows
        improved[key] = tuple(improved_metrics)
        worsened[key] = tuple(worsened_metrics)
    return diff_parameters(
        baseline,
        candidate_weights,
        reasons=reasons,
        source_windows=source_windows,
        improved_metrics=improved,
        worsened_metrics=worsened,
    )


def _metric_direction(
    baseline: dict[str, float],
    candidate: dict[str, float],
) -> tuple[list[str], list[str]]:
    improved: list[str] = []
    worsened: list[str] = []
    for key in ("annualized_return", "sharpe_ratio", "sortino_ratio", "calmar_ratio"):
        _append_metric_direction(
            key,
            baseline.get(key, 0.0),
            candidate.get(key, 0.0),
            improved,
            worsened,
        )
    _append_metric_direction(
        "max_drawdown",
        baseline.get("max_drawdown", 0.0),
        candidate.get("max_drawdown", 0.0),
        improved,
        worsened,
    )
    _append_metric_direction(
        "turnover",
        candidate.get("turnover", 0.0),
        baseline.get("turnover", 0.0),
        improved,
        worsened,
    )
    return improved, worsened


def _append_metric_direction(
    key: str,
    baseline_value: float,
    candidate_value: float,
    improved: list[str],
    worsened: list[str],
) -> None:
    if candidate_value > baseline_value + 1e-12:
        improved.append(key)
    elif candidate_value < baseline_value - 1e-12:
        worsened.append(key)


def _shadow_data_quality_status(
    *,
    config: ShadowBacktestConfig,
    baseline: ProductionParameters,
    prices: pd.DataFrame,
    as_of: date,
    data_quality_report: DataQualityReport,
) -> tuple[DataQualityStatus, list[str]]:
    warnings: list[str] = []
    if not data_quality_report.passed:
        return "FAILED", [f"validate-data failed: {data_quality_report.error_count} errors"]
    required = [asset for asset in baseline.flattened_asset_universe() if asset != "CASH"]
    if prices.empty or not {"date", "ticker", "adj_close"}.issubset(prices.columns):
        return "FAILED", ["prices_daily.csv missing required date/ticker/adj_close columns"]
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[frame["_date"].dt.date <= as_of]
    available = set(frame["ticker"].astype(str).unique())
    missing_assets = sorted(asset for asset in required if asset not in available)
    if missing_assets:
        return "FAILED", [f"missing required price assets: {', '.join(missing_assets)}"]
    history_days = len(_trading_dates(prices, baseline, as_of))
    if history_days < config.walk_forward.min_history_days:
        return (
            "INSUFFICIENT_DATA",
            [
                f"history_days={history_days} below min_history_days="
                f"{config.walk_forward.min_history_days}"
            ],
        )
    status: DataQualityStatus = "OK"
    if data_quality_report.warning_count:
        status = "LIMITED"
        warnings.append(f"validate-data produced {data_quality_report.warning_count} warnings")
    return status, warnings


def _merge_backtest_input_diagnostic_status(
    status: DataQualityStatus,
    warnings: list[str],
    diagnostic_payload: dict[str, Any],
) -> tuple[DataQualityStatus, list[str]]:
    summary = diagnostic_payload.get("summary")
    if not isinstance(summary, dict):
        return status, warnings
    diagnostic_status = str(summary.get("overall_status") or "UNKNOWN")
    blocking_errors = int(summary.get("blocking_errors") or 0)
    diagnostic_warnings = int(summary.get("warnings") or 0)
    merged_warnings = list(warnings)
    if diagnostic_status == "FAILED":
        merged_warnings.append(f"backtest input diagnostics failed: {blocking_errors} blockers")
        if status == "INSUFFICIENT_DATA":
            return status, merged_warnings
        return "FAILED", merged_warnings
    if diagnostic_status == "LIMITED" and status == "OK":
        merged_warnings.append(
            f"backtest input diagnostics limited: {diagnostic_warnings} warnings"
        )
        return "LIMITED", merged_warnings
    return status, merged_warnings


def _diagnostic_blocking_errors(payload: dict[str, Any]) -> int:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return 0
    try:
        return int(summary.get("blocking_errors") or 0)
    except (TypeError, ValueError):
        return 0


def _diagnostic_blocking_reasons(payload: dict[str, Any]) -> list[str]:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return []
    reasons = summary.get("blocking_reasons")
    if not isinstance(reasons, list):
        return []
    return [str(reason) for reason in reasons if str(reason)]


def _diagnostic_overall_status(payload: dict[str, Any]) -> str:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return "UNKNOWN"
    return str(summary.get("overall_status") or "UNKNOWN")


def _diagnostic_backtest_mode(payload: dict[str, Any]) -> str:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return "blocked"
    return str(summary.get("backtest_mode") or "blocked")


def _diagnostic_check_status(payload: dict[str, Any], check_name: str) -> str:
    checks = payload.get("checks")
    if not isinstance(checks, dict):
        return "UNKNOWN"
    check = checks.get(check_name)
    if not isinstance(check, dict):
        return "UNKNOWN"
    return str(check.get("status") or "UNKNOWN")


def _diagnostic_can_run_shadow_backtest(payload: dict[str, Any]) -> bool:
    summary = payload.get("summary")
    return bool(summary.get("can_run_shadow_backtest")) if isinstance(summary, dict) else False


def _diagnostic_can_promote_candidate(payload: dict[str, Any]) -> bool:
    summary = payload.get("summary")
    return bool(summary.get("can_promote_candidate")) if isinstance(summary, dict) else False


def _signal_snapshot_path_from_diagnostics(payload: dict[str, Any]) -> Path | None:
    checks = payload.get("checks")
    if not isinstance(checks, dict):
        return None
    signal_snapshots = checks.get("signal_snapshots")
    if not isinstance(signal_snapshots, dict):
        return None
    files = signal_snapshots.get("snapshot_files")
    if not isinstance(files, list):
        return None
    for file_path in files:
        path = Path(str(file_path))
        if path.name == "signal_snapshot.json" and path.exists():
            return path
    for file_path in files:
        path = Path(str(file_path))
        if path.suffix.lower() == ".json" and path.exists():
            return path
    return None


def _score_calculation_payload(
    *,
    backtest_mode: str,
    weights: dict[str, float],
    signal_snapshot_payload: dict[str, Any],
    signal_snapshot_path: str,
) -> dict[str, Any]:
    from ai_trading_system.trading_engine.signal_snapshots import (
        PRICE_DERIVED_SIGNALS,
        signal_snapshot_summary,
    )

    summary = signal_snapshot_summary(signal_snapshot_payload) if signal_snapshot_payload else {}
    fallback_signals = [
        *_strings(summary.get("proxy_signals")),
        *_strings(summary.get("neutral_fallback_signals")),
    ]
    return {
        "mode": backtest_mode,
        "weights": dict(weights),
        "signal_snapshot": signal_snapshot_path,
        "signal_snapshot_status": summary.get(
            "status",
            "MISSING" if backtest_mode == "price_only_shadow_backtest" else "UNKNOWN",
        ),
        "fallback_signals": sorted(dict.fromkeys(fallback_signals)),
        "price_derived_signals": list(PRICE_DERIVED_SIGNALS),
        "real_signals": _strings(summary.get("real_signals")),
        "proxy_signals": _strings(summary.get("proxy_signals")),
        "neutral_fallback_signals": _strings(summary.get("neutral_fallback_signals")),
    }


def _promotion_constraints_for_mode(backtest_mode: str) -> dict[str, object]:
    if backtest_mode == "price_only_shadow_backtest":
        return {
            "max_promotion_status": "rejected",
            "allow_candidate": False,
            "allow_production_promotion": False,
            "manual_review_required": True,
            "reason": "signal_snapshot_missing",
        }
    if backtest_mode == "full_signal_backtest_limited":
        return {
            "max_promotion_status": "watch",
            "allow_candidate": False,
            "allow_production_promotion": False,
            "manual_review_required": True,
            "reason": "signal_quality_limited",
        }
    if backtest_mode == "blocked":
        return {
            "max_promotion_status": "rejected",
            "allow_candidate": False,
            "allow_production_promotion": False,
            "manual_review_required": True,
            "reason": "backtest_input_quality_blocked",
        }
    return {
        "max_promotion_status": "candidate",
        "allow_candidate": True,
        "allow_production_promotion": False,
        "manual_review_required": True,
        "reason": "observe_only_shadow_backtest",
    }


def _apply_backtest_mode_promotion_constraints(
    decision: PromotionDecision,
    *,
    backtest_mode: str,
) -> PromotionDecision:
    if backtest_mode == "full_signal_backtest":
        return decision
    if backtest_mode == "full_signal_backtest_limited":
        review_items = tuple(
            dict.fromkeys(
                [
                    *decision.manual_review_items,
                    "full_signal_backtest_limited_signal_quality_limited",
                ]
            )
        )
        if decision.status == "rejected":
            reason = (
                "Full-signal-limited backtest completed, but original promotion decision "
                f"was rejected: {decision.reason}. Signal quality is limited, so "
                "candidate promotion remains disabled until signal snapshots reach OK."
            )
            return PromotionDecision(
                status="rejected",
                reason=reason,
                hard_rejections=decision.hard_rejections,
                manual_review_items=review_items,
                criteria_results=decision.criteria_results,
            )
        return PromotionDecision(
            status="watch",
            reason=(
                "Signal snapshot is available in limited mode. Full-signal-limited "
                "shadow backtest may be reviewed, but candidate promotion is disabled "
                "until signal quality reaches OK."
            ),
            hard_rejections=decision.hard_rejections,
            manual_review_items=review_items,
            criteria_results=decision.criteria_results,
        )
    if backtest_mode == "blocked":
        review_items = tuple(
            dict.fromkeys([*decision.manual_review_items, "backtest_input_quality_blocked"])
        )
        return PromotionDecision(
            status="rejected",
            reason=(
                "Shadow backtest input quality is blocked. Candidate promotion is rejected "
                "until blocking data quality issues are repaired."
            ),
            hard_rejections=decision.hard_rejections,
            manual_review_items=review_items,
            criteria_results=decision.criteria_results,
        )
    review_items = tuple(
        dict.fromkeys(
            [
                *decision.manual_review_items,
                "price_only_shadow_backtest_signal_snapshot_missing",
            ]
        )
    )
    reason = (
        "Price-only shadow backtest completed, but signal snapshot is missing. "
        "Candidate promotion is rejected until full signal inputs are available."
    )
    if decision.status == "rejected":
        reason = (
            "Price-only backtest completed but cannot be promoted because signal snapshot "
            f"is missing. Original promotion decision was rejected: {decision.reason}"
        )
    return PromotionDecision(
        status="rejected",
        reason=reason,
        hard_rejections=decision.hard_rejections,
        manual_review_items=review_items,
        criteria_results=decision.criteria_results,
    )


def _trading_dates(
    prices: pd.DataFrame,
    baseline: ProductionParameters,
    as_of: date,
) -> tuple[date, ...]:
    if prices.empty or "date" not in prices.columns:
        return ()
    required_assets = [asset for asset in baseline.flattened_asset_universe() if asset != "CASH"]
    frame = prices.loc[prices["ticker"].astype(str).isin(required_assets)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & (frame["_date"].dt.date <= as_of)]
    counts = frame.groupby(frame["_date"].dt.date)["ticker"].nunique()
    required_count = len(required_assets)
    return tuple(sorted(counts.loc[counts >= required_count].index))


def _read_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (OSError, pd.errors.ParserError):
        return pd.DataFrame()


def _latest_price_date(prices: pd.DataFrame) -> date | None:
    if prices.empty or "date" not in prices.columns:
        return None
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).date()


def _full_result_start(
    windows: tuple[WalkForwardWindow, ...],
    trading_dates: tuple[date, ...],
    config: ShadowBacktestConfig,
) -> date:
    if windows:
        return max(windows[0].validation_start, config.market_regime.default_backtest_start)
    if trading_dates:
        return max(trading_dates[0], config.market_regime.default_backtest_start)
    return config.market_regime.default_backtest_start


def _relative_comparison(
    baseline: dict[str, float],
    candidate: dict[str, float],
) -> dict[str, float]:
    keys = ("annualized_return", "max_drawdown", "sharpe_ratio", "turnover")
    return {f"{key}_delta": candidate.get(key, 0.0) - baseline.get(key, 0.0) for key in keys}


def _overfitting_risk(passing_ratio: float, window_count: int) -> str:
    if window_count <= 1 or passing_ratio < 0.4:
        return "HIGH"
    if passing_ratio < 0.6:
        return "MEDIUM"
    return "LOW"


def _missing_required_input_artifacts(input_artifacts: dict[str, str]) -> bool:
    for key, value in input_artifacts.items():
        if key == "signal_snapshot":
            continue
        path = Path(value)
        if key.endswith("_report"):
            continue
        if not path.exists():
            return True
    return False


def _date_range_from_rows(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return f"{rows[0].get('date')}..{rows[-1].get('date')}"


def _output_roots(config: ShadowBacktestConfig, *, dry_run: bool) -> dict[str, Path]:
    if dry_run:
        root = PROJECT_ROOT / "outputs" / "dry_runs" / "shadow_backtest"
        return {
            "shadow_backtest_dir": root / "shadow_backtest",
            "shadow_parameters_dir": root / "shadow_parameters",
            "candidate_parameters_dir": root / "candidate_parameters",
            "parameter_promotion_dir": root / "parameter_promotion",
            "report_alias_dir": root / "reports",
            "data_quality_report_dir": root / "reports",
        }
    return {
        "shadow_backtest_dir": resolve_project_path(config.output.shadow_backtest_dir),
        "shadow_parameters_dir": resolve_project_path(config.output.shadow_parameters_dir),
        "candidate_parameters_dir": resolve_project_path(config.output.candidate_parameters_dir),
        "parameter_promotion_dir": resolve_project_path(config.output.parameter_promotion_dir),
        "report_alias_dir": resolve_project_path(config.output.report_alias_dir),
        "data_quality_report_dir": resolve_project_path(config.data.data_quality_report_dir),
    }


def _config_hash(config_path: Path, input_artifacts: dict[str, str]) -> str:
    digest = sha256()
    for path_text in [str(config_path), *input_artifacts.values()]:
        path = Path(path_text)
        if path.exists() and path.is_file():
            digest.update(str(path).encode("utf-8"))
            digest.update(sha256_file(path).encode("utf-8"))
    return digest.hexdigest()


def _day_label(value: int) -> str:
    return "day" if value == 1 else "days"


def _is_are(value: int) -> str:
    return "is" if value == 1 else "are"


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []
