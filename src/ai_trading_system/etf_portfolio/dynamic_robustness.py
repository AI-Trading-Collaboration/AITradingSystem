from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from hashlib import sha256
from math import prod, sqrt
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Literal, Self

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import calculate_portfolio_accounting_step
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DynamicAllocationPolicyConfig,
    build_dynamic_allocation_decision_record,
    load_dynamic_allocation_policy_config,
    select_dynamic_regime_state,
)
from ai_trading_system.etf_portfolio.models import (
    ETFConfigBundle,
    PolicyMetadata,
    load_etf_config_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DYNAMIC_ROBUSTNESS_POLICY_SCHEMA_VERSION = "etf_dynamic_robustness_policy_v1"
DYNAMIC_ROBUSTNESS_REPORT_SCHEMA_VERSION = "etf_dynamic_robustness_report_v1"
DYNAMIC_ROBUSTNESS_VALIDATION_SCHEMA_VERSION = "etf_dynamic_robustness_validation_v1"

DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_robustness.yaml"
)
DEFAULT_DYNAMIC_ROBUSTNESS_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_robustness"
)
DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR = DEFAULT_DYNAMIC_ROBUSTNESS_ROOT / "reports"
DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR = DEFAULT_DYNAMIC_ROBUSTNESS_ROOT / "validation"

DYNAMIC_ROBUSTNESS_SAFETY: dict[str, Any] = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_enrollment_without_owner_approval": False,
}
FORBIDDEN_DYNAMIC_ROBUSTNESS_KEYS = {
    "broker_order",
    "production_weight_update",
    "baseline_config_mutation",
    "official_target_weights_write",
    "automatic_candidate_promotion",
    "auto_enrollment_without_owner_approval",
}


class DynamicRobustnessError(RuntimeError):
    """Raised when dynamic robustness inputs or outputs are invalid."""


class DynamicRobustnessMarketRegime(BaseModel):
    regime_id: str = Field(min_length=1)
    anchor_event: str = Field(min_length=1)
    anchor_date: date
    default_backtest_start: date

    @model_validator(mode="after")
    def validate_ai_regime(self) -> Self:
        if self.regime_id != "ai_after_chatgpt":
            raise ValueError("TRADING-086 default market regime must be ai_after_chatgpt")
        if self.default_backtest_start < date(2022, 12, 1):
            raise ValueError("dynamic robustness default backtest start cannot predate 2022-12-01")
        return self


class DynamicRobustnessSafety(BaseModel):
    observe_only: bool
    candidate_only: bool
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    manual_review_required: bool
    production_state_mutated: bool
    baseline_config_mutated: bool
    official_target_weights_mutated: bool
    automatic_candidate_promotion: bool
    auto_enrollment_without_owner_approval: bool

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        if self.model_dump(mode="json") != DYNAMIC_ROBUSTNESS_SAFETY:
            raise ValueError("dynamic robustness safety fields are unsafe")
        return self


class DynamicRobustnessPriceBacktestConfig(BaseModel):
    price_field: Literal["adj_close", "close"]
    warmup_days: int = Field(ge=30)
    signal_lag_days: int = Field(ge=1)
    primary_benchmark_symbol: str = Field(min_length=1)
    required_symbols: list[str] = Field(min_length=1)
    comparison_order: list[str] = Field(min_length=1)
    best_static_historical_source_glob: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_symbols(self) -> Self:
        required = {"SPY", "QQQ", "SMH", "SOXX", "CASH"}
        missing = required - set(self.required_symbols)
        if missing:
            raise ValueError(
                "dynamic robustness required_symbols missing: " + ", ".join(sorted(missing))
            )
        return self


class DynamicRobustnessScoreModelConfig(BaseModel):
    short_momentum_window: int = Field(gt=1)
    medium_momentum_window: int = Field(gt=1)
    long_momentum_window: int = Field(gt=1)
    volatility_window: int = Field(gt=1)
    drawdown_window: int = Field(gt=1)
    relative_strength_window: int = Field(gt=1)
    return_score_floor: float
    return_score_ceiling: float
    relative_strength_floor: float
    relative_strength_ceiling: float
    volatility_low: float = Field(gt=0)
    volatility_high: float = Field(gt=0)
    drawdown_low: float = Field(le=0)
    drawdown_high: float = Field(le=0)
    event_risk_volatility_weight: float = Field(ge=0, le=1)
    event_risk_drawdown_weight: float = Field(ge=0, le=1)
    event_risk_negative_trend_weight: float = Field(ge=0, le=1)
    event_risk_trend_penalty: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_bands(self) -> Self:
        if self.return_score_floor >= self.return_score_ceiling:
            raise ValueError("return_score_floor must be below return_score_ceiling")
        if self.relative_strength_floor >= self.relative_strength_ceiling:
            raise ValueError(
                "relative_strength_floor must be below relative_strength_ceiling"
            )
        if self.volatility_low >= self.volatility_high:
            raise ValueError("volatility_low must be below volatility_high")
        if self.drawdown_low <= self.drawdown_high:
            raise ValueError("drawdown_low must be less severe than drawdown_high")
        total = (
            self.event_risk_volatility_weight
            + self.event_risk_drawdown_weight
            + self.event_risk_negative_trend_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError("event risk component weights must sum to 1.0")
        return self


class DynamicRobustnessFalseSignalConfig(BaseModel):
    forward_horizon_days: int = Field(gt=1)
    market_up_threshold: float = Field(gt=0)
    forward_drawdown_threshold: float = Field(le=0)
    cash_overweight_threshold: float = Field(gt=0)
    growth_underweight_threshold: float = Field(le=0)
    growth_overweight_threshold: float = Field(gt=0)
    benchmark_symbol: str = Field(min_length=1)


class DynamicRobustnessWalkForwardConfig(BaseModel):
    window_days: int = Field(gt=10)
    step_days: int = Field(gt=0)
    min_windows: int = Field(gt=0)
    min_pass_ratio: float = Field(ge=0, le=1)
    min_sharpe: float
    max_drawdown_floor: float = Field(le=0)


class DynamicRobustnessSensitivityConfig(BaseModel):
    score_change_thresholds: list[float] = Field(min_length=1)
    minimum_holding_days: list[int] = Field(min_length=1)
    regime_confirmation_days: list[int] = Field(min_length=1)
    weekly_turnover_caps: list[float] = Field(min_length=1)


class DynamicRobustnessOverfitConfig(BaseModel):
    max_single_window_return_share: float = Field(ge=0, le=1)
    min_positive_walk_forward_ratio: float = Field(ge=0, le=1)
    max_parameter_sensitivity_return_range: float = Field(ge=0)
    max_regime_return_concentration: float = Field(ge=0, le=1)
    max_turnover: float = Field(ge=0)
    max_allocation_path_instability: float = Field(ge=0)


class DynamicRobustnessPolicyConfig(BaseModel):
    schema_version: Literal["etf_dynamic_robustness_policy_v1"]
    policy_metadata: PolicyMetadata
    market_regime: DynamicRobustnessMarketRegime
    safety: DynamicRobustnessSafety
    price_backtest: DynamicRobustnessPriceBacktestConfig
    score_model: DynamicRobustnessScoreModelConfig
    false_signal: DynamicRobustnessFalseSignalConfig
    walk_forward: DynamicRobustnessWalkForwardConfig
    sensitivity: DynamicRobustnessSensitivityConfig
    overfit: DynamicRobustnessOverfitConfig


def load_dynamic_robustness_policy_config(
    path: Path = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
) -> DynamicRobustnessPolicyConfig:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise DynamicRobustnessError("dynamic robustness policy must be a mapping")
    try:
        return DynamicRobustnessPolicyConfig.model_validate(raw)
    except Exception as exc:  # noqa: BLE001
        raise DynamicRobustnessError(f"invalid dynamic robustness policy: {exc}") from exc


def build_dynamic_robustness_report(
    *,
    prices: pd.DataFrame,
    etf_config: ETFConfigBundle,
    policy: DynamicRobustnessPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    candidate_id: str | None = None,
    dynamic_calibration_report: Mapping[str, Any] | None = None,
    dynamic_calibration_report_path: Path | None = None,
    start: date | None = None,
    end: date | None = None,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    prices_path: Path | None = None,
) -> dict[str, Any]:
    candidate = _resolve_dynamic_candidate(
        candidate_id=candidate_id,
        dynamic_calibration_report=dynamic_calibration_report,
    )
    resolved_candidate_id = candidate["candidate_id"]
    pivot = _price_pivot(prices, policy)
    trading_dates = list(pivot.index)
    requested_start = start or policy.market_regime.default_backtest_start
    requested_end = end or trading_dates[-1].date()
    signal_indices = _signal_indices(
        trading_dates,
        requested_start=requested_start,
        requested_end=requested_end,
        policy=policy,
    )
    if not signal_indices:
        raise DynamicRobustnessError("dynamic robustness backtest has no valid signal dates")

    dynamic_daily = _build_dynamic_daily_path(
        pivot=pivot,
        signal_indices=signal_indices,
        etf_config=etf_config,
        policy=policy,
        dynamic_policy=dynamic_policy,
        candidate_id=resolved_candidate_id,
        source_calibration_report=dynamic_calibration_report_path,
        data_quality_status=data_quality_status,
    )
    static_base_weights = _normalise_weights(dynamic_policy.base_weights)
    current_baseline_weights = _current_baseline_weights(etf_config)
    best_static = _load_best_static_historical_candidate(policy)
    comparison_inputs = [
        ("dynamic_candidate", "dynamic", None, dynamic_daily),
        ("static_base_candidate", "static_portfolio", static_base_weights, None),
        ("current_etf_baseline", "static_portfolio", current_baseline_weights, None),
        ("QQQ_buy_and_hold", "buy_and_hold", {"QQQ": 1.0}, None),
        ("SPY_buy_and_hold", "buy_and_hold", {"SPY": 1.0}, None),
        ("SMH_buy_and_hold", "buy_and_hold", {"SMH": 1.0}, None),
    ]
    comparison_frames: dict[str, pd.DataFrame] = {"dynamic_candidate": dynamic_daily}
    comparison_rows: list[dict[str, Any]] = []
    for comparison_id, row_type, weights, frame in comparison_inputs:
        daily = (
            frame
            if frame is not None
            else _build_static_daily_path(
                pivot=pivot,
                signal_indices=signal_indices,
                etf_config=etf_config,
                policy=policy,
                weights=_normalise_weights(weights or {}),
                comparison_id=comparison_id,
                row_type=row_type,
                data_quality_status=data_quality_status,
            )
        )
        comparison_frames[comparison_id] = daily
        comparison_rows.append(
            _comparison_row(
                comparison_id=comparison_id,
                row_type=row_type,
                daily=daily,
                benchmark_daily=_benchmark_daily(comparison_frames, "QQQ_buy_and_hold"),
                source_status="AVAILABLE",
                source_path="",
            )
        )
    if best_static["weights"]:
        best_daily = _build_static_daily_path(
            pivot=pivot,
            signal_indices=signal_indices,
            etf_config=etf_config,
            policy=policy,
            weights=_normalise_weights(_mapping(best_static["weights"])),
            comparison_id="best_static_historical_candidate",
            row_type="static_portfolio",
            data_quality_status=data_quality_status,
        )
        comparison_frames["best_static_historical_candidate"] = best_daily
        comparison_rows.append(
            _comparison_row(
                comparison_id="best_static_historical_candidate",
                row_type="static_portfolio",
                daily=best_daily,
                benchmark_daily=_benchmark_daily(comparison_frames, "QQQ_buy_and_hold"),
                source_status=_text(best_static["source_status"], "AVAILABLE"),
                source_path=_text(best_static["source_path"]),
                source_candidate_id=_text(best_static["candidate_id"]),
            )
        )
    else:
        comparison_rows.append(_missing_best_static_row(best_static))

    comparison_rows = _ordered_comparison_rows(comparison_rows, policy)
    static_base_daily = comparison_frames["static_base_candidate"]
    qqq_daily = comparison_frames["QQQ_buy_and_hold"]
    walk_forward = _walk_forward_review(dynamic_daily, policy)
    regime_attribution = _regime_attribution(dynamic_daily, static_base_daily)
    false_signal = _false_signal_diagnostics(
        dynamic_daily=dynamic_daily,
        static_base_daily=static_base_daily,
        benchmark_daily=qqq_daily,
        static_base_weights=static_base_weights,
        policy=policy,
    )
    sensitivity = _turnover_sensitivity(
        pivot=pivot,
        signal_indices=signal_indices,
        etf_config=etf_config,
        policy=policy,
        dynamic_policy=dynamic_policy,
        candidate_id=resolved_candidate_id,
        data_quality_status=data_quality_status,
    )
    ai_semi = _ai_semiconductor_attribution(dynamic_daily)
    event_risk = _event_risk_overlay_attribution(dynamic_daily, static_base_daily, dynamic_policy)
    overfit = _dynamic_overfit_diagnostics(
        dynamic_daily=dynamic_daily,
        walk_forward=walk_forward,
        regime_attribution=regime_attribution,
        sensitivity=sensitivity,
        policy=policy,
    )
    dynamic_row = next(
        row for row in comparison_rows if row["comparison_id"] == "dynamic_candidate"
    )
    static_row = next(
        row for row in comparison_rows if row["comparison_id"] == "static_base_candidate"
    )
    report_id = _stable_id(
        "dynamic-robustness-report",
        resolved_candidate_id,
        requested_start.isoformat(),
        requested_end.isoformat(),
        _stable_hash(dynamic_row),
    )
    summary = {
        "status": _robustness_status(dynamic_row, static_row, overfit),
        "dynamic_candidate_id": resolved_candidate_id,
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "effective_start": str(dynamic_daily.iloc[0]["signal_date"]),
        "effective_end": str(dynamic_daily.iloc[-1]["signal_date"]),
        "market_regime": policy.market_regime.regime_id,
        "data_quality_status": data_quality_status,
        "dynamic_total_return": dynamic_row["total_return"],
        "dynamic_cagr": dynamic_row["CAGR"],
        "dynamic_max_drawdown": dynamic_row["max_drawdown"],
        "excess_vs_static_base": dynamic_row["total_return"] - static_row["total_return"],
        "false_risk_off_count": false_signal["false_risk_off"]["event_count"],
        "false_risk_on_count": false_signal["false_risk_on"]["event_count"],
        "overfit_status": overfit["status"],
        "shadow_enrollment_allowed": False,
        "handoff_task": "TRADING-087",
    }
    payload = {
        "schema_version": DYNAMIC_ROBUSTNESS_REPORT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_robustness_report",
        "dynamic_robustness_report_id": report_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": summary["status"],
        "policy_version": policy.policy_metadata.version,
        "policy_config_hash": _stable_hash(policy.model_dump(mode="json")),
        "dynamic_allocation_policy_id": dynamic_policy.default_policy_id,
        "dynamic_allocation_policy_hash": _stable_hash(dynamic_policy.model_dump(mode="json")),
        "market_regime": policy.market_regime.model_dump(mode="json"),
        "candidate_context": candidate,
        "summary": summary,
        "comparison_table": comparison_rows,
        "walk_forward": walk_forward,
        "regime_attribution": regime_attribution,
        "false_signal_diagnostics": false_signal,
        "turnover_sensitivity": sensitivity,
        "ai_semiconductor_attribution": ai_semi,
        "event_risk_overlay_attribution": event_risk,
        "overfit_diagnostics": overfit,
        "daily_path_summary": _daily_path_summary(dynamic_daily),
        "comparison_daily_paths": {
            comparison_id: frame.to_dict(orient="records")
            for comparison_id, frame in comparison_frames.items()
        },
        "source_artifacts": {
            "prices_path": "" if prices_path is None else str(prices_path),
            "data_quality_report": data_quality_report,
            "dynamic_calibration_report": (
                ""
                if dynamic_calibration_report_path is None
                else str(dynamic_calibration_report_path)
            ),
            "best_static_historical_source": _text(best_static["source_path"]),
        },
        "validation_context": {
            "validate_data_status": data_quality_status,
            "validate_data_report": data_quality_report,
            "no_lookahead_timing": "signal_date < execution_date < return_date",
            "price_field": policy.price_backtest.price_field,
        },
        "safety": policy.safety.model_dump(mode="json"),
        **DYNAMIC_ROBUSTNESS_SAFETY,
        "commands_executed": False,
        "shadow_enrollment_allowed": False,
    }
    _assert_dynamic_robustness_payload_safe(payload)
    return payload


def build_dynamic_robustness_validation_report(
    *,
    policy_config_path: Path = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_policy_path: Path | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    policy: DynamicRobustnessPolicyConfig | None = None
    try:
        policy = load_dynamic_robustness_policy_config(policy_config_path)
        _append_check(checks, "policy_config_valid", True, "dynamic robustness policy loads")
    except Exception as exc:  # noqa: BLE001
        _append_check(checks, "policy_config_valid", False, str(exc))

    sample_report: dict[str, Any] | None = None
    if policy is not None:
        try:
            etf_config = load_etf_config_bundle()
            dynamic_policy = load_dynamic_allocation_policy_config(
                dynamic_policy_path
                if dynamic_policy_path is not None
                else PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_allocation_policy.yaml"
            )
            sample_report = build_dynamic_robustness_report(
                prices=_synthetic_validation_prices(policy),
                etf_config=etf_config,
                policy=policy,
                dynamic_policy=dynamic_policy,
                candidate_id="validation_dynamic_candidate",
                dynamic_calibration_report=None,
                data_quality_status="VALIDATION_SAMPLE",
                data_quality_report="validation_sample",
            )
            _append_check(
                checks,
                "price_driven_backtest_available",
                True,
                "built validation sample dynamic robustness report",
            )
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "price_driven_backtest_available", False, str(exc))

    if sample_report is not None:
        comparison_ids = {
            str(row.get("comparison_id")) for row in _records(sample_report.get("comparison_table"))
        }
        required = {
            "dynamic_candidate",
            "static_base_candidate",
            "current_etf_baseline",
            "QQQ_buy_and_hold",
            "SPY_buy_and_hold",
            "SMH_buy_and_hold",
            "best_static_historical_candidate",
        }
        _append_check(
            checks,
            "required_comparisons_visible",
            required.issubset(comparison_ids),
            "dynamic/static/current/QQQ/SPY/SMH/best-static comparison ids are present",
        )
        _append_check(
            checks,
            "false_signal_diagnostics_visible",
            bool(sample_report.get("false_signal_diagnostics")),
            "false risk-on/off diagnostics are present",
        )
        _append_check(
            checks,
            "overfit_diagnostics_visible",
            bool(sample_report.get("overfit_diagnostics")),
            "dynamic overfit diagnostics are present",
        )
        try:
            _assert_dynamic_robustness_payload_safe(sample_report)
            _append_check(checks, "sample_report_safety", True, "sample report safety holds")
        except Exception as exc:  # noqa: BLE001
            _append_check(checks, "sample_report_safety", False, str(exc))

    registry_text = (PROJECT_ROOT / "config" / "report_registry.yaml").read_text(
        encoding="utf-8"
    )
    _append_check(
        checks,
        "report_registry_visibility",
        "etf_dynamic_robustness_report" in registry_text
        and "etf_dynamic_robustness_validation" in registry_text,
        "report registry includes TRADING-086 report and validation artifacts",
    )
    reader_brief_path = (
        PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "reader_brief.py"
    )
    reader_brief_text = reader_brief_path.read_text(encoding="utf-8")
    _append_check(
        checks,
        "reader_brief_visibility",
        "Dynamic Robustness Review" in reader_brief_text
        and "_etf_dynamic_robustness_summary" in reader_brief_text,
        "Reader Brief has Dynamic Robustness Review section",
    )
    failed = [check for check in checks if check["status"] != "PASS"]
    payload = {
        "schema_version": DYNAMIC_ROBUSTNESS_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_dynamic_robustness_validation",
        "validation_id": _stable_id(
            "dynamic-robustness-validation",
            datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
            _stable_hash([check["check_id"] for check in checks]),
        ),
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "PASS" if not failed else "FAIL",
        "check_count": len(checks),
        "failed_check_count": len(failed),
        "checks": checks,
        "source_schema_versions": {
            "policy": DYNAMIC_ROBUSTNESS_POLICY_SCHEMA_VERSION,
            "report": DYNAMIC_ROBUSTNESS_REPORT_SCHEMA_VERSION,
        },
        "official_target_weights_write_blocked": True,
        "automatic_candidate_promotion_blocked": True,
        "auto_enrollment_without_owner_approval_blocked": True,
        "safety": DYNAMIC_ROBUSTNESS_SAFETY,
        **DYNAMIC_ROBUSTNESS_SAFETY,
        "commands_executed": False,
    }
    _assert_dynamic_robustness_payload_safe(payload)
    return payload


def write_dynamic_robustness_report(
    payload: dict[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_id = str(payload["dynamic_robustness_report_id"])
    json_path = output_dir / f"{report_id}.json"
    markdown_path = output_dir / f"{report_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_robustness_report_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def write_dynamic_robustness_validation_report(
    payload: dict[str, Any],
    output_dir: Path = DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_id = str(payload["validation_id"])
    json_path = output_dir / f"{validation_id}.json"
    markdown_path = output_dir / f"{validation_id}.md"
    _write_json(payload, json_path)
    _write_text(render_dynamic_robustness_validation_markdown(payload), markdown_path)
    return {"json": json_path, "markdown": markdown_path}


def latest_dynamic_robustness_report_path(
    report_dir: Path = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
) -> Path | None:
    return _latest_json(report_dir, "dynamic-robustness-report_*.json")


def latest_dynamic_calibration_report_path(
    report_dir: Path | None = None,
) -> Path | None:
    root = report_dir or (
        PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_calibration" / "reports"
    )
    return _latest_json(root, "dynamic-calibration-report_*.json")


def load_latest_dynamic_calibration_report(
    report_path: Path | None = None,
) -> tuple[Path | None, dict[str, Any]]:
    resolved = report_path or latest_dynamic_calibration_report_path()
    if resolved is None or not resolved.exists():
        return None, {}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DynamicRobustnessError(
            f"invalid dynamic calibration report JSON: {resolved}"
        ) from exc
    if not isinstance(payload, dict):
        raise DynamicRobustnessError("dynamic calibration report must be a mapping")
    return resolved, payload


def render_dynamic_robustness_report_markdown(payload: dict[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Dynamic Robustness Report {payload.get('dynamic_robustness_report_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Candidate: {summary.get('dynamic_candidate_id')}",
        f"- Market Regime: {summary.get('market_regime')}",
        f"- Requested Range: {summary.get('requested_start')} 至 {summary.get('requested_end')}",
        f"- Effective Range: {summary.get('effective_start')} 至 {summary.get('effective_end')}",
        f"- Data Quality: {summary.get('data_quality_status')}",
        (
            "- Safety: observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action=none; manual_review_required=true"
        ),
        "",
        "## Comparison",
        "",
        (
            "| Comparison | Status | Total Return | CAGR | Max Drawdown | Sharpe | "
            "Turnover | Upside Capture | Downside Capture |"
        ),
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _records(payload.get("comparison_table")):
        lines.append(
            "| "
            f"{row.get('comparison_id')} | {row.get('status')} | "
            f"{_fmt_pct(row.get('total_return'))} | {_fmt_pct(row.get('CAGR'))} | "
            f"{_fmt_pct(row.get('max_drawdown'))} | {_fmt_num(row.get('Sharpe'))} | "
            f"{_fmt_num(row.get('turnover'))} | {_fmt_num(row.get('upside_capture'))} | "
            f"{_fmt_num(row.get('downside_capture'))} |"
        )
    false_signal = _mapping(payload.get("false_signal_diagnostics"))
    overfit = _mapping(payload.get("overfit_diagnostics"))
    false_off = _mapping(false_signal.get("false_risk_off"))
    false_on = _mapping(false_signal.get("false_risk_on"))
    lines.extend(
        [
            "",
            "## False Signal Diagnostics",
            "",
            f"- False Risk-Off Count: {false_off.get('event_count')}",
            (
                "- False Risk-Off Opportunity Cost: "
                f"{_fmt_pct(false_off.get('total_opportunity_cost'))}"
            ),
            f"- False Risk-On Count: {false_on.get('event_count')}",
            (
                "- False Risk-On Drawdown Cost: "
                f"{_fmt_pct(false_on.get('total_drawdown_cost'))}"
            ),
            "",
            "## Walk Forward",
            "",
            f"- Status: {_mapping(payload.get('walk_forward')).get('status')}",
            f"- Pass Ratio: {_fmt_num(_mapping(payload.get('walk_forward')).get('pass_ratio'))}",
            "",
            "## Overfit Diagnostics",
            "",
            f"- Status: {overfit.get('status')}",
            f"- Risk Level: {overfit.get('risk_level')}",
            f"- Shadow Enrollment Allowed: {str(payload.get('shadow_enrollment_allowed')).lower()}",
            "",
            "## Correctness Notes",
            "",
            (
                "- 本报告使用真实 ETF price return 构造 dynamic path，"
                "不使用 TRADING-085 calibration proxy 作为 robustness 结论。"
            ),
            "- Timing 固定为 signal_date < execution_date < return_date，避免未来函数。",
            (
                "- 本报告只作为 TRADING-087 owner-approved shadow review 输入，"
                "不写 production weights 或 broker state。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def render_dynamic_robustness_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Dynamic Robustness Validation {payload.get('validation_id')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Checks: {payload.get('check_count')}",
        f"- Failed: {payload.get('failed_check_count')}",
        (
            "- Safety: observe_only=true; candidate_only=true; "
            "production_effect=none; broker_action=none"
        ),
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | "
            f"{check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def _build_dynamic_daily_path(
    *,
    pivot: pd.DataFrame,
    signal_indices: list[int],
    etf_config: ETFConfigBundle,
    policy: DynamicRobustnessPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    candidate_id: str,
    source_calibration_report: Path | None,
    data_quality_status: str,
) -> pd.DataFrame:
    previous_weights = _normalise_weights(dynamic_policy.base_weights)
    previous_scores: dict[str, float] | None = None
    previous_regime = ""
    confirmed_regime_days = 0
    last_rebalance_signal_index = (
        signal_indices[0] - dynamic_policy.rebalance_policy.minimum_holding_days
    )
    rows: list[dict[str, Any]] = []
    equity = 1.0
    total_cost_bps = _total_cost_bps(etf_config)
    for signal_index in signal_indices:
        signal_date = pivot.index[signal_index].date()
        scores = _price_derived_scores(pivot, signal_index, policy)
        regime, _ = select_dynamic_regime_state(scores, dynamic_policy)
        confirmed_regime_days = confirmed_regime_days + 1 if regime == previous_regime else 1
        days_since_rebalance = signal_index - last_rebalance_signal_index
        decision = build_dynamic_allocation_decision_record(
            policy=dynamic_policy,
            decision_date=signal_date,
            input_scores=scores,
            previous_weights=previous_weights,
            previous_scores=previous_scores,
            days_since_last_rebalance=days_since_rebalance,
            confirmed_regime_days=confirmed_regime_days,
            source_trend_report=(
                "" if source_calibration_report is None else str(source_calibration_report)
            ),
            data_quality_status=data_quality_status,
        )
        target_weights = _normalise_weights(_mapping(decision["candidate_target_weights"]))
        execution_index = signal_index + policy.price_backtest.signal_lag_days
        return_index = execution_index + 1
        accounting = calculate_portfolio_accounting_step(
            pivot,
            signal_date=signal_date,
            execution_date=pivot.index[execution_index].date(),
            return_date=pivot.index[return_index].date(),
            target_weights=target_weights,
            previous_weights=previous_weights,
            asset_symbols=tuple(policy.price_backtest.required_symbols),
            total_cost_bps=total_cost_bps,
            starting_equity=equity,
        )
        equity = accounting.ending_equity
        rebalance_decision = _mapping(decision.get("rebalance_decision"))
        if rebalance_decision.get("decision") == "rebalance_candidate":
            last_rebalance_signal_index = signal_index
        rows.append(
            {
                "decision_id": decision["decision_id"],
                "candidate_id": candidate_id,
                "signal_date": signal_date.isoformat(),
                "execution_date": accounting.execution_date.isoformat(),
                "return_date": accounting.return_date.isoformat(),
                "dynamic_policy_id": decision["policy_id"],
                "selected_regime": regime,
                "rebalance_decision": rebalance_decision.get("decision"),
                "strategy_return": accounting.strategy_return,
                "gross_return": accounting.gross_return,
                "transaction_cost": accounting.transaction_cost,
                "turnover": accounting.turnover,
                "portfolio_equity": accounting.ending_equity,
                "input_scores_json": json.dumps(scores, ensure_ascii=False, sort_keys=True),
                "previous_weights_json": json.dumps(
                    previous_weights, ensure_ascii=False, sort_keys=True
                ),
                "target_weights_json": json.dumps(
                    target_weights, ensure_ascii=False, sort_keys=True
                ),
                "pre_rebalance_candidate_weights_json": json.dumps(
                    decision["pre_rebalance_candidate_weights"],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "trade_deltas_json": json.dumps(
                    decision["trade_deltas"], ensure_ascii=False, sort_keys=True
                ),
                "asset_returns_json": json.dumps(
                    accounting.period_returns, ensure_ascii=False, sort_keys=True
                ),
                "asset_contributions_json": json.dumps(
                    accounting.asset_contributions, ensure_ascii=False, sort_keys=True
                ),
                "constraints_applied_json": json.dumps(
                    decision["constraints_applied"], ensure_ascii=False, sort_keys=True
                ),
                "constraint_diagnostics_json": json.dumps(
                    decision["constraint_diagnostics"], ensure_ascii=False, sort_keys=True
                ),
                "rebalance_decision_json": json.dumps(
                    rebalance_decision, ensure_ascii=False, sort_keys=True
                ),
                "reason_codes_json": json.dumps(
                    decision.get("reason_codes", []), ensure_ascii=False
                ),
                "CompositeTrendScore": scores["CompositeTrendScore"],
                "RiskRegimeScore": scores["RiskRegimeScore"],
                "GrowthLeadershipScore": scores["GrowthLeadershipScore"],
                "SemiconductorLeadershipScore": scores["SemiconductorLeadershipScore"],
                "EventRiskScore": scores["EventRiskScore"],
                "data_quality_status": data_quality_status,
            }
        )
        previous_weights = target_weights
        previous_scores = scores
        previous_regime = regime
    return pd.DataFrame(rows)


def _build_static_daily_path(
    *,
    pivot: pd.DataFrame,
    signal_indices: list[int],
    etf_config: ETFConfigBundle,
    policy: DynamicRobustnessPolicyConfig,
    weights: dict[str, float],
    comparison_id: str,
    row_type: str,
    data_quality_status: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    equity = 1.0
    previous_weights = weights
    for signal_index in signal_indices:
        signal_date = pivot.index[signal_index].date()
        execution_index = signal_index + policy.price_backtest.signal_lag_days
        return_index = execution_index + 1
        accounting = calculate_portfolio_accounting_step(
            pivot,
            signal_date=signal_date,
            execution_date=pivot.index[execution_index].date(),
            return_date=pivot.index[return_index].date(),
            target_weights=weights,
            previous_weights=previous_weights,
            asset_symbols=tuple(policy.price_backtest.required_symbols),
            total_cost_bps=_total_cost_bps(etf_config),
            starting_equity=equity,
        )
        equity = accounting.ending_equity
        rows.append(
            {
                "candidate_id": comparison_id,
                "row_type": row_type,
                "signal_date": signal_date.isoformat(),
                "execution_date": accounting.execution_date.isoformat(),
                "return_date": accounting.return_date.isoformat(),
                "selected_regime": "static",
                "strategy_return": accounting.strategy_return,
                "gross_return": accounting.gross_return,
                "transaction_cost": accounting.transaction_cost,
                "turnover": accounting.turnover,
                "portfolio_equity": accounting.ending_equity,
                "target_weights_json": json.dumps(weights, ensure_ascii=False, sort_keys=True),
                "asset_returns_json": json.dumps(
                    accounting.period_returns, ensure_ascii=False, sort_keys=True
                ),
                "asset_contributions_json": json.dumps(
                    accounting.asset_contributions, ensure_ascii=False, sort_keys=True
                ),
                "data_quality_status": data_quality_status,
            }
        )
        previous_weights = weights
    return pd.DataFrame(rows)


def _comparison_row(
    *,
    comparison_id: str,
    row_type: str,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame | None,
    source_status: str,
    source_path: str,
    source_candidate_id: str = "",
) -> dict[str, Any]:
    metrics = _metrics_from_daily(daily)
    returns = _return_series(daily)
    benchmark_returns = _return_series(benchmark_daily) if benchmark_daily is not None else []
    capture = _capture_metrics(returns, benchmark_returns)
    weights = _latest_weights(daily)
    return {
        "comparison_id": comparison_id,
        "row_type": row_type,
        "status": "AVAILABLE",
        "source_status": source_status,
        "source_path": source_path,
        "source_candidate_id": source_candidate_id,
        "trading_days": len(returns),
        "total_return": metrics.total_return,
        "CAGR": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "volatility": _annualized_volatility(returns),
        "Sharpe": metrics.sharpe,
        "Sortino": metrics.sortino,
        "Calmar": metrics.calmar,
        "turnover": metrics.turnover,
        "downside_capture": capture["downside_capture"],
        "upside_capture": capture["upside_capture"],
        "average_cash_weight": _average_weight(daily, "CASH"),
        "average_semiconductor_weight": _average_weight(daily, "SMH")
        + _average_weight(daily, "SOXX"),
        "latest_weights": weights,
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _walk_forward_review(
    daily: pd.DataFrame,
    policy: DynamicRobustnessPolicyConfig,
) -> dict[str, Any]:
    returns = _return_series(daily)
    cfg = policy.walk_forward
    windows: list[dict[str, Any]] = []
    for start_idx in range(0, max(0, len(returns) - cfg.window_days + 1), cfg.step_days):
        end_idx = start_idx + cfg.window_days
        window_returns = returns[start_idx:end_idx]
        window_daily = daily.iloc[start_idx:end_idx]
        metrics = _metrics_from_returns(window_returns, _turnover_series(window_daily))
        passed = (
            (metrics.sharpe is not None and metrics.sharpe >= cfg.min_sharpe)
            and metrics.max_drawdown >= cfg.max_drawdown_floor
            and metrics.total_return > 0.0
        )
        windows.append(
            {
                "window_id": f"wf_{len(windows) + 1:03d}",
                "start_date": str(window_daily.iloc[0]["signal_date"]),
                "end_date": str(window_daily.iloc[-1]["signal_date"]),
                "trading_days": len(window_returns),
                "total_return": metrics.total_return,
                "CAGR": metrics.cagr,
                "max_drawdown": metrics.max_drawdown,
                "Sharpe": metrics.sharpe,
                "turnover": metrics.turnover,
                "passed": passed,
            }
        )
    pass_count = sum(1 for window in windows if window["passed"])
    pass_ratio = 0.0 if not windows else pass_count / len(windows)
    status = (
        "PASS"
        if len(windows) >= cfg.min_windows and pass_ratio >= cfg.min_pass_ratio
        else "REVIEW_REQUIRED"
    )
    if len(windows) < cfg.min_windows:
        status = "LIMITED_SAMPLE"
    return {
        "status": status,
        "window_count": len(windows),
        "pass_count": pass_count,
        "pass_ratio": pass_ratio,
        "windows": windows,
    }


def _regime_attribution(dynamic_daily: pd.DataFrame, static_daily: pd.DataFrame) -> dict[str, Any]:
    static_by_date = {
        str(row["return_date"]): float(row["strategy_return"])
        for _, row in static_daily.iterrows()
    }
    regimes: list[dict[str, Any]] = []
    for regime, group in dynamic_daily.groupby("selected_regime", sort=True):
        returns = _return_series(group)
        static_returns = [
            static_by_date[str(row["return_date"])]
            for _, row in group.iterrows()
            if str(row["return_date"]) in static_by_date
        ]
        metrics = _metrics_from_returns(returns, _turnover_series(group))
        static_total = _compound_return(static_returns) if static_returns else None
        regimes.append(
            {
                "regime": str(regime),
                "trading_days": len(returns),
                "total_return": metrics.total_return,
                "CAGR": metrics.cagr,
                "max_drawdown": metrics.max_drawdown,
                "Sharpe": metrics.sharpe,
                "turnover": metrics.turnover,
                "excess_vs_static_base": (
                    None if static_total is None else metrics.total_return - static_total
                ),
                "average_cash_weight": _average_weight(group, "CASH"),
                "average_semiconductor_weight": _average_weight(group, "SMH")
                + _average_weight(group, "SOXX"),
            }
        )
    total_abs_positive = sum(max(0.0, float(row["total_return"])) for row in regimes)
    max_share = 0.0
    if total_abs_positive > 0:
        max_share = max(
            max(0.0, float(row["total_return"])) / total_abs_positive
            for row in regimes
        )
    return {
        "regime_count": len(regimes),
        "max_positive_return_share": max_share,
        "regimes": regimes,
    }


def _false_signal_diagnostics(
    *,
    dynamic_daily: pd.DataFrame,
    static_base_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    static_base_weights: dict[str, float],
    policy: DynamicRobustnessPolicyConfig,
) -> dict[str, Any]:
    cfg = policy.false_signal
    dynamic_returns = _return_series(dynamic_daily)
    static_returns = _return_series(static_base_daily)
    benchmark_returns = _return_series(benchmark_daily)
    base_cash = static_base_weights.get("CASH", 0.0)
    base_growth = _growth_weight(static_base_weights)
    false_off_events: list[dict[str, Any]] = []
    false_on_events: list[dict[str, Any]] = []
    horizon = cfg.forward_horizon_days
    for idx in range(0, max(0, len(dynamic_returns) - horizon + 1)):
        weights = _weights_from_row(dynamic_daily.iloc[idx])
        cash_diff = weights.get("CASH", 0.0) - base_cash
        growth_diff = _growth_weight(weights) - base_growth
        benchmark_forward = _compound_return(benchmark_returns[idx : idx + horizon])
        dynamic_forward = _compound_return(dynamic_returns[idx : idx + horizon])
        benchmark_drawdown = _max_drawdown_from_returns(benchmark_returns[idx : idx + horizon])
        dynamic_drawdown = _max_drawdown_from_returns(dynamic_returns[idx : idx + horizon])
        static_drawdown = _max_drawdown_from_returns(static_returns[idx : idx + horizon])
        if (
            (
                cash_diff >= cfg.cash_overweight_threshold
                or growth_diff <= cfg.growth_underweight_threshold
            )
            and benchmark_forward >= cfg.market_up_threshold
        ):
            false_off_events.append(
                {
                    "signal_date": str(dynamic_daily.iloc[idx]["signal_date"]),
                    "cash_diff_vs_static": cash_diff,
                    "growth_diff_vs_static": growth_diff,
                    "benchmark_forward_return": benchmark_forward,
                    "dynamic_forward_return": dynamic_forward,
                    "opportunity_cost": max(0.0, benchmark_forward - dynamic_forward),
                }
            )
        if (
            growth_diff >= cfg.growth_overweight_threshold
            and benchmark_drawdown <= cfg.forward_drawdown_threshold
        ):
            false_on_events.append(
                {
                    "signal_date": str(dynamic_daily.iloc[idx]["signal_date"]),
                    "growth_diff_vs_static": growth_diff,
                    "benchmark_forward_drawdown": benchmark_drawdown,
                    "dynamic_forward_drawdown": dynamic_drawdown,
                    "static_forward_drawdown": static_drawdown,
                    "drawdown_cost": max(0.0, abs(dynamic_drawdown) - abs(static_drawdown)),
                }
            )
    return {
        "benchmark_symbol": cfg.benchmark_symbol,
        "forward_horizon_days": horizon,
        "false_risk_off": {
            "event_count": len(false_off_events),
            "total_opportunity_cost": sum(
                float(item["opportunity_cost"]) for item in false_off_events
            ),
            "events": false_off_events[:20],
        },
        "false_risk_on": {
            "event_count": len(false_on_events),
            "total_drawdown_cost": sum(float(item["drawdown_cost"]) for item in false_on_events),
            "events": false_on_events[:20],
        },
    }


def _turnover_sensitivity(
    *,
    pivot: pd.DataFrame,
    signal_indices: list[int],
    etf_config: ETFConfigBundle,
    policy: DynamicRobustnessPolicyConfig,
    dynamic_policy: DynamicAllocationPolicyConfig,
    candidate_id: str,
    data_quality_status: str,
) -> dict[str, Any]:
    variants: list[dict[str, Any]] = []
    baseline = dynamic_policy.rebalance_policy
    for field_name, values in (
        ("score_change_threshold", policy.sensitivity.score_change_thresholds),
        ("minimum_holding_days", policy.sensitivity.minimum_holding_days),
        ("regime_confirmation_days", policy.sensitivity.regime_confirmation_days),
        ("weekly_turnover_cap", policy.sensitivity.weekly_turnover_caps),
    ):
        for value in values:
            variant_policy = dynamic_policy.model_copy(deep=True)
            setattr(variant_policy.rebalance_policy, field_name, value)
            daily = _build_dynamic_daily_path(
                pivot=pivot,
                signal_indices=signal_indices,
                etf_config=etf_config,
                policy=policy,
                dynamic_policy=variant_policy,
                candidate_id=f"{candidate_id}:{field_name}={value}",
                source_calibration_report=None,
                data_quality_status=data_quality_status,
            )
            metrics = _metrics_from_daily(daily)
            variants.append(
                {
                    "parameter": field_name,
                    "value": value,
                    "is_baseline": getattr(baseline, field_name) == value,
                    "total_return": metrics.total_return,
                    "max_drawdown": metrics.max_drawdown,
                    "Sharpe": metrics.sharpe,
                    "turnover": metrics.turnover,
                    "rebalance_count": int(
                        (daily["rebalance_decision"] == "rebalance_candidate").sum()
                    ),
                }
            )
    returns = [float(item["total_return"]) for item in variants]
    return_range = max(returns) - min(returns) if returns else 0.0
    return {
        "variant_count": len(variants),
        "return_range": return_range,
        "max_turnover": max((float(item["turnover"]) for item in variants), default=0.0),
        "variants": variants,
    }


def _ai_semiconductor_attribution(dynamic_daily: pd.DataFrame) -> dict[str, Any]:
    contribution_by_symbol: dict[str, float] = {
        symbol: 0.0 for symbol in ("SPY", "QQQ", "SMH", "SOXX", "CASH")
    }
    for _, row in dynamic_daily.iterrows():
        for symbol, value in _mapping_from_json(row.get("asset_contributions_json")).items():
            contribution_by_symbol[str(symbol)] = contribution_by_symbol.get(
                str(symbol), 0.0
            ) + _float(value)
    semiconductor = contribution_by_symbol.get("SMH", 0.0) + contribution_by_symbol.get(
        "SOXX", 0.0
    )
    growth = contribution_by_symbol.get("QQQ", 0.0)
    return {
        "source": "price_return_contribution_by_dynamic_candidate_weight",
        "ai_proxy_symbols": ["QQQ", "SMH", "SOXX"],
        "semiconductor_symbols": ["SMH", "SOXX"],
        "contribution_by_symbol": contribution_by_symbol,
        "growth_proxy_contribution": growth,
        "semiconductor_contribution": semiconductor,
        "cash_contribution": contribution_by_symbol.get("CASH", 0.0),
        "limitation": (
            "AI attribution is ETF-proxy price attribution; it is not issuer-level "
            "fundamental AI revenue attribution."
        ),
    }


def _event_risk_overlay_attribution(
    dynamic_daily: pd.DataFrame,
    static_base_daily: pd.DataFrame,
    dynamic_policy: DynamicAllocationPolicyConfig,
) -> dict[str, Any]:
    threshold = dynamic_policy.event_risk_overlay.high_threshold
    event_rows = dynamic_daily.loc[
        pd.to_numeric(dynamic_daily["EventRiskScore"], errors="coerce") >= threshold
    ]
    non_event_rows = dynamic_daily.loc[
        pd.to_numeric(dynamic_daily["EventRiskScore"], errors="coerce") < threshold
    ]
    static_by_date = {
        str(row["return_date"]): float(row["strategy_return"])
        for _, row in static_base_daily.iterrows()
    }
    event_static = [
        static_by_date[str(row["return_date"])]
        for _, row in event_rows.iterrows()
        if str(row["return_date"]) in static_by_date
    ]
    event_dynamic = _return_series(event_rows)
    return {
        "event_risk_threshold": threshold,
        "event_risk_day_count": int(len(event_rows)),
        "non_event_day_count": int(len(non_event_rows)),
        "event_risk_dynamic_return": _compound_return(event_dynamic) if event_dynamic else 0.0,
        "event_risk_static_return": _compound_return(event_static) if event_static else 0.0,
        "event_risk_drawdown_reduction_vs_static": (
            abs(_max_drawdown_from_returns(event_static))
            - abs(_max_drawdown_from_returns(event_dynamic))
            if event_dynamic and event_static
            else None
        ),
        "average_event_cash_weight": _average_weight(event_rows, "CASH"),
        "average_non_event_cash_weight": _average_weight(non_event_rows, "CASH"),
    }


def _dynamic_overfit_diagnostics(
    *,
    dynamic_daily: pd.DataFrame,
    walk_forward: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    policy: DynamicRobustnessPolicyConfig,
) -> dict[str, Any]:
    cfg = policy.overfit
    windows = _records(walk_forward.get("windows"))
    positive_returns = [max(0.0, _float(window.get("total_return"))) for window in windows]
    total_positive = sum(positive_returns)
    single_window_share = max(positive_returns) / total_positive if total_positive > 0 else 0.0
    pass_ratio = _float(walk_forward.get("pass_ratio"))
    regime_concentration = _float(regime_attribution.get("max_positive_return_share"))
    return_range = _float(sensitivity.get("return_range"))
    metrics = _metrics_from_daily(dynamic_daily)
    allocation_instability = _average_turnover(dynamic_daily)
    findings: list[dict[str, Any]] = []
    findings.append(
        _finding(
            "single_period_dependency",
            single_window_share <= cfg.max_single_window_return_share,
            single_window_share,
            cfg.max_single_window_return_share,
        )
    )
    findings.append(
        _finding(
            "walk_forward_pass_ratio",
            pass_ratio >= cfg.min_positive_walk_forward_ratio,
            pass_ratio,
            cfg.min_positive_walk_forward_ratio,
        )
    )
    findings.append(
        _finding(
            "parameter_sensitivity",
            return_range <= cfg.max_parameter_sensitivity_return_range,
            return_range,
            cfg.max_parameter_sensitivity_return_range,
        )
    )
    findings.append(
        _finding(
            "regime_return_concentration",
            regime_concentration <= cfg.max_regime_return_concentration,
            regime_concentration,
            cfg.max_regime_return_concentration,
        )
    )
    findings.append(
        _finding(
            "turnover",
            metrics.turnover <= cfg.max_turnover,
            metrics.turnover,
            cfg.max_turnover,
        )
    )
    findings.append(
        _finding(
            "allocation_path_instability",
            allocation_instability <= cfg.max_allocation_path_instability,
            allocation_instability,
            cfg.max_allocation_path_instability,
        )
    )
    failed = [item for item in findings if item["status"] != "PASS"]
    return {
        "status": "PASS" if not failed else "REVIEW_REQUIRED",
        "risk_level": "low" if not failed else ("medium" if len(failed) <= 2 else "high"),
        "failed_finding_count": len(failed),
        "findings": findings,
    }


def _price_pivot(prices: pd.DataFrame, policy: DynamicRobustnessPolicyConfig) -> pd.DataFrame:
    frame = prices.copy()
    required = set(policy.price_backtest.required_symbols)
    if "symbol" not in frame.columns:
        raise DynamicRobustnessError("prices must include symbol column")
    if policy.price_backtest.price_field not in frame.columns:
        raise DynamicRobustnessError(
            f"prices must include {policy.price_backtest.price_field} column"
        )
    frame = frame.loc[frame["symbol"].astype(str).isin(required)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[policy.price_backtest.price_field], errors="coerce")
    frame = frame.dropna(subset=["_date", "_price"])
    pivot = frame.pivot_table(
        index="_date",
        columns="symbol",
        values="_price",
        aggfunc="last",
    ).sort_index()
    if "CASH" not in pivot.columns:
        pivot["CASH"] = 1.0
    pivot["CASH"] = pivot["CASH"].fillna(1.0)
    missing = sorted(required - set(pivot.columns))
    if missing:
        raise DynamicRobustnessError("prices missing required symbols: " + ", ".join(missing))
    pivot = pivot[list(policy.price_backtest.required_symbols)].ffill()
    required_non_cash = [
        symbol for symbol in policy.price_backtest.required_symbols if symbol != "CASH"
    ]
    pivot = pivot.dropna(subset=required_non_cash)
    if pivot.empty:
        raise DynamicRobustnessError("price pivot has no complete ETF rows")
    return pivot


def _signal_indices(
    trading_dates: list[pd.Timestamp],
    *,
    requested_start: date,
    requested_end: date,
    policy: DynamicRobustnessPolicyConfig,
) -> list[int]:
    start_ts = pd.Timestamp(requested_start)
    end_ts = pd.Timestamp(requested_end)
    max_signal_index = len(trading_dates) - policy.price_backtest.signal_lag_days - 2
    return [
        idx
        for idx, item in enumerate(trading_dates)
        if idx >= policy.price_backtest.warmup_days
        and idx <= max_signal_index
        and start_ts <= item <= end_ts
    ]


def _price_derived_scores(
    pivot: pd.DataFrame,
    signal_index: int,
    policy: DynamicRobustnessPolicyConfig,
) -> dict[str, float]:
    cfg = policy.score_model
    qqq_short = _trailing_return(pivot, "QQQ", signal_index, cfg.short_momentum_window)
    qqq_medium = _trailing_return(pivot, "QQQ", signal_index, cfg.medium_momentum_window)
    qqq_long = _trailing_return(pivot, "QQQ", signal_index, cfg.long_momentum_window)
    spy_medium = _trailing_return(pivot, "SPY", signal_index, cfg.medium_momentum_window)
    smh_medium = _trailing_return(pivot, "SMH", signal_index, cfg.medium_momentum_window)
    soxx_medium = _trailing_return(pivot, "SOXX", signal_index, cfg.medium_momentum_window)
    composite = mean(
        [
            _score_between(qqq_short, cfg.return_score_floor, cfg.return_score_ceiling),
            _score_between(qqq_medium, cfg.return_score_floor, cfg.return_score_ceiling),
            _score_between(qqq_long, cfg.return_score_floor, cfg.return_score_ceiling),
            _score_between(spy_medium, cfg.return_score_floor, cfg.return_score_ceiling),
            _score_between(smh_medium, cfg.return_score_floor, cfg.return_score_ceiling),
        ]
    )
    qqq_vol = _annualized_window_volatility(pivot, "QQQ", signal_index, cfg.volatility_window)
    spy_vol = _annualized_window_volatility(pivot, "SPY", signal_index, cfg.volatility_window)
    qqq_drawdown = _window_drawdown(pivot, "QQQ", signal_index, cfg.drawdown_window)
    spy_drawdown = _window_drawdown(pivot, "SPY", signal_index, cfg.drawdown_window)
    vol_risk = _score_between(mean([qqq_vol, spy_vol]), cfg.volatility_low, cfg.volatility_high)
    drawdown_risk = _score_between(
        mean([qqq_drawdown, spy_drawdown]),
        cfg.drawdown_low,
        cfg.drawdown_high,
    )
    risk_regime = _clamp(100.0 - mean([vol_risk, drawdown_risk]), 0.0, 100.0)
    growth = _score_between(
        qqq_medium - spy_medium,
        cfg.relative_strength_floor,
        cfg.relative_strength_ceiling,
    )
    semi = _score_between(
        mean([smh_medium, soxx_medium]) - qqq_medium,
        cfg.relative_strength_floor,
        cfg.relative_strength_ceiling,
    )
    negative_trend_risk = _clamp(100.0 - composite, 0.0, 100.0)
    event_risk = (
        vol_risk * cfg.event_risk_volatility_weight
        + drawdown_risk * cfg.event_risk_drawdown_weight
        + negative_trend_risk * cfg.event_risk_negative_trend_weight
    )
    adjusted = _clamp(composite - event_risk * cfg.event_risk_trend_penalty, 0.0, 100.0)
    return {
        "CompositeTrendScore": round(composite, 6),
        "RiskRegimeScore": round(risk_regime, 6),
        "GrowthLeadershipScore": round(growth, 6),
        "SemiconductorLeadershipScore": round(semi, 6),
        "EventRiskAdjustedTrendScore": round(adjusted, 6),
        "EventRiskScore": round(event_risk, 6),
    }


def _resolve_dynamic_candidate(
    *,
    candidate_id: str | None,
    dynamic_calibration_report: Mapping[str, Any] | None,
) -> dict[str, Any]:
    report = _mapping(dynamic_calibration_report)
    requested = candidate_id or _text(
        _mapping(report.get("summary")).get("top_dynamic_candidate_pack_id")
    )
    packs = _records(report.get("candidate_packs")) + _records(report.get("top_candidate_packs"))
    selected = next(
        (
            pack
            for pack in packs
            if _text(pack.get("dynamic_candidate_pack_id")) == requested and requested
        ),
        {},
    )
    if not requested:
        requested = "dynamic_candidate_unspecified"
    return {
        "candidate_id": requested,
        "source_status": (
            "FOUND_IN_DYNAMIC_CALIBRATION_REPORT"
            if selected
            else "NOT_FOUND_OR_NOT_PROVIDED"
        ),
        "trend_signal_config_id": _text(selected.get("trend_signal_config_id")),
        "allocation_profile_id": _text(selected.get("allocation_profile_id")),
        "calibration_proxy": bool(selected.get("calibration_proxy", True)),
        "full_robustness_backtest_required": bool(
            selected.get("full_robustness_backtest_required", True)
        ),
        "ranking": _mapping(selected.get("ranking")),
    }


def _load_best_static_historical_candidate(
    policy: DynamicRobustnessPolicyConfig,
) -> dict[str, Any]:
    pattern = str(PROJECT_ROOT / policy.price_backtest.best_static_historical_source_glob)
    paths = sorted(
        Path(PROJECT_ROOT).glob(policy.price_backtest.best_static_historical_source_glob)
    )
    if not paths:
        return {
            "source_status": "MISSING",
            "source_path": pattern,
            "candidate_id": "",
            "weights": {},
        }
    path = max(paths, key=lambda item: item.stat().st_mtime)
    payload = json.loads(path.read_text(encoding="utf-8"))
    candidates = _records(payload.get("candidates"))
    if not candidates:
        return {
            "source_status": "MISSING_CANDIDATES",
            "source_path": str(path),
            "candidate_id": "",
            "weights": {},
        }
    top = candidates[0]
    return {
        "source_status": "AVAILABLE",
        "source_path": str(path),
        "candidate_id": _text(top.get("weight_set_id") or top.get("candidate_id")),
        "weights": _mapping(top.get("weights")),
    }


def _missing_best_static_row(best_static: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "comparison_id": "best_static_historical_candidate",
        "row_type": "static_portfolio",
        "status": "MISSING",
        "source_status": best_static.get("source_status", "MISSING"),
        "source_path": best_static.get("source_path", ""),
        "source_candidate_id": "",
        "metric_null_reasons": {
            "best_static_historical_candidate": "top weight candidate artifact missing"
        },
        "total_return": None,
        "CAGR": None,
        "max_drawdown": None,
        "volatility": None,
        "Sharpe": None,
        "Sortino": None,
        "Calmar": None,
        "turnover": None,
        "downside_capture": None,
        "upside_capture": None,
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _ordered_comparison_rows(
    rows: list[dict[str, Any]],
    policy: DynamicRobustnessPolicyConfig,
) -> list[dict[str, Any]]:
    order = {
        comparison_id: idx
        for idx, comparison_id in enumerate(policy.price_backtest.comparison_order)
    }
    return sorted(rows, key=lambda row: order.get(str(row.get("comparison_id")), 999))


def _current_baseline_weights(config: ETFConfigBundle) -> dict[str, float]:
    return _normalise_weights(
        {
            symbol: float(asset.default_weight)
            for symbol, asset in config.assets.assets.items()
            if symbol in {"SPY", "QQQ", "SMH", "SOXX", "CASH"}
        }
    )


def _metrics_from_daily(daily: pd.DataFrame) -> BacktestMetrics:
    return _metrics_from_returns(_return_series(daily), _turnover_series(daily))


def _metrics_from_returns(returns: list[float], turnovers: list[float]) -> BacktestMetrics:
    exposures = [1.0 for _ in returns]
    if not returns:
        raise DynamicRobustnessError("cannot compute metrics from empty returns")
    return summarize_long_only_backtest(returns, exposures, turnovers)


def _capture_metrics(
    strategy_returns: list[float],
    benchmark_returns: list[float],
) -> dict[str, float | None]:
    if not strategy_returns or len(strategy_returns) != len(benchmark_returns):
        return {"upside_capture": None, "downside_capture": None}
    up_strategy = [s for s, b in zip(strategy_returns, benchmark_returns, strict=True) if b > 0]
    up_benchmark = [b for b in benchmark_returns if b > 0]
    down_strategy = [s for s, b in zip(strategy_returns, benchmark_returns, strict=True) if b < 0]
    down_benchmark = [b for b in benchmark_returns if b < 0]
    return {
        "upside_capture": _capture_ratio(up_strategy, up_benchmark),
        "downside_capture": _capture_ratio(down_strategy, down_benchmark),
    }


def _capture_ratio(strategy_returns: list[float], benchmark_returns: list[float]) -> float | None:
    if not strategy_returns or not benchmark_returns:
        return None
    benchmark = _compound_return(benchmark_returns)
    if abs(benchmark) < 1e-12:
        return None
    return _compound_return(strategy_returns) / benchmark


def _benchmark_daily(
    frames: Mapping[str, pd.DataFrame],
    key: str,
) -> pd.DataFrame | None:
    return frames.get(key)


def _daily_path_summary(dynamic_daily: pd.DataFrame) -> dict[str, Any]:
    records = dynamic_daily.to_dict(orient="records")
    regimes = [
        str(value)
        for value in dynamic_daily.get("selected_regime", pd.Series(dtype=object)).tolist()
    ]
    regime_switch_count = sum(
        1 for previous, current in zip(regimes, regimes[1:], strict=False) if current != previous
    )
    constraint_hit_count = 0
    constraint_prefixes = (
        "MAX_",
        "MIN_",
        "WEEKLY_TURNOVER_CAP",
        "REGIME_CONFIRMATION_WINDOW",
    )
    for value in dynamic_daily.get("reason_codes_json", pd.Series(dtype=object)).tolist():
        try:
            reason_codes = json.loads(str(value))
        except (TypeError, ValueError):
            reason_codes = []
        if not isinstance(reason_codes, list):
            continue
        if any(str(code).startswith(constraint_prefixes) for code in reason_codes):
            constraint_hit_count += 1
    return {
        "row_count": len(records),
        "first_signal_date": (
            "" if dynamic_daily.empty else str(dynamic_daily.iloc[0]["signal_date"])
        ),
        "last_signal_date": (
            "" if dynamic_daily.empty else str(dynamic_daily.iloc[-1]["signal_date"])
        ),
        "regime_switch_count": regime_switch_count,
        "constraint_hit_count": constraint_hit_count,
        "records": records,
        "sample_rows": records[:5] + records[-5:] if len(records) > 10 else records,
        "no_lookahead_timing": "signal_date < execution_date < return_date",
    }


def _robustness_status(
    dynamic_row: Mapping[str, Any],
    static_row: Mapping[str, Any],
    overfit: Mapping[str, Any],
) -> str:
    dynamic_return = _float(dynamic_row.get("total_return"))
    static_return = _float(static_row.get("total_return"))
    if overfit.get("status") != "PASS":
        return "REVIEW_REQUIRED"
    return "PASS" if dynamic_return >= static_return else "REVIEW_REQUIRED"


def _synthetic_validation_prices(policy: DynamicRobustnessPolicyConfig) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=360)
    rows: list[dict[str, Any]] = []
    bases = {"SPY": 100.0, "QQQ": 100.0, "SMH": 100.0, "SOXX": 100.0, "CASH": 1.0}
    drifts = {"SPY": 0.00035, "QQQ": 0.00050, "SMH": 0.00065, "SOXX": 0.00060, "CASH": 0.0}
    for idx, item in enumerate(dates):
        for symbol in policy.price_backtest.required_symbols:
            cyclical = 1.0 + (0.0015 if idx % 23 < 11 else -0.001)
            value = (
                bases[symbol]
                * ((1.0 + drifts[symbol]) ** idx)
                * (cyclical if symbol != "CASH" else 1.0)
            )
            rows.append(
                {
                    "date": item.date().isoformat(),
                    "symbol": symbol,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "adj_close": value,
                    "volume": 1000.0 if symbol != "CASH" else 0.0,
                    "source": "dynamic_robustness_validation_sample",
                    "created_at": "validation_sample",
                }
            )
    return pd.DataFrame(rows)


def _trailing_return(pivot: pd.DataFrame, symbol: str, signal_index: int, window: int) -> float:
    start_index = max(0, signal_index - window)
    start = _float(pivot.iloc[start_index][symbol])
    end = _float(pivot.iloc[signal_index][symbol])
    if start <= 0:
        return 0.0
    return end / start - 1.0


def _annualized_window_volatility(
    pivot: pd.DataFrame,
    symbol: str,
    signal_index: int,
    window: int,
) -> float:
    start = max(0, signal_index - window)
    series = pivot[symbol].iloc[start : signal_index + 1].pct_change().dropna()
    if len(series) < 2:
        return 0.0
    return float(series.std(ddof=0) * sqrt(252.0))


def _window_drawdown(pivot: pd.DataFrame, symbol: str, signal_index: int, window: int) -> float:
    start = max(0, signal_index - window)
    series = pivot[symbol].iloc[start : signal_index + 1]
    peak = float(series.max())
    current = _float(series.iloc[-1])
    if peak <= 0:
        return 0.0
    return current / peak - 1.0


def _score_between(value: float, low: float, high: float) -> float:
    if abs(high - low) < 1e-12:
        return 0.0
    return _clamp((value - low) / (high - low) * 100.0, 0.0, 100.0)


def _return_series(daily: pd.DataFrame | None) -> list[float]:
    if daily is None or daily.empty:
        return []
    return [
        float(value)
        for value in pd.to_numeric(daily["strategy_return"], errors="coerce").dropna()
    ]


def _turnover_series(daily: pd.DataFrame) -> list[float]:
    if daily.empty:
        return []
    return [float(value) for value in pd.to_numeric(daily["turnover"], errors="coerce").fillna(0.0)]


def _annualized_volatility(returns: list[float]) -> float | None:
    if len(returns) < 2:
        return None
    return pstdev(returns) * sqrt(252.0)


def _compound_return(returns: list[float]) -> float:
    if not returns:
        return 0.0
    return prod(1.0 + value for value in returns) - 1.0


def _max_drawdown_from_returns(returns: list[float]) -> float:
    if not returns:
        return 0.0
    running = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in returns:
        running *= 1.0 + value
        peak = max(peak, running)
        drawdown = min(drawdown, running / peak - 1.0)
    return drawdown


def _latest_weights(daily: pd.DataFrame) -> dict[str, float]:
    if daily.empty:
        return {}
    return _weights_from_row(daily.iloc[-1])


def _weights_from_row(row: pd.Series) -> dict[str, float]:
    return _normalise_weights(_mapping_from_json(row.get("target_weights_json")))


def _average_weight(daily: pd.DataFrame, symbol: str) -> float:
    if daily.empty:
        return 0.0
    weights = [_weights_from_row(row).get(symbol, 0.0) for _, row in daily.iterrows()]
    return mean(weights) if weights else 0.0


def _average_turnover(daily: pd.DataFrame) -> float:
    turnovers = _turnover_series(daily)
    return mean(turnovers) if turnovers else 0.0


def _growth_weight(weights: Mapping[str, float]) -> float:
    return _float(weights.get("QQQ")) + _float(weights.get("SMH")) + _float(weights.get("SOXX"))


def _total_cost_bps(config: ETFConfigBundle) -> float:
    costs = config.risk.transaction_costs
    return float(costs.commission_bps + costs.slippage_bps)


def _normalise_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    symbols = ("SPY", "QQQ", "SMH", "SOXX", "CASH")
    cleaned = {symbol: max(0.0, _float(weights.get(symbol))) for symbol in symbols}
    total = sum(cleaned.values())
    if total <= 0:
        raise DynamicRobustnessError("weights must have positive total")
    return {symbol: cleaned[symbol] / total for symbol in symbols}


def _finding(check_id: str, passed: bool, value: float, threshold: float) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "REVIEW_REQUIRED",
        "value": value,
        "threshold": threshold,
    }


def _append_check(checks: list[dict[str, Any]], check_id: str, passed: bool, message: str) -> None:
    checks.append(
        {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}
    )


def _assert_dynamic_robustness_payload_safe(payload: Mapping[str, Any]) -> None:
    for key, expected in DYNAMIC_ROBUSTNESS_SAFETY.items():
        if payload.get(key) != expected:
            raise DynamicRobustnessError(f"unsafe dynamic robustness payload field: {key}")
    safety = _mapping(payload.get("safety"))
    if safety and safety != DYNAMIC_ROBUSTNESS_SAFETY:
        raise DynamicRobustnessError("unsafe dynamic robustness nested safety payload")
    if payload.get("commands_executed") is not False:
        raise DynamicRobustnessError("dynamic robustness payload must keep commands_executed=false")
    _assert_no_forbidden_keys(payload)


def _assert_no_forbidden_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) in FORBIDDEN_DYNAMIC_ROBUSTNESS_KEYS and item not in {False, None, "none"}:
                raise DynamicRobustnessError(f"forbidden dynamic robustness key present: {key}")
            _assert_no_forbidden_keys(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_forbidden_keys(item)


def _write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_text(text: str, path: Path) -> None:
    path.write_text(text, encoding="utf-8")


def _latest_json(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    paths = sorted(directory.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
    return paths[0] if paths else None


def _stable_id(prefix: str, *parts: object) -> str:
    return f"{prefix}_{_stable_hash(parts)[:12]}"


def _stable_hash(value: object) -> str:
    return sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return (
        [dict(item) for item in value if isinstance(item, Mapping)]
        if isinstance(value, list)
        else []
    )


def _mapping_from_json(value: object) -> dict[str, float]:
    if isinstance(value, Mapping):
        return {str(key): _float(item) for key, item in value.items()}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, Mapping):
        return {}
    return {str(key): _float(item) for key, item in parsed.items()}


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _fmt_pct(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.2%}"


def _fmt_num(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.3f}"
