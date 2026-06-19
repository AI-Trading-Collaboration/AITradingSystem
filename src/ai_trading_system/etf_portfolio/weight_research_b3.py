from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    ETFConfigBundle,
)
from ai_trading_system.etf_portfolio.weight_research_execution import (
    comparison_payload,
    metrics_from_execution_daily,
    metrics_payload,
    simulate_target_path_execution,
)
from ai_trading_system.etf_portfolio.weight_research_interfaces import (
    build_signal_diagnostics_report,
)
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    DEFAULT_HOLDOUT_POLICY_PATH,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SCOPE_FREEZE_PATH,
    DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    B1ExecutionPolicy,
    ResearchDataContext,
    load_b1_execution_policy,
    prepare_research_data_context,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_research_modules.yaml"
)
DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

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


@dataclass(frozen=True)
class B3RelativeTiltSignalPolicy:
    signal_id: str
    source_feature_columns: dict[str, tuple[str, ...]]
    neutral_score: float
    underweight_score_max: float
    overweight_score_min: float
    min_coverage: float
    max_stale_days: int


@dataclass(frozen=True)
class B3TargetMappingPolicy:
    mapping_id: str
    max_relative_tilt: float
    preserve_total_equity_exposure: bool
    cash_symbol: str


def run_b3_relative_tilt_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    modules_config_path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    generated_at: datetime | None = None,
    alias_dir: Path | None = None,
) -> tuple[dict[str, Any], Path, Path, dict[str, Path]]:
    context = prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=start,
        end=end,
        scope_path=DEFAULT_SCOPE_FREEZE_PATH,
        signal_contract_path=DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
        holdout_policy_path=DEFAULT_HOLDOUT_POLICY_PATH,
        config_path=DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
        generated_at=generated_at,
        data_quality_output_path=None,
    )
    signal_policy, target_policy = load_b3_policies(modules_config_path)
    if context.contract_validation["status"] != "PASS" or not context.data_quality_report.passed:
        payload = _blocked_b3_payload(
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="contract_or_data_quality_failed",
            details={
                "contract_status": context.contract_validation["status"],
                "data_quality_status": context.data_quality_report.status,
            },
        )
        json_path, md_path = write_b3_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, {}

    feature_frame = build_feature_store(
        context.prices,
        assets=context.etf_config.assets,
        strategy=context.etf_config.strategy,
        start=context.etf_config.backtest.backtest.warmup_start_date,
        end=end,
    )
    feature_artifact = _filter_feature_artifact(feature_frame, start=start, end=end)
    signal_artifact = build_b3_relative_tilt_signal(
        feature_artifact,
        config=context.etf_config,
        policy=signal_policy,
    )
    diagnostics = build_signal_diagnostics_report(
        signal_artifact,
        signal_artifact_id=signal_policy.signal_id,
        as_of=end,
        max_stale_days=signal_policy.max_stale_days,
        generated_at=context.generated_at,
    )
    signal_gate_status = _b3_signal_gate_status(signal_artifact, diagnostics, signal_policy)
    if signal_gate_status != "B3_SIGNAL_READY":
        payload = build_b3_result_payload(
            context=context,
            start=start,
            end=end,
            signal_policy=signal_policy,
            target_policy=target_policy,
            diagnostics=diagnostics,
            signal_gate_status=signal_gate_status,
            feature_artifact=feature_artifact,
            signal_artifact=signal_artifact,
            target_path=pd.DataFrame(),
            e0_daily=pd.DataFrame(),
            e1_daily=pd.DataFrame(),
            prices_path=prices_path,
            modules_config_path=modules_config_path,
        )
        paths = write_b3_component_artifacts(
            feature_artifact=feature_artifact,
            signal_artifact=signal_artifact,
            target_path=pd.DataFrame(),
            e0_daily=pd.DataFrame(),
            e1_daily=pd.DataFrame(),
            diagnostics=diagnostics,
            generated_at=context.generated_at,
            output_dir=output_dir,
        )
        json_path, md_path = write_b3_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, paths

    target_path = build_b3_target_path(
        signal_artifact,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=target_policy,
        signal_policy=signal_policy,
        start=start,
        end=end,
    )
    b1_policy = load_b1_execution_policy()
    e0_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=target_path,
        mode="naive",
    )
    e1_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=target_path,
        mode="controlled",
        execution_policy=b1_policy,
    )
    payload = build_b3_result_payload(
        context=context,
        start=start,
        end=end,
        signal_policy=signal_policy,
        target_policy=target_policy,
        diagnostics=diagnostics,
        signal_gate_status=signal_gate_status,
        feature_artifact=feature_artifact,
        signal_artifact=signal_artifact,
        target_path=target_path,
        e0_daily=e0_daily,
        e1_daily=e1_daily,
        prices_path=prices_path,
        modules_config_path=modules_config_path,
        b1_policy=b1_policy,
    )
    paths = write_b3_component_artifacts(
        feature_artifact=feature_artifact,
        signal_artifact=signal_artifact,
        target_path=target_path,
        e0_daily=e0_daily,
        e1_daily=e1_daily,
        diagnostics=diagnostics,
        generated_at=context.generated_at,
        output_dir=output_dir,
    )
    json_path, md_path = write_b3_result(payload, output_dir=output_dir, alias_dir=alias_dir)
    return payload, json_path, md_path, paths


def build_b3_relative_tilt_signal(
    feature_artifact: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    policy: B3RelativeTiltSignalPolicy,
) -> pd.DataFrame:
    baseline = _default_weights(config)
    active_symbols = [
        symbol
        for symbol, weight in baseline.items()
        if symbol != "CASH" and weight > 0.0
    ]
    rows: list[dict[str, Any]] = []
    for signal_date, group in feature_artifact.groupby("date", sort=True):
        by_symbol = {str(row["symbol"]): row for _, row in group.iterrows()}
        for symbol in active_symbols:
            row = by_symbol.get(symbol)
            score, coverage, missing_fields, source_values = _relative_signal_score(
                row,
                symbol=symbol,
                config=config,
                policy=policy,
            )
            rows.append(
                {
                    "date": str(signal_date),
                    "symbol": symbol,
                    "relative_strength_score": score,
                    "signal_score": score,
                    "state": _relative_state(score, policy),
                    "confidence": coverage,
                    "relative_strength_coverage": coverage,
                    "source_fields_json": json.dumps(
                        list(policy.source_feature_columns.get(symbol, ())),
                        ensure_ascii=False,
                    ),
                    "source_values_json": json.dumps(
                        source_values,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "missing_fields_json": json.dumps(missing_fields, ensure_ascii=False),
                    "official_target_weights": False,
                    "production_effect": "none",
                }
            )
    return pd.DataFrame(rows)


def build_b3_target_path(
    signal_artifact: pd.DataFrame,
    *,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    mapping_policy: B3TargetMappingPolicy,
    signal_policy: B3RelativeTiltSignalPolicy,
    start: date,
    end: date,
) -> pd.DataFrame:
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [item.date() for item in close_pivot.index if start <= item.date() <= end]
    signal_by_key = {
        (date.fromisoformat(str(row["date"])), str(row["symbol"])): row
        for _, row in signal_artifact.iterrows()
    }
    baseline = _default_weights(config)
    active_symbols = [
        symbol
        for symbol, weight in baseline.items()
        if symbol != mapping_policy.cash_symbol and weight > 0.0
    ]
    rows: list[dict[str, Any]] = []
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        signal_scores: dict[str, float] = {}
        raw_weights: dict[str, float] = {}
        multipliers: dict[str, float] = {}
        for symbol in active_symbols:
            signal_row = signal_by_key.get((signal_date, symbol))
            score = (
                signal_policy.neutral_score
                if signal_row is None
                else float(signal_row["signal_score"])
            )
            signal_scores[symbol] = score
            multiplier = _tilt_multiplier(score, signal_policy, mapping_policy)
            multipliers[symbol] = multiplier
            raw_weights[symbol] = baseline[symbol] * multiplier
        target_weights = _target_weights_from_tilt(
            baseline,
            raw_weights,
            cash_symbol=mapping_policy.cash_symbol,
            preserve_total_equity_exposure=mapping_policy.preserve_total_equity_exposure,
        )
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": trading_dates[execution_index].isoformat(),
                "return_date": trading_dates[return_index].isoformat(),
                "target_path_module": "B3",
                "signal_scores_json": json.dumps(
                    signal_scores,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "tilt_multipliers_json": json.dumps(
                    multipliers,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    target_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "official_target_weights": False,
                "production_effect": "none",
            }
        )
    return pd.DataFrame(rows)


def build_b3_result_payload(
    *,
    context: ResearchDataContext,
    start: date,
    end: date,
    signal_policy: B3RelativeTiltSignalPolicy,
    target_policy: B3TargetMappingPolicy,
    diagnostics: dict[str, Any],
    signal_gate_status: str,
    feature_artifact: pd.DataFrame,
    signal_artifact: pd.DataFrame,
    target_path: pd.DataFrame,
    e0_daily: pd.DataFrame,
    e1_daily: pd.DataFrame,
    prices_path: Path,
    modules_config_path: Path,
    b1_policy: B1ExecutionPolicy | None = None,
) -> dict[str, Any]:
    run_id = f"WRP1-{context.generated_at.strftime('%Y%m%dT%H%M%SZ')}-B3-RELATIVE-TILT"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "task_id": "TRADING-513A_to_513D",
        "report_type": "b3_relative_tilt_research_result",
        "status": (
            "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            if signal_gate_status == "B3_SIGNAL_READY" and not e0_daily.empty and not e1_daily.empty
            else signal_gate_status
        ),
        "generated_at": context.generated_at.isoformat(),
        "market_regime": context.etf_config.backtest.backtest.regime,
        "run_id": run_id,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "input_artifacts": {
            "prices_path": str(prices_path),
            "modules_config_path": str(modules_config_path),
            "data_quality_report": str(context.data_quality_output_path),
            "contract_validation_status": context.contract_validation["status"],
        },
        "data_quality_gate": {
            "required_command": "aits validate-data",
            "status": context.data_quality_report.status,
            "passed": context.data_quality_report.passed,
            "error_count": context.data_quality_report.error_count,
            "warning_count": context.data_quality_report.warning_count,
            "info_count": context.data_quality_report.info_count,
            "report_path": str(context.data_quality_output_path),
        },
        "policy": {
            "relative_tilt_signal": {
                "signal_id": signal_policy.signal_id,
                "source_feature_columns": {
                    symbol: list(columns)
                    for symbol, columns in signal_policy.source_feature_columns.items()
                },
                "neutral_score": signal_policy.neutral_score,
                "underweight_score_max": signal_policy.underweight_score_max,
                "overweight_score_min": signal_policy.overweight_score_min,
                "min_coverage": signal_policy.min_coverage,
                "max_stale_days": signal_policy.max_stale_days,
            },
            "target_mapping": target_policy.__dict__,
            "execution_control_policy_id": None if b1_policy is None else b1_policy.policy_id,
        },
        "feature_artifact": {
            "row_count": int(len(feature_artifact)),
            "checksum": _frame_checksum(feature_artifact),
        },
        "signal_artifact": {
            "row_count": int(len(signal_artifact)),
            "checksum": _frame_checksum(signal_artifact),
            "diagnostics_status": diagnostics["status"],
            "signal_gate_status": signal_gate_status,
        },
        "target_path_artifact": {
            "row_count": int(len(target_path)),
            "checksum": _frame_checksum(target_path),
        },
        "signal_diagnostics": diagnostics,
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }
    if not e0_daily.empty and not e1_daily.empty:
        e0_metrics = metrics_from_execution_daily(e0_daily)
        e1_metrics = metrics_from_execution_daily(e1_daily)
        payload.update(
            {
                "b3_e0_metrics": metrics_payload(e0_metrics),
                "b3_e1_metrics": metrics_payload(e1_metrics),
                "b3_e1_vs_b3_e0_comparison": comparison_payload(e1_metrics, e0_metrics),
                "execution_interaction": {
                    "gross_target_turnover": float(e0_daily["turnover"].sum()),
                    "executed_turnover": float(e1_daily["turnover"].sum()),
                    "turnover_saved": float(e0_daily["turnover"].sum())
                    - float(e1_daily["turnover"].sum()),
                    "cost_saved": float(e0_daily["transaction_cost"].sum())
                    - float(e1_daily["transaction_cost"].sum()),
                    "skipped_trades": int((e1_daily["decision"] == "NO_TRADE").sum()),
                },
            }
        )
    payload["reader_brief"] = {
        "summary": (
            "B3 relative-tilt signal, diagnostics, target mapping and E0/E1 "
            "mini-backfill completed."
            if payload["status"] == "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            else "B3 stopped before target/backfill because signal diagnostics did not pass."
        ),
        "key_result": payload["status"],
        "blocking_issues": (
            "none"
            if payload["status"] == "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            else signal_gate_status
        ),
        "warnings": (
            "B3 is research-only and changes relative non-cash asset selection only, "
            "not total equity exposure."
        ),
        "safety_boundary": (
            "research_only=true; official_target_weights=false; production_effect=none"
        ),
        "next_action": "Proceed to B4 interaction only if B2 and B3 are non-BLOCKED.",
    }
    return payload


def write_b3_component_artifacts(
    *,
    feature_artifact: pd.DataFrame,
    signal_artifact: pd.DataFrame,
    target_path: pd.DataFrame,
    e0_daily: pd.DataFrame,
    e1_daily: pd.DataFrame,
    diagnostics: dict[str, Any],
    generated_at: datetime,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(generated_at.isoformat())
    paths = {
        "features": output_dir / f"b3_relative_tilt_feature_artifact_{stamp}.csv",
        "signal": output_dir / f"b3_relative_tilt_signal_artifact_{stamp}.csv",
        "target_path": output_dir / f"b3_relative_tilt_target_path_{stamp}.csv",
        "e0_daily": output_dir / f"b3_e0_naive_execution_daily_{stamp}.csv",
        "e1_daily": output_dir / f"b3_e1_controlled_execution_daily_{stamp}.csv",
        "diagnostics": output_dir / f"b3_signal_diagnostics_{stamp}.json",
    }
    feature_artifact.to_csv(paths["features"], index=False)
    signal_artifact.to_csv(paths["signal"], index=False)
    target_path.to_csv(paths["target_path"], index=False)
    e0_daily.to_csv(paths["e0_daily"], index=False)
    e1_daily.to_csv(paths["e1_daily"], index=False)
    _write_json(paths["diagnostics"], diagnostics)
    return paths


def write_b3_result(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b3_relative_tilt_research_result_{stamp}.json"
    md_path = output_dir / f"b3_relative_tilt_research_result_{stamp}.md"
    markdown = render_b3_result(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    if alias_dir is not None:
        alias_dir.mkdir(parents=True, exist_ok=True)
        _write_json(alias_dir / "b3_relative_tilt_research_result.json", payload)
        (alias_dir / "b3_relative_tilt_research_result.md").write_text(
            markdown,
            encoding="utf-8",
        )
    return json_path, md_path


def render_b3_result(payload: dict[str, Any]) -> str:
    lines = [
        "# B3 Relative Tilt Research Result",
        "",
        f"- Status：{payload['status']}",
        f"- Signal Gate：{payload['signal_artifact']['signal_gate_status']}",
        f"- Data Quality：{payload['data_quality_gate']['status']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b3_e0_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B3-E0", payload["b3_e0_metrics"]),
                _metric_line("B3-E1", payload["b3_e1_metrics"]),
                "",
                "## B3-E1 vs B3-E0",
                "",
            ]
        )
        for key, value in payload["b3_e1_vs_b3_e0_comparison"].items():
            lines.append(f"- {key}：{float(value):.6f}")
        lines.append("")
    lines.extend(
        [
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def load_b3_policies(
    path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
) -> tuple[B3RelativeTiltSignalPolicy, B3TargetMappingPolicy]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"expected YAML mapping: {path}")
    signal = raw.get("b3_relative_tilt_signal")
    mapping = raw.get("b3_target_mapping")
    if not isinstance(signal, dict) or not isinstance(mapping, dict):
        raise ValueError("B3 policy config missing b3_relative_tilt_signal or b3_target_mapping")
    source_columns = {
        str(symbol): tuple(str(column) for column in columns)
        for symbol, columns in dict(signal["source_feature_columns"]).items()
    }
    return (
        B3RelativeTiltSignalPolicy(
            signal_id=str(signal["signal_id"]),
            source_feature_columns=source_columns,
            neutral_score=float(signal["neutral_score"]),
            underweight_score_max=float(signal["underweight_score_max"]),
            overweight_score_min=float(signal["overweight_score_min"]),
            min_coverage=float(signal["min_coverage"]),
            max_stale_days=int(signal["max_stale_days"]),
        ),
        B3TargetMappingPolicy(
            mapping_id=str(mapping["mapping_id"]),
            max_relative_tilt=float(mapping["max_relative_tilt"]),
            preserve_total_equity_exposure=bool(mapping["preserve_total_equity_exposure"]),
            cash_symbol=str(mapping["cash_symbol"]),
        ),
    )


def _relative_signal_score(
    row: pd.Series | None,
    *,
    symbol: str,
    config: ETFConfigBundle,
    policy: B3RelativeTiltSignalPolicy,
) -> tuple[float, float, list[str], dict[str, float]]:
    fields = policy.source_feature_columns.get(symbol, ())
    if row is None:
        return policy.neutral_score, 0.0, [*fields, "missing_symbol_row"], {}
    if not fields:
        return policy.neutral_score, 1.0, [], {}

    scores: list[float] = []
    missing_fields: list[str] = []
    source_values: dict[str, float] = {}
    for field in fields:
        value = _optional_float(row.get(field))
        if value is None:
            missing_fields.append(field)
            continue
        source_values[field] = value
        scores.append(_score_return(value, config))
    coverage = float(len(scores)) / float(max(1, len(fields)))
    if not scores:
        return policy.neutral_score, coverage, missing_fields, source_values
    return sum(scores) / len(scores), coverage, missing_fields, source_values


def _score_return(value: float, config: ETFConfigBundle) -> float:
    mapping = config.strategy.score_mapping
    floor = float(mapping.return_score_floor)
    ceiling = float(mapping.return_score_ceiling)
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 100.0
    return (value - floor) / (ceiling - floor) * 100.0


def _relative_state(score: float, policy: B3RelativeTiltSignalPolicy) -> str:
    if score >= policy.overweight_score_min:
        return "RELATIVE_OVERWEIGHT"
    if score <= policy.underweight_score_max:
        return "RELATIVE_UNDERWEIGHT"
    return "RELATIVE_NEUTRAL"


def _tilt_multiplier(
    score: float,
    signal_policy: B3RelativeTiltSignalPolicy,
    mapping_policy: B3TargetMappingPolicy,
) -> float:
    denominator = max(signal_policy.neutral_score, 100.0 - signal_policy.neutral_score)
    offset = (score - signal_policy.neutral_score) / denominator if denominator else 0.0
    offset = max(-1.0, min(1.0, offset))
    return 1.0 + offset * mapping_policy.max_relative_tilt


def _target_weights_from_tilt(
    baseline: dict[str, float],
    raw_active_weights: dict[str, float],
    *,
    cash_symbol: str,
    preserve_total_equity_exposure: bool,
) -> dict[str, float]:
    target = dict(baseline)
    active_total = sum(raw_active_weights.values())
    baseline_equity = sum(
        weight for symbol, weight in baseline.items() if symbol != cash_symbol and weight > 0.0
    )
    target_equity = baseline_equity if preserve_total_equity_exposure else active_total
    scale = 0.0 if active_total == 0 else target_equity / active_total
    for symbol, raw_weight in raw_active_weights.items():
        target[symbol] = raw_weight * scale
    target[cash_symbol] = 1.0 - sum(
        weight for symbol, weight in target.items() if symbol != cash_symbol
    )
    return target


def _b3_signal_gate_status(
    signal_artifact: pd.DataFrame,
    diagnostics: dict[str, Any],
    policy: B3RelativeTiltSignalPolicy,
) -> str:
    if diagnostics["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED":
        return "B3_SIGNAL_BLOCKED"
    if signal_artifact.empty:
        return "B3_SIGNAL_BLOCKED"
    min_coverage = float(signal_artifact["relative_strength_coverage"].min())
    if min_coverage < policy.min_coverage:
        return "B3_SIGNAL_NEEDS_REVISION"
    return "B3_SIGNAL_READY"


def _blocked_b3_payload(
    *,
    generated_at: datetime,
    start: date,
    end: date,
    reason: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": "TRADING-513A_to_513D",
        "report_type": "b3_relative_tilt_research_result",
        "status": "B3_SIGNAL_BLOCKED",
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "blocking_reason": reason,
        "blocking_details": details,
        "reader_brief": {
            "summary": "B3 blocked before producing signal metrics.",
            "key_result": "B3_SIGNAL_BLOCKED",
            "blocking_issues": reason,
            "warnings": "No B3 metrics should be inferred from blocked output.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "repair blocker before B3 rerun.",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _filter_feature_artifact(features: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] >= pd.Timestamp(start))
        & (frame["_date"] <= pd.Timestamp(end))
    ].copy()
    return selected.drop(columns=["_date"]).reset_index(drop=True)


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _default_weights(config: ETFConfigBundle) -> dict[str, float]:
    return {symbol: float(asset.default_weight) for symbol, asset in config.assets.assets.items()}


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _frame_checksum(frame: pd.DataFrame) -> str:
    records = frame.to_dict(orient="records") if not frame.empty else []
    normalized = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp_from_generated_at(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


def _metric_line(label: str, metrics: dict[str, Any]) -> str:
    return (
        f"- {label} Total Return：{float(metrics['total_return']):.2%}；"
        f"CAGR：{float(metrics['cagr']):.2%}；"
        f"Max Drawdown：{float(metrics['max_drawdown']):.2%}；"
        f"Turnover：{float(metrics['turnover']):.4f}"
    )
