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
class B2RiskSignalPolicy:
    signal_id: str
    source_feature_columns: tuple[str, ...]
    risk_off_score_max: float
    elevated_risk_score_max: float
    min_coverage: float
    max_stale_days: int


@dataclass(frozen=True)
class B2TargetMappingPolicy:
    mapping_id: str
    normal_exposure_scaler: float
    elevated_risk_exposure_scaler: float
    risk_off_exposure_scaler: float
    cash_symbol: str


def run_b2_risk_scaler_research(
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
    risk_policy, target_policy = load_b2_policies(modules_config_path)
    if context.contract_validation["status"] != "PASS" or not context.data_quality_report.passed:
        payload = _blocked_b2_payload(
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="contract_or_data_quality_failed",
            details={
                "contract_status": context.contract_validation["status"],
                "data_quality_status": context.data_quality_report.status,
            },
        )
        json_path, md_path = write_b2_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, {}

    feature_frame = build_feature_store(
        context.prices,
        assets=context.etf_config.assets,
        strategy=context.etf_config.strategy,
        start=context.etf_config.backtest.backtest.warmup_start_date,
        end=end,
    )
    feature_artifact = _filter_feature_artifact(feature_frame, start=start, end=end)
    signal_artifact = build_b2_risk_signal(
        feature_artifact,
        config=context.etf_config,
        policy=risk_policy,
    )
    diagnostics = build_signal_diagnostics_report(
        signal_artifact.rename(columns={"risk_score": "signal_score", "risk_state": "state"}),
        signal_artifact_id=risk_policy.signal_id,
        as_of=end,
        max_stale_days=risk_policy.max_stale_days,
        generated_at=context.generated_at,
    )
    signal_gate_status = _b2_signal_gate_status(signal_artifact, diagnostics, risk_policy)
    if signal_gate_status != "B2_SIGNAL_READY":
        payload = build_b2_result_payload(
            context=context,
            start=start,
            end=end,
            risk_policy=risk_policy,
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
        paths = write_b2_component_artifacts(
            feature_artifact=feature_artifact,
            signal_artifact=signal_artifact,
            target_path=pd.DataFrame(),
            e0_daily=pd.DataFrame(),
            e1_daily=pd.DataFrame(),
            diagnostics=diagnostics,
            generated_at=context.generated_at,
            output_dir=output_dir,
        )
        json_path, md_path = write_b2_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, paths

    target_path = build_b2_target_path(
        signal_artifact,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=target_policy,
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
    payload = build_b2_result_payload(
        context=context,
        start=start,
        end=end,
        risk_policy=risk_policy,
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
    paths = write_b2_component_artifacts(
        feature_artifact=feature_artifact,
        signal_artifact=signal_artifact,
        target_path=target_path,
        e0_daily=e0_daily,
        e1_daily=e1_daily,
        diagnostics=diagnostics,
        generated_at=context.generated_at,
        output_dir=output_dir,
    )
    json_path, md_path = write_b2_result(payload, output_dir=output_dir, alias_dir=alias_dir)
    return payload, json_path, md_path, paths


def build_b2_risk_signal(
    feature_artifact: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    policy: B2RiskSignalPolicy,
) -> pd.DataFrame:
    weights = {
        symbol: float(asset.default_weight)
        for symbol, asset in config.assets.assets.items()
        if symbol != "CASH" and float(asset.default_weight) > 0.0
    }
    expected_symbols = set(weights)
    rows: list[dict[str, Any]] = []
    for signal_date, group in feature_artifact.groupby("date", sort=True):
        by_symbol = {str(row["symbol"]): row for _, row in group.iterrows()}
        risk_scores: list[tuple[float, float]] = []
        missing_symbols: list[str] = []
        for symbol, weight in weights.items():
            row = by_symbol.get(symbol)
            if row is None:
                missing_symbols.append(symbol)
                continue
            score = _risk_score_for_row(row, config)
            if score is None:
                missing_symbols.append(symbol)
                continue
            risk_scores.append((score, weight))
        covered_weight = sum(weight for _, weight in risk_scores)
        total_weight = sum(weights.values())
        coverage = covered_weight / total_weight if total_weight else 0.0
        if risk_scores and covered_weight > 0:
            risk_score = sum(score * weight for score, weight in risk_scores) / covered_weight
        else:
            risk_score = 0.0
        risk_state = _risk_state(risk_score, policy)
        blocking_reason = (
            "none"
            if coverage >= policy.min_coverage
            else "risk_feature_coverage_below_policy_floor"
        )
        rows.append(
            {
                "date": str(signal_date),
                "symbol": "PORTFOLIO",
                "risk_score": risk_score,
                "risk_state": risk_state,
                "confidence": coverage,
                "risk_confidence": coverage,
                "risk_coverage": coverage,
                "missing_symbols_json": json.dumps(sorted(set(missing_symbols))),
                "expected_symbols_json": json.dumps(sorted(expected_symbols)),
                "risk_blocking_reason": blocking_reason,
                "official_target_weights": False,
                "production_effect": "none",
            }
        )
    return pd.DataFrame(rows)


def build_b2_target_path(
    signal_artifact: pd.DataFrame,
    *,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    mapping_policy: B2TargetMappingPolicy,
    start: date,
    end: date,
) -> pd.DataFrame:
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [item.date() for item in close_pivot.index if start <= item.date() <= end]
    signal_by_date = {
        date.fromisoformat(str(row["date"])): row
        for _, row in signal_artifact.iterrows()
    }
    baseline = _default_weights(config)
    rows: list[dict[str, Any]] = []
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        signal_row = signal_by_date.get(signal_date)
        if signal_row is None:
            continue
        scaler = _state_scaler(str(signal_row["risk_state"]), mapping_policy)
        target_weights = _scale_total_exposure(baseline, scaler, mapping_policy.cash_symbol)
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": trading_dates[execution_index].isoformat(),
                "return_date": trading_dates[return_index].isoformat(),
                "target_path_module": "B2",
                "risk_state": str(signal_row["risk_state"]),
                "risk_score": float(signal_row["risk_score"]),
                "exposure_scaler": scaler,
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


def build_b2_result_payload(
    *,
    context: ResearchDataContext,
    start: date,
    end: date,
    risk_policy: B2RiskSignalPolicy,
    target_policy: B2TargetMappingPolicy,
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
    run_id = f"WRP1-{context.generated_at.strftime('%Y%m%dT%H%M%SZ')}-B2-RISK"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "task_id": "TRADING-512C_to_512F",
        "report_type": "b2_risk_scaler_research_result",
        "status": (
            "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            if signal_gate_status == "B2_SIGNAL_READY" and not e0_daily.empty and not e1_daily.empty
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
            "risk_signal": risk_policy.__dict__,
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
                "b2_e0_metrics": metrics_payload(e0_metrics),
                "b2_e1_metrics": metrics_payload(e1_metrics),
                "b2_e1_vs_b2_e0_comparison": comparison_payload(e1_metrics, e0_metrics),
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
        "summary": "B2 risk signal, diagnostics, target mapping and E0/E1 mini-backfill completed."
        if payload["status"] == "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
        else "B2 stopped before target/backfill because signal diagnostics did not pass.",
        "key_result": payload["status"],
        "blocking_issues": "none"
        if payload["status"] == "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
        else signal_gate_status,
        "warnings": (
            "B2 is research-only and changes total exposure only, "
            "not relative asset selection."
        ),
        "safety_boundary": (
            "research_only=true; official_target_weights=false; production_effect=none"
        ),
        "next_action": "Proceed to B3 only if B2 signal diagnostics are non-BLOCKED.",
    }
    return payload


def write_b2_component_artifacts(
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
        "features": output_dir / f"b2_risk_feature_artifact_{stamp}.csv",
        "signal": output_dir / f"b2_risk_signal_artifact_{stamp}.csv",
        "target_path": output_dir / f"b2_risk_target_path_{stamp}.csv",
        "e0_daily": output_dir / f"b2_e0_naive_execution_daily_{stamp}.csv",
        "e1_daily": output_dir / f"b2_e1_controlled_execution_daily_{stamp}.csv",
        "diagnostics": output_dir / f"b2_signal_diagnostics_{stamp}.json",
    }
    feature_artifact.to_csv(paths["features"], index=False)
    signal_artifact.to_csv(paths["signal"], index=False)
    target_path.to_csv(paths["target_path"], index=False)
    e0_daily.to_csv(paths["e0_daily"], index=False)
    e1_daily.to_csv(paths["e1_daily"], index=False)
    _write_json(paths["diagnostics"], diagnostics)
    return paths


def write_b2_result(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b2_risk_scaler_research_result_{stamp}.json"
    md_path = output_dir / f"b2_risk_scaler_research_result_{stamp}.md"
    markdown = render_b2_result(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    if alias_dir is not None:
        alias_dir.mkdir(parents=True, exist_ok=True)
        _write_json(alias_dir / "b2_risk_scaler_research_result.json", payload)
        (alias_dir / "b2_risk_scaler_research_result.md").write_text(
            markdown,
            encoding="utf-8",
        )
    return json_path, md_path


def render_b2_result(payload: dict[str, Any]) -> str:
    lines = [
        "# B2 Risk Scaler Research Result",
        "",
        f"- Status：{payload['status']}",
        f"- Signal Gate：{payload['signal_artifact']['signal_gate_status']}",
        f"- Data Quality：{payload['data_quality_gate']['status']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b2_e0_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B2-E0", payload["b2_e0_metrics"]),
                _metric_line("B2-E1", payload["b2_e1_metrics"]),
                "",
                "## B2-E1 vs B2-E0",
                "",
            ]
        )
        for key, value in payload["b2_e1_vs_b2_e0_comparison"].items():
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


def load_b2_policies(
    path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
) -> tuple[B2RiskSignalPolicy, B2TargetMappingPolicy]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"expected YAML mapping: {path}")
    signal = raw.get("b2_risk_signal")
    mapping = raw.get("b2_target_mapping")
    if not isinstance(signal, dict) or not isinstance(mapping, dict):
        raise ValueError("B2 policy config missing b2_risk_signal or b2_target_mapping")
    return (
        B2RiskSignalPolicy(
            signal_id=str(signal["signal_id"]),
            source_feature_columns=tuple(str(item) for item in signal["source_feature_columns"]),
            risk_off_score_max=float(signal["risk_off_score_max"]),
            elevated_risk_score_max=float(signal["elevated_risk_score_max"]),
            min_coverage=float(signal["min_coverage"]),
            max_stale_days=int(signal["max_stale_days"]),
        ),
        B2TargetMappingPolicy(
            mapping_id=str(mapping["mapping_id"]),
            normal_exposure_scaler=float(mapping["normal_exposure_scaler"]),
            elevated_risk_exposure_scaler=float(mapping["elevated_risk_exposure_scaler"]),
            risk_off_exposure_scaler=float(mapping["risk_off_exposure_scaler"]),
            cash_symbol=str(mapping["cash_symbol"]),
        ),
    )


def _filter_feature_artifact(features: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] >= pd.Timestamp(start))
        & (frame["_date"] <= pd.Timestamp(end))
    ].copy()
    return selected.drop(columns=["_date"]).reset_index(drop=True)


def _risk_score_for_row(row: pd.Series, config: ETFConfigBundle) -> float | None:
    mapping = config.strategy.score_mapping
    required = ["realized_vol_20d", "drawdown_63d", "above_ma_200"]
    if any(pd.isna(row.get(column)) for column in required):
        return None
    score = 100.0
    vol = float(row["realized_vol_20d"])
    score -= _linear_penalty(vol, mapping.vol_low, mapping.vol_high, mapping.vol_max_penalty)
    drawdown = float(row["drawdown_63d"])
    score -= _drawdown_penalty(
        drawdown,
        mapping.drawdown_low,
        mapping.drawdown_high,
        mapping.drawdown_max_penalty,
    )
    if not _as_bool(row["above_ma_200"]):
        score -= mapping.below_ma_200_penalty
    return min(100.0, max(0.0, score))


def _risk_state(score: float, policy: B2RiskSignalPolicy) -> str:
    if score <= policy.risk_off_score_max:
        return "RISK_OFF"
    if score <= policy.elevated_risk_score_max:
        return "ELEVATED_RISK"
    return "NORMAL"


def _state_scaler(state: str, policy: B2TargetMappingPolicy) -> float:
    if state == "RISK_OFF":
        return policy.risk_off_exposure_scaler
    if state == "ELEVATED_RISK":
        return policy.elevated_risk_exposure_scaler
    return policy.normal_exposure_scaler


def _scale_total_exposure(
    baseline: dict[str, float],
    scaler: float,
    cash_symbol: str,
) -> dict[str, float]:
    scaled = {
        symbol: (weight * scaler if symbol != cash_symbol else 0.0)
        for symbol, weight in baseline.items()
    }
    scaled[cash_symbol] = 1.0 - sum(
        weight for symbol, weight in scaled.items() if symbol != cash_symbol
    )
    return scaled


def _b2_signal_gate_status(
    signal_artifact: pd.DataFrame,
    diagnostics: dict[str, Any],
    policy: B2RiskSignalPolicy,
) -> str:
    if diagnostics["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED":
        return "B2_SIGNAL_BLOCKED"
    if signal_artifact.empty:
        return "B2_SIGNAL_BLOCKED"
    min_coverage = float(signal_artifact["risk_coverage"].min())
    if min_coverage < policy.min_coverage:
        return "B2_SIGNAL_NEEDS_REVISION"
    return "B2_SIGNAL_READY"


def _blocked_b2_payload(
    *,
    generated_at: datetime,
    start: date,
    end: date,
    reason: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": "TRADING-512C_to_512F",
        "report_type": "b2_risk_scaler_research_result",
        "status": "B2_SIGNAL_BLOCKED",
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "blocking_reason": reason,
        "blocking_details": details,
        "reader_brief": {
            "summary": "B2 blocked before producing signal metrics.",
            "key_result": "B2_SIGNAL_BLOCKED",
            "blocking_issues": reason,
            "warnings": "No B2 metrics should be inferred from blocked output.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "repair blocker before B2 rerun.",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _linear_penalty(value: float, low: float, high: float, max_penalty: float) -> float:
    if value <= low:
        return 0.0
    if value >= high:
        return max_penalty
    return (value - low) / (high - low) * max_penalty


def _drawdown_penalty(value: float, low: float, high: float, max_penalty: float) -> float:
    if value >= low:
        return 0.0
    if value <= high:
        return max_penalty
    return (low - value) / (low - high) * max_penalty


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "1.0"}


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _default_weights(config: ETFConfigBundle) -> dict[str, float]:
    return {symbol: float(asset.default_weight) for symbol, asset in config.assets.assets.items()}


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
