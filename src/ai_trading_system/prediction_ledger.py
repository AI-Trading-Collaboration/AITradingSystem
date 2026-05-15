from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any, Literal

import pandas as pd

from ai_trading_system.backtest.daily import DEFAULT_BENCHMARK_TICKERS, BacktestRegimeContext
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import DataQualityReport
from ai_trading_system.feedback_sample_policy import (
    load_feedback_sample_policy,
    sample_floor_summary,
)

DEFAULT_PREDICTION_LEDGER_PATH = PROJECT_ROOT / "data" / "processed" / "prediction_ledger.csv"
DEFAULT_PREDICTION_OUTCOMES_PATH = PROJECT_ROOT / "data" / "processed" / "prediction_outcomes.csv"
DEFAULT_SHADOW_RUN_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
DEFAULT_SHADOW_MATURITY_REPORT_DIR = PROJECT_ROOT / "outputs" / "reports"
PREDICTION_LEDGER_COLUMNS = (
    "prediction_id",
    "run_id",
    "model_version",
    "rule_version_hash",
    "candidate_id",
    "production_effect",
    "feature_snapshot_id",
    "data_snapshot_id",
    "trace_bundle_ref",
    "instrument_id",
    "decision_time",
    "decision_date",
    "market_regime_id",
    "signal",
    "score",
    "confidence_score",
    "confidence_level",
    "model_target_position",
    "gated_target_position",
    "execution_assumption",
    "label_horizon_days",
    "label_available_time",
    "realized_return",
    "max_drawdown_after_signal",
    "slippage",
    "fee",
    "outcome_status",
)


@dataclass(frozen=True)
class PredictionOutcomeBuildResult:
    as_of: date
    prediction_rows: tuple[dict[str, Any], ...]
    outcome_rows: tuple[dict[str, Any], ...]
    available_rows: tuple[dict[str, Any], ...]
    pending_rows: tuple[dict[str, Any], ...]
    missing_rows: tuple[dict[str, Any], ...]
    horizons: tuple[int, ...]
    strategy_ticker: str
    benchmark_tickers: tuple[str, ...]
    market_regime: BacktestRegimeContext | None
    data_quality_report: DataQualityReport


@dataclass(frozen=True)
class ShadowPredictionRunReport:
    as_of: date
    decision_snapshot_path: Path
    trace_bundle_path: Path
    rule_experiment_path: Path
    prediction_ledger_path: Path
    candidate_count: int
    appended_count: int
    records: tuple[dict[str, Any], ...]
    warnings: tuple[str, ...]
    source_label: str = "Rule experiment ledger"
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if self.warnings or self.appended_count == 0:
            return "PASS_WITH_LIMITATIONS"
        return "PASS"


@dataclass(frozen=True)
class ShadowMaturityReport:
    as_of: date
    outcomes_path: Path
    min_available_samples: int
    review_mode: Literal["promotion", "validation"]
    groups: tuple[dict[str, Any], ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        ready_statuses = {"READY_FOR_GOV_REVIEW", "READY_FOR_VALIDATION_REVIEW"}
        if any(group["maturity_status"] in ready_statuses for group in self.groups):
            return "PASS"
        return "PASS_WITH_LIMITATIONS"


def default_prediction_outcome_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"prediction_outcomes_{as_of.isoformat()}.md"


def default_shadow_prediction_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_predictions_{as_of.isoformat()}.md"


def default_shadow_maturity_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"shadow_maturity_{as_of.isoformat()}.md"


def build_prediction_record_from_decision_snapshot(
    *,
    snapshot: dict[str, Any],
    trace_bundle: dict[str, Any],
    trace_bundle_path: Path,
    features_path: Path,
    data_quality_report_path: Path,
    candidate_id: str = "production",
    production_effect: str = "production",
    instrument_id: str = "AI_PORTFOLIO",
    label_horizon_days: int | None = None,
) -> dict[str, Any]:
    if production_effect not in {"production", "none"}:
        raise ValueError("production_effect must be production or none")
    signal_date = str(snapshot.get("signal_date") or "")
    generated_at = str(snapshot.get("generated_at") or signal_date)
    rule_versions = snapshot.get("rule_versions") or {}
    rule_hash = _stable_hash(rule_versions)
    model_version = f"{candidate_id}:{rule_hash[:12]}"
    run_id = str((trace_bundle.get("run_manifest") or {}).get("run_id") or "")
    positions = snapshot.get("positions") or {}
    scores = snapshot.get("scores") or {}
    final_band = positions.get("final_risk_asset_ai_band") or {}
    model_band = positions.get("model_risk_asset_ai_band") or {}
    prediction_key = "|".join(
        [
            run_id,
            model_version,
            signal_date,
            generated_at,
            str(trace_bundle_path),
        ]
    )
    return {
        "prediction_id": f"prediction:{_stable_hash(prediction_key)[:16]}",
        "run_id": run_id,
        "model_version": model_version,
        "rule_version_hash": rule_hash,
        "candidate_id": candidate_id,
        "production_effect": production_effect,
        "feature_snapshot_id": str(features_path),
        "data_snapshot_id": str(data_quality_report_path),
        "trace_bundle_ref": str(trace_bundle_path),
        "instrument_id": instrument_id,
        "decision_time": generated_at,
        "decision_date": signal_date,
        "market_regime_id": (snapshot.get("market_regime") or {}).get("regime_id"),
        "signal": final_band.get("label"),
        "score": scores.get("overall_score"),
        "confidence_score": scores.get("confidence_score"),
        "confidence_level": scores.get("confidence_level"),
        "model_target_position": _band_midpoint(model_band),
        "gated_target_position": _band_midpoint(final_band),
        "execution_assumption": "advisory_next_session_no_broker_fill",
        "label_horizon_days": "" if label_horizon_days is None else label_horizon_days,
        "label_available_time": "",
        "realized_return": "",
        "max_drawdown_after_signal": "",
        "slippage": "",
        "fee": "",
        "outcome_status": "PENDING",
    }


def append_prediction_records(
    records: tuple[dict[str, Any], ...],
    output_path: Path = DEFAULT_PREDICTION_LEDGER_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(records)
    if new_frame.empty:
        if not output_path.exists():
            pd.DataFrame(columns=PREDICTION_LEDGER_COLUMNS).to_csv(output_path, index=False)
        return output_path
    for column in PREDICTION_LEDGER_COLUMNS:
        if column not in new_frame.columns:
            new_frame[column] = ""
    new_frame = new_frame.loc[:, list(PREDICTION_LEDGER_COLUMNS)]
    if output_path.exists():
        existing = pd.read_csv(output_path, dtype=str, keep_default_na=False)
        missing = set(PREDICTION_LEDGER_COLUMNS) - set(existing.columns)
        if missing:
            raise ValueError(
                "existing prediction ledger is missing columns: "
                f"{', '.join(sorted(missing))}"
            )
        duplicate_ids = set(existing["prediction_id"]) & set(new_frame["prediction_id"])
        if duplicate_ids:
            duplicates = ", ".join(sorted(duplicate_ids))
            raise ValueError(f"prediction ledger duplicate prediction_id: {duplicates}")
        frame = pd.concat(
            [existing.loc[:, list(PREDICTION_LEDGER_COLUMNS)], new_frame],
            ignore_index=True,
        )
    else:
        frame = new_frame
    frame = frame.sort_values(["decision_date", "candidate_id", "prediction_id"])
    frame.to_csv(output_path, index=False)
    return output_path


def load_prediction_ledger(input_path: Path) -> tuple[dict[str, Any], ...]:
    if not input_path.exists():
        return ()
    frame = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    return tuple(frame.to_dict(orient="records"))


def load_prediction_outcomes(input_path: Path) -> tuple[dict[str, Any], ...]:
    if not input_path.exists():
        return ()
    frame = pd.read_csv(input_path, dtype=str, keep_default_na=False)
    return tuple(frame.to_dict(orient="records"))


def build_shadow_prediction_records(
    *,
    snapshot: dict[str, Any],
    trace_bundle: dict[str, Any],
    trace_bundle_path: Path,
    features_path: Path,
    data_quality_report_path: Path,
    rule_experiment_ledger: dict[str, Any],
    as_of: date,
    selected_candidate_ids: tuple[str, ...] = (),
) -> tuple[dict[str, Any], ...]:
    selected = set(selected_candidate_ids)
    records: list[dict[str, Any]] = []
    for candidate in rule_experiment_ledger.get("candidates", []):
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            continue
        if selected and candidate_id not in selected:
            continue
        if not _candidate_shadow_is_runnable(candidate, as_of=as_of):
            continue
        shadow_plan = candidate.get("forward_shadow_plan") or {}
        label_horizon = _positive_int_or_none(shadow_plan.get("min_observation_days"))
        record = build_prediction_record_from_decision_snapshot(
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_bundle_path,
            features_path=features_path,
            data_quality_report_path=data_quality_report_path,
            candidate_id=candidate_id,
            production_effect="none",
            label_horizon_days=label_horizon,
        )
        record["execution_assumption"] = "shadow_candidate_no_order_no_position_change"
        records.append(record)
    return tuple(records)


def build_parameter_shadow_prediction_records(
    *,
    snapshot: dict[str, Any],
    trace_bundle: dict[str, Any],
    trace_bundle_path: Path,
    features_path: Path,
    data_quality_report_path: Path,
    parameter_candidate_ledger: dict[str, Any],
    selected_candidate_ids: tuple[str, ...] = (),
) -> tuple[dict[str, Any], ...]:
    selected = set(selected_candidate_ids)
    records: list[dict[str, Any]] = []
    for candidate in parameter_candidate_ledger.get("candidates", []):
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id:
            continue
        if selected and candidate_id not in selected:
            continue
        if candidate.get("production_effect") not in {None, "", "none"}:
            continue
        if str(candidate.get("recommendation_status") or "") != "READY_FOR_FORWARD_SHADOW":
            continue
        label_horizon = _positive_int_or_none(candidate.get("label_horizon_days"))
        record = build_prediction_record_from_decision_snapshot(
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_bundle_path,
            features_path=features_path,
            data_quality_report_path=data_quality_report_path,
            candidate_id=candidate_id,
            production_effect="none",
            label_horizon_days=label_horizon,
        )
        record["execution_assumption"] = (
            "parameter_flow_validation_shadow_no_order_no_position_change"
        )
        records.append(record)
    return tuple(records)


def build_shadow_prediction_run_report(
    *,
    as_of: date,
    decision_snapshot_path: Path,
    trace_bundle_path: Path,
    rule_experiment_path: Path,
    prediction_ledger_path: Path,
    records: tuple[dict[str, Any], ...],
    candidate_count: int,
    warnings: tuple[str, ...] = (),
    source_label: str = "Rule experiment ledger",
) -> ShadowPredictionRunReport:
    return ShadowPredictionRunReport(
        as_of=as_of,
        decision_snapshot_path=decision_snapshot_path,
        trace_bundle_path=trace_bundle_path,
        rule_experiment_path=rule_experiment_path,
        prediction_ledger_path=prediction_ledger_path,
        candidate_count=candidate_count,
        appended_count=len(records),
        records=records,
        warnings=warnings,
        source_label=source_label,
    )


def write_shadow_prediction_run_report(
    report: ShadowPredictionRunReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_shadow_prediction_run_report(report), encoding="utf-8")
    return output_path


def render_shadow_prediction_run_report(report: ShadowPredictionRunReport) -> str:
    lines = [
        "# Challenger Shadow Runner 报告",
        "",
        f"- 状态：{report.status}",
        f"- 运行日期：{report.as_of.isoformat()}",
        f"- 候选规则数：{report.candidate_count}",
        f"- 写入 prediction 行数：{report.appended_count}",
        f"- Decision snapshot：`{report.decision_snapshot_path}`",
        f"- Trace bundle：`{report.trace_bundle_path}`",
        f"- {report.source_label}：`{report.rule_experiment_path}`",
        f"- Prediction ledger：`{report.prediction_ledger_path}`",
        f"- production_effect={report.production_effect}",
        "- 治理边界：shadow runner 只追加 candidate-only prediction；不写正式日报动作，"
        "不改变 `scores_daily.csv`、position gate、belief_state 或 production rule。",
        "",
        "## 写入记录",
        "",
    ]
    if not report.records:
        lines.append("本次没有可写入的 challenger prediction。")
    else:
        lines.extend(
            [
                "| Prediction | Candidate | Model Version | Decision Date | Production Effect |",
                "|---|---|---|---|---|",
            ]
        )
        for record in report.records:
            lines.append(
                "| "
                f"`{record.get('prediction_id')}` | "
                f"`{record.get('candidate_id')}` | "
                f"`{record.get('model_version')}` | "
                f"{record.get('decision_date')} | "
                f"{record.get('production_effect')} |"
            )
    lines.extend(["", "## 警告", ""])
    if report.warnings:
        lines.extend(f"- {warning}" for warning in report.warnings)
    else:
        lines.append("- 无")
    return "\n".join(lines).rstrip() + "\n"


def build_prediction_outcomes(
    *,
    prediction_rows: tuple[dict[str, Any], ...],
    prices: pd.DataFrame,
    as_of: date,
    horizons: tuple[int, ...],
    strategy_ticker: str = "SMH",
    benchmark_tickers: tuple[str, ...] = DEFAULT_BENCHMARK_TICKERS,
    market_regime: BacktestRegimeContext | None,
    data_quality_report: DataQualityReport,
) -> PredictionOutcomeBuildResult:
    if not horizons:
        raise ValueError("至少需要一个 prediction outcome 观察窗口")
    if any(horizon <= 0 for horizon in horizons):
        raise ValueError("prediction outcome 观察窗口必须为正整数交易日")
    close_pivot = _prepare_close_pivot(prices, (strategy_ticker, *benchmark_tickers))
    rows: list[dict[str, Any]] = []
    for prediction in prediction_rows:
        if not prediction.get("decision_date"):
            continue
        decision_date = date.fromisoformat(str(prediction["decision_date"]))
        for horizon in horizons:
            rows.append(
                _prediction_outcome_row(
                    prediction=prediction,
                    decision_date=decision_date,
                    close_pivot=close_pivot,
                    as_of=as_of,
                    horizon=horizon,
                    strategy_ticker=strategy_ticker,
                    benchmark_tickers=benchmark_tickers,
                )
            )
    available = tuple(row for row in rows if row["outcome_status"] == "AVAILABLE")
    pending = tuple(row for row in rows if row["outcome_status"] == "PENDING")
    missing = tuple(row for row in rows if row["outcome_status"] == "MISSING_DATA")
    return PredictionOutcomeBuildResult(
        as_of=as_of,
        prediction_rows=prediction_rows,
        outcome_rows=tuple(rows),
        available_rows=available,
        pending_rows=pending,
        missing_rows=missing,
        horizons=horizons,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
    )


def build_shadow_maturity_report(
    *,
    outcome_rows: tuple[dict[str, Any], ...],
    outcomes_path: Path,
    as_of: date,
    min_available_samples: int | None = None,
    review_mode: Literal["promotion", "validation"] = "promotion",
) -> ShadowMaturityReport:
    if review_mode not in {"promotion", "validation"}:
        raise ValueError("review_mode must be promotion or validation")
    sample_policy = load_feedback_sample_policy()
    if min_available_samples is None:
        min_available_samples = (
            sample_policy.prediction_outcomes.pilot_floor
            if review_mode == "validation"
            else sample_policy.prediction_outcomes.promotion_floor
        )
    if min_available_samples <= 0:
        raise ValueError("min_available_samples must be positive")
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in outcome_rows:
        key = (
            str(row.get("candidate_id") or "unknown"),
            str(row.get("horizon_days") or "unknown"),
            str(row.get("market_regime_id") or "unknown"),
            str(row.get("production_effect") or "unknown"),
        )
        grouped.setdefault(key, []).append(row)
    groups = tuple(
        _shadow_maturity_group(
            key=key,
            rows=tuple(rows),
            min_available_samples=min_available_samples,
            review_mode=review_mode,
        )
        for key, rows in sorted(grouped.items())
    )
    return ShadowMaturityReport(
        as_of=as_of,
        outcomes_path=outcomes_path,
        min_available_samples=min_available_samples,
        review_mode=review_mode,
        groups=groups,
    )


def write_shadow_maturity_report(
    report: ShadowMaturityReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_shadow_maturity_report(report), encoding="utf-8")
    return output_path


def render_shadow_maturity_report(report: ShadowMaturityReport) -> str:
    lines = [
        "# Forward Shadow 样本成熟度报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Review mode：{report.review_mode}",
        f"- 最低可用样本：{report.min_available_samples}",
        f"- Prediction outcomes：`{report.outcomes_path}`",
        f"- production_effect={report.production_effect}",
        "- 治理边界：validation mode 只允许启动后续验证复核；样本不足、pending outcome "
        "或 missing data 时只能保持 `READY_FOR_SHADOW` / `MISSING`，不能作为 "
        "production rule 晋级证据。",
        "",
        "## Candidate / Horizon 成熟度",
        "",
    ]
    if not report.groups:
        lines.append("暂无 prediction outcome，所有候选规则均不能晋级。")
    else:
        lines.extend(
            [
                (
                    "| Candidate | Horizon | Regime | Production Effect | 状态 | Total | "
                    "Available | Pending | Missing | Avg Return | Hit Rate | Avg MDD | Avg Excess |"
                ),
                "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for group in report.groups:
            lines.append(
                "| "
                f"`{group['candidate_id']}` | "
                f"{group['horizon_days']} | "
                f"{group['market_regime_id']} | "
                f"{group['production_effect']} | "
                f"{group['maturity_status']} | "
                f"{group['total_count']} | "
                f"{group['available_count']} | "
                f"{group['pending_count']} | "
                f"{group['missing_count']} | "
                f"{_format_pct(group['average_ai_proxy_return'])} | "
                f"{_format_pct(group['hit_rate'])} | "
                f"{_format_pct(group['average_ai_proxy_max_drawdown'])} | "
                f"{group['average_excess_return_text']} |"
            )
    return "\n".join(lines).rstrip() + "\n"


def write_prediction_outcomes_csv(
    result: PredictionOutcomeBuildResult,
    output_path: Path = DEFAULT_PREDICTION_OUTCOMES_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(result.outcome_rows)
    if output_path.exists():
        existing = pd.read_csv(output_path)
        required = {"prediction_id", "horizon_days"}
        missing = required - set(existing.columns)
        if missing:
            raise ValueError(
                "existing prediction outcome file is missing columns: "
                f"{', '.join(sorted(missing))}"
            )
        current_keys = set(
            zip(new_frame["prediction_id"], new_frame["horizon_days"], strict=True)
        )
        existing = existing.loc[
            [
                (prediction_id, horizon) not in current_keys
                for prediction_id, horizon in zip(
                    existing["prediction_id"],
                    existing["horizon_days"],
                    strict=True,
                )
            ]
        ]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    sort_columns = [
        column
        for column in ("decision_date", "candidate_id", "prediction_id", "horizon_days")
        if column in new_frame.columns
    ]
    if sort_columns:
        new_frame = new_frame.sort_values(sort_columns).reset_index(drop=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def write_prediction_outcome_report(
    result: PredictionOutcomeBuildResult,
    *,
    outcomes_path: Path,
    data_quality_report_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_prediction_outcome_report(
            result,
            outcomes_path=outcomes_path,
            data_quality_report_path=data_quality_report_path,
        ),
        encoding="utf-8",
    )
    return output_path


def render_prediction_outcome_report(
    result: PredictionOutcomeBuildResult,
    *,
    outcomes_path: Path,
    data_quality_report_path: Path,
) -> str:
    sample_policy = load_feedback_sample_policy()
    lines = [
        "# Prediction / Shadow Outcome 报告",
        "",
        f"- 状态：{_prediction_report_status(result)}",
        f"- 生成日期：{result.as_of.isoformat()}",
        f"- prediction 行数：{len(result.prediction_rows)}",
        f"- outcome 行数：{len(result.outcome_rows)}",
        f"- 可用观察：{len(result.available_rows)}",
        f"- 样本政策：{sample_policy.version}",
        f"- Prediction 样本门槛：{sample_floor_summary(sample_policy.prediction_outcomes)}",
        f"- 等待窗口完成：{len(result.pending_rows)}",
        f"- 缺失价格数据：{len(result.missing_rows)}",
        f"- 观察窗口：{', '.join(f'{horizon}D' for horizon in result.horizons)}",
        f"- AI proxy：{result.strategy_ticker}",
        f"- 对比基准：{', '.join(result.benchmark_tickers)}",
        f"- 数据质量状态：{result.data_quality_report.status}",
        f"- 数据质量报告：`{data_quality_report_path}`",
        f"- 机器可读 outcome：`{outcomes_path}`",
        "- 治理边界：production 与 challenger/shadow 分开统计；"
        "challenger 的 `production_effect=none` 不得改变正式日报或仓位闸门。",
        "",
        "## Production vs Shadow",
        "",
        _group_table(result.available_rows, "candidate_id", result.benchmark_tickers),
        "",
        "## Model Version 分桶",
        "",
        _group_table(result.available_rows, "model_version", result.benchmark_tickers),
    ]
    return "\n".join(lines).rstrip() + "\n"


def _prediction_outcome_row(
    *,
    prediction: dict[str, Any],
    decision_date: date,
    close_pivot: pd.DataFrame,
    as_of: date,
    horizon: int,
    strategy_ticker: str,
    benchmark_tickers: tuple[str, ...],
) -> dict[str, Any]:
    base = {
        "prediction_id": prediction.get("prediction_id"),
        "run_id": prediction.get("run_id"),
        "model_version": prediction.get("model_version"),
        "candidate_id": prediction.get("candidate_id"),
        "production_effect": prediction.get("production_effect"),
        "decision_date": prediction.get("decision_date"),
        "market_regime_id": prediction.get("market_regime_id"),
        "horizon_days": horizon,
        "score": _float_or_none(prediction.get("score")),
        "confidence_score": _float_or_none(prediction.get("confidence_score")),
        "confidence_level": prediction.get("confidence_level"),
        "gated_target_position": _float_or_none(prediction.get("gated_target_position")),
    }
    if strategy_ticker not in close_pivot.columns:
        return {
            **base,
            "outcome_status": "MISSING_DATA",
            "outcome_reason": f"缺少 AI proxy 价格：{strategy_ticker}",
        }
    series = close_pivot[strategy_ticker].dropna()
    if decision_date not in set(series.index.date):
        return {
            **base,
            "outcome_status": "MISSING_DATA",
            "outcome_reason": f"decision_date 无 AI proxy 收盘价：{decision_date.isoformat()}",
        }
    start_index = _date_position(series, decision_date)
    end_index = start_index + horizon
    if end_index >= len(series):
        return {
            **base,
            "outcome_status": "PENDING",
            "outcome_reason": "价格历史尚未覆盖完整观察窗口",
            "available_through": series.index[-1].date().isoformat(),
        }
    end_date = series.index[end_index].date()
    if end_date > as_of:
        return {
            **base,
            "outcome_status": "PENDING",
            "outcome_reason": "观察窗口结束日在本次 as_of 之后",
            "available_through": as_of.isoformat(),
        }
    window = series.iloc[start_index : end_index + 1]
    strategy_return = _window_return(window)
    row = {
        **base,
        "outcome_status": "AVAILABLE",
        "outcome_reason": "",
        "outcome_start_date": decision_date.isoformat(),
        "outcome_end_date": end_date.isoformat(),
        "ai_proxy_ticker": strategy_ticker,
        "ai_proxy_return": strategy_return,
        "ai_proxy_max_drawdown": _max_drawdown(window),
        "hit": strategy_return > 0,
    }
    for benchmark in benchmark_tickers:
        benchmark_return = _benchmark_window_return(
            close_pivot,
            benchmark,
            decision_date,
            end_date,
        )
        row[f"{benchmark}_return"] = benchmark_return
        row[f"excess_{benchmark}_return"] = (
            None if benchmark_return is None else strategy_return - benchmark_return
        )
    return row


def _prepare_close_pivot(prices: pd.DataFrame, tickers: tuple[str, ...]) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"价格数据缺少必需字段：{', '.join(sorted(missing))}")
    frame = prices.loc[prices["ticker"].isin(tickers)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if frame.empty:
        raise ValueError("价格数据中没有 prediction outcome 所需 ticker")
    return (
        frame.pivot_table(index="_date", columns="ticker", values="_adj_close", aggfunc="last")
        .sort_index()
        .ffill()
    )


def _benchmark_window_return(
    close_pivot: pd.DataFrame,
    ticker: str,
    start_date: date,
    end_date: date,
) -> float | None:
    if ticker not in close_pivot.columns:
        return None
    series = close_pivot[ticker].dropna()
    if start_date not in set(series.index.date) or end_date not in set(series.index.date):
        return None
    window = series.iloc[_date_position(series, start_date) : _date_position(series, end_date) + 1]
    return _window_return(window)


def _date_position(series: pd.Series, value: date) -> int:
    matches = series.index[series.index.date == value]
    if len(matches) == 0:
        raise ValueError(f"date not found in series: {value.isoformat()}")
    return int(series.index.get_loc(matches[0]))


def _window_return(window: pd.Series) -> float:
    return float(window.iloc[-1] / window.iloc[0] - 1.0)


def _max_drawdown(window: pd.Series) -> float:
    running_max = window.cummax()
    drawdowns = window / running_max - 1.0
    return float(drawdowns.min())


def _band_midpoint(band: dict[str, Any]) -> float | None:
    min_position = _float_or_none(band.get("min_position"))
    max_position = _float_or_none(band.get("max_position"))
    if min_position is None or max_position is None:
        return None
    return (min_position + max_position) / 2.0


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stable_hash(value: object) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _prediction_report_status(result: PredictionOutcomeBuildResult) -> str:
    diagnostic_floor = load_feedback_sample_policy().prediction_outcomes.diagnostic_floor
    if len(result.available_rows) >= diagnostic_floor:
        return "PASS"
    return "PASS_WITH_LIMITATIONS"


def _candidate_shadow_is_runnable(candidate: dict[str, Any], *, as_of: date) -> bool:
    if candidate.get("production_effect") not in {None, "", "none"}:
        return False
    if candidate.get("approved_for_production") is True:
        return False
    shadow_plan = candidate.get("forward_shadow_plan") or {}
    if shadow_plan.get("production_effect") not in {None, "", "none"}:
        return False
    if str(shadow_plan.get("status") or "").upper() not in {"PENDING", "ACTIVE", "RUNNING"}:
        return False
    start = _date_or_none(shadow_plan.get("start_date"))
    end = _date_or_none(shadow_plan.get("end_date"))
    if start is not None and as_of < start:
        return False
    if end is not None and as_of > end:
        return False
    return True


def _shadow_maturity_group(
    *,
    key: tuple[str, str, str, str],
    rows: tuple[dict[str, Any], ...],
    min_available_samples: int,
    review_mode: Literal["promotion", "validation"],
) -> dict[str, Any]:
    candidate_id, horizon_days, market_regime_id, production_effect = key
    available = tuple(row for row in rows if row.get("outcome_status") == "AVAILABLE")
    pending = tuple(row for row in rows if row.get("outcome_status") == "PENDING")
    missing = tuple(row for row in rows if row.get("outcome_status") == "MISSING_DATA")
    return {
        "candidate_id": candidate_id,
        "horizon_days": horizon_days,
        "market_regime_id": market_regime_id,
        "production_effect": production_effect,
        "total_count": len(rows),
        "available_count": len(available),
        "pending_count": len(pending),
        "missing_count": len(missing),
        "min_available_samples": min_available_samples,
        "maturity_status": _shadow_maturity_status(
            available_count=len(available),
            total_count=len(rows),
            min_available_samples=min_available_samples,
            review_mode=review_mode,
        ),
        "average_ai_proxy_return": _mean(_row_float_values(available, "ai_proxy_return")),
        "average_ai_proxy_max_drawdown": _mean(
            _row_float_values(available, "ai_proxy_max_drawdown")
        ),
        "hit_rate": _hit_rate(available),
        "average_excess_return_text": _average_excess_text(available),
    }


def _shadow_maturity_status(
    *,
    available_count: int,
    total_count: int,
    min_available_samples: int,
    review_mode: Literal["promotion", "validation"],
) -> str:
    if total_count == 0 or available_count == 0:
        return "MISSING"
    if available_count < min_available_samples:
        return "READY_FOR_SHADOW"
    if review_mode == "validation":
        return "READY_FOR_VALIDATION_REVIEW"
    return "READY_FOR_GOV_REVIEW"


def _row_float_values(rows: tuple[dict[str, Any], ...], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _float_or_none(row.get(column))
        if value is not None:
            values.append(value)
    return values


def _hit_rate(rows: tuple[dict[str, Any], ...]) -> float | None:
    if not rows:
        return None
    return sum(str(row.get("hit")).lower() == "true" for row in rows) / len(rows)


def _average_excess_text(rows: tuple[dict[str, Any], ...]) -> str:
    if not rows:
        return "无"
    columns = sorted(
        {
            column
            for row in rows
            for column in row
            if column.startswith("excess_") and column.endswith("_return")
        }
    )
    parts = []
    for column in columns:
        values = _row_float_values(rows, column)
        if values:
            label = column.removeprefix("excess_").removesuffix("_return")
            parts.append(f"{label} {_format_pct(_mean(values))}")
    return "；".join(parts) if parts else "无"


def _positive_int_or_none(value: object) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _date_or_none(value: object) -> date | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _group_table(
    rows: tuple[dict[str, Any], ...],
    group_column: str,
    benchmark_tickers: tuple[str, ...],
) -> str:
    if not rows:
        return "暂无可用 prediction outcome。"
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(group_column, "unknown")), []).append(row)
    preferred_benchmarks = [
        ticker
        for ticker in ("SPY", "QQQ", "SMH", "SOXX")
        if ticker in benchmark_tickers
    ]
    lines = [
        (
            "| 分组 | 样本数 | 平均 AI proxy return | 中位 AI proxy return | "
            "胜率 | 平均最大回撤 | 平均超额收益 |"
        ),
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for group, group_rows in sorted(groups.items()):
        returns = _float_values(group_rows, "ai_proxy_return")
        drawdowns = _float_values(group_rows, "ai_proxy_max_drawdown")
        hit_rate = sum(bool(row.get("hit")) for row in group_rows) / len(group_rows)
        excess_text = "；".join(
            f"{ticker} {_format_pct(_mean(_float_values(group_rows, f'excess_{ticker}_return')))}"
            for ticker in preferred_benchmarks
            if _float_values(group_rows, f"excess_{ticker}_return")
        )
        lines.append(
            "| "
            f"{group} | "
            f"{len(group_rows)} | "
            f"{_format_pct(_mean(returns))} | "
            f"{_format_pct(median(returns) if returns else None)} | "
            f"{hit_rate:.0%} | "
            f"{_format_pct(_mean(drawdowns))} | "
            f"{excess_text or '无'} |"
        )
    return "\n".join(lines)


def _float_values(rows: list[dict[str, Any]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(column)
        if value is None or pd.isna(value):
            continue
        values.append(float(value))
    return values


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _format_pct(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2%}"
