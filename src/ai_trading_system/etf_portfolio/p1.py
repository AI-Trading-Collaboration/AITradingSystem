from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.etf_portfolio.models import ETFConfigBundle, ETFP1Config
from ai_trading_system.etf_portfolio.signals import score_return
from ai_trading_system.yaml_loader import safe_load_yaml_path

EXPERIMENT_METRIC_KEYS = ("total_return", "cagr", "max_drawdown", "sharpe", "turnover")


def build_relative_strength_table(
    features: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    run_date: date,
) -> pd.DataFrame:
    selected = _features_for_date(features, run_date)
    rows: list[dict[str, object]] = []
    for numerator_symbol, denominator_symbol, meaning in _relative_strength_pairs(config):
        numerator = selected.get(numerator_symbol)
        denominator = selected.get(denominator_symbol)
        if numerator is None or denominator is None:
            rows.append(_missing_pair_row(run_date, numerator_symbol, denominator_symbol, meaning))
            continue
        ratio = _safe_ratio(numerator.get("adj_close"), denominator.get("adj_close"))
        ratio_returns = {
            window: _optional_float(numerator.get(f"rs_vs_{denominator_symbol.lower()}_{window}d"))
            for window in config.strategy.relative_strength.windows
        }
        score_source = ratio_returns.get(60)
        rs_score = 50.0 if score_source is None else score_return(score_source, config.strategy)
        rows.append(
            {
                "date": run_date.isoformat(),
                "pair": f"{numerator_symbol}/{denominator_symbol}",
                "meaning": meaning,
                "ratio": ratio,
                "ratio_ret_20d": ratio_returns.get(20),
                "ratio_ret_60d": ratio_returns.get(60),
                "ratio_ret_120d": ratio_returns.get(120),
                "rs_score": rs_score,
                "direction": _direction(rs_score),
                "reason_codes": json.dumps(
                    [_rs_reason(numerator_symbol, denominator_symbol, score_source)],
                    ensure_ascii=False,
                ),
            }
        )
    return pd.DataFrame(rows)


def build_confirmation_scores(
    relative_strength: pd.DataFrame,
    *,
    p1_config: ETFP1Config,
    run_date: date,
) -> pd.DataFrame:
    pair_scores = {
        str(row["pair"]): _optional_float(row.get("rs_score"))
        for _, row in relative_strength.iterrows()
    }
    semiconductor_score = _average_scores(
        pair_scores.get(pair) for pair in p1_config.confirmation.semiconductor_pairs
    )
    mega_cap_score = _average_scores(
        pair_scores.get(pair) for pair in p1_config.confirmation.mega_cap_pairs
    )
    ai_score = _average_scores([semiconductor_score, mega_cap_score])
    rows = [
        ("AIConfirmationScore", ai_score),
        ("SemiconductorLeadershipScore", semiconductor_score),
        ("MegaCapConfirmationScore", mega_cap_score),
    ]
    return pd.DataFrame(
        [
            {
                "date": run_date.isoformat(),
                "score_id": score_id,
                "score": score,
                "status": (
                    "positive"
                    if score >= p1_config.confirmation.score_positive_min
                    else "neutral_or_limited"
                ),
                "production_effect": "none",
                "model_stage": "observe_only",
            }
            for score_id, score in rows
        ]
    )


def evaluate_satellite_candidates(
    features: pd.DataFrame,
    signals: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    p1_config: ETFP1Config,
    run_date: date,
    regime: str | None = None,
) -> pd.DataFrame:
    feature_rows = _features_for_date(features, run_date)
    signal_rows = _signals_for_date(signals, run_date)
    satellite_cap = _satellite_cap_for_regime(config, regime)
    benchmark_allowed_by_regime = satellite_cap is None or satellite_cap > 0
    rows: list[dict[str, object]] = []
    for symbol, stock_config in sorted(p1_config.satellite_stocks.items()):
        feature_row = feature_rows.get(symbol)
        signal_row = signal_rows.get(symbol)
        if feature_row is None or signal_row is None:
            rows.append(
                {
                    "date": run_date.isoformat(),
                    "symbol": symbol,
                    "benchmark_etf": stock_config.benchmark_etf,
                    "qualified": False,
                    "benchmark_allowed_by_regime": benchmark_allowed_by_regime,
                    "regime": regime or "",
                    "suggested_substitution_weight": 0.0,
                    "reason_codes": json.dumps(["SATELLITE_DATA_MISSING"], ensure_ascii=False),
                    "production_effect": "none",
                }
            )
            continue
        trend_score = _optional_float(signal_row.get("trend_score")) or 0.0
        risk_score = _optional_float(signal_row.get("risk_score")) or 0.0
        rs_value = _optional_float(
            feature_row.get(f"rs_vs_{stock_config.benchmark_etf.lower()}_60d")
        )
        rs_score = 50.0 if rs_value is None else score_return(rs_value, config.strategy)
        qualified = (
            trend_score >= p1_config.satellite_rules.trend_score_min
            and rs_score >= p1_config.satellite_rules.relative_strength_score_min
            and risk_score >= p1_config.satellite_rules.risk_score_min
            and benchmark_allowed_by_regime
        )
        substitution = (
            min(
                stock_config.max_weight,
                p1_config.satellite_rules.default_substitution_weight,
                satellite_cap if satellite_cap is not None else stock_config.max_weight,
            )
            if qualified
            else 0.0
        )
        rows.append(
            {
                "date": run_date.isoformat(),
                "symbol": symbol,
                "benchmark_etf": stock_config.benchmark_etf,
                "trend_score": trend_score,
                "relative_strength_score": rs_score,
                "risk_score": risk_score,
                "qualified": qualified,
                "benchmark_allowed_by_regime": benchmark_allowed_by_regime,
                "regime": regime or "",
                "suggested_substitution_weight": substitution,
                "reason_codes": json.dumps(
                    _satellite_reasons(
                        qualified,
                        trend_score,
                        rs_score,
                        risk_score,
                        benchmark_allowed_by_regime,
                    ),
                    ensure_ascii=False,
                ),
                "production_effect": "none",
            }
        )
    return pd.DataFrame(rows)


def build_portfolio_attribution(
    allocation: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    run_date: date,
) -> pd.DataFrame:
    pivot = _price_pivot(prices)
    trading_dates = [item for item in pivot.index if item <= pd.Timestamp(run_date)]
    if len(trading_dates) < 2:
        raise ValueError("ETF attribution requires at least two trading dates")
    previous_date = trading_dates[-2]
    current_date = trading_dates[-1]
    rows: list[dict[str, object]] = []
    for _, row in allocation.iterrows():
        symbol = str(row["symbol"])
        weight = float(row["target_weight"])
        trade_delta = _optional_float(row.get("trade_delta")) or 0.0
        asset_return = (
            0.0 if symbol == "CASH" else _asset_return(pivot, symbol, previous_date, current_date)
        )
        risk_contribution = (
            0.0 if symbol == "CASH" else weight * _realized_volatility(pivot, symbol, current_date)
        )
        rows.append(
            {
                "date": run_date.isoformat(),
                "symbol": symbol,
                "target_weight": weight,
                "asset_return_1d": asset_return,
                "weight_contribution_1d": weight * asset_return,
                "turnover_contribution": abs(trade_delta),
                "risk_contribution_20d": risk_contribution,
                "sleeve": "cash" if symbol == "CASH" else "etf",
            }
        )
    frame = pd.DataFrame(rows)
    portfolio_return = float(frame["weight_contribution_1d"].sum())
    sleeve_contribution = frame.groupby("sleeve")["weight_contribution_1d"].transform("sum")
    frame["sleeve_contribution_1d"] = sleeve_contribution
    frame["allocation_effect_1d"] = frame["target_weight"] * (
        frame["asset_return_1d"] - portfolio_return
    )
    return frame


def evaluate_event_risk(
    *,
    p1_config: ETFP1Config,
    run_date: date,
) -> pd.DataFrame:
    rows = []
    for event in p1_config.event_calendar.events:
        days_to_event = (event.event_date - run_date).days
        in_window = 0 <= days_to_event <= p1_config.event_calendar.warning_window_days
        rows.append(
            {
                "date": run_date.isoformat(),
                "event_id": event.event_id,
                "name": event.name,
                "event_date": event.event_date.isoformat(),
                "days_to_event": days_to_event,
                "risk_flag": in_window,
                "severity": event.severity,
                "affected_symbols": ",".join(event.affected_symbols),
                "production_effect": "none",
            }
        )
    if rows:
        return pd.DataFrame(rows)
    return pd.DataFrame(
        [
            {
                "date": run_date.isoformat(),
                "event_id": "NO_CONFIGURED_EVENTS",
                "name": "No configured ETF event risk",
                "event_date": "",
                "days_to_event": "",
                "risk_flag": False,
                "severity": "low",
                "affected_symbols": "",
                "production_effect": "none",
            }
        ]
    )


def append_experiment_registry(
    *,
    registry_path: Path,
    model_version: str,
    parent_model_version: str,
    config_hash: str,
    parameter_diff: dict[str, object],
    metrics: dict[str, object],
    status: str,
    notes: str,
    extra_fields: dict[str, object] | None = None,
) -> Path:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "experiment_id": f"etf-exp-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}",
        "created_at": datetime.now(UTC).isoformat(),
        "model_version": model_version,
        "parent_model_version": parent_model_version,
        "config_hash": config_hash,
        "parameter_diff": parameter_diff,
        "metrics": metrics,
        "status": status,
        "notes": notes,
        "production_effect": "none",
        "manual_review_required": True,
    }
    if extra_fields:
        record.update(extra_fields)
    with registry_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return registry_path


def append_experiment_run(
    *,
    registry_path: Path,
    candidate_config_path: Path,
    baseline_config_path: Path,
    config: ETFConfigBundle,
    metrics_path: Path | None = None,
    status: str = "candidate",
    notes: str = "candidate experiment run",
) -> Path:
    candidate_config = _load_yaml_mapping(candidate_config_path)
    baseline_config = _load_yaml_mapping(baseline_config_path)
    parameter_diff = build_parameter_diff(baseline_config, candidate_config)
    metrics = load_experiment_metrics(metrics_path)
    candidate_hash = _file_sha256(candidate_config_path)
    candidate_model_version = _candidate_model_version(candidate_config, config)
    return append_experiment_registry(
        registry_path=registry_path,
        model_version=candidate_model_version,
        parent_model_version=config.strategy.model.version,
        config_hash=config.config_hash,
        parameter_diff=parameter_diff,
        metrics=metrics,
        status=status,
        notes=notes,
        extra_fields={
            "candidate_config_path": str(candidate_config_path),
            "candidate_config_hash": candidate_hash,
            "baseline_config_path": str(baseline_config_path),
            "baseline_config_hash": _file_sha256(baseline_config_path),
            "candidate_only": True,
            "auto_promotion": False,
        },
    )


def build_parameter_diff(
    baseline_config: dict[str, Any],
    candidate_config: dict[str, Any],
) -> dict[str, object]:
    baseline_flat = _flatten_mapping(baseline_config)
    candidate_flat = _flatten_mapping(candidate_config)
    changes: list[dict[str, object]] = []
    for key in sorted(set(baseline_flat) | set(candidate_flat)):
        baseline_value = baseline_flat.get(key)
        candidate_value = candidate_flat.get(key)
        if _normalized_value(baseline_value) == _normalized_value(candidate_value):
            continue
        changes.append(
            {
                "path": key,
                "baseline": baseline_value,
                "candidate": candidate_value,
            }
        )
    return {
        "changed_count": len(changes),
        "changes": changes[:100],
        "truncated": len(changes) > 100,
    }


def load_experiment_metrics(summary_path: Path | None) -> dict[str, object]:
    if summary_path is None:
        return {"metric_status": "MISSING_METRICS"}
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"ETF experiment metrics must be a JSON object: {summary_path}")
    strategy_metrics = payload.get("strategy_metrics", payload)
    if not isinstance(strategy_metrics, dict):
        raise ValueError("ETF experiment metrics missing strategy_metrics object")
    metrics: dict[str, object] = {
        "metric_status": "AVAILABLE",
        "metrics_path": str(summary_path),
    }
    for key in EXPERIMENT_METRIC_KEYS:
        metrics[key] = _optional_float(strategy_metrics.get(key))
    for key in ("market_regime", "data_quality_status", "row_count", "model_version"):
        if key in payload:
            metrics[key] = payload[key]
    return metrics


def load_experiment_registry(registry_path: Path) -> list[dict[str, object]]:
    if not registry_path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(registry_path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if not stripped:
            continue
        item = json.loads(stripped)
        if not isinstance(item, dict):
            raise ValueError(f"ETF experiment registry line {line_number} is not an object")
        rows.append(item)
    return rows


def build_experiment_comparison(
    *,
    registry_path: Path,
    baseline: str = "production",
    baseline_metrics_path: Path | None = None,
) -> pd.DataFrame:
    records = load_experiment_registry(registry_path)
    baseline_metrics = (
        load_experiment_metrics(baseline_metrics_path)
        if baseline_metrics_path is not None
        else _baseline_metrics_from_records(records, baseline)
    )
    if not records:
        return pd.DataFrame(
            [
                {
                    "comparison_status": "MISSING_REGISTRY",
                    "baseline": baseline,
                    "metric_status": "MISSING_METRICS",
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                    "reason_codes": "EXPERIMENT_REGISTRY_MISSING",
                }
            ]
        )
    rows = []
    for record in records:
        metrics = record.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        metric_status = str(metrics.get("metric_status") or "MISSING_METRICS")
        row = {
            "experiment_id": record.get("experiment_id"),
            "model_version": record.get("model_version"),
            "parent_model_version": record.get("parent_model_version"),
            "status": record.get("status"),
            "candidate_config_hash": record.get("candidate_config_hash"),
            "candidate_config_path": record.get("candidate_config_path"),
            "parameter_changed_count": _parameter_changed_count(record),
            "baseline": baseline,
            "metric_status": metric_status,
            "comparison_status": "OBSERVE_ONLY",
            "production_effect": "none",
            "manual_review_required": True,
            "candidate_only": bool(record.get("candidate_only", True)),
            "auto_promotion": False,
            "reason_codes": _experiment_reason_codes(metric_status, baseline_metrics),
        }
        for key in EXPERIMENT_METRIC_KEYS:
            value = _optional_float(metrics.get(key))
            baseline_value = _optional_float(baseline_metrics.get(key))
            row[key] = value
            row[f"baseline_{key}"] = baseline_value
            row[f"delta_{key}"] = (
                None if value is None or baseline_value is None else value - baseline_value
            )
        rows.append(row)
    return pd.DataFrame(rows)


def build_governance_status(config: ETFConfigBundle) -> pd.DataFrame:
    if config.p1 is None:
        raise ValueError("ETF P1 config is required for governance status")
    return pd.DataFrame(
        [
            {
                "model_version": config.strategy.model.version,
                "state": "production",
                "config_hash": config.config_hash,
                "manual_review_required": config.p1.governance.manual_review_required,
                "auto_promotion": config.p1.governance.auto_promotion,
                "min_shadow_observations": config.p1.governance.min_shadow_observations,
                "production_effect": "none",
            }
        ]
    )


def write_frame_and_report(
    frame: pd.DataFrame,
    csv_path: Path,
    markdown_path: Path,
    title: str,
    metadata: dict[str, object] | None = None,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    markdown_path.write_text(
        _render_frame_report(frame, title, csv_path, metadata or {}),
        encoding="utf-8",
    )


def _render_frame_report(
    frame: pd.DataFrame,
    title: str,
    csv_path: Path,
    metadata: dict[str, object],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"- CSV：`{csv_path}`",
        f"- Rows：{len(frame)}",
        "- production_effect：none",
    ]
    for key, value in metadata.items():
        lines.append(f"- {key}：{value}")
    lines.append("")
    if frame.empty:
        lines.append("No rows.")
        return "\n".join(lines) + "\n"
    lines.extend(["```csv", frame.head(20).to_csv(index=False).strip(), "```"])
    return "\n".join(lines) + "\n"


def _features_for_date(features: pd.DataFrame, run_date: date) -> dict[str, pd.Series]:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)]
    return {str(row["symbol"]): row for _, row in selected.iterrows()}


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"ETF experiment config must be a YAML mapping: {path}")
    return raw


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _candidate_model_version(candidate_config: dict[str, Any], config: ETFConfigBundle) -> str:
    model = candidate_config.get("model")
    if isinstance(model, dict) and model.get("version"):
        return str(model["version"])
    metadata = candidate_config.get("policy_metadata")
    if isinstance(metadata, dict) and metadata.get("version"):
        return str(metadata["version"])
    return f"{config.strategy.model.version}-candidate"


def _flatten_mapping(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix or "value": value}
    flattened: dict[str, Any] = {}
    for key, item in sorted(value.items()):
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            flattened.update(_flatten_mapping(item, path))
        else:
            flattened[path] = item
    return flattened


def _normalized_value(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _baseline_metrics_from_records(
    records: list[dict[str, object]],
    baseline: str,
) -> dict[str, object]:
    for record in reversed(records):
        if record.get("status") != baseline and record.get("model_version") != baseline:
            continue
        metrics = record.get("metrics")
        if isinstance(metrics, dict):
            return metrics
    return {"metric_status": "MISSING_METRICS"}


def _parameter_changed_count(record: dict[str, object]) -> int | None:
    parameter_diff = record.get("parameter_diff")
    if not isinstance(parameter_diff, dict):
        return None
    changed = parameter_diff.get("changed_count")
    try:
        return int(changed) if changed is not None else None
    except (TypeError, ValueError):
        return None


def _experiment_reason_codes(
    metric_status: str,
    baseline_metrics: dict[str, object],
) -> str:
    reasons = ["OBSERVE_ONLY", "MANUAL_REVIEW_REQUIRED", "NO_AUTO_PROMOTION"]
    if metric_status != "AVAILABLE":
        reasons.append("MISSING_METRICS")
    if baseline_metrics.get("metric_status") != "AVAILABLE":
        reasons.append("BASELINE_METRICS_MISSING")
    return json.dumps(reasons, ensure_ascii=False)


def _satellite_cap_for_regime(config: ETFConfigBundle, regime: str | None) -> float | None:
    if regime is None:
        return None
    constraint = config.risk.regime_constraints.get(regime)
    if constraint is None:
        return 0.0
    return constraint.satellite_cap


def _relative_strength_pairs(config: ETFConfigBundle) -> list[tuple[str, str, str]]:
    pairs: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_pair(numerator: str, denominator: str, meaning: str) -> None:
        key = (numerator, denominator)
        if key in seen:
            return
        seen.add(key)
        pairs.append((numerator, denominator, meaning))

    for pair in config.strategy.relative_strength.pairs:
        add_pair(pair.numerator, pair.denominator, pair.meaning)

    if config.p1 is None:
        return pairs

    for text in config.p1.confirmation.semiconductor_pairs:
        numerator, denominator = _parse_pair_text(text)
        add_pair(numerator, denominator, "p1_semiconductor_confirmation")
    for text in config.p1.confirmation.mega_cap_pairs:
        numerator, denominator = _parse_pair_text(text)
        add_pair(numerator, denominator, "p1_mega_cap_confirmation")
    for symbol, satellite in sorted(config.p1.satellite_stocks.items()):
        add_pair(symbol, satellite.benchmark_etf, f"p1_satellite_vs_{satellite.benchmark_etf}")
    return pairs


def _parse_pair_text(text: str) -> tuple[str, str]:
    pieces = [piece.strip().upper() for piece in text.split("/")]
    if len(pieces) != 2 or not pieces[0] or not pieces[1]:
        raise ValueError(f"ETF P1 relative strength pair must use NUMERATOR/DENOMINATOR: {text}")
    return pieces[0], pieces[1]


def _signals_for_date(signals: pd.DataFrame, run_date: date) -> dict[str, pd.Series]:
    frame = signals.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[frame["_date"] == pd.Timestamp(run_date)]
    return {str(row["symbol"]): row for _, row in selected.iterrows()}


def _missing_pair_row(
    run_date: date,
    numerator: str,
    denominator: str,
    meaning: str,
) -> dict[str, object]:
    return {
        "date": run_date.isoformat(),
        "pair": f"{numerator}/{denominator}",
        "meaning": meaning,
        "ratio": None,
        "ratio_ret_20d": None,
        "ratio_ret_60d": None,
        "ratio_ret_120d": None,
        "rs_score": 50.0,
        "direction": "missing",
        "reason_codes": json.dumps(["RELATIVE_STRENGTH_PAIR_MISSING"], ensure_ascii=False),
    }


def _optional_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _safe_ratio(left: object, right: object) -> float | None:
    left_value = _optional_float(left)
    right_value = _optional_float(right)
    if left_value is None or right_value is None or right_value == 0:
        return None
    return left_value / right_value


def _rs_reason(numerator: str, denominator: str, value: float | None) -> str:
    if value is None:
        return f"{numerator}_{denominator}_RS_MISSING"
    if value > 0:
        return f"{numerator}_OUTPERFORMS_{denominator}"
    if value < 0:
        return f"{numerator}_UNDERPERFORMS_{denominator}"
    return f"{numerator}_FLAT_VS_{denominator}"


def _direction(score: float) -> str:
    if score >= 60:
        return "outperforming"
    if score <= 40:
        return "underperforming"
    return "neutral"


def _average_scores(values) -> float:
    parsed = [value for value in values if value is not None]
    if not parsed:
        return 50.0
    return sum(parsed) / len(parsed)


def _satellite_reasons(
    qualified: bool,
    trend_score: float,
    rs_score: float,
    risk_score: float,
    benchmark_allowed_by_regime: bool,
) -> list[str]:
    reasons = []
    reasons.append("SATELLITE_QUALIFIED" if qualified else "SATELLITE_NOT_QUALIFIED")
    reasons.append(f"TREND_SCORE_{trend_score:.1f}")
    reasons.append(f"RELATIVE_STRENGTH_SCORE_{rs_score:.1f}")
    reasons.append(f"RISK_SCORE_{risk_score:.1f}")
    reasons.append(
        "BENCHMARK_REGIME_ALLOWED"
        if benchmark_allowed_by_regime
        else "SATELLITE_CAP_ZERO_BY_REGIME"
    )
    return reasons


def _price_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    return frame.pivot(index="_date", columns="symbol", values="_price").sort_index()


def _asset_return(
    pivot: pd.DataFrame,
    symbol: str,
    previous_date: pd.Timestamp,
    current_date: pd.Timestamp,
) -> float:
    if symbol not in pivot.columns:
        return 0.0
    previous = pivot.loc[previous_date, symbol]
    current = pivot.loc[current_date, symbol]
    if pd.isna(previous) or pd.isna(current) or float(previous) == 0:
        return 0.0
    return float(current) / float(previous) - 1.0


def _realized_volatility(pivot: pd.DataFrame, symbol: str, current_date: pd.Timestamp) -> float:
    if symbol not in pivot.columns:
        return 0.0
    history = pivot.loc[pivot.index <= current_date, symbol].dropna().tail(21)
    if len(history) < 2:
        return 0.0
    return float(history.pct_change().dropna().std() * (252**0.5))
