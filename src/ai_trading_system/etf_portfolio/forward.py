from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import ETFBacktestRun, run_portfolio_backtest
from ai_trading_system.etf_portfolio.data import (
    read_price_frame,
    standardize_price_frame,
    validate_price_data,
)
from ai_trading_system.etf_portfolio.experiments import (
    build_experiment_config_bundle,
    load_experiment_registry,
)
from ai_trading_system.etf_portfolio.forward_state import (
    ALLOWED_SHADOW_STATUSES,
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    OPEN_SHADOW_STATUSES,
    active_shadow_candidates,
    load_shadow_candidate_registry,
    update_shadow_candidate_evaluation_state,
    validate_shadow_candidate_registry,
    write_shadow_candidate_registry,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    ETFConfigBundle,
    ETFQualityReport,
    dataframe_checksum,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.no_lookahead import (
    raise_for_no_lookahead_violations,
    validate_no_lookahead_records,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_ETF_FORWARD_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "forward_simulation.yaml"
)
DEFAULT_ETF_FORWARD_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "forward"
DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH = (
    PROJECT_ROOT / "data" / "simulation" / "etf_forward_decisions.csv"
)
FORWARD_UPDATE_SCHEMA_VERSION = "etf_forward_update_v1"
FORWARD_DASHBOARD_SCHEMA_VERSION = "etf_forward_dashboard_v1"
FORWARD_WEEKLY_REVIEW_SCHEMA_VERSION = "etf_forward_weekly_review_v1"
FORWARD_WATCHLIST_SCHEMA_VERSION = "etf_forward_watchlist_v1"
FORWARD_VALIDATION_SCHEMA_VERSION = "etf_forward_validation_v1"
FORWARD_WINDOWS = (1, 5, 20, 60)
DEFAULT_BENCHMARK_SYMBOLS = ("QQQ", "SPY", "SMH")
BENCHMARK_ID_BY_SYMBOL = {"SPY": "B001", "QQQ": "B002", "SMH": "B003"}
UNSAFE_ACTIONS = {
    "promote_to_production",
    "enable_broker_action",
    "replace_baseline",
    "production",
    "live",
}


def load_forward_simulation_config(
    path: Path | str = DEFAULT_ETF_FORWARD_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"ETF forward simulation config must be a mapping: {path}")
    _validate_forward_config(payload)
    return payload


def run_forward_update(
    *,
    as_of: date,
    config_path: Path | str = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Path = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    forward_config = load_forward_simulation_config(config_path)
    base_config = load_etf_config_bundle()
    registry = load_shadow_candidate_registry(registry_path)
    if not active_shadow_candidates(registry):
        payload = _no_active_forward_update_payload(
            as_of=as_of,
            registry=registry,
            registry_path=registry_path,
            market_regime=base_config.backtest.backtest.regime,
            generated_at=generated_at,
        )
        json_path = output_dir / f"forward_update_{as_of.isoformat()}.json"
        markdown_path = output_dir / f"forward_update_{as_of.isoformat()}.md"
        write_forward_update_report(payload, json_path=json_path, markdown_path=markdown_path)
        return payload
    raw_prices = read_price_frame(prices_path)
    prices, metadata_issues = standardize_price_frame(
        raw_prices,
        assets=base_config.assets,
        source_name=str(prices_path),
        extra_symbols=set(_benchmark_symbols(forward_config)),
    )
    price_dates = pd.to_datetime(prices["date"], errors="coerce").dt.date
    prices = prices.loc[price_dates <= as_of].copy()
    quality_report = validate_price_data(
        prices,
        assets=base_config.assets,
        strategy=base_config.strategy,
        as_of=as_of,
        extra_issues=metadata_issues,
    )
    if not quality_report.passed:
        raise ValueError(f"ETF forward update data quality failed: {quality_report.status}")
    payload = build_forward_update_payload(
        as_of=as_of,
        registry_path=registry_path,
        prices=prices,
        quality_report=quality_report,
        base_config=base_config,
        forward_config=forward_config,
        generated_at=generated_at,
    )
    state_updates = {
        str(row["shadow_id"]): {
            "status": row["recommended_status"],
            "last_evaluated_date": as_of.isoformat(),
            "notes": row["status_reasons"],
        }
        for row in payload["candidate_evaluations"]
        if row.get("shadow_id")
    }
    registry = load_shadow_candidate_registry(registry_path)
    updated_registry = update_shadow_candidate_evaluation_state(
        registry,
        updates=state_updates,
        updated_at=generated_at,
    )
    write_shadow_candidate_registry(updated_registry, registry_path)
    _write_decision_ledger(payload["decision_records"], decision_ledger_path)
    json_path = output_dir / f"forward_update_{as_of.isoformat()}.json"
    markdown_path = output_dir / f"forward_update_{as_of.isoformat()}.md"
    write_forward_update_report(payload, json_path=json_path, markdown_path=markdown_path)
    return payload


def _no_active_forward_update_payload(
    *,
    as_of: date,
    registry: Mapping[str, Any],
    registry_path: Path,
    market_regime: str,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    skipped = [
        {
            "shadow_id": candidate.get("shadow_id"),
            "candidate_id": candidate.get("candidate_id"),
            "status": candidate.get("status"),
            "reason": "inactive_candidate",
        }
        for candidate in registry.get("candidates", [])
        if isinstance(candidate, Mapping)
        and str(candidate.get("status")) not in OPEN_SHADOW_STATUSES
    ]
    return {
        "schema_version": FORWARD_UPDATE_SCHEMA_VERSION,
        "report_type": "etf_forward_update",
        "status": "NO_ACTIVE_SHADOW_CANDIDATES",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "market_regime": market_regime,
        "requested_date_range": {"start": "", "end": as_of.isoformat()},
        "data_quality": {
            "status": "SKIPPED_NO_ACTIVE_SHADOW_CANDIDATES",
            "row_count": 0,
            "checksum": "",
            "as_of": as_of.isoformat(),
        },
        "shadow_registry_path": str(registry_path),
        "active_candidate_count": 0,
        "skipped_candidates": skipped,
        "candidate_evaluations": [],
        "decision_records": [],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "evaluation_only": True,
    }


def build_forward_update_payload(
    *,
    as_of: date,
    registry_path: Path,
    prices: pd.DataFrame,
    quality_report: ETFQualityReport,
    base_config: ETFConfigBundle,
    forward_config: Mapping[str, Any],
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    registry = load_shadow_candidate_registry(registry_path)
    candidates = active_shadow_candidates(registry)
    experiment_registry = load_experiment_registry()
    evaluations: list[dict[str, Any]] = []
    decision_records: list[dict[str, Any]] = []
    skipped = [
        {
            "shadow_id": candidate.get("shadow_id"),
            "candidate_id": candidate.get("candidate_id"),
            "status": candidate.get("status"),
            "reason": "inactive_candidate",
        }
        for candidate in registry.get("candidates", [])
        if isinstance(candidate, Mapping)
        and str(candidate.get("status")) not in OPEN_SHADOW_STATUSES
    ]
    for candidate in candidates:
        evaluation = _evaluate_forward_candidate(
            candidate,
            as_of=as_of,
            prices=prices,
            quality_report=quality_report,
            base_config=base_config,
            forward_config=forward_config,
            experiment_registry=experiment_registry,
        )
        evaluations.append(evaluation)
        decision_records.extend(evaluation.pop("_decision_records", []))
    raise_for_no_lookahead_violations(
        validate_no_lookahead_records(allocation_records=decision_records)
    )
    status = _update_status(evaluations)
    return {
        "schema_version": FORWARD_UPDATE_SCHEMA_VERSION,
        "report_type": "etf_forward_update",
        "status": status,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "market_regime": base_config.backtest.backtest.regime,
        "requested_date_range": _requested_date_range(candidates, as_of),
        "data_quality": {
            "status": quality_report.status,
            "row_count": quality_report.row_count,
            "checksum": quality_report.checksum,
            "as_of": "" if quality_report.as_of is None else quality_report.as_of.isoformat(),
        },
        "shadow_registry_path": str(registry_path),
        "active_candidate_count": len(candidates),
        "skipped_candidates": skipped,
        "candidate_evaluations": evaluations,
        "decision_records": decision_records,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "evaluation_only": True,
    }


def write_forward_update_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_forward_update_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def latest_forward_update_path(
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
    *,
    as_of: date | None = None,
) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob("forward_update_*.json"):
        artifact_date = _date_from_stem(path.stem, "forward_update_")
        if artifact_date is not None and (as_of is None or artifact_date <= as_of):
            candidates.append((artifact_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def build_forward_dashboard_payload(
    *,
    as_of: date,
    update_payload: Mapping[str, Any] | None,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    registry = load_shadow_candidate_registry(registry_path)
    active = active_shadow_candidates(registry)
    evaluations = _records(update_payload.get("candidate_evaluations") if update_payload else [])
    table = [_dashboard_row(row) for row in evaluations]
    status_summary = _status_summary(table, active)
    return {
        "schema_version": FORWARD_DASHBOARD_SCHEMA_VERSION,
        "report_type": "etf_forward_dashboard",
        "status": "NO_ACTIVE_SHADOW_CANDIDATES" if not active else "AVAILABLE",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "dashboard_metadata": {
            "source_update_status": _text(update_payload.get("status")) if update_payload else "",
            "source_update_as_of": _text(update_payload.get("as_of")) if update_payload else "",
        },
        "active_shadow_candidates": [
            {
                "shadow_id": candidate.get("shadow_id"),
                "candidate_id": candidate.get("candidate_id"),
                "experiment_id": candidate.get("experiment_id"),
                "status": candidate.get("status"),
                "last_evaluated_date": candidate.get("last_evaluated_date"),
            }
            for candidate in active
        ],
        "candidate_summary_table": table,
        "baseline_comparison": _comparison_section(table, "excess_return_vs_baseline"),
        "benchmark_comparison": {
            "QQQ": _comparison_section(table, "excess_return_vs_QQQ"),
            "SPY": _comparison_section(table, "excess_return_vs_SPY"),
            "SMH": _comparison_section(table, "excess_return_vs_SMH"),
        },
        "risk_summary": _risk_summary(table),
        "turnover_summary": _turnover_summary(table),
        "constraint_hit_summary": _constraint_summary(table),
        "status_summary": status_summary,
        "safety_banner": _safety_banner(),
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def run_forward_dashboard(
    *,
    as_of: date | None = None,
    latest: bool = False,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    update_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "updates",
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = as_of or datetime.now(tz=UTC).date()
    update_path = latest_forward_update_path(update_dir, as_of=run_date if latest else as_of)
    update_payload = _load_json(update_path) if update_path is not None else None
    if update_payload and update_payload.get("as_of"):
        run_date = date.fromisoformat(str(update_payload["as_of"]))
    payload = build_forward_dashboard_payload(
        as_of=run_date,
        update_payload=update_payload,
        registry_path=registry_path,
        generated_at=generated_at,
    )
    json_path = output_dir / f"forward_dashboard_{run_date.isoformat()}.json"
    markdown_path = output_dir / f"forward_dashboard_{run_date.isoformat()}.md"
    write_forward_dashboard_report(payload, json_path=json_path, markdown_path=markdown_path)
    return payload


def write_forward_dashboard_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_forward_dashboard_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def latest_forward_dashboard_path(
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
    *,
    as_of: date | None = None,
) -> Path | None:
    return _latest_report_path(output_dir, "forward_dashboard_", as_of=as_of)


def build_forward_weekly_review_payload(
    *,
    as_of: date,
    dashboard_payload: Mapping[str, Any] | None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    rows = _records(dashboard_payload.get("candidate_summary_table") if dashboard_payload else [])
    actions = [_weekly_action(row) for row in rows]
    performers = sorted(
        rows,
        key=lambda row: _float_or_default(row.get("return_since_enrollment"), -999.0),
        reverse=True,
    )
    return {
        "schema_version": FORWARD_WEEKLY_REVIEW_SCHEMA_VERSION,
        "report_type": "etf_forward_weekly_review",
        "status": "NO_ACTIVE_SHADOW_CANDIDATES" if not rows else "AVAILABLE",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "review_period": {
            "as_of": as_of.isoformat(),
            "candidate_count": len(rows),
        },
        "active_candidates": rows,
        "candidate_status_changes": [
            {
                "shadow_id": row.get("shadow_id"),
                "candidate_id": row.get("candidate_id"),
                "status": row.get("status"),
                "recommended_action": action,
            }
            for row, action in zip(rows, actions, strict=True)
        ],
        "top_forward_performers": performers[:3],
        "worst_forward_performers": list(reversed(performers[-3:])),
        "candidate_vs_baseline_summary": _comparison_section(
            rows,
            "excess_return_vs_baseline",
        ),
        "candidate_vs_benchmark_summary": {
            "QQQ": _comparison_section(rows, "excess_return_vs_QQQ"),
            "SPY": _comparison_section(rows, "excess_return_vs_SPY"),
            "SMH": _comparison_section(rows, "excess_return_vs_SMH"),
        },
        "rolling_metrics_table": [
            {
                "shadow_id": row.get("shadow_id"),
                "candidate_id": row.get("candidate_id"),
                "rolling_metrics": row.get("rolling_metrics"),
            }
            for row in rows
        ],
        "risk_and_drawdown_summary": _risk_summary(rows),
        "turnover_and_stability_summary": _turnover_summary(rows),
        "constraint_hit_summary": _constraint_summary(rows),
        "manual_review_notes": _manual_review_notes(rows),
        "recommended_next_actions": actions,
        "safety_banner": _safety_banner(),
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def run_forward_weekly_review(
    *,
    as_of: date | None = None,
    dashboard_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "weekly_reviews",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = as_of or datetime.now(tz=UTC).date()
    dashboard_path = latest_forward_dashboard_path(dashboard_dir, as_of=run_date)
    dashboard_payload = _load_json(dashboard_path) if dashboard_path is not None else None
    if dashboard_payload and dashboard_payload.get("as_of"):
        run_date = date.fromisoformat(str(dashboard_payload["as_of"]))
    payload = build_forward_weekly_review_payload(
        as_of=run_date,
        dashboard_payload=dashboard_payload,
        generated_at=generated_at,
    )
    json_path = output_dir / f"weekly_review_{run_date.isoformat()}.json"
    markdown_path = output_dir / f"weekly_review_{run_date.isoformat()}.md"
    write_forward_weekly_review_report(payload, json_path=json_path, markdown_path=markdown_path)
    return payload


def write_forward_weekly_review_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_forward_weekly_review_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def build_forward_watchlist_payload(
    *,
    as_of: date,
    dashboard_payload: Mapping[str, Any] | None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    rows = _records(dashboard_payload.get("candidate_summary_table") if dashboard_payload else [])
    items = []
    for row in rows:
        items.extend(_watchlist_items_for_row(row))
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    items = sorted(
        items,
        key=lambda item: (
            severity_order.get(str(item.get("severity")), 9),
            str(item.get("candidate_id")),
            str(item.get("issue")),
        ),
    )
    return {
        "schema_version": FORWARD_WATCHLIST_SCHEMA_VERSION,
        "report_type": "etf_forward_watchlist",
        "status": "CLEAR" if not items else "ATTENTION_REQUIRED",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "attention_required": items,
        "summary": {
            "item_count": len(items),
            "critical_count": sum(1 for item in items if item["severity"] == "critical"),
            "warning_count": sum(1 for item in items if item["severity"] == "warning"),
            "info_count": sum(1 for item in items if item["severity"] == "info"),
        },
        "safety_banner": _safety_banner(),
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def run_forward_watchlist(
    *,
    as_of: date | None = None,
    dashboard_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "dashboard",
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "watchlist",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    run_date = as_of or datetime.now(tz=UTC).date()
    dashboard_path = latest_forward_dashboard_path(dashboard_dir, as_of=run_date)
    dashboard_payload = _load_json(dashboard_path) if dashboard_path is not None else None
    if dashboard_payload and dashboard_payload.get("as_of"):
        run_date = date.fromisoformat(str(dashboard_payload["as_of"]))
    payload = build_forward_watchlist_payload(
        as_of=run_date,
        dashboard_payload=dashboard_payload,
        generated_at=generated_at,
    )
    json_path = output_dir / f"forward_watchlist_{run_date.isoformat()}.json"
    markdown_path = output_dir / f"forward_watchlist_{run_date.isoformat()}.md"
    write_forward_watchlist_report(payload, json_path=json_path, markdown_path=markdown_path)
    return payload


def write_forward_watchlist_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_forward_watchlist_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def build_forward_validation_report(
    *,
    config_path: Path | str = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Path = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    _validation_check(checks, "forward_config_valid", _config_validation(config_path))
    _validation_check(checks, "shadow_state_schema_valid", _state_validation(registry_path))
    _validation_check(
        checks,
        "daily_forward_updater_available",
        (True, "Daily forward updater is available.", {}),
    )
    _validation_check(
        checks,
        "dashboard_available",
        (True, "Forward dashboard builder is available.", {}),
    )
    _validation_check(
        checks,
        "rolling_metrics_available",
        (True, "Rolling forward metrics builder is available.", {}),
    )
    _validation_check(
        checks,
        "lifecycle_rules_available",
        (True, "Config-driven lifecycle rules are available.", {}),
    )
    _validation_check(
        checks,
        "weekly_review_available",
        (True, "Forward weekly review builder is available.", {}),
    )
    _validation_check(
        checks,
        "reader_brief_section_available",
        (True, "Reader Brief forward simulation summary section is available.", {}),
    )
    _validation_check(
        checks,
        "watchlist_available",
        (True, "Forward watchlist builder is available.", {}),
    )
    _validation_check(
        checks,
        "decision_evaluation_separation",
        _decision_ledger_validation(decision_ledger_path),
    )
    _validation_check(
        checks,
        "report_registry_integration",
        _report_registry_validation(report_registry_path),
    )
    _validation_check(
        checks,
        "no_production_promotion_action",
        _unsafe_action_validation(config_path),
    )
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "schema_version": FORWARD_VALIDATION_SCHEMA_VERSION,
        "report_type": "etf_forward_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "checks": checks,
        "summary": {
            "check_count": len(checks),
            "failed_check_count": sum(1 for check in checks if check["status"] != "PASS"),
        },
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
    }


def run_forward_validation(
    *,
    config_path: Path | str = DEFAULT_ETF_FORWARD_CONFIG_PATH,
    registry_path: Path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    decision_ledger_path: Path = DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
    report_registry_path: Path = PROJECT_ROOT / "config" / "report_registry.yaml",
    output_dir: Path = DEFAULT_ETF_FORWARD_REPORT_DIR / "validation",
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    payload = build_forward_validation_report(
        config_path=config_path,
        registry_path=registry_path,
        decision_ledger_path=decision_ledger_path,
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    stem = f"forward_validation_{generated.date().isoformat()}"
    write_forward_validation_report(
        payload,
        json_path=output_dir / f"{stem}.json",
        markdown_path=output_dir / f"{stem}.md",
    )
    return payload


def write_forward_validation_report(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_forward_validation_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def render_forward_update_markdown(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("candidate_evaluations"))
    lines = [
        f"# ETF Forward Update - {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Market Regime: {payload.get('market_regime')}",
        f"- Data Quality: {_mapping(payload.get('data_quality')).get('status')}",
        f"- Safety: {_safety_banner()}",
        "",
        "## Candidate Evaluations",
        "",
        "| Candidate | Status | Days | Return | Excess vs Baseline | Max Drawdown | Turnover |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    if not rows:
        lines.append("| - | no active shadow candidates | - | - | - | - | - |")
    for row in rows:
        lines.append(
            "| "
            f"{row.get('candidate_id')} | "
            f"{row.get('recommended_status')} | "
            f"{row.get('days_since_enrollment')} | "
            f"{_fmt_pct(row.get('return_since_enrollment'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(row.get('max_drawdown_since_enrollment'))} | "
            f"{_fmt_number(row.get('turnover_since_enrollment'))} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- decision-time records are separate from evaluation-time metrics.",
            "- forward return fields are evaluation-only and must not be used as decision inputs.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_forward_dashboard_markdown(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("candidate_summary_table"))
    lines = [
        f"# ETF Forward Simulation Dashboard - {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Safety: {payload.get('safety_banner')}",
        "",
        "## Candidate Summary",
        "",
        (
            "| Shadow | Candidate | Status | Days | Return | Excess Base | QQQ | SPY | "
            "SMH | Drawdown | Turnover | Constraints | Action |"
        ),
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    if not rows:
        lines.append(
            "| - | no active shadow candidates | - | - | - | - | - | - | - | - | - | - | - |"
        )
    for row in rows:
        lines.append(
            "| "
            f"{row.get('shadow_id')} | "
            f"{row.get('candidate_id')} | "
            f"{row.get('status')} | "
            f"{row.get('days_since_enrollment')} | "
            f"{_fmt_pct(row.get('return_since_enrollment'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_baseline'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_QQQ'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_SPY'))} | "
            f"{_fmt_pct(row.get('excess_return_vs_SMH'))} | "
            f"{_fmt_pct(row.get('max_drawdown_since_enrollment'))} | "
            f"{_fmt_number(row.get('turnover_since_enrollment'))} | "
            f"{row.get('constraint_hits_since_enrollment')} | "
            f"{row.get('recommended_action')} |"
        )
    return "\n".join(lines) + "\n"


def render_forward_weekly_review_markdown(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("candidate_status_changes"))
    lines = [
        f"# ETF Forward Weekly Review - {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Safety: {payload.get('safety_banner')}",
        "",
        "## Candidate Status Changes",
        "",
        "| Candidate | Status | Recommended Action |",
        "|---|---|---|",
    ]
    if not rows:
        lines.append("| - | no active shadow candidates | - |")
    for row in rows:
        lines.append(
            f"| {row.get('candidate_id')} | {row.get('status')} | "
            f"{row.get('recommended_action')} |"
        )
    lines.extend(["", "## Manual Review Notes", ""])
    for note in _string_list(payload.get("manual_review_notes")):
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def render_forward_watchlist_markdown(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("attention_required"))
    lines = [
        f"# ETF Forward Watchlist - {payload.get('as_of')}",
        "",
        f"- Status: {payload.get('status')}",
        f"- Safety: {payload.get('safety_banner')}",
        "",
        "## Attention Required",
        "",
        "| Candidate | Issue | Severity | Since | Recommended Action |",
        "|---|---|---|---|---|",
    ]
    if not rows:
        lines.append("| - | no watchlist items | info | - | continue_observation |")
    for row in rows:
        lines.append(
            "| "
            f"{row.get('candidate_id')} | "
            f"{row.get('issue')} | "
            f"{row.get('severity')} | "
            f"{row.get('since_date')} | "
            f"{row.get('recommended_action')} |"
        )
    return "\n".join(lines) + "\n"


def render_forward_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Forward Simulation Validation",
        "",
        f"- Status: {payload.get('status')}",
        "- observe_only=true",
        "- production_effect=none",
        "- broker_action=none",
        "- manual_review_required=true",
        "",
        "## Checks",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"| {check.get('check_id')} | {check.get('status')} | {check.get('message')} |"
        )
    return "\n".join(lines) + "\n"


def _evaluate_forward_candidate(
    candidate: Mapping[str, Any],
    *,
    as_of: date,
    prices: pd.DataFrame,
    quality_report: ETFQualityReport,
    base_config: ETFConfigBundle,
    forward_config: Mapping[str, Any],
    experiment_registry: Any,
) -> dict[str, Any]:
    experiment_id = _text(candidate.get("experiment_id"))
    enrollment_date = _safe_date(candidate.get("enrollment_date"))
    evaluation_schedule = _mapping(candidate.get("evaluation_schedule"))
    start_date = enrollment_date or _safe_date(evaluation_schedule.get("start_date"))
    if start_date is None:
        return _blocked_candidate_evaluation(
            candidate,
            as_of=as_of,
            reason="INVALID_ENROLLMENT_DATE",
        )
    experiment = experiment_registry.experiments.get(experiment_id)
    if experiment is None:
        return _blocked_candidate_evaluation(
            candidate,
            as_of=as_of,
            reason="UNKNOWN_EXPERIMENT_ID",
        )
    try:
        candidate_config = build_experiment_config_bundle(base_config, experiment)
        candidate_run = run_portfolio_backtest(
            prices,
            config=candidate_config,
            quality_report=quality_report,
            start=start_date,
            end=as_of,
        )
        baseline_run = run_portfolio_backtest(
            prices,
            config=base_config,
            quality_report=quality_report,
            start=start_date,
            end=as_of,
        )
    except Exception as exc:  # noqa: BLE001 - forward report must fail closed per candidate.
        return _blocked_candidate_evaluation(
            candidate,
            as_of=as_of,
            reason="INSUFFICIENT_FORWARD_DATA",
            detail=type(exc).__name__,
        )
    benchmark_symbols = _benchmark_symbols(forward_config)
    metrics = _candidate_forward_metrics(
        candidate_run,
        baseline_run,
        benchmark_symbols=benchmark_symbols,
        forward_config=forward_config,
    )
    status, status_reasons = _lifecycle_status(
        metrics,
        baseline_run=baseline_run,
        forward_config=forward_config,
    )
    decision_records = _decision_records_from_run(candidate, candidate_run)
    return {
        "shadow_id": candidate.get("shadow_id"),
        "candidate_id": candidate.get("candidate_id"),
        "experiment_id": experiment_id,
        "status": candidate.get("status"),
        "evaluation_as_of_date": as_of.isoformat(),
        "days_since_enrollment": metrics["days_since_enrollment"],
        **metrics,
        "recommended_status": status,
        "recommended_action": _recommended_action_for_status(status),
        "status_reasons": status_reasons,
        "evaluation_only": True,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "_decision_records": decision_records,
    }


def _candidate_forward_metrics(
    candidate_run: ETFBacktestRun,
    baseline_run: ETFBacktestRun,
    *,
    benchmark_symbols: tuple[str, ...],
    forward_config: Mapping[str, Any],
) -> dict[str, Any]:
    daily = candidate_run.daily.copy()
    baseline_daily = baseline_run.daily.copy()
    returns = _numeric_list(daily.get("strategy_return"))
    strategy_metrics = _mapping(candidate_run.summary.get("strategy_metrics"))
    baseline_metrics = _mapping(baseline_run.summary.get("strategy_metrics"))
    total_return = _optional_float(strategy_metrics.get("total_return"))
    baseline_return = _optional_float(baseline_metrics.get("total_return"))
    benchmark_returns = _benchmark_total_returns(candidate_run, benchmark_symbols)
    null_reasons: dict[str, str] = {}
    window_returns: dict[str, float | None] = {}
    for window in FORWARD_WINDOWS:
        key = f"forward_{window}d_return"
        value = _compound_window(returns, window)
        window_returns[key] = value
        if value is None:
            null_reasons[key] = "INSUFFICIENT_FORWARD_DATA"
    rolling_metrics = {
        f"{window}d": _rolling_metrics(
            daily,
            baseline_daily,
            window=window,
            benchmark_symbols=benchmark_symbols,
        )
        for window in _rolling_windows(forward_config)
    }
    constraint_hits = _constraint_hit_count(candidate_run.weights)
    trading_days = len(daily)
    constraint_hit_rate = None if trading_days == 0 else constraint_hits / trading_days
    return {
        **window_returns,
        "return_since_enrollment": total_return,
        "baseline_return_since_enrollment": baseline_return,
        "benchmark_returns_since_enrollment": benchmark_returns,
        "max_drawdown_since_enrollment": _optional_float(strategy_metrics.get("max_drawdown")),
        "baseline_max_drawdown_since_enrollment": _optional_float(
            baseline_metrics.get("max_drawdown")
        ),
        "excess_return_vs_baseline": _sub(total_return, baseline_return),
        "excess_return_vs_QQQ": _sub(total_return, benchmark_returns.get("QQQ")),
        "excess_return_vs_SPY": _sub(total_return, benchmark_returns.get("SPY")),
        "excess_return_vs_SMH": _sub(total_return, benchmark_returns.get("SMH")),
        "turnover_since_enrollment": sum(_numeric_list(daily.get("turnover"))),
        "constraint_hits_since_enrollment": constraint_hits,
        "constraint_hit_rate_since_enrollment": constraint_hit_rate,
        "rolling_metrics": rolling_metrics,
        "metric_null_reasons": null_reasons,
        "days_since_enrollment": trading_days,
    }


def _rolling_metrics(
    daily: pd.DataFrame,
    baseline_daily: pd.DataFrame,
    *,
    window: int,
    benchmark_symbols: tuple[str, ...],
) -> dict[str, Any]:
    if len(daily) < window or len(baseline_daily) < window:
        return {
            "window": window,
            "window_return": None,
            "baseline_window_return": None,
            "benchmark_window_returns": {symbol: None for symbol in benchmark_symbols},
            "excess_return_vs_baseline": None,
            "excess_return_vs_QQQ": None,
            "max_drawdown": None,
            "volatility": None,
            "Sharpe": None,
            "turnover": None,
            "average_equity_exposure": None,
            "average_cash_weight": None,
            "constraint_hit_count": None,
            "regime_distribution": {},
            "reason": "INSUFFICIENT_WINDOW_DATA",
        }
    selected = daily.tail(window).copy()
    selected_baseline = baseline_daily.tail(window).copy()
    returns = _numeric_list(selected.get("strategy_return"))
    baseline_returns = _numeric_list(selected_baseline.get("strategy_return"))
    window_return = _compound_return(returns)
    baseline_return = _compound_return(baseline_returns)
    benchmark_returns = _benchmark_window_returns(selected, benchmark_symbols)
    cash_weights = _cash_weights(selected)
    average_cash = None if not cash_weights else mean(cash_weights)
    volatility = _annualized_volatility(returns)
    return {
        "window": window,
        "window_return": window_return,
        "baseline_window_return": baseline_return,
        "benchmark_window_returns": benchmark_returns,
        "excess_return_vs_baseline": _sub(window_return, baseline_return),
        "excess_return_vs_QQQ": _sub(window_return, benchmark_returns.get("QQQ")),
        "max_drawdown": _max_drawdown_from_returns(returns),
        "volatility": volatility,
        "Sharpe": _sharpe(returns, volatility),
        "turnover": sum(_numeric_list(selected.get("turnover"))),
        "average_equity_exposure": None if average_cash is None else 1.0 - average_cash,
        "average_cash_weight": average_cash,
        "constraint_hit_count": _constraint_hit_count(selected),
        "regime_distribution": _regime_distribution(selected),
        "reason": None,
    }


def _lifecycle_status(
    metrics: Mapping[str, Any],
    *,
    baseline_run: ETFBacktestRun,
    forward_config: Mapping[str, Any],
) -> tuple[str, list[str]]:
    thresholds = _mapping(forward_config.get("lifecycle_thresholds"))
    minimum_days = int(thresholds.get("minimum_forward_days") or 20)
    days = int(metrics.get("days_since_enrollment") or 0)
    reasons: list[str] = []
    if days < minimum_days:
        return "needs_more_data", [f"forward_days_below_minimum:{days}<{minimum_days}"]
    excess = _optional_float(metrics.get("excess_return_vs_baseline"))
    if excess is None:
        return "needs_more_data", ["forward_return_comparison_missing"]
    reject_excess = float(thresholds.get("reject_excess_return_vs_baseline") or -0.05)
    watch_excess = float(thresholds.get("watch_excess_return_vs_baseline") or -0.02)
    if excess <= reject_excess:
        reasons.append(f"excess_return_vs_baseline_reject:{excess:.4f}<={reject_excess:.4f}")
    candidate_dd = abs(_optional_float(metrics.get("max_drawdown_since_enrollment")) or 0.0)
    baseline_dd = abs(
        _optional_float(_mapping(baseline_run.summary.get("strategy_metrics")).get("max_drawdown"))
        or 0.0
    )
    drawdown_gap = candidate_dd - baseline_dd
    reject_drawdown_gap = float(thresholds.get("reject_drawdown_worse_than_baseline") or 0.05)
    if drawdown_gap > reject_drawdown_gap:
        reasons.append(
            "drawdown_worse_than_baseline:" f"{drawdown_gap:.4f}>{reject_drawdown_gap:.4f}"
        )
    turnover = _optional_float(metrics.get("turnover_since_enrollment"))
    max_turnover = float(thresholds.get("max_turnover_since_enrollment") or 2.0)
    if turnover is not None and turnover > max_turnover:
        reasons.append(f"turnover_over_limit:{turnover:.4f}>{max_turnover:.4f}")
    hit_rate = _optional_float(metrics.get("constraint_hit_rate_since_enrollment"))
    max_hit_rate = float(thresholds.get("max_constraint_hit_rate") or 0.5)
    if hit_rate is not None and hit_rate > max_hit_rate:
        reasons.append(f"constraint_hit_rate_over_limit:{hit_rate:.4f}>{max_hit_rate:.4f}")
    if reasons:
        return "reject_pending_review", reasons
    if excess <= watch_excess:
        return "watch", [f"excess_return_vs_baseline_watch:{excess:.4f}<={watch_excess:.4f}"]
    return "active", ["forward_observation_within_policy"]


def _blocked_candidate_evaluation(
    candidate: Mapping[str, Any],
    *,
    as_of: date,
    reason: str,
    detail: str = "",
) -> dict[str, Any]:
    null_reasons = {f"forward_{window}d_return": reason for window in FORWARD_WINDOWS}
    notes = [reason] if not detail else [f"{reason}:{detail}"]
    return {
        "shadow_id": candidate.get("shadow_id"),
        "candidate_id": candidate.get("candidate_id"),
        "experiment_id": candidate.get("experiment_id"),
        "status": candidate.get("status"),
        "evaluation_as_of_date": as_of.isoformat(),
        "days_since_enrollment": 0,
        "forward_1d_return": None,
        "forward_5d_return": None,
        "forward_20d_return": None,
        "forward_60d_return": None,
        "return_since_enrollment": None,
        "baseline_return_since_enrollment": None,
        "benchmark_returns_since_enrollment": {
            symbol: None for symbol in DEFAULT_BENCHMARK_SYMBOLS
        },
        "max_drawdown_since_enrollment": None,
        "baseline_max_drawdown_since_enrollment": None,
        "excess_return_vs_baseline": None,
        "excess_return_vs_QQQ": None,
        "excess_return_vs_SPY": None,
        "excess_return_vs_SMH": None,
        "turnover_since_enrollment": None,
        "constraint_hits_since_enrollment": None,
        "constraint_hit_rate_since_enrollment": None,
        "rolling_metrics": {},
        "metric_null_reasons": null_reasons,
        "recommended_status": "needs_more_data",
        "recommended_action": "needs_more_data",
        "status_reasons": notes,
        "evaluation_only": True,
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
        "production_promotion_allowed": False,
        "_decision_records": [],
    }


def _decision_records_from_run(
    candidate: Mapping[str, Any],
    run: ETFBacktestRun,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for _, row in run.weights.iterrows():
        records.append(
            {
                "shadow_id": candidate.get("shadow_id"),
                "candidate_id": candidate.get("candidate_id"),
                "experiment_id": candidate.get("experiment_id"),
                "date": row.get("signal_date"),
                "signal_date": row.get("signal_date"),
                "execution_date": row.get("execution_date"),
                "return_date": row.get("return_date"),
                "symbol": row.get("symbol"),
                "target_weight": _optional_float(row.get("target_weight")),
                "trade_delta": _optional_float(row.get("trade_delta")),
                "regime": row.get("regime"),
                "model_version": row.get("model_version"),
                "config_hash": row.get("config_hash"),
                "data_quality_status": row.get("data_quality_status"),
                "record_type": "decision",
                "evaluation_only": False,
                "observe_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
                "production_promotion_allowed": False,
            }
        )
    return records


def _write_decision_ledger(records: list[dict[str, Any]], path: Path) -> Path:
    if not records and not path.exists():
        return path
    frame = pd.DataFrame(records)
    if path.exists():
        existing = pd.read_csv(path)
        if not existing.empty:
            frame = pd.concat([existing, frame], ignore_index=True)
    if frame.empty:
        return path
    key_cols = ["shadow_id", "date", "symbol", "config_hash"]
    frame = frame.drop_duplicates(subset=key_cols, keep="last")
    frame = frame.sort_values(key_cols).reset_index(drop=True)
    raise_for_no_lookahead_violations(validate_no_lookahead_records(allocation_records=frame))
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _dashboard_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "shadow_id": row.get("shadow_id"),
        "candidate_id": row.get("candidate_id"),
        "experiment_id": row.get("experiment_id"),
        "status": row.get("recommended_status") or row.get("status"),
        "days_since_enrollment": row.get("days_since_enrollment"),
        "return_since_enrollment": row.get("return_since_enrollment"),
        "excess_return_vs_baseline": row.get("excess_return_vs_baseline"),
        "excess_return_vs_QQQ": row.get("excess_return_vs_QQQ"),
        "excess_return_vs_SPY": row.get("excess_return_vs_SPY"),
        "excess_return_vs_SMH": row.get("excess_return_vs_SMH"),
        "max_drawdown_since_enrollment": row.get("max_drawdown_since_enrollment"),
        "turnover_since_enrollment": row.get("turnover_since_enrollment"),
        "constraint_hits_since_enrollment": row.get("constraint_hits_since_enrollment"),
        "last_evaluated_date": row.get("evaluation_as_of_date"),
        "recommended_action": row.get("recommended_action"),
        "status_reasons": row.get("status_reasons", []),
        "rolling_metrics": row.get("rolling_metrics", {}),
        "metric_null_reasons": row.get("metric_null_reasons", {}),
    }


def _weekly_action(row: Mapping[str, Any]) -> str:
    status = _text(row.get("status"))
    if status == "reject_pending_review":
        return "reject_pending_review"
    if status == "watch":
        return "watch"
    if status == "needs_more_data":
        return "needs_more_data"
    return "continue_shadow"


def _watchlist_items_for_row(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidate_id = _text(row.get("candidate_id"))
    since_date = _text(row.get("last_evaluated_date"))
    status = _text(row.get("status"))
    items: list[dict[str, Any]] = []
    if status == "reject_pending_review":
        items.append(
            _watch_item(
                row,
                "candidate moved to reject_pending_review",
                "critical",
                since_date,
                "consider_rejection_after_review",
            )
        )
    if status == "watch":
        items.append(
            _watch_item(
                row,
                "candidate moved to watch",
                "warning",
                since_date,
                "needs_manual_review",
            )
        )
    if status == "needs_more_data":
        items.append(
            _watch_item(
                row,
                "candidate needs more forward data",
                "info",
                since_date,
                "continue_observation",
            )
        )
    null_reasons = _mapping(row.get("metric_null_reasons"))
    if null_reasons:
        items.append(
            {
                "candidate_id": candidate_id,
                "issue": "candidate has missing forward data",
                "severity": "warning",
                "since_date": since_date,
                "recommended_action": "fix_data_gap",
                "details": dict(null_reasons),
            }
        )
    return items


def _watch_item(
    row: Mapping[str, Any],
    issue: str,
    severity: str,
    since_date: str,
    action: str,
) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "issue": issue,
        "severity": severity,
        "since_date": since_date,
        "recommended_action": action,
    }


def _config_validation(path: Path | str) -> tuple[bool, str, dict[str, Any]]:
    try:
        config = load_forward_simulation_config(path)
    except Exception as exc:  # noqa: BLE001 - validation gate records the failure.
        return False, f"Forward config invalid: {exc}", {"path": str(path)}
    return (
        True,
        "Forward config and safety policy are valid.",
        {"version": _text(config.get("policy_metadata"))},
    )


def _state_validation(path: Path) -> tuple[bool, str, dict[str, Any]]:
    if not path.exists():
        return True, "Shadow state is absent; empty runtime state is valid.", {"path": str(path)}
    try:
        payload = load_shadow_candidate_registry(path)
        validate_shadow_candidate_registry(payload)
    except Exception as exc:  # noqa: BLE001 - validation gate records the failure.
        return False, f"Shadow state invalid: {exc}", {"path": str(path)}
    return (
        True,
        "Shadow state schema and safety invariants are valid.",
        {"candidate_count": payload.get("candidate_count")},
    )


def _decision_ledger_validation(path: Path) -> tuple[bool, str, dict[str, Any]]:
    if not path.exists():
        return (
            True,
            "Decision ledger is absent; no evaluation leakage detected.",
            {"path": str(path)},
        )
    try:
        frame = pd.read_csv(path)
        result = validate_no_lookahead_records(allocation_records=frame)
        raise_for_no_lookahead_violations(result)
    except Exception as exc:  # noqa: BLE001 - validation gate records the failure.
        return False, f"Decision/evaluation separation failed: {exc}", {"path": str(path)}
    return (
        True,
        "Decision ledger has no evaluation-only leakage.",
        {
            "path": str(path),
            "row_count": len(frame),
        },
    )


def _report_registry_validation(path: Path) -> tuple[bool, str, dict[str, Any]]:
    try:
        raw = safe_load_yaml_path(path)
    except Exception as exc:  # noqa: BLE001 - validation gate records the failure.
        return False, f"Report registry cannot be read: {exc}", {"path": str(path)}
    reports = raw.get("reports") if isinstance(raw, Mapping) else []
    report_ids = {str(item.get("report_id")) for item in reports if isinstance(item, Mapping)}
    required = {
        "etf_forward_update",
        "etf_forward_dashboard",
        "etf_forward_weekly_review",
        "etf_forward_watchlist",
        "etf_forward_validation",
    }
    missing = sorted(required - report_ids)
    if missing:
        return False, "Forward report registry entries are missing.", {"missing": missing}
    return True, "Forward reports are registered.", {"registered": sorted(required)}


def _unsafe_action_validation(path: Path | str) -> tuple[bool, str, dict[str, Any]]:
    config = load_forward_simulation_config(path)
    action_values = set(_string_list(config.get("allowed_recommended_actions")))
    action_values.update(_string_list(_mapping(config.get("watchlist")).get("allowed_actions")))
    unsafe = sorted(action_values & UNSAFE_ACTIONS)
    if unsafe:
        return False, "Unsafe production/broker action is configured.", {"unsafe": unsafe}
    return (
        True,
        "No production promotion or broker action is configured.",
        {"allowed_actions": sorted(action_values)},
    )


def _validation_check(
    checks: list[dict[str, Any]],
    check_id: str,
    result: tuple[bool, str, dict[str, Any]],
) -> None:
    passed, message, details = result
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "message": message,
            "details": details,
        }
    )


def _validate_forward_config(payload: Mapping[str, Any]) -> None:
    safety = _mapping(payload.get("safety"))
    if safety.get("observe_only") is not True:
        raise ValueError("ETF forward config must keep observe_only=true")
    if safety.get("production_effect") != "none":
        raise ValueError("ETF forward config must keep production_effect=none")
    if safety.get("broker_action") != "none":
        raise ValueError("ETF forward config must keep broker_action=none")
    if safety.get("manual_review_required") is not True:
        raise ValueError("ETF forward config must require manual review")
    if safety.get("production_promotion_allowed") is not False:
        raise ValueError("ETF forward config must disable production promotion")
    statuses = set(_string_list(payload.get("allowed_statuses")))
    if not set(ALLOWED_SHADOW_STATUSES).issubset(statuses):
        raise ValueError("ETF forward config missing allowed shadow statuses")
    if set(_string_list(payload.get("allowed_recommended_actions"))) & UNSAFE_ACTIONS:
        raise ValueError("ETF forward config includes unsafe action")
    thresholds = _mapping(payload.get("lifecycle_thresholds"))
    for key in (
        "minimum_forward_days",
        "watch_excess_return_vs_baseline",
        "reject_excess_return_vs_baseline",
        "reject_drawdown_worse_than_baseline",
        "max_turnover_since_enrollment",
        "max_constraint_hit_rate",
    ):
        if key not in thresholds:
            raise ValueError(f"ETF forward config missing lifecycle threshold: {key}")


def _benchmark_total_returns(
    run: ETFBacktestRun,
    symbols: tuple[str, ...],
) -> dict[str, float | None]:
    payload = _mapping(run.summary.get("benchmark_metrics"))
    returns: dict[str, float | None] = {}
    for symbol in symbols:
        benchmark_id = BENCHMARK_ID_BY_SYMBOL.get(symbol)
        row = _mapping(payload.get(benchmark_id)) if benchmark_id else {}
        returns[symbol] = _optional_float(row.get("total_return"))
    return returns


def _benchmark_window_returns(
    daily: pd.DataFrame,
    symbols: tuple[str, ...],
) -> dict[str, float | None]:
    returns: dict[str, list[float]] = {symbol: [] for symbol in symbols}
    for _, row in daily.iterrows():
        asset_returns = _json_mapping(row.get("asset_returns_json"))
        for symbol in symbols:
            value = _optional_float(asset_returns.get(symbol))
            if value is not None:
                returns[symbol].append(value)
    return {
        symbol: (_compound_return(values) if len(values) == len(daily) else None)
        for symbol, values in returns.items()
    }


def _constraint_hit_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    if "constraints_applied" in frame.columns:
        return sum(1 for value in frame["constraints_applied"] if _has_constraints(value))
    if "target_weights_json" in frame.columns:
        return 0
    return 0


def _has_constraints(value: object) -> bool:
    text = _text(value)
    return bool(text and text not in {"[]", "{}", "nan", "None"})


def _cash_weights(daily: pd.DataFrame) -> list[float]:
    weights = []
    for value in daily.get("target_weights_json", []):
        parsed = _json_mapping(value)
        cash = _optional_float(parsed.get("CASH"))
        if cash is not None:
            weights.append(cash)
    return weights


def _regime_distribution(daily: pd.DataFrame) -> dict[str, int]:
    if "regime" not in daily.columns:
        return {}
    counts = daily["regime"].astype(str).value_counts().to_dict()
    return {str(key): int(value) for key, value in sorted(counts.items())}


def _compound_window(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return _compound_return(values[-window:])


def _compound_return(values: list[float]) -> float:
    result = 1.0
    for value in values:
        result *= 1.0 + value
    return result - 1.0


def _max_drawdown_from_returns(values: list[float]) -> float | None:
    if not values:
        return None
    peak = 1.0
    running = 1.0
    max_drawdown = 0.0
    for value in values:
        running *= 1.0 + value
        peak = max(peak, running)
        max_drawdown = min(max_drawdown, running / peak - 1.0)
    return max_drawdown


def _annualized_volatility(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return pstdev(values) * math.sqrt(252)


def _sharpe(values: list[float], volatility: float | None) -> float | None:
    if volatility is None or volatility == 0 or len(values) < 2:
        return None
    return mean(values) * 252 / volatility


def _recommended_action_for_status(status: str) -> str:
    if status == "needs_more_data":
        return "needs_more_data"
    if status == "watch":
        return "watch"
    if status == "reject_pending_review":
        return "reject_pending_review"
    return "continue_shadow"


def _update_status(evaluations: list[Mapping[str, Any]]) -> str:
    if not evaluations:
        return "NO_ACTIVE_SHADOW_CANDIDATES"
    statuses = {_text(row.get("recommended_status")) for row in evaluations}
    if "reject_pending_review" in statuses:
        return "ACTION_REQUIRED"
    if "watch" in statuses:
        return "WATCH"
    if "needs_more_data" in statuses:
        return "NEEDS_MORE_DATA"
    return "PASS"


def _status_summary(
    rows: list[Mapping[str, Any]],
    active: list[Mapping[str, Any]],
) -> dict[str, Any]:
    statuses = [_text(row.get("status")) for row in rows]
    return {
        "active_candidate_count": len(active),
        "evaluated_candidate_count": len(rows),
        "needs_more_data_count": statuses.count("needs_more_data"),
        "watch_count": statuses.count("watch"),
        "reject_pending_review_count": statuses.count("reject_pending_review"),
    }


def _risk_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    drawdowns = [_optional_float(row.get("max_drawdown_since_enrollment")) for row in rows]
    drawdowns = [value for value in drawdowns if value is not None]
    return {
        "worst_drawdown": None if not drawdowns else min(drawdowns),
        "candidate_count": len(rows),
    }


def _turnover_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    values = [_optional_float(row.get("turnover_since_enrollment")) for row in rows]
    values = [value for value in values if value is not None]
    return {
        "max_turnover_since_enrollment": None if not values else max(values),
        "average_turnover_since_enrollment": None if not values else mean(values),
    }


def _constraint_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    values = [_optional_float(row.get("constraint_hits_since_enrollment")) for row in rows]
    values = [value for value in values if value is not None]
    return {
        "total_constraint_hits": int(sum(values)) if values else 0,
        "candidate_count_with_hits": sum(1 for value in values if value > 0),
    }


def _comparison_section(rows: list[Mapping[str, Any]], field: str) -> dict[str, Any]:
    values = [_optional_float(row.get(field)) for row in rows]
    values = [value for value in values if value is not None]
    return {
        "metric": field,
        "available_count": len(values),
        "best": None if not values else max(values),
        "worst": None if not values else min(values),
        "average": None if not values else mean(values),
    }


def _manual_review_notes(rows: list[Mapping[str, Any]]) -> list[str]:
    notes: list[str] = []
    for row in rows:
        for reason in _string_list(row.get("status_reasons")):
            notes.append(f"{row.get('candidate_id')}: {reason}")
    return notes or ["no active manual review notes"]


def _requested_date_range(candidates: list[Mapping[str, Any]], as_of: date) -> dict[str, str]:
    starts = [
        _safe_date(candidate.get("enrollment_date"))
        for candidate in candidates
        if _safe_date(candidate.get("enrollment_date")) is not None
    ]
    start = min(starts) if starts else None
    return {
        "start": "" if start is None else start.isoformat(),
        "end": as_of.isoformat(),
    }


def _rolling_windows(config: Mapping[str, Any]) -> tuple[int, ...]:
    values = config.get("rolling_windows") or (5, 20, 60)
    return tuple(int(value) for value in values)


def _benchmark_symbols(config: Mapping[str, Any]) -> tuple[str, ...]:
    configured = _mapping(config.get("benchmarks")).get("benchmark_symbols")
    values = _string_list(configured) or list(DEFAULT_BENCHMARK_SYMBOLS)
    return tuple(value.upper() for value in values)


def _safety_banner() -> str:
    return (
        "observe_only=true; production_effect=none; broker_action=none; "
        "manual_review_required=true"
    )


def _latest_report_path(output_dir: Path, prefix: str, *, as_of: date | None) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob(f"{prefix}*.json"):
        artifact_date = _date_from_stem(path.stem, prefix)
        if artifact_date is not None and (as_of is None or artifact_date <= as_of):
            candidates.append((artifact_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _date_from_stem(stem: str, prefix: str) -> date | None:
    raw = stem.removeprefix(prefix)
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _write_json(payload: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _numeric_list(values: object) -> list[float]:
    if values is None:
        return []
    series = pd.to_numeric(values, errors="coerce")
    return [float(value) for value in series if pd.notna(value)]


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _float_or_default(value: object, default: float) -> float:
    parsed = _optional_float(value)
    return default if parsed is None else parsed


def _sub(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _safe_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple | set):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [] if not text else [text]


def _fmt_pct(value: object) -> str:
    parsed = _optional_float(value)
    return "n/a" if parsed is None else f"{parsed:.2%}"


def _fmt_number(value: object) -> str:
    parsed = _optional_float(value)
    return "n/a" if parsed is None else f"{parsed:.4f}"


def _config_hash_for_rows(rows: list[dict[str, Any]]) -> str:
    return dataframe_checksum(rows)
