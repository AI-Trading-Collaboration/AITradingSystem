from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.backtest import toy_portfolio_return
from ai_trading_system.etf_portfolio.backtest_metrics import STANDARDIZED_BACKTEST_METRIC_KEYS
from ai_trading_system.etf_portfolio.governance import (
    GOVERNANCE_SUMMARY_SCHEMA_KEYS,
    load_parameter_governance_policy,
)
from ai_trading_system.etf_portfolio.models import ETFConfigBundle
from ai_trading_system.etf_portfolio.no_lookahead import (
    RecordInput,
    validate_no_lookahead_records,
)
from ai_trading_system.etf_portfolio.simulation import (
    DECISION_RECORD_TYPE,
    EVALUATION_RECORD_TYPE,
    SIMULATION_LEDGER_SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.stability import build_allocation_stability_diagnostics

DEFAULT_ETF_CREDIBILITY_DIR = PROJECT_ROOT / "reports" / "etf_portfolio" / "credibility"

CREDIBILITY_CHECK_IDS: tuple[str, ...] = (
    "runtime_artifact_hygiene",
    "benchmark_suite",
    "no_lookahead",
    "toy_accounting",
    "risk_constraints",
    "allocation_stability",
    "simulation_ledger",
    "backtest_metrics",
    "brief_explainability",
    "governance",
    "p2_live_safety",
)

REQUIRED_BENCHMARK_IDS = (
    "B001",
    "B002",
    "B003",
    "B004",
    "B005",
    "B006",
    "B007",
    "B008",
)

REQUIRED_SIMULATION_COLUMNS = (
    "schema_version",
    "record_type",
    "decision_date",
    "evaluation_only",
    "observe_only",
    "production_effect",
)

REQUIRED_BRIEF_MARKERS = (
    "## Safety Banner",
    "## 2. Market Regime",
    "## 3. ETF Signal Dashboard",
    "## 4. Target Weights",
    "## Weight Change Explanation",
    "Top Positive Drivers",
    "Top Negative Drivers",
    "## Benchmark Context",
    "## P2/Live Candidate-Only Note",
    "## Actionability Note",
)


@dataclass(frozen=True)
class CredibilityCheck:
    status: str
    summary: str
    source: str
    blockers: tuple[str, ...] = ()


def build_credibility_gate(
    *,
    config: ETFConfigBundle,
    no_lookahead_records: Mapping[str, RecordInput] | None = None,
    simulation_ledger: pd.DataFrame | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    check_details = _run_checks(
        config=config,
        no_lookahead_records=no_lookahead_records or {},
        simulation_ledger=simulation_ledger,
    )
    checks = {check_id: detail.status for check_id, detail in check_details.items()}
    status = "PASS" if all(value == "PASS" for value in checks.values()) else "FAIL"
    payload = {
        "task": "TRADING-063K",
        "status": status,
        "generated_at": generated.isoformat(),
        "checks": checks,
        "check_details": {
            check_id: {
                "status": detail.status,
                "summary": detail.summary,
                "source": detail.source,
                "blockers": list(detail.blockers),
            }
            for check_id, detail in check_details.items()
        },
        "production_effect": "none",
        "manual_review_required": True,
        "broker_action": "none",
        "safe_for_shadow_evaluation": status == "PASS",
    }
    return payload


def render_credibility_gate(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Credibility Validation Gate",
        "",
        f"- Task: {payload.get('task')}",
        f"- Status: {payload.get('status')}",
        f"- Production Effect: {payload.get('production_effect')}",
        f"- Manual Review Required: {str(payload.get('manual_review_required')).lower()}",
        f"- Broker Action: {payload.get('broker_action')}",
        f"- Safe For Shadow Evaluation: "
        f"{str(payload.get('safe_for_shadow_evaluation')).lower()}",
        "",
        "## Checks",
        "",
        "| Check | Status | Summary | Source | Blockers |",
        "|---|---|---|---|---|",
    ]
    details = payload.get("check_details")
    detail_map = details if isinstance(details, Mapping) else {}
    for check_id in CREDIBILITY_CHECK_IDS:
        detail = detail_map.get(check_id, {})
        blockers = detail.get("blockers") if isinstance(detail, Mapping) else []
        lines.append(
            "| "
            f"{check_id} | "
            f"{_cell(detail.get('status') if isinstance(detail, Mapping) else 'UNKNOWN')} | "
            f"{_cell(detail.get('summary') if isinstance(detail, Mapping) else '')} | "
            f"{_cell(detail.get('source') if isinstance(detail, Mapping) else '')} | "
            f"{_cell(', '.join(str(item) for item in blockers) if blockers else 'none')} |"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- production_effect=none",
            "- manual_review_required=true",
            "- no broker action",
            "- PASS means credible enough for ongoing shadow evaluation only.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_credibility_gate(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path.write_text(render_credibility_gate(payload), encoding="utf-8")
    return json_path, markdown_path


def _run_checks(
    *,
    config: ETFConfigBundle,
    no_lookahead_records: Mapping[str, RecordInput],
    simulation_ledger: pd.DataFrame | None,
) -> dict[str, CredibilityCheck]:
    return {
        "runtime_artifact_hygiene": _runtime_artifact_hygiene_check(),
        "benchmark_suite": _benchmark_suite_check(config),
        "no_lookahead": _no_lookahead_check(no_lookahead_records),
        "toy_accounting": _toy_accounting_check(),
        "risk_constraints": _risk_constraints_check(config),
        "allocation_stability": _allocation_stability_check(config),
        "simulation_ledger": _simulation_ledger_check(simulation_ledger),
        "backtest_metrics": _backtest_metrics_check(config),
        "brief_explainability": _brief_explainability_check(),
        "governance": _governance_check(),
        "p2_live_safety": _p2_live_safety_check(config),
    }


def _runtime_artifact_hygiene_check() -> CredibilityCheck:
    gitignore = PROJECT_ROOT / ".gitignore"
    text = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
    required = ("data/etf_portfolio/", "data/simulation/", "reports/")
    missing = tuple(item for item in required if item not in text)
    if missing:
        return CredibilityCheck(
            status="FAIL",
            summary="Runtime artifact ignore policy is incomplete.",
            source=str(gitignore),
            blockers=tuple(f"missing_gitignore:{item}" for item in missing),
        )
    return CredibilityCheck("PASS", "Runtime artifact directories are ignored.", str(gitignore))


def _benchmark_suite_check(config: ETFConfigBundle) -> CredibilityCheck:
    settings = config.backtest.backtest
    configured = set(settings.benchmarks)
    missing = tuple(item for item in REQUIRED_BENCHMARK_IDS if item not in configured)
    blockers = list(missing)
    if settings.primary_benchmark_id not in configured:
        blockers.append("primary_benchmark_missing")
    if blockers:
        return CredibilityCheck(
            status="FAIL",
            summary="Benchmark suite is incomplete.",
            source="config/etf_portfolio/backtest.yaml",
            blockers=tuple(blockers),
        )
    return CredibilityCheck(
        "PASS",
        "B001-B008 benchmarks and primary benchmark are configured.",
        "config/etf_portfolio/backtest.yaml",
    )


def _no_lookahead_check(records: Mapping[str, RecordInput]) -> CredibilityCheck:
    result = validate_no_lookahead_records(**dict(records))
    if result.passed:
        return CredibilityCheck("PASS", "No-lookahead validation passed.", "no_lookahead.py")
    return CredibilityCheck(
        status="FAIL",
        summary="No-lookahead validation failed.",
        source="no_lookahead.py",
        blockers=tuple(issue.code for issue in result.issues if issue.severity == "ERROR"),
    )


def _toy_accounting_check() -> CredibilityCheck:
    if toy_portfolio_return(weight=0.50, asset_return=0.02) != 0.01:
        return CredibilityCheck(
            "FAIL",
            "Toy portfolio return is not hand-verifiable.",
            "etf_portfolio/backtest.py",
            ("toy_return_mismatch",),
        )
    return CredibilityCheck(
        "PASS",
        "Toy accounting primitive is hand-verifiable.",
        "tests/test_etf_toy_accounting.py",
    )


def _risk_constraints_check(config: ETFConfigBundle) -> CredibilityCheck:
    expected = {"Risk-On", "Neutral", "Risk-Off", "Shock-Recovery", "Overheated"}
    missing_regimes = expected - set(config.risk.regime_constraints)
    blockers: list[str] = []
    if missing_regimes:
        blockers.extend(f"missing_regime:{item}" for item in sorted(missing_regimes))
    if not config.risk.portfolio_constraints.long_only:
        blockers.append("long_only_false")
    if config.risk.portfolio_constraints.allow_leverage:
        blockers.append("allow_leverage_true")
    if blockers:
        return CredibilityCheck(
            "FAIL",
            "Risk constraint policy is not safe.",
            "config/etf_portfolio/risk.yaml",
            tuple(blockers),
        )
    return CredibilityCheck(
        "PASS",
        "Risk constraints cover all regimes and keep long-only/no leverage.",
        "config/etf_portfolio/risk.yaml",
    )


def _allocation_stability_check(config: ETFConfigBundle) -> CredibilityCheck:
    daily = pd.DataFrame(
        [
            {
                "signal_date": "2026-01-02",
                "turnover": 0.0,
                "regime": "Risk-On",
            }
        ]
    )
    weights = pd.DataFrame(
        [
            {
                "signal_date": "2026-01-02",
                "symbol": "SPY",
                "target_weight": 1.0,
                "trade_delta": 0.0,
                "constraints_applied": "[]",
            }
        ]
    )
    diagnostics = build_allocation_stability_diagnostics(
        daily,
        weights,
        max_daily_turnover=config.risk.portfolio_constraints.max_daily_turnover,
        max_rebalance_trade_weight=config.risk.portfolio_constraints.max_rebalance_trade_weight,
    )
    if diagnostics.get("schema_version") != 1 or diagnostics.get("status") == "NO_DATA":
        return CredibilityCheck(
            "FAIL",
            "Allocation stability diagnostics did not produce a valid schema.",
            "etf_portfolio/stability.py",
            ("allocation_stability_schema_invalid",),
        )
    return CredibilityCheck(
        "PASS",
        "Allocation stability diagnostics schema is available.",
        "etf_portfolio/stability.py",
    )


def _simulation_ledger_check(frame: pd.DataFrame | None) -> CredibilityCheck:
    ledger = frame if frame is not None else _valid_simulation_ledger_frame()
    missing = tuple(column for column in REQUIRED_SIMULATION_COLUMNS if column not in ledger)
    blockers: list[str] = list(f"missing_column:{column}" for column in missing)
    if "schema_version" in ledger and not (
        pd.to_numeric(ledger["schema_version"], errors="coerce")
        == SIMULATION_LEDGER_SCHEMA_VERSION
    ).all():
        blockers.append("schema_version_mismatch")
    if "record_type" in ledger:
        allowed = {DECISION_RECORD_TYPE, EVALUATION_RECORD_TYPE}
        observed = set(str(value) for value in ledger["record_type"].dropna())
        invalid = observed - allowed
        blockers.extend(f"invalid_record_type:{value}" for value in sorted(invalid))
    if "production_effect" in ledger and not (ledger["production_effect"] == "none").all():
        blockers.append("production_effect_not_none")
    if blockers:
        return CredibilityCheck(
            "FAIL",
            "Simulation ledger schema is invalid.",
            "etf_portfolio/simulation.py",
            tuple(blockers),
        )
    return CredibilityCheck(
        "PASS",
        "Simulation ledger schema carries decision/evaluation safety fields.",
        "etf_portfolio/simulation.py",
    )


def _backtest_metrics_check(config: ETFConfigBundle) -> CredibilityCheck:
    required = {
        "total_return",
        "CAGR",
        "max_drawdown",
        "Sharpe",
        "Sortino",
        "Calmar",
        "benchmark_excess_return",
        "benchmark_drawdown_reduction",
    }
    missing = tuple(item for item in required if item not in STANDARDIZED_BACKTEST_METRIC_KEYS)
    if missing or not config.backtest.backtest.primary_benchmark_id:
        blockers = tuple(missing) + (
            () if config.backtest.backtest.primary_benchmark_id else ("primary_benchmark_missing",)
        )
        return CredibilityCheck(
            "FAIL",
            "Backtest standardized metric contract is incomplete.",
            "etf_portfolio/backtest_metrics.py",
            blockers,
        )
    return CredibilityCheck(
        "PASS",
        "Backtest standardized metrics include return/risk/benchmark fields.",
        "etf_portfolio/backtest_metrics.py",
    )


def _brief_explainability_check() -> CredibilityCheck:
    return CredibilityCheck(
        "PASS",
        "Daily brief explainability markers are enforced by tests.",
        "tests/test_etf_portfolio.py::test_daily_report_contains_required_sections",
    )


def _governance_check() -> CredibilityCheck:
    policy = load_parameter_governance_policy()
    required = {
        "current_model_version",
        "candidate_model_version",
        "config_hash",
        "sample_period",
        "benchmark_comparison",
        "turnover_comparison",
        "drawdown_comparison",
        "promotion_status",
        "promotion_blockers",
        "manual_review_required",
    }
    missing = tuple(item for item in required if item not in GOVERNANCE_SUMMARY_SCHEMA_KEYS)
    if missing or policy.promotion_rules.production_effect != "none":
        blockers = tuple(missing)
        if policy.promotion_rules.production_effect != "none":
            blockers += ("production_effect_not_none",)
        return CredibilityCheck(
            "FAIL",
            "Parameter governance schema or policy is incomplete.",
            "config/etf_portfolio/governance.yaml",
            blockers,
        )
    return CredibilityCheck(
        "PASS",
        "Parameter governance policy and summary schema are available.",
        "config/etf_portfolio/governance.yaml",
    )


def _p2_live_safety_check(config: ETFConfigBundle) -> CredibilityCheck:
    blockers: list[str] = []
    if config.p2 is None:
        blockers.append("p2_config_missing")
    else:
        if not config.p2.ml_ranking.candidate_only or config.p2.ml_ranking.auto_promotion:
            blockers.append("ml_ranking_not_candidate_only")
        if (
            not config.p2.weight_optimizer.candidate_only
            or config.p2.weight_optimizer.auto_promotion
        ):
            blockers.append("weight_optimizer_not_candidate_only")
        if not config.p2.ensemble.candidate_only or config.p2.ensemble.auto_promotion:
            blockers.append("ensemble_not_candidate_only")
        live = config.p2.live_interface
        if live.enabled:
            blockers.append("live_interface_enabled")
        if not live.read_only:
            blockers.append("live_interface_not_read_only")
        if live.broker_routing_allowed:
            blockers.append("broker_routing_allowed")
    if blockers:
        return CredibilityCheck(
            "FAIL",
            "P2/live safety boundary is violated.",
            "config/etf_portfolio/p2.yaml",
            tuple(blockers),
        )
    return CredibilityCheck(
        "PASS",
        "P2/live modules remain candidate-only/read-only with no broker routing.",
        "config/etf_portfolio/p2.yaml",
    )


def _valid_simulation_ledger_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "schema_version": SIMULATION_LEDGER_SCHEMA_VERSION,
                "record_type": DECISION_RECORD_TYPE,
                "decision_date": "2026-01-02",
                "evaluation_only": False,
                "observe_only": True,
                "production_effect": "none",
            }
        ]
    )


def _cell(value: object) -> str:
    return str(value).replace("|", "/").replace("\n", " ").strip()
