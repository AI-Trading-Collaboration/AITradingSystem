from __future__ import annotations

import json
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
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_b3 import (
    build_b3_relative_tilt_signal,
    build_b3_target_path,
    load_b3_policies,
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
    simulate_b1_execution_control,
    simulate_static_baseline_path,
)

DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_research_modules.yaml"
)
DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"
DEFAULT_B2_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_scaler_research_result.json"
DEFAULT_B3_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_relative_tilt_research_result.json"

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

B2_READY_STATUS = "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
B3_READY_STATUS = "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"


def run_b4_interaction_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    modules_config_path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    b2_result_path: Path = DEFAULT_B2_RESULT_PATH,
    b3_result_path: Path = DEFAULT_B3_RESULT_PATH,
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
    b2_result = _read_json(b2_result_path)
    b3_result = _read_json(b3_result_path)
    branch_gate = _branch_gate_status(b2_result, b3_result, start=start, end=end)
    if (
        context.contract_validation["status"] != "PASS"
        or not context.data_quality_report.passed
        or branch_gate != "B4_BRANCH_INPUTS_READY"
    ):
        payload = _blocked_b4_payload(
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="contract_data_quality_or_branch_gate_failed",
            details={
                "contract_status": context.contract_validation["status"],
                "data_quality_status": context.data_quality_report.status,
                "branch_gate": branch_gate,
                "b2_status": b2_result.get("status"),
                "b3_status": b3_result.get("status"),
            },
        )
        json_path, md_path = write_b4_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, {}

    b2_risk_policy, b2_target_policy = load_b2_policies(modules_config_path)
    b3_signal_policy, b3_target_policy = load_b3_policies(modules_config_path)
    feature_frame = build_feature_store(
        context.prices,
        assets=context.etf_config.assets,
        strategy=context.etf_config.strategy,
        start=context.etf_config.backtest.backtest.warmup_start_date,
        end=end,
    )
    feature_artifact = _filter_feature_artifact(feature_frame, start=start, end=end)
    b2_signal = build_b2_risk_signal(
        feature_artifact,
        config=context.etf_config,
        policy=b2_risk_policy,
    )
    b2_diagnostics = build_signal_diagnostics_report(
        b2_signal.rename(columns={"risk_score": "signal_score", "risk_state": "state"}),
        signal_artifact_id=b2_risk_policy.signal_id,
        as_of=end,
        max_stale_days=b2_risk_policy.max_stale_days,
        generated_at=context.generated_at,
    )
    b3_signal = build_b3_relative_tilt_signal(
        feature_artifact,
        config=context.etf_config,
        policy=b3_signal_policy,
    )
    b3_diagnostics = build_signal_diagnostics_report(
        b3_signal,
        signal_artifact_id=b3_signal_policy.signal_id,
        as_of=end,
        max_stale_days=b3_signal_policy.max_stale_days,
        generated_at=context.generated_at,
    )
    signal_gate_status = _b4_signal_gate_status(
        b2_signal,
        b2_diagnostics,
        b3_signal,
        b3_diagnostics,
    )
    if signal_gate_status != "B4_COMPONENT_SIGNALS_READY":
        payload = build_b4_result_payload(
            context=context,
            start=start,
            end=end,
            b2_result=b2_result,
            b3_result=b3_result,
            signal_gate_status=signal_gate_status,
            b2_diagnostics=b2_diagnostics,
            b3_diagnostics=b3_diagnostics,
            feature_artifact=feature_artifact,
            b2_signal=b2_signal,
            b3_signal=b3_signal,
            b2_target_path=pd.DataFrame(),
            b3_target_path=pd.DataFrame(),
            b4_target_path=pd.DataFrame(),
            e0_daily=pd.DataFrame(),
            e1_daily=pd.DataFrame(),
            prices_path=prices_path,
            modules_config_path=modules_config_path,
            b2_result_path=b2_result_path,
            b3_result_path=b3_result_path,
        )
        paths = write_b4_component_artifacts(
            feature_artifact=feature_artifact,
            b2_signal=b2_signal,
            b3_signal=b3_signal,
            b2_target_path=pd.DataFrame(),
            b3_target_path=pd.DataFrame(),
            b4_target_path=pd.DataFrame(),
            e0_daily=pd.DataFrame(),
            e1_daily=pd.DataFrame(),
            b2_diagnostics=b2_diagnostics,
            b3_diagnostics=b3_diagnostics,
            generated_at=context.generated_at,
            output_dir=output_dir,
        )
        json_path, md_path = write_b4_result(payload, output_dir=output_dir, alias_dir=alias_dir)
        return payload, json_path, md_path, paths

    b2_target_path = build_b2_target_path(
        b2_signal,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=b2_target_policy,
        start=start,
        end=end,
    )
    b3_target_path = build_b3_target_path(
        b3_signal,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=b3_target_policy,
        signal_policy=b3_signal_policy,
        start=start,
        end=end,
    )
    b4_target_path = build_b4_interaction_target_path(
        b2_target_path,
        b3_target_path,
        config=context.etf_config,
        cash_symbol=b3_target_policy.cash_symbol,
    )
    b1_policy = load_b1_execution_policy()
    b0r_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=start,
        end=end,
        variant_id="B0R",
    )
    b1e_daily = simulate_b1_execution_control(
        prices=context.prices,
        config=context.etf_config,
        policy=b1_policy,
        start=start,
        end=end,
    )
    e0_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=b4_target_path,
        mode="naive",
    )
    e1_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=b4_target_path,
        mode="controlled",
        execution_policy=b1_policy,
    )
    payload = build_b4_result_payload(
        context=context,
        start=start,
        end=end,
        b2_result=b2_result,
        b3_result=b3_result,
        signal_gate_status=signal_gate_status,
        b2_diagnostics=b2_diagnostics,
        b3_diagnostics=b3_diagnostics,
        feature_artifact=feature_artifact,
        b2_signal=b2_signal,
        b3_signal=b3_signal,
        b2_target_path=b2_target_path,
        b3_target_path=b3_target_path,
        b4_target_path=b4_target_path,
        b0r_daily=b0r_daily,
        b1e_daily=b1e_daily,
        e0_daily=e0_daily,
        e1_daily=e1_daily,
        prices_path=prices_path,
        modules_config_path=modules_config_path,
        b2_result_path=b2_result_path,
        b3_result_path=b3_result_path,
        b1_policy=b1_policy,
    )
    paths = write_b4_component_artifacts(
        feature_artifact=feature_artifact,
        b2_signal=b2_signal,
        b3_signal=b3_signal,
        b2_target_path=b2_target_path,
        b3_target_path=b3_target_path,
        b4_target_path=b4_target_path,
        e0_daily=e0_daily,
        e1_daily=e1_daily,
        b2_diagnostics=b2_diagnostics,
        b3_diagnostics=b3_diagnostics,
        generated_at=context.generated_at,
        output_dir=output_dir,
    )
    json_path, md_path = write_b4_result(payload, output_dir=output_dir, alias_dir=alias_dir)
    return payload, json_path, md_path, paths


def build_b4_interaction_target_path(
    b2_target_path: pd.DataFrame,
    b3_target_path: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    cash_symbol: str = "CASH",
) -> pd.DataFrame:
    b2_by_key = {
        _target_key(row): row
        for _, row in b2_target_path.iterrows()
    }
    rows: list[dict[str, Any]] = []
    for _, b3_row in b3_target_path.sort_values("signal_date").iterrows():
        key = _target_key(b3_row)
        b2_row = b2_by_key.get(key)
        if b2_row is None:
            continue
        b2_weights = _load_weights(b2_row["target_weights_json"])
        b3_weights = _load_weights(b3_row["target_weights_json"])
        b2_equity = 1.0 - b2_weights.get(cash_symbol, 0.0)
        b3_equity = sum(
            weight for symbol, weight in b3_weights.items() if symbol != cash_symbol
        )
        target_weights: dict[str, float] = {}
        for symbol in _non_cash_symbols(config, cash_symbol):
            relative_share = 0.0 if b3_equity == 0.0 else b3_weights.get(symbol, 0.0) / b3_equity
            target_weights[symbol] = relative_share * b2_equity
        target_weights[cash_symbol] = 1.0 - sum(target_weights.values())
        rows.append(
            {
                "signal_date": str(b3_row["signal_date"]),
                "execution_date": str(b3_row["execution_date"]),
                "return_date": str(b3_row["return_date"]),
                "target_path_module": "B4",
                "interaction_formula": "B2_total_exposure_x_B3_relative_non_cash_mix",
                "b2_target_weights_json": str(b2_row["target_weights_json"]),
                "b3_target_weights_json": str(b3_row["target_weights_json"]),
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


def build_b4_result_payload(
    *,
    context: ResearchDataContext,
    start: date,
    end: date,
    b2_result: dict[str, Any],
    b3_result: dict[str, Any],
    signal_gate_status: str,
    b2_diagnostics: dict[str, Any],
    b3_diagnostics: dict[str, Any],
    feature_artifact: pd.DataFrame,
    b2_signal: pd.DataFrame,
    b3_signal: pd.DataFrame,
    b2_target_path: pd.DataFrame,
    b3_target_path: pd.DataFrame,
    b4_target_path: pd.DataFrame,
    e0_daily: pd.DataFrame,
    e1_daily: pd.DataFrame,
    b0r_daily: pd.DataFrame | None = None,
    b1e_daily: pd.DataFrame | None = None,
    prices_path: Path,
    modules_config_path: Path,
    b2_result_path: Path,
    b3_result_path: Path,
    b1_policy: B1ExecutionPolicy | None = None,
) -> dict[str, Any]:
    run_id = f"WRP1-{context.generated_at.strftime('%Y%m%dT%H%M%SZ')}-B4-RISK-TILT"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "task_id": "TRADING-514A_to_514B",
        "report_type": "b4_risk_tilt_interaction_result",
        "status": (
            "B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            if signal_gate_status == "B4_COMPONENT_SIGNALS_READY"
            and not b4_target_path.empty
            and not e0_daily.empty
            and not e1_daily.empty
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
            "b2_result_path": str(b2_result_path),
            "b2_result_status": b2_result.get("status"),
            "b3_result_path": str(b3_result_path),
            "b3_result_status": b3_result.get("status"),
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
        "component_signal_gate": signal_gate_status,
        "feature_artifact": {
            "row_count": int(len(feature_artifact)),
            "checksum": _frame_checksum(feature_artifact),
        },
        "b2_signal_artifact": {
            "row_count": int(len(b2_signal)),
            "checksum": _frame_checksum(b2_signal),
            "diagnostics_status": b2_diagnostics["status"],
        },
        "b3_signal_artifact": {
            "row_count": int(len(b3_signal)),
            "checksum": _frame_checksum(b3_signal),
            "diagnostics_status": b3_diagnostics["status"],
        },
        "b2_target_path_artifact": {
            "row_count": int(len(b2_target_path)),
            "checksum": _frame_checksum(b2_target_path),
        },
        "b3_target_path_artifact": {
            "row_count": int(len(b3_target_path)),
            "checksum": _frame_checksum(b3_target_path),
        },
        "b4_target_path_artifact": {
            "row_count": int(len(b4_target_path)),
            "checksum": _frame_checksum(b4_target_path),
        },
        "signal_diagnostics": {"B2": b2_diagnostics, "B3": b3_diagnostics},
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "policy": {
            "interaction_formula": "B2_total_exposure_x_B3_relative_non_cash_mix",
            "execution_control_policy_id": None if b1_policy is None else b1_policy.policy_id,
            "source_branch_statuses": {
                "B2": b2_result.get("status"),
                "B3": b3_result.get("status"),
            },
        },
    }
    if not e0_daily.empty and not e1_daily.empty:
        e0_metrics = metrics_from_execution_daily(e0_daily)
        e1_metrics = metrics_from_execution_daily(e1_daily)
        b4_e1 = metrics_payload(e1_metrics)
        b0r_metrics = (
            metrics_payload(metrics_from_execution_daily(b0r_daily))
            if b0r_daily is not None and not b0r_daily.empty
            else {}
        )
        b1e_metrics = (
            metrics_payload(metrics_from_execution_daily(b1e_daily))
            if b1e_daily is not None and not b1e_daily.empty
            else {}
        )
        payload.update(
            {
                "b4_e0_metrics": metrics_payload(e0_metrics),
                "b4_e1_metrics": b4_e1,
                "b4_e1_vs_b4_e0_comparison": comparison_payload(e1_metrics, e0_metrics),
                "b4_e1_vs_b2_e1_comparison": _metric_dict_comparison(
                    b4_e1,
                    _metric_dict(b2_result, "b2_e1_metrics"),
                ),
                "b4_e1_vs_b3_e1_comparison": _metric_dict_comparison(
                    b4_e1,
                    _metric_dict(b3_result, "b3_e1_metrics"),
                ),
                "same_window_controls": {
                    "b0r_metrics": b0r_metrics,
                    "b1e_metrics": b1e_metrics,
                    "control_window": {
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    },
                },
                "interaction_effects": _interaction_effects(
                    b0r_metrics=b0r_metrics,
                    b1e_metrics=b1e_metrics,
                    b2_e0_metrics=_metric_dict(b2_result, "b2_e0_metrics"),
                    b2_e1_metrics=_metric_dict(b2_result, "b2_e1_metrics"),
                    b3_e0_metrics=_metric_dict(b3_result, "b3_e0_metrics"),
                    b3_e1_metrics=_metric_dict(b3_result, "b3_e1_metrics"),
                    b4_e0_metrics=metrics_payload(e0_metrics),
                    b4_e1_metrics=b4_e1,
                ),
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
            "B4 combined B2 total-exposure scaling with B3 relative tilt and completed "
            "E0/E1 mini-backfill."
            if payload["status"]
            == "B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            else "B4 stopped before interaction metrics because component gates did not pass."
        ),
        "key_result": payload["status"],
        "blocking_issues": (
            "none"
            if payload["status"]
            == "B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
            else signal_gate_status
        ),
        "warnings": (
            "B4 is interaction evidence only; it does not select a candidate or create "
            "official target weights."
        ),
        "safety_boundary": (
            "research_only=true; official_target_weights=false; production_effect=none"
        ),
        "next_action": "Continue only to governed B5/B6 evidence or keep v3 candidate blocked.",
    }
    return payload


def write_b4_component_artifacts(
    *,
    feature_artifact: pd.DataFrame,
    b2_signal: pd.DataFrame,
    b3_signal: pd.DataFrame,
    b2_target_path: pd.DataFrame,
    b3_target_path: pd.DataFrame,
    b4_target_path: pd.DataFrame,
    e0_daily: pd.DataFrame,
    e1_daily: pd.DataFrame,
    b2_diagnostics: dict[str, Any],
    b3_diagnostics: dict[str, Any],
    generated_at: datetime,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(generated_at.isoformat())
    paths = {
        "features": output_dir / f"b4_feature_artifact_{stamp}.csv",
        "b2_signal": output_dir / f"b4_b2_signal_artifact_{stamp}.csv",
        "b3_signal": output_dir / f"b4_b3_signal_artifact_{stamp}.csv",
        "b2_target_path": output_dir / f"b4_b2_target_path_{stamp}.csv",
        "b3_target_path": output_dir / f"b4_b3_target_path_{stamp}.csv",
        "b4_target_path": output_dir / f"b4_interaction_target_path_{stamp}.csv",
        "e0_daily": output_dir / f"b4_e0_naive_execution_daily_{stamp}.csv",
        "e1_daily": output_dir / f"b4_e1_controlled_execution_daily_{stamp}.csv",
        "b2_diagnostics": output_dir / f"b4_b2_signal_diagnostics_{stamp}.json",
        "b3_diagnostics": output_dir / f"b4_b3_signal_diagnostics_{stamp}.json",
    }
    feature_artifact.to_csv(paths["features"], index=False)
    b2_signal.to_csv(paths["b2_signal"], index=False)
    b3_signal.to_csv(paths["b3_signal"], index=False)
    b2_target_path.to_csv(paths["b2_target_path"], index=False)
    b3_target_path.to_csv(paths["b3_target_path"], index=False)
    b4_target_path.to_csv(paths["b4_target_path"], index=False)
    e0_daily.to_csv(paths["e0_daily"], index=False)
    e1_daily.to_csv(paths["e1_daily"], index=False)
    _write_json(paths["b2_diagnostics"], b2_diagnostics)
    _write_json(paths["b3_diagnostics"], b3_diagnostics)
    return paths


def write_b4_result(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b4_risk_tilt_interaction_result_{stamp}.json"
    md_path = output_dir / f"b4_risk_tilt_interaction_result_{stamp}.md"
    markdown = render_b4_result(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    if alias_dir is not None:
        alias_dir.mkdir(parents=True, exist_ok=True)
        _write_json(alias_dir / "b4_risk_tilt_interaction_result.json", payload)
        (alias_dir / "b4_risk_tilt_interaction_result.md").write_text(
            markdown,
            encoding="utf-8",
        )
    return json_path, md_path


def render_b4_result(payload: dict[str, Any]) -> str:
    lines = [
        "# B4 Risk x Tilt Interaction Result",
        "",
        f"- Status：{payload['status']}",
        f"- Component Signal Gate：{payload.get('component_signal_gate', 'not_evaluated')}",
        f"- Data Quality：{payload.get('data_quality_gate', {}).get('status', 'not_available')}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b4_e0_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B4-E0", payload["b4_e0_metrics"]),
                _metric_line("B4-E1", payload["b4_e1_metrics"]),
                "",
                "## B4-E1 vs B4-E0",
                "",
            ]
        )
        for key, value in payload["b4_e1_vs_b4_e0_comparison"].items():
            lines.append(f"- {key}：{float(value):.6f}")
        lines.extend(["", "## Branch Comparisons", ""])
        for label, comparison in (
            ("B4-E1 vs B2-E1", payload["b4_e1_vs_b2_e1_comparison"]),
            ("B4-E1 vs B3-E1", payload["b4_e1_vs_b3_e1_comparison"]),
        ):
            lines.append(f"### {label}")
            for key, value in comparison.items():
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


def _target_key(row: pd.Series) -> tuple[str, str, str]:
    return (str(row["signal_date"]), str(row["execution_date"]), str(row["return_date"]))


def _load_weights(value: Any) -> dict[str, float]:
    return {str(symbol): float(weight) for symbol, weight in json.loads(str(value)).items()}


def _non_cash_symbols(config: ETFConfigBundle, cash_symbol: str) -> list[str]:
    return [
        symbol
        for symbol, asset in config.assets.assets.items()
        if symbol != cash_symbol and float(asset.default_weight) > 0.0
    ]


def _branch_gate_status(
    b2_result: dict[str, Any],
    b3_result: dict[str, Any],
    *,
    start: date,
    end: date,
) -> str:
    if b2_result.get("status") != B2_READY_STATUS:
        return "B4_BLOCKED_B2_NOT_READY"
    if b3_result.get("status") != B3_READY_STATUS:
        return "B4_BLOCKED_B3_NOT_READY"
    expected_start = start.isoformat()
    expected_end = end.isoformat()
    if (
        b2_result.get("requested_start") != expected_start
        or b2_result.get("requested_end") != expected_end
    ):
        return "B4_BLOCKED_B2_WINDOW_MISMATCH"
    if (
        b3_result.get("requested_start") != expected_start
        or b3_result.get("requested_end") != expected_end
    ):
        return "B4_BLOCKED_B3_WINDOW_MISMATCH"
    return "B4_BRANCH_INPUTS_READY"


def _b4_signal_gate_status(
    b2_signal: pd.DataFrame,
    b2_diagnostics: dict[str, Any],
    b3_signal: pd.DataFrame,
    b3_diagnostics: dict[str, Any],
) -> str:
    if b2_signal.empty or b3_signal.empty:
        return "B4_COMPONENT_SIGNAL_BLOCKED"
    if b2_diagnostics["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED":
        return "B4_COMPONENT_SIGNAL_BLOCKED"
    if b3_diagnostics["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED":
        return "B4_COMPONENT_SIGNAL_BLOCKED"
    return "B4_COMPONENT_SIGNALS_READY"


def _metric_dict(payload: dict[str, Any], key: str) -> dict[str, float]:
    metrics = payload.get(key)
    if not isinstance(metrics, dict):
        return {}
    return {str(name): float(value) for name, value in metrics.items() if value is not None}


def _metric_dict_comparison(
    candidate: dict[str, Any],
    comparator: dict[str, Any],
) -> dict[str, float]:
    return {
        "return_delta": float(candidate["total_return"]) - float(comparator["total_return"]),
        "cagr_delta": float(candidate["cagr"]) - float(comparator["cagr"]),
        "drawdown_reduction": abs(float(comparator["max_drawdown"]))
        - abs(float(candidate["max_drawdown"])),
        "sharpe_delta": float(candidate["sharpe"] or 0.0) - float(comparator["sharpe"] or 0.0),
        "turnover_delta": float(candidate["turnover"]) - float(comparator["turnover"]),
    }


def _interaction_effects(
    *,
    b0r_metrics: dict[str, Any],
    b1e_metrics: dict[str, Any],
    b2_e0_metrics: dict[str, Any],
    b2_e1_metrics: dict[str, Any],
    b3_e0_metrics: dict[str, Any],
    b3_e1_metrics: dict[str, Any],
    b4_e0_metrics: dict[str, Any],
    b4_e1_metrics: dict[str, Any],
) -> dict[str, Any]:
    utility = {
        "B0R": _partial_utility(b0r_metrics),
        "B1E": _partial_utility(b1e_metrics),
        "B2_E0": _partial_utility(b2_e0_metrics),
        "B2_E1": _partial_utility(b2_e1_metrics),
        "B3_E0": _partial_utility(b3_e0_metrics),
        "B3_E1": _partial_utility(b3_e1_metrics),
        "B4_E0": _partial_utility(b4_e0_metrics),
        "B4_E1": _partial_utility(b4_e1_metrics),
    }
    effects = {
        "r_x_t_partial_utility": (
            utility["B4_E0"] - utility["B2_E0"] - utility["B3_E0"] + utility["B0R"]
        ),
        "e_x_r_partial_utility": (
            utility["B2_E1"] - utility["B2_E0"] - utility["B1E"] + utility["B0R"]
        ),
        "e_x_t_partial_utility": (
            utility["B3_E1"] - utility["B3_E0"] - utility["B1E"] + utility["B0R"]
        ),
        "e_x_r_x_t_partial_utility": (
            utility["B4_E1"]
            - utility["B4_E0"]
            - utility["B2_E1"]
            + utility["B2_E0"]
            - utility["B3_E1"]
            + utility["B3_E0"]
            + utility["B1E"]
            - utility["B0R"]
        ),
    }
    return {
        "classification": "INCONCLUSIVE",
        "classification_reason": (
            "Only return/drawdown/turnover partial utility is available; frozen "
            "scorecard components for tracking error, worst-window, dispersion, "
            "cost drag, stress and signal robustness penalties are not complete."
        ),
        "partial_utility_formula": (
            "total_return - 0.75 * abs(max_drawdown) - 0.25 * turnover"
        ),
        "missing_scorecard_components": [
            "tracking_error_penalty",
            "worst_window_penalty",
            "window_dispersion_penalty",
            "cost_drag",
            "signal_robustness_penalty",
            "stress_gate",
            "benchmark_relative_gate",
        ],
        "partial_utility_by_branch": utility,
        **effects,
    }


def _partial_utility(metrics: dict[str, Any]) -> float:
    if not metrics:
        return 0.0
    return (
        float(metrics["total_return"])
        - 0.75 * abs(float(metrics["max_drawdown"]))
        - 0.25 * float(metrics["turnover"])
    )


def _blocked_b4_payload(
    *,
    generated_at: datetime,
    start: date,
    end: date,
    reason: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": "TRADING-514A_to_514B",
        "report_type": "b4_risk_tilt_interaction_result",
        "status": "B4_INTERACTION_BLOCKED",
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "blocking_reason": reason,
        "blocking_details": details,
        "reader_brief": {
            "summary": "B4 blocked before producing interaction metrics.",
            "key_result": "B4_INTERACTION_BLOCKED",
            "blocking_issues": reason,
            "warnings": "No B4 metrics should be inferred from blocked output.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "repair B2/B3 branch or validation blocker before B4 rerun.",
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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


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
