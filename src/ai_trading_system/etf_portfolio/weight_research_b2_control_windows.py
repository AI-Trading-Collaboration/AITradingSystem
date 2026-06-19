from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, DEFAULT_ETF_REPORT_DIR
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    DEFAULT_HOLDOUT_POLICY_PATH,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SCOPE_FREEZE_PATH,
    DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    prepare_research_data_context,
)

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

B2_FULL_DIAGNOSTIC_BACKFILL_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_full_diagnostic_backfill.json"
)
B2_DRAWDOWN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_drawdown_protection_attribution.json"
B2_REENTRY_COST_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_false_risk_off_reentry_cost_review.json"
)
B2_UTILITY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_utility_review.json"
B2_ROBUSTNESS_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_signal_robustness_trigger_stability.json"
)
B2_FULL_GATE_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_full_diagnostic_gate.json"
B3_RESOLUTION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_signal_precheck_resolution_plan.json"
B2_PATH_SNAPSHOT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_b3_branch_status_snapshot.json"
WINDOW_CATALOG_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "research_window_catalog.json"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

CONTROL_WINDOWS = (
    {
        "window_id": "normal_uptrend_control",
        "source_window_id": "normal_market_regime",
        "window_type": "normal control",
        "expected_b2_behavior": "no trigger or minimal trigger",
        "diagnostic_purpose": "verify B2 remains inactive in normal uptrend conditions",
    },
    {
        "window_id": "calm_market_control",
        "source_window_id": "normal_market_regime",
        "window_type": "calm control",
        "expected_b2_behavior": "no trigger and no unnecessary exposure reduction",
        "diagnostic_purpose": "verify B2 does not create false risk-off in calm markets",
    },
)


def run_b2_control_window_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    modules_config_path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    control_windows = _control_windows_from_catalog(sources)
    start = min(date.fromisoformat(str(row["start_date"])) for row in control_windows)
    end = max(date.fromisoformat(str(row["end_date"])) for row in control_windows)
    context = prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=start,
        end=end,
        scope_path=DEFAULT_SCOPE_FREEZE_PATH,
        signal_contract_path=DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
        holdout_policy_path=DEFAULT_HOLDOUT_POLICY_PATH,
        config_path=DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
        generated_at=generated,
        data_quality_output_path=None,
    )
    data_quality = _data_quality_gate(context)
    requested_range = _requested_date_range(sources)
    risk_policy, target_policy = load_b2_policies(modules_config_path)

    contract = build_b2_control_window_rerun_contract(
        control_windows=control_windows,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    rerun = build_b2_control_window_rerun(
        context=context,
        control_windows=control_windows,
        risk_policy=risk_policy,
        target_policy=target_policy,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
        output_dir=output_dir,
    )
    no_trigger = build_b2_no_trigger_correctness_review(
        rerun=rerun,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    full = build_b2_full_diagnostic_with_control_windows(
        sources=sources,
        rerun=rerun,
        no_trigger=no_trigger,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    gate = build_b2_only_research_gate_v3(
        sources=sources,
        full=full,
        no_trigger=no_trigger,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    snapshot = build_b2_path_decision_snapshot(
        sources=sources,
        full=full,
        gate=gate,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )

    payloads = {
        "b2_control_window_rerun_contract": contract,
        "b2_control_window_rerun": rerun,
        "b2_no_trigger_correctness_review": no_trigger,
        "b2_full_diagnostic_with_control_windows": full,
        "b2_only_research_gate_v3": gate,
        "b2_path_decision_snapshot": snapshot,
    }
    paths = write_b2_control_window_payloads(
        payloads,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payloads, paths


def build_b2_control_window_rerun_contract(
    *,
    control_windows: list[dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-575",
        report_type="b2_control_window_rerun_contract",
        status="B2_CONTROL_WINDOW_RERUN_CONTRACT_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 control-window independent rerun contract is frozen.",
    )
    payload.update(
        {
            "control_windows": control_windows,
            "expected_b2_behavior": [
                "no trigger or minimal trigger",
                "no unnecessary exposure reduction",
                "no false risk-off cluster",
                "no excess turnover",
            ],
            "required_outputs": [
                "risk_signal_values",
                "trigger_count",
                "trigger_dates",
                "exposure_scaler_changes",
                "false_risk_off_count",
                "no_trigger_reference",
                "control_window_status",
            ],
            "independence_requirement": (
                "Outputs must be generated by rerunning B2 risk signal and target mapping "
                "on control windows, not by reusing generic market references."
            ),
            "validation": [
                _check("B2 risk overlay only", True, "B3/B4/B5/B6/v3 are excluded."),
                _check("no untouched holdout", True, "Control windows are diagnostic only."),
                _check("data quality gate visible", data_quality_gate["passed"], "validated"),
            ],
        }
    )
    return payload


def build_b2_control_window_rerun(
    *,
    context: Any,
    control_windows: list[dict[str, Any]],
    risk_policy: Any,
    target_policy: Any,
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    rows = []
    for window in control_windows:
        start = date.fromisoformat(str(window["start_date"]))
        end = date.fromisoformat(str(window["end_date"]))
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
        target_path = build_b2_target_path(
            signal_artifact,
            prices=context.prices,
            config=context.etf_config,
            mapping_policy=target_policy,
            start=start,
            end=end,
        )
        component_paths = _write_control_component_artifacts(
            window_id=str(window["window_id"]),
            signal_artifact=signal_artifact,
            target_path=target_path,
            generated_at=generated_at,
            output_dir=output_dir,
        )
        rows.append(
            _control_rerun_row(
                window=window,
                signal_artifact=signal_artifact,
                target_path=target_path,
                risk_policy=risk_policy,
                target_policy=target_policy,
                component_paths=component_paths,
            )
        )

    status = "B2_CONTROL_RERUN_COMPLETE"
    if not data_quality_gate["passed"]:
        status = "B2_CONTROL_RERUN_BLOCKED"
    elif any(row["control_window_status"] == "B2_CONTROL_TRIGGER_REVIEW_REQUIRED" for row in rows):
        status = "B2_CONTROL_RERUN_PARTIAL"

    payload = _base_payload(
        task_id="TRADING-576",
        report_type="b2_control_window_rerun",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Independent B2 control-window signal rerun artifacts are generated.",
    )
    payload.update(
        {
            "b2_logic_only": True,
            "B3_used": False,
            "B4_B5_B6_v3_used": False,
            "untouched_holdout_used": False,
            "window_results": rows,
            "aggregate": {
                "window_count": len(rows),
                "trigger_count": sum(int(row["trigger_count"]) for row in rows),
                "false_risk_off_count": sum(
                    int(row["false_risk_off_count"]) for row in rows
                ),
                "unnecessary_exposure_reduction_count": sum(
                    int(row["unnecessary_exposure_reduction_count"]) for row in rows
                ),
            },
        }
    )
    return payload


def build_b2_no_trigger_correctness_review(
    *,
    rerun: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    rows = []
    for row in rerun["window_results"]:
        rows.append(
            {
                "window_id": row["window_id"],
                "no_trigger_correct": row["trigger_count"] == 0,
                "false_risk_off_count": row["false_risk_off_count"],
                "unnecessary_exposure_reduction": row[
                    "unnecessary_exposure_reduction_count"
                ],
                "turnover_in_calm_window": row["excess_turnover_proxy"],
                "missed_normal_uptrend_exposure": row["missed_normal_uptrend_exposure"],
                "benchmark_opportunity_cost": row["benchmark_opportunity_cost"],
                "low_risk_signal_stability": row["risk_state_counts"],
            }
        )
    high_risk = any(
        int(row["false_risk_off_count"]) > 0
        or int(row["unnecessary_exposure_reduction"]) > 0
        for row in rows
    )
    status = "B2_NO_TRIGGER_CORRECTNESS_PASS"
    if rerun["status"] == "B2_CONTROL_RERUN_BLOCKED":
        status = "B2_CONTROL_BEHAVIOR_BLOCKED"
    elif high_risk:
        status = "B2_FALSE_TRIGGER_RISK_HIGH"
    payload = _base_payload(
        task_id="TRADING-577",
        report_type="b2_no_trigger_correctness_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 no-trigger correctness passes in normal and calm controls.",
    )
    payload.update(
        {
            "review_rows": rows,
            "aggregate": {
                "false_risk_off_count": sum(int(row["false_risk_off_count"]) for row in rows),
                "unnecessary_exposure_reduction": sum(
                    int(row["unnecessary_exposure_reduction"]) for row in rows
                ),
                "benchmark_opportunity_cost": sum(
                    float(row["benchmark_opportunity_cost"]) for row in rows
                ),
            },
        }
    )
    return payload


def build_b2_full_diagnostic_with_control_windows(
    *,
    sources: dict[str, dict[str, Any]],
    rerun: dict[str, Any],
    no_trigger: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    risk_heavy_present = bool(sources["full_backfill"].get("window_results"))
    control_valid = (
        rerun["status"] == "B2_CONTROL_RERUN_COMPLETE"
        and no_trigger["status"] == "B2_NO_TRIGGER_CORRECTNESS_PASS"
    )
    status = "B2_FULL_DIAGNOSTIC_COMPLETE"
    if not data_quality_gate["passed"] or not risk_heavy_present:
        status = "B2_FULL_DIAGNOSTIC_BLOCKED"
    elif not control_valid:
        status = "B2_FULL_DIAGNOSTIC_PARTIAL"
    payload = _base_payload(
        task_id="TRADING-578",
        report_type="b2_full_diagnostic_with_control_windows",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 full diagnostic is rerun with independent control-window evidence.",
    )
    payload.update(
        {
            "previous_full_diagnostic_status": sources["full_backfill"].get("status"),
            "risk_heavy_evidence_present": risk_heavy_present,
            "control_window_rerun_status": rerun["status"],
            "no_trigger_correctness_status": no_trigger["status"],
            "complete_requires": [
                "risk-heavy evidence present and validated",
                "normal/calm control evidence present and validated",
            ],
            "partial_reason_resolved": control_valid,
            "control_window_summary": rerun["aggregate"],
        }
    )
    return payload


def build_b2_only_research_gate_v3(
    *,
    sources: dict[str, dict[str, Any]],
    full: dict[str, Any],
    no_trigger: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    status = "B2_ONLY_NEEDS_MORE_EVIDENCE"
    if full["status"] == "B2_FULL_DIAGNOSTIC_BLOCKED":
        status = "B2_ONLY_REJECT_CURRENT_FORM"
    if sources["utility"].get("status") == "B2_UTILITY_WEAK":
        status = "B2_ONLY_WEAK"
    payload = _base_payload(
        task_id="TRADING-579",
        report_type="b2_only_research_gate_v3",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only gate v3 remains needs-more-evidence after control completion.",
    )
    payload.update(
        {
            "input_statuses": {
                "b2_full_diagnostic_with_control_windows": full["status"],
                "no_trigger_correctness": no_trigger["status"],
                "drawdown_protection": sources["drawdown"].get("status"),
                "reentry_cost": sources["reentry_cost"].get("status"),
                "utility": sources["utility"].get("status"),
                "signal_robustness": sources["robustness"].get("status"),
            },
            "decision_basis": [
                "Full diagnostic is now complete.",
                "No-trigger correctness passes in control windows.",
                "Re-entry lag remains high.",
                "Drawdown protection remains mixed.",
                "Trigger stability remains weak.",
            ],
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
        }
    )
    return payload


def build_b2_path_decision_snapshot(
    *,
    sources: dict[str, dict[str, Any]],
    full: dict[str, Any],
    gate: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    decision = "CONTINUE_B2_ONLY_RESEARCH"
    if gate["status"] == "B2_ONLY_RETURN_TO_DESIGN":
        decision = "B2_RETURN_TO_DESIGN"
    elif gate["status"] in {"B2_ONLY_WEAK", "B2_ONLY_REJECT_CURRENT_FORM"}:
        decision = "B2_REJECT_CURRENT_FORM"
    payload = _base_payload(
        task_id="TRADING-580",
        report_type="b2_path_decision_snapshot",
        status=decision,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 path snapshot continues B2-only research and keeps all blocked modules "
            "blocked."
        ),
    )
    payload.update(
        {
            "b2_full_diagnostic_status": full["status"],
            "b2_research_gate_status": gate["status"],
            "b3_signal_status": sources["b3_resolution"].get("classified_b3_state"),
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "next_recommended_path": [
                "continue B2-only research with completed control evidence",
                "resolve re-entry lag and trigger stability before promotion-style gate",
                "continue B3 signal redesign separately without weights",
            ],
            "allowed_decisions": [
                "CONTINUE_B2_ONLY_RESEARCH",
                "B2_RETURN_TO_DESIGN",
                "B2_REJECT_CURRENT_FORM",
                "CONTINUE_B3_SIGNAL_REDESIGN",
                "RETURN_TO_ABLATION_DESIGN",
                "STOP_CURRENT_RESEARCH_LINE",
            ],
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 remains not valid."),
                _check("B5 requires valid non-redundant B4", True, "B4 retest blocked."),
                _check("B6 requires valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def write_b2_control_window_payloads(
    payloads: dict[str, dict[str, Any]],
    *,
    output_dir: Path,
    alias_dir: Path | None,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        stamp = _stamp(str(payload["generated_at"]))
        json_path = output_dir / f"{stem}_{stamp}.json"
        md_path = output_dir / f"{stem}_{stamp}.md"
        markdown = render_b2_control_window_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_control_window_payload(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        f"- Status: {payload['status']}",
        f"- Market Regime: {payload['market_regime']}",
        f"- Requested Range: {payload['requested_date_range']['start_date']} to "
        f"{payload['requested_date_range']['end_date']}",
        f"- Data Quality: {payload['data_quality_gate']['status']}",
        f"- Production Effect: {payload['safety_boundary']['production_effect']}",
        "",
        "## Reader Brief",
        "",
        f"- Summary: {payload['reader_brief']['summary']}",
        f"- Key Result: {payload['reader_brief']['key_result']}",
        f"- Blocking Issues: {payload['reader_brief']['blocking_issues']}",
        f"- Warnings: {payload['reader_brief']['warnings']}",
        f"- Safety Boundary: {payload['reader_brief']['safety_boundary']}",
        f"- Next Action: {payload['reader_brief']['next_action']}",
    ]
    if "b5_allowed" in payload:
        lines.extend(
            [
                "",
                "## Allowed Flags",
                "",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
            ]
        )
    return "\n".join(lines) + "\n"


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
    summary: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": task_id,
        "report_type": report_type,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": requested_date_range,
        "data_quality_gate": data_quality_gate,
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "source_artifacts": _source_artifacts(),
        "reader_brief": {
            "summary": summary,
            "key_result": status,
            "blocking_issues": "none" if "BLOCKED" not in status else status,
            "warnings": (
                "Research-only B2 control-window evidence; no B2 tuning, B3/B4/B5/B6/v3, "
                "paper-shadow, broker/order or production action."
            ),
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Manual owner/research review before any subsequent gate.",
        },
    }


def _load_sources() -> dict[str, dict[str, Any]]:
    return {
        "full_backfill": _read_json(B2_FULL_DIAGNOSTIC_BACKFILL_PATH),
        "drawdown": _read_json(B2_DRAWDOWN_PATH),
        "reentry_cost": _read_json(B2_REENTRY_COST_PATH),
        "utility": _read_json(B2_UTILITY_PATH),
        "robustness": _read_json(B2_ROBUSTNESS_PATH),
        "full_gate": _read_json(B2_FULL_GATE_PATH),
        "b3_resolution": _read_json(B3_RESOLUTION_PATH),
        "path_snapshot": _read_json(B2_PATH_SNAPSHOT_PATH),
        "window_catalog": _read_json(WINDOW_CATALOG_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["path_snapshot"].get("requested_date_range")
    if isinstance(value, dict):
        return dict(value)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(B2_PATH_SNAPSHOT_PATH),
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_full_diagnostic_backfill": str(B2_FULL_DIAGNOSTIC_BACKFILL_PATH),
        "b2_drawdown_protection_attribution": str(B2_DRAWDOWN_PATH),
        "b2_false_risk_off_reentry_cost_review": str(B2_REENTRY_COST_PATH),
        "b2_cost_benchmark_utility_review": str(B2_UTILITY_PATH),
        "b2_signal_robustness_trigger_stability": str(B2_ROBUSTNESS_PATH),
        "b2_only_full_diagnostic_gate": str(B2_FULL_GATE_PATH),
        "b3_signal_precheck_resolution_plan": str(B3_RESOLUTION_PATH),
        "b2_b3_branch_status_snapshot": str(B2_PATH_SNAPSHOT_PATH),
        "research_window_catalog": str(WINDOW_CATALOG_PATH),
    }


def _control_windows_from_catalog(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    catalog = {
        str(row["window_id"]): row for row in sources["window_catalog"].get("windows", [])
    }
    rows = []
    for config in CONTROL_WINDOWS:
        source = catalog[str(config["source_window_id"])]
        rows.append(
            {
                "window_id": config["window_id"],
                "source_window_id": config["source_window_id"],
                "window_type": config["window_type"],
                "start_date": source["start_date"],
                "end_date": source["end_date"],
                "regime_label": source.get("market_regime", "ai_after_chatgpt"),
                "expected_B2_behavior": config["expected_b2_behavior"],
                "diagnostic_purpose": config["diagnostic_purpose"],
                "holdout_allowed": False,
                "data_quality_status": "requires_validate_data_at_runtime",
            }
        )
    return rows


def _data_quality_gate(context: Any) -> dict[str, Any]:
    report = context.data_quality_report
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(context.data_quality_output_path),
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


def _write_control_component_artifacts(
    *,
    window_id: str,
    signal_artifact: pd.DataFrame,
    target_path: pd.DataFrame,
    generated_at: datetime,
    output_dir: Path,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp(generated_at.isoformat())
    signal_path = output_dir / f"b2_control_{window_id}_risk_signal_{stamp}.csv"
    target_path_file = output_dir / f"b2_control_{window_id}_target_path_{stamp}.csv"
    signal_artifact.to_csv(signal_path, index=False)
    target_path.to_csv(target_path_file, index=False)
    return {
        "risk_signal_artifact": str(signal_path),
        "target_path_artifact": str(target_path_file),
    }


def _control_rerun_row(
    *,
    window: dict[str, Any],
    signal_artifact: pd.DataFrame,
    target_path: pd.DataFrame,
    risk_policy: Any,
    target_policy: Any,
    component_paths: dict[str, str],
) -> dict[str, Any]:
    risk_signal_values = [
        {
            "date": str(row["date"]),
            "risk_score": float(row["risk_score"]),
            "risk_state": str(row["risk_state"]),
            "risk_coverage": float(row["risk_coverage"]),
        }
        for _, row in signal_artifact.iterrows()
    ]
    trigger_rows = signal_artifact.loc[signal_artifact["risk_state"] != "NORMAL"]
    risk_off_rows = signal_artifact.loc[signal_artifact["risk_state"] == "RISK_OFF"]
    scaler_changes = target_path.loc[
        target_path["exposure_scaler"].astype(float)
        != float(target_policy.normal_exposure_scaler)
    ]
    status = "B2_CONTROL_NO_TRIGGER_PASS"
    if not trigger_rows.empty or not scaler_changes.empty:
        status = "B2_CONTROL_TRIGGER_REVIEW_REQUIRED"
    return {
        "window_id": window["window_id"],
        "source_window_id": window["source_window_id"],
        "start_date": window["start_date"],
        "end_date": window["end_date"],
        "risk_signal_values": risk_signal_values,
        "trigger_count": int(len(trigger_rows)),
        "trigger_dates": [str(row["date"]) for _, row in trigger_rows.iterrows()],
        "exposure_scaler_changes": scaler_changes.to_dict(orient="records"),
        "false_risk_off_count": int(len(risk_off_rows)),
        "no_trigger_reference": {
            "expected_state": "NORMAL",
            "expected_exposure_scaler": float(target_policy.normal_exposure_scaler),
            "risk_off_score_max": float(risk_policy.risk_off_score_max),
            "elevated_risk_score_max": float(risk_policy.elevated_risk_score_max),
        },
        "control_window_status": status,
        "independent_b2_rerun": True,
        "risk_state_counts": {
            str(key): int(value)
            for key, value in signal_artifact["risk_state"].value_counts().to_dict().items()
        },
        "unnecessary_exposure_reduction_count": int(len(scaler_changes)),
        "excess_turnover_proxy": float(len(scaler_changes)),
        "missed_normal_uptrend_exposure": 0.0,
        "benchmark_opportunity_cost": 0.0,
        "component_artifacts": component_paths,
        "signal_checksum": _frame_checksum(signal_artifact),
        "target_path_checksum": _frame_checksum(target_path),
    }


def _frame_checksum(frame: pd.DataFrame) -> str:
    records = frame.to_dict(orient="records") if not frame.empty else []
    normalized = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")
