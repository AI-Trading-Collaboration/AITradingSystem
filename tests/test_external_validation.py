from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
)
from ai_trading_system.external_validation import (
    EXTERNAL_VALIDATION_STRATEGY_IDS,
    STATIC_BASELINE_IDS,
    run_dynamic_weight_path_external_support_check,
    run_external_independent_return_replay,
    run_external_platform_feasibility_review,
    run_external_platform_metric_convention_signoff,
    run_external_validation_difference_attribution,
    run_external_validation_manual_evidence_master_review,
    run_external_validation_manual_evidence_owner_signoff,
    run_external_validation_master_review,
    run_external_validation_owner_report,
    run_external_validation_reader_brief_safe_preview,
    run_external_validation_scope_contract,
    run_manual_external_record_template,
    run_metric_definition_reconciliation,
    run_quantconnect_replication_dry_run_plan,
    run_quantconnect_weight_path_replay_preflight,
    run_sgov_external_convention_signoff,
    run_sgov_total_return_external_check,
    run_static_baseline_external_manual_input_ingestion,
    run_static_baseline_external_manual_runbook,
    run_static_baseline_external_reconciliation,
    run_static_baseline_final_reconciliation_after_manual_input,
    run_strategy_weight_path_export,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

EXTERNAL_VALIDATION_REPORT_IDS = {
    "external_validation_scope_contract",
    "static_baseline_external_reconciliation",
    "strategy_weight_path_export",
    "external_independent_return_replay",
    "metric_definition_reconciliation",
    "sgov_total_return_external_check",
    "external_platform_feasibility_review",
    "quantconnect_replication_dry_run_plan",
    "external_validation_difference_attribution",
    "external_validation_owner_report",
    "external_validation_master_review",
    "external_validation_reader_brief_safe_preview",
    "external_validation_real_result_status_reader",
    "static_baseline_reconciliation_final_check",
    "dynamic_weight_path_replay_final_check",
    "metric_and_sgov_reconciliation_signoff",
    "external_validation_to_launch_gate",
    "manual_external_record_template",
    "static_baseline_external_manual_runbook",
    "static_baseline_external_manual_input_ingestion",
    "external_platform_metric_convention_signoff",
    "sgov_external_convention_signoff",
    "static_baseline_final_reconciliation_after_manual_input",
    "dynamic_weight_path_external_support_check",
    "quantconnect_weight_path_replay_preflight",
    "external_validation_manual_evidence_owner_signoff",
    "external_validation_manual_evidence_master_review",
}

WEIGHT_PATH_COLUMNS = {
    "date",
    "strategy_id",
    "definition_hash",
    "target_weight_qqq",
    "target_weight_tqqq",
    "target_weight_sgov",
    "rebalance_flag",
    "signal_time",
    "execution_assumption",
    "data_quality_status",
}


def test_external_validation_builders_reconcile_and_preserve_safety(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_external_validation_caches(tmp_path)
    growth_config_path = _write_small_growth_config(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "external_validation"
    docs_root = tmp_path / "docs" / "research"
    static_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "output_root": output_root,
        "as_of_date": as_of,
    }
    data_kwargs = {**static_kwargs, "growth_config_path": growth_config_path}

    scope = run_external_validation_scope_contract(output_root=output_root)
    pending_static = run_static_baseline_external_reconciliation(**static_kwargs)
    external_records_path = _write_matching_external_records(tmp_path, pending_static)
    static = run_static_baseline_external_reconciliation(
        **static_kwargs,
        external_records_path=external_records_path,
    )
    weight_export = run_strategy_weight_path_export(**data_kwargs)
    replay = run_external_independent_return_replay(
        **data_kwargs,
        _weight_export_payload=weight_export,
    )
    metric = run_metric_definition_reconciliation(output_root=output_root)
    sgov = run_sgov_total_return_external_check(**data_kwargs)
    feasibility = run_external_platform_feasibility_review(output_root=output_root)
    quantconnect = run_quantconnect_replication_dry_run_plan(output_root=output_root)
    pending_difference = run_external_validation_difference_attribution(
        **data_kwargs,
        _static_reconciliation_payload=pending_static,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
    )
    difference = run_external_validation_difference_attribution(
        **data_kwargs,
        _static_reconciliation_payload=static,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
    )
    owner = run_external_validation_owner_report(
        **data_kwargs,
        docs_path=docs_root / "external_validation_owner_report.md",
        _scope_payload=scope,
        _static_payload=static,
        _replay_payload=replay,
        _metric_payload=metric,
        _sgov_payload=sgov,
        _feasibility_payload=feasibility,
        _difference_payload=difference,
    )
    master = run_external_validation_master_review(
        **data_kwargs,
        docs_path=docs_root / "external_validation_master_review.md",
        owner_docs_path=docs_root / "external_validation_owner_report.md",
        _owner_payload=owner,
    )
    reader = run_external_validation_reader_brief_safe_preview(
        **data_kwargs,
        _master_payload=master,
    )

    assert scope["status"] == "EXTERNAL_VALIDATION_SCOPE_READY"
    assert set(scope["strategy_ids"]) == set(EXTERNAL_VALIDATION_STRATEGY_IDS)
    assert set(scope["baseline_ids"]) == set(STATIC_BASELINE_IDS)
    assert pending_static["status"] == "STATIC_BASELINE_RECONCILED_WITH_WARNINGS"
    assert pending_static["external_record_status"] == "MANUAL_EXTERNAL_INPUT_PENDING"
    assert static["status"] == "STATIC_BASELINE_RECONCILED"
    assert all(row["within_tolerance"] is True for row in static["reconciliation_rows"])
    assert weight_export["status"] in {"WEIGHT_PATH_EXPORT_READY", "WEIGHT_PATH_EXPORT_WARN"}
    assert set(weight_export["exported_weight_paths"]) == {
        "equal_risk_qqq_sgov",
        FOCUSED_GROWTH_TILT_CANDIDATE_ID,
    }
    for path_text in weight_export["exported_weight_paths"].values():
        frame = pd.read_csv(path_text)
        assert WEIGHT_PATH_COLUMNS <= set(frame.columns)
        assert not frame.empty
        assert set(frame["data_quality_status"]) == {weight_export["data_quality"]["status"]}
    assert replay["status"] in {
        "INDEPENDENT_REPLAY_MATCHED",
        "INDEPENDENT_REPLAY_MATCHED_WITH_WARNINGS",
    }
    assert len(replay["replay_rows"]) == 2
    assert all(row["all_metrics_within_tolerance"] is True for row in replay["replay_rows"])
    assert metric["status"] == "METRIC_DEFINITIONS_RECONCILED_WITH_WARNINGS"
    assert sgov["status"] in {
        "SGOV_TOTAL_RETURN_RECONCILED",
        "SGOV_PRICE_ONLY_DIFFERENCE_WARN",
    }
    assert feasibility["status"] == "EXTERNAL_PLATFORM_DYNAMIC_REPLAY_NEEDED"
    assert quantconnect["status"] == "QUANTCONNECT_REPLICATION_PLAN_READY"
    assert any(
        row["primary_difference_reason"] == "manual_external_input_pending"
        for row in pending_difference["difference_rows"]
    )
    assert difference["status"] in {"DIFFERENCES_EXPLAINED", "DIFFERENCE_ATTRIBUTION_READY"}
    assert owner["owner_recommendation"] in {
        "EXTERNAL_VALIDATION_ACCEPTED",
        "EXTERNAL_VALIDATION_ACCEPTED_WITH_WARNINGS",
    }
    assert master["status"] in {
        "EXTERNAL_VALIDATION_PASS",
        "EXTERNAL_VALIDATION_PASS_WITH_WARNINGS",
    }
    assert reader["status"] == "EXTERNAL_VALIDATION_READER_PREVIEW_SAFE"
    assert reader["prohibited_phrase_hits"] == []
    assert (docs_root / "external_validation_owner_report.md").exists()
    assert (docs_root / "external_validation_master_review.md").exists()

    for payload in (
        scope,
        pending_static,
        static,
        weight_export,
        replay,
        metric,
        sgov,
        feasibility,
        quantconnect,
        pending_difference,
        difference,
        owner,
        master,
        reader,
    ):
        _assert_external_validation_safety(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "strategy-weight-path-export",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--growth-config",
            str(growth_config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert (output_root / "strategy_weight_path_export.json").exists()


def test_manual_external_evidence_signoff_builders_and_cli(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_external_validation_caches(tmp_path)
    growth_config_path = _write_small_growth_config(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "external_validation"
    docs_root = tmp_path / "docs" / "research"
    template_dir = tmp_path / "inputs" / "external_validation" / "manual_external_records"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "growth_config_path": growth_config_path,
        "output_root": output_root,
        "as_of_date": as_of,
        "end_date": as_of,
    }
    static_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "output_root": output_root,
        "as_of_date": as_of,
        "end_date": as_of,
    }

    template = run_manual_external_record_template(
        output_root=output_root,
        template_dir=template_dir,
        guide_path=docs_root / "manual_external_record_input_guide.md",
        end_date=as_of,
    )
    runbook = run_static_baseline_external_manual_runbook(
        output_root=output_root,
        docs_path=docs_root / "static_baseline_external_manual_runbook.md",
        end_date=as_of,
    )
    pending_static = run_static_baseline_external_reconciliation(**static_kwargs)
    manual_records_path = _write_manual_external_records(tmp_path, pending_static, as_of)
    metric_signoff_path = _write_metric_convention_signoff(tmp_path)
    sgov_check = run_sgov_total_return_external_check(**common)
    sgov_signoff_path = _write_sgov_convention_signoff(tmp_path, sgov_check)
    manual_input = run_static_baseline_external_manual_input_ingestion(
        output_root=output_root,
        input_yaml_path=manual_records_path,
        input_csv_path=tmp_path / "missing_manual_records.csv",
        end_date=as_of,
    )
    metric_signoff = run_external_platform_metric_convention_signoff(
        output_root=output_root,
        signoff_path=metric_signoff_path,
        input_yaml_path=manual_records_path,
        input_csv_path=tmp_path / "missing_manual_records.csv",
        _manual_input_payload=manual_input,
    )
    sgov_signoff = run_sgov_external_convention_signoff(
        **common,
        signoff_path=sgov_signoff_path,
        _sgov_check_payload=sgov_check,
    )
    final_reconciliation = run_static_baseline_final_reconciliation_after_manual_input(
        **common,
        input_yaml_path=manual_records_path,
        input_csv_path=tmp_path / "missing_manual_records.csv",
        metric_signoff_path=metric_signoff_path,
        sgov_signoff_path=sgov_signoff_path,
        _manual_input_payload=manual_input,
        _metric_signoff_payload=metric_signoff,
        _sgov_signoff_payload=sgov_signoff,
    )
    dynamic_support = run_dynamic_weight_path_external_support_check(output_root=output_root)
    qc_preflight = run_quantconnect_weight_path_replay_preflight(output_root=output_root)
    owner = run_external_validation_manual_evidence_owner_signoff(
        **common,
        docs_path=docs_root / "external_validation_manual_evidence_owner_signoff.md",
        input_yaml_path=manual_records_path,
        input_csv_path=tmp_path / "missing_manual_records.csv",
        metric_signoff_path=metric_signoff_path,
        sgov_signoff_path=sgov_signoff_path,
        _manual_input_payload=manual_input,
        _metric_signoff_payload=metric_signoff,
        _sgov_signoff_payload=sgov_signoff,
        _final_reconciliation_payload=final_reconciliation,
        _dynamic_support_payload=dynamic_support,
        _qc_preflight_payload=qc_preflight,
    )
    master = run_external_validation_manual_evidence_master_review(
        **common,
        docs_path=docs_root / "external_validation_manual_evidence_master_review.md",
        owner_docs_path=docs_root / "external_validation_manual_evidence_owner_signoff.md",
        _owner_signoff_payload=owner,
    )

    assert template["status"] == "MANUAL_EXTERNAL_TEMPLATE_READY"
    assert Path(template["template_paths"]["yaml"]).exists()
    assert Path(template["template_paths"]["csv"]).exists()
    assert Path(template["template_paths"]["guide"]).exists()
    assert runbook["status"] == "MANUAL_RUNBOOK_READY"
    assert manual_input["status"] == "MANUAL_EXTERNAL_INPUT_RECORDED"
    assert {row["strategy_id"] for row in manual_input["valid_records"]} == set(
        STATIC_BASELINE_IDS
    )
    assert metric_signoff["status"] == "METRIC_CONVENTIONS_CONFIRMED"
    assert sgov_signoff["status"] == "SGOV_CONVENTION_CONFIRMED"
    assert final_reconciliation["status"] == "STATIC_BASELINE_MANUAL_RECONCILED"
    assert all(
        row["within_tolerance"] is True
        for row in final_reconciliation["reconciliation_rows"]
    )
    assert dynamic_support["status"] == "DYNAMIC_EXTERNAL_SUPPORT_REQUIRES_CUSTOM_ENGINE"
    assert qc_preflight["status"] == "QC_WEIGHT_PATH_PREFLIGHT_NEEDS_MANUAL_IMPLEMENTATION"
    assert owner["owner_recommendation"] == "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS"
    assert master["status"] == "EXTERNAL_MANUAL_EVIDENCE_ACCEPTED_WITH_WARNINGS"
    assert (docs_root / "external_validation_manual_evidence_owner_signoff.md").exists()
    assert (docs_root / "external_validation_manual_evidence_master_review.md").exists()

    for payload in (
        template,
        runbook,
        manual_input,
        metric_signoff,
        sgov_check,
        sgov_signoff,
        final_reconciliation,
        dynamic_support,
        qc_preflight,
        owner,
        master,
    ):
        _assert_external_validation_safety(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "static-baseline-external-manual-input-ingestion",
            "--input-yaml-path",
            str(manual_records_path),
            "--input-csv-path",
            str(tmp_path / "missing_manual_records.csv"),
            "--end-date",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert (output_root / "static_baseline_external_manual_input_ingestion.json").exists()


def test_external_validation_report_registry_contracts() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert EXTERNAL_VALIDATION_REPORT_IDS <= set(entries)
    for report_id in EXTERNAL_VALIDATION_REPORT_IDS:
        entry = entries[report_id]
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"
        assert entry["command"].startswith("aits research strategies ")
        assert entry["artifact_globs"]


def _assert_external_validation_safety(payload: dict[str, object]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["market_regime"] == "ai_after_chatgpt"
    artifact_paths = payload["artifact_paths"]
    assert isinstance(artifact_paths, dict)
    assert Path(str(artifact_paths["json_path"])).exists()
    assert Path(str(artifact_paths["markdown_path"])).exists()


def _write_matching_external_records(tmp_path: Path, payload: dict[str, object]) -> Path:
    records = []
    for row in payload["reconciliation_rows"]:
        assert isinstance(row, dict)
        records.append(
            {
                "external_tool": "test_fixture_external",
                "strategy_id": row["strategy_id"],
                "date_range": row["date_range"],
                "rebalance_frequency": "monthly",
                "external_annual_return": row["internal_annual_return"],
                "external_max_drawdown": row["internal_max_drawdown"],
                "external_sharpe": row["internal_sharpe"],
                "external_calmar": row["internal_calmar"],
                "external_monthly_returns_if_available": [],
                "manual_input_notes": "fixture mirrors internal metrics to test tolerance wiring",
                "screenshot_or_export_reference": "fixture://static-baseline",
            }
        )
    path = tmp_path / "external_static_records.json"
    path.write_text(json.dumps({"records": records}, indent=2), encoding="utf-8")
    return path


def _write_manual_external_records(
    tmp_path: Path,
    payload: dict[str, object],
    as_of: date,
) -> Path:
    weights = {
        "100_qqq": {"QQQ": 1.0},
        "qqq_50_sgov_50": {"QQQ": 0.5, "SGOV": 0.5},
        "qqq_60_sgov_40": {"QQQ": 0.6, "SGOV": 0.4},
    }
    records = []
    for row in payload["reconciliation_rows"]:
        assert isinstance(row, dict)
        records.append(
            {
                "external_tool": "test_fixture_external",
                "external_tool_url_or_name": "fixture://external-platform",
                "strategy_id": row["strategy_id"],
                "date_range_start": "2022-12-01",
                "date_range_end": as_of.isoformat(),
                "asset_weights": weights[row["strategy_id"]],
                "rebalance_frequency": "monthly",
                "dividend_reinvestment": "reinvested",
                "price_or_total_return_policy": "adjusted",
                "annual_return": row["internal_annual_return"],
                "max_drawdown": row["internal_max_drawdown"],
                "sharpe": row["internal_sharpe"],
                "calmar": row["internal_calmar"],
                "turnover": "metric_unavailable_on_platform",
                "monthly_returns_available": True,
                "export_file_path": "fixture://external-platform-export.csv",
                "screenshot_reference": "",
                "manual_notes": (
                    "fixture external record mirrors platform export for tolerance wiring"
                ),
                "owner": "test_owner",
                "recorded_at": "2026-06-26T00:00:00Z",
            }
        )
    path = tmp_path / "static_baseline_external_records.yaml"
    path.write_text(yaml.safe_dump({"records": records}, sort_keys=False), encoding="utf-8")
    return path


def _write_metric_convention_signoff(tmp_path: Path) -> Path:
    records = []
    for metric_name, platform_definition in {
        "annual_return": "CAGR",
        "max_drawdown": "daily path",
        "sharpe": "zero_rf",
        "calmar": "CAGR / abs(max_drawdown)",
        "turnover": "unavailable",
        "rebalance": "monthly",
        "dividend": "reinvested",
    }.items():
        records.append(
            {
                "external_tool": "test_fixture_external",
                "metric_name": metric_name,
                "platform_definition": platform_definition,
                "definition_match_status": "match",
                "manual_confirmation_status": "confirmed",
                "difference_expected": False,
                "owner_notes": "fixture signoff",
            }
        )
    path = tmp_path / "external_platform_metric_convention_signoff.yaml"
    path.write_text(yaml.safe_dump({"records": records}, sort_keys=False), encoding="utf-8")
    return path


def _write_sgov_convention_signoff(
    tmp_path: Path,
    sgov_check: dict[str, object],
) -> Path:
    summary = sgov_check["sgov_total_return_summary"]
    assert isinstance(summary, dict)
    annual_return = summary["SGOV_adjusted_close_return"]
    path = tmp_path / "sgov_external_convention_signoff.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "records": [
                    {
                        "external_tool": "test_fixture_external",
                        "sgov_convention": "adjusted",
                        "internal_sgov_convention": "adj_close",
                        "sgov_annual_return_external": annual_return,
                        "sgov_annual_return_internal": annual_return,
                        "annual_return_delta": 0.0,
                        "impact_on_static_baselines": "no material fixture delta",
                        "impact_on_dynamic_strategies": "no material fixture delta",
                        "convention_accepted": True,
                        "owner_notes": "fixture SGOV adjusted close convention accepted",
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_small_growth_config(tmp_path: Path) -> Path:
    config = yaml.safe_load(DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH.read_text())
    policy = config["research_policy"]
    grids = policy["search_grids"]
    grids["cap_floor_tilt"] = {
        "qqq_max_weight": [0.70],
        "sgov_min_weight": [0.30],
        "rebalance": ["monthly"],
    }
    grids["risk_budget_tilt"] = {
        "qqq_risk_budget": [0.65],
        "sgov_risk_budget": [0.35],
        "vol_lookback": [60],
        "rebalance": ["monthly"],
    }
    grids["trend_on_qqq_boost"] = {
        "boost_amount": [0.10],
        "rebalance": ["monthly"],
    }
    policy["missed_upside_policy"]["thresholds"] = [0.05]
    policy["missed_upside_policy"]["compensation_amounts"] = [0.10]
    policy["missed_upside_policy"]["ramp_days"] = [10]
    grids["small_tqqq_overlay"] = {
        "max_tqqq_weight": [0.05],
        "rebalance": ["monthly"],
    }
    grids["vol_target_growth_tilt"] = {
        "target_vol_absolute": [0.15],
        "target_vol_additive_pp": [0.04],
        "vol_lookback": [120],
        "qqq_max_weight": [0.70],
        "sgov_min_weight": [0.10],
    }
    config_path = tmp_path / "equal_risk_growth_tilt_candidate_registry.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _write_external_validation_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.00065 + 0.0018 * math.sin(day_index / 19.0)
        if 90 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 185:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.00025
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            adj_close = close
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{adj_close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)

    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")

    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result
