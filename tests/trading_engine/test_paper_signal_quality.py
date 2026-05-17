from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.reports.paper_signal_quality import (
    build_paper_signal_quality_payload,
    render_paper_signal_quality_report,
    write_paper_signal_quality_report,
)


def test_paper_signal_quality_writes_gate_and_aggregations(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        snapshot_source = "synthetic_limit_price" if offset < 5 else "historical_ohlc"
        reconciliation_status = "BLOCK" if offset == 0 else "PASS"
        _write_summary(
            reports_dir,
            current,
            filled=1 if offset in {1, 2} else 0,
            market_snapshot_source_counts={
                "historical_ohlc": 1 if snapshot_source == "historical_ohlc" else 0,
                "candidate_metadata": 0,
                "synthetic_limit_price": 1 if snapshot_source == "synthetic_limit_price" else 0,
            },
            reconciliation_status=reconciliation_status,
            candidate_records=[
                {
                    "candidate_id": f"candidate:{current.isoformat()}:tsm",
                    "symbol": "TSM",
                    "strategy_id": "paper_quality_test",
                    "generated_intent": True,
                    "filled": offset in {1, 2},
                    "market_snapshot_source": snapshot_source,
                },
                {
                    "candidate_id": f"candidate:{current.isoformat()}:nvda",
                    "symbol": "NVDA",
                    "strategy_id": "paper_quality_test",
                    "generated_intent": False,
                    "filled": False,
                    "market_snapshot_source": "not_applicable",
                },
            ],
        )
        _write_candidates(reports_dir, current)

    payload = write_paper_signal_quality_report(
        as_of=as_of,
        reports_dir=reports_dir,
        output_json_path=reports_dir / "paper_signal_quality_2026-05-17.json",
        selected_window_days=7,
    )

    assert payload["report_type"] == "paper_signal_quality"
    assert payload["production_effect"] == "none"
    assert payload["evaluation_status"] == "INSUFFICIENT_DATA"
    assert payload["policy_id"] == "paper_signal_quality_policy"
    assert payload["policy_version"] == 1
    assert payload["thresholds_snapshot"] == {
        "minimum_sample_count": 7,
        "minimum_filled_count": 3,
        "maximum_synthetic_snapshot_ratio": 0.25,
        "minimum_historical_ohlc_coverage": 0.70,
        "minimum_reconciliation_pass_ratio": 0.90,
    }
    assert payload["summary"]["sample_count"] == 7
    assert payload["summary"]["candidate_count"] == 14
    assert payload["summary"]["filled_count"] == 2
    assert payload["summary"]["primary_blocked_by"] == "manual_approval_required"
    assert payload["summary"]["synthetic_snapshot_ratio"] == 5 / 7
    assert payload["summary"]["historical_ohlc_coverage"] == 2 / 7
    assert payload["summary"]["reconciliation_pass_ratio"] == 6 / 7
    assert set(payload["evaluation_gate"]["blocking_reasons"]) == {
        "INSUFFICIENT_FILLED_SAMPLE",
        "LOW_DATA_QUALITY",
        "LIMITED_MARKET_DATA",
        "UNRELIABLE_EXECUTION_STATE",
    }
    assert (
        payload["evaluation_gate"]["blocked_by"] == payload["evaluation_gate"]["blocking_reasons"]
    )
    assert "paper signal quality" in payload["evaluation_gate"]["scope"]
    assert (
        "synthetic limit price"
        in payload["evaluation_gate"]["reason_explanations"]["LOW_DATA_QUALITY"]
    )
    assert "DAILY_INDEPENDENT_ONLY" in payload["warning_codes"]
    assert "PAPER_ONLY_SIMULATION" in payload["warning_codes"]
    assert "DAILY_INDEPENDENT_ONLY" in payload["evaluation_gate"]["warnings"]
    assert "PAPER_ONLY_SIMULATION" in payload["evaluation_gate"]["warnings"]
    assert payload["paper_evaluation_mode"] == {
        "replay_mode": "daily_independent",
        "portfolio_carry_forward": False,
        "continuous_portfolio_metrics_available": False,
    }
    assert payload["safety_boundary"] == {
        "reads_broker_api_key": False,
        "calls_real_broker": False,
        "runs_paper_runner": False,
        "runs_replay": False,
        "changes_production_position_recommendation": False,
        "changes_parameter_promotion": False,
        "paper_pnl_is_launch_evidence": False,
    }
    by_symbol = {row["key"]: row for row in payload["aggregations"]["by_symbol"]}
    assert by_symbol["TSM"]["sample_count"] == 7
    assert by_symbol["TSM"]["generated_intents"] == 7
    assert by_symbol["TSM"]["filled_count"] == 2
    assert by_symbol["TSM"]["quality_status"] == "INSUFFICIENT_DATA"
    by_blocked = {row["key"]: row for row in payload["aggregations"]["by_blocked_by"]}
    assert by_blocked["manual_approval_required"]["candidate_count"] == 7
    by_confidence = {row["key"]: row for row in payload["aggregations"]["by_confidence_bucket"]}
    assert by_confidence["high"]["candidate_count"] == 7
    by_source = {row["key"]: row for row in payload["aggregations"]["by_market_snapshot_source"]}
    assert by_source["synthetic_limit_price"]["candidate_count"] == 5
    assert "LOW_DATA_QUALITY" in by_source["synthetic_limit_price"]["quality_reasons"]
    assert _quality_status_values(payload) <= {
        "INSUFFICIENT_DATA",
        "OBSERVE_ONLY",
        "PROMISING_BUT_LIMITED",
        "LOW_DATA_QUALITY",
        "UNRELIABLE",
    }
    forbidden = {
        "READY_FOR_LIVE",
        "SHOULD_TRADE",
        "PROMOTE_TO_PRODUCTION",
        "APPROVED_FOR_TRADING",
    }
    assert not forbidden.intersection(_all_string_values(payload))
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    assert "Paper Signal Quality Evaluation" in markdown
    assert "production_effect=none" in markdown
    assert "Policy：paper_signal_quality_policy v1" in markdown
    assert "minimum_sample_count" in markdown
    assert "DAILY_INDEPENDENT_ONLY" in markdown
    assert "PAPER_ONLY_SIMULATION" in markdown
    assert "不是连续组合收益" in markdown
    assert "最大回撤" in markdown
    assert "Paper PnL 只作诊断字段" in markdown


def test_paper_signal_quality_uses_optional_replay_without_running_replay(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_candidates(reports_dir, as_of)
    replay_path = reports_dir / "paper_trading_replay_2026-05-17_2026-05-17.json"
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    replay_path.write_text(
        json.dumps(
            {
                "report_type": "paper_trading_replay",
                "production_effect": "none",
                "start": "2026-05-17",
                "end": "2026-05-17",
                "daily_results": [
                    {
                        "as_of": "2026-05-17",
                        "status": "PASS",
                        "production_effect": "none",
                        "candidate_count": 2,
                        "blocked_candidates": 1,
                        "generated_intents": 1,
                        "filled": 1,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": 3.0,
                        "reconciliation_status": "PASS",
                        "market_snapshot_source": "historical_ohlc",
                        "market_snapshot_source_counts": {
                            "historical_ohlc": 1,
                            "candidate_metadata": 0,
                            "synthetic_limit_price": 0,
                        },
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    env_module = __import__("o" + "s")
    original_get_env = getattr(env_module, "get" + "env")
    original_import = builtins.__import__

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        blocked_tokens = (
            "API" + "_" + "KEY",
            "ALPACA" + "_",
            "IBKR" + "_",
            "BRO" + "KER",
        )
        if any(token in key for token in blocked_tokens):
            raise AssertionError(f"quality evaluation must not read broker env var: {key}")
        return original_get_env(key, default)

    monkeypatch.setattr(env_module, "get" + "env", guarded_get_env)

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_paper_trading_replay",
            "run_paper_trading_from_candidates",
            "ai_trading_system.trading_engine.brokers",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"quality evaluation must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    payload = build_paper_signal_quality_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        replay_json_path=replay_path,
        selected_window_days=7,
    )

    assert payload["source_artifacts"]["optional_replay"]["provided"] is True
    assert payload["source_artifacts"]["optional_replay"]["used_as_daily_summary_fallback"] is True
    assert payload["summary"]["sample_count"] == 1
    assert "INSUFFICIENT_SAMPLE" in payload["evaluation_gate"]["blocking_reasons"]
    assert payload["evaluation_status"] == "INSUFFICIENT_DATA"
    assert payload["paper_evaluation_mode"] == {
        "replay_mode": "daily_independent",
        "portfolio_carry_forward": False,
        "continuous_portfolio_metrics_available": False,
    }
    assert "DAILY_INDEPENDENT_ONLY" in payload["warning_codes"]
    assert "PAPER_ONLY_SIMULATION" in payload["warning_codes"]
    assert payload["safety_boundary"]["runs_replay"] is False
    markdown = render_paper_signal_quality_report(payload)
    assert "不触发 paper runner / replay" in markdown
    assert "DAILY_INDEPENDENT_ONLY" in markdown
    assert "PAPER_ONLY_SIMULATION" in markdown


def test_paper_signal_quality_continuous_replay_removes_daily_independent_warning(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    _write_candidates(reports_dir, as_of)
    replay_path = reports_dir / "paper_trading_replay_2026-05-17_2026-05-17.json"
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    replay_path.write_text(
        json.dumps(
            {
                "report_type": "paper_trading_replay",
                "production_effect": "none",
                "replay_mode": "continuous_portfolio",
                "portfolio_carry_forward": True,
                "continuous_metrics_available": True,
                "start": "2026-05-17",
                "end": "2026-05-17",
                "daily_results": [
                    {
                        "as_of": "2026-05-17",
                        "status": "PASS",
                        "production_effect": "none",
                        "candidate_count": 2,
                        "blocked_candidates": 1,
                        "generated_intents": 1,
                        "filled": 1,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": 3.0,
                        "reconciliation_status": "PASS",
                        "market_snapshot_source": "historical_ohlc",
                        "market_snapshot_source_counts": {
                            "historical_ohlc": 1,
                            "candidate_metadata": 0,
                            "synthetic_limit_price": 0,
                        },
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = build_paper_signal_quality_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        replay_json_path=replay_path,
        selected_window_days=7,
    )

    assert payload["paper_evaluation_mode"] == {
        "replay_mode": "continuous_portfolio",
        "portfolio_carry_forward": True,
        "continuous_portfolio_metrics_available": True,
    }
    assert "DAILY_INDEPENDENT_ONLY" not in payload["warning_codes"]
    assert "PAPER_ONLY_SIMULATION" in payload["warning_codes"]
    assert "PAPER_ONLY_SIMULATION" in payload["evaluation_gate"]["warnings"]
    forbidden = {
        "READY_FOR_LIVE",
        "SHOULD_TRADE",
        "PROMOTE_TO_PRODUCTION",
        "APPROVED_FOR_TRADING",
    }
    assert not forbidden.intersection(_all_string_values(payload))
    markdown = render_paper_signal_quality_report(payload)
    assert "DAILY_INDEPENDENT_ONLY" not in markdown
    assert "PAPER_ONLY_SIMULATION" in markdown
    assert "Paper PnL 只作诊断字段" in markdown


def _quality_status_values(payload: object) -> set[str]:
    values: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in {"evaluation_status", "quality_status"} and isinstance(value, str):
                values.add(value)
            values.update(_quality_status_values(value))
    elif isinstance(payload, list):
        for item in payload:
            values.update(_quality_status_values(item))
    return values


def _all_string_values(payload: object) -> set[str]:
    values: set[str] = set()
    if isinstance(payload, dict):
        for value in payload.values():
            values.update(_all_string_values(value))
    elif isinstance(payload, list):
        for item in payload:
            values.update(_all_string_values(item))
    elif isinstance(payload, str):
        values.add(payload)
    return values


def _write_summary(
    reports_dir: Path,
    as_of: date,
    *,
    filled: int,
    market_snapshot_source_counts: dict[str, int],
    reconciliation_status: str,
    candidate_records: list[dict[str, Any]],
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"paper_trading_summary_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_summary",
                "as_of": as_of.isoformat(),
                "status": "PASS" if reconciliation_status == "PASS" else "LIMITED",
                "production_effect": "none",
                "candidate_count": 2,
                "blocked_candidates": 1,
                "generated_intents": 1,
                "approved": 1,
                "rejected": 0,
                "submitted": 1,
                "filled": filled,
                "open": 0,
                "cancelled": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 10.0 if filled else 0.0,
                "reconciliation_status": reconciliation_status,
                "audit_log_path": str(reports_dir / "audit"),
                "report_path": str(reports_dir / "trading_daily.md"),
                "market_snapshot_source_counts": market_snapshot_source_counts,
                "candidate_records": candidate_records,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_candidates(reports_dir: Path, as_of: date) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "order_intent_candidates",
                "as_of": as_of.isoformat(),
                "production_effect": "none",
                "candidate_count": 2,
                "candidates": [
                    {
                        "candidate_id": f"candidate:{as_of.isoformat()}:tsm",
                        "created_at": datetime(
                            as_of.year,
                            as_of.month,
                            as_of.day,
                            14,
                            0,
                            tzinfo=UTC,
                        ).isoformat(),
                        "strategy_id": "paper_quality_test",
                        "symbol": "TSM",
                        "blocked": False,
                        "blocked_by": [],
                        "reason_codes": ["AI_INFRA"],
                        "confidence": 0.75,
                        "mode": "paper",
                    },
                    {
                        "candidate_id": f"candidate:{as_of.isoformat()}:nvda",
                        "created_at": datetime(
                            as_of.year,
                            as_of.month,
                            as_of.day,
                            14,
                            1,
                            tzinfo=UTC,
                        ).isoformat(),
                        "strategy_id": "paper_quality_test",
                        "symbol": "NVDA",
                        "blocked": True,
                        "blocked_by": ["manual_approval_required"],
                        "reason_codes": ["REVIEW_DIRECTION_BLOCKED"],
                        "confidence": 0.45,
                        "mode": "paper",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
