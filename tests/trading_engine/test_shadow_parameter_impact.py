from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.reports.shadow_parameter_impact import (
    ALLOWED_IMPACT_STATUSES,
    build_shadow_parameter_impact_payload,
    render_shadow_parameter_impact_report,
    write_shadow_parameter_impact_report,
)

DANGEROUS_OUTPUT_TERMS = (
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
    "APPROVED",
)


def test_shadow_parameter_impact_missing_shadow_sample_is_insufficient(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_day(
            reports_dir,
            current,
            [
                _candidate_record(
                    current,
                    "production",
                    generated=True,
                    filled=True,
                    unrealized_pnl=1.0,
                    market_snapshot_source="historical_ohlc",
                )
            ],
        )
        _write_signal_quality(reports_dir, current)

    payload = write_shadow_parameter_impact_report(
        as_of=as_of,
        reports_dir=reports_dir,
        selected_window_days=7,
    )

    assert payload["report_type"] == "shadow_parameter_impact"
    assert payload["production_effect"] == "none"
    assert payload["policy_id"] == "shadow_parameter_impact_policy"
    assert payload["policy_version"] == 1
    assert payload["thresholds_snapshot"]["minimum_shadow_sample_count"] == 7
    assert payload["impact_status"] == "INSUFFICIENT_DATA"
    assert payload["impact_status"] in ALLOWED_IMPACT_STATUSES
    assert payload["evaluation_scope"]["observe_only"] is True
    assert payload["safety_boundary"]["reads_broker_api_key"] is False
    assert payload["summary"]["sample_counts"] == {
        "production": 7,
        "shadow": 0,
        "unknown": 0,
    }
    assert "insufficient_shadow_sample" in payload["impact_gate"]["blocking_reasons"]
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    json_text = Path(payload["outputs"]["json"]).read_text(encoding="utf-8")
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    assert "Shadow Parameter Impact Evaluation" in markdown
    assert "Policy：shadow_parameter_impact_policy v1" in markdown
    assert "minimum_shadow_sample_count" in markdown
    assert "production_effect=none" in markdown
    _assert_no_dangerous_terms(json_text, markdown)


def test_shadow_parameter_impact_blocks_high_synthetic_snapshot_ratio(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_day(
            reports_dir,
            current,
            [
                _candidate_record(
                    current,
                    "production",
                    generated=True,
                    filled=True,
                    unrealized_pnl=1.0,
                    market_snapshot_source="historical_ohlc",
                ),
                _candidate_record(
                    current,
                    "shadow",
                    generated=True,
                    filled=True,
                    unrealized_pnl=3.0,
                    market_snapshot_source="synthetic_limit_price",
                ),
            ],
        )
        _write_signal_quality(reports_dir, current)

    payload = build_shadow_parameter_impact_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        selected_window_days=7,
    )

    assert payload["impact_status"] == "LOW_DATA_QUALITY"
    assert payload["impact_status"] in ALLOWED_IMPACT_STATUSES
    assert payload["impact_gate"]["blocked"] is True
    assert "synthetic_snapshot_ratio_too_high" in payload["impact_gate"]["blocking_reasons"]
    assert "low_data_quality" in payload["impact_gate"]["blocking_reasons"]
    shadow = payload["profile_comparison"]["shadow"]
    assert shadow["synthetic_snapshot_ratio"] == 1.0
    assert shadow["historical_ohlc_coverage"] == 0.0


def test_shadow_parameter_impact_warns_when_continuous_replay_missing(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_day(
            reports_dir,
            current,
            [
                _candidate_record(
                    current,
                    "production",
                    generated=True,
                    filled=True,
                    unrealized_pnl=1.0,
                    market_snapshot_source="historical_ohlc",
                ),
                _candidate_record(
                    current,
                    "shadow",
                    generated=True,
                    filled=True,
                    unrealized_pnl=2.0,
                    market_snapshot_source="historical_ohlc",
                ),
            ],
        )
        _write_signal_quality(reports_dir, current)

    payload = build_shadow_parameter_impact_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        selected_window_days=7,
    )

    assert payload["impact_status"] == "SHADOW_PROMISING_BUT_LIMITED"
    assert payload["impact_status"] in ALLOWED_IMPACT_STATUSES
    assert "continuous_replay_missing" in payload["warning_codes"]
    assert "continuous_replay_missing" in payload["impact_gate"]["warnings"]
    assert payload["continuous_replay"]["available"] is False
    _assert_gate_explanations_cover_codes(payload["impact_gate"])
    markdown = render_shadow_parameter_impact_report(payload)
    assert "continuous_replay_missing" in markdown


def test_daily_independent_replay_is_not_treated_as_continuous_portfolio(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_day(
            reports_dir,
            current,
            [
                _candidate_record(
                    current,
                    "production",
                    generated=True,
                    filled=True,
                    unrealized_pnl=1.0,
                    market_snapshot_source="historical_ohlc",
                ),
                _candidate_record(
                    current,
                    "shadow",
                    generated=True,
                    filled=True,
                    unrealized_pnl=2.0,
                    market_snapshot_source="historical_ohlc",
                ),
            ],
        )
    replay_path = reports_dir / "paper_trading_replay_2026-05-11_2026-05-17.json"
    _write_continuous_replay(replay_path, replay_mode="daily_independent")

    payload = build_shadow_parameter_impact_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        replay_json_path=replay_path,
        selected_window_days=7,
    )

    replay = payload["continuous_replay"]
    assert replay["available"] is False
    assert replay["replay_mode"] == "daily_independent"
    assert replay["source_artifact"]["path"] == str(replay_path)
    assert replay["source_artifact"]["mode"] == "daily_independent"
    assert replay["source_artifact"]["used_for_comparison"] is False
    assert replay["profiles"]["shadow"]["available"] is False
    assert replay["profiles"]["shadow"]["final_equity"] is None
    assert "continuous_replay_missing" in payload["warning_codes"]
    assert "daily_independent_only" in payload["warning_codes"]


def test_shadow_pnl_better_but_sample_insufficient_has_no_promote_semantics(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        records = [
            _candidate_record(
                current,
                "production",
                generated=True,
                filled=True,
                unrealized_pnl=1.0,
                market_snapshot_source="historical_ohlc",
            )
        ]
        if offset < 2:
            records.append(
                _candidate_record(
                    current,
                    "shadow",
                    generated=True,
                    filled=True,
                    unrealized_pnl=25.0,
                    market_snapshot_source="historical_ohlc",
                )
            )
        _write_paper_day(reports_dir, current, records)

    payload = write_shadow_parameter_impact_report(
        as_of=as_of,
        reports_dir=reports_dir,
        selected_window_days=7,
    )

    assert payload["profile_comparison"]["shadow"]["paper_pnl_total"] == 50.0
    assert payload["impact_status"] == "INSUFFICIENT_DATA"
    assert payload["impact_status"] in ALLOWED_IMPACT_STATUSES
    assert "insufficient_shadow_sample" in payload["impact_gate"]["blocking_reasons"]
    json_text = Path(payload["outputs"]["json"]).read_text(encoding="utf-8")
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    _assert_no_dangerous_terms(json_text, markdown)


def test_shadow_parameter_impact_is_read_only_and_does_not_use_broker(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 17)
    for offset in range(7):
        current = date.fromordinal(as_of.toordinal() - offset)
        _write_paper_day(
            reports_dir,
            current,
            [
                _candidate_record(
                    current,
                    "production",
                    generated=True,
                    filled=True,
                    unrealized_pnl=1.0,
                    market_snapshot_source="historical_ohlc",
                ),
                _candidate_record(
                    current,
                    "shadow",
                    generated=True,
                    filled=True,
                    unrealized_pnl=2.0,
                    market_snapshot_source="historical_ohlc",
                ),
            ],
        )
    replay_path = reports_dir / "paper_trading_replay_2026-05-11_2026-05-17.json"
    _write_continuous_replay(replay_path)

    env_module = __import__("o" + "s")
    original_get_env = getattr(env_module, "get" + "env")
    original_import = builtins.__import__

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        blocked_tokens = ("API" + "_" + "KEY", "ALPACA" + "_", "IBKR" + "_", "BRO" + "KER")
        if any(token in key for token in blocked_tokens):
            raise AssertionError(f"impact evaluation must not read broker env var: {key}")
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
            raise AssertionError(f"impact evaluation must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    payload = build_shadow_parameter_impact_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        replay_json_path=replay_path,
        selected_window_days=7,
    )

    assert payload["production_effect"] == "none"
    assert payload["impact_status"] in ALLOWED_IMPACT_STATUSES
    assert payload["safety_boundary"] == {
        "reads_broker_api_key": False,
        "calls_real_broker": False,
        "runs_paper_runner": False,
        "runs_replay": False,
        "changes_production_parameters": False,
        "changes_production_position_recommendation": False,
        "changes_parameter_promotion": False,
        "changes_trade_execution": False,
        "paper_pnl_is_launch_evidence": False,
    }
    assert payload["continuous_replay"]["available"] is True
    assert payload["continuous_replay"]["source_artifact"] == {
        "exists": True,
        "path": str(replay_path),
        "mode": "continuous_portfolio",
        "date_range": {"start": "2026-05-11", "end": "2026-05-17"},
        "used_for_comparison": True,
    }
    assert "continuous_replay_missing" not in payload["warning_codes"]
    assert "daily_independent_only" not in payload["warning_codes"]
    _assert_gate_explanations_cover_codes(payload["impact_gate"])


def _candidate_record(
    as_of: date,
    profile: str,
    *,
    generated: bool,
    filled: bool,
    unrealized_pnl: float,
    market_snapshot_source: str,
) -> dict[str, Any]:
    candidate_id = f"candidate:{as_of.isoformat()}:{profile}"
    return {
        "candidate_id": candidate_id,
        "source_profile": profile,
        "strategy_id": f"{profile}_paper_strategy",
        "strategy_version": f"{profile}_v1",
        "mode": "paper",
        "symbol": "TSM" if profile == "production" else "NVDA",
        "blocked": not generated,
        "blocked_by": [] if generated else ["manual_approval_required"],
        "reason_codes": ["AI_INFRA"] if generated else ["REVIEW_DIRECTION_BLOCKED"],
        "confidence": 0.76 if profile == "shadow" else 0.72,
        "generated_intent": generated,
        "filled": filled,
        "unrealized_pnl": unrealized_pnl,
        "market_snapshot_source": market_snapshot_source,
    }


def _write_paper_day(
    reports_dir: Path,
    as_of: date,
    records: list[dict[str, Any]],
    *,
    reconciliation_status: str = "PASS",
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        {
            "candidate_id": record["candidate_id"],
            "source_profile": record["source_profile"],
            "strategy_id": record["strategy_id"],
            "strategy_version": record["strategy_version"],
            "mode": record["mode"],
            "symbol": record["symbol"],
            "blocked": record["blocked"],
            "blocked_by": record["blocked_by"],
            "reason_codes": record["reason_codes"],
            "confidence": record["confidence"],
            "metadata": {"source_profile": record["source_profile"]},
        }
        for record in records
    ]
    generated = [record for record in records if record["generated_intent"]]
    filled = [record for record in records if record["filled"]]
    source_counts: dict[str, int] = {}
    for record in generated:
        source = str(record["market_snapshot_source"])
        source_counts[source] = source_counts.get(source, 0) + 1
    (reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "order_intent_candidates",
                "as_of": as_of.isoformat(),
                "production_effect": "none",
                "candidate_count": len(candidates),
                "candidates": candidates,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (reports_dir / f"paper_trading_summary_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_summary",
                "as_of": as_of.isoformat(),
                "status": "PASS" if reconciliation_status == "PASS" else "LIMITED",
                "production_effect": "none",
                "candidate_count": len(records),
                "blocked_candidates": sum(1 for record in records if record["blocked"]),
                "generated_intents": len(generated),
                "approved": len(generated),
                "rejected": 0,
                "submitted": len(generated),
                "filled": len(filled),
                "open": 0,
                "cancelled": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": sum(float(record["unrealized_pnl"]) for record in records),
                "reconciliation_status": reconciliation_status,
                "market_snapshot_source_counts": source_counts,
                "candidate_records": [
                    {
                        "candidate_id": record["candidate_id"],
                        "source_profile": record["source_profile"],
                        "generated_intent": record["generated_intent"],
                        "filled": record["filled"],
                        "realized_pnl": 0.0,
                        "unrealized_pnl": record["unrealized_pnl"] if record["filled"] else 0.0,
                        "market_snapshot_source": record["market_snapshot_source"],
                        "reconciliation_status": reconciliation_status,
                    }
                    for record in records
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_signal_quality(reports_dir: Path, as_of: date) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"paper_signal_quality_{as_of.isoformat()}.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_signal_quality",
                "as_of": as_of.isoformat(),
                "production_effect": "none",
                "evaluation_status": "OBSERVE_ONLY",
                "summary": {"sample_count": 7},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_continuous_replay(
    path: Path,
    *,
    replay_mode: str = "continuous_portfolio",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "paper_trading_replay",
                "generated_at": datetime(2026, 5, 17, 23, 0, tzinfo=UTC).isoformat(),
                "start": "2026-05-11",
                "end": "2026-05-17",
                "production_effect": "none",
                "replay_mode": replay_mode,
                "portfolio_carry_forward": replay_mode == "continuous_portfolio",
                "continuous_metrics_available": replay_mode == "continuous_portfolio",
                "profile_results": {
                    "production": {
                        "final_equity": 100007.0,
                        "max_drawdown_pct": -0.01,
                        "filled_count": 7,
                    },
                    "shadow": {
                        "final_equity": 100014.0,
                        "max_drawdown_pct": -0.01,
                        "filled_count": 7,
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _assert_no_dangerous_terms(*texts: str) -> None:
    combined = "\n".join(texts)
    for term in DANGEROUS_OUTPUT_TERMS:
        assert term not in combined


def _assert_gate_explanations_cover_codes(gate: dict[str, Any]) -> None:
    reason_explanations = gate["reason_explanations"]
    warning_explanations = gate["warning_explanations"]
    for reason in gate["blocking_reasons"]:
        assert reason in reason_explanations
        assert reason_explanations[reason]
    for warning in gate["warnings"]:
        assert warning in reason_explanations
        assert warning in warning_explanations
        assert warning_explanations[warning]
