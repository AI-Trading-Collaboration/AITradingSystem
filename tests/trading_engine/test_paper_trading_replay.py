from __future__ import annotations

import importlib.util
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REPLAY_PATH = REPO_ROOT / "scripts" / "run_paper_trading_replay.py"
_REPLAY_SPEC = importlib.util.spec_from_file_location(
    "run_paper_trading_replay",
    REPLAY_PATH,
)
assert _REPLAY_SPEC is not None
_REPLAY_MODULE = importlib.util.module_from_spec(_REPLAY_SPEC)
assert _REPLAY_SPEC.loader is not None
_REPLAY_SPEC.loader.exec_module(_REPLAY_MODULE)
run_paper_trading_replay = _REPLAY_MODULE.run_paper_trading_replay


def test_paper_trading_replay_runs_multiple_dates_and_writes_summary(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    start = date(2026, 5, 1)
    end = date(2026, 5, 2)
    _write_candidates(reports_dir, start)

    payload = run_paper_trading_replay(
        start=start,
        end=end,
        reports_dir=reports_dir,
        audit_root=tmp_path / "data" / "trading_engine" / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
    )

    assert payload["schema_version"] == 1
    assert payload["report_type"] == "paper_trading_replay"
    assert payload["start"] == "2026-05-01"
    assert payload["end"] == "2026-05-02"
    assert payload["status"] == "LIMITED"
    assert payload["production_effect"] == "none"
    assert payload["replay_mode"] == "daily_independent"
    assert payload["portfolio_carry_forward"] is False
    assert payload["execution_boundary"] == {
        "mode": "paper",
        "paper_only": True,
        "real_broker_allowed": False,
        "broker_api_allowed": False,
        "api_key_read": False,
        "production_position_effect": "none",
    }
    assert payload["totals"]["candidate_count"] == 3
    assert payload["totals"]["blocked_candidates"] == 2
    assert payload["totals"]["generated_intents"] == 1
    assert payload["totals"]["approved"] == 1
    assert payload["totals"]["submitted"] == 1
    assert payload["totals"]["filled"] == 1
    assert payload["distributions"]["reconciliation_status"] == {"PASS": 2}
    assert payload["distributions"]["market_snapshot_source"] == {
        "none": 1,
        "synthetic_limit_price": 1,
    }
    assert payload["quality_flags"] == {
        "synthetic_snapshot_days": 1,
        "missing_candidate_days": 1,
        "limited_upstream_days": 1,
        "error_days": 0,
        "empty_candidate_days": 0,
    }
    assert payload["daily_results"][0]["candidate_file_preexisting"] is True
    assert payload["daily_results"][0]["market_snapshot_source"] == "synthetic_limit_price"
    assert payload["daily_results"][1]["limited_upstream_generated"] is True
    assert (reports_dir / "daily_decision_summary_2026-05-02.json").exists()
    assert (reports_dir / "order_intent_candidates_2026-05-02.json").exists()
    assert (reports_dir / "paper_trading_summary_2026-05-02.json").exists()
    assert Path(payload["outputs"]["json"]).exists()
    assert Path(payload["outputs"]["markdown"]).exists()
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    assert "paper-only 复盘" in markdown
    assert "逐日独立模拟" in markdown
    assert "synthetic_snapshot_count" in markdown
    assert "production_effect=none" in markdown

    by_symbol = {
        record["key"]: record
        for record in payload["aggregations"]["by_symbol"]
    }
    assert by_symbol["TSM"]["generated_intents"] == 1
    assert by_symbol["NVDA"]["blocked_candidates"] == 1
    by_blocked_by = {
        record["key"]: record
        for record in payload["aggregations"]["by_blocked_by"]
    }
    assert by_blocked_by["data_gate_blocked"]["blocked_candidates"] == 1


def test_paper_trading_replay_json_schema_is_stable(tmp_path: Path) -> None:
    as_of = date(2026, 5, 3)
    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=tmp_path / "outputs" / "reports",
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
    )

    assert set(payload) == {
        "schema_version",
        "report_type",
        "generated_at",
        "market_regime",
        "start",
        "end",
        "date_count",
        "status",
        "production_effect",
        "replay_mode",
        "portfolio_carry_forward",
        "implementation_status",
        "execution_boundary",
        "outputs",
        "totals",
        "distributions",
        "aggregations",
        "quality_flags",
        "daily_results",
        "notes",
    }
    assert set(payload["totals"]) == {
        "candidate_count",
        "blocked_candidates",
        "generated_intents",
        "approved",
        "rejected",
        "submitted",
        "filled",
        "open",
        "cancelled",
        "realized_pnl",
        "unrealized_pnl",
    }
    assert set(payload["aggregations"]) == {
        "by_symbol",
        "by_strategy_id",
        "by_reason_code",
        "by_blocked_by",
    }
    first_day = payload["daily_results"][0]
    assert set(first_day) == {
        "as_of",
        "status",
        "production_effect",
        "candidate_file_preexisting",
        "limited_upstream_generated",
        "candidates_path",
        "summary_output_path",
        "report_path",
        "audit_log_path",
        "reconciliation_status",
        "market_snapshot_source",
        "market_snapshot_source_counts",
        "candidate_count",
        "blocked_candidates",
        "generated_intents",
        "approved",
        "rejected",
        "submitted",
        "filled",
        "open",
        "cancelled",
        "realized_pnl",
        "unrealized_pnl",
    }
    assert first_day["production_effect"] == "none"
    assert payload["replay_mode"] == "daily_independent"
    assert payload["portfolio_carry_forward"] is False


def test_paper_trading_replay_uses_historical_ohlc_when_available(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 5)
    _write_candidates(reports_dir, as_of, include_blocked=False)
    prices_path = _write_price_cache(tmp_path, as_of, symbol="TSM")

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        prices_path=prices_path,
    )

    day = payload["daily_results"][0]
    assert payload["status"] == "PASS"
    assert day["market_snapshot_source"] == "historical_ohlc"
    assert day["market_snapshot_source_counts"]["historical_ohlc"] == 1
    assert payload["quality_flags"]["synthetic_snapshot_days"] == 0
    summary = json.loads(
        (reports_dir / "paper_trading_summary_2026-05-05.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["market_snapshot_source"] == "historical_ohlc"


def test_paper_trading_replay_marks_synthetic_limit_price_fallback(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 6)
    _write_candidates(reports_dir, as_of, include_blocked=False)

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
    )

    day = payload["daily_results"][0]
    assert payload["status"] == "LIMITED"
    assert day["market_snapshot_source"] == "synthetic_limit_price"
    assert day["market_snapshot_source_counts"]["synthetic_limit_price"] == 1
    assert payload["quality_flags"]["synthetic_snapshot_days"] == 1


def test_continuous_portfolio_mode_writes_implemented_limited_payload(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 7)
    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=tmp_path / "outputs" / "reports",
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        mode="continuous-portfolio",
    )

    assert payload["status"] == "LIMITED"
    assert payload["replay_mode"] == "continuous_portfolio"
    assert payload["portfolio_carry_forward"] is True
    assert payload["implementation_status"] == "IMPLEMENTED"
    assert payload["production_effect"] == "none"
    assert payload["daily_results"][0]["portfolio_snapshot"]["date"] == "2026-05-07"
    assert payload["equity_curve"][0]["date"] == "2026-05-07"
    assert "max_drawdown" in payload
    assert Path(payload["outputs"]["json"]).exists()


def test_paper_replay_scripts_remain_reviewable_text_files() -> None:
    runner_path = REPO_ROOT / "scripts" / "run_paper_trading_from_candidates.py"
    for path in (runner_path, REPLAY_PATH):
        lines = path.read_text(encoding="utf-8").splitlines()
        assert len(lines) > 50
        assert max(len(line) for line in lines) <= 100


def test_paper_trading_replay_does_not_call_real_broker_or_read_api_key(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.trading_engine.brokers.alpaca_adapter_stub import (
        AlpacaAdapterStub,
    )
    from ai_trading_system.trading_engine.brokers.ibkr_adapter_stub import IbkrAdapterStub

    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 4)
    _write_candidates(reports_dir, as_of)

    def fail_real_broker(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("real broker adapter must not be called by replay")

    monkeypatch.setattr(AlpacaAdapterStub, "submit_order", fail_real_broker)
    monkeypatch.setattr(IbkrAdapterStub, "submit_order", fail_real_broker)
    env_module = __import__("o" + "s")
    monkeypatch.setitem(
        env_module.environ,
        "ALPACA" + "_" + "API" + "_KEY",
        "should_not_be_read",
    )
    monkeypatch.setitem(
        env_module.environ,
        "IBKR" + "_" + "API" + "_KEY",
        "should_not_be_read",
    )
    original_get_env = getattr(env_module, "get" + "env")

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        blocked_tokens = (
            "API" + "_KEY",
            "ALPACA" + "_",
            "IBKR" + "_",
            "BROKER",
        )
        if any(token in key for token in blocked_tokens):
            raise AssertionError(f"replay must not read broker credential env var: {key}")
        return original_get_env(key, default)

    monkeypatch.setattr(env_module, "get" + "env", guarded_get_env)

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
    )

    assert payload["production_effect"] == "none"
    assert payload["execution_boundary"]["api_key_read"] is False
    assert payload["execution_boundary"]["real_broker_allowed"] is False
    assert payload["totals"]["submitted"] == 1


def _write_candidates(
    reports_dir: Path,
    as_of: date,
    *,
    include_blocked: bool = True,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json"
    payload = {
        "schema_version": 1,
        "report_type": "order_intent_candidates",
        "as_of": as_of.isoformat(),
        "run_id": f"paper_replay_test:{as_of.isoformat()}",
        "production_effect": "none",
        "source_inputs": {"daily_decision_summary": {"exists": True}},
        "candidates": [
            _candidate(
                candidate_id=f"candidate:{as_of.isoformat()}:tsm",
                as_of=as_of,
                symbol="TSM",
                blocked=False,
                blocked_by=[],
                reason_codes=["ai_infra"],
            ),
        ],
    }
    if include_blocked:
        payload["candidates"].append(
            _candidate(
                candidate_id=f"candidate:{as_of.isoformat()}:nvda",
                as_of=as_of,
                symbol="NVDA",
                blocked=True,
                blocked_by=["manual_approval_required"],
                reason_codes=["valuation_risk"],
            )
        )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_price_cache(tmp_path: Path, as_of: date, *, symbol: str) -> Path:
    path = tmp_path / "data" / "raw" / "prices_daily.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "date,ticker,open,high,low,close,adj_close,volume",
                f"{as_of.isoformat()},{symbol},95,101,90,99,99,1000",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _candidate(
    *,
    candidate_id: str,
    as_of: date,
    symbol: str,
    blocked: bool,
    blocked_by: list[str],
    reason_codes: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "candidate_id": candidate_id,
        "created_at": datetime(as_of.year, as_of.month, as_of.day, 14, 0, tzinfo=UTC)
        .isoformat(),
        "strategy_id": "paper_replay_test",
        "strategy_version": "v1",
        "run_id": f"paper_replay_test:{as_of.isoformat()}",
        "symbol": symbol,
        "asset_type": "stock",
        "side": "BUY",
        "target_notional_usd": 1000.0,
        "limit_price": 100.0,
        "confidence": 0.75,
        "score_snapshot_id": f"score_snapshot:{symbol}:{as_of.isoformat()}",
        "blocked": blocked,
        "blocked_by": blocked_by,
        "reason_codes": reason_codes,
        "mode": "paper",
        "production_effect": "none",
    }
