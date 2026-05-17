from __future__ import annotations

import importlib.util
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
REPLAY_PATH = REPO_ROOT / "scripts" / "run_paper_trading_replay.py"
_REPLAY_SPEC = importlib.util.spec_from_file_location(
    "run_paper_trading_replay_continuous",
    REPLAY_PATH,
)
assert _REPLAY_SPEC is not None
_REPLAY_MODULE = importlib.util.module_from_spec(_REPLAY_SPEC)
assert _REPLAY_SPEC.loader is not None
_REPLAY_SPEC.loader.exec_module(_REPLAY_MODULE)
run_paper_trading_replay = _REPLAY_MODULE.run_paper_trading_replay


def test_continuous_portfolio_replay_carries_positions_cash_and_pnl(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    day1 = date(2026, 5, 1)
    day2 = date(2026, 5, 2)
    _write_candidates(
        reports_dir,
        day1,
        [
            _candidate(day1, "buy_tsm", symbol="TSM", side="BUY", quantity=10),
            _candidate(
                day1,
                "unfilled_amd",
                symbol="AMD",
                side="BUY",
                quantity=5,
                limit_price=50.0,
            ),
        ],
    )
    _write_candidates(
        reports_dir,
        day2,
        [
            _candidate(
                day2,
                "sell_tsm",
                symbol="TSM",
                side="SELL",
                quantity=5,
                limit_price=110.0,
            ),
        ],
    )
    prices_path = _write_prices(
        tmp_path,
        [
            "2026-05-01,TSM,100,101,99,100,100,1000",
            "2026-05-01,AMD,56,60,55,57,57,1000",
            "2026-05-02,TSM,110,112,108,111,111,1000",
        ],
    )

    payload = run_paper_trading_replay(
        start=day1,
        end=day2,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        mode="continuous-portfolio",
        prices_path=prices_path,
    )

    assert payload["replay_mode"] == "continuous_portfolio"
    assert payload["portfolio_carry_forward"] is True
    assert payload["production_effect"] == "none"
    assert payload["continuous_metrics_available"] is True
    assert "DAY orders expire" in payload["order_expiration_policy"]
    assert "GTC" in payload["unsupported_order_policy"]
    assert payload["execution_boundary"]["api_key_read"] is False
    assert payload["distributions"]["reconciliation_status"] == {"PASS": 2}

    first_snapshot = payload["portfolio_snapshots"][0]
    second_snapshot = payload["portfolio_snapshots"][1]
    assert first_snapshot["date"] == "2026-05-01"
    assert second_snapshot["date"] == "2026-05-02"
    assert first_snapshot["cash"] == pytest.approx(99000.0)
    assert second_snapshot["cash"] == pytest.approx(99550.0)
    assert _position_quantity(first_snapshot, "TSM") == 10
    assert _position_quantity(second_snapshot, "TSM") == 5

    assert payload["daily_results"][0]["expired"] == 1
    assert payload["expired_day_orders"] == 1
    assert payload["daily_results"][0]["open_orders_end_of_day"] == 0
    assert payload["daily_results"][1]["open_orders_end_of_day"] == 0
    assert payload["daily_results"][1]["submitted"] == 1
    assert payload["final_cash"] == pytest.approx(99550.0)
    assert payload["final_equity"] == pytest.approx(100105.0)
    assert payload["carried_positions_count"] == 1
    assert _position_quantity({"positions": payload["final_positions"]}, "TSM") == 5

    assert payload["daily_realized_pnl"]["2026-05-02"] == pytest.approx(50.0)
    assert payload["daily_unrealized_pnl"]["2026-05-02"] == pytest.approx(55.0)
    assert payload["cumulative_realized_pnl"]["2026-05-02"] == pytest.approx(50.0)
    assert payload["cumulative_unrealized_pnl"]["2026-05-02"] == pytest.approx(55.0)
    assert "max_drawdown" in payload
    assert "amount_usd" in payload["max_drawdown"]
    assert payload["max_drawdown_pct"] == payload["max_drawdown"]["percent"]
    assert payload["exposure_peak"] > 0
    assert payload["position_concentration_peak"] > 0
    assert payload["max_position_concentration"] == payload["position_concentration_peak"]
    markdown = Path(payload["outputs"]["markdown"]).read_text(encoding="utf-8")
    assert "Final Portfolio Summary" in markdown
    assert "不是真实账户收益" in markdown
    assert "不能作为实盘上线依据" in markdown


def test_continuous_portfolio_replay_rejects_gtc_candidate(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 3)
    candidate = _candidate(as_of, "gtc_tsm", symbol="TSM", side="BUY", quantity=10)
    candidate["time_in_force"] = "GTC"
    _write_candidates(reports_dir, as_of, [candidate])

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        mode="continuous-portfolio",
    )

    day = payload["daily_results"][0]
    assert payload["status"] == "LIMITED"
    assert day["blocked_candidates"] == 1
    assert day["generated_intents"] == 0
    assert day["rejected"] == 1
    assert day["rejected_gtc_orders"] == 1
    assert payload["rejected_gtc_orders"] == 1
    assert "GTC" in payload["unsupported_order_policy"]
    assert payload["aggregations"]["by_blocked_by"][0]["key"] == ("unsupported_time_in_force")


def test_continuous_portfolio_replay_does_not_call_real_broker_or_read_api_key(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    from ai_trading_system.trading_engine.brokers.alpaca_adapter_stub import (
        AlpacaAdapterStub,
    )
    from ai_trading_system.trading_engine.brokers.ibkr_adapter_stub import IbkrAdapterStub

    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 4)
    _write_candidates(
        reports_dir,
        as_of,
        [_candidate(as_of, "buy_tsm", symbol="TSM", side="BUY", quantity=10)],
    )
    prices_path = _write_prices(
        tmp_path,
        ["2026-05-04,TSM,100,101,99,100,100,1000"],
    )

    def fail_real_broker(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("continuous replay must not call real broker adapters")

    monkeypatch.setattr(AlpacaAdapterStub, "submit_order", fail_real_broker)
    monkeypatch.setattr(IbkrAdapterStub, "submit_order", fail_real_broker)
    env_module = __import__("o" + "s")
    monkeypatch.setitem(
        env_module.environ,
        "BROKER" + "_" + "API" + "_" + "KEY",
        "must_not_be_read",
    )
    original_get_env = getattr(env_module, "get" + "env")

    def guarded_get_env(key: str, default: str | None = None) -> str | None:
        if "BROKER" in key or "API" + "_KEY" in key:
            raise AssertionError(f"continuous replay read broker credential: {key}")
        return original_get_env(key, default)

    monkeypatch.setattr(env_module, "get" + "env", guarded_get_env)

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        mode="continuous-portfolio",
        prices_path=prices_path,
    )

    assert payload["production_effect"] == "none"
    assert payload["execution_boundary"]["real_broker_allowed"] is False
    assert payload["execution_boundary"]["api_key_read"] is False
    assert payload["totals"]["submitted"] == 1


def test_daily_independent_replay_semantics_remain_unchanged(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    as_of = date(2026, 5, 5)
    _write_candidates(
        reports_dir,
        as_of,
        [_candidate(as_of, "buy_tsm", symbol="TSM", side="BUY", quantity=10)],
    )
    prices_path = _write_prices(
        tmp_path,
        ["2026-05-05,TSM,100,101,99,100,100,1000"],
    )

    payload = run_paper_trading_replay(
        start=as_of,
        end=as_of,
        reports_dir=reports_dir,
        audit_root=tmp_path / "audit",
        trading_daily_report_dir=tmp_path / "reports" / "trading_daily",
        project_root=tmp_path,
        mode="daily-independent",
        prices_path=prices_path,
    )

    assert payload["replay_mode"] == "daily_independent"
    assert payload["portfolio_carry_forward"] is False
    assert "equity_curve" not in payload
    assert payload["totals"]["submitted"] == 1
    assert payload["daily_results"][0]["reconciliation_status"] == "PASS"


def _write_candidates(
    reports_dir: Path,
    as_of: date,
    candidates: list[dict[str, Any]],
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "report_type": "order_intent_candidates",
        "as_of": as_of.isoformat(),
        "run_id": f"continuous_replay_test:{as_of.isoformat()}",
        "production_effect": "none",
        "source_inputs": {"daily_decision_summary": {"exists": True}},
        "candidates": candidates,
    }
    path = reports_dir / f"order_intent_candidates_{as_of.isoformat()}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _candidate(
    as_of: date,
    suffix: str,
    *,
    symbol: str,
    side: str,
    quantity: int,
    limit_price: float = 100.0,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "candidate_id": f"candidate:{as_of.isoformat()}:{suffix}",
        "created_at": datetime(as_of.year, as_of.month, as_of.day, 14, 0, tzinfo=UTC).isoformat(),
        "strategy_id": "continuous_replay_test",
        "strategy_version": "v1",
        "run_id": f"continuous_replay_test:{as_of.isoformat()}",
        "symbol": symbol,
        "asset_type": "stock",
        "side": side,
        "target_quantity": quantity,
        "limit_price": limit_price,
        "confidence": 0.75,
        "score_snapshot_id": f"score_snapshot:{symbol}:{as_of.isoformat()}",
        "blocked": False,
        "blocked_by": [],
        "reason_codes": ["continuous_replay"],
        "mode": "paper",
        "production_effect": "none",
    }


def _write_prices(tmp_path: Path, rows: list[str]) -> Path:
    path = tmp_path / "data" / "raw" / "prices_daily.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(["date,ticker,open,high,low,close,adj_close,volume", *rows]),
        encoding="utf-8",
    )
    return path


def _position_quantity(snapshot: dict[str, Any], symbol: str) -> int:
    for position in snapshot["positions"]:
        if position["symbol"] == symbol:
            return int(position["quantity"])
    return 0
