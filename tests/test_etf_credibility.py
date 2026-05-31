from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.credibility import (
    CREDIBILITY_CHECK_IDS,
    build_credibility_gate,
    write_credibility_gate,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle


def test_etf_credibility_gate_passes_when_all_subchecks_pass() -> None:
    payload = _gate()

    assert payload["task"] == "TRADING-063K"
    assert payload["status"] == "PASS"
    assert set(payload["checks"]) == set(CREDIBILITY_CHECK_IDS)
    assert set(payload["checks"].values()) == {"PASS"}
    assert payload["production_effect"] == "none"
    assert payload["manual_review_required"] is True
    assert payload["broker_action"] == "none"


def test_etf_credibility_gate_fails_if_no_lookahead_fails() -> None:
    payload = _gate(
        no_lookahead_records={
            "trade_records": [
                {
                    "signal_date": "2026-01-02",
                    "execution_date": "2026-01-02",
                }
            ]
        }
    )

    assert payload["status"] == "FAIL"
    assert payload["checks"]["no_lookahead"] == "FAIL"
    assert "execution_date_not_after_signal_date" in (
        payload["check_details"]["no_lookahead"]["blockers"]
    )


def test_etf_credibility_gate_fails_if_p2_live_safety_is_violated() -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    config.p2.live_interface.broker_routing_allowed = True

    payload = _gate(config=config)

    assert payload["status"] == "FAIL"
    assert payload["checks"]["p2_live_safety"] == "FAIL"
    assert "broker_routing_allowed" in payload["check_details"]["p2_live_safety"]["blockers"]


def test_etf_credibility_gate_fails_if_benchmark_suite_is_missing() -> None:
    config = load_etf_config_bundle()
    config.backtest.backtest.benchmarks.pop("B008")

    payload = _gate(config=config)

    assert payload["status"] == "FAIL"
    assert payload["checks"]["benchmark_suite"] == "FAIL"
    assert "B008" in payload["check_details"]["benchmark_suite"]["blockers"]


def test_etf_credibility_gate_fails_if_simulation_schema_is_invalid() -> None:
    payload = _gate(
        simulation_ledger=pd.DataFrame(
            [
                {
                    "schema_version": 2,
                    "decision_date": "2026-01-02",
                    "production_effect": "none",
                }
            ]
        )
    )

    assert payload["status"] == "FAIL"
    assert payload["checks"]["simulation_ledger"] == "FAIL"
    assert "missing_column:record_type" in (
        payload["check_details"]["simulation_ledger"]["blockers"]
    )


def test_etf_credibility_gate_writes_json_and_markdown_outputs(tmp_path: Path) -> None:
    payload = _gate()
    json_path = tmp_path / "credibility_gate.json"
    md_path = tmp_path / "credibility_gate.md"

    write_credibility_gate(payload, json_path=json_path, markdown_path=md_path)

    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "PASS"
    markdown = md_path.read_text(encoding="utf-8")
    assert "# ETF Credibility Validation Gate" in markdown
    assert "no broker action" in markdown


def _gate(**kwargs: object) -> dict[str, object]:
    config = kwargs.pop("config", load_etf_config_bundle())
    return build_credibility_gate(
        config=config,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
        **kwargs,
    )
