from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from typer.testing import CliRunner

from ai_trading_system import cli
from ai_trading_system.cli_commands import data as data_cli


def test_recover_freshness_cli_runs_refresh_chain(monkeypatch) -> None:
    calls: list[str] = []

    @dataclass(frozen=True)
    class FreshnessRun:
        as_of: date
        payload: dict[str, object]
        json_path: str = "freshness.json"

    @dataclass(frozen=True)
    class RefreshRun:
        payload: dict[str, object]
        json_path: str = "refresh.json"

    def fake_freshness(**_kwargs) -> FreshnessRun:
        calls.append("freshness")
        return FreshnessRun(
            as_of=date(2026, 1, 6),
            payload={"freshness": {"status": "STALE"}},
        )

    def fake_refresh(**_kwargs) -> RefreshRun:
        calls.append("refresh")
        return RefreshRun(
            payload={
                "metadata": {"status": "OK"},
                "after": {
                    "freshness_status": "OK",
                    "tracking_readiness": "can_track",
                    "candidate_tracking_status": "active_tracking",
                },
            }
        )

    monkeypatch.setattr(data_cli, "run_market_data_freshness", fake_freshness)
    monkeypatch.setattr(data_cli, "run_market_data_refresh", fake_refresh)

    result = CliRunner().invoke(cli.app, ["data", "recover-freshness", "--latest"])

    assert result.exit_code == 0
    assert calls == ["freshness", "refresh"]
    assert "Freshness recovery：OK" in result.output
    assert "tracking_status=active_track" in result.output
    assert "production_effect=none" in result.output
