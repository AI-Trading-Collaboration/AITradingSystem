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

    refresh_kwargs: list[dict[str, object]] = []

    def fake_refresh(**kwargs) -> RefreshRun:
        calls.append("refresh")
        refresh_kwargs.append(kwargs)
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
    assert refresh_kwargs[0]["allow_price_writes"] is True
    assert "Freshness recovery：OK" in result.output
    assert "tracking_status=active_track" in result.output
    assert "production_effect=none" in result.output


def test_recover_freshness_cli_no_price_writes_reaches_refresh(monkeypatch) -> None:
    # OPS-080: the daily run calls recover-freshness before DQ with --no-price-writes.
    @dataclass(frozen=True)
    class FreshnessRun:
        as_of: date
        payload: dict[str, object]
        json_path: str = "freshness.json"

    @dataclass(frozen=True)
    class RefreshRun:
        payload: dict[str, object]
        json_path: str = "refresh.json"

    refresh_kwargs: list[dict[str, object]] = []

    def fake_refresh(**kwargs) -> RefreshRun:
        refresh_kwargs.append(kwargs)
        return RefreshRun(payload={"metadata": {"status": "SOURCE_DELAYED"}, "after": {}})

    monkeypatch.setattr(
        data_cli,
        "run_market_data_freshness",
        lambda **_: FreshnessRun(as_of=date(2026, 1, 6), payload={"freshness": {}}),
    )
    monkeypatch.setattr(data_cli, "run_market_data_refresh", fake_refresh)

    result = CliRunner().invoke(
        cli.app,
        ["data", "recover-freshness", "--latest", "--no-price-writes"],
    )

    assert result.exit_code == 0
    assert refresh_kwargs[0]["allow_price_writes"] is False
