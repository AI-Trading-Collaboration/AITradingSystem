from __future__ import annotations

from ai_trading_system import cli_direct


def test_cli_direct_score_daily_maps_skip_openai_precheck(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-05-11",
            "--skip-risk-event-openai-precheck",
            "--run-id",
            "daily_ops_run:2026-05-11:test",
        ]
    )

    assert exit_code == 0
    assert captured["as_of"] == "2026-05-11"
    assert captured["risk_event_openai_precheck"] is False
    assert captured["risk_event_openai_precheck_max_candidates"] == 20
    assert captured["run_id"] == "daily_ops_run:2026-05-11:test"


def test_cli_direct_score_daily_keeps_openai_precheck_enabled(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-05-11",
            "--risk-event-openai-precheck-max-candidates",
            "7",
        ]
    )

    assert exit_code == 0
    assert captured["risk_event_openai_precheck"] is True
    assert captured["risk_event_openai_precheck_max_candidates"] == 7
