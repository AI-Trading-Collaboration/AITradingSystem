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
    assert captured["risk_event_openai_precheck_max_candidates"] is None
    assert captured["llm_request_profile"] == "risk_event_daily_official_precheck"
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
    assert captured["llm_request_profile"] == "risk_event_daily_official_precheck"


def test_cli_direct_score_daily_threads_llm_request_profile(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-05-11",
            "--llm-request-profile",
            "risk_event_triaged_official_candidates",
        ]
    )

    assert exit_code == 0
    assert captured["llm_request_profile"] == "risk_event_triaged_official_candidates"


def test_cli_direct_dispatches_daily_feedback_reports(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_market_feedback(**kwargs: object) -> None:
        calls.append(("market_feedback", kwargs))

    def fake_loop_review(**kwargs: object) -> None:
        calls.append(("loop_review", kwargs))

    def fake_investment_review(**kwargs: object) -> None:
        calls.append(("investment_review", kwargs))

    monkeypatch.setattr(
        cli_direct.cli,
        "optimize_market_feedback_command",
        fake_market_feedback,
    )
    monkeypatch.setattr(
        cli_direct.cli,
        "feedback_loop_review_command",
        fake_loop_review,
    )
    monkeypatch.setattr(
        cli_direct.cli,
        "investment_periodic_review_command",
        fake_investment_review,
    )

    assert cli_direct.main(["feedback", "optimize-market-feedback", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["feedback", "loop-review", "--as-of", "2026-05-13"]) == 0
    assert (
        cli_direct.main(
            [
                "reports",
                "investment-review",
                "--period",
                "weekly",
                "--as-of",
                "2026-05-13",
            ]
        )
        == 0
    )

    assert calls == [
        ("market_feedback", {"as_of": "2026-05-13"}),
        ("loop_review", {"as_of": "2026-05-13"}),
        ("investment_review", {"period": "weekly", "as_of": "2026-05-13"}),
    ]
