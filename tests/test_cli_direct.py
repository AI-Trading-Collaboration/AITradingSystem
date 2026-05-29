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

    def fake_parameter_governance(**kwargs: object) -> None:
        calls.append(("parameter_governance", kwargs))

    def fake_market_feedback(**kwargs: object) -> None:
        calls.append(("market_feedback", kwargs))

    def fake_loop_review(**kwargs: object) -> None:
        calls.append(("loop_review", kwargs))

    def fake_investment_review(**kwargs: object) -> None:
        calls.append(("investment_review", kwargs))

    monkeypatch.setattr(
        cli_direct.cli,
        "evaluate_parameter_governance_command",
        fake_parameter_governance,
    )
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

    assert (
        cli_direct.main(["feedback", "evaluate-parameter-governance", "--as-of", "2026-05-13"]) == 0
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
        ("parameter_governance", {"as_of": "2026-05-13"}),
        ("market_feedback", {"as_of": "2026-05-13"}),
        ("loop_review", {"as_of": "2026-05-13"}),
        ("investment_review", {"period": "weekly", "as_of": "2026-05-13"}),
    ]


def test_cli_direct_dispatches_report_index(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_report_index(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_direct.cli, "report_index_command", fake_report_index)

    assert cli_direct.main(["reports", "index", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "index", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_dispatches_reader_brief_date_and_latest(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_reader_brief(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_direct.cli, "reader_brief_command", fake_reader_brief)

    assert cli_direct.main(["reports", "reader-brief", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "reader-brief", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_dispatches_scheduled_task_commands(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_validate_data(**kwargs: object) -> None:
        calls.append(("validate_data", kwargs))

    def fake_build_manifest(**kwargs: object) -> None:
        calls.append(("build_manifest", kwargs))

    def fake_validate_pit(**kwargs: object) -> None:
        calls.append(("validate_pit", kwargs))

    def fake_docs_contract(**kwargs: object) -> None:
        calls.append(("docs_contract", kwargs))

    def fake_shadow_observe(**kwargs: object) -> None:
        calls.append(("shadow_observe", kwargs))

    def fake_shadow_monitor(**kwargs: object) -> None:
        calls.append(("shadow_monitor", kwargs))

    monkeypatch.setattr(cli_direct.cli, "validate_data", fake_validate_data)
    monkeypatch.setattr(cli_direct.cli, "build_pit_snapshot_manifest_command", fake_build_manifest)
    monkeypatch.setattr(cli_direct.cli, "validate_pit_snapshots_command", fake_validate_pit)
    monkeypatch.setattr(
        cli_direct.docs_cli,
        "documentation_contract_command",
        fake_docs_contract,
    )
    monkeypatch.setattr(cli_direct.sec_pit_cli, "shadow_observe_command", fake_shadow_observe)
    monkeypatch.setattr(cli_direct.sec_pit_cli, "shadow_monitor_command", fake_shadow_monitor)

    assert cli_direct.main(["validate-data", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["pit-snapshots", "build-manifest", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["pit-snapshots", "validate", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["docs", "report-contract", "--latest"]) == 0
    assert cli_direct.main(["sec-pit", "shadow-observe", "--latest"]) == 0
    assert cli_direct.main(["sec-pit", "shadow-monitor", "--latest"]) == 0

    assert calls == [
        ("validate_data", {"as_of": "2026-05-13", "full_universe": False}),
        ("build_manifest", {"as_of": "2026-05-13"}),
        ("validate_pit", {"as_of": "2026-05-13"}),
        ("docs_contract", {"as_of": None, "latest": True}),
        ("shadow_observe", {"latest": True}),
        ("shadow_monitor", {"latest": True}),
    ]


def test_cli_direct_dispatches_validate_reader_brief(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_validate_reader_brief(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_direct.cli, "validate_reader_brief_command", fake_validate_reader_brief)

    assert cli_direct.main(["reports", "validate-reader-brief", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "validate-reader-brief", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_dispatches_score_change_and_market_panel_latest(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_score_change(**kwargs: object) -> None:
        calls.append(("score_change", kwargs))

    def fake_market_panel(**kwargs: object) -> None:
        calls.append(("market_panel", kwargs))

    monkeypatch.setattr(cli_direct.cli, "score_change_attribution_command", fake_score_change)
    monkeypatch.setattr(cli_direct.cli, "market_panel_command", fake_market_panel)

    assert cli_direct.main(["reports", "score-change-attribution", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "score-change-attribution", "--latest"]) == 0
    assert cli_direct.main(["reports", "market-panel", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "market-panel", "--latest"]) == 0

    assert calls == [
        ("score_change", {"as_of": "2026-05-13", "latest": False}),
        ("score_change", {"as_of": None, "latest": True}),
        ("market_panel", {"as_of": "2026-05-13", "latest": False}),
        ("market_panel", {"as_of": None, "latest": True}),
    ]


def test_cli_direct_dispatches_research_governance_date_and_latest(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_research_governance(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(
        cli_direct.cli,
        "research_governance_summary_command",
        fake_research_governance,
    )

    assert cli_direct.main(["reports", "research-governance-summary", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "research-governance-summary", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]
