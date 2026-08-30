from __future__ import annotations

import json
import shlex
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import typer

from ai_trading_system import cli_direct
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config


def test_cli_direct_score_daily_maps_skip_openai_precheck(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)

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

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-05-11",
            "--risk-event-openai-precheck-max-candidates",
            "7",
            "--risk-event-openai-precheck-visibility-cutoff",
            "2026-05-12T04:10:00+00:00",
        ]
    )

    assert exit_code == 0
    assert captured["risk_event_openai_precheck"] is True
    assert captured["risk_event_openai_precheck_max_candidates"] == 7
    assert captured["risk_event_openai_precheck_visibility_cutoff"] == "2026-05-12T04:10:00+00:00"
    assert captured["llm_request_profile"] == "risk_event_daily_official_precheck"


def test_cli_direct_score_daily_threads_llm_request_profile(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)

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


def test_cli_direct_score_daily_threads_date_scoped_valuation_path(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    valuation_path = tmp_path / "valuation"

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-07-27",
            "--valuation-path",
            str(valuation_path),
        ]
    )

    assert exit_code == 0
    assert captured["valuation_path"] == valuation_path


def test_cli_direct_score_daily_threads_official_policy_capture_manifest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    manifest_path = tmp_path / "daily_input_capture_manifest_2026-08-04.json"

    def fake_score_daily(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-08-04",
            "--official-policy-capture-manifest-path",
            str(manifest_path),
        ]
    )

    assert exit_code == 0
    assert captured["official_policy_capture_manifest_path"] == manifest_path


def test_cli_direct_dispatches_capture_and_date_scoped_source_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: dict[str, dict[str, object]] = {}

    def recorder(name: str):
        def _fake(**kwargs: object) -> None:
            calls[name] = kwargs

        return _fake

    monkeypatch.setattr(
        cli_direct.ops_cli,
        "capture_daily_inputs_command",
        recorder("capture"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "fetch_fmp_forward_pit_command",
        recorder("pit_fetch"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "build_pit_snapshot_manifest_command",
        recorder("pit_build"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "validate_pit_snapshots_command",
        recorder("pit_validate"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "download_sec_companyfacts_command",
        recorder("sec_download"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "extract_sec_metrics_command",
        recorder("sec_extract"),
    )
    monkeypatch.setattr(
        cli_direct.valuation_cli,
        "fetch_fmp_valuations",
        recorder("valuation"),
    )
    monkeypatch.setattr(
        cli_direct.risk_events_cli,
        "fetch_official_policy_sources_command",
        recorder("official"),
    )

    policy_path = tmp_path / "capture.yaml"
    pit_raw = tmp_path / "raw" / "pit"
    pit_normalized = tmp_path / "processed" / "pit.csv"
    pit_manifest = tmp_path / "raw" / "pit_manifest.csv"
    pit_fetch_report = tmp_path / "reports" / "pit_fetch.md"
    pit_validation_report = tmp_path / "reports" / "pit_validation.md"
    sec_raw = tmp_path / "raw" / "sec"
    valuation_dir = tmp_path / "external" / "valuation"
    valuation_history_dir = tmp_path / "external" / "history"
    analyst_dir = tmp_path / "raw" / "analyst"
    valuation_fetch_report = tmp_path / "reports" / "valuation_fetch.md"
    valuation_validation_report = tmp_path / "reports" / "valuation_validation.md"
    official_raw = tmp_path / "raw" / "official"
    official_processed = tmp_path / "processed" / "official"
    official_manifest = tmp_path / "raw" / "official_manifest.csv"
    official_report = tmp_path / "reports" / "official.md"

    assert (
        cli_direct.main(
            [
                "ops",
                "capture-daily-inputs",
                "--as-of",
                "2026-07-27",
                "--policy-path",
                str(policy_path),
                "--download-start",
                "2021-02-22",
                "--full-universe",
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "pit-snapshots",
                "fetch-fmp-forward",
                "--as-of",
                "2026-07-27",
                "--raw-output-dir",
                str(pit_raw),
                "--normalized-output-path",
                str(pit_normalized),
                "--manifest-path",
                str(pit_manifest),
                "--output-path",
                str(pit_fetch_report),
                "--pit-validation-report-path",
                str(pit_validation_report),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "pit-snapshots",
                "build-manifest",
                "--as-of",
                "2026-07-27",
                "--output-path",
                str(pit_manifest),
                "--fmp-analyst-history-dir",
                str(analyst_dir),
                "--fmp-forward-pit-dir",
                str(pit_raw),
                "--validation-report-path",
                str(pit_validation_report),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "pit-snapshots",
                "validate",
                "--as-of",
                "2026-07-27",
                "--input-path",
                str(pit_manifest),
                "--output-path",
                str(pit_validation_report),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "fundamentals",
                "download-sec-companyfacts",
                "--output-dir",
                str(sec_raw),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "fundamentals",
                "extract-sec-metrics",
                "--as-of",
                "2026-07-27",
                "--input-dir",
                str(sec_raw),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "valuation",
                "fetch-fmp",
                "--as-of",
                "2026-07-27",
                "--output-dir",
                str(valuation_dir),
                "--analyst-history-dir",
                str(analyst_dir),
                "--pit-normalized-path",
                str(pit_normalized),
                "--valuation-history-dir",
                str(valuation_history_dir),
                "--output-path",
                str(valuation_fetch_report),
                "--validation-report-path",
                str(valuation_validation_report),
            ]
        )
        == 0
    )
    assert (
        cli_direct.main(
            [
                "risk-events",
                "fetch-official-sources",
                "--as-of",
                "2026-07-27",
                "--raw-dir",
                str(official_raw),
                "--processed-dir",
                str(official_processed),
                "--download-manifest-path",
                str(official_manifest),
                "--output-path",
                str(official_report),
            ]
        )
        == 0
    )

    assert calls["capture"] == {
        "as_of": "2026-07-27",
        "policy_path": policy_path,
        "download_start": "2021-02-22",
        "full_universe": True,
    }
    assert calls["pit_fetch"]["raw_output_dir"] == pit_raw
    assert calls["pit_fetch"]["normalized_output_path"] == pit_normalized
    assert calls["pit_fetch"]["manifest_path"] == pit_manifest
    assert calls["pit_fetch"]["output_path"] == pit_fetch_report
    assert calls["pit_fetch"]["pit_validation_report_path"] == pit_validation_report
    assert calls["pit_build"]["output_path"] == pit_manifest
    assert calls["pit_build"]["fmp_analyst_history_dir"] == analyst_dir
    assert calls["pit_build"]["fmp_forward_pit_dir"] == pit_raw
    assert calls["pit_build"]["validation_report_path"] == pit_validation_report
    assert calls["pit_validate"]["input_path"] == pit_manifest
    assert calls["pit_validate"]["output_path"] == pit_validation_report
    assert calls["sec_download"]["output_dir"] == sec_raw
    assert calls["sec_extract"]["input_dir"] == sec_raw
    assert calls["valuation"]["output_dir"] == valuation_dir
    assert calls["valuation"]["analyst_history_dir"] == analyst_dir
    assert calls["valuation"]["pit_normalized_path"] == pit_normalized
    assert calls["valuation"]["valuation_history_dir"] == valuation_history_dir
    assert calls["valuation"]["output_path"] == valuation_fetch_report
    assert calls["valuation"]["validation_report_path"] == valuation_validation_report
    assert calls["official"]["raw_dir"] == official_raw
    assert calls["official"]["processed_dir"] == official_processed
    assert calls["official"]["download_manifest_path"] == official_manifest
    assert calls["official"]["output_path"] == official_report


def test_cli_direct_routes_daily_score_authorization_profile_before_score(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}
    score_calls: list[dict[str, object]] = []

    def fake_score_daily(**kwargs: object) -> None:
        score_calls.append(kwargs)

    def fake_authorized_dispatch(**kwargs: object) -> None:
        captured.update(kwargs)
        kwargs["score_runner"]()

    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", fake_score_daily)
    monkeypatch.setattr(
        cli_direct,
        "_dispatch_daily_score_with_consumer_authorization",
        fake_authorized_dispatch,
    )

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-07-23",
            "--consumer-authorization-profile",
            "daily_score_daily@1.0.0",
            "--skip-risk-event-openai-precheck",
            "--run-id",
            "daily-wave15",
        ]
    )

    assert exit_code == 0
    assert captured["as_of"] == date(2026, 7, 23)
    assert captured["run_id"] == "daily-wave15"
    assert captured["project_root"] == Path.cwd()
    assert len(score_calls) == 1
    assert score_calls[0]["as_of"] == "2026-07-23"


def test_cli_direct_rejects_unknown_consumer_profile_before_score(monkeypatch) -> None:
    score_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli_direct.score_daily_cli,
        "score_daily",
        lambda **kwargs: score_calls.append(kwargs),
    )

    exit_code = cli_direct.main(
        [
            "score-daily",
            "--as-of",
            "2026-07-23",
            "--consumer-authorization-profile",
            "weekly_backtest@1.0.0",
        ]
    )

    assert exit_code == 1
    assert score_calls == []


def test_cli_direct_dispatches_signal_commands(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def recorder(name: str):
        def _fake(**kwargs: object) -> None:
            calls.append((name, kwargs))

        return _fake

    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_build_snapshot_command",
        recorder("build_snapshot"),
    )
    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_validate_snapshot_command",
        recorder("validate_snapshot"),
    )
    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_ablation_command",
        recorder("ablation"),
    )
    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_calibrate_command",
        recorder("calibrate"),
    )
    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_explain_ablation_command",
        recorder("explain_ablation"),
    )
    monkeypatch.setattr(
        cli_direct.signals_cli,
        "signals_validate_ablation_command",
        recorder("validate_ablation"),
    )

    assert cli_direct.main(["signals", "build-snapshot", "--latest"]) == 0
    assert cli_direct.main(["signals", "validate-snapshot", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["signals", "ablation", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["signals", "calibrate", "--latest"]) == 0
    assert cli_direct.main(["signals", "explain-ablation", "--latest"]) == 0
    assert cli_direct.main(["signals", "validate-ablation", "--latest"]) == 0

    assert calls == [
        (
            "build_snapshot",
            {
                "latest": True,
                "as_of": None,
                "config_path": cli_direct.signals_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
                "dry_run": False,
                "price_derived_only": False,
            },
        ),
        (
            "validate_snapshot",
            {"latest": False, "as_of": "2026-05-13", "input_path": None},
        ),
        (
            "ablation",
            {
                "ctx": SimpleNamespace(args=[]),
                "latest": False,
                "as_of": "2026-05-13",
                "config_path": cli_direct.signals_cli.DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
                "signals": [],
                "dry_run": False,
                "debug": False,
            },
        ),
        (
            "calibrate",
            {
                "ctx": SimpleNamespace(args=[]),
                "latest": True,
                "as_of": None,
                "config_path": cli_direct.signals_cli.DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
                "profile": None,
                "profiles": [],
                "dry_run": False,
            },
        ),
        ("explain_ablation", {"latest": True, "as_of": None, "input_path": None}),
        ("validate_ablation", {"latest": True, "as_of": None, "input_path": None}),
    ]


def test_cli_direct_dispatches_etf_ops_dry_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_ops_dry_run_command(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(
        cli_direct.etf_operations_cli,
        "ops_dry_run_command",
        fake_ops_dry_run_command,
    )

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "dry-run",
            "--cadence",
            "weekly",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--output-path",
            str(tmp_path / "dry_run.json"),
            "--skip-optional",
            "--no-write",
        ]
    )

    assert exit_code == 0
    assert captured["cadence"] == "weekly"
    assert captured["as_of"] == "2026-06-03"
    assert captured["root_path"] == tmp_path
    assert captured["output_path"] == tmp_path / "dry_run.json"
    assert captured["include_optional"] is False
    assert captured["no_write"] is True


def test_cli_direct_etf_ops_dry_run_writes_non_executing_json(tmp_path: Path) -> None:
    output_path = tmp_path / "operations_dry_run.json"

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "dry-run",
            "--cadence",
            "daily",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--output-path",
            str(output_path),
        ]
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["schema_version"] == "etf_operations_scheduler_dry_run_v1"
    assert payload["cadence"] == "daily"
    assert payload["dry_run_only"] is True
    assert payload["commands_executed"] is False
    assert payload["production_state_mutated"] is False
    assert payload["planned_steps"]


def test_cli_direct_dispatches_etf_ops_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_ops_report_command(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(
        cli_direct.etf_operations_cli,
        "ops_report_command",
        fake_ops_report_command,
    )

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "report",
            "--cadence",
            "weekly",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--output-dir",
            str(tmp_path / "reports"),
            "--json-path",
            str(tmp_path / "operations_health.json"),
            "--markdown-path",
            str(tmp_path / "operations_health.md"),
            "--skip-optional",
        ]
    )

    assert exit_code == 0
    assert captured["cadence"] == "weekly"
    assert captured["as_of"] == "2026-06-03"
    assert captured["root_path"] == tmp_path
    assert captured["output_dir"] == tmp_path / "reports"
    assert captured["json_path"] == tmp_path / "operations_health.json"
    assert captured["markdown_path"] == tmp_path / "operations_health.md"
    assert captured["include_optional"] is False


def test_cli_direct_etf_ops_report_writes_json_and_markdown(tmp_path: Path) -> None:
    json_path = tmp_path / "operations_health.json"
    markdown_path = tmp_path / "operations_health.md"

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "report",
            "--cadence",
            "daily",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--json-path",
            str(json_path),
            "--markdown-path",
            str(markdown_path),
        ]
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert exit_code == 0
    assert payload["schema_version"] == "etf_operations_health_report_v1"
    assert payload["cadence"] == "daily"
    assert payload["commands_executed"] is False
    assert payload["production_state_mutated"] is False
    assert "## Safety Banner / 安全边界" in markdown
    assert "## Source Artifacts / Source Artifacts" in markdown


def test_cli_direct_dispatches_etf_ops_validate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_ops_validate_command(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(
        cli_direct.etf_operations_cli,
        "ops_validate_command",
        fake_ops_validate_command,
    )

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "validate",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--config-path",
            str(tmp_path / "operations_schedule.yaml"),
            "--output-dir",
            str(tmp_path / "validation"),
            "--json-path",
            str(tmp_path / "operations_validation.json"),
            "--markdown-path",
            str(tmp_path / "operations_validation.md"),
        ]
    )

    assert exit_code == 0
    assert captured["as_of"] == "2026-06-03"
    assert captured["root_path"] == tmp_path
    assert captured["config_path"] == tmp_path / "operations_schedule.yaml"
    assert captured["output_dir"] == tmp_path / "validation"
    assert captured["json_path"] == tmp_path / "operations_validation.json"
    assert captured["markdown_path"] == tmp_path / "operations_validation.md"


def test_cli_direct_etf_ops_validate_writes_json_and_markdown(tmp_path: Path) -> None:
    json_path = tmp_path / "operations_validation.json"
    markdown_path = tmp_path / "operations_validation.md"

    exit_code = cli_direct.main(
        [
            "etf",
            "ops",
            "validate",
            "--as-of",
            "2026-06-03",
            "--root-path",
            str(tmp_path),
            "--json-path",
            str(json_path),
            "--markdown-path",
            str(markdown_path),
        ]
    )

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert exit_code == 0
    assert payload["schema_version"] == "etf_operations_validation_v1"
    assert payload["status"] == "PASS"
    assert payload["commands_executed"] is False
    assert payload["production_state_mutated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True
    assert "## Checks / 校验项" in markdown


def test_cli_direct_dispatches_dynamic_v3_schedule_observe(
    monkeypatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    def fake_schedule_observe_command(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(
        cli_direct.etf_observation_lifecycle_cli,
        "dynamic_v3_schedule_observe_command",
        fake_schedule_observe_command,
    )

    exit_code = cli_direct.main(
        [
            "etf",
            "dynamic-v3-rescue",
            "schedule",
            "observe",
            "--as-of",
            "2026-05-08",
            "--pointer-dir",
            str(tmp_path / "latest"),
            "--registry-path",
            str(tmp_path / "registry.yaml"),
            "--output-dir",
            str(tmp_path / "schedule"),
            "--skip-shadow-monitor",
            "--force-due",
        ]
    )

    assert exit_code == 0
    assert captured["as_of"] == "2026-05-08"
    assert captured["pointer_dir"] == tmp_path / "latest"
    assert captured["registry_path"] == tmp_path / "registry.yaml"
    assert captured["output_dir"] == tmp_path / "schedule"
    assert captured["run_shadow_monitor"] is False
    assert captured["force_due"] is True


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
        cli_direct.feedback_cli,
        "evaluate_parameter_governance_command",
        fake_parameter_governance,
    )
    monkeypatch.setattr(
        cli_direct.feedback_cli,
        "optimize_market_feedback_command",
        fake_market_feedback,
    )
    monkeypatch.setattr(
        cli_direct.feedback_cli,
        "feedback_loop_review_command",
        fake_loop_review,
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
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

    monkeypatch.setattr(cli_direct.reports_cli, "report_index_command", fake_report_index)

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

    monkeypatch.setattr(cli_direct.reports_cli, "reader_brief_command", fake_reader_brief)

    assert cli_direct.main(["reports", "reader-brief", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "reader-brief", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_dispatches_scheduled_task_commands(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_validate_data(**kwargs: object) -> None:
        calls.append(("validate_data", kwargs))

    def fake_build_manifest(**kwargs: object) -> None:
        calls.append(("build_manifest", kwargs))

    def fake_validate_pit(**kwargs: object) -> None:
        calls.append(("validate_pit", kwargs))

    def fake_docs_contract(**kwargs: object) -> None:
        calls.append(("docs_contract", kwargs))

    def fake_heuristic_audit(**kwargs: object) -> None:
        calls.append(("heuristic_audit", kwargs))

    def fake_shadow_observe(**kwargs: object) -> None:
        calls.append(("shadow_observe", kwargs))

    def fake_shadow_monitor(**kwargs: object) -> None:
        calls.append(("shadow_monitor", kwargs))

    monkeypatch.setattr(cli_direct.data_cache_cli, "validate_data", fake_validate_data)
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "build_pit_snapshot_manifest_command",
        fake_build_manifest,
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "validate_pit_snapshots_command",
        fake_validate_pit,
    )
    monkeypatch.setattr(
        cli_direct.docs_cli,
        "documentation_contract_command",
        fake_docs_contract,
    )
    monkeypatch.setattr(
        cli_direct.docs_cli,
        "heuristic_governance_audit_command",
        fake_heuristic_audit,
    )
    monkeypatch.setattr(cli_direct.sec_pit_cli, "shadow_observe_command", fake_shadow_observe)
    monkeypatch.setattr(cli_direct.sec_pit_cli, "shadow_monitor_command", fake_shadow_monitor)

    assert (
        cli_direct.main(
            [
                "validate-data",
                "--as-of",
                "2026-05-13",
                "--execution-profile",
                "daily_default.v1",
            ]
        )
        == 0
    )
    assert cli_direct.main(["pit-snapshots", "build-manifest", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["pit-snapshots", "validate", "--as-of", "2026-05-13"]) == 0
    assert cli_direct.main(["docs", "report-contract", "--latest"]) == 0
    assert (
        cli_direct.main(
            [
                "docs",
                "heuristic-audit",
                "--date",
                "2026-06-07",
                "--config-path",
                str(tmp_path / "heuristic.yaml"),
                "--output-path",
                str(tmp_path / "audit.md"),
                "--json-output-path",
                str(tmp_path / "audit.json"),
                "--fail-on-warning",
            ]
        )
        == 0
    )
    assert cli_direct.main(["sec-pit", "shadow-observe", "--latest", "--end", "2026-05-13"]) == 0
    assert cli_direct.main(["sec-pit", "shadow-monitor", "--latest", "--as-of", "2026-05-13"]) == 0

    assert calls == [
        (
            "validate_data",
            {
                "as_of": "2026-05-13",
                "execution_profile": "daily_default.v1",
                "full_universe": False,
            },
        ),
        (
            "build_manifest",
            {
                "as_of": "2026-05-13",
                "output_path": cli_direct.pit_snapshots_cli.DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
                "fmp_analyst_history_dir": (
                    cli_direct.pit_snapshots_cli.DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR
                ),
                "fmp_forward_pit_dir": (
                    cli_direct.pit_snapshots_cli.DEFAULT_FMP_FORWARD_PIT_RAW_DIR
                ),
                "validation_report_path": None,
                "required_snapshot_kinds": None,
            },
        ),
        (
            "validate_pit",
            {
                "as_of": "2026-05-13",
                "input_path": cli_direct.pit_snapshots_cli.DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
                "output_path": None,
                "required_snapshot_kinds": None,
            },
        ),
        ("docs_contract", {"as_of": None, "latest": True}),
        (
            "heuristic_audit",
            {
                "as_of": "2026-06-07",
                "config_path": tmp_path / "heuristic.yaml",
                "output_path": tmp_path / "audit.md",
                "json_output_path": tmp_path / "audit.json",
                "fail_on_warning": True,
            },
        ),
        ("shadow_observe", {"start": None, "end": "2026-05-13", "latest": True}),
        ("shadow_monitor", {"as_of": "2026-05-13", "latest": True}),
    ]


def test_cli_direct_validate_data_preserves_profile_default_and_override(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_validate_data(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_direct.data_cache_cli, "validate_data", fake_validate_data)

    assert cli_direct.main(["validate-data", "--as-of", "2026-05-13"]) == 0
    assert (
        cli_direct.main(
            [
                "validate-data",
                "--as-of",
                "2026-05-13",
                "--execution-profile",
                "manual.v1",
            ]
        )
        == 0
    )

    assert calls == [
        {
            "as_of": "2026-05-13",
            "execution_profile": cli_direct.data_cache_cli.AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID,
            "full_universe": False,
        },
        {
            "as_of": "2026-05-13",
            "execution_profile": "manual.v1",
            "full_universe": False,
        },
    ]


def test_cli_direct_dispatches_data_commands(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def recorder(name: str):
        def _fake(**kwargs: object) -> None:
            calls.append((name, kwargs))

        return _fake

    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_diagnose_backtest_inputs_command",
        recorder("diagnose"),
    )
    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_repair_backtest_inputs_command",
        recorder("repair"),
    )
    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_freshness_command",
        recorder("freshness"),
    )
    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_recover_freshness_command",
        recorder("recover"),
    )

    assert cli_direct.main(["data", "diagnose-backtest-inputs", "--latest"]) == 0
    assert (
        cli_direct.main(
            [
                "data",
                "repair-backtest-inputs",
                "--date",
                "2026-05-13",
                "--dry-run",
                "--price-only",
                "--symbols",
                "GOOGL",
                "BRK.B",
            ]
        )
        == 0
    )
    assert cli_direct.main(["data", "freshness", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["data", "recover-freshness", "--latest"]) == 0

    assert calls == [
        (
            "diagnose",
            {
                "latest": True,
                "as_of": None,
                "config_path": cli_direct.data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
            },
        ),
        (
            "repair",
            {
                "ctx": SimpleNamespace(args=[]),
                "latest": False,
                "as_of": "2026-05-13",
                "config_path": cli_direct.data_cli.DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
                "dry_run": True,
                "price_only": True,
                "symbols": ["GOOGL", "BRK.B"],
                "price_provider": "fmp",
                "fmp_api_key_env": "FMP_API_KEY",
            },
        ),
        (
            "freshness",
            {
                "latest": False,
                "as_of": "2026-05-13",
                "market": "US",
                "config_path": cli_direct.data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
                "dry_run": False,
            },
        ),
        (
            "recover",
            {
                "latest": True,
                "as_of": None,
                "refresh_config_path": cli_direct.data_cli.DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
                "freshness_config_path": (
                    cli_direct.data_cli.DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH
                ),
            },
        ),
    ]


def test_cli_direct_dispatches_validate_reader_brief(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_validate_reader_brief(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(
        cli_direct.reports_cli, "validate_reader_brief_command", fake_validate_reader_brief
    )

    assert cli_direct.main(["reports", "validate-reader-brief", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "validate-reader-brief", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_propagates_reader_brief_quality_failure_exit(monkeypatch) -> None:
    def fake_validate_reader_brief(**kwargs: object) -> None:
        raise typer.Exit(code=1)

    monkeypatch.setattr(
        cli_direct.reports_cli, "validate_reader_brief_command", fake_validate_reader_brief
    )

    assert cli_direct.main(["reports", "validate-reader-brief", "--date", "2026-05-13"]) == 1


def test_cli_direct_dispatches_report_quality_gate(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_report_quality_gate(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(
        cli_direct.reports_cli, "report_quality_gate_command", fake_report_quality_gate
    )

    assert cli_direct.main(["reports", "quality-gate", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "quality-gate", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_propagates_report_quality_failure_exit(monkeypatch) -> None:
    def fake_report_quality_gate(**kwargs: object) -> None:
        raise typer.Exit(code=1)

    monkeypatch.setattr(
        cli_direct.reports_cli, "report_quality_gate_command", fake_report_quality_gate
    )

    assert cli_direct.main(["reports", "quality-gate", "--date", "2026-05-13"]) == 1


def test_cli_direct_dispatches_artifact_lineage(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_artifact_lineage(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(cli_direct.reports_cli, "artifact_lineage_command", fake_artifact_lineage)

    assert cli_direct.main(["reports", "artifact-lineage", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "artifact-lineage", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_dispatches_validate_artifact_lineage(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_validate_artifact_lineage(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(
        cli_direct.reports_cli,
        "validate_artifact_lineage_command",
        fake_validate_artifact_lineage,
    )

    assert cli_direct.main(["reports", "validate-artifact-lineage", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "validate-artifact-lineage", "--latest"]) == 0

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

    monkeypatch.setattr(
        cli_direct.reports_cli, "score_change_attribution_command", fake_score_change
    )
    monkeypatch.setattr(cli_direct.reports_cli, "market_panel_command", fake_market_panel)

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
        cli_direct.reports_cli,
        "research_governance_summary_command",
        fake_research_governance,
    )

    assert cli_direct.main(["reports", "research-governance-summary", "--date", "2026-05-13"]) == 0
    assert cli_direct.main(["reports", "research-governance-summary", "--latest"]) == 0

    assert calls == [
        {"as_of": "2026-05-13", "latest": False},
        {"as_of": None, "latest": True},
    ]


def test_cli_direct_covers_all_scheduled_daily_commands(monkeypatch) -> None:
    calls: list[str] = []

    def recorder(name: str):
        def _fake(**kwargs: object) -> None:
            calls.append(name)

        return _fake

    monkeypatch.setattr(cli_direct.data_cache_cli, "download_data", recorder("download_data"))
    monkeypatch.setattr(cli_direct.data_cache_cli, "validate_data", recorder("validate_data"))
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "fetch_fmp_forward_pit_command",
        recorder("pit_snapshots_fetch_fmp_forward"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "project_fmp_forward_pit_capture_command",
        recorder("pit_snapshots_project_fmp_forward"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "build_pit_snapshot_manifest_command",
        recorder("pit_snapshots_build_manifest"),
    )
    monkeypatch.setattr(
        cli_direct.pit_snapshots_cli,
        "validate_pit_snapshots_command",
        recorder("pit_snapshots_validate"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "download_sec_companyfacts_command",
        recorder("sec_companyfacts"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "extract_sec_metrics_command",
        recorder("sec_metrics"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "merge_tsm_ir_sec_metrics",
        recorder("tsm_ir_sec_metrics_merge"),
    )
    monkeypatch.setattr(
        cli_direct.fundamentals_cli,
        "validate_sec_metrics_command",
        recorder("sec_metrics_validation"),
    )
    monkeypatch.setattr(
        cli_direct.valuation_cli,
        "fetch_fmp_valuations",
        recorder("valuation_snapshots"),
    )
    monkeypatch.setattr(cli_direct.score_daily_cli, "score_daily", recorder("score_daily"))
    monkeypatch.setattr(
        cli_direct.forward_evidence_cli,
        "forward_evidence_capture_dry_run_daily_command",
        recorder("forward_evidence_dry_run_daily"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "evidence_dashboard_command",
        recorder("reports_dashboard"),
    )
    monkeypatch.setattr(
        cli_direct.sec_pit_cli,
        "shadow_observe_command",
        recorder("sec_pit_shadow_observe"),
    )
    monkeypatch.setattr(
        cli_direct.sec_pit_cli,
        "shadow_monitor_command",
        recorder("sec_pit_shadow_monitor"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "score_change_attribution_command",
        recorder("score_change_attribution"),
    )
    monkeypatch.setattr(cli_direct.reports_cli, "market_panel_command", recorder("market_panel"))
    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_freshness_command",
        recorder("market_data_freshness"),
    )
    monkeypatch.setattr(
        cli_direct.data_cli,
        "data_recover_freshness_command",
        recorder("market_data_recover_freshness"),
    )
    monkeypatch.setattr(
        cli_direct.portfolio_cli,
        "portfolio_track_candidate_command",
        recorder("portfolio_candidate_tracking"),
    )
    monkeypatch.setattr(
        cli_direct.portfolio_cli,
        "portfolio_review_tracking_command",
        recorder("portfolio_tracking_review"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "portfolio_tracking_review_report_command",
        recorder("portfolio_tracking_review_report"),
    )
    monkeypatch.setattr(
        cli_direct.etf_cli,
        "forward_update_command",
        recorder("etf_forward_update"),
    )
    monkeypatch.setattr(
        cli_direct.etf_cli,
        "forward_dashboard_command",
        recorder("etf_forward_dashboard"),
    )
    monkeypatch.setattr(
        cli_direct.etf_cli,
        "forward_watchlist_command",
        recorder("etf_forward_watchlist"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "artifact_lineage_command",
        recorder("artifact_lineage"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "validate_artifact_lineage_command",
        recorder("validate_artifact_lineage"),
    )
    monkeypatch.setattr(cli_direct.reports_cli, "report_index_command", recorder("report_index"))
    monkeypatch.setattr(
        cli_direct.docs_cli,
        "documentation_contract_command",
        recorder("documentation_contract"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "research_governance_summary_command",
        recorder("research_governance_summary"),
    )
    monkeypatch.setattr(cli_direct.reports_cli, "reader_brief_command", recorder("reader_brief"))
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "report_quality_gate_command",
        recorder("report_quality_gate"),
    )
    monkeypatch.setattr(
        cli_direct.reports_cli,
        "validate_reader_brief_command",
        recorder("validate_reader_brief"),
    )
    monkeypatch.setattr(
        cli_direct.etf_observation_lifecycle_cli,
        "dynamic_v3_schedule_observe_command",
        recorder("dynamic_v3_rescue_schedule_observe"),
    )
    monkeypatch.setattr(
        cli_direct.ops_cli,
        "capture_daily_inputs_command",
        recorder("capture_daily_inputs"),
    )
    monkeypatch.setattr(
        cli_direct.ops_cli,
        "pipeline_health_command",
        recorder("pipeline_health"),
    )
    monkeypatch.setattr(
        cli_direct.security_cli,
        "security_scan_secrets_command",
        recorder("secret_hygiene"),
    )

    tasks = load_scheduled_tasks_config().daily_tasks(is_trading_day=True)
    for task in tasks:
        args = _daily_command_args(task.command)
        assert args[0] == "aits"
        assert cli_direct.main(args[1:]) == 0, task.daily_plan_step_id

    assert calls == [task.daily_plan_step_id for task in tasks]


def _daily_command_args(command: str) -> list[str]:
    rendered = (
        command.replace("{download_start}", "2018-01-01")
        .replace("{download_end}", "2026-05-29")
        .replace("{as_of}", "2026-05-29")
    )
    return shlex.split(rendered)
