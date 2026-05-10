from __future__ import annotations

import csv
import json
import subprocess
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.historical_replay import (
    run_historical_day_replay,
    run_historical_replay_window,
)


def test_replay_day_filters_future_inputs_and_uses_isolated_commands(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    calls: list[tuple[str, ...]] = []
    envs: list[dict[str, str]] = []

    def fake_runner(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        envs.append(dict(kwargs["env"]))  # type: ignore[index]
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="unit_replay",
        runner=fake_runner,
        env={
            "OPENAI_API_KEY": "should_not_leak",
            "FMP_API_KEY": "should_not_leak",
            "MARKETSTACK_API_KEY": "should_not_leak",
        },
    )

    assert replay.status == "PASS"
    assert replay.cutoff_policy == "production_daily_run_metadata"
    assert replay.visible_at.isoformat() == "2026-05-08T12:00:00+00:00"
    assert replay.openai_replay_policy == "disabled"
    assert len(calls) == 4
    assert all(env["OPENAI_API_KEY"] == "" for env in envs)
    assert all(env["FMP_API_KEY"] == "" for env in envs)

    filtered_prices = replay.paths.data_raw_dir / "prices_daily.csv"
    filtered_marketstack = replay.paths.data_raw_dir / "prices_marketstack_daily.csv"
    filtered_rates = replay.paths.data_raw_dir / "rates_daily.csv"
    assert {row["date"] for row in _read_csv(filtered_prices)} == {
        "2026-05-07",
        "2026-05-08",
    }
    assert {row["date"] for row in _read_csv(filtered_marketstack)} == {
        "2026-05-07",
        "2026-05-08",
    }
    assert {row["date"] for row in _read_csv(filtered_rates)} == {
        "2026-05-07",
        "2026-05-08",
    }
    raw_records = {record.artifact_id: record for record in replay.input_records}
    assert raw_records["prices_daily"].status == "PASS_WITH_EXCLUSIONS"
    assert raw_records["prices_daily"].included_count == 2
    assert raw_records["prices_daily"].excluded_count == 1
    assert raw_records["rates_daily"].status == "PASS_WITH_EXCLUSIONS"
    assert raw_records["download_manifest"].status == "PASS"
    replay_manifest = replay.paths.data_raw_dir / "download_manifest.csv"
    assert replay_manifest.exists()

    filtered_manifest = (
        replay.paths.data_raw_dir / "pit_snapshots" / "manifest.csv"
    )
    assert _csv_row_count(filtered_manifest) == 1
    assert _csv_row_count(project_root / "data" / "raw" / "pit_snapshots" / "manifest.csv") == 2

    replay_valuation_dir = (
        replay.paths.input_root / "data" / "external" / "valuation_snapshots"
    )
    assert sorted(path.name for path in replay_valuation_dir.glob("*.yaml")) == [
        "fmp_amd_valuation_2026_05_08.yaml"
    ]
    assert (
        project_root
        / "data"
        / "external"
        / "valuation_snapshots"
        / "fmp_amd_valuation_2026_05_10.yaml"
    ).exists()

    replay_risk_occurrence_dir = (
        replay.paths.input_root / "data" / "external" / "risk_event_occurrences"
    )
    assert sorted(path.name for path in replay_risk_occurrence_dir.glob("*.yaml")) == [
        "visible_policy_event.yaml"
    ]
    assert (
        project_root
        / "data"
        / "external"
        / "risk_event_occurrences"
        / "future_policy_event.yaml"
    ).exists()
    risk_occurrence_record = next(
        record
        for record in replay.input_records
        if record.artifact_id == "risk_event_occurrences"
    )
    assert risk_occurrence_record.status == "PASS"
    assert risk_occurrence_record.included_count == 1
    assert risk_occurrence_record.excluded_count == 2

    replay_thesis_dir = replay.paths.input_root / "data" / "external" / "trade_theses"
    assert sorted(path.name for path in replay_thesis_dir.glob("*.yaml")) == [
        "visible_amd_thesis.yaml"
    ]
    thesis_record = next(
        record for record in replay.input_records if record.artifact_id == "trade_theses"
    )
    assert thesis_record.status == "PASS_WITH_EXCLUSIONS"
    assert thesis_record.included_count == 1
    assert thesis_record.excluded_count == 1

    replay_trades_dir = replay.paths.input_root / "data" / "external" / "trades"
    assert sorted(path.name for path in replay_trades_dir.glob("*.yaml")) == [
        "visible_amd_trade.yaml"
    ]
    visible_trade_text = (replay_trades_dir / "visible_amd_trade.yaml").read_text(
        encoding="utf-8"
    )
    assert "closed_at: null" in visible_trade_text
    assert "exit_price: null" in visible_trade_text
    assert "2026-05-10" not in visible_trade_text
    trade_record = next(
        record for record in replay.input_records if record.artifact_id == "trade_records"
    )
    assert trade_record.status == "PASS_WITH_EXCLUSIONS"
    assert trade_record.included_count == 1
    assert trade_record.excluded_count == 1

    seeded_scores = replay.paths.data_processed_dir / "scores_daily.csv"
    score_dates = {row["as_of"] for row in _read_csv(seeded_scores)}
    assert score_dates == {"2026-05-07"}

    score_command = replay.command_results[0].command
    assert "--prices-path" in score_command
    assert str(filtered_prices) in score_command
    assert "--rates-path" in score_command
    assert str(filtered_rates) in score_command
    assert "--valuation-path" in score_command
    assert str(replay_valuation_dir) in score_command
    assert "--risk-event-occurrences-path" in score_command
    assert str(replay_risk_occurrence_dir) in score_command
    assert "--thesis-path" in score_command
    assert str(replay_thesis_dir) in score_command
    assert "--trades-path" in score_command
    assert str(replay_trades_dir) in score_command
    assert "--prediction-production-effect" in score_command
    assert "none" in score_command

    dashboard_command = replay.command_results[1].command
    assert dashboard_command[:3] == ("aits", "reports", "dashboard")
    assert "--daily-report-path" in dashboard_command
    assert str(replay.paths.reports_dir / "daily_score_2026-05-08.md") in dashboard_command
    assert "--output-path" in dashboard_command
    assert str(replay.paths.reports_dir / "evidence_dashboard_2026-05-08.html") in dashboard_command

    health_command = replay.command_results[2].command
    assert "--prices-path" in health_command
    assert str(filtered_prices) in health_command
    assert "--rates-path" in health_command
    assert str(filtered_rates) in health_command
    assert "--pit-manifest-path" in health_command
    assert str(filtered_manifest) in health_command

    secret_command = replay.command_results[3].command
    assert "--scan-paths" in secret_command
    assert str(replay.paths.root) in secret_command


def test_replay_day_inventory_cli_writes_bundle(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    output_root = tmp_path / "replays"
    _write_replay_fixture(project_root)

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "replay-day",
            "--as-of",
            "2026-05-08",
            "--project-root",
            str(project_root),
            "--output-root",
            str(output_root),
            "--run-id",
            "cli_inventory",
            "--inventory-only",
        ],
    )

    assert result.exit_code == 0
    assert "历史交易日回放：PASS_INVENTORY" in result.output
    bundle = output_root / "2026-05-08" / "cli_inventory"
    assert (bundle / "input_freeze_manifest.csv").exists()
    assert (bundle / "input_freeze_manifest.json").exists()
    assert (bundle / "replay_run.md").exists()
    assert not (
        bundle
        / "input"
        / "data"
        / "external"
        / "valuation_snapshots"
        / "fmp_amd_valuation_2026_05_10.yaml"
    ).exists()


def test_replay_day_caps_pit_inputs_to_as_of_when_production_cutoff_is_later(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    (project_root / "outputs" / "reports" / "daily_ops_run_metadata_2026-05-08.json").write_text(
        json.dumps(
            {
                "visibility_cutoff": "2026-05-10T12:00:00+00:00",
                "visibility_cutoff_source": "daily_run_finished_at_utc",
            }
        ),
        encoding="utf-8",
    )

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="unit_replay_late_cutoff",
        inventory_only=True,
    )

    assert replay.status == "PASS_INVENTORY"
    assert replay.visible_at.isoformat() == "2026-05-10T12:00:00+00:00"
    records = {record.artifact_id: record for record in replay.input_records}
    assert records["pit_manifest"].included_count == 1
    assert records["pit_manifest"].excluded_count == 1
    assert records["fmp_forward_pit_normalized"].included_count == 1
    assert records["fmp_forward_pit_normalized"].excluded_count == 1


def test_replay_day_rebuilds_pit_normalized_from_filtered_raw_manifest(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)
    _write_csv(
        project_root / "data" / "processed" / "pit_snapshots" / "fmp_forward_pit_2026-05-08.csv",
        [
            {
                "normalized_id": "future_only",
                "available_time": "2026-05-10T02:00:00+00:00",
            }
        ],
    )
    _write_fmp_forward_pit_raw_payload(
        project_root / "data" / "raw" / "fmp_forward_pit" / "amd" / "2026-05-08.json"
    )

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="unit_replay_rebuild_pit",
        inventory_only=True,
    )

    assert replay.status == "PASS_INVENTORY"
    record = next(
        record
        for record in replay.input_records
        if record.artifact_id == "fmp_forward_pit_normalized"
    )
    assert record.included_count == 1
    assert "rebuilt" in record.reason
    normalized_rows = _read_csv(
        replay.paths.data_processed_dir / "pit_snapshots" / "fmp_forward_pit_2026-05-08.csv"
    )
    assert normalized_rows[0]["available_time"] == "2026-05-08T02:00:00+00:00"
    assert normalized_rows[0]["raw_payload_path"] == (
        "data/raw/fmp_forward_pit/amd/2026-05-08.json"
    )


def test_replay_day_compare_to_production_writes_diff(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="unit_replay_diff",
        inventory_only=True,
        compare_to_production=True,
    )

    assert replay.status == "PASS_INVENTORY"
    assert replay.production_diff is not None
    assert replay.production_diff.status == "INCOMPLETE_DIFF"
    statuses = {
        artifact.artifact_id: artifact.status
        for artifact in replay.production_diff.artifacts
    }
    assert statuses["daily_score_report"] == "MISSING_REPLAY"
    assert statuses["features_daily_rows"] == "MATCH"
    assert replay.production_diff.report_path.exists()
    assert replay.production_diff.json_path.exists()

    replay_json = replay.paths.run_json_path.read_text(encoding="utf-8")
    assert "INCOMPLETE_DIFF" in replay_json


def test_replay_day_cache_only_openai_policy_filters_archived_prereview_by_cutoff(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)

    replay = run_historical_day_replay(
        as_of=date(2026, 5, 8),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="unit_replay_openai_cache",
        inventory_only=True,
        openai_replay_policy="cache-only",
    )

    assert replay.status == "PASS_INVENTORY"
    assert replay.openai_replay_policy == "cache-only"
    queue_record = next(
        record
        for record in replay.input_records
        if record.artifact_id == "risk_event_openai_prereview_queue"
    )
    assert queue_record.status == "PASS_WITH_EXCLUSIONS"
    assert queue_record.row_count == 3
    assert queue_record.included_count == 1
    assert queue_record.excluded_count == 2
    replay_queue_path = replay.paths.data_processed_dir / "risk_event_prereview_queue.json"
    assert replay_queue_path.exists()
    replay_queue = json.loads(replay_queue_path.read_text(encoding="utf-8"))
    assert [record["precheck_id"] for record in replay_queue["records"]] == [
        "visible_policy_export_controls"
    ]
    assert replay_queue["replay_filter"]["included_count"] == 1
    assert replay_queue["replay_filter"]["excluded_count"] == 2
    assert {
        record["reason"] for record in replay_queue["replay_filter"]["excluded_records"]
    } == {"available_after_replay_cutoff", "missing_provable_available_time"}
    replay_report_path = (
        replay.paths.reports_dir / "risk_event_prereview_openai_2026-05-08.md"
    )
    assert replay_report_path.exists()
    assert "PASS_WITH_EXCLUSIONS" in replay_report_path.read_text(encoding="utf-8")


def test_replay_window_inventory_skips_non_trading_days(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_replay_fixture(project_root)

    window = run_historical_replay_window(
        start=date(2026, 5, 8),
        end=date(2026, 5, 10),
        project_root=project_root,
        output_root=tmp_path / "replays",
        run_id="window_unit",
        inventory_only=True,
    )

    assert window.status == "PASS_WITH_SKIPS"
    assert [replay.as_of for replay in window.day_runs] == [date(2026, 5, 8)]
    assert [skipped.as_of for skipped in window.skipped_dates] == [
        date(2026, 5, 9),
        date(2026, 5, 10),
    ]
    assert window.day_runs[0].run_id == "window_unit_20260508"
    assert window.report_path.exists()
    assert window.json_path.exists()


def test_replay_window_inventory_cli_writes_window_report(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    output_root = tmp_path / "replays"
    _write_replay_fixture(project_root)

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "replay-window",
            "--start",
            "2026-05-08",
            "--end",
            "2026-05-10",
            "--project-root",
            str(project_root),
            "--output-root",
            str(output_root),
            "--run-id",
            "cli_window_inventory",
            "--inventory-only",
        ],
    )

    assert result.exit_code == 0
    assert "历史交易日批量回放：PASS_WITH_SKIPS" in result.output
    window_root = output_root / "windows" / "cli_window_inventory"
    assert (window_root / "replay_window.md").exists()
    assert (window_root / "replay_window.json").exists()


def _write_replay_fixture(project_root: Path) -> None:
    raw_dir = project_root / "data" / "raw"
    _write_csv(
        raw_dir / "prices_daily.csv",
        [
            _price_row("2026-05-07", "AMD", "100"),
            _price_row("2026-05-08", "AMD", "101"),
            _price_row("2026-05-10", "AMD", "102"),
        ],
    )
    _write_csv(
        raw_dir / "prices_marketstack_daily.csv",
        [
            _price_row("2026-05-07", "AMD", "100"),
            _price_row("2026-05-08", "AMD", "101"),
            _price_row("2026-05-10", "AMD", "102"),
        ],
    )
    _write_csv(
        raw_dir / "rates_daily.csv",
        [
            {"date": "2026-05-07", "series": "DGS10", "value": "4.0"},
            {"date": "2026-05-08", "series": "DGS10", "value": "4.1"},
            {"date": "2026-05-10", "series": "DGS10", "value": "4.2"},
        ],
    )
    _write_csv(
        raw_dir / "download_manifest.csv",
        [
            {
                "downloaded_at": "2026-05-10T00:00:00+00:00",
                "source_id": "fmp_eod_daily_prices",
                "provider": "Financial Modeling Prep",
                "endpoint": "historical-price-full",
                "request_parameters": "{}",
                "output_path": str(raw_dir / "prices_daily.csv"),
                "row_count": "3",
                "checksum_sha256": "source-cache-checksum",
            }
        ],
    )
    raw_pit = project_root / "data" / "raw" / "pit_snapshots"
    raw_pit.mkdir(parents=True)
    _write_csv(
        raw_pit / "manifest.csv",
        [
            {
                "snapshot_id": "pit_2026_05_08",
                "available_time": "2026-05-08T02:00:00+00:00",
                "raw_payload_path": "data/raw/fmp_forward_pit/amd/2026-05-08.json",
                "raw_payload_sha256": "abc",
                "row_count": "10",
            },
            {
                "snapshot_id": "pit_2026_05_10",
                "available_time": "2026-05-10T02:00:00+00:00",
                "raw_payload_path": "data/raw/fmp_forward_pit/amd/2026-05-10.json",
                "raw_payload_sha256": "def",
                "row_count": "10",
            },
        ],
    )
    pit_processed = project_root / "data" / "processed" / "pit_snapshots"
    pit_processed.mkdir(parents=True)
    _write_csv(
        pit_processed / "fmp_forward_pit_2026-05-08.csv",
        [
            {
                "normalized_id": "n1",
                "available_time": "2026-05-08T02:00:00+00:00",
            },
            {
                "normalized_id": "n2",
                "available_time": "2026-05-10T02:00:00+00:00",
            },
        ],
    )
    reports = project_root / "outputs" / "reports"
    reports.mkdir(parents=True)
    (reports / "pit_snapshots_validation_2026-05-08.md").write_text(
        "- 状态：PASS\n",
        encoding="utf-8",
    )
    (reports / "fmp_forward_pit_fetch_2026-05-08.md").write_text(
        "- 状态：PASS\n",
        encoding="utf-8",
    )
    (reports / "risk_event_prereview_openai_2026-05-08.md").write_text(
        "- 状态：PASS\n",
        encoding="utf-8",
    )
    (reports / "daily_score_2026-05-08.md").write_text(
        "# Daily score\n",
        encoding="utf-8",
    )
    (reports / "alerts_2026-05-08.md").write_text(
        "# Alerts\n",
        encoding="utf-8",
    )
    evidence_dir = reports / "evidence"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "daily_score_2026-05-08_trace.json").write_text(
        "{}\n",
        encoding="utf-8",
    )

    valuation_dir = project_root / "data" / "external" / "valuation_snapshots"
    valuation_dir.mkdir(parents=True)
    _write_valuation(valuation_dir / "fmp_amd_valuation_2026_05_08.yaml", "2026-05-08")
    _write_valuation(valuation_dir / "fmp_amd_valuation_2026_05_10.yaml", "2026-05-10")

    risk_occurrence_dir = project_root / "data" / "external" / "risk_event_occurrences"
    risk_occurrence_dir.mkdir(parents=True)
    _write_risk_occurrence(
        risk_occurrence_dir / "visible_policy_event.yaml",
        occurrence_id="visible_policy_event",
        triggered_at="2026-05-07",
        captured_at="2026-05-08",
    )
    _write_risk_occurrence(
        risk_occurrence_dir / "future_policy_event.yaml",
        occurrence_id="future_policy_event",
        triggered_at="2026-05-10",
        captured_at="2026-05-10",
    )
    (risk_occurrence_dir / "future_review_attestation.yaml").write_text(
        "\n".join(
            [
                "review_attestation:",
                "  attestation_id: future_review_attestation",
                "  review_date: '2026-05-10'",
                "  coverage_start: '2026-05-10'",
                "  coverage_end: '2026-05-10'",
                "  reviewer: policy_owner",
                "  reviewed_at: '2026-05-10'",
                "  review_decision: confirmed_no_unrecorded_material_events",
                "  rationale: 测试未来复核声明。",
                "  next_review_due: '2026-05-11'",
                "  review_scope:",
                "    - policy_event_occurrences",
                "  checked_sources:",
                "    - source_name: manual_policy_review",
                "      source_type: manual_input",
                "      captured_at: '2026-05-10'",
            ]
        ),
        encoding="utf-8",
    )

    thesis_dir = project_root / "data" / "external" / "trade_theses"
    thesis_dir.mkdir(parents=True)
    _write_trade_thesis(
        thesis_dir / "visible_amd_thesis.yaml",
        thesis_id="visible_amd_thesis",
        created_at="2026-05-07",
        updated_at="2026-05-08",
    )
    _write_trade_thesis(
        thesis_dir / "future_amd_thesis.yaml",
        thesis_id="future_amd_thesis",
        created_at="2026-05-10",
        updated_at="2026-05-10",
    )

    trades_dir = project_root / "data" / "external" / "trades"
    trades_dir.mkdir(parents=True)
    _write_trade_record(
        trades_dir / "visible_amd_trade.yaml",
        trade_id="visible_amd_trade",
        recorded_at="2026-05-07",
        opened_at="2026-05-07",
        closed_at="2026-05-10",
    )
    _write_trade_record(
        trades_dir / "future_amd_trade.yaml",
        trade_id="future_amd_trade",
        recorded_at="2026-05-10",
        opened_at="2026-05-10",
        closed_at=None,
    )

    processed = project_root / "data" / "processed"
    _write_csv(
        processed / "sec_fundamentals_2026-05-08.csv",
        [{"as_of": "2026-05-08", "ticker": "AMD", "metric_id": "revenue", "value": "1"}],
    )
    _write_csv(
        processed / "features_daily.csv",
        [
            {"as_of": "2026-05-07", "category": "price", "subject": "AMD"},
            {"as_of": "2026-05-10", "category": "price", "subject": "AMD"},
        ],
    )
    _write_csv(
        processed / "scores_daily.csv",
        [
            {"as_of": "2026-05-07", "component": "overall", "score": "70"},
            {"as_of": "2026-05-10", "component": "overall", "score": "80"},
        ],
    )
    (processed / "risk_event_prereview_queue.json").write_text(
        json.dumps(
            {
                "schema_version": "risk_event_prereview_queue.v2",
                "generated_at": "2026-05-10T00:00:00+00:00",
                "records": [
                    {
                        "precheck_id": "visible_policy_export_controls",
                        "risk_id": "policy_export_controls",
                        "request_timestamp": "2026-05-08T05:00:00+00:00",
                        "cache_created_at": "2026-05-08T05:00:00+00:00",
                    },
                    {
                        "precheck_id": "future_policy_export_controls",
                        "risk_id": "policy_export_controls",
                        "request_timestamp": "2026-05-10T05:00:00+00:00",
                        "cache_created_at": "2026-05-10T05:00:00+00:00",
                    },
                    {
                        "precheck_id": "legacy_without_timestamp",
                        "risk_id": "policy_export_controls",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    decision_dir = processed / "decision_snapshots"
    decision_dir.mkdir(parents=True)
    (decision_dir / "decision_snapshot_2026-05-08.json").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (reports / "daily_ops_run_metadata_2026-05-08.json").write_text(
        json.dumps(
            {
                "visibility_cutoff": "2026-05-08T12:00:00+00:00",
                "visibility_cutoff_source": "daily_run_finished_at_utc",
            }
        ),
        encoding="utf-8",
    )


def _write_valuation(path: Path, snapshot_date: str) -> None:
    path.write_text(
        "\n".join(
            [
                f"snapshot_id: fmp_amd_valuation_{snapshot_date.replace('-', '_')}",
                "ticker: AMD",
                f"as_of: '{snapshot_date}'",
                f"captured_at: '{snapshot_date}'",
                "valuation_metrics: []",
                "expectation_metrics: []",
            ]
        ),
        encoding="utf-8",
    )


def _write_risk_occurrence(
    path: Path,
    *,
    occurrence_id: str,
    triggered_at: str,
    captured_at: str,
) -> None:
    path.write_text(
        "\n".join(
            [
                f"occurrence_id: {occurrence_id}",
                "event_id: ai_chip_export_control_upgrade",
                "status: active",
                f"triggered_at: '{triggered_at}'",
                f"last_confirmed_at: '{captured_at}'",
                "evidence_grade: B",
                "severity: medium",
                "probability: medium",
                "scope: industry_chain_node",
                "time_sensitivity: medium",
                "reversibility: unknown",
                "action_class: score_eligible",
                "reviewer: policy_owner",
                f"reviewed_at: '{captured_at}'",
                "review_decision: confirmed_active",
                "rationale: 测试风险事件。",
                f"next_review_due: '{captured_at}'",
                "evidence_sources:",
                "  - source_name: manual_policy_review",
                "    source_type: manual_input",
                f"    captured_at: '{captured_at}'",
                f"    published_at: '{captured_at}'",
                "summary: 测试风险事件。",
            ]
        ),
        encoding="utf-8",
    )


def _write_trade_thesis(
    path: Path,
    *,
    thesis_id: str,
    created_at: str,
    updated_at: str,
) -> None:
    path.write_text(
        "\n".join(
            [
                f"thesis_id: {thesis_id}",
                "ticker: AMD",
                "direction: long",
                f"created_at: '{created_at}'",
                "time_horizon: medium",
                "position_scope: core_ai_bucket",
                "entry_reason:",
                "  - 测试 replay thesis 可见性过滤。",
                "ai_chain_nodes:",
                "  - gpu_asic_demand",
                "validation_metrics:",
                "  - metric_id: data_center_gpu_growth",
                "    description: 测试指标。",
                "    evidence_source: manual_review",
                "    expected_direction: improve",
                "    latest_status: pending",
                f"    updated_at: '{updated_at}'",
                "falsification_conditions:",
                "  - condition_id: demand_breaks",
                "    description: 测试证伪条件。",
                "    severity: high",
                "    triggered: false",
                "risk_events:",
                "  - risk_id: ai_chip_export_control_upgrade",
                "    level: L2",
                "    description: 测试风险事件。",
                "    action: review_if_active",
                "    active: false",
                f"    updated_at: '{updated_at}'",
                "review_frequency: weekly",
                "status: active",
                f"status_updated_at: '{updated_at}'",
            ]
        ),
        encoding="utf-8",
    )


def _write_trade_record(
    path: Path,
    *,
    trade_id: str,
    recorded_at: str,
    opened_at: str,
    closed_at: str | None,
) -> None:
    close_lines = (
        [f"closed_at: '{closed_at}'", "exit_price: 110.0"]
        if closed_at is not None
        else []
    )
    path.write_text(
        "\n".join(
            [
                f"trade_id: {trade_id}",
                "ticker: AMD",
                "direction: long",
                f"recorded_at: '{recorded_at}'",
                f"updated_at: '{closed_at or recorded_at}'",
                f"opened_at: '{opened_at}'",
                *close_lines,
                "thesis_id: visible_amd_thesis",
                "entry_price: 100.0",
                "position_size_pct: 0.10",
                "fees: 0",
            ]
        ),
        encoding="utf-8",
    )


def _write_fmp_forward_pit_raw_payload(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "provider": "Financial Modeling Prep",
                "source_type": "paid_vendor",
                "ticker": "AMD",
                "provider_symbol": "AMD",
                "as_of": "2026-05-08",
                "captured_at": "2026-05-08",
                "downloaded_at": "2026-05-08T02:00:00+00:00",
                "request_parameters_by_endpoint": {
                    "analyst-estimates": {"symbol": "AMD"},
                },
                "records_by_endpoint": {
                    "analyst-estimates": [
                        {
                            "symbol": "AMD",
                            "date": "2026-05-08",
                            "estimatedEpsAvg": 1.23,
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )


def _price_row(day: str, ticker: str, close: str) -> dict[str, str]:
    return {
        "date": day,
        "ticker": ticker,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": close,
        "volume": "1000",
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _csv_row_count(path: Path) -> int:
    return len(_read_csv(path))
