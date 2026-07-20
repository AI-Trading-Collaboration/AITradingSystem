from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_formal_research_method_contract_fixture,
    run_signal_input_completeness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.platform.artifacts.validation_session import (
    artifact_validation_session,
)


@pytest.fixture(scope="module")
def paper_shadow_daily_fixtures(
    tmp_path_factory: pytest.TempPathFactory,
) -> Any:
    root = tmp_path_factory.mktemp("paper-shadow-drift-upstream")
    monkeypatch = pytest.MonkeyPatch()
    try:
        with artifact_validation_session():
            contract_fixture = run_formal_research_method_contract_fixture(root, monkeypatch)
            contract_id = contract_fixture["formal_research_method_contract"]["contract_id"]
            protocol_result = readiness.build_paper_shadow_protocol(
                contract_id=contract_id,
                contract_dir=root / "formal_research_method_contract",
                output_dir=root / "paper_shadow_protocol",
                generated_at=datetime(2026, 6, 15, tzinfo=UTC),
            )
            signal_input = run_signal_input_completeness_fixture(
                root,
                as_of="2026-06-12",
            )
            shared = {
                "contract_id": contract_id,
                "protocol_id": protocol_result["protocol_id"],
                "signal_input_completeness_id": signal_input["monitor_id"],
                "contract_dir": root / "formal_research_method_contract",
                "protocol_dir": root / "paper_shadow_protocol",
                "signal_input_completeness_dir": root / "signal_input_completeness",
            }
            yield {
                "clean": _paper_shadow_daily_fixture(root / "clean", shared=shared),
                "missing_market_panel": _paper_shadow_daily_fixture(
                    root / "missing-market-panel",
                    shared=shared,
                    missing_market_panel=True,
                ),
            }
    finally:
        monkeypatch.undo()


def test_paper_shadow_drift_monitor_builds_clean_report(
    tmp_path: Path,
    paper_shadow_daily_fixtures: dict[str, dict[str, Any]],
) -> None:
    fixture = paper_shadow_daily_fixtures["clean"]
    result = drift.build_paper_shadow_drift_monitor_report(
        observation_id=fixture["observation_id"],
        observation_dir=fixture["observation_dir"],
        contract_id=fixture["contract_id"],
        contract_dir=fixture["contract_dir"],
        output_dir=tmp_path / "paper_shadow_drift_monitor",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    report = result["paper_shadow_drift_report"]
    validation = result["paper_shadow_drift_validation"]
    payload = drift.paper_shadow_drift_monitor_report_payload(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "paper_shadow_drift_monitor",
    )

    assert report["drift_severity"] == "BLOCKING"
    assert report["next_action"] == "return_to_research"
    assert report["blocking_count"] == 1
    assert {row["family"] for row in report["findings"]} == set(drift.DRIFT_FAMILIES)
    assert validation["status"] == "PASS"
    assert payload["paper_shadow_drift_report"]["drift_severity"] == "BLOCKING"
    assert "paper_shadow_drift_severity" in result["reader_brief_section"]
    assert_research_safe(report)
    assert report["paper_account_state_mutated"] is False


def test_paper_shadow_drift_monitor_flags_missing_signal_inputs(
    tmp_path: Path,
    paper_shadow_daily_fixtures: dict[str, dict[str, Any]],
) -> None:
    fixture = paper_shadow_daily_fixtures["missing_market_panel"]
    result = drift.build_paper_shadow_drift_monitor_report(
        observation_id=fixture["observation_id"],
        observation_dir=fixture["observation_dir"],
        contract_id=fixture["contract_id"],
        contract_dir=fixture["contract_dir"],
        output_dir=tmp_path / "paper_shadow_drift_monitor",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    report = result["paper_shadow_drift_report"]
    missing = next(row for row in report["findings"] if row["family"] == "missing_signal_inputs")

    assert report["drift_severity"] == "BLOCKING"
    assert report["next_action"] == "return_to_research"
    assert missing["severity"] == "BLOCKING"
    assert "market_panel_artifact" in missing["current_value"]
    assert result["paper_shadow_drift_validation"]["status"] == "PASS"


def test_paper_shadow_drift_monitor_cli_report_and_validate(
    tmp_path: Path,
    paper_shadow_daily_fixtures: dict[str, dict[str, Any]],
) -> None:
    fixture = paper_shadow_daily_fixtures["clean"]
    output_dir = tmp_path / "paper_shadow_drift_monitor"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-drift-monitor",
            "report",
            "--observation-id",
            fixture["observation_id"],
            "--observation-dir",
            str(fixture["observation_dir"]),
            "--contract-id",
            fixture["contract_id"],
            "--contract-dir",
            str(fixture["contract_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "drift_severity=BLOCKING" in result.output
    monitor_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("monitor_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-drift-monitor",
            "report",
            "--monitor-id",
            monitor_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "validation_status=PASS" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-paper-shadow-drift-monitor",
            "--monitor-id",
            monitor_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def _paper_shadow_daily_fixture(
    tmp_path: Path,
    *,
    shared: dict[str, Any],
    missing_market_panel: bool = False,
) -> dict[str, Any]:
    tmp_path.mkdir(parents=True)
    market_panel = tmp_path / "market_panel_2026-06-12.json"
    if not missing_market_panel:
        market_panel.write_text(
            json.dumps({"as_of": "2026-06-12", "report_type": "market_panel"}),
            encoding="utf-8",
        )
    signal_artifact = tmp_path / "candidate_signal_summary.json"
    signal_artifact.write_text(
        json.dumps(
            {
                "as_of": "2026-06-12",
                "candidate": readiness.TOP_FILTERED_CANDIDATE,
                "signal_output": "OBSERVE_RISK_ON",
            }
        ),
        encoding="utf-8",
    )
    observation = daily.run_paper_shadow_daily_observation(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        observation_date="2026-06-12",
        market_panel_artifact=market_panel,
        signal_artifact=signal_artifact,
        signal_output="OBSERVE_RISK_ON",
        hypothetical_weight_recommendation="paper_shadow_only_no_official_weight",
        risk_off_risk_on_state="risk_on",
        drawdown_state="normal",
        rotation_event="none",
        mismatch_event="none",
        benchmark_comparison="tracking_QQQ_SPY_SMH",
        manual_reviewer_notes="synthetic drift monitor fixture",
        contract_id=shared["contract_id"],
        protocol_id=shared["protocol_id"],
        signal_input_completeness_id=shared["signal_input_completeness_id"],
        contract_dir=shared["contract_dir"],
        protocol_dir=shared["protocol_dir"],
        signal_input_completeness_dir=shared["signal_input_completeness_dir"],
        output_dir=tmp_path / "paper_shadow_daily",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    return {
        "contract_id": shared["contract_id"],
        "contract_dir": shared["contract_dir"],
        "protocol_id": shared["protocol_id"],
        "observation_id": observation["observation_id"],
        "observation_dir": tmp_path / "paper_shadow_daily",
    }
