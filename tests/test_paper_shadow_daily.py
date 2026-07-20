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
from ai_trading_system.platform.artifacts.validation_session import (
    artifact_validation_session,
)


@pytest.fixture(scope="module")
def paper_shadow_upstream_fixture(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("paper-shadow-daily-upstream")
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
            signal_input = run_signal_input_completeness_fixture(root, as_of="2026-06-12")
            market_panel = root / "market_panel_2026-06-12.json"
            market_panel.write_text(
                json.dumps({"as_of": "2026-06-12", "report_type": "market_panel"}),
                encoding="utf-8",
            )
            signal_artifact = root / "candidate_signal_summary.json"
            signal_artifact.write_text(
                json.dumps(
                    {
                        "as_of": "2026-06-12",
                        "candidate": "median_plus_regime_mismatch_filter",
                        "signal_output": "OBSERVE_RISK_ON",
                    }
                ),
                encoding="utf-8",
            )
            yield {
                "contract_id": contract_id,
                "protocol_id": protocol_result["protocol_id"],
                "signal_input_completeness_id": signal_input["monitor_id"],
                "contract_dir": root / "formal_research_method_contract",
                "protocol_dir": root / "paper_shadow_protocol",
                "signal_input_completeness_dir": root / "signal_input_completeness",
                "market_panel": market_panel,
                "signal_artifact": signal_artifact,
            }
    finally:
        monkeypatch.undo()


def test_paper_shadow_daily_observation_builds_and_validates(
    tmp_path: Path,
    paper_shadow_upstream_fixture: dict[str, Any],
) -> None:
    fixture = paper_shadow_upstream_fixture
    result = daily.run_paper_shadow_daily_observation(
        candidate="median_plus_regime_mismatch_filter",
        observation_date="2026-06-12",
        market_panel_artifact=fixture["market_panel"],
        signal_artifact=fixture["signal_artifact"],
        signal_output="OBSERVE_RISK_ON",
        hypothetical_weight_recommendation="paper_shadow_only_no_official_weight",
        risk_off_risk_on_state="risk_on",
        drawdown_state="normal",
        rotation_event="none",
        mismatch_event="none",
        benchmark_comparison="tracking_QQQ_SPY_SMH",
        manual_reviewer_notes="synthetic daily observation fixture",
        contract_id=fixture["contract_id"],
        protocol_id=fixture["protocol_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        contract_dir=fixture["contract_dir"],
        protocol_dir=fixture["protocol_dir"],
        signal_input_completeness_dir=fixture["signal_input_completeness_dir"],
        output_dir=tmp_path / "paper_shadow_daily",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    observation = result["paper_shadow_daily_observation"]
    validation = result["paper_shadow_daily_validation"]
    payload = daily.paper_shadow_daily_report_payload(
        observation_id=result["observation_id"],
        output_dir=tmp_path / "paper_shadow_daily",
    )

    assert observation["observation_status"] == "BLOCKED"
    assert observation["signal_input_status"] == "OK"
    assert set(observation["blocking_reasons"]) == {
        "formal_contract_not_ready",
        "paper_shadow_protocol_not_ready",
    }
    assert observation["candidate"] == "median_plus_regime_mismatch_filter"
    assert observation["daily_review"]["signal_output"] == "OBSERVE_RISK_ON"
    assert (
        observation["daily_review"]["hypothetical_weight_recommendation"]["paper_shadow_only"]
        is True
    )
    assert validation["status"] == "PASS"
    assert payload["paper_shadow_daily_observation"]["observation_status"] == "BLOCKED"
    assert "paper_shadow_daily_observation_id" in result["reader_brief_section"]
    assert_research_safe(observation)
    assert observation["paper_account_state_mutated"] is False


def test_paper_shadow_daily_validation_fails_missing_input(
    tmp_path: Path,
    paper_shadow_upstream_fixture: dict[str, Any],
) -> None:
    fixture = paper_shadow_upstream_fixture
    result = daily.run_paper_shadow_daily_observation(
        candidate="median_plus_regime_mismatch_filter",
        observation_date="2026-06-12",
        market_panel_artifact=tmp_path / "missing_market_panel.json",
        signal_artifact=fixture["signal_artifact"],
        signal_output="OBSERVE_RISK_ON",
        hypothetical_weight_recommendation="paper_shadow_only_no_official_weight",
        risk_off_risk_on_state="risk_on",
        drawdown_state="normal",
        rotation_event="none",
        mismatch_event="none",
        benchmark_comparison="tracking_QQQ_SPY_SMH",
        manual_reviewer_notes="synthetic daily observation fixture",
        contract_id=fixture["contract_id"],
        protocol_id=fixture["protocol_id"],
        signal_input_completeness_id=fixture["signal_input_completeness_id"],
        contract_dir=fixture["contract_dir"],
        protocol_dir=fixture["protocol_dir"],
        signal_input_completeness_dir=fixture["signal_input_completeness_dir"],
        output_dir=tmp_path / "paper_shadow_daily",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )

    validation = result["paper_shadow_daily_validation"]
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}
    assert result["paper_shadow_daily_observation"]["observation_status"] == "BLOCKED"
    assert validation["status"] == "FAIL"
    assert "input_artifacts_exist" in failed


def test_paper_shadow_daily_cli_run_report_and_validate(
    tmp_path: Path,
    paper_shadow_upstream_fixture: dict[str, Any],
) -> None:
    fixture = paper_shadow_upstream_fixture
    output_dir = tmp_path / "paper_shadow_daily"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-daily",
            "run",
            "--candidate",
            "median_plus_regime_mismatch_filter",
            "--date",
            "2026-06-12",
            "--market-panel-artifact",
            str(fixture["market_panel"]),
            "--signal-artifact",
            str(fixture["signal_artifact"]),
            "--signal-output",
            "OBSERVE_RISK_ON",
            "--hypothetical-weight-recommendation",
            "paper_shadow_only_no_official_weight",
            "--risk-state",
            "risk_on",
            "--drawdown-state",
            "normal",
            "--rotation-event",
            "none",
            "--mismatch-event",
            "none",
            "--benchmark-comparison",
            "tracking_QQQ_SPY_SMH",
            "--manual-reviewer-notes",
            "synthetic daily observation fixture",
            "--contract-id",
            fixture["contract_id"],
            "--protocol-id",
            fixture["protocol_id"],
            "--signal-input-completeness-id",
            fixture["signal_input_completeness_id"],
            "--contract-dir",
            str(fixture["contract_dir"]),
            "--protocol-dir",
            str(fixture["protocol_dir"]),
            "--signal-input-completeness-dir",
            str(fixture["signal_input_completeness_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "observation_status=BLOCKED" in result.output
    assert "signal_input_status=OK" in result.output
    observation_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("observation_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "paper-shadow-daily",
            "report",
            "--observation-id",
            observation_id,
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
            "validate-paper-shadow-daily",
            "--observation-id",
            observation_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output
