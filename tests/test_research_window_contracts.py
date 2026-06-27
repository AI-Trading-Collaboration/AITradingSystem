from __future__ import annotations

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.research_window_extension import (
    artifact_has_required_window_fields,
    load_research_window_registry,
    validate_research_window_contracts,
    window_metadata,
)


def test_research_window_registry_contracts_pass() -> None:
    registry = load_research_window_registry()

    validation = validate_research_window_contracts(registry)

    assert validation["status"] == "PASS"
    windows = registry["windows"]
    assert str(windows["exact_three_asset_validated"]["actual_portfolio_start"]) == "2021-02-22"
    assert (
        str(windows["exact_three_asset_primary_only_extension"]["actual_portfolio_start"])
        == "2020-05-28"
    )
    assert (
        windows["requested_sgov_inception_range"]["requested_start"]
        != windows["requested_sgov_inception_range"]["actual_portfolio_start"]
    )


def test_primary_only_extension_carries_sgov_secondary_gap_caveat() -> None:
    registry = load_research_window_registry()
    extension = registry["windows"]["exact_three_asset_primary_only_extension"]

    assert extension["data_quality_contract"] == "primary_only_for_sgov_before_2021_02_22"
    assert "sgov_secondary_gap_2020_05_28_to_2021_02_19" in extension["caveats"]
    assert "primary_leaderboard_without_owner_accepting_caveat" in extension["blocked_usage"]


def test_proxy_window_cannot_mix_with_exact_leaderboard() -> None:
    registry = load_research_window_registry()
    proxy = registry["windows"]["qqq_tqqq_sgov_proxy_robustness"]

    assert proxy["exact_or_proxy"] == "proxy"
    assert "exact_three_asset_leaderboard" in proxy["blocked_usage"]
    assert "exact_sgov_promotion_evidence" in proxy["blocked_usage"]


def test_window_metadata_has_required_artifact_fields() -> None:
    registry = load_research_window_registry()
    window = registry["windows"]["exact_three_asset_validated"]

    metadata = window_metadata(window)

    assert artifact_has_required_window_fields(metadata) is True
    assert validate_research_window_contracts(registry, artifact=metadata)["status"] == "PASS"


def test_missing_window_metadata_fails_artifact_contract() -> None:
    registry = load_research_window_registry()
    artifact = {"research_window_id": "exact_three_asset_validated"}

    validation = validate_research_window_contracts(registry, artifact=artifact)

    assert validation["status"] == "FAIL"
    assert any(
        str(issue["code"]).startswith("artifact_missing_window_fields")
        for issue in validation["issues"]
    )


def test_research_window_extension_cli_is_registered() -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "window-extension" in result.output
