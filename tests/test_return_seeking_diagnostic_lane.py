from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/research/return_seeking_diagnostic_lane_policy.yaml")
SIGNAL_AUDIT_PATH = Path("inputs/research_reviews/return_seeking_signal_audit.yaml")
ACTUAL_PATH = Path("inputs/research_reviews/return_seeking_actual_path_matrix.yaml")
BETA_ATTRIBUTION_PATH = Path("inputs/research_reviews/return_seeking_beta_tqqq_attribution.yaml")
CONTRAST_PATH = Path("inputs/research_reviews/return_seeking_2022_vs_2023_contrast.yaml")
FINAL_MATRIX_PATH = Path("inputs/research_reviews/return_seeking_diagnostic_lane_final_matrix.yaml")


def test_return_seeking_diagnostic_lane_cli_is_registered() -> None:
    result = CliRunner().invoke(app, ["research", "trends", "--help"])

    assert result.exit_code == 0
    assert "return-seeking-diagnostic-lane" in result.output


def test_return_seeking_policy_is_diagnostic_only() -> None:
    policy = _load_yaml(POLICY_PATH)

    assert set(policy["allowed_signals"]) == {
        "stay_constructive",
        "add_risk",
        "high_confidence_risk_on",
    }
    for blocked in (
        "defensive_overlay",
        "full_allocation",
        "gated_integration",
        "promotion",
        "paper_shadow",
        "production",
        "broker_order",
    ):
        assert blocked in policy["blocked_downstream_usage"]
    safety = policy["safety_boundary"]
    assert safety["defensive_overlay_usage_allowed"] is False
    assert safety["full_allocation_usage_allowed"] is False
    assert safety["gated_integration_allowed"] is False
    assert safety["promotion_allowed"] is False
    assert safety["broker_action"] == "none"


def test_return_seeking_signal_audit_blocks_defensive_and_promotion_usage() -> None:
    audit = _load_yaml(SIGNAL_AUDIT_PATH)

    assert audit["status"] == "RETURN_SEEKING_SIGNAL_AUDIT_READY_PROMOTION_BLOCKED"
    assert audit["summary"]["return_seeking_probe_count"] == 7
    assert audit["summary"]["add_risk_count"] > 0
    assert audit["summary"]["high_confidence_risk_on_count"] > 0
    assert audit["summary"]["defensive_overlay_usage_allowed"] is False
    assert audit["summary"]["full_allocation_usage_allowed"] is False
    assert audit["summary"]["gated_integration_allowed"] is False
    for row in audit["signal_rows"]:
        assert row["allowed_usage"] == ["return_seeking_diagnostic"]
        assert "defensive_overlay" in row["blocked_usage"]
        assert row["defensive_overlay_usage_allowed"] is False
        assert row["promotion_allowed"] is False


def test_return_seeking_actual_path_upside_has_drawdown_regression() -> None:
    actual_path = _load_yaml(ACTUAL_PATH)
    summary = actual_path["summary"]

    assert (
        actual_path["status"]
        == "RETURN_SEEKING_ACTUAL_PATH_UPSIDE_WITH_DRAWDOWN_REGRESSION_PROMOTION_BLOCKED"
    )
    assert summary["probe_count"] == 7
    assert summary["positive_return_delta_probe_count"] == 7
    assert summary["drawdown_regression_probe_count"] == 7
    assert summary["diagnostic_value_probe_count"] == 0
    assert summary["defensive_overlay_usage_allowed"] is False
    assert summary["full_allocation_usage_allowed"] is False
    assert summary["gated_integration_allowed"] is False
    assert all(
        row["annual_return_delta_vs_no_return_seeking"] > 0
        for row in actual_path["probe_rows"]
    )
    assert all(row["drawdown_delta_vs_no_return_seeking"] < 0 for row in actual_path["probe_rows"])
    assert all(not row["return_seeking_diagnostic_value"] for row in actual_path["probe_rows"])
    assert actual_path["promotion_allowed"] is False
    assert actual_path["broker_action"] == "none"


def test_return_seeking_beta_tqqq_attribution_identifies_dependency() -> None:
    attribution = _load_yaml(BETA_ATTRIBUTION_PATH)

    assert (
        attribution["status"]
        == "RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_DEPENDENT_PROMOTION_BLOCKED"
    )
    assert attribution["summary"]["tqqq_beta_dependency_suspected_count"] > 0
    tqqq_rows = [
        row for row in attribution["attribution_rows"] if row["tqqq_usage"] != "none"
    ]
    assert tqqq_rows
    assert any(row["tqqq_beta_dependency_suspected"] for row in tqqq_rows)
    assert all(row["promotion_allowed"] is False for row in attribution["attribution_rows"])
    assert attribution["production_allowed"] is False
    assert attribution["broker_action"] == "none"


def test_return_seeking_contrast_is_2023_plus_dependent() -> None:
    contrast = _load_yaml(CONTRAST_PATH)

    assert contrast["status"] == "RETURN_SEEKING_2022_VS_2023_CONTRAST_READY_PROMOTION_BLOCKED"
    assert contrast["summary"]["positive_delta_count_2022"] == 0
    assert contrast["summary"]["positive_delta_count_2023_plus"] == 7
    assert contrast["summary"]["depends_on_2023_plus"] is True
    assert contrast["promotion_allowed"] is False


def test_return_seeking_final_matrix_blocks_all_downstream_usage() -> None:
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert (
        final["status"]
        == "RETURN_SEEKING_DIAGNOSTIC_UPSIDE_DEPENDENT_DRAWDOWN_REGRESSED_PROMOTION_BLOCKED"
    )
    assert final["summary"]["positive_return_delta_probe_count"] == 7
    assert final["summary"]["drawdown_regression_probe_count"] == 7
    assert final["summary"]["diagnostic_value_probe_count"] == 0
    assert final["summary"]["beta_or_tqqq_dependency_suspected"] is True
    assert final["summary"]["depends_on_2023_plus"] is True
    assert final["summary"]["defensive_overlay_usage_allowed"] is False
    assert final["summary"]["full_allocation_usage_allowed"] is False
    assert final["summary"]["gated_integration_allowed"] is False
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
