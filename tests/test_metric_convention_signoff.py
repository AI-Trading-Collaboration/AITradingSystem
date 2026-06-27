from __future__ import annotations

from pathlib import Path

from ai_trading_system.external_validation import (
    DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    load_external_metric_convention_namespace,
    run_external_platform_metric_convention_signoff,
    validate_external_metric_convention_usage,
)


def test_portfolio_visualizer_metric_namespace_blocks_cross_comparable_risk() -> None:
    namespace = load_external_metric_convention_namespace(
        DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH
    )

    assert namespace["status"] == "METRIC_CONVENTION_NAMESPACE_READY"
    annual = namespace["return_metrics"]["annual_return"]
    assert annual["internal_metric_id"] == "annual_return_total_return_portfolio_path"
    drawdown = namespace["risk_metrics"]["max_drawdown"]
    assert drawdown["external_metric_id"] == "max_drawdown_monthly_return"
    assert drawdown["internal_metric_id"] == "max_drawdown_daily_equity"
    assert drawdown["promotion_usage"] == "not_cross_comparable"

    allowed = validate_external_metric_convention_usage(
        external_metric_id="annual_return_total_return_monthly_rebalanced_portfolio",
        internal_metric_id="annual_return_total_return_portfolio_path",
    )
    blocked = validate_external_metric_convention_usage(
        external_metric_id="max_drawdown_monthly_return",
        internal_metric_id="max_drawdown_daily_equity",
    )

    assert allowed["status"] == "METRIC_CONVENTION_USAGE_ALLOWED"
    assert allowed["hard_warning"] is False
    assert blocked["status"] == "METRIC_CONVENTION_USAGE_BLOCKED"
    assert blocked["hard_warning"] is True
    assert blocked["reason"] == "signed_off_as_not_cross_comparable"


def test_metric_convention_signoff_payload_exposes_namespace(tmp_path: Path) -> None:
    payload = run_external_platform_metric_convention_signoff(
        output_root=tmp_path / "outputs",
        _manual_input_payload={
            "status": "MANUAL_EXTERNAL_INPUT_RECORDED",
            "valid_records": [{"external_tool": "Portfolio Visualizer"}],
            "invalid_records": [],
            "missing_strategy_ids": [],
            "artifact_paths": {},
        },
    )

    assert payload["status"] == "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS"
    assert payload["convention_namespace"]["status"] == "METRIC_CONVENTION_NAMESPACE_READY"
    assert (
        payload["convention_namespace"]["risk_metrics"]["sharpe"]["internal_metric_id"]
        == "sharpe_daily_zero_rf"
    )
