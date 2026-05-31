from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    render_weight_stability_readiness_explanation,
    validate_weight_stability_readiness_payload,
    weight_stability_readiness_payload_date,
    write_weight_stability_readiness_report_alias,
    write_weight_stability_readiness_summary,
)
from trading_engine.weight_stability_readiness_helpers import (
    sample_weight_stability_readiness_payload,
)


def test_weight_stability_readiness_report_alias(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    payload = sample_weight_stability_readiness_payload(as_of=as_of)
    artifact_dir = tmp_path / "artifacts" / "weight_stability_readiness" / as_of.isoformat()
    json_path, _ = write_weight_stability_readiness_summary(
        payload,
        artifact_dir / "weight_stability_readiness_summary.json",
        artifact_dir / "weight_stability_readiness_summary.md",
    )

    json_alias, markdown_alias = write_weight_stability_readiness_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        as_of,
    )

    assert weight_stability_readiness_payload_date(payload, json_path) == as_of
    assert json_alias.exists()
    assert markdown_alias.exists()
    assert validate_weight_stability_readiness_payload(payload) == []
    explanation = render_weight_stability_readiness_explanation(payload)
    assert "status=RECOVERY_FAILED" in explanation
    assert "blocking_checks=freshness, backtest_manifest, price_coverage" in explanation
