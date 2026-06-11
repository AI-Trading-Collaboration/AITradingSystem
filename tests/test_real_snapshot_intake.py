from __future__ import annotations

from pathlib import Path

import yaml
from manual_portfolio_guardrail_helpers import manual_snapshot_payload, write_manual_snapshot

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    intake_real_snapshot,
    lint_real_snapshot_file,
    validate_real_snapshot,
    write_real_snapshot_template,
)


def test_real_snapshot_template_generation_is_redaction_safe(tmp_path: Path) -> None:
    result = write_real_snapshot_template(tmp_path / "real_template.yaml")
    payload = yaml.safe_load(Path(result["template_path"]).read_text(encoding="utf-8"))

    assert result["status"] == "PASS"
    assert payload["snapshot"]["broker_imported"] is False
    assert payload["metadata"]["contains_order_id"] is False
    assert payload["metadata"]["broker_action_taken"] is False


def test_real_snapshot_lint_sensitive_field_fails(tmp_path: Path) -> None:
    payload = manual_snapshot_payload()
    payload["metadata"]["contains_order_id"] = True
    snapshot_path = write_manual_snapshot(tmp_path, payload)

    redaction = lint_real_snapshot_file(snapshot_path)

    assert redaction["redaction_status"] == "FAIL"
    assert redaction["contains_order_id"] is True
    assert any("contains_order_id" in issue for issue in redaction["blocking_issues"])


def test_real_snapshot_intake_validates_manual_snapshot(tmp_path: Path) -> None:
    snapshot_path = write_manual_snapshot(tmp_path)
    intake = intake_real_snapshot(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "real_snapshot_intake",
        manual_snapshot_output_dir=tmp_path / "manual_portfolio_snapshot",
    )
    manifest = intake["manifest"]
    validation = validate_real_snapshot(
        snapshot_intake_id=intake["snapshot_intake_id"],
        output_dir=tmp_path / "real_snapshot_intake",
    )

    assert manifest["status"] == "PASS"
    assert manifest["redaction_status"] == "PASS"
    assert manifest["snapshot_status"] == "PASS"
    assert manifest["manual_portfolio_snapshot_id"]
    assert validation["status"] == "PASS"
    assert manifest["broker_action_allowed"] is False
