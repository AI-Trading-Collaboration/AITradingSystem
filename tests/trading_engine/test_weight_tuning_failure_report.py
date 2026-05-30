from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    WEIGHT_TUNING_FAILURE_ALIAS_REPORT_TYPE,
    WEIGHT_TUNING_FAILURE_REPORT_TYPE,
    load_weight_tuning_failure_payload,
    validate_weight_tuning_failure_payload,
    weight_tuning_failure_payload_date,
    write_weight_tuning_failure_report_alias,
)
from trading_engine.test_weight_tuning_failure_attribution import (
    write_weight_tuning_failure_artifact,
)


def test_weight_tuning_failure_report_alias_is_auditable(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    source_path = write_weight_tuning_failure_artifact(tmp_path, as_of=as_of)

    payload = load_weight_tuning_failure_payload(source_path)
    report_date = weight_tuning_failure_payload_date(payload, source_path)
    alias_json, alias_markdown = write_weight_tuning_failure_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert validate_weight_tuning_failure_payload(payload) == []
    assert report_date == as_of
    assert "Weight Tuning Failure Attribution Summary" in source_path.with_suffix(
        ".md"
    ).read_text(encoding="utf-8")
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == WEIGHT_TUNING_FAILURE_ALIAS_REPORT_TYPE
    assert alias_payload["source_report_type"] == WEIGHT_TUNING_FAILURE_REPORT_TYPE
    assert "Root Cause Assessment" in alias_markdown.read_text(encoding="utf-8")
