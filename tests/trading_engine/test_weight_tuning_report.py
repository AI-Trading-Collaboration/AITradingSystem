from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import yaml

from ai_trading_system.trading_engine.parameters.weight_tuning import (
    WEIGHT_TUNING_ALIAS_REPORT_TYPE,
    WEIGHT_TUNING_REPORT_TYPE,
    load_weight_tuning_payload,
    validate_weight_tuning_payload,
    weight_tuning_payload_date,
    write_recommended_shadow_weights,
    write_weight_tuning_candidates,
    write_weight_tuning_report_alias,
    write_weight_tuning_summary,
)
from trading_engine.weight_tuning_helpers import sample_weight_tuning_payload


def test_weight_tuning_summary_alias_and_shadow_weights_are_auditable(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    payload = sample_weight_tuning_payload(as_of=as_of)
    summary_json = tmp_path / "artifacts" / "weight_tuning" / as_of.isoformat()
    json_path = summary_json / "weight_tuning_summary.json"
    markdown_path = summary_json / "weight_tuning_summary.md"
    recommended_path = summary_json / "recommended_shadow_weights.yaml"
    candidates_path = summary_json / "weight_tuning_candidates.json"

    write_weight_tuning_summary(payload, json_path, markdown_path)
    write_recommended_shadow_weights(payload, recommended_path)
    write_weight_tuning_candidates(
        {
            "schema_version": payload["schema_version"],
            "report_type": "weight_tuning_candidates",
            "metadata": payload["metadata"],
            "summary_artifact": str(json_path),
            "candidate_count": 1,
            "candidates": payload["candidate_ranking"],
            "safety": payload["safety"],
        },
        candidates_path,
    )
    source_payload = load_weight_tuning_payload(json_path)
    report_date = weight_tuning_payload_date(source_payload, json_path)
    alias_json, alias_markdown = write_weight_tuning_report_alias(
        source_payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert validate_weight_tuning_payload(source_payload) == []
    assert report_date == as_of
    assert json_path.exists()
    assert markdown_path.exists()
    assert candidates_path.exists()
    assert "Restricted Backtest Weight Tuning Summary" in markdown_path.read_text(
        encoding="utf-8"
    )

    recommended_yaml = yaml.safe_load(recommended_path.read_text(encoding="utf-8"))
    assert recommended_yaml["metadata"]["production_effect"] == "none"
    assert recommended_yaml["metadata"]["manual_review_required"] is True
    assert recommended_yaml["metadata"]["auto_promotion"] is False
    assert recommended_yaml["constraints"]["fallback_signals_free_tuned"] is False
    assert recommended_yaml["constraints"]["production_write_allowed"] is False

    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == WEIGHT_TUNING_ALIAS_REPORT_TYPE
    assert alias_payload["source_report_type"] == WEIGHT_TUNING_REPORT_TYPE
    assert "Restricted Backtest Weight Tuning Summary" in alias_markdown.read_text(
        encoding="utf-8"
    )
