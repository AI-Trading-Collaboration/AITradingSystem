from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import yaml

from ai_trading_system.trading_engine.parameters.weight_stability import (
    WEIGHT_STABILITY_ALIAS_REPORT_TYPE,
    WEIGHT_STABILITY_REPORT_TYPE,
    load_weight_stability_payload,
    validate_weight_stability_payload,
    weight_stability_payload_date,
    write_recommended_stable_shadow_weights,
    write_stable_weight_candidates,
    write_weight_stability_report_alias,
    write_weight_stability_summary,
)
from trading_engine.weight_stability_helpers import sample_weight_stability_payload


def test_weight_stability_report_alias_and_shadow_weights_are_auditable(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    payload = sample_weight_stability_payload(
        as_of=as_of,
        candidate_status="watch",
    )
    artifact_dir = tmp_path / "artifacts" / "weight_stability" / as_of.isoformat()
    json_path = artifact_dir / "weight_stability_summary.json"
    markdown_path = artifact_dir / "weight_stability_summary.md"
    candidates_path = artifact_dir / "stable_weight_candidates.json"
    recommended_path = artifact_dir / "recommended_stable_shadow_weights.yaml"

    write_weight_stability_summary(payload, json_path, markdown_path)
    write_stable_weight_candidates(
        {
            "schema_version": 1,
            "report_type": "stable_weight_candidates",
            "metadata": payload["metadata"],
            "summary_artifact": str(json_path),
            "candidate_count": 1,
            "candidates": [payload["recommended_candidate"]],
            "safety": payload["safety"],
        },
        candidates_path,
    )
    write_recommended_stable_shadow_weights(payload, recommended_path)
    source_payload = load_weight_stability_payload(json_path)
    report_date = weight_stability_payload_date(source_payload, json_path)
    alias_json, alias_markdown = write_weight_stability_report_alias(
        source_payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert validate_weight_stability_payload(source_payload) == []
    assert report_date == as_of
    assert json_path.exists()
    assert markdown_path.exists()
    assert candidates_path.exists()
    assert "Weight Search Stability Summary" in markdown_path.read_text(encoding="utf-8")

    recommended_yaml = yaml.safe_load(recommended_path.read_text(encoding="utf-8"))
    assert recommended_yaml["metadata"]["production_effect"] == "none"
    assert recommended_yaml["metadata"]["manual_review_required"] is True
    assert recommended_yaml["metadata"]["auto_promotion"] is False
    assert recommended_yaml["constraints"]["production_write_allowed"] is False
    assert recommended_yaml["constraints"]["promotion_allowed"] is False

    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == WEIGHT_STABILITY_ALIAS_REPORT_TYPE
    assert alias_payload["source_report_type"] == WEIGHT_STABILITY_REPORT_TYPE
    assert "Weight Search Stability Summary" in alias_markdown.read_text(encoding="utf-8")
