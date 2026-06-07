from __future__ import annotations

import json

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_medium_real_report,
    validate_medium_real_sweep,
    validate_sweep_profiles_payload,
)


def test_medium_real_profile_and_report_contract(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)

    validation = validate_medium_real_sweep(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        min_expected_candidates=1,
    )
    report = build_medium_real_report(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "medium_real",
    )

    assert validate_sweep_profiles_payload()["status"] == "PASS"
    assert validation["status"] == "PASS"
    assert report["evaluator_mode"] == "real_dynamic_v3_rescue"
    assert report["completed_count"] > 0
    assert report["artifact_size_summary"]["file_count"] > 0


def test_medium_real_validation_allows_real_manual_resume_without_profile(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    manifest_path = sweep["sweep_dir"] / "sweep_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("profile", None)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    validation = validate_medium_real_sweep(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        min_expected_candidates=1,
    )

    assert validation["status"] == "PASS"
