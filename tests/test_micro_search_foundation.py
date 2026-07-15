from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_micro_search_foundation_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_micro_search_foundation as micro
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as legacy
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


@pytest.fixture(scope="module")
def foundation_fixture(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("micro_search_foundation")
    with artifact_validation_session():
        yield {"root": root, **run_micro_search_foundation_fixture(root)}


def _artifact_cases(
    fixture: dict[str, Any],
) -> list[tuple[str, Path, str, Callable[..., dict[str, Any]], str, Path]]:
    root = fixture["root"]
    return [
        (
            "design",
            fixture["v4_design"]["v4_design_dir"],
            fixture["v4_design"]["v4_design_id"],
            micro.validate_micro_search_v4_design_artifact,
            "v4_design_id",
            root / "micro_search_v4_design",
        ),
        (
            "backfill",
            fixture["v4_backfill"]["v4_backfill_dir"],
            fixture["v4_backfill"]["v4_backfill_id"],
            micro.validate_micro_search_v4_backfill_artifact,
            "v4_backfill_id",
            root / "micro_search_v4_backfill",
        ),
        (
            "gate",
            fixture["gate_review"]["gate_review_dir"],
            fixture["gate_review"]["gate_review_id"],
            micro.validate_gate_calibrated_review_artifact,
            "gate_review_id",
            root / "gate_calibrated_review",
        ),
        (
            "attribution",
            fixture["signal_vs_parameter"]["attribution_dir"],
            fixture["signal_vs_parameter"]["signal_vs_parameter_id"],
            micro.validate_signal_vs_parameter_attribution_artifact,
            "attribution_id",
            root / "signal_vs_parameter_attribution",
        ),
    ]


def _validate_case(
    validator: Callable[..., dict[str, Any]],
    id_key: str,
    artifact_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    return validator(**{id_key: artifact_id, "output_dir": output_dir})


def test_micro_search_foundation_business_contract(
    foundation_fixture: dict[str, Any],
) -> None:
    design = foundation_fixture["v4_design"]
    backfill = foundation_fixture["v4_backfill"]
    gate = foundation_fixture["gate_review"]
    attribution = foundation_fixture["signal_vs_parameter"]
    variants = design["v4_variant_specs"]
    variant_ids = {row["variant_id"] for row in variants}

    assert design["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert design["manifest"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert 20 <= design["manifest"]["variant_count"] <= 40
    assert "smooth_3d_plus_dispersion_gate" in variant_ids
    assert "median_consensus_plus_smooth_3d" in variant_ids
    assert {row["evidence_role"] for row in variants} == {"PILOT_HYPOTHESIS_ONLY"}

    assert backfill["manifest"]["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert backfill["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert backfill["v4_backfill_progress"]["variants_completed"] > 0
    assert backfill["v4_variant_signal_metrics"]
    assert backfill["manifest"]["calculation_cache_role"] == "HISTORICAL_WINDOW_INPUT"
    assert backfill["manifest"]["data_quality_cache_role"] == "CURRENT_QUALITY_EVIDENCE"

    summary = gate["gate_calibrated_summary"]
    assert gate["manifest"]["status"] == "PASS"
    assert gate["official_gate_results"]
    assert gate["diagnostic_gate_results"]
    assert summary["official_research_score_min"] == 0.72
    assert summary["diagnostic_score_min"] == pytest.approx(0.67)
    assert summary["gate_policy_change_recommended"] is False
    assert summary["official_gate_changed"] is False

    failure = attribution["failure_source_attribution"]
    shift = attribution["recommended_research_shift"]
    assert attribution["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert failure["failure_source"] == "INCONCLUSIVE"
    assert failure["confidence"] == "LOW"
    assert failure["market_regime_failure_claimed"] is False
    assert shift["recommended_shift"] == "DEFER_AND_BUILD_DATED_EVIDENCE"
    assert "Signal vs Parameter Attribution" in attribution["reader_brief_section"]
    assert all(
        artifact["manifest"]["broker_action_allowed"] is False
        for artifact in (design, backfill, gate, attribution)
    )


def test_micro_search_foundation_v2_snapshots_bind_one_lineage(
    foundation_fixture: dict[str, Any],
) -> None:
    design_root = Path(foundation_fixture["v4_design"]["v4_design_dir"])
    backfill_root = Path(foundation_fixture["v4_backfill"]["v4_backfill_dir"])
    gate_root = Path(foundation_fixture["gate_review"]["gate_review_dir"])
    attribution_root = Path(foundation_fixture["signal_vs_parameter"]["attribution_dir"])
    design = json.loads((design_root / "micro_search_v4_design_input_snapshot.json").read_bytes())
    backfill = json.loads(
        (backfill_root / "micro_search_v4_backfill_input_snapshot.json").read_bytes()
    )
    gate = json.loads((gate_root / "gate_calibrated_review_input_snapshot.json").read_bytes())
    attribution = json.loads(
        (attribution_root / "signal_vs_parameter_attribution_input_snapshot.json").read_bytes()
    )

    assert design["schema_version"] == micro.DESIGN_INPUT_SCHEMA
    assert backfill["schema_version"] == micro.BACKFILL_INPUT_SCHEMA
    assert gate["schema_version"] == micro.GATE_REVIEW_INPUT_SCHEMA
    assert attribution["schema_version"] == micro.ATTRIBUTION_INPUT_SCHEMA
    assert backfill["design_source"]["artifact_id"] == design["v4_design_id"]
    assert gate["backfill_source"]["artifact_id"] == backfill["v4_backfill_id"]
    assert gate["design_source"]["artifact_id"] == design["v4_design_id"]
    assert attribution["gate_review_source"]["artifact_id"] == gate["gate_review_id"]
    policy_hashes = {
        item["policy_source"]["sha256"] for item in (design, backfill, gate, attribution)
    }
    assert len(policy_hashes) == 1
    assert backfill["calculation_price_source"] == backfill["data_quality_price_source"]


def test_micro_search_foundation_rebuilds_all_canonical_bytes(
    foundation_fixture: dict[str, Any],
) -> None:
    for _, _, artifact_id, validator, id_key, output_dir in _artifact_cases(
        foundation_fixture
    ):
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "PASS", validation


@pytest.mark.parametrize(
    ("case_name", "view_name"),
    [
        ("design", "micro_search_v4_design_report.md"),
        ("backfill", "micro_search_v4_backfill_report.md"),
        ("gate", "gate_calibrated_review_report.md"),
        ("attribution", "reader_brief_section.md"),
    ],
)
def test_micro_search_foundation_rejects_output_tamper(
    foundation_fixture: dict[str, Any], case_name: str, view_name: str
) -> None:
    cases = {case[0]: case for case in _artifact_cases(foundation_fixture)}
    _, artifact_root, artifact_id, validator, id_key, output_dir = cases[case_name]
    view = artifact_root / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(original + b"\nTAMPER")
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


@pytest.mark.parametrize(
    ("case_name", "snapshot_name"),
    [
        ("design", "micro_search_v4_design_input_snapshot.json"),
        ("backfill", "micro_search_v4_backfill_input_snapshot.json"),
        ("gate", "gate_calibrated_review_input_snapshot.json"),
        ("attribution", "signal_vs_parameter_attribution_input_snapshot.json"),
    ],
)
def test_micro_search_foundation_rejects_policy_binding_tamper(
    foundation_fixture: dict[str, Any], case_name: str, snapshot_name: str
) -> None:
    cases = {case[0]: case for case in _artifact_cases(foundation_fixture)}
    _, artifact_root, artifact_id, validator, id_key, output_dir = cases[case_name]
    snapshot_path = artifact_root / snapshot_name
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["policy_source"]["sha256"] = "0" * 64
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_gate_review_rejects_cross_artifact_lineage_tamper(
    foundation_fixture: dict[str, Any],
) -> None:
    cases = {case[0]: case for case in _artifact_cases(foundation_fixture)}
    _, artifact_root, artifact_id, validator, id_key, output_dir = cases["gate"]
    snapshot_path = artifact_root / "gate_calibrated_review_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["design_source"]["artifact_id"] = "wrong-design-id"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_legacy_public_api_forwards_to_canonical_reader(
    foundation_fixture: dict[str, Any],
) -> None:
    design = foundation_fixture["v4_design"]
    output_dir = foundation_fixture["root"] / "micro_search_v4_design"
    canonical = micro.micro_search_v4_design_report_payload(
        v4_design_id=design["v4_design_id"], output_dir=output_dir
    )
    compatibility = legacy.micro_search_v4_design_report_payload(
        v4_design_id=design["v4_design_id"], output_dir=output_dir
    )
    assert compatibility == canonical
