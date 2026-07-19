from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import (
    run_candidate_quality_filter_design_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.etf_portfolio import dynamic_v3_signal_filter_foundation as signal_filter
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as legacy
from ai_trading_system.interfaces.cli.etf_portfolio import etf_app
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


@pytest.fixture(scope="module")
def foundation_fixture(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("signal_filter_foundation")
    with artifact_validation_session():
        yield {"root": root, **run_candidate_quality_filter_design_fixture(root)}


def _artifact_cases(
    fixture: dict[str, Any],
) -> list[tuple[str, Path, str, Callable[..., dict[str, Any]], str, Path, tuple[str, ...]]]:
    root = fixture["root"]
    return [
        (
            "taxonomy",
            Path(fixture["signal_failure_taxonomy"]["taxonomy_dir"]),
            fixture["signal_failure_taxonomy"]["taxonomy_id"],
            signal_filter.validate_signal_failure_taxonomy_artifact,
            "taxonomy_id",
            root / "signal_failure_taxonomy",
            signal_filter.TAXONOMY_VIEWS,
        ),
        (
            "ledger",
            Path(fixture["candidate_signal_ledger"]["ledger_dir"]),
            fixture["candidate_signal_ledger"]["ledger_id"],
            signal_filter.validate_candidate_signal_ledger_artifact,
            "ledger_id",
            root / "candidate_signal_ledger",
            signal_filter.LEDGER_VIEWS,
        ),
        (
            "churn",
            Path(fixture["signal_churn_root_cause"]["root_cause_dir"]),
            fixture["signal_churn_root_cause"]["root_cause_id"],
            signal_filter.validate_signal_churn_root_cause_artifact,
            "root_cause_id",
            root / "signal_churn_root_cause",
            signal_filter.CHURN_VIEWS,
        ),
        (
            "mismatch",
            Path(fixture["regime_mismatch_attribution"]["mismatch_dir"]),
            fixture["regime_mismatch_attribution"]["mismatch_id"],
            signal_filter.validate_regime_mismatch_attribution_artifact,
            "mismatch_id",
            root / "regime_mismatch_attribution",
            signal_filter.MISMATCH_VIEWS,
        ),
        (
            "filter",
            Path(fixture["candidate_quality_filter_design"]["filter_design_dir"]),
            fixture["candidate_quality_filter_design"]["filter_design_id"],
            signal_filter.validate_candidate_quality_filter_design_artifact,
            "filter_design_id",
            root / "candidate_quality_filter_design",
            signal_filter.FILTER_VIEWS,
        ),
    ]


def _validate_case(
    validator: Callable[..., dict[str, Any]],
    id_key: str,
    artifact_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    return validator(**{id_key: artifact_id, "output_dir": output_dir})


def test_signal_filter_foundation_preserves_missing_evidence(
    foundation_fixture: dict[str, Any],
) -> None:
    ledger = foundation_fixture["candidate_signal_ledger"]
    churn = foundation_fixture["signal_churn_root_cause"]
    mismatch = foundation_fixture["regime_mismatch_attribution"]
    design = foundation_fixture["candidate_quality_filter_design"]

    summary = ledger["candidate_signal_summary"]
    assert ledger["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert ledger["signal_events"] == []
    assert summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert summary["event_count"] == 0
    assert summary["dominant_failure_mode"] is None
    assert summary["unstable_method_count"] is None
    assert all(row["event_count"] is None for row in summary["methods"])

    assert churn["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert churn["churn_root_cause_summary"]["dominant_root_cause"] is None
    assert churn["churn_root_cause_summary"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert churn["churn_mitigation_candidates"]["mitigations"] == []

    assert mismatch["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert mismatch["regime_mismatch_events"] == []
    assert mismatch["regime_mismatch_summary"]["dominant_mismatch_type"] is None
    assert mismatch["regime_mismatch_summary"]["evidence_status"] == "INSUFFICIENT_DATA"

    assert design["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert design["proposed_quality_filters"]["filters"] == []
    assert design["proposed_quality_filters"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert (
        design["filter_design_config"]["method"]["automatic_filter_implementation_allowed"] is False
    )


def test_signal_filter_foundation_snapshots_bind_exact_lineage(
    foundation_fixture: dict[str, Any],
) -> None:
    cases = {case[0]: case for case in _artifact_cases(foundation_fixture)}
    snapshots = {
        "taxonomy": json.loads(
            (cases["taxonomy"][1] / "signal_failure_taxonomy_input_snapshot.json").read_bytes()
        ),
        "ledger": json.loads(
            (cases["ledger"][1] / "candidate_signal_ledger_input_snapshot.json").read_bytes()
        ),
        "churn": json.loads(
            (cases["churn"][1] / "signal_churn_root_cause_input_snapshot.json").read_bytes()
        ),
        "mismatch": json.loads(
            (cases["mismatch"][1] / "regime_mismatch_attribution_input_snapshot.json").read_bytes()
        ),
        "filter": json.loads(
            (
                cases["filter"][1] / "candidate_quality_filter_design_input_snapshot.json"
            ).read_bytes()
        ),
    }
    assert snapshots["taxonomy"]["schema_version"] == signal_filter.TAXONOMY_INPUT_SCHEMA
    assert snapshots["ledger"]["schema_version"] == signal_filter.LEDGER_INPUT_SCHEMA
    assert snapshots["churn"]["schema_version"] == signal_filter.CHURN_INPUT_SCHEMA
    assert snapshots["mismatch"]["schema_version"] == signal_filter.MISMATCH_INPUT_SCHEMA
    assert snapshots["filter"]["schema_version"] == signal_filter.FILTER_INPUT_SCHEMA
    assert snapshots["ledger"]["taxonomy_source"]["artifact_id"] == cases["taxonomy"][2]
    assert snapshots["churn"]["ledger_source"]["artifact_id"] == cases["ledger"][2]
    assert snapshots["mismatch"]["ledger_source"]["artifact_id"] == cases["ledger"][2]
    assert snapshots["filter"]["root_cause_source"]["artifact_id"] == cases["churn"][2]
    assert snapshots["filter"]["mismatch_source"]["artifact_id"] == cases["mismatch"][2]
    assert len({item["policy_source"]["sha256"] for item in snapshots.values()}) == 1


def test_signal_filter_foundation_rebuilds_every_canonical_view(
    foundation_fixture: dict[str, Any],
) -> None:
    for _, _, artifact_id, validator, id_key, output_dir, _ in _artifact_cases(foundation_fixture):
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "PASS", validation


@pytest.mark.parametrize(
    ("case_name", "view_name"),
    [
        ("taxonomy", "normalized_signal_failure_taxonomy.yaml"),
        ("ledger", "signal_events.jsonl"),
        ("churn", "churn_root_cause_summary.json"),
        ("mismatch", "regime_mismatch_report.md"),
        ("filter", "reader_brief_section.md"),
    ],
)
def test_signal_filter_foundation_rejects_output_tamper(
    foundation_fixture: dict[str, Any], case_name: str, view_name: str
) -> None:
    case = {item[0]: item for item in _artifact_cases(foundation_fixture)}[case_name]
    _, artifact_root, artifact_id, validator, id_key, output_dir, _ = case
    view = artifact_root / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(original + b"\nTAMPER")
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


def test_signal_filter_foundation_rejects_live_source_tamper(
    foundation_fixture: dict[str, Any],
) -> None:
    cases = {item[0]: item for item in _artifact_cases(foundation_fixture)}
    ledger_summary = cases["ledger"][1] / "candidate_signal_summary.json"
    original = ledger_summary.read_bytes()
    try:
        ledger_summary.write_bytes(original + b"\n")
        churn = cases["churn"]
        validation = _validate_case(churn[3], churn[4], churn[2], churn[5])
        assert validation["status"] == "FAIL"
    finally:
        ledger_summary.write_bytes(original)


def test_signal_filter_foundation_rejects_cross_lineage_tamper(
    foundation_fixture: dict[str, Any],
) -> None:
    case = {item[0]: item for item in _artifact_cases(foundation_fixture)}["filter"]
    snapshot_path = case[1] / "candidate_quality_filter_design_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["source_ledger_id"] = "wrong-ledger"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(case[3], case[4], case[2], case[5])
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_signal_filter_foundation_rejects_chronology_tamper(
    foundation_fixture: dict[str, Any],
) -> None:
    case = {item[0]: item for item in _artifact_cases(foundation_fixture)}["ledger"]
    snapshot_path = case[1] / "candidate_signal_ledger_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["generated_at"] = datetime(2000, 1, 1, tzinfo=UTC).isoformat()
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(case[3], case[4], case[2], case[5])
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_signal_filter_legacy_api_and_cli_use_canonical_owner(
    foundation_fixture: dict[str, Any],
) -> None:
    taxonomy = foundation_fixture["signal_failure_taxonomy"]
    output_dir = foundation_fixture["root"] / "signal_failure_taxonomy"
    canonical = signal_filter.signal_failure_taxonomy_report_payload(
        taxonomy_id=taxonomy["taxonomy_id"], output_dir=output_dir
    )
    compatibility = legacy.signal_failure_taxonomy_report_payload(
        taxonomy_id=taxonomy["taxonomy_id"], output_dir=output_dir
    )
    assert compatibility == canonical

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "signal-failure-taxonomy",
            "report",
            "--taxonomy-id",
            taxonomy["taxonomy_id"],
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"taxonomy_id={taxonomy['taxonomy_id']}" in result.output
