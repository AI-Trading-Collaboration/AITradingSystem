from __future__ import annotations

import json
import shutil
from hashlib import sha256
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research.session_finalization_dq_pit_evidence_admission import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_POLICY_PATH,
    SessionFinalizationDQPITAdmissionError,
    build_dq_pit_evidence_admission,
    validate_dq_pit_evidence_admission_package,
    verify_retained_raw_results,
    write_dq_pit_evidence_admission_package,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = Path(
    "inputs/research/qqq_options/"
    "trading_2532_session_finalization_v2_external_validation_execution_v1"
)
DQ_POLICY_PATH = Path("config/research/qqq_options_dq_pit_identity_v1.yaml")
EXPECTED_CHECK_IDS = tuple(
    sorted(
        (
            "cache_identity",
            "chain_presence",
            "engine_identity",
            "evidence_identity",
            "exchange_calendar_identity",
            "fill_forward_ambiguity",
            "local_cache_dq_scope_separation",
            "open_interest_freshness",
            "order_fill_chronology",
            "prior_day_model_freshness",
            "provider_raw_checksum",
            "quote_freshness",
            "quote_integrity",
            "signal_selection_chronology",
            "symbol_mapping_identity",
        )
    )
)


def _canonical(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _checks(report: object) -> dict[str, dict[str, object]]:
    assert isinstance(report, dict)
    rows = report["required_checks"]
    assert isinstance(rows, list)
    return {str(row["check_id"]): row for row in rows}


def _copy_authority(tmp_path: Path) -> Path:
    for relative in (DEFAULT_POLICY_PATH, DQ_POLICY_PATH):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / relative, target)
    shutil.copytree(PROJECT_ROOT / SOURCE_ROOT, tmp_path / SOURCE_ROOT)
    return tmp_path


def test_build_derives_fail_closed_15_check_admission() -> None:
    built = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT)
    report = built.report
    checks = _checks(report)

    assert tuple(checks) == EXPECTED_CHECK_IDS
    assert report["coverage_summary"] == {
        "required_check_count": 15,
        "pass_count": 1,
        "fail_count": 1,
        "not_evaluated_count": 13,
    }
    assert checks["local_cache_dq_scope_separation"]["status"] == "PASS"
    assert checks["chain_presence"]["status"] == "FAIL"
    assert checks["chain_presence"]["reason_code"] == "CHAIN_MISSING"
    assert checks["quote_integrity"]["status"] == "NOT_EVALUATED"
    assert report["decision"] == {
        "admission_status": "BLOCKED_INSUFFICIENT_CANONICAL_DQ_PIT_EVIDENCE",
        "dq_status": "FAIL",
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "investment_conclusion_authorized": False,
        "pit_status": "NOT_EVALUATED",
        "selection_status": "POLICY_BLOCKED_CASH_PRESERVATION",
    }


def test_transport_facts_remain_exact_and_do_not_authorize_trading() -> None:
    report = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT).report
    assert report["transport_facts"] == {
        "chain_present_sessions": 1201,
        "final_never_chain_sessions": 1,
        "recovered_after_chainless_sessions": 1019,
        "orders": 0,
        "fills": 0,
    }
    safety = report["safety"]
    for field in (
        "external_action_authorized",
        "cloud_run_authorized",
        "raw_option_rows_authorized",
        "object_store_authorized",
        "selection_authorized",
        "investment_conclusion_authorized",
    ):
        assert safety[field] is False
    assert safety["maximum_orders"] == 0
    assert safety["maximum_fills"] == 0


def test_manual_results_hash_does_not_substitute_for_provider_raw_checksum() -> None:
    report = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT).report
    checks = _checks(report)
    provider = checks["provider_raw_checksum"]
    assert provider["status"] == "NOT_EVALUATED"
    assert provider["reason_code"] == "PROVIDER_RAW_CHECKSUM_UNAVAILABLE"
    assert "not a substitute" in str(provider["missing_requirement"])
    verification = report["source_identity"]["raw_results_verification"]
    assert verification["verification_scope"] == (
        "WHOLE_RESULTS_BYTES_ONLY_NO_RAW_OPTION_ROW_EXTRACTION"
    )


def test_all_pit_checks_remain_not_evaluated() -> None:
    report = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT).report
    checks = _checks(report)
    pit_ids = {
        "exchange_calendar_identity",
        "fill_forward_ambiguity",
        "open_interest_freshness",
        "order_fill_chronology",
        "prior_day_model_freshness",
        "quote_freshness",
        "signal_selection_chronology",
        "symbol_mapping_identity",
    }
    assert {checks[check_id]["status"] for check_id in pit_ids} == {"NOT_EVALUATED"}
    assert report["decision"]["pit_status"] == "NOT_EVALUATED"


def test_build_is_deterministic() -> None:
    first = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT)
    second = build_dq_pit_evidence_admission(project_root=PROJECT_ROOT)
    assert first == second
    assert _canonical(first.report) == _canonical(second.report)
    assert _canonical(first.manifest) == _canonical(second.manifest)


def test_write_and_validate_package_without_local_raw_dependency(tmp_path: Path) -> None:
    output = tmp_path / "package"
    written = write_dq_pit_evidence_admission_package(output, project_root=PROJECT_ROOT)
    validated = validate_dq_pit_evidence_admission_package(output, project_root=PROJECT_ROOT)
    assert written == validated
    assert sorted(path.name for path in output.iterdir()) == [
        "dq_pit_evidence_admission.json",
        "package_manifest.json",
    ]


def test_manifest_binds_exact_report_bytes(tmp_path: Path) -> None:
    output = tmp_path / "package"
    built = write_dq_pit_evidence_admission_package(output, project_root=PROJECT_ROOT)
    report_bytes = (output / "dq_pit_evidence_admission.json").read_bytes()
    binding = built.manifest["artifacts"]["dq_pit_evidence_admission.json"]
    assert binding["byte_count"] == len(report_bytes)
    assert binding["file_sha256"] == sha256(report_bytes).hexdigest()
    assert binding["content_sha256"] == built.report["content_sha256"]


def test_report_tamper_fails_closed(tmp_path: Path) -> None:
    output = tmp_path / "package"
    write_dq_pit_evidence_admission_package(output, project_root=PROJECT_ROOT)
    report_path = output / "dq_pit_evidence_admission.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["decision"]["dq_status"] = "PASS"
    report_path.write_bytes(_canonical(payload))
    with pytest.raises(
        SessionFinalizationDQPITAdmissionError,
        match="TRADING_2533_CONTENT_SEAL_INVALID",
    ):
        validate_dq_pit_evidence_admission_package(output, project_root=PROJECT_ROOT)


def test_source_artifact_drift_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path / "project")
    evidence_path = root / SOURCE_ROOT / "export_safe_aggregate_evidence.json"
    evidence_path.write_bytes(evidence_path.read_bytes() + b" ")
    with pytest.raises(
        SessionFinalizationDQPITAdmissionError,
        match="TRADING_2533_SOURCE_ARTIFACT_DRIFT",
    ):
        build_dq_pit_evidence_admission(project_root=root)


def test_required_check_policy_drift_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path / "project")
    policy_path = root / DEFAULT_POLICY_PATH
    text = policy_path.read_text(encoding="utf-8")
    policy_path.write_text(
        text.replace("    - cache_identity\n", "    - invented_check\n", 1),
        encoding="utf-8",
    )
    with pytest.raises(
        SessionFinalizationDQPITAdmissionError,
        match="TRADING_2533_REQUIRED_CHECK_SET_DRIFT",
    ):
        build_dq_pit_evidence_admission(project_root=root)


def test_retained_raw_results_verification_uses_whole_file_only(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path / "project")
    raw = b'{"aggregate_only":true}\n'
    raw_path = tmp_path / "results.json"
    raw_path.write_bytes(raw)
    policy_path = root / DEFAULT_POLICY_PATH
    policy_text = policy_path.read_text(encoding="utf-8")
    policy_text = policy_text.replace(
        "raw_results_byte_count: 814999", f"raw_results_byte_count: {len(raw)}"
    )
    policy_text = policy_text.replace(
        "raw_results_sha256: 5d3220342c96217f2c4a4d624b0dc7fbbcad98427de728e749dc2e4f3168d50d",
        f"raw_results_sha256: {sha256(raw).hexdigest()}",
    )
    policy_path.write_text(policy_text, encoding="utf-8")

    verification = verify_retained_raw_results(raw_path, project_root=root)
    assert verification["status"] == "PASS"
    assert verification["byte_count"] == len(raw)
    assert verification["sha256"] == sha256(raw).hexdigest()


def test_retained_raw_results_mismatch_fails_closed(tmp_path: Path) -> None:
    raw_path = tmp_path / "wrong.json"
    raw_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(
        SessionFinalizationDQPITAdmissionError,
        match="TRADING_2533_RAW_RESULTS_IDENTITY_MISMATCH",
    ):
        verify_retained_raw_results(raw_path, project_root=PROJECT_ROOT)


def test_canonical_repository_package_is_fresh() -> None:
    validated = validate_dq_pit_evidence_admission_package(
        PROJECT_ROOT / DEFAULT_OUTPUT_ROOT,
        project_root=PROJECT_ROOT,
    )
    assert validated.report["decision"]["dq_status"] == "FAIL"
    assert validated.report["decision"]["pit_status"] == "NOT_EVALUATED"
