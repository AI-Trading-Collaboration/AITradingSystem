from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_authorization_admission as admission_v4,
)
from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_execution_evidence as evidence,
)

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2522_primary_window_daily_slice_revalidation_execution_v1"
)
def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _isolated_root(tmp_path: Path) -> Path:
    target = tmp_path / evidence.PACKAGE_RELATIVE_PATH
    target.parent.mkdir(parents=True)
    shutil.copytree(PACKAGE, target)
    return tmp_path


def test_actual_result_builds_canonical_typed_failure_package() -> None:
    loaded = evidence.load_daily_slice_execution_evidence_package(project_root=ROOT)
    receipt = loaded.failure_receipt

    assert receipt.backtest_id == "60ce7e0bec3ad2d83a4d1341e0221492"
    assert receipt.backtest_name == "Logical Red Bison"
    assert receipt.result_file_sha256 == evidence.RESULT_FILE_SHA256
    assert receipt.strict_admission_status == "FAIL"
    assert receipt.strict_parser_reason_code == "DAILY_SLICE_RESULT_PARSER_REJECTED"
    assert receipt.typed_failure_reason == (
        "DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS"
    )
    assert receipt.failure_axis_resolution == "UNRESOLVED_REQUIRES_TARGETED_DIAGNOSTIC"
    assert loaded.manifest.failure_receipt_content_sha256 == receipt.content_sha256


def test_result_identity_scope_and_cash_preservation_are_exact() -> None:
    loaded = evidence.load_daily_slice_execution_evidence_package(project_root=ROOT)
    receipt = loaded.failure_receipt

    assert _sha256(PACKAGE / "result.json") == (
        "45e8647f4d4b0e3590252acedacca4235695341574f44bc593d8ab9b283f603e"
    )
    assert (PACKAGE / "result.json").stat().st_size == 813_386
    assert receipt.requested_start.isoformat() == "2021-02-22"
    assert receipt.requested_end.isoformat() == "2025-12-02"
    assert receipt.expected_session_count == 1202
    assert receipt.orders == receipt.fills == 0
    assert receipt.total_fees == "$0.00"
    assert receipt.start_equity == receipt.end_equity == "100000"
    assert receipt.portfolio_invested is False


def test_daily_slice_diagnostic_does_not_overclaim_the_failed_sub_axis() -> None:
    receipt = evidence.load_daily_slice_execution_evidence_package(
        project_root=ROOT
    ).failure_receipt

    assert receipt.observed_session_count == 0
    assert receipt.invalid_session_count == 1202
    assert receipt.daily_slice_chain_session_count == 1201
    assert receipt.valid_candidate_session_count == 0
    assert receipt.transport_rejected_session_count == 1201
    assert receipt.chart_present is True
    assert receipt.chart_series_count == 0
    assert "quote" not in receipt.typed_failure_reason.lower()
    assert "greek" not in receipt.typed_failure_reason.lower()


def test_external_lifecycle_pass_does_not_turn_evidence_or_dq_into_pass() -> None:
    loaded = evidence.load_daily_slice_execution_evidence_package(project_root=ROOT)
    receipt = loaded.failure_receipt

    assert loaded.external_action_ledger.lifecycle_status == "COMPLETE"
    assert loaded.external_action_ledger.scope_status == "PASS"
    assert loaded.external_action_ledger.attempted_project_mutations == 1
    assert loaded.external_action_ledger.attempted_cloud_backtests == 1
    assert loaded.external_action_ledger.completed_result_downloads == 1
    assert loaded.manifest.evidence_admission_status == "FAIL"
    assert receipt.local_derived_aggregate_dq_status == "NOT_EVALUATED"
    assert receipt.local_derived_aggregate_pit_status == "NOT_EVALUATED"
    assert receipt.option_event_dq_status == "NOT_EVALUATED"
    assert receipt.option_event_pit_status == "NOT_EVALUATED"
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert receipt.selection_authorized is False


def test_first_attempt_consumes_v4_and_blocks_a_second_run() -> None:
    loaded = evidence.load_daily_slice_execution_evidence_package(project_root=ROOT)
    receipt = loaded.run_attempt_consumption

    assert receipt.authorization_consumed is True
    assert receipt.authorization_invalidated_for_further_cloud_runs is True
    assert loaded.failure_receipt.further_cloud_run_authorized is False
    with pytest.raises(
        admission_v4.QCQQQOptionsDailySliceAuthorizationAdmissionError,
        match="OWNER_V4_AUTHORIZATION_ALREADY_CONSUMED",
    ):
        admission_v4.build_qc_qqq_options_daily_slice_run_attempt_consumption(
            consumption_id="trading-2522-prohibited-second-run",
            recorded_at_utc=evidence.OBSERVED_AT_UTC,
            admitted_authorization=loaded.admitted_authorization,
            external_action_ledger=loaded.run_attempt_ledger,
            prior_consumption_receipts=(receipt,),
        )


def test_owner_token_and_all_package_artifacts_are_exactly_bound() -> None:
    loaded = evidence.load_daily_slice_execution_evidence_package(project_root=ROOT)

    assert _sha256(PACKAGE / "owner_decision.txt") == (
        "f37e778a8f8c71e126efe622ef7d3f659af944164f7c97d82269125fa663e197"
    )
    assert loaded.failure_receipt.owner_decision_content_sha256 == (
        "d62b681d2fafdea939f30278ae2dca39ab28048973868faa0301c650ea00fcd0"
    )
    assert tuple(item.relative_path for item in loaded.manifest.artifacts) == (
        "external_action_ledger.json",
        "failure_receipt.json",
        "owner_decision.txt",
        "result.json",
        "run_attempt_consumption_receipt.json",
        "run_attempt_ledger.json",
    )
    for artifact in loaded.manifest.artifacts:
        path = PACKAGE / artifact.relative_path
        assert path.stat().st_size == artifact.byte_count
        assert _sha256(path) == artifact.sha256


@pytest.mark.parametrize(
    "file_name",
    (
        "external_action_ledger.json",
        "failure_receipt.json",
        "owner_decision.txt",
        "result.json",
        "run_attempt_consumption_receipt.json",
        "run_attempt_ledger.json",
    ),
)
def test_any_evidence_artifact_tamper_fails_closed(tmp_path: Path, file_name: str) -> None:
    root = _isolated_root(tmp_path)
    path = root / evidence.PACKAGE_RELATIVE_PATH / file_name
    path.write_bytes(path.read_bytes() + b"\n")

    with pytest.raises((ValidationError, ValueError)):
        evidence.load_daily_slice_execution_evidence_package(project_root=root)


def test_extra_package_file_fails_exact_inventory(tmp_path: Path) -> None:
    root = _isolated_root(tmp_path)
    package = root / evidence.PACKAGE_RELATIVE_PATH
    (package / "unreviewed.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="inventory"):
        evidence.load_daily_slice_execution_evidence_package(project_root=root)


def test_forged_pass_failure_receipt_is_rejected() -> None:
    payload = json.loads((PACKAGE / "failure_receipt.json").read_bytes())
    payload["strict_admission_status"] = "PASS"
    semantic = {key: value for key, value in payload.items() if key != "content_sha256"}
    payload["content_sha256"] = hashlib.sha256(
        evidence._canonical_json_bytes(semantic)  # noqa: SLF001
    ).hexdigest()
    forged = evidence._canonical_json_bytes(payload)  # noqa: SLF001

    with pytest.raises(ValidationError):
        evidence.DailySliceExecutionFailureReceipt.from_json_bytes(forged)


def test_noncanonical_or_duplicate_failure_receipt_is_rejected() -> None:
    raw = (PACKAGE / "failure_receipt.json").read_bytes()

    with pytest.raises(ValueError, match="canonical"):
        evidence.DailySliceExecutionFailureReceipt.from_json_bytes(raw.rstrip())
    duplicated = b'{"schema_version":"duplicate",' + raw[1:]
    with pytest.raises(ValueError, match="duplicate"):
        evidence.DailySliceExecutionFailureReceipt.from_json_bytes(duplicated)


def test_platform_identity_is_disclosed_as_ui_observation_not_result_fact() -> None:
    receipt = evidence.load_daily_slice_execution_evidence_package(
        project_root=ROOT
    ).failure_receipt

    assert receipt.build_id == "2095dc-5e494a"
    assert receipt.engine_version == "2.5.0.0.18004"
    assert receipt.host_class == "Community B-MICRO"
    assert receipt.platform_identity_source == "CODEX_SIGNED_IN_QC_RESULTS_UI_OBSERVATION"
    assert receipt.investment_interpretation_generated is False
    assert receipt.production_effect == "none"
    assert receipt.broker_action == "none"
