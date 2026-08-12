from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQReportRecord,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionEvidenceClass,
)
from ai_trading_system.qqq_options_research.primary_window_policy_calibration import (
    CalibrationAggregateStatistic,
    CalibrationEvidenceReference,
    CalibrationReadinessStatus,
    CalibrationSlotEvidenceStatus,
    QQQOptionsPrimaryWindowCalibrationContractError,
    QQQOptionsPrimaryWindowCalibrationEvidenceRecord,
    build_qqq_options_primary_window_calibration_evaluation,
    load_qqq_options_primary_window_calibration_policy,
    resolve_qqq_options_primary_window_calibration_evaluation,
)

REPOSITORY_SHA = "a" * 40
SOURCE_ID = "qc.qqq.options.daily.derived"
SOURCE_SHA = "b" * 64
DQ_POLICY_SHA = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
PRIMARY_START = date(2021, 2, 22)
PRIMARY_END = date(2021, 2, 26)
SESSIONS = (
    date(2021, 2, 22),
    date(2021, 2, 23),
    date(2021, 2, 24),
    date(2021, 2, 25),
    date(2021, 2, 26),
)
ISSUED_AT = datetime(2026, 8, 12, 2, tzinfo=UTC)
DQ_GENERATED_AT = datetime(2026, 8, 12, 0, tzinfo=UTC)
EVIDENCE_CREATED_AT = datetime(2026, 8, 12, 1, tzinfo=UTC)
REQUIRED_DQ_CHECK_IDS = (
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


def _safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def _dq_report(
    *,
    slot_id: str,
    dq_status: str = "PASS",
    pit_status: str = "PASS",
    scope: str = "qqq_options_event_dq_pit_identity",
    requested_end: date = PRIMARY_END,
    evaluated_end: date = PRIMARY_END,
    source_id: str = SOURCE_ID,
) -> DQReportRecord:
    checks = []
    for index, check_id in enumerate(REQUIRED_DQ_CHECK_IDS):
        status = dq_status if index == 0 and dq_status != "PASS" else "PASS"
        checks.append(
            DQCheckResult(
                check_id=check_id,
                status=status,
                reason_code=None if status == "PASS" else "CALIBRATION_DQ_NOT_PASS",
                observed_at_utc=DQ_GENERATED_AT,
            )
        )
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id=f"dq-{slot_id.lower()}",
        record_id=f"dq-{slot_id.lower()}-record",
        created_at_utc=DQ_GENERATED_AT,
        producer_version="trading-2510-test-v1",
        repository_code_sha=REPOSITORY_SHA,
        policy_id="qqq_options_dq_pit_identity_v1",
        policy_version="1.0.0",
        policy_sha256=DQ_POLICY_SHA,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=(source_id,),
        source_checksums=(SOURCE_SHA,),
        requested_start=PRIMARY_START,
        requested_end=requested_end,
        evaluated_start=PRIMARY_START,
        evaluated_end=evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=f"dq-{slot_id.lower()}-lineage",
        safety=_safety(),
        scope=scope,
        report_version="1.0.0",
        generated_at_utc=DQ_GENERATED_AT,
        checks=tuple(checks),
    )


def _evidence_record(
    *,
    slot_id: str,
    evidence_class: OwnerDecisionEvidenceClass,
    repository_sha: str = REPOSITORY_SHA,
    requested_start: date = PRIMARY_START,
    evaluated_start: date = PRIMARY_START,
    as_of_session: date = PRIMARY_END,
    source_checksum: str = SOURCE_SHA,
    evidence_class_override: OwnerDecisionEvidenceClass | None = None,
) -> QQQOptionsPrimaryWindowCalibrationEvidenceRecord:
    return QQQOptionsPrimaryWindowCalibrationEvidenceRecord.seal(
        schema_version="qqq_options_primary_window_calibration_evidence.v1",
        record_id=f"evidence-{slot_id.lower()}",
        created_at_utc=EVIDENCE_CREATED_AT,
        repository_code_sha=repository_sha,
        slot_id=slot_id,
        evidence_class=evidence_class_override or evidence_class,
        metric_definition_id=f"metric-{slot_id.lower()}",
        metric_definition_sha256=hashlib.sha256(slot_id.encode()).hexdigest(),
        requested_start=requested_start,
        requested_end=PRIMARY_END,
        evaluated_start=evaluated_start,
        evaluated_end=PRIMARY_END,
        primary_research_role="PRIMARY",
        exchange_calendar="XNYS",
        session_ids=SESSIONS,
        as_of_session=as_of_session,
        provider_id="QuantConnect",
        dataset_id=SOURCE_ID,
        source_checksum=source_checksum,
        statistics=(
            CalibrationAggregateStatistic(
                statistic_id="observed_sample_count",
                value="5",
                unit_id="sessions",
                sample_count=5,
                is_policy_value=False,
            ),
        ),
        derived_export_safe=True,
        contains_raw_option_rows=False,
        raw_options_data_exported=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
    )


def _write_reference(
    tmp_path: Path,
    *,
    slot_id: str,
    evidence_class: OwnerDecisionEvidenceClass,
    dq_report: DQReportRecord | None = None,
    evidence_record: QQQOptionsPrimaryWindowCalibrationEvidenceRecord | None = None,
) -> CalibrationEvidenceReference:
    evidence = evidence_record or _evidence_record(slot_id=slot_id, evidence_class=evidence_class)
    report = dq_report or _dq_report(slot_id=slot_id)
    evidence_relative = f"evidence/{slot_id}.json"
    dq_relative = f"dq/{slot_id}.json"
    evidence_path = tmp_path / Path(evidence_relative)
    dq_path = tmp_path / Path(dq_relative)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    dq_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_bytes(evidence.canonical_bytes)
    dq_path.write_bytes(report.canonical_bytes)
    return CalibrationEvidenceReference(
        slot_id=slot_id,
        evidence_path=evidence_relative,
        evidence_file_sha256=hashlib.sha256(evidence.canonical_bytes).hexdigest(),
        evidence_content_sha256=evidence.content_sha256,
        dq_report_path=dq_relative,
        dq_report_file_sha256=hashlib.sha256(report.canonical_bytes).hexdigest(),
        dq_report_content_sha256=report.content_sha256,
    )


def _empty_evaluation():
    return build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2510-empty",
        issued_at_utc=ISSUED_AT,
        implementation_repository_code_sha=REPOSITORY_SHA,
    )


def test_policy_and_g3_scope_are_exact_and_threshold_free() -> None:
    loaded = load_qqq_options_primary_window_calibration_policy()

    assert loaded.policy.primary_research_start == PRIMARY_START
    assert len(loaded.policy.g3_slot_ids) == 18
    assert loaded.policy.g3_slot_ids == tuple(sorted(loaded.policy.g3_slot_ids))
    assert loaded.policy.safety.owner_policy_value_count == 0
    assert loaded.policy.safety.executable_policy_authorized is False
    raw = loaded.policy_path.read_text(encoding="utf-8")
    assert "2022-12-01" not in raw
    assert "threshold" not in raw.lower()


def test_empty_evidence_is_policy_blocked_and_mechanically_derived() -> None:
    evaluation = _empty_evaluation()

    assert evaluation.catalog.required_slot_count == 18
    assert tuple(item.slot_id for item in evaluation.catalog.requirements) == (
        "ACC_CASH_RESERVATION",
        "ACC_DQ_PIT_REPRO",
        "ACC_FEE_SCHEDULE",
        "ACC_RESULT_INCLUSION",
        "ACC_SAMPLE_COVERAGE",
        "ACC_SIZING_EXPOSURE",
        "EXE_MARKETABLE_LIMIT",
        "EXE_QUOTE_DISPOSITION",
        "LIFE_EXPIRY_EXIT_GUARD",
        "LIFE_TERMINAL_VALUATION",
        "SEL_DELTA_SOURCE_RANGE",
        "SEL_DTE_WINDOW",
        "SEL_MONEYNESS_RANGE",
        "SEL_OPEN_INTEREST_FLOOR",
        "SEL_QUOTE_FRESHNESS",
        "SEL_RANK_PRIORITY",
        "SEL_SPREAD_LIMIT",
        "SEL_VOLUME_FLOOR",
    )
    assert evaluation.readiness.readiness_status is (
        CalibrationReadinessStatus.EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED
    )
    assert evaluation.readiness.admitted_slot_count == 0
    assert evaluation.readiness.missing_slot_count == 18
    assert all(
        item.status is CalibrationSlotEvidenceStatus.MISSING
        for item in evaluation.readiness.slot_readiness
    )
    assert evaluation.handoff.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert evaluation.handoff.safety.orders == evaluation.handoff.safety.fills == 0


def test_empty_evaluation_seal_and_replay_are_canonical() -> None:
    evaluation = _empty_evaluation()

    replayed = resolve_qqq_options_primary_window_calibration_evaluation(
        catalog_bytes=evaluation.catalog.canonical_bytes,
        receipt_bytes=evaluation.receipt.canonical_bytes,
        readiness_bytes=evaluation.readiness.canonical_bytes,
        handoff_bytes=evaluation.handoff.canonical_bytes,
        expected_implementation_repository_code_sha=REPOSITORY_SHA,
    )

    assert replayed == evaluation


def test_partial_evidence_is_admitted_from_bytes_but_remains_blocked(
    tmp_path: Path,
) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    )

    evaluation = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2510-partial",
        issued_at_utc=ISSUED_AT,
        implementation_repository_code_sha=REPOSITORY_SHA,
        evidence_references=(reference,),
        evidence_root=tmp_path,
    )

    assert evaluation.readiness.readiness_status is (
        CalibrationReadinessStatus.PARTIAL_EVIDENCE_POLICY_BLOCKED
    )
    assert evaluation.receipt.admitted_slot_count == 1
    assert evaluation.readiness.missing_slot_count == 17
    assert evaluation.handoff.safety.owner_policy_value_count == 0
    assert evaluation.handoff.safety.executable_policy_authorized is False


def test_complete_evidence_is_permutation_invariant_and_not_executable(
    tmp_path: Path,
) -> None:
    requirements = _empty_evaluation().catalog.requirements
    references = tuple(
        _write_reference(
            tmp_path,
            slot_id=requirement.slot_id,
            evidence_class=requirement.evidence_class,
        )
        for requirement in requirements
    )
    left = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2510-complete",
        issued_at_utc=ISSUED_AT,
        implementation_repository_code_sha=REPOSITORY_SHA,
        evidence_references=references,
        evidence_root=tmp_path,
    )
    right = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2510-complete",
        issued_at_utc=ISSUED_AT,
        implementation_repository_code_sha=REPOSITORY_SHA,
        evidence_references=tuple(reversed(references)),
        evidence_root=tmp_path,
    )

    assert left == right
    assert left.readiness.readiness_status is (
        CalibrationReadinessStatus.READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE
    )
    assert left.readiness.admitted_slot_count == 18
    assert left.readiness.missing_slot_count == 0
    assert left.handoff.review_disposition == "NO_OWNER_POLICY_VALUES_EMITTED"
    assert left.handoff.evidence_review_is_not_policy_approval is True
    assert left.handoff.safety.selection_authorized is False
    assert left.handoff.safety.external_action_authorized is False


def test_complete_evidence_replays_from_exact_files(tmp_path: Path) -> None:
    requirements = _empty_evaluation().catalog.requirements
    references = tuple(
        _write_reference(
            tmp_path,
            slot_id=requirement.slot_id,
            evidence_class=requirement.evidence_class,
        )
        for requirement in requirements
    )
    evaluation = build_qqq_options_primary_window_calibration_evaluation(
        evaluation_id="trading-2510-complete-replay",
        issued_at_utc=ISSUED_AT,
        implementation_repository_code_sha=REPOSITORY_SHA,
        evidence_references=references,
        evidence_root=tmp_path,
    )

    replayed = resolve_qqq_options_primary_window_calibration_evaluation(
        catalog_bytes=evaluation.catalog.canonical_bytes,
        receipt_bytes=evaluation.receipt.canonical_bytes,
        readiness_bytes=evaluation.readiness.canonical_bytes,
        handoff_bytes=evaluation.handoff.canonical_bytes,
        expected_implementation_repository_code_sha=REPOSITORY_SHA,
        evidence_root=tmp_path,
    )

    assert replayed == evaluation


def test_duplicate_slot_reference_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-duplicate",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference, reference),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_DUPLICATE_SLOT"


@pytest.mark.parametrize(
    "slot_id",
    ["LIFE_CLOSE_HOLD_ROLL", "EXE_SLIPPAGE", "ACC_CASH_CARRY_BENCHMARK"],
)
def test_g1_g4_and_added_axis_cannot_enter_g3_scope(tmp_path: Path, slot_id: str) -> None:
    arbitrary = tmp_path / "arbitrary.json"
    arbitrary.write_bytes(b"{}\n")
    file_sha = hashlib.sha256(arbitrary.read_bytes()).hexdigest()
    reference = CalibrationEvidenceReference(
        slot_id=slot_id,
        evidence_path="arbitrary.json",
        evidence_file_sha256=file_sha,
        evidence_content_sha256="c" * 64,
        dq_report_path="arbitrary.json",
        dq_report_file_sha256=file_sha,
        dq_report_content_sha256="d" * 64,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-non-g3",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_SLOT_SCOPE_VIOLATION"


def test_forged_pass_declaration_with_arbitrary_bytes_fails_closed(
    tmp_path: Path,
) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    arbitrary = tmp_path / "arbitrary.json"
    arbitrary.write_bytes(b'{"status":"PASS"}\n')
    file_sha = hashlib.sha256(arbitrary.read_bytes()).hexdigest()
    reference = CalibrationEvidenceReference(
        slot_id=requirement.slot_id,
        evidence_path="arbitrary.json",
        evidence_file_sha256=file_sha,
        evidence_content_sha256="c" * 64,
        dq_report_path="arbitrary.json",
        dq_report_file_sha256=file_sha,
        dq_report_content_sha256="d" * 64,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-forged",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_RECORD_INVALID"


def test_evidence_file_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    ).model_copy(update={"evidence_file_sha256": "e" * 64})

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-hash-mismatch",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_FILE_HASH_MISMATCH"


@pytest.mark.parametrize("dq_status", ["FAIL", "NOT_EVALUATED"])
def test_semantic_non_pass_dq_never_admits(tmp_path: Path, dq_status: str) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        dq_report=_dq_report(slot_id=requirement.slot_id, dq_status=dq_status),
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-dq-not-pass",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_DQ_REJECTED"


def test_unknown_dq_status_bytes_never_admit(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    )
    dq_path = tmp_path / Path(reference.dq_report_path)
    payload = json.loads(dq_path.read_text(encoding="utf-8"))
    payload["dq_status"] = "UNKNOWN"
    raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
    dq_path.write_bytes(raw)
    tampered = reference.model_copy(
        update={"dq_report_file_sha256": hashlib.sha256(raw).hexdigest()}
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-dq-unknown",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(tampered,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_DQ_REJECTED"


def test_dq_scope_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        dq_report=_dq_report(slot_id=requirement.slot_id, scope="wrong_scope"),
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-dq-scope",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_DQ_REJECTED"


def test_dq_range_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        dq_report=_dq_report(
            slot_id=requirement.slot_id,
            requested_end=date(2021, 2, 25),
            evaluated_end=date(2021, 2, 25),
        ),
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-dq-range",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_RANGE_MISMATCH"


def test_dq_source_checksum_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    evidence = _evidence_record(
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        source_checksum="f" * 64,
    )
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        evidence_record=evidence,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-source-mismatch",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_SOURCE_MISMATCH"


def test_evidence_class_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    evidence = _evidence_record(
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        evidence_class_override=OwnerDecisionEvidenceClass.DERIVED_MARKET_AGGREGATES,
    )
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        evidence_record=evidence,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-class-mismatch",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_SLOT_SCOPE_VIOLATION"


def test_repository_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    evidence = _evidence_record(
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        repository_sha="f" * 40,
    )
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
        evidence_record=evidence,
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-repository-mismatch",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(reference,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_AUTHORITY_BINDING_MISMATCH"


def test_pre_window_primary_evidence_is_rejected() -> None:
    requirement = _empty_evaluation().catalog.requirements[0]

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        _evidence_record(
            slot_id=requirement.slot_id,
            evidence_class=requirement.evidence_class,
            requested_start=date(2021, 2, 19),
            evaluated_start=date(2021, 2, 19),
        )

    assert exc.value.code == "CALIBRATION_PAYLOAD_MISMATCH"


def test_as_of_mismatch_is_rejected() -> None:
    requirement = _empty_evaluation().catalog.requirements[0]

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        _evidence_record(
            slot_id=requirement.slot_id,
            evidence_class=requirement.evidence_class,
            as_of_session=date(2021, 2, 25),
        )

    assert exc.value.code == "CALIBRATION_PAYLOAD_MISMATCH"


def test_raw_rows_or_raw_export_cannot_be_sealed() -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    valid = _evidence_record(
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    )
    payload = valid.model_dump(exclude={"content_sha256"})

    for field_name in ("contains_raw_option_rows", "raw_options_data_exported"):
        with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
            QQQOptionsPrimaryWindowCalibrationEvidenceRecord.seal(**{**payload, field_name: True})
        assert exc.value.code == "CALIBRATION_PAYLOAD_MISMATCH"


def test_path_traversal_is_rejected_before_io() -> None:
    with pytest.raises(ValidationError):
        CalibrationEvidenceReference(
            slot_id="ACC_CASH_RESERVATION",
            evidence_path="../escape.json",
            evidence_file_sha256="a" * 64,
            evidence_content_sha256="b" * 64,
            dq_report_path="dq/report.json",
            dq_report_file_sha256="c" * 64,
            dq_report_content_sha256="d" * 64,
        )


def test_symlinked_evidence_path_is_rejected(tmp_path: Path) -> None:
    requirement = _empty_evaluation().catalog.requirements[0]
    reference = _write_reference(
        tmp_path,
        slot_id=requirement.slot_id,
        evidence_class=requirement.evidence_class,
    )
    source = tmp_path / Path(reference.evidence_path)
    link = tmp_path / "evidence-link.json"
    try:
        os.symlink(source, link)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")
    linked = reference.model_copy(update={"evidence_path": "evidence-link.json"})

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        build_qqq_options_primary_window_calibration_evaluation(
            evaluation_id="trading-2510-symlink",
            issued_at_utc=ISSUED_AT,
            implementation_repository_code_sha=REPOSITORY_SHA,
            evidence_references=(linked,),
            evidence_root=tmp_path,
        )

    assert exc.value.code == "CALIBRATION_PATH_INVALID"


def test_duplicate_json_key_and_noncanonical_bytes_are_rejected() -> None:
    evidence = _empty_evaluation().catalog
    payload = json.loads(evidence.canonical_bytes)
    duplicate = (
        b"{\n"
        b'  "schema_version": "qqq_options_primary_window_calibration_catalog.v1",\n'
        b'  "schema_version": "qqq_options_primary_window_calibration_catalog.v1"\n'
        b"}\n"
    )

    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        type(evidence).from_json_bytes(duplicate)
    assert exc.value.code == "CALIBRATION_RECORD_INVALID"

    noncanonical = json.dumps(payload, sort_keys=True).encode()
    with pytest.raises(QQQOptionsPrimaryWindowCalibrationContractError) as exc:
        type(evidence).from_json_bytes(noncanonical)
    assert exc.value.code == "CALIBRATION_RECORD_NOT_CANONICAL"


def test_default_build_performs_no_external_or_production_action() -> None:
    evaluation = _empty_evaluation()

    for record in (
        evaluation.catalog,
        evaluation.receipt,
        evaluation.readiness,
        evaluation.handoff,
    ):
        assert record.safety.external_action_authorized is False
        assert record.safety.investment_interpretation_allowed is False
        assert record.safety.paper_allowed is False
        assert record.safety.live_allowed is False
        assert record.safety.broker_allowed is False
        assert record.safety.production_effect == "none"
        assert record.safety.broker_action == "none"


def test_project_root_policy_path_is_regular_and_bound() -> None:
    loaded = load_qqq_options_primary_window_calibration_policy()

    assert loaded.policy_path.is_file()
    assert loaded.policy_path.resolve().is_relative_to(PROJECT_ROOT.resolve())
