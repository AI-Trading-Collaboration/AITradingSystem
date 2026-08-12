from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQReportRecord,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.primary_window_derived_calibration_evidence_generator import (  # noqa: E501
    DerivedSessionStatistic,
    DerivedSlotSessionObservation,
    QQQOptionsDerivedCalibrationEvidenceGeneratorError,
    QQQOptionsPrimaryWindowDerivedObservationBundle,
    build_qqq_options_primary_window_derived_observation_bundle,
    generate_qqq_options_primary_window_derived_calibration_evidence_package,
    load_qqq_options_derived_calibration_evidence_generator_policy,
    resolve_qqq_options_primary_window_derived_calibration_evidence_package,
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
DQ_AT = datetime(2026, 8, 12, 3, tzinfo=UTC)
BUNDLE_AT = datetime(2026, 8, 12, 4, tzinfo=UTC)
GENERATED_AT = datetime(2026, 8, 12, 5, tzinfo=UTC)
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


def _dq_report(*, dq_status: str = "PASS", pit_status: str = "PASS") -> DQReportRecord:
    checks = tuple(
        DQCheckResult(
            check_id=check_id,
            status=(dq_status if index == 0 and dq_status != "PASS" else "PASS"),
            reason_code=(
                "DERIVED_SOURCE_DQ_NOT_PASS"
                if index == 0 and dq_status != "PASS"
                else None
            ),
            observed_at_utc=DQ_AT,
        )
        for index, check_id in enumerate(REQUIRED_DQ_CHECK_IDS)
    )
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id="dq-trading-2511-derived-source",
        record_id="dq-trading-2511-derived-source-record",
        created_at_utc=DQ_AT,
        producer_version="trading-2511-test-v1",
        repository_code_sha=REPOSITORY_SHA,
        policy_id="qqq_options_dq_pit_identity_v1",
        policy_version="1.0.0",
        policy_sha256=DQ_POLICY_SHA,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=(SOURCE_ID,),
        source_checksums=(SOURCE_SHA,),
        requested_start=PRIMARY_START,
        requested_end=PRIMARY_END,
        evaluated_start=PRIMARY_START,
        evaluated_end=PRIMARY_END,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="dq-trading-2511-derived-source-lineage",
        safety=_safety(),
        scope="qqq_options_event_dq_pit_identity",
        report_version="1.0.0",
        generated_at_utc=DQ_AT,
        checks=checks,
    )


def _write_dq(root: Path, report: DQReportRecord | None = None) -> tuple[str, str, str]:
    dq = report or _dq_report()
    relative = "dq/report.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(dq.canonical_bytes)
    return relative, hashlib.sha256(dq.canonical_bytes).hexdigest(), dq.content_sha256


def _observations(*, slot_ids: tuple[str, ...]) -> tuple[DerivedSlotSessionObservation, ...]:
    policy = load_qqq_options_derived_calibration_evidence_generator_policy().policy
    definitions = {item.slot_id: item for item in policy.metric_definitions}
    observations = []
    for slot_id in slot_ids:
        definition = definitions[slot_id]
        for session_index, session_id in enumerate(SESSIONS, start=1):
            observations.append(
                DerivedSlotSessionObservation(
                    slot_id=slot_id,
                    session_id=session_id,
                    statistics=tuple(
                        DerivedSessionStatistic(
                            statistic_id=item.statistic_id,
                            value=str(session_index),
                            unit_id=item.unit_id,
                            sample_count=session_index,
                            is_policy_value=False,
                        )
                        for item in definition.statistics
                    ),
                    derived_export_safe=True,
                    contains_raw_option_rows=False,
                )
            )
    return tuple(observations)


def _bundle(
    root: Path,
    *,
    slot_ids: tuple[str, ...],
    observations: tuple[DerivedSlotSessionObservation, ...] | None = None,
    report: DQReportRecord | None = None,
) -> QQQOptionsPrimaryWindowDerivedObservationBundle:
    dq_path, dq_file_sha, dq_content_sha = _write_dq(root, report)
    return build_qqq_options_primary_window_derived_observation_bundle(
        bundle_id="trading-2511-test-source",
        created_at_utc=BUNDLE_AT,
        repository_code_sha=REPOSITORY_SHA,
        requested_start=PRIMARY_START,
        requested_end=PRIMARY_END,
        evaluated_start=PRIMARY_START,
        evaluated_end=PRIMARY_END,
        provider_id="QuantConnect",
        dataset_id=SOURCE_ID,
        source_checksum=SOURCE_SHA,
        dq_report_path=dq_path,
        dq_report_file_sha256=dq_file_sha,
        dq_report_content_sha256=dq_content_sha,
        observations=observations or _observations(slot_ids=slot_ids),
    )


def test_policy_is_exact_empty_production_inventory_and_policy_blocked() -> None:
    loaded = load_qqq_options_derived_calibration_evidence_generator_policy()

    assert loaded.policy.primary_research_start == PRIMARY_START
    assert len(loaded.policy.metric_definitions) == 18
    assert tuple(item.slot_id for item in loaded.policy.metric_definitions) == tuple(
        sorted(item.slot_id for item in loaded.policy.metric_definitions)
    )
    assert loaded.policy.production_source_inventory == ()
    assert loaded.policy.safety.owner_policy_value_count == 0
    assert loaded.policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert loaded.policy.safety.selection_authorized is False
    raw = loaded.policy_path.read_text(encoding="utf-8")
    assert "2022-12-01" not in raw


def test_source_bundle_is_permutation_invariant_and_canonical(tmp_path: Path) -> None:
    observations = _observations(slot_ids=("SEL_DELTA_SOURCE_RANGE",))
    left = _bundle(tmp_path, slot_ids=(), observations=observations)
    right = _bundle(tmp_path, slot_ids=(), observations=tuple(reversed(observations)))

    assert left == right
    assert left.canonical_bytes == right.canonical_bytes
    assert QQQOptionsPrimaryWindowDerivedObservationBundle.from_json_bytes(
        left.canonical_bytes
    ) == left


def test_partial_package_aggregates_and_replays_but_remains_blocked(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path, slot_ids=("SEL_DELTA_SOURCE_RANGE",))

    package = generate_qqq_options_primary_window_derived_calibration_evidence_package(
        package_id="trading-2511-partial-package",
        generated_at_utc=GENERATED_AT,
        source_bundle=bundle,
        evidence_root=tmp_path,
        package_relative_path="packages/partial",
    )

    assert len(package.evidence_records) == 1
    record = package.evidence_records[0]
    assert tuple(item.value for item in record.statistics) == ("5", "1")
    assert tuple(item.sample_count for item in record.statistics) == (15, 15)
    assert package.calibration_evaluation.readiness.admitted_slot_count == 1
    assert package.calibration_evaluation.readiness.missing_slot_count == 17
    assert package.manifest.readiness_status == "PARTIAL_EVIDENCE_POLICY_BLOCKED"
    assert package.manifest.owner_policy_value_count == 0
    assert package.manifest.orders == package.manifest.fills == 0
    assert package.manifest.external_action_performed is False
    replayed = resolve_qqq_options_primary_window_derived_calibration_evidence_package(
        evidence_root=tmp_path,
        package_relative_path="packages/partial",
        expected_implementation_repository_code_sha=REPOSITORY_SHA,
    )
    assert replayed == package


def test_complete_package_is_owner_review_ready_but_never_executable(
    tmp_path: Path,
) -> None:
    slot_ids = tuple(
        item.slot_id
        for item in (
            load_qqq_options_derived_calibration_evidence_generator_policy()
            .policy.metric_definitions
        )
    )
    bundle = _bundle(tmp_path, slot_ids=slot_ids)

    package = generate_qqq_options_primary_window_derived_calibration_evidence_package(
        package_id="trading-2511-complete-package",
        generated_at_utc=GENERATED_AT,
        source_bundle=bundle,
        evidence_root=tmp_path,
        package_relative_path="packages/complete",
    )

    assert package.manifest.generated_slot_count == 18
    assert package.manifest.readiness_status == (
        "READY_FOR_OWNER_POLICY_REVIEW_NOT_EXECUTABLE"
    )
    assert package.calibration_evaluation.handoff.owner_review_required is True
    assert package.calibration_evaluation.handoff.evidence_review_is_not_policy_approval is True
    assert package.calibration_evaluation.handoff.safety.executable_policy_authorized is False
    assert package.calibration_evaluation.handoff.safety.selection_authorized is False
    assert package.calibration_evaluation.handoff.safety.production_effect == "none"


def test_incomplete_slot_fails_closed_before_package_creation(tmp_path: Path) -> None:
    observations = _observations(slot_ids=("SEL_DTE_WINDOW",))[:-1]
    bundle = _bundle(tmp_path, slot_ids=(), observations=observations)

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_INCOMPLETE_SLOT",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-incomplete",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="packages/incomplete",
        )

    assert not (tmp_path / "packages/incomplete").exists()


def test_duplicate_and_unknown_slot_inputs_fail_closed(tmp_path: Path) -> None:
    observation = _observations(slot_ids=("SEL_DTE_WINDOW",))[0]
    with pytest.raises(QQQOptionsDerivedCalibrationEvidenceGeneratorError):
        _bundle(tmp_path, slot_ids=(), observations=(observation, observation))

    unknown = DerivedSlotSessionObservation(
        slot_id="UNKNOWN_SLOT",
        session_id=SESSIONS[0],
        statistics=(
            DerivedSessionStatistic(
                statistic_id="observed_count",
                value="1",
                unit_id="observations",
                sample_count=1,
                is_policy_value=False,
            ),
        ),
        derived_export_safe=True,
        contains_raw_option_rows=False,
    )
    bundle = _bundle(tmp_path, slot_ids=(), observations=(unknown,))
    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_SLOT_SCOPE_VIOLATION",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-unknown-slot",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="packages/unknown-slot",
        )


def test_metric_unit_drift_fails_closed(tmp_path: Path) -> None:
    observations = list(_observations(slot_ids=("SEL_DTE_WINDOW",)))
    first = observations[0]
    first_stat = first.statistics[0]
    observations[0] = DerivedSlotSessionObservation(
        slot_id=first.slot_id,
        session_id=first.session_id,
        statistics=(
            DerivedSessionStatistic(
                statistic_id=first_stat.statistic_id,
                value=first_stat.value,
                unit_id="wrong_unit",
                sample_count=first_stat.sample_count,
                is_policy_value=False,
            ),
            *first.statistics[1:],
        ),
        derived_export_safe=True,
        contains_raw_option_rows=False,
    )
    bundle = _bundle(tmp_path, slot_ids=(), observations=tuple(observations))

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_METRIC_DEFINITION_MISMATCH",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-unit-drift",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="packages/unit-drift",
        )


def test_raw_row_flags_and_path_escape_cannot_be_enabled(tmp_path: Path) -> None:
    observation = _observations(slot_ids=("SEL_DTE_WINDOW",))[0]
    invalid_payload = observation.model_dump(mode="python")
    invalid_payload["contains_raw_option_rows"] = True
    with pytest.raises(ValidationError, match="contains_raw_option_rows"):
        DerivedSlotSessionObservation.model_validate(invalid_payload)

    bundle = _bundle(tmp_path, slot_ids=("SEL_DTE_WINDOW",))
    with pytest.raises(ValueError, match="package_relative_path"):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-path-escape",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="../escaped",
        )


@pytest.mark.parametrize(
    "dq_status,pit_status", [("FAIL", "PASS"), ("PASS", "NOT_EVALUATED")]
)
def test_semantic_dq_fail_or_unknown_stops_generation(
    tmp_path: Path,
    dq_status: str,
    pit_status: str,
) -> None:
    report = _dq_report(dq_status=dq_status, pit_status=pit_status)
    bundle = _bundle(tmp_path, slot_ids=("SEL_DTE_WINDOW",), report=report)

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_DQ_REJECTED",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-dq-rejected",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="packages/dq-rejected",
        )


def test_arbitrary_report_bytes_cannot_be_declared_pass(tmp_path: Path) -> None:
    dq_path = tmp_path / "dq/report.json"
    dq_path.parent.mkdir(parents=True)
    arbitrary = b'{"status":"PASS"}\n'
    dq_path.write_bytes(arbitrary)
    bundle = build_qqq_options_primary_window_derived_observation_bundle(
        bundle_id="trading-2511-forged-pass",
        created_at_utc=BUNDLE_AT,
        repository_code_sha=REPOSITORY_SHA,
        requested_start=PRIMARY_START,
        requested_end=PRIMARY_END,
        evaluated_start=PRIMARY_START,
        evaluated_end=PRIMARY_END,
        provider_id="QuantConnect",
        dataset_id=SOURCE_ID,
        source_checksum=SOURCE_SHA,
        dq_report_path="dq/report.json",
        dq_report_file_sha256=hashlib.sha256(arbitrary).hexdigest(),
        dq_report_content_sha256="c" * 64,
        observations=_observations(slot_ids=("SEL_DTE_WINDOW",)),
    )

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_DQ_REJECTED",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-forged-pass",
            generated_at_utc=GENERATED_AT,
            source_bundle=bundle,
            evidence_root=tmp_path,
            package_relative_path="packages/forged-pass",
        )


def test_dq_hash_mismatch_stops_generation(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, slot_ids=("SEL_VOLUME_FLOOR",))
    payload = bundle.model_dump(mode="python", exclude={"content_sha256"})
    payload["dq_report_file_sha256"] = "d" * 64
    mismatched = QQQOptionsPrimaryWindowDerivedObservationBundle.seal(**payload)

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_DQ_REJECTED",
    ):
        generate_qqq_options_primary_window_derived_calibration_evidence_package(
            package_id="trading-2511-dq-hash-mismatch",
            generated_at_utc=GENERATED_AT,
            source_bundle=mismatched,
            evidence_root=tmp_path,
            package_relative_path="packages/dq-hash-mismatch",
        )


def test_extra_package_file_and_tampered_evidence_fail_replay(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, slot_ids=("SEL_SPREAD_LIMIT",))
    generate_qqq_options_primary_window_derived_calibration_evidence_package(
        package_id="trading-2511-tamper",
        generated_at_utc=GENERATED_AT,
        source_bundle=bundle,
        evidence_root=tmp_path,
        package_relative_path="packages/tamper",
    )
    extra = tmp_path / "packages/tamper/unexpected.txt"
    extra.write_text("unexpected", encoding="utf-8")

    with pytest.raises(
        QQQOptionsDerivedCalibrationEvidenceGeneratorError,
        match="GENERATOR_PACKAGE_REPLAY_REJECTED",
    ):
        resolve_qqq_options_primary_window_derived_calibration_evidence_package(
            evidence_root=tmp_path,
            package_relative_path="packages/tamper",
            expected_implementation_repository_code_sha=REPOSITORY_SHA,
        )


def test_primary_window_start_cannot_drift() -> None:
    with pytest.raises(QQQOptionsDerivedCalibrationEvidenceGeneratorError):
        build_qqq_options_primary_window_derived_observation_bundle(
            bundle_id="trading-2511-wrong-start",
            created_at_utc=BUNDLE_AT,
            repository_code_sha=REPOSITORY_SHA,
            requested_start=date(2021, 2, 23),
            requested_end=PRIMARY_END,
            evaluated_start=date(2021, 2, 23),
            evaluated_end=PRIMARY_END,
            provider_id="QuantConnect",
            dataset_id=SOURCE_ID,
            source_checksum=SOURCE_SHA,
            dq_report_path="dq/report.json",
            dq_report_file_sha256="d" * 64,
            dq_report_content_sha256="e" * 64,
            observations=(),
        )
