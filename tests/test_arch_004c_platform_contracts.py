from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from ai_trading_system.contracts import (
    ArtifactEnvelope,
    ArtifactEnvelopeError,
    ArtifactLifecycle,
    ArtifactPointer,
    ArtifactVisibility,
    CanonicalStatus,
    CoverageInterval,
    DataQualityContractRef,
    DataQualityEvidence,
    DataQualityEvidenceError,
    DateRange,
    EffectiveCoverage,
    EntrypointRef,
    EvidenceRole,
    FailurePropagation,
    MarketRegimeSpec,
    PolicyRef,
    PolicyRole,
    ReaderTier,
    ReportAudience,
    ReportContractError,
    ReportSpec,
    ResearchWindowRole,
    ResearchWindowSpec,
    RunLedger,
    WorkflowCadence,
    WorkflowContractError,
    WorkflowSpec,
    WorkflowStepSpec,
    resolve_complete_research_context,
)
from ai_trading_system.core import ProductionEffect

AS_OF = date(2026, 7, 10)
CHECKED_AT = datetime(2026, 7, 10, 22, 0, tzinfo=UTC)
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def test_data_quality_evidence_round_trip_and_ready_contract() -> None:
    evidence = _quality()

    restored = DataQualityEvidence.from_dict(evidence.to_dict())

    assert restored == evidence
    assert restored.evidence_id == evidence.evidence_id
    assert restored.assert_ready() is restored
    assert restored.to_contract_ref().policy_ref_id == "data_quality"


def test_data_quality_evidence_fails_closed_on_missing_report_or_status_conflict() -> None:
    missing_report = DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id="data_quality",
        policy_version="data_quality.v1",
        status="PASS",
        passed=True,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        report_path=None,
        report_sha256=None,
    )

    assert missing_report.ready is False
    with pytest.raises(DataQualityEvidenceError, match="DATA_QUALITY_REPORT_REF_REQUIRED"):
        missing_report.assert_ready()
    with pytest.raises(DataQualityEvidenceError, match="DQ_PASS_STATUS_CONFLICT"):
        DataQualityEvidence(
            contract_id="invalid",
            policy_id="data_quality",
            policy_version="data_quality.v1",
            status="FAIL",
            passed=True,
            checked_at=CHECKED_AT,
            as_of=AS_OF,
            report_path="outputs/reports/data_quality.json",
            report_sha256=SHA_A,
        )


def test_artifact_envelope_round_trip_preserves_complete_semantics() -> None:
    envelope = _envelope()

    restored = ArtifactEnvelope.from_dict(envelope.to_dict())

    assert restored == envelope
    assert restored.envelope_id == envelope.envelope_id
    assert restored.research_context is not None
    assert restored.research_context.context_id == _complete_context().context_id


def test_investment_artifact_envelope_requires_context_and_ready_quality() -> None:
    kwargs = _envelope_kwargs()
    kwargs["research_context"] = None
    with pytest.raises(ArtifactEnvelopeError, match="RESEARCH_CONTEXT_REQUIRED"):
        ArtifactEnvelope(**kwargs)

    kwargs = _envelope_kwargs()
    kwargs["data_quality"] = DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id="data_quality",
        policy_version="data_quality.v1",
        status="PASS",
        passed=True,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        report_path=None,
        report_sha256=None,
    )
    with pytest.raises(ArtifactEnvelopeError, match="ARTIFACT_DQ_NOT_READY"):
        ArtifactEnvelope(**kwargs)


def test_workflow_spec_round_trip_and_typed_entrypoint() -> None:
    spec = _workflow_spec()

    restored = WorkflowSpec.from_dict(spec.to_dict())

    assert restored == spec
    assert restored.spec_id == spec.spec_id
    assert restored.steps[1].entrypoint.display == ("ai_trading_system.ops_daily:run_daily_reports")
    assert restored.steps[1].legacy_command == ("aits", "ops", "daily-run")


def test_workflow_spec_rejects_cycles_and_non_idempotent_retries() -> None:
    with pytest.raises(WorkflowContractError, match="NON_IDEMPOTENT_RETRY_FORBIDDEN"):
        WorkflowStepSpec(
            step_id="broker_side_effect",
            entrypoint=EntrypointRef("ai_trading_system.broker", "submit"),
            idempotent=False,
            lock_key="broker-side-effect",
            max_attempts=2,
        )

    step_a = WorkflowStepSpec(
        step_id="a",
        entrypoint=EntrypointRef("ai_trading_system.ops", "a"),
        dependencies=("b",),
    )
    step_b = WorkflowStepSpec(
        step_id="b",
        entrypoint=EntrypointRef("ai_trading_system.ops", "b"),
        dependencies=("a",),
    )
    with pytest.raises(WorkflowContractError, match="WORKFLOW_DEPENDENCY_CYCLE"):
        WorkflowSpec(
            workflow_id="cycle",
            owner="operations",
            cadence=WorkflowCadence.DAILY,
            timezone="Asia/Tokyo",
            due_policy_id="daily.v1",
            steps=(step_a, step_b),
        )


def test_run_ledger_enforces_dependencies_quality_and_round_trip() -> None:
    spec = _workflow_spec()
    ledger = RunLedger.initialize(
        spec,
        run_id="daily:2026-07-10",
        as_of=AS_OF,
        created_at=CHECKED_AT,
    )
    due_at = datetime(2026, 7, 10, 22, 1, tzinfo=UTC)
    started_at = datetime(2026, 7, 10, 22, 2, tzinfo=UTC)
    finished_at = datetime(2026, 7, 10, 22, 3, tzinfo=UTC)

    validate = ledger.entry("validate_data").transition(CanonicalStatus.DUE, at=due_at)
    ledger = ledger.with_entry(spec, validate)
    validate = validate.transition(CanonicalStatus.RUNNING, at=started_at)
    ledger = ledger.with_entry(spec, validate)
    validate = validate.transition(
        CanonicalStatus.PASS,
        at=finished_at,
        artifacts=(_pointer("outputs/reports/data_quality.json", SHA_A),),
        data_quality=_quality(),
    )
    ledger = ledger.with_entry(spec, validate)

    report = ledger.entry("daily_reports").transition(CanonicalStatus.DUE, at=due_at)
    ledger = ledger.with_entry(spec, report)
    report = report.transition(CanonicalStatus.RUNNING, at=started_at)
    ledger = ledger.with_entry(spec, report)
    report = report.transition(
        CanonicalStatus.PASS,
        at=finished_at,
        artifacts=(_pointer("outputs/reports/daily.json", SHA_B),),
        data_quality=_quality(),
    )
    ledger = ledger.with_entry(spec, report)

    legacy_payload = ledger.to_dict()
    legacy_ledger_id = ledger.ledger_id
    assert "run_status" not in legacy_payload
    assert "run_blocker_codes" not in legacy_payload

    restored = RunLedger.from_dict(legacy_payload)
    assert restored == ledger
    assert restored.to_dict() == legacy_payload
    assert restored.ledger_id == legacy_ledger_id
    assert restored.entry("daily_reports").status is CanonicalStatus.PASS


def test_run_ledger_blocks_unmet_dependency_and_missing_quality() -> None:
    spec = _workflow_spec()
    ledger = RunLedger.initialize(
        spec,
        run_id="daily:2026-07-10",
        as_of=AS_OF,
        created_at=CHECKED_AT,
    )
    started = datetime(2026, 7, 10, 22, 1, tzinfo=UTC)

    report = ledger.entry("daily_reports").transition(CanonicalStatus.DUE, at=started)
    ledger = ledger.with_entry(spec, report)
    report = report.transition(CanonicalStatus.RUNNING, at=started)
    with pytest.raises(WorkflowContractError, match="WORKFLOW_DEPENDENCY_NOT_PASSED"):
        ledger.with_entry(spec, report)

    validate = ledger.entry("validate_data").transition(CanonicalStatus.DUE, at=started)
    ledger = ledger.with_entry(spec, validate)
    validate = validate.transition(CanonicalStatus.RUNNING, at=started)
    ledger = ledger.with_entry(spec, validate)
    validate = validate.transition(CanonicalStatus.PASS, at=started)
    with pytest.raises(WorkflowContractError, match="WORKFLOW_DQ_EVIDENCE_REQUIRED"):
        ledger.with_entry(spec, validate)


def test_run_ledger_rejects_invalid_terminal_transition() -> None:
    spec = _workflow_spec()
    ledger = RunLedger.initialize(
        spec,
        run_id="daily:2026-07-10",
        as_of=AS_OF,
        created_at=CHECKED_AT,
    )
    with pytest.raises(WorkflowContractError, match="INVALID_LEDGER_TRANSITION"):
        ledger.entry("validate_data").transition(
            CanonicalStatus.PASS,
            at=CHECKED_AT,
            data_quality=_quality(),
        )


def test_report_spec_round_trip_separates_source_provider_and_renderer() -> None:
    spec = _report_spec()

    restored = ReportSpec.from_dict(spec.to_dict())

    assert restored == spec
    assert restored.spec_id == spec.spec_id
    assert restored.canonical_source != restored.renderer
    assert restored.reader_tier is ReaderTier.OWNER_DAILY_BRIEF


def test_report_spec_rejects_non_actionable_owner_daily_report() -> None:
    with pytest.raises(ReportContractError, match="NON_ACTIONABLE_OWNER_DAILY_REPORT"):
        ReportSpec(
            **{**_report_kwargs(), "actionable": False},
        )


def _quality() -> DataQualityEvidence:
    return DataQualityEvidence(
        contract_id="cached_market_macro_validation",
        policy_id="data_quality",
        policy_version="data_quality.v1",
        status="PASS_WITH_WARNINGS",
        passed=True,
        checked_at=CHECKED_AT,
        as_of=AS_OF,
        report_path="outputs/reports/data_quality_2026-07-10.json",
        report_sha256=SHA_A,
        warning_count=1,
        checked_input_count=12,
    )


def _pointer(path: str, sha256: str) -> ArtifactPointer:
    return ArtifactPointer(
        path=path,
        artifact_type="json",
        sha256=sha256,
        size_bytes=128,
        schema_version="example.v1",
    )


def _envelope_kwargs() -> dict[str, object]:
    return {
        "artifact_id": "first_layer_v2_effective_coverage_audit",
        "producer": "ai_trading_system.upper_state_label_feature_reset:build_audit",
        "run_id": "arch-004c-contract-test",
        "generated_at": CHECKED_AT,
        "as_of": AS_OF,
        "status": CanonicalStatus.PASS,
        "production_effect": ProductionEffect.NONE,
        "payload": _pointer("outputs/research/effective_coverage.json", SHA_B),
        "owner": "research_governance",
        "lifecycle": ArtifactLifecycle.CURRENT,
        "visibility": ArtifactVisibility.RESEARCH,
        "investment_facing": True,
        "data_quality_required": True,
        "data_quality": _quality(),
        "research_context": _complete_context(),
        "input_artifacts": (_pointer("data/processed/features.json", SHA_C),),
        "policy_refs": (),
        "limitations": ("read_only",),
        "next_actions": ("review",),
    }


def _envelope() -> ArtifactEnvelope:
    return ArtifactEnvelope(**_envelope_kwargs())


def _workflow_spec() -> WorkflowSpec:
    validate = WorkflowStepSpec(
        step_id="validate_data",
        entrypoint=EntrypointRef("ai_trading_system.cli_commands.data", "validate_data"),
        expected_artifact_types=("data_quality",),
        quality_gate_required=True,
    )
    reports = WorkflowStepSpec(
        step_id="daily_reports",
        entrypoint=EntrypointRef("ai_trading_system.ops_daily", "run_daily_reports"),
        dependencies=("validate_data",),
        expected_artifact_types=("daily_report",),
        quality_gate_required=True,
        failure_propagation=FailurePropagation.BLOCK_DEPENDENTS,
        legacy_command=("aits", "ops", "daily-run"),
    )
    return WorkflowSpec(
        workflow_id="daily_ops",
        owner="operations",
        cadence=WorkflowCadence.DAILY,
        timezone="Asia/Tokyo",
        due_policy_id="daily_unified_trigger.v1",
        trading_calendar="XNYS",
        steps=(validate, reports),
    )


def _report_kwargs() -> dict[str, object]:
    return {
        "report_id": "daily_score",
        "title": "Daily Score Report",
        "owner": "system",
        "audience": ReportAudience.INVESTOR,
        "reader_tier": ReaderTier.OWNER_DAILY_BRIEF,
        "cadence": WorkflowCadence.DAILY,
        "canonical_source": EntrypointRef("ai_trading_system.scoring", "load_daily_score"),
        "section_provider": EntrypointRef(
            "ai_trading_system.reports.sections", "daily_score_section"
        ),
        "view_model": EntrypointRef(
            "ai_trading_system.reports.view_models", "daily_score_view_model"
        ),
        "renderer": EntrypointRef("ai_trading_system.reports.renderers", "render_daily_score"),
        "artifact_globs": ("outputs/reports/daily_score_*.md",),
        "freshness_sla_days": 1,
        "owner_action": "review_if_missing_or_stale",
        "actionable": True,
        "lifecycle": ArtifactLifecycle.CURRENT,
        "production_effect": ProductionEffect.NONE,
    }


def _report_spec() -> ReportSpec:
    return ReportSpec(**_report_kwargs())


def _complete_context():
    policy_refs = (
        PolicyRef(
            policy_id="market_regimes",
            role=PolicyRole.MARKET_REGIME,
            version="market_regimes.v1",
            status="active",
            path="config/market_regimes.yaml",
            sha256=SHA_A,
        ),
        PolicyRef(
            policy_id="research_windows",
            role=PolicyRole.RESEARCH_WINDOW,
            version="research_windows.v1",
            status="active",
            path="config/research/research_window_registry.yaml",
            sha256=SHA_B,
        ),
        PolicyRef(
            policy_id="data_quality",
            role=PolicyRole.DATA_QUALITY,
            version="data_quality.v1",
            status="active",
            path="config/data_quality.yaml",
            sha256=SHA_C,
        ),
    )
    return resolve_complete_research_context(
        regime=MarketRegimeSpec(
            regime_id="ai_after_chatgpt",
            anchor_date=date(2022, 11, 30),
            start_date=date(2022, 12, 1),
        ),
        window=ResearchWindowSpec(
            window_id="exact_three_asset_validated",
            start_date=date(2021, 2, 22),
            role=ResearchWindowRole.PRIMARY_VALIDATED,
            evidence_role=EvidenceRole.PRIMARY_DECISION_EVIDENCE,
        ),
        requested_range=DateRange(date(2021, 2, 22), AS_OF),
        actual_data_range=DateRange(date(2021, 2, 22), AS_OF),
        effective_feature_start=date(2021, 2, 22),
        effective_prediction_start=date(2022, 2, 22),
        actual_portfolio_start=date(2021, 2, 22),
        evaluation_range=DateRange(date(2022, 2, 22), AS_OF),
        as_of=AS_OF,
        trading_calendar="XNYS",
        effective_coverage=EffectiveCoverage(
            (
                CoverageInterval(
                    source_id="prices",
                    date_range=DateRange(date(2021, 2, 22), AS_OF),
                ),
            )
        ),
        data_quality=DataQualityContractRef(
            contract_id="cached_market_macro_validation",
            status="PASS_WITH_WARNINGS",
            passed=True,
            as_of=AS_OF,
            policy_ref_id="data_quality",
            report_path="outputs/reports/data_quality_2026-07-10.json",
            report_sha256=SHA_A,
        ),
        policy_refs=policy_refs,
    )
