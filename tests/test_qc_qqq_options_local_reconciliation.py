from __future__ import annotations

import csv
import hashlib
import json
import runpy
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    DQCheckResult,
    DQReportRecord,
    ReconciliationReportRecord,
)
from ai_trading_system.qqq_options_research.local_reconciliation import (
    ActiveLocalReconciliationCriteria,
    QCCanonicalArtifact,
    QCExactReconciliationCheck,
    QCLocalReconciliationContractError,
    QCLocalReconciliationRequest,
    QCLocalReconciliationResult,
    QCPlatformCashFact,
    QCPlatformColumnMapping,
    QCPlatformOrderFact,
    QCReconciliationDifference,
    QCReconciliationDifferenceClass,
    UnresolvedLocalReconciliationCriteria,
    build_qc_qqq_options_local_reconciliation_input_sha256,
    load_qc_qqq_options_local_reconciliation_policy,
    reconcile_qc_qqq_options_local_evidence,
)
from ai_trading_system.qqq_options_research.platform_evidence_bundle import (
    LoadedQCPlatformEvidenceBundle,
)
from ai_trading_system.qqq_options_research.position_lifecycle import (
    replay_qqq_option_position_lifecycle,
)

_HELPERS = runpy.run_path(str(Path(__file__).with_name("test_qqq_options_position_lifecycle.py")))
_EVALUATED_AT = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
_AS_OF_SESSION = date(2021, 3, 10)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _artifact(artifact_id: str, content: bytes) -> QCCanonicalArtifact:
    return QCCanonicalArtifact(
        artifact_id=artifact_id,
        content=content,
        sha256=_sha(content),
    )


def _request(tmp_path: Path, *, execution_dq_status: str = "PASS") -> tuple[Any, ...]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    manifest, candidate, execution, accounting, lifecycle_policy_path = _HELPERS["_fixture"](
        tmp_path,
        evaluation_at=_EVALUATED_AT,
        as_of_session=_AS_OF_SESSION,
    )
    if execution_dq_status != "PASS":
        payload = {
            name: getattr(execution, name)
            for name in execution.__class__.model_fields
            if name != "content_sha256"
        }
        payload["global_dq_status"] = execution_dq_status
        payload["execution_stage_dq_status"] = execution_dq_status
        execution = type(execution).seal(**payload)
        cash_request = _HELPERS["_accounting_request"](
            manifest,
            execution,
            evaluation_at=_EVALUATED_AT,
            as_of_session=_AS_OF_SESSION,
        )
        failed_execution_root = tmp_path / "failed-execution"
        failed_execution_root.mkdir(parents=True, exist_ok=True)
        cash_policy_path = _HELPERS["_active_cash_policy_path"](failed_execution_root)
        from ai_trading_system.qqq_options_research.cash_accounting import (
            load_qqq_options_cash_accounting_policy,
            replay_qqq_option_cash_accounting,
        )

        accounting = replay_qqq_option_cash_accounting(
            cash_request,
            policy_path=cash_policy_path,
        )
        failed_lifecycle_root = tmp_path / "failed-lifecycle"
        failed_lifecycle_root.mkdir(parents=True, exist_ok=True)
        lifecycle_policy_path = _HELPERS["_active_lifecycle_policy_path"](
            failed_lifecycle_root,
            accounting_policy_sha256=load_qqq_options_cash_accounting_policy(
                cash_policy_path
            ).policy_sha256,
        )
    lifecycle_request = _HELPERS["_request"](
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=_EVALUATED_AT,
        as_of_session=_AS_OF_SESSION,
    )
    lifecycle = replay_qqq_option_position_lifecycle(
        lifecycle_request,
        policy_path=lifecycle_policy_path,
    )
    request = QCLocalReconciliationRequest.seal(
        schema_version="qc_qqq_options_local_reconciliation_request.v1",
        request_id="synthetic-local-reconciliation-request",
        evaluated_at_utc=_EVALUATED_AT,
        run_manifest=_artifact("run_manifest", manifest.canonical_bytes),
        execution_results=(_artifact("execution_result.000", execution.canonical_bytes),),
        cash_accounting_result=_artifact("cash_accounting_result", accounting.canonical_bytes),
        lifecycle_result=_artifact("lifecycle_result", lifecycle.canonical_bytes),
    )
    return request, manifest, execution, accounting, lifecycle


def _dq_report(
    manifest: Any,
    *,
    status: Literal["PASS", "FAIL", "NOT_EVALUATED"] = "PASS",
) -> DQReportRecord:
    envelope = _HELPERS["_envelope"](
        schema_name="dq_report",
        record_id="dq-report-platform-option-events",
        created_at_utc=_EVALUATED_AT,
        suffix="dq.platform.option-events",
        policy_sha256=("1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"),
        dq_status=status,
        pit_status=status,
        source_ids=manifest.source_ids,
        source_checksums=manifest.source_checksums,
    )
    envelope["lineage_id"] = manifest.lineage_id
    envelope["repository_code_sha"] = manifest.repository_code_sha
    envelope["requested_start"] = manifest.requested_start
    envelope["requested_end"] = manifest.requested_end
    envelope["evaluated_start"] = manifest.evaluated_start
    envelope["evaluated_end"] = manifest.evaluated_end
    return DQReportRecord.seal(
        **envelope,
        scope="qqq_options_platform_event_reconciliation",
        report_version="1.0",
        generated_at_utc=_EVALUATED_AT,
        checks=(
            DQCheckResult(
                check_id="platform_event_reconciliation",
                status=status,
                reason_code=None if status == "PASS" else f"PLATFORM_EVENT_{status}",
                observed_at_utc=_EVALUATED_AT,
            ),
        ),
    )


def _active_policy_path(tmp_path: Path) -> Path:
    source = PROJECT_ROOT / "config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "reconciliation_authorized": True,
            "owner_authorization_status": "OWNER_REVIEWED:SYNTHETIC_TEST_ONLY",
            "decision": "LOCAL_QC_RECONCILIATION_V1_READY",
        }
    )
    order_fields = (
        "event_at_utc",
        "event_sequence",
        "order_contracts",
        "order_id",
        "order_state",
        "option_sid",
        "side",
        "underlying",
        "filled_contracts_total",
        "limit_price_per_share",
    )
    trade_fields = (
        "contract_multiplier",
        "fee_usd",
        "fill_at_utc",
        "fill_id",
        "fill_price_per_share",
        "filled_contracts",
        "gross_cash_delta_usd",
        "order_id",
        "option_sid",
        "side",
    )
    payload["criteria"] = {
        "mode": "ACTIVE",
        "ingest_profile_status": "OWNER_REVIEWED_ACTIVE",
        "tolerance_policy_status": "OWNER_REVIEWED_ACTIVE",
        "results_projection_key": "aits_reconciliation_v1",
        "orders_csv_column_mapping": [
            {"canonical_field": item, "source_column": item} for item in order_fields
        ],
        "trades_csv_column_mapping": [
            {"canonical_field": item, "source_column": item} for item in trade_fields
        ],
        "monetary_absolute_tolerance_usd": "0.01",
        "metric_absolute_tolerance": "0",
        "timing_absolute_tolerance_seconds": "1",
        "decimal_rounding_policy": "NO_ROUNDING_EXACT_DECIMAL",
        "reviewed_authority_id": "SYNTHETIC_TEST_ONLY",
    }
    path = tmp_path / "active-local-reconciliation.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _loaded_bundle_stub() -> LoadedQCPlatformEvidenceBundle:
    return cast(
        LoadedQCPlatformEvidenceBundle,
        SimpleNamespace(
            metadata=SimpleNamespace(
                engine_identity_status="CONFIRMED",
                license_status="CONFIRMED",
                content_sha256="a" * 64,
            ),
            validation=SimpleNamespace(
                disposition="MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION",
                content_sha256="b" * 64,
            ),
        ),
    )


def _reseal(model: Any, **updates: Any) -> Any:
    payload = {
        name: getattr(model, name)
        for name in model.__class__.model_fields
        if name != "content_sha256"
    }
    payload.update(updates)
    return model.__class__.seal(**payload)


def test_tracked_default_is_unresolved_unauthorized_and_exact() -> None:
    loaded = load_qc_qqq_options_local_reconciliation_policy()

    assert loaded.policy.status == "OWNER_REVIEW_REQUIRED_BASELINE"
    assert loaded.policy.reconciliation_authorized is False
    assert isinstance(loaded.policy.criteria, UnresolvedLocalReconciliationCriteria)
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy.difference_classes == (
        "LICENSE",
        "LOGIC",
        "MANUAL_COLLECTION",
        "PLATFORM",
        "PROVIDER",
        "REALITY_MODEL",
        "TIMING",
    )
    assert loaded.policy.safety.external_pass_may_override_internal_failure is False


def test_default_returns_typed_policy_block_without_reading_platform(
    tmp_path: Path,
) -> None:
    request, *_ = _request(tmp_path)

    result = reconcile_qc_qqq_options_local_evidence(
        request,
        package_root=tmp_path / "must-not-be-read",
        capability_receipt_path=tmp_path / "must-not-be-read.json",
        capability_policy_path=tmp_path / "must-not-be-read.yaml",
        capability_evidence_path=tmp_path / "must-not-be-read-evidence.yaml",
    )

    assert result.outcome == "LOCAL_RECONCILIATION_POLICY_BLOCKED"
    assert result.reason_codes == ("RECONCILIATION_POLICY_REVIEW_REQUIRED",)
    assert result.option_event_dq_status == result.option_event_pit_status == "NOT_EVALUATED"
    assert result.new_order_count == result.new_fill_count == 0
    assert result.investment_interpretation_allowed is False
    assert result.range_expansion_allowed is False
    assert QCLocalReconciliationResult.from_json_bytes(result.canonical_bytes) == result


def test_request_and_input_identity_are_canonical_and_permutation_safe(
    tmp_path: Path,
) -> None:
    request, *_ = _request(tmp_path)
    loaded = load_qc_qqq_options_local_reconciliation_policy()

    assert QCLocalReconciliationRequest.from_json_bytes(request.canonical_bytes) == request
    assert build_qc_qqq_options_local_reconciliation_input_sha256(
        request, policy_sha256=loaded.policy_sha256
    ) == build_qc_qqq_options_local_reconciliation_input_sha256(
        request, policy_sha256=loaded.policy_sha256
    )
    payload = json.loads(request.canonical_bytes)
    payload["content_sha256"] = "0" * 64
    with pytest.raises(QCLocalReconciliationContractError):
        QCLocalReconciliationRequest.from_json_bytes(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )


def test_policy_hash_drift_and_unreviewed_active_mapping_fail_closed(
    tmp_path: Path,
) -> None:
    source = PROJECT_ROOT / "config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["execution_policy_sha256"] = "0" * 64
    drift = tmp_path / "drift.yaml"
    drift.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QCLocalReconciliationContractError) as exc:
        load_qc_qqq_options_local_reconciliation_policy(drift)
    assert exc.value.code == "LOCAL_RECONCILIATION_INHERITED_AUTHORITY_DRIFT"

    active = yaml.safe_load(_active_policy_path(tmp_path).read_text(encoding="utf-8"))
    active["criteria"]["orders_csv_column_mapping"] = []
    bad_mapping = tmp_path / "bad-mapping.yaml"
    bad_mapping.write_text(yaml.safe_dump(active, sort_keys=False), encoding="utf-8")
    with pytest.raises(QCLocalReconciliationContractError) as mapping_exc:
        load_qc_qqq_options_local_reconciliation_policy(bad_mapping)
    assert mapping_exc.value.code == "LOCAL_RECONCILIATION_POLICY_INVALID"


def test_active_policy_uses_explicit_reviewed_mapping_and_tolerances(
    tmp_path: Path,
) -> None:
    loaded = load_qc_qqq_options_local_reconciliation_policy(_active_policy_path(tmp_path))

    assert loaded.policy.reconciliation_authorized is True
    assert isinstance(loaded.policy.criteria, ActiveLocalReconciliationCriteria)
    assert loaded.policy.criteria.monetary_absolute_tolerance_usd.as_tuple().exponent == -2
    assert loaded.policy.criteria.decimal_rounding_policy == "NO_ROUNDING_EXACT_DECIMAL"
    assert all(
        isinstance(item, QCPlatformColumnMapping)
        for item in loaded.policy.criteria.orders_csv_column_mapping
    )


def test_active_reconciliation_reuses_shared_reports_and_is_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module

    request, manifest, *_ = _request(tmp_path)
    projection = module._build_local_projection(request, _dq_report(manifest))
    bundle = _loaded_bundle_stub()
    monkeypatch.setattr(
        module,
        "load_qc_qqq_options_manual_evidence_bundle",
        lambda *args, **kwargs: bundle,
    )
    monkeypatch.setattr(
        module,
        "_load_platform_projection",
        lambda loaded, criteria: projection,
    )
    kwargs = {
        "package_root": tmp_path / "synthetic-bundle",
        "capability_receipt_path": tmp_path / "receipt.json",
        "policy_path": _active_policy_path(tmp_path),
        "capability_policy_path": tmp_path / "capability.yaml",
        "capability_evidence_path": tmp_path / "evidence.yaml",
    }

    first = reconcile_qc_qqq_options_local_evidence(request, **kwargs)
    second = reconcile_qc_qqq_options_local_evidence(request, **kwargs)

    assert first == second
    assert first.outcome == "LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW"
    assert first.exact_checks and all(item.status == "PASS" for item in first.exact_checks)
    assert first.numeric_reports and all(
        isinstance(item, ReconciliationReportRecord) for item in first.numeric_reports
    )
    assert all(item.status == "PASS" for item in first.numeric_reports)
    assert first.differences == ()


@pytest.mark.parametrize(
    ("cash_delta", "expected_outcome", "expected_disposition"),
    (
        ("0.01", "LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW", "ACCEPTED_EXPLAINED"),
        ("0.02", "LOCAL_RECONCILIATION_REQUIRES_FIX", "REQUIRES_FIX"),
    ),
)
def test_reviewed_monetary_tolerance_boundary_is_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cash_delta: str,
    expected_outcome: str,
    expected_disposition: str,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module

    request, manifest, *_ = _request(tmp_path)
    local = module._build_local_projection(request, _dq_report(manifest))
    platform_cash = _reseal(
        local.cash,
        settled_cash_usd=local.cash.settled_cash_usd + Decimal(cash_delta),
    )
    assert isinstance(platform_cash, QCPlatformCashFact)
    platform = _reseal(local, cash=platform_cash)
    loaded = load_qc_qqq_options_local_reconciliation_policy(_active_policy_path(tmp_path))
    monkeypatch.setattr(
        module,
        "_load_platform_projection",
        lambda bundle, criteria: platform,
    )
    result = module._reconcile_loaded_bundle(
        request=request,
        manifest=manifest,
        policy=loaded.policy,
        policy_sha256=loaded.policy_sha256,
        input_sha256=build_qc_qqq_options_local_reconciliation_input_sha256(
            request, policy_sha256=loaded.policy_sha256
        ),
        bundle=_loaded_bundle_stub(),
    )

    assert result.outcome == expected_outcome
    difference = next(
        item for item in result.differences if item.check_id == "cash.settled_cash_usd"
    )
    assert difference.disposition == expected_disposition
    report = next(
        item for item in result.numeric_reports if item.check_id == "cash.settled_cash_usd"
    )
    assert report.delta == -Decimal(cash_delta)


def test_exact_order_state_mismatch_requires_fix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module

    request, manifest, *_ = _request(tmp_path)
    local = module._build_local_projection(request, _dq_report(manifest))
    changed = _reseal(local.orders[-1], order_state="CANCELED")
    platform = _reseal(local, orders=(*local.orders[:-1], changed))
    loaded = load_qc_qqq_options_local_reconciliation_policy(_active_policy_path(tmp_path))
    monkeypatch.setattr(
        module,
        "_load_platform_projection",
        lambda bundle, criteria: platform,
    )
    result = module._reconcile_loaded_bundle(
        request=request,
        manifest=manifest,
        policy=loaded.policy,
        policy_sha256=loaded.policy_sha256,
        input_sha256=build_qc_qqq_options_local_reconciliation_input_sha256(
            request, policy_sha256=loaded.policy_sha256
        ),
        bundle=_loaded_bundle_stub(),
    )

    assert result.outcome == "LOCAL_RECONCILIATION_REQUIRES_FIX"
    state_check = next(
        item
        for item in result.exact_checks
        if item.check_id.endswith("order_state") and item.status == "FAIL"
    )
    assert state_check.status == "FAIL"
    assert state_check.difference_class == "PLATFORM"


def test_manual_collection_error_becomes_typed_incomplete_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module
    from ai_trading_system.qqq_options_research.platform_evidence_bundle import (
        QCPlatformEvidenceBundleContractError,
    )

    request, *_ = _request(tmp_path)

    def _raise(*args: Any, **kwargs: Any) -> None:
        raise QCPlatformEvidenceBundleContractError(
            "MANUAL_COLLECTION_INCOMPLETE", "synthetic mandatory file missing"
        )

    monkeypatch.setattr(module, "load_qc_qqq_options_manual_evidence_bundle", _raise)
    result = reconcile_qc_qqq_options_local_evidence(
        request,
        package_root=tmp_path / "missing-bundle",
        capability_receipt_path=tmp_path / "missing-receipt.json",
        policy_path=_active_policy_path(tmp_path),
        capability_policy_path=tmp_path / "capability.yaml",
        capability_evidence_path=tmp_path / "evidence.yaml",
    )

    assert result.outcome == "LOCAL_RECONCILIATION_INCOMPLETE"
    assert result.reason_codes == ("MANUAL_COLLECTION_INCOMPLETE",)
    assert result.option_event_dq_status == "NOT_EVALUATED"


def test_reviewed_results_orders_and_trades_ingest_is_content_bound(
    tmp_path: Path,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module

    request, manifest, *_ = _request(tmp_path / "internal")
    projection = module._build_local_projection(request, _dq_report(manifest))
    loaded = load_qc_qqq_options_local_reconciliation_policy(_active_policy_path(tmp_path))
    assert isinstance(loaded.policy.criteria, ActiveLocalReconciliationCriteria)
    root = tmp_path / "bundle"
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True)
    results = {"aits_reconciliation_v1": projection.model_dump(mode="json")}
    (artifacts / "results.json").write_text(
        json.dumps(results, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    def _write_csv(path: Path, mappings: tuple[Any, ...], facts: tuple[Any, ...]) -> None:
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=[item.source_column for item in mappings])
            writer.writeheader()
            for fact in facts:
                payload = fact.model_dump(mode="json", exclude={"content_sha256"})
                writer.writerow(
                    {item.source_column: payload[item.canonical_field] for item in mappings}
                )

    _write_csv(
        artifacts / "orders.csv",
        loaded.policy.criteria.orders_csv_column_mapping,
        projection.orders,
    )
    _write_csv(
        artifacts / "trades.csv",
        loaded.policy.criteria.trades_csv_column_mapping,
        projection.fills,
    )
    file_sha256s = {
        path.relative_to(root).as_posix(): _sha(path.read_bytes()) for path in artifacts.iterdir()
    }
    bundle = cast(
        LoadedQCPlatformEvidenceBundle,
        SimpleNamespace(package_root=root, file_sha256s=file_sha256s),
    )

    replayed = module._load_platform_projection(bundle, loaded.policy.criteria)

    assert replayed == projection
    with (artifacts / "orders.csv").open("a", encoding="utf-8") as stream:
        stream.write("tamper\n")
    with pytest.raises(QCLocalReconciliationContractError) as exc:
        module._load_platform_projection(bundle, loaded.policy.criteria)
    assert exc.value.code == "LOCAL_RECONCILIATION_PLATFORM_FILE_TAMPERED"


def test_external_pass_never_overrides_internal_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ai_trading_system.qqq_options_research.local_reconciliation as module

    pass_request, pass_manifest, *_ = _request(tmp_path / "pass")
    projection = module._build_local_projection(pass_request, _dq_report(pass_manifest))
    request, manifest, *_ = _request(tmp_path / "failed", execution_dq_status="FAIL")
    monkeypatch.setattr(
        module,
        "load_qc_qqq_options_manual_evidence_bundle",
        lambda *args, **kwargs: _loaded_bundle_stub(),
    )
    monkeypatch.setattr(
        module,
        "_load_platform_projection",
        lambda loaded, criteria: projection,
    )

    result = reconcile_qc_qqq_options_local_evidence(
        request,
        package_root=tmp_path / "synthetic-bundle",
        capability_receipt_path=tmp_path / "receipt.json",
        policy_path=_active_policy_path(tmp_path),
        capability_policy_path=tmp_path / "capability.yaml",
        capability_evidence_path=tmp_path / "evidence.yaml",
    )

    assert result.outcome == "LOCAL_RECONCILIATION_INPUT_INVALID"
    assert result.external_pass_overrode_internal_failure is False
    assert result.local_dq_status == "FAIL"
    assert result.option_event_dq_status == "PASS"
    assert "INTERNAL_OR_OPTION_EVENT_DQ_PIT_NOT_PASS" in result.reason_codes


def test_exact_check_and_difference_status_are_fact_derived() -> None:
    common = {
        "schema_version": "qc_exact_reconciliation_check.v1",
        "check_id": "order.identity",
        "layer": "orders",
        "difference_class": "LOGIC",
        "local_value": "left",
        "platform_value": "right",
        "local_evidence_sha256": "1" * 64,
        "platform_evidence_sha256": "2" * 64,
        "explanation": "Exact values differ.",
    }
    with pytest.raises(ValidationError):
        QCExactReconciliationCheck.seal(**common, status="PASS")
    failed = QCExactReconciliationCheck.seal(**common, status="FAIL")
    assert QCExactReconciliationCheck.from_json_bytes(failed.canonical_bytes) == failed

    difference = QCReconciliationDifference.seal(
        schema_version="qc_reconciliation_difference.v1",
        check_id=failed.check_id,
        layer=failed.layer,
        difference_class="LOGIC",
        disposition="REQUIRES_FIX",
        owner="independent_validation",
        impact="Exact identity mismatch blocks reconciliation.",
        explanation="Local and platform order identities differ.",
        local_evidence_sha256=failed.local_evidence_sha256,
        platform_evidence_sha256=failed.platform_evidence_sha256,
    )
    assert QCReconciliationDifference.from_json_bytes(difference.canonical_bytes) == difference


@pytest.mark.parametrize(
    "difference_class",
    (
        "LOGIC",
        "PLATFORM",
        "PROVIDER",
        "TIMING",
        "REALITY_MODEL",
        "LICENSE",
        "MANUAL_COLLECTION",
    ),
)
def test_all_seven_difference_classes_are_typed(
    difference_class: QCReconciliationDifferenceClass,
) -> None:
    difference = QCReconciliationDifference.seal(
        schema_version="qc_reconciliation_difference.v1",
        check_id=f"typed.{difference_class.lower()}",
        layer="taxonomy",
        difference_class=difference_class,
        disposition="BLOCKED_EVIDENCE",
        owner="independent_validation",
        impact="Synthetic taxonomy coverage only.",
        explanation="No investment conclusion is formed.",
        local_evidence_sha256="3" * 64,
        platform_evidence_sha256="4" * 64,
    )
    assert difference.difference_class == difference_class


def test_platform_fact_rejects_noncanonical_json_and_invalid_identity() -> None:
    order = QCPlatformOrderFact.seal(
        schema_version="qc_platform_order_fact.v1",
        order_id="order-1",
        event_sequence=0,
        option_sid="QQQ-20210319-C-100",
        underlying="QQQ",
        side="BUY_TO_OPEN",
        order_contracts=1,
        filled_contracts_total=1,
        order_state="FILLED",
        limit_price_per_share="2.10",
        event_at_utc=datetime(2021, 3, 1, 15, 1, tzinfo=UTC),
    )
    assert QCPlatformOrderFact.from_json_bytes(order.canonical_bytes) == order
    noncanonical = json.dumps(order.model_dump(mode="json"), indent=2).encode() + b"\n"
    with pytest.raises(QCLocalReconciliationContractError) as exc:
        QCPlatformOrderFact.from_json_bytes(noncanonical)
    assert exc.value.code == "LOCAL_RECONCILIATION_RECORD_NONCANONICAL"


def test_primary_window_and_legacy_marker_cannot_drift(tmp_path: Path) -> None:
    source = PROJECT_ROOT / "config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["primary_research_start"] = date(2022, 12, 1)
    path = tmp_path / "legacy-default.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QCLocalReconciliationContractError) as exc:
        load_qc_qqq_options_local_reconciliation_policy(path)
    assert exc.value.code == "LOCAL_RECONCILIATION_POLICY_INVALID"
