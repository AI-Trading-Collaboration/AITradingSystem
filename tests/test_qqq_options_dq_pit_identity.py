from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
    PlatformEvidenceManifestRecord,
    QQQOptionsContractError,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH,
    LocalCachedDataGateDeclaration,
    QQQOptionsCacheIdentityMaterial,
    QQQOptionsCacheIdentityReceipt,
    QQQOptionsDQObservation,
    SourceChecksumEvidence,
    build_qqq_options_cache_identity,
    evaluate_qqq_options_dq_pit_identity,
    load_qqq_options_dq_pit_identity_policy,
)
from ai_trading_system.qqq_options_research.policy import (
    load_qqq_options_shared_contract_policy,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_SHA_D = "d" * 64
_REPOSITORY_SHA = "e" * 40
_SELECTION = datetime(2021, 2, 24, 14, 31, tzinfo=UTC)
_OBSERVED = datetime(2021, 2, 24, 14, 35, tzinfo=UTC)
_CREATED = datetime(2021, 2, 24, 14, 36, tzinfo=UTC)


def _safety() -> dict[str, object]:
    return {
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "raw_options_data_export_allowed": False,
        "strategy_execution_allowed": False,
        "bounded_cloud_pilot_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _shared_envelope(schema_name: str, record_id: str) -> dict[str, Any]:
    shared = load_qqq_options_shared_contract_policy()
    return {
        "schema_name": schema_name,
        "schema_version": "1.0.0",
        "run_id": "run-20210224",
        "record_id": record_id,
        "created_at_utc": _CREATED,
        "producer_version": "test.v1",
        "repository_code_sha": _REPOSITORY_SHA,
        "policy_id": shared.policy.policy_id,
        "policy_version": shared.policy.policy_version,
        "policy_sha256": shared.policy_sha256,
        "contract_schema_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "source_ids": ("qqq.signal.daily", "qqq.source.manifest"),
        "source_checksums": (_SHA_A, _SHA_B),
        "requested_start": date(2021, 2, 22),
        "requested_end": date(2021, 2, 26),
        "evaluated_start": date(2021, 2, 22),
        "evaluated_end": date(2021, 2, 24),
        "storage_timezone": "UTC",
        "exchange_timezone": "America/New_York",
        "dq_status": "PASS",
        "pit_status": "PASS",
        "export_classification": "EXPORT_ALLOWED_DERIVED",
        "lineage_id": "lineage-20210224",
        "safety": _safety(),
    }


def _source_record() -> RunManifestRecord:
    return RunManifestRecord.seal(
        **_shared_envelope("run_manifest", "manifest-1"),
        underlying="QQQ",
        initial_cash_usd=Decimal("100000"),
        account_currency="USD",
        account_type="CASH",
        signal_resolution="DAILY",
        execution_resolution="MINUTE",
        signal_artifact_sha256=_SHA_C,
        engine_identity_status="CONFIRMED",
        engine_identity="LEAN-test-engine",
        evidence_admission_decision="CAPABILITY_OR_LICENSE_BLOCKED",
    )


def _platform_manifest(*, artifact_sha: str = _SHA_A) -> PlatformEvidenceManifestRecord:
    return PlatformEvidenceManifestRecord.seal(
        **_shared_envelope("platform_evidence_manifest", "bundle-1"),
        bundle_id="bundle-1",
        platform="QuantConnect",
        backtest_id="backtest-1",
        tier_status="CONFIRMED",
        engine_identity_status="CONFIRMED",
        collected_at_utc=_OBSERVED - timedelta(minutes=1),
        collected_by="offline_fixture",
        artifacts=(
            {
                "artifact_id": "summary",
                "locator": "outputs/summary.json",
                "sha256": artifact_sha,
                "byte_count": 12,
                "export_classification": "EXPORT_ALLOWED_DERIVED",
            },
        ),
        limitations=("Synthetic offline fixture; no external action.",),
        raw_option_rows_included=False,
        account_or_broker_identifiers_included=False,
    )


def _identity(
    identity_id: str,
    *,
    assessment: str = "PASS",
    expected_sha: str = _SHA_A,
    observed_sha: str | None = _SHA_A,
) -> dict[str, Any]:
    return {
        "assessment": assessment,
        "expected_id": identity_id,
        "expected_version": "1.0.0",
        "expected_sha256": expected_sha,
        "observed_id": identity_id if observed_sha is not None else None,
        "observed_version": "1.0.0" if observed_sha is not None else None,
        "observed_sha256": observed_sha,
    }


def _cache_material_payload() -> dict[str, Any]:
    policy = load_qqq_options_dq_pit_identity_policy()
    return {
        "provider": "QuantConnect",
        "dataset": "USOptions",
        "underlying": "QQQ",
        "option_sid": "QQQ-20210319-C-325",
        "resolution": "MINUTE",
        "requested_start": date(2021, 2, 22),
        "requested_end": date(2021, 2, 26),
        "calendar_identity": _identity("xnys.calendar"),
        "mapping_identity": _identity("qqq.mapping"),
        "normalization_identity": _identity("raw.normalization"),
        "dq_policy_sha256": policy.policy_sha256,
        "shared_contract_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "repository_code_sha": _REPOSITORY_SHA,
        "engine_identity": _identity("lean.engine"),
        "source_checksum_evidence": {
            "availability": "AVAILABLE",
            "sha256": _SHA_D,
            "export_classification": "QC_ONLY_NOT_EXPORTED",
        },
    }


def _observation_payload() -> dict[str, Any]:
    manifest = _platform_manifest()
    return {
        "observed_at_utc": _OBSERVED,
        "chain_present": True,
        "candidate_present": True,
        "quote_bid_per_share": Decimal("5.10"),
        "quote_ask_per_share": Decimal("5.20"),
        "quote_end_utc": _SELECTION - timedelta(seconds=1),
        "quote_freshness_assessment": "PASS",
        "selection_session": date(2021, 2, 24),
        "expected_prior_session": date(2021, 2, 23),
        "prior_day_model_as_of_session": date(2021, 2, 23),
        "model_freshness_assessment": "PASS",
        "open_interest_as_of_session": date(2021, 2, 23),
        "open_interest_freshness_assessment": "PASS",
        "exchange_calendar_identity": _identity("xnys.calendar"),
        "symbol_mapping_identity": _identity("qqq.mapping"),
        "signal_as_of_utc": datetime(2021, 2, 22, 21, 0, tzinfo=UTC),
        "selection_snapshot_utc": _SELECTION,
        "order_intent_utc": _SELECTION + timedelta(minutes=1),
        "order_submit_utc": _SELECTION + timedelta(minutes=1),
        "fill_quote_end_utc": _SELECTION + timedelta(minutes=2),
        "fill_utc": _SELECTION + timedelta(minutes=2, seconds=1),
        "fill_forward_assessment": "PASS",
        "cache_key": "qc.qqq.options.minute.20210222.20210226",
        "prior_cache_identity_sha256": None,
        "cache_material": _cache_material_payload(),
        "engine_identity": _identity("lean.engine"),
        "evidence_identity": _identity(
            "qc.bundle",
            expected_sha=manifest.content_sha256,
            observed_sha=manifest.content_sha256,
        ),
        "platform_evidence_manifest": manifest,
        "local_cached_data_gate": {
            "status": "FAIL",
            "scope": "CACHED_MARKET_MACRO",
            "as_of_utc": _OBSERVED - timedelta(minutes=2),
            "report_locator": "outputs/data_quality_report.json",
            "report_sha256": _SHA_B,
        },
    }


def _observation(**updates: Any) -> QQQOptionsDQObservation:
    payload = _observation_payload()
    payload.update(updates)
    return QQQOptionsDQObservation(**payload)


def _evaluate(observation: QQQOptionsDQObservation | None = None):
    return evaluate_qqq_options_dq_pit_identity(
        source_record=_source_record(),
        observation=observation or _observation(),
        record_id="dq-report-1",
        created_at_utc=_CREATED,
        producer_version="test.v1",
        lineage_id="dq-lineage-20210224",
    )


def _checks(report: DQReportRecord) -> dict[str, Any]:
    return {check.check_id: check for check in report.checks}


def test_policy_exactly_binds_2481_and_keeps_numeric_thresholds_unknown() -> None:
    loaded = load_qqq_options_dq_pit_identity_policy()
    shared = load_qqq_options_shared_contract_policy()

    assert loaded.policy_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()
    assert loaded.policy.shared_contract_sha256 == QQQ_OPTIONS_CONTRACT_SHA256
    assert loaded.policy.shared_policy_sha256 == shared.policy_sha256
    assert set(loaded.policy.numeric_thresholds.model_dump().values()) == {
        "UNKNOWN_REQUIRES_POLICY_REVIEW"
    }
    assert loaded.policy.unknown_can_pass is False
    assert loaded.policy.local_cache_dq_substitution_allowed is False


def test_cache_identity_is_deterministic_canonical_and_tamper_evident() -> None:
    material = QQQOptionsCacheIdentityMaterial(**_cache_material_payload())
    first = build_qqq_options_cache_identity(cache_key="cache-1", material=material)
    second = build_qqq_options_cache_identity(cache_key="cache-1", material=material)

    assert first == second
    assert first.identity_sha256 == first.compute_identity_sha256()
    assert QQQOptionsCacheIdentityReceipt.from_json_bytes(first.canonical_bytes) == first

    noncanonical = first.canonical_bytes.replace(
        b'"cache_key": "cache-1"', b'"cache_key":"cache-1"'
    )
    tampered = first.canonical_bytes.replace(b'"provider": "QuantConnect"', b'"provider": "Other"')
    with pytest.raises(QQQOptionsContractError, match="NOT_CANONICAL"):
        QQQOptionsCacheIdentityReceipt.from_json_bytes(noncanonical)
    with pytest.raises(QQQOptionsContractError, match="INVALID"):
        QQQOptionsCacheIdentityReceipt.from_json_bytes(tampered)


def test_all_pass_option_event_report_does_not_overwrite_local_cache_fail() -> None:
    result = _evaluate()

    assert result.report.dq_status == "PASS"
    assert result.report.pit_status == "PASS"
    assert all(check.status == "PASS" for check in result.report.checks)
    assert result.local_cached_data_gate.status == "FAIL"
    assert result.report.policy_sha256 == result.policy_sha256
    assert result.report.contract_schema_sha256 == QQQ_OPTIONS_CONTRACT_SHA256
    assert result.report.export_classification == "EXPORT_ALLOWED_DERIVED"
    assert DQReportRecord.from_json_bytes(result.report.canonical_bytes) == result.report


@pytest.mark.parametrize(
    ("updates", "check_id", "reason_code"),
    (
        (
            {
                "quote_bid_per_share": None,
                "quote_ask_per_share": None,
                "quote_end_utc": None,
            },
            "quote_integrity",
            "QUOTE_MISSING",
        ),
        ({"quote_ask_per_share": None}, "quote_integrity", "QUOTE_SINGLE_SIDED"),
        (
            {"quote_bid_per_share": Decimal("-0.01")},
            "quote_integrity",
            "QUOTE_NEGATIVE_BID",
        ),
        (
            {"quote_ask_per_share": Decimal("0")},
            "quote_integrity",
            "QUOTE_ZERO_ASK",
        ),
        (
            {
                "quote_bid_per_share": Decimal("5.30"),
                "quote_ask_per_share": Decimal("5.20"),
            },
            "quote_integrity",
            "QUOTE_CROSSED",
        ),
        (
            {"quote_end_utc": _SELECTION + timedelta(seconds=1)},
            "quote_integrity",
            "QUOTE_AFTER_SELECTION",
        ),
        (
            {"quote_freshness_assessment": "FAIL"},
            "quote_freshness",
            "QUOTE_FRESHNESS_FAIL",
        ),
    ),
)
def test_quote_integrity_and_freshness_fail_closed(
    updates: dict[str, Any], check_id: str, reason_code: str
) -> None:
    result = _evaluate(_observation(**updates))

    assert result.report.dq_status == "FAIL"
    assert _checks(result.report)[check_id].reason_code == reason_code


@pytest.mark.parametrize(
    ("updates", "check_id", "status", "reason_code"),
    (
        (
            {"prior_day_model_as_of_session": None},
            "prior_day_model_freshness",
            "FAIL",
            "MODEL_AS_OF_MISSING",
        ),
        (
            {"prior_day_model_as_of_session": date(2021, 2, 24)},
            "prior_day_model_freshness",
            "FAIL",
            "MODEL_SESSION_NOT_EXACT_PRIOR",
        ),
        (
            {"model_freshness_assessment": "UNKNOWN_REQUIRES_POLICY_REVIEW"},
            "prior_day_model_freshness",
            "NOT_EVALUATED",
            "MODEL_FRESHNESS_UNKNOWN",
        ),
        (
            {"open_interest_as_of_session": None},
            "open_interest_freshness",
            "FAIL",
            "OI_AS_OF_MISSING",
        ),
        (
            {"open_interest_as_of_session": date(2021, 2, 21)},
            "open_interest_freshness",
            "FAIL",
            "OI_SESSION_NOT_EXACT_PRIOR",
        ),
        (
            {"open_interest_freshness_assessment": "UNKNOWN_REQUIRES_POLICY_REVIEW"},
            "open_interest_freshness",
            "NOT_EVALUATED",
            "OI_FRESHNESS_UNKNOWN",
        ),
    ),
)
def test_daily_model_and_oi_require_exact_prior_session_and_known_freshness(
    updates: dict[str, Any], check_id: str, status: str, reason_code: str
) -> None:
    result = _evaluate(_observation(**updates))
    check = _checks(result.report)[check_id]

    assert check.status == status
    assert check.reason_code == reason_code
    assert result.report.pit_status != "PASS"
    assert result.report.dq_status != "PASS"


@pytest.mark.parametrize(
    ("updates", "check_id", "reason_code"),
    (
        (
            {"signal_as_of_utc": _SELECTION},
            "signal_selection_chronology",
            "SIGNAL_SELECTION_CHRONOLOGY_INVALID",
        ),
        (
            {"order_intent_utc": _SELECTION},
            "order_fill_chronology",
            "ORDER_FILL_CHRONOLOGY_INVALID",
        ),
        (
            {"fill_quote_end_utc": _SELECTION + timedelta(minutes=1)},
            "order_fill_chronology",
            "ORDER_FILL_CHRONOLOGY_INVALID",
        ),
        (
            {"fill_forward_assessment": "FAIL"},
            "fill_forward_ambiguity",
            "FILL_FORWARD_AMBIGUITY",
        ),
    ),
)
def test_same_bar_future_fill_and_fill_forward_ambiguity_fail_pit(
    updates: dict[str, Any], check_id: str, reason_code: str
) -> None:
    result = _evaluate(_observation(**updates))

    assert result.report.pit_status == "FAIL"
    assert _checks(result.report)[check_id].reason_code == reason_code


def test_missing_chronology_is_not_evaluated_and_never_passes() -> None:
    result = _evaluate(_observation(order_submit_utc=None))

    check = _checks(result.report)["order_fill_chronology"]
    assert check.status == "NOT_EVALUATED"
    assert check.reason_code == "ORDER_FILL_CHRONOLOGY_MISSING"
    assert result.report.dq_status == "NOT_EVALUATED"
    assert result.report.pit_status == "NOT_EVALUATED"


@pytest.mark.parametrize(
    ("field", "check_id", "reason_code"),
    (
        (
            "exchange_calendar_identity",
            "exchange_calendar_identity",
            "EXCHANGE_CALENDAR_IDENTITY_MISMATCH",
        ),
        (
            "symbol_mapping_identity",
            "symbol_mapping_identity",
            "SYMBOL_MAPPING_IDENTITY_MISMATCH",
        ),
        ("engine_identity", "engine_identity", "ENGINE_IDENTITY_MISMATCH"),
    ),
)
def test_calendar_mapping_and_engine_identity_drift_fail_closed(
    field: str, check_id: str, reason_code: str
) -> None:
    result = _evaluate(
        _observation(**{field: _identity(f"{field}.v1", observed_sha=_SHA_B)})
    )

    assert result.report.dq_status == "FAIL"
    assert _checks(result.report)[check_id].reason_code == reason_code


def test_cache_collision_and_policy_or_contract_drift_are_typed_failures() -> None:
    baseline = _evaluate()
    collision = _evaluate(
        _observation(prior_cache_identity_sha256="f" * 64)
    )
    assert _checks(collision.report)["cache_identity"].reason_code == "CACHE_IDENTITY_COLLISION"

    policy_material = _cache_material_payload()
    policy_material["dq_policy_sha256"] = _SHA_B
    policy_drift = _evaluate(_observation(cache_material=policy_material))
    assert (
        _checks(policy_drift.report)["cache_identity"].reason_code
        == "CACHE_POLICY_IDENTITY_MISMATCH"
    )

    contract_material = _cache_material_payload()
    contract_material["shared_contract_sha256"] = _SHA_C
    contract_drift = _evaluate(_observation(cache_material=contract_material))
    assert (
        _checks(contract_drift.report)["cache_identity"].reason_code
        == "CACHE_CONTRACT_IDENTITY_MISMATCH"
    )
    assert baseline.report.dq_status == "PASS"


def test_provider_raw_checksum_unavailable_is_explicit_and_not_fabricated() -> None:
    material = _cache_material_payload()
    material["source_checksum_evidence"] = {
        "availability": "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
        "sha256": None,
        "export_classification": "QC_ONLY_NOT_EXPORTED",
    }
    result = _evaluate(_observation(cache_material=material))

    check = _checks(result.report)["provider_raw_checksum"]
    assert check.status == "NOT_EVALUATED"
    assert check.reason_code == "PROVIDER_RAW_CHECKSUM_UNAVAILABLE"
    assert "qqq.options.provider_raw" not in result.report.source_ids
    assert result.report.dq_status == "NOT_EVALUATED"

    with pytest.raises(ValidationError, match="cannot be fabricated"):
        SourceChecksumEvidence(
            availability="UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
            sha256=_SHA_A,
            export_classification="QC_ONLY_NOT_EXPORTED",
        )


def test_missing_or_unconfirmed_evidence_and_unknown_engine_never_pass() -> None:
    missing = _evaluate(_observation(platform_evidence_manifest=None))
    assert _checks(missing.report)["evidence_identity"].reason_code == "EVIDENCE_MANIFEST_MISSING"
    assert missing.report.dq_status == "NOT_EVALUATED"

    unknown = _evaluate(
        _observation(
            engine_identity=_identity(
                "lean.engine",
                assessment="UNKNOWN_REQUIRES_POLICY_REVIEW",
                observed_sha=None,
            )
        )
    )
    assert _checks(unknown.report)["engine_identity"].reason_code == "ENGINE_IDENTITY_UNKNOWN"
    assert unknown.report.dq_status == "NOT_EVALUATED"


def test_strict_inputs_reject_float_naive_time_extra_and_invalid_local_scope() -> None:
    float_payload = _observation_payload()
    float_payload["quote_bid_per_share"] = 5.10
    naive_payload = _observation_payload()
    naive_payload["selection_snapshot_utc"] = datetime(2021, 2, 24, 14, 31)
    extra_payload = _observation_payload()
    extra_payload["undocumented_field"] = "drift"

    for payload in (float_payload, naive_payload, extra_payload):
        with pytest.raises(ValidationError):
            QQQOptionsDQObservation(**payload)

    with pytest.raises(ValidationError, match="scoped report evidence"):
        LocalCachedDataGateDeclaration(
            status="PASS",
            scope="CACHED_MARKET_MACRO",
            as_of_utc=None,
            report_locator=None,
            report_sha256=None,
        )


def test_policy_rejects_numeric_threshold_hash_and_unknown_field_drift(tmp_path: Path) -> None:
    source = DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH.read_text(encoding="utf-8")

    numeric_payload = yaml.safe_load(source)
    numeric_payload["numeric_thresholds"]["max_quote_age_seconds"] = 60
    numeric_path = tmp_path / "numeric.yaml"
    numeric_path.write_text(yaml.safe_dump(numeric_payload, sort_keys=False), encoding="utf-8")

    hash_payload = yaml.safe_load(source)
    hash_payload["shared_contract_sha256"] = _SHA_B
    hash_path = tmp_path / "hash.yaml"
    hash_path.write_text(yaml.safe_dump(hash_payload, sort_keys=False), encoding="utf-8")

    extra_payload = yaml.safe_load(source)
    extra_payload["temporary_fallback"] = True
    extra_path = tmp_path / "extra.yaml"
    extra_path.write_text(yaml.safe_dump(extra_payload, sort_keys=False), encoding="utf-8")

    for path in (numeric_path, hash_path, extra_path):
        with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_DQ_PIT_POLICY_INVALID"):
            load_qqq_options_dq_pit_identity_policy(path)


def test_cache_material_rejects_reversed_range_and_bad_code_hash() -> None:
    reversed_payload = _cache_material_payload()
    reversed_payload["requested_start"] = date(2021, 2, 27)
    bad_hash_payload = _cache_material_payload()
    bad_hash_payload["repository_code_sha"] = "A" * 40

    with pytest.raises(ValidationError, match="range is reversed"):
        QQQOptionsCacheIdentityMaterial(**reversed_payload)
    with pytest.raises(ValidationError, match="Git object SHA"):
        QQQOptionsCacheIdentityMaterial(**bad_hash_payload)


def test_report_and_cache_identity_are_byte_deterministic_across_repeated_evaluation() -> None:
    first = _evaluate()
    second = _evaluate()

    assert first.policy_sha256 == second.policy_sha256
    assert first.cache_identity.canonical_bytes == second.cache_identity.canonical_bytes
    assert first.report.canonical_bytes == second.report.canonical_bytes
    assert first.report.content_sha256 == second.report.content_sha256


def test_observation_copy_cannot_hide_cache_material_mutation() -> None:
    payload = _observation_payload()
    baseline = deepcopy(payload["cache_material"])
    payload["cache_material"]["dataset"] = "OtherDataset"
    mutated = _evaluate(QQQOptionsDQObservation(**payload))
    original = _evaluate(_observation(cache_material=baseline))

    assert mutated.cache_identity.identity_sha256 != original.cache_identity.identity_sha256
