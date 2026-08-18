from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.contracts import QQQOptionsContractError
from ai_trading_system.qqq_options_research.staged_dq_pit_readiness import (
    DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH,
    PlatformAttestedDerivedEvidence,
    QQQOptionsStagedReadinessDecision,
    ReadinessCheckEvidence,
    evaluate_qqq_options_staged_readiness,
    load_qqq_options_staged_readiness_policy,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_REPOSITORY_SHA = "d" * 40
_EVALUATED = datetime(2026, 8, 18, 14, 0, tzinfo=UTC)


def _checks(
    overrides: dict[str, tuple[str, str | None]] | None = None,
) -> tuple[ReadinessCheckEvidence, ...]:
    policy = load_qqq_options_staged_readiness_policy().policy
    changes = overrides or {}
    return tuple(
        ReadinessCheckEvidence(
            check_id=check_id,
            status=changes.get(check_id, ("PASS", None))[0],  # type: ignore[arg-type]
            reason_code=changes.get(check_id, ("PASS", None))[1],
        )
        for check_id in policy.required_check_ids
    )


def _attestation(
    *, source_report_content_sha256: str = _SHA_A, **updates: Any
) -> PlatformAttestedDerivedEvidence:
    policy = load_qqq_options_staged_readiness_policy().policy
    payload: dict[str, Any] = {
        "schema_version": "platform_attested_derived_evidence.v1",
        "provider": "QuantConnect",
        "provider_checksum_availability": "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
        "provider_raw_checksum_claimed": False,
        "derived_evidence_export_classification": "EXPORT_ALLOWED_DERIVED",
        "raw_option_field_classification": "QC_ONLY_NOT_EXPORTED",
        "raw_option_rows_included": False,
        "account_or_broker_identifiers_included": False,
        "platform_identity_status": "CONFIRMED",
        "tier_status": "CONFIRMED",
        "engine_identity_status": "CONFIRMED",
        "evidence_manifest_status": "CONFIRMED",
        "platform_tier": "research-paid",
        "engine_id": "lean-engine-v2",
        "bundle_id": "bundle-2534",
        "project_id": 34808569,
        "backtest_id": "backtest-2534",
        "repository_code_sha": _REPOSITORY_SHA,
        "shared_contract_sha256": policy.shared_contract_sha256,
        "base_dq_policy_file_sha256": policy.base_dq_policy_file_sha256,
        "base_dq_evaluator_file_sha256": policy.base_dq_evaluator_file_sha256,
        "source_report_content_sha256": source_report_content_sha256,
        "derived_evidence_content_sha256": _SHA_B,
        "evidence_manifest_content_sha256": _SHA_C,
        "requested_start": date(2021, 2, 22),
        "requested_end": date(2025, 12, 2),
        "expected_session_count": 1202,
        "observed_session_count": 1202,
        "deterministic_replay_status": "PASS",
        "license_state": "CONFIRMED_EXPORT_SAFE_DERIVED",
        "attested_at_utc": _EVALUATED,
    }
    payload.update(updates)
    return PlatformAttestedDerivedEvidence(**payload)


def _evaluate(
    *,
    checks: tuple[ReadinessCheckEvidence, ...] | None = None,
    derived_evidence: PlatformAttestedDerivedEvidence | None = None,
) -> QQQOptionsStagedReadinessDecision:
    return evaluate_qqq_options_staged_readiness(
        source_report_content_sha256=_SHA_A,
        checks=checks or _checks(),
        evaluated_at_utc=_EVALUATED,
        derived_evidence=derived_evidence,
    )


def _canonical_reseal(payload: dict[str, Any]) -> bytes:
    body = deepcopy(payload)
    body.pop("content_sha256", None)
    payload["content_sha256"] = hashlib.sha256(
        (json.dumps(body, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    ).hexdigest()
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def test_policy_binds_frozen_2482_bytes_and_safety_boundary() -> None:
    loaded = load_qqq_options_staged_readiness_policy()

    assert loaded.policy_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()
    assert loaded.policy.stage_order == (
        "DATA_RESEARCH",
        "SHADOW_SELECTION",
        "EXECUTION",
    )
    assert sum(map(len, loaded.policy.stage_required_checks.values())) == 15
    assert loaded.policy.external_action_authorized is False
    assert loaded.policy.current_window_session_exclusion_allowed is False
    assert loaded.policy.safety.strategy_execution_allowed is False
    assert set(loaded.policy.numeric_thresholds.model_dump().values()) == {
        "UNKNOWN_REQUIRES_POLICY_REVIEW"
    }


def test_all_pass_checks_are_ready_in_order_without_authorizing_action() -> None:
    decision = _evaluate()

    assert decision.source_evidence_route == "PROVIDER_RAW_CHECKSUM"
    assert tuple(stage.status for stage in decision.stages) == (
        "READY",
        "READY",
        "READY",
    )
    assert decision.external_action_authorized is False
    assert decision.production_effect == "none"
    assert decision.broker_action == "none"


def test_stage_separation_does_not_require_order_fill_before_data_research() -> None:
    decision = _evaluate(
        checks=_checks(
            {
                "signal_selection_chronology": (
                    "NOT_EVALUATED",
                    "SIGNAL_SELECTION_CHRONOLOGY_MISSING",
                ),
                "fill_forward_ambiguity": (
                    "NOT_EVALUATED",
                    "FILL_FORWARD_STATUS_UNKNOWN",
                ),
                "order_fill_chronology": (
                    "NOT_EVALUATED",
                    "ORDER_FILL_CHRONOLOGY_MISSING",
                ),
            }
        )
    )

    assert tuple(stage.status for stage in decision.stages) == (
        "READY",
        "NOT_READY",
        "BLOCKED",
    )
    assert "order_fill_chronology" not in decision.stages[0].required_check_ids
    assert "PREDECESSOR_STAGE_NOT_READY" in decision.stages[2].reason_codes


def test_platform_attested_route_satisfies_source_evidence_without_rewriting_check() -> None:
    checks = _checks(
        {
            "provider_raw_checksum": (
                "NOT_EVALUATED",
                "PROVIDER_RAW_CHECKSUM_UNAVAILABLE",
            )
        }
    )
    decision = _evaluate(checks=checks, derived_evidence=_attestation())

    assert decision.source_evidence_route == "PLATFORM_ATTESTED_DERIVED"
    assert decision.derived_evidence == _attestation()
    assert tuple(stage.status for stage in decision.stages) == (
        "READY",
        "READY",
        "READY",
    )
    source_check = next(
        check for check in decision.source_checks if check.check_id == "provider_raw_checksum"
    )
    assert source_check.status == "NOT_EVALUATED"
    assert source_check.reason_code == "PROVIDER_RAW_CHECKSUM_UNAVAILABLE"
    assert "provider_raw_checksum" in decision.stages[0].satisfied_check_ids


def test_alternate_route_identity_mismatch_remains_not_ready() -> None:
    checks = _checks(
        {
            "provider_raw_checksum": (
                "NOT_EVALUATED",
                "PROVIDER_RAW_CHECKSUM_UNAVAILABLE",
            )
        }
    )
    decision = _evaluate(
        checks=checks,
        derived_evidence=_attestation(source_report_content_sha256=_SHA_B),
    )

    assert decision.source_evidence_route == "UNSATISFIED"
    assert decision.stages[0].status == "NOT_READY"
    assert "provider_raw_checksum" in decision.stages[0].not_evaluated_check_ids


def test_passing_provider_checksum_rejects_ambiguous_alternate_evidence() -> None:
    with pytest.raises(QQQOptionsContractError, match="AMBIGUOUS_SOURCE_ROUTE"):
        _evaluate(derived_evidence=_attestation())


def test_2533_canonical_replay_keeps_all_stages_blocked() -> None:
    path = Path(
        "inputs/research/qqq_options/"
        "trading_2533_session_finalization_dq_pit_evidence_admission_v1/"
        "dq_pit_evidence_admission.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    checks = tuple(
        ReadinessCheckEvidence(
            check_id=item["check_id"],
            status=item["status"],
            reason_code=item["reason_code"],
        )
        for item in payload["required_checks"]
    )
    decision = evaluate_qqq_options_staged_readiness(
        source_report_content_sha256=payload["content_sha256"],
        checks=checks,
        evaluated_at_utc=datetime.fromisoformat(payload["admission_as_of_utc"]),
    )

    assert tuple(stage.status for stage in decision.stages) == (
        "BLOCKED",
        "BLOCKED",
        "BLOCKED",
    )
    assert all("chain_presence" in stage.failed_check_ids for stage in decision.stages)
    assert decision.source_evidence_route == "UNSATISFIED"


def test_check_set_must_be_complete_unique_and_canonical() -> None:
    checks = _checks()
    with pytest.raises(QQQOptionsContractError, match="CHECK_SET_DRIFT"):
        _evaluate(checks=checks[:-1])
    with pytest.raises(QQQOptionsContractError, match="CHECK_SET_DRIFT"):
        _evaluate(checks=tuple(reversed(checks)))


def test_strict_attestation_rejects_extra_bad_hash_float_naive_and_incomplete_scope() -> None:
    base = _attestation().model_dump()
    invalid_payloads = []
    extra = dict(base)
    extra["unexpected"] = True
    invalid_payloads.append(extra)
    bad_hash = dict(base)
    bad_hash["derived_evidence_content_sha256"] = "bad"
    invalid_payloads.append(bad_hash)
    float_project = dict(base)
    float_project["project_id"] = 34808569.0
    invalid_payloads.append(float_project)
    naive = dict(base)
    naive["attested_at_utc"] = datetime(2026, 8, 18, 14, 0)
    invalid_payloads.append(naive)
    incomplete = dict(base)
    incomplete["observed_session_count"] = 1201
    invalid_payloads.append(incomplete)

    for payload in invalid_payloads:
        with pytest.raises(ValidationError):
            PlatformAttestedDerivedEvidence(**payload)


def test_decision_is_byte_deterministic_and_tamper_evident() -> None:
    first = _evaluate()
    second = _evaluate()

    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.content_sha256 == first.compute_content_sha256()
    assert QQQOptionsStagedReadinessDecision.from_json_bytes(first.canonical_bytes) == first

    noncanonical = first.canonical_bytes.replace(b'": "', b'":"', 1)
    with pytest.raises(QQQOptionsContractError, match="NOT_CANONICAL"):
        QQQOptionsStagedReadinessDecision.from_json_bytes(noncanonical)

    payload = json.loads(first.canonical_bytes)
    payload["source_report_content_sha256"] = _SHA_B
    tampered = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    with pytest.raises(QQQOptionsContractError, match="INVALID"):
        QQQOptionsStagedReadinessDecision.from_json_bytes(tampered)


def test_seal_rejects_forged_route_and_stage_applicability() -> None:
    decision = _evaluate()
    payload = json.loads(decision.canonical_bytes)
    payload["source_evidence_route"] = "PLATFORM_ATTESTED_DERIVED"
    with pytest.raises(QQQOptionsContractError, match="platform-attested route"):
        QQQOptionsStagedReadinessDecision.from_json_bytes(_canonical_reseal(payload))

    payload = json.loads(decision.canonical_bytes)
    payload["stages"][0]["required_check_ids"] = [
        item for item in payload["stages"][0]["required_check_ids"] if item != "chain_presence"
    ]
    payload["stages"][0]["satisfied_check_ids"] = [
        item for item in payload["stages"][0]["satisfied_check_ids"] if item != "chain_presence"
    ]
    with pytest.raises(QQQOptionsContractError, match="check applicability drifted"):
        QQQOptionsStagedReadinessDecision.from_json_bytes(_canonical_reseal(payload))


def test_policy_rejects_threshold_and_base_identity_drift(tmp_path: Path) -> None:
    source = Path(DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH)
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    variants = []
    threshold = deepcopy(payload)
    threshold["numeric_thresholds"]["max_quote_age_seconds"] = 30
    variants.append(threshold)
    identity = deepcopy(payload)
    identity["base_dq_policy_file_sha256"] = _SHA_A
    variants.append(identity)

    for index, variant in enumerate(variants):
        path = tmp_path / f"policy-{index}.yaml"
        path.write_text(yaml.safe_dump(variant, sort_keys=False), encoding="utf-8")
        with pytest.raises(QQQOptionsContractError, match="POLICY_INVALID"):
            load_qqq_options_staged_readiness_policy(path)
