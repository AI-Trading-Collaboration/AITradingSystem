from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.contracts.qc_qqq_options_capability_discovery_evidence import (
    CAPABILITY_DISCOVERY_RESULT_SURFACES,
    QCCapabilityDiscoveryEvidence,
    QCCapabilityDiscoveryEvidenceContractError,
)
from ai_trading_system.qqq_options_capability_discovery_evidence import (
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH,
    load_qc_qqq_options_capability_discovery_evidence,
)


def _tracked_payload() -> dict[str, object]:
    return json.loads(
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH.read_text(
            encoding="utf-8"
        )
    )


def _canonical_bytes(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def test_tracked_capability_discovery_evidence_is_exact_and_cash_preserving() -> None:
    first = load_qc_qqq_options_capability_discovery_evidence()
    second = load_qc_qqq_options_capability_discovery_evidence()
    evidence = first.evidence

    assert first.evidence_file_sha256 == (
        "2d4c14e23d8b8f824d5b4f93db257f6d4852af31a12966535d21cc5d26a4807a"
    )
    assert first.evidence_canonical_sha256 == first.evidence_file_sha256
    assert first.evidence == second.evidence
    assert evidence.content_sha256 == (
        "bd00355e1609c591778f53f745ca2762b9da83542ee602f2faec58cc11662702"
    )
    assert evidence.repository_code_sha == "2db95f6422bfccb4d53876ad4b3e86912fff1309"
    assert evidence.project_code_sha256 == (
        "fcf2c8f4717a47bb685d8ea54d241092f525f24899585a0b9c26c6b73b1af86c"
    )
    assert evidence.option_contract_count == 48
    assert evidence.two_sided_quote_count == 48
    assert evidence.open_interest_nonzero_count == 48
    assert evidence.total_orders == evidence.fills == evidence.holdings == 0
    assert evidence.start_equity_usd == evidence.end_equity_usd
    assert tuple(item.surface_id for item in evidence.result_surfaces) == (
        CAPABILITY_DISCOVERY_RESULT_SURFACES
    )
    assert evidence.independent_review_status == "PENDING_OWNER_REVIEW"
    assert evidence.bounded_pilot_preparation_allowed is False
    assert evidence.option_event_dq_status == "NOT_EVALUATED"
    assert evidence.option_event_pit_status == "NOT_EVALUATED"


def test_evidence_replays_the_exact_authorization_overlay() -> None:
    loaded = load_qc_qqq_options_capability_discovery_evidence()
    authorization = loaded.authorization

    assert loaded.evidence.authorization_policy_sha256 == (
        authorization.authorization_policy_sha256
    )
    assert loaded.evidence.authorization_canonical_sha256 == (
        authorization.authorization_canonical_sha256
    )
    assert loaded.evidence.owner_authorization_id == (
        authorization.authorization.owner_authorization_id
    )
    assert loaded.evidence.requested_start == (
        authorization.authorization.scope.requested_start
    )
    assert loaded.evidence.requested_end == authorization.authorization.scope.requested_end


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("total_orders", 1),
        ("fills", 1),
        ("portfolio_invested", True),
        ("independent_review_status", "REVIEWED"),
    ),
)
def test_order_position_or_premature_review_tamper_fails_closed(
    field: str,
    value: object,
) -> None:
    payload = _tracked_payload()
    payload[field] = value

    with pytest.raises(QCCapabilityDiscoveryEvidenceContractError) as raised:
        QCCapabilityDiscoveryEvidence.from_json_bytes(_canonical_bytes(payload))
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_EVIDENCE_INVALID"


def test_raw_export_second_run_and_actor_collision_fail_closed() -> None:
    for safety_field in (
        "raw_options_data_downloaded",
        "raw_rows_logged",
        "result_artifacts_downloaded",
        "second_cloud_backtest_used",
    ):
        payload = _tracked_payload()
        payload["safety"][safety_field] = True  # type: ignore[index]
        with pytest.raises(QCCapabilityDiscoveryEvidenceContractError):
            QCCapabilityDiscoveryEvidence.from_json_bytes(_canonical_bytes(payload))

    payload = _tracked_payload()
    payload["collector_id"] = "project_owner"
    with pytest.raises(QCCapabilityDiscoveryEvidenceContractError):
        QCCapabilityDiscoveryEvidence.from_json_bytes(_canonical_bytes(payload))


def test_count_range_date_and_content_hash_tamper_fail_closed() -> None:
    mutations: tuple[tuple[str, object], ...] = (
        ("option_contract_count", 47),
        ("evaluated_end", "2025-12-03"),
        ("content_sha256", "f" * 64),
    )
    for field, value in mutations:
        payload = _tracked_payload()
        payload[field] = value
        with pytest.raises(QCCapabilityDiscoveryEvidenceContractError):
            QCCapabilityDiscoveryEvidence.from_json_bytes(_canonical_bytes(payload))


def test_noncanonical_bytes_and_caller_supplied_seal_hash_are_rejected() -> None:
    payload = _tracked_payload()
    noncanonical = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with pytest.raises(QCCapabilityDiscoveryEvidenceContractError) as raised:
        QCCapabilityDiscoveryEvidence.from_json_bytes(noncanonical)
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_EVIDENCE_NOT_CANONICAL"

    with pytest.raises(QCCapabilityDiscoveryEvidenceContractError) as raised:
        QCCapabilityDiscoveryEvidence.seal(**payload)
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_EVIDENCE_HASH_CALLER_SUPPLIED"


def test_evidence_default_path_is_project_relative() -> None:
    assert not DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH.is_absolute()
    assert ".." not in DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH.parts
    assert Path("inputs/external_validation") in (
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_EVIDENCE_PATH.parents
    )
