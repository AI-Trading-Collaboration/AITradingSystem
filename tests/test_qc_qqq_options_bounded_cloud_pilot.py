from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.bounded_cloud_pilot import (
    DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH,
    QQQOptionsBoundedCloudPilotContractError,
    QQQOptionsBoundedCloudPilotPreregistration,
    QQQOptionsBoundedCloudPilotReadinessReport,
    build_qc_qqq_options_bounded_cloud_pilot_preregistration,
    evaluate_qc_qqq_options_bounded_cloud_pilot_readiness,
    load_qc_qqq_options_bounded_cloud_pilot_policy,
)
from ai_trading_system.qqq_options_research.cross_layer_validation import (
    build_qqq_options_cloud_smoke_checklist,
)

ROOT = Path(__file__).resolve().parents[1]
AT = datetime(2026, 8, 3, tzinfo=UTC)
BASE_SHA = "55858bedfa898f076eae496675aa2d669a5a6eed"
POLICY_SHA256 = "60ed5237fc37e4d44737fe295f4d341a58d318ecad59f8cdf753a0486609f66e"
AUTHORITY_SET_SHA256 = "34d960e7f90c5270495bf4dbbf010a6b67354a43713c00fabdbaa098e72515df"
PREREGISTRATION_SHA256 = "3b2f38fc2672dc2915a0e1b48a9df195bd76295c75cbfe9f82e708581bb49233"
READINESS_SHA256 = "c75c4008fec682c8a227f3fd37bce6e44a705308f9233dc74072cde9ca1c3bd6"
BLOCKERS = (
    "OWNER_AUTHORIZATION_NOT_GRANTED",
    "OWNER_REVIEWED_PILOT_SCOPE_NOT_GRANTED",
)
DISPOSITIONS = (
    "BOUNDED_PILOT_ACCEPTED_FOR_RANGE_EXPANSION",
    "PILOT_NO_GO_LICENSE_OR_EVIDENCE",
    "PILOT_REQUIRES_PAID_TIER",
)


def _loaded():
    return load_qc_qqq_options_bounded_cloud_pilot_policy(project_root=ROOT)


def _preregistration(**overrides: object) -> QQQOptionsBoundedCloudPilotPreregistration:
    payload: dict[str, object] = {
        "preregistration_id": "pilot-2492-blocked",
        "created_at_utc": AT,
        "repository_code_sha": BASE_SHA,
        "project_root": ROOT,
    }
    payload.update(overrides)
    return build_qc_qqq_options_bounded_cloud_pilot_preregistration(**payload)


def _readiness(
    preregistration: QQQOptionsBoundedCloudPilotPreregistration | None = None,
) -> QQQOptionsBoundedCloudPilotReadinessReport:
    preregistration = preregistration or _preregistration()
    return evaluate_qc_qqq_options_bounded_cloud_pilot_readiness(
        report_id="pilot-2492-readiness",
        evaluated_at_utc=AT,
        preregistration_bytes=preregistration.canonical_bytes(),
        project_root=ROOT,
    )


def _write_modified(source: Path, target: Path, old: str, new: str) -> Path:
    content = source.read_text(encoding="utf-8")
    assert old in content
    target.write_text(content.replace(old, new, 1), encoding="utf-8")
    return target


def test_policy_freezes_exact_blocked_authority_window_and_safety() -> None:
    loaded = _loaded()
    policy = loaded.policy

    assert loaded.policy_sha256 == POLICY_SHA256
    assert loaded.authority_set_sha256 == AUTHORITY_SET_SHA256
    assert policy.status == "BLOCKED_OWNER_INPUT"
    assert policy.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert policy.primary_research_start.isoformat() == "2021-02-22"
    assert policy.legacy_non_default_start.isoformat() == "2022-12-01"
    assert policy.legacy_non_default_start_is_default is False
    assert policy.pilot_role == "BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"
    assert policy.blocking_reason_codes == BLOCKERS
    assert policy.allowed_final_dispositions == DISPOSITIONS
    assert policy.decision == "PILOT_PREREGISTRATION_READY_OWNER_BLOCKED"
    assert policy.safety.external_platform_action_allowed is False
    assert policy.safety.cloud_run_authorized is False
    assert policy.safety.synthetic_pass_may_authorize_pilot is False
    assert policy.safety.caller_token_may_authorize_pilot is False
    assert policy.safety.production_effect == "none"
    assert policy.safety.broker_action == "none"


def test_every_2480_through_2491_authority_hash_replays() -> None:
    bindings = _loaded().policy.authority_bindings

    assert len(bindings) == 25
    assert tuple(item.authority_id for item in bindings) == tuple(
        sorted(item.authority_id for item in bindings)
    )
    for binding in bindings:
        content = (ROOT / binding.path).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == binding.sha256


def test_scope_and_evidence_inventories_are_complete_and_policy_blocked() -> None:
    policy = _loaded().policy

    assert len(policy.scope_fields) == 12
    assert all(item.status == "UNKNOWN_REQUIRES_OWNER_REVIEW" for item in policy.scope_fields)
    assert tuple(item.field_id for item in policy.scope_fields) == tuple(
        sorted(item.field_id for item in policy.scope_fields)
    )
    assert len(policy.required_evidence) == 10
    assert all(item.required_for_disposition for item in policy.required_evidence)
    assert all(
        item.current_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
        for item in policy.required_evidence
    )
    assert len(policy.readiness_items) == 12
    assert all(item.current_status == "BLOCKED" for item in policy.readiness_items)
    assert all(item.evidence_status == "NOT_EVALUATED" for item in policy.readiness_items)


def test_default_preregistration_is_canonical_cash_preserving_and_no_action() -> None:
    preregistration = _preregistration()

    assert preregistration.canonical_sha256() == PREREGISTRATION_SHA256
    assert preregistration.status == "BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE"
    assert preregistration.blocking_reason_codes == BLOCKERS
    assert preregistration.final_disposition_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
    assert preregistration.cash_preservation_required is True
    assert preregistration.order_creation_allowed is False
    assert preregistration.fill_creation_allowed is False
    assert preregistration.external_action_executed is False
    assert preregistration.pilot_authorized is False
    assert preregistration.range_expansion_allowed is False
    assert all(item.value == "NOT_GRANTED" for item in preregistration.scope_fields)
    assert all(item.status == "BLOCKED" for item in preregistration.readiness_items)


def test_cross_layer_checklist_is_built_from_inherited_blocked_authority() -> None:
    preregistration = _preregistration()
    checklist = build_qqq_options_cloud_smoke_checklist(
        checklist_id="pilot-2492-blocked-inherited-checklist",
        created_at_utc=AT,
        project_root=ROOT,
    )

    assert preregistration.cross_layer_checklist_sha256 == checklist.canonical_sha256()
    assert checklist.status == "BLOCKED_OWNER_AUTHORIZATION"
    assert checklist.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert checklist.external_action_executed is False
    assert all(item.evidence_status == "NOT_EVALUATED" for item in checklist.items)


def test_readiness_report_replays_preregistration_and_remains_no_order_no_fill() -> None:
    preregistration = _preregistration()
    report = _readiness(preregistration)

    assert report.canonical_sha256() == READINESS_SHA256
    assert report.preregistration_sha256 == preregistration.canonical_sha256()
    assert report.policy_sha256 == POLICY_SHA256
    assert report.status == "BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE"
    assert report.external_evidence_status == "NOT_EVALUATED_NO_AUTHORIZED_PILOT"
    assert report.order_count == 0
    assert report.fill_count == 0
    assert report.cash_preservation_required is True
    assert report.pilot_authorized is False
    assert report.external_action_executed is False
    assert report.range_expansion_allowed is False


def test_sealed_records_round_trip_only_from_canonical_bytes() -> None:
    preregistration = _preregistration()
    report = _readiness(preregistration)

    assert (
        QQQOptionsBoundedCloudPilotPreregistration.from_json_bytes(
            preregistration.canonical_bytes()
        )
        == preregistration
    )
    assert (
        QQQOptionsBoundedCloudPilotReadinessReport.from_json_bytes(
            report.canonical_bytes()
        )
        == report
    )
    pretty = json.dumps(
        json.loads(preregistration.canonical_bytes()),
        indent=2,
        sort_keys=True,
    ).encode()
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="NONCANONICAL"):
        QQQOptionsBoundedCloudPilotPreregistration.from_json_bytes(pretty)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_authorization_token", "FORGED_OWNER_TOKEN"),
        ("status", "READY_FOR_CLOUD_RUN"),
        ("pilot_authorized", True),
        ("external_action_executed", True),
        ("order_creation_allowed", True),
        ("fill_creation_allowed", True),
        ("range_expansion_allowed", True),
        ("final_disposition_status", "BOUNDED_PILOT_ACCEPTED_FOR_RANGE_EXPANSION"),
    ],
)
def test_caller_cannot_forge_authorization_or_success(field: str, value: object) -> None:
    payload = json.loads(_preregistration().canonical_bytes())
    payload[field] = value
    payload["content_sha256"] = "0" * 64

    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="RECORD_INVALID"):
        QQQOptionsBoundedCloudPilotPreregistration.from_json_bytes(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        )


def test_arbitrary_or_tampered_preregistration_bytes_cannot_produce_readiness() -> None:
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="RECORD_INVALID"):
        evaluate_qc_qqq_options_bounded_cloud_pilot_readiness(
            report_id="arbitrary",
            evaluated_at_utc=AT,
            preregistration_bytes=b'{"status":"READY"}',
            project_root=ROOT,
        )

    payload = json.loads(_preregistration().canonical_bytes())
    payload["repository_code_sha"] = "f" * 40
    tampered = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="RECORD_INVALID"):
        evaluate_qc_qqq_options_bounded_cloud_pilot_readiness(
            report_id="tampered",
            evaluated_at_utc=AT,
            preregistration_bytes=tampered,
            project_root=ROOT,
        )


def test_invalid_repository_sha_and_timestamps_fail_closed() -> None:
    with pytest.raises(ValidationError, match="Git SHA"):
        _preregistration(repository_code_sha="not-a-sha")
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        _preregistration(created_at_utc=datetime(2026, 8, 3))
    with pytest.raises(ValidationError, match="future"):
        _preregistration(created_at_utc=datetime.now(UTC) + timedelta(days=1))


def test_policy_authority_hash_drift_fails_closed(tmp_path: Path) -> None:
    source = ROOT / DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH
    drifted = _write_modified(
        source,
        tmp_path / "drifted.yaml",
        "7c75abeaff4d99b085c95889b686554f3589be93c62fdf82e25c9ea37f44a7c4",
        "f" * 64,
    )
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="hash drifted"):
        load_qc_qqq_options_bounded_cloud_pilot_policy(drifted, project_root=ROOT)


def test_missing_escaping_and_symlink_authority_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = ROOT / DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH
    missing = _write_modified(
        source,
        tmp_path / "missing.yaml",
        "src/ai_trading_system/contracts/qc_qqq_options_capability_admission.py",
        "src/ai_trading_system/contracts/not_present.py",
    )
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="required regular"):
        load_qc_qqq_options_bounded_cloud_pilot_policy(missing, project_root=ROOT)

    escaping = _write_modified(
        source,
        tmp_path / "escaping.yaml",
        "src/ai_trading_system/contracts/qc_qqq_options_capability_admission.py",
        "../outside.py",
    )
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="repository-relative"):
        load_qc_qqq_options_bounded_cloud_pilot_policy(escaping, project_root=ROOT)

    target = ROOT / "src/ai_trading_system/contracts/qc_qqq_options_capability_admission.py"
    original = Path.is_symlink

    def fake_is_symlink(path: Path) -> bool:
        return path == target or original(path)

    monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match="symlink"):
        load_qc_qqq_options_bounded_cloud_pilot_policy(project_root=ROOT)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("primary_research_start: 2021-02-22", "primary_research_start: 2022-12-01", "2021-02-22"),
        (
            "legacy_non_default_start_is_default: false",
            "legacy_non_default_start_is_default: true",
            "literal_error",
        ),
        (
            "status: UNKNOWN_REQUIRES_OWNER_REVIEW",
            "status: ACTIVE",
            "literal_error",
        ),
        (
            "  - OWNER_REVIEWED_PILOT_SCOPE_NOT_GRANTED\n",
            "",
            "blocking reasons",
        ),
    ],
)
def test_unreviewed_window_scope_and_blocker_policy_changes_fail_closed(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    source = ROOT / DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH
    changed = _write_modified(
        source,
        tmp_path / f"changed-{len(list(tmp_path.iterdir()))}.yaml",
        old,
        new,
    )
    with pytest.raises(QQQOptionsBoundedCloudPilotContractError, match=message):
        load_qc_qqq_options_bounded_cloud_pilot_policy(changed, project_root=ROOT)


def test_same_inputs_have_stable_identity_and_no_input_order_can_change_policy_order() -> None:
    first = _preregistration()
    second = _preregistration()

    assert first.canonical_bytes() == second.canonical_bytes()
    assert tuple(item.field_id for item in first.scope_fields) == tuple(
        sorted(item.field_id for item in first.scope_fields)
    )
    assert tuple(item.item_id for item in first.readiness_items) == tuple(
        sorted(item.item_id for item in first.readiness_items)
    )
    assert tuple(item.evidence_role for item in _loaded().policy.required_evidence) == tuple(
        sorted(item.evidence_role for item in _loaded().policy.required_evidence)
    )
