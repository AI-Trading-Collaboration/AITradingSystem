from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.qqq_options_research import (
    session_finalization_external_validation_admission as admission_module,
)

OwnerAuthorizationAdmissionReceipt = admission_module.OwnerAuthorizationAdmissionReceipt
RunAttemptConsumptionReceipt = admission_module.RunAttemptConsumptionReceipt
ExportSafeSessionFinalizationEvidence = admission_module.ExportSafeSessionFinalizationEvidence
ExternalActionLedger = admission_module.ExternalActionLedger
ExecutionEvidenceManifest = admission_module.ExecutionEvidenceManifest
SessionFinalizationExternalValidationError = (
    admission_module.SessionFinalizationExternalValidationError
)
admit_owner_authorization = admission_module.admit_owner_authorization
build_execution_evidence_manifest = admission_module.build_execution_evidence_manifest
build_external_action_ledger = admission_module.build_external_action_ledger
consume_on_first_run_attempt = admission_module.consume_on_first_run_attempt
validate_owner_token_candidate = admission_module.validate_owner_token_candidate
validate_results_json = admission_module.validate_results_json

ROOT = Path(__file__).resolve().parents[1]
PROPOSAL_MAIN = "c3e593b0e0739ca5f2494f3d55d52af019b0fc47"
ADMISSION_MAIN = "1002cfd21de4f9ca33f816f9a418c4a256b7d1bd"
PROJECT_CODE_SHA256 = "0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45"
EXPIRY = "2026-08-18T13:02:48Z"


def _policy() -> dict[str, Any]:
    return admission_module._load_policy(ROOT)


def _owner_token(policy: dict[str, Any] | None = None, **overrides: str) -> bytes:
    selected = _policy() if policy is None else policy
    fields = admission_module._expected_token_fields(selected)
    fields["ordinary_pushed_admission_main_sha"] = ADMISSION_MAIN
    fields["authorization_expires_at_utc"] = EXPIRY
    fields.update(overrides)
    lines = [str(selected["owner_decision"])]
    lines.extend(f"{key}:{fields[key]}" for key in admission_module._FIELD_ORDER)
    return "\n".join(lines).encode("utf-8")


def _admit() -> OwnerAuthorizationAdmissionReceipt:
    token = _owner_token()
    return admit_owner_authorization(
        owner_token_bytes=token,
        owner_token_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        reviewed_at_utc="2026-08-17T13:05:00Z",
        local_main_sha=ADMISSION_MAIN,
        origin_main_sha=ADMISSION_MAIN,
        project_root=ROOT,
    )


def _consume(
    admission: OwnerAuthorizationAdmissionReceipt,
) -> RunAttemptConsumptionReceipt:
    return consume_on_first_run_attempt(
        admission=admission,
        attempted_at_utc="2026-08-17T13:10:00Z",
        project_id=34808569,
        project_code_lf_sha256=PROJECT_CODE_SHA256,
        backtest_id="backtest-2532-once",
        attempt_status="COMPLETED",
    )


def _runtime_statistics() -> dict[str, str]:
    runtime: dict[str, str] = {}
    for axis in admission_module._AXES:
        for status in admission_module._STATUSES:
            runtime[f"TRADING2531_{axis}_{status}_SESSIONS"] = (
                "1202" if status == "PRESENT" else "0"
            )
    runtime.update(
        {
            "TRADING2531_CHAINLESS_SLICE_EVENTS": "10",
            "TRADING2531_SESSIONS_WITH_CHAINLESS_SLICE": "5",
            "TRADING2531_SESSIONS_RECOVERED_AFTER_CHAINLESS": "5",
            "TRADING2531_SESSIONS_NEVER_CHAIN": "0",
            "TRADING2531_SESSIONS_WITH_CANONICAL_EQUITY_PRESENT": "1202",
            "TRADING2531_SESSIONS_WITH_CANONICAL_EQUITY_MISSING": "0",
            "TRADING2531_SESSIONS_WITH_CANONICAL_EQUITY_INVALID": "0",
            "TRADING2531_SESSIONS_WITH_CONTRACT_ZERO_IGNORED": "182",
            "TRADING2531_SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS": "20",
            "TRADING2531_IDENTITY": (
                "schema=qc_qqq_options_daily_transport_per_axis_runtime.v2"
                "|contract=f3c3918dd5dfd6fc1c6e84b63471c652d34090c9d50fab25d77dc58f9190b378"
            ),
            "TRADING2531_TERMINAL": (
                "status=COMPLETE|expected_sessions=1202|observed_sessions=1202"
                "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
                "|logs_as_data=false|object_store=false"
                "|stale_underlying_fallback=false"
            ),
        }
    )
    return runtime


def _result_payload() -> dict[str, Any]:
    return {
        "state": {"Status": "Completed", "RuntimeError": None, "OrderCount": 0},
        "orders": {},
        "statistics": {"Total Orders": "0", "Total Fees": "$0.00"},
        "algorithmConfiguration": {
            "startDate": "2021-02-22T00:00:00Z",
            "endDate": "2025-12-02T23:59:59Z",
        },
        "runtimeStatistics": _runtime_statistics(),
    }


def _result_bytes(payload: dict[str, Any] | None = None) -> bytes:
    selected = _result_payload() if payload is None else payload
    return json.dumps(selected, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _assert_code(code: str, call: Any) -> None:
    with pytest.raises(SessionFinalizationExternalValidationError) as caught:
        call()
    assert caught.value.code == code


def test_current_pending_policy_directly_admits_exact_owner_token() -> None:
    token = _owner_token()
    candidate = validate_owner_token_candidate(owner_token_bytes=token, project_root=ROOT)
    assert candidate.token_sha256 == hashlib.sha256(token).hexdigest()
    assert candidate.token_byte_count == len(token)
    assert candidate.expires_at_utc == EXPIRY

    receipt = admit_owner_authorization(
        owner_token_bytes=token,
        owner_token_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        reviewed_at_utc="2026-08-17T13:05:00Z",
        local_main_sha=ADMISSION_MAIN,
        origin_main_sha=ADMISSION_MAIN,
        project_root=ROOT,
    )
    assert receipt.payload["proposal_publication_main_sha"] == PROPOSAL_MAIN
    assert receipt.payload["ordinary_pushed_admission_main_sha"] == ADMISSION_MAIN
    assert receipt.payload["authorization_consumed"] is False


@pytest.mark.parametrize(
    ("mutator", "expected_code"),
    [
        (
            lambda token: token + b"\r\n",
            "SESSION_FINALIZATION_EXTERNAL_TOKEN_CANONICAL_LF_REQUIRED",
        ),
        (
            lambda token: token.replace(
                b"maximum_project_mutations:1", b"maximum_project_mutations:2"
            ),
            "SESSION_FINALIZATION_EXTERNAL_TOKEN_SCOPE_OR_HASH_MISMATCH",
        ),
        (
            lambda token: token.replace(
                b"proposal_publication_main_sha:c3e593",
                b"proposal_publication_main_sha:000000",
            ),
            "SESSION_FINALIZATION_EXTERNAL_TOKEN_SCOPE_OR_HASH_MISMATCH",
        ),
        (
            lambda token: token.replace(ADMISSION_MAIN.encode(), b"g" * 40),
            "SESSION_FINALIZATION_EXTERNAL_ADMISSION_MAIN_FORMAT_INVALID",
        ),
        (
            lambda token: token.replace(ADMISSION_MAIN.encode(), PROPOSAL_MAIN.encode()),
            "SESSION_FINALIZATION_EXTERNAL_ADMISSION_MAIN_EQUALS_PROPOSAL",
        ),
        (
            lambda token: b"\n".join(
                [token.split(b"\n")[0], token.split(b"\n")[2], token.split(b"\n")[1]]
                + token.split(b"\n")[3:]
            ),
            "SESSION_FINALIZATION_EXTERNAL_TOKEN_FIELD_ORDER_INVALID",
        ),
        (
            lambda token: token.replace(EXPIRY.encode(), b"2026-08-25T00:00:01Z"),
            "SESSION_FINALIZATION_EXTERNAL_TOKEN_EXPIRY_OUTSIDE_WINDOW",
        ),
    ],
)
def test_owner_token_is_exact_and_tamper_evident(mutator: Any, expected_code: str) -> None:
    _assert_code(
        expected_code,
        lambda: validate_owner_token_candidate(
            owner_token_bytes=mutator(_owner_token()), project_root=ROOT
        ),
    )


def _identity_contract_sandbox(
    tmp_path: Path,
) -> tuple[dict[str, Any], Path, Path]:
    policy = _policy()
    contract_relative = Path("identity/contract.json")
    request_relative = Path("identity/owner_decision_request.md")
    contract_path = tmp_path / contract_relative
    request_path = tmp_path / request_relative
    contract_path.parent.mkdir(parents=True)
    contract_path.write_bytes((ROOT / policy["admission_identity_contract_path"]).read_bytes())
    request_path.write_bytes((ROOT / policy["admission_identity_request_path"]).read_bytes())
    selected = {
        **policy,
        "admission_identity_contract_path": contract_relative.as_posix(),
        "admission_identity_request_path": request_relative.as_posix(),
    }
    return selected, contract_path, request_path


def test_admission_identity_contract_tamper_fails_closed(tmp_path: Path) -> None:
    policy, contract_path, _request_path = _identity_contract_sandbox(tmp_path)
    contract_path.write_text(
        contract_path.read_text(encoding="utf-8").replace(
            '"maximum_orders": 0', '"maximum_orders": 1'
        ),
        encoding="utf-8",
    )
    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_SEAL_INVALID",
        lambda: admission_module._load_admission_identity_contract(
            project_root=tmp_path, policy=policy
        ),
    )


def test_admission_identity_request_tamper_fails_closed(tmp_path: Path) -> None:
    policy, _contract_path, request_path = _identity_contract_sandbox(tmp_path)
    request_path.write_bytes(request_path.read_bytes() + b"tamper")
    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_IDENTITY_REQUEST_MISMATCH",
        lambda: admission_module._load_admission_identity_contract(
            project_root=tmp_path, policy=policy
        ),
    )


def test_exact_token_admits_unused_receipt_without_post_token_policy_mutation() -> None:
    receipt = _admit()
    assert receipt.payload["status"] == "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
    assert receipt.payload["authorization_consumed"] is False
    assert receipt.payload["external_action_performed"] is False
    assert receipt.payload["maximum_project_mutations"] == 1
    assert receipt.payload["maximum_cloud_backtests"] == 1
    assert receipt.payload["maximum_orders"] == 0
    assert receipt.payload["maximum_fills"] == 0
    assert receipt.payload["content_sha256"] == receipt.content_sha256


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        (
            "owner_token_source",
            "UNREVIEWED_SOURCE",
            "SESSION_FINALIZATION_EXTERNAL_OWNER_TOKEN_SOURCE_INVALID",
        ),
        (
            "local_main_sha",
            "0" * 40,
            "SESSION_FINALIZATION_EXTERNAL_PUBLISHED_MAIN_MISMATCH",
        ),
        (
            "origin_main_sha",
            "0" * 40,
            "SESSION_FINALIZATION_EXTERNAL_PUBLISHED_MAIN_MISMATCH",
        ),
        (
            "reviewed_at_utc",
            EXPIRY,
            "SESSION_FINALIZATION_EXTERNAL_REVIEW_TIME_OUTSIDE_WINDOW",
        ),
    ],
)
def test_admission_fails_closed_on_authority_or_publication_drift(
    field: str,
    value: str,
    expected_code: str,
) -> None:
    token = _owner_token()
    arguments = {
        "owner_token_bytes": token,
        "owner_token_source": "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        "reviewed_at_utc": "2026-08-17T13:05:00Z",
        "local_main_sha": ADMISSION_MAIN,
        "origin_main_sha": ADMISSION_MAIN,
        "project_root": ROOT,
    }
    arguments[field] = value
    _assert_code(expected_code, lambda: admit_owner_authorization(**arguments))


def test_direct_admission_rejects_post_token_tracked_policy_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = _owner_token()
    policy = {
        **_policy(),
        "policy_status": "OWNER_TOKEN_OBSERVED_ADMISSION_REQUIRED",
        "owner_token_status": "EXACT_OWNER_TOKEN_OBSERVED",
        "owner_token_sha256": hashlib.sha256(token).hexdigest(),
        "owner_token_byte_count": len(token),
        "authorization_expires_at_utc": EXPIRY,
    }
    monkeypatch.setattr(admission_module, "_load_policy", lambda project_root=ROOT: policy)
    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_DIRECT_ADMISSION_POLICY_INVALID",
        lambda: admit_owner_authorization(
            owner_token_bytes=token,
            owner_token_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            reviewed_at_utc="2026-08-17T13:05:00Z",
            local_main_sha=ADMISSION_MAIN,
            origin_main_sha=ADMISSION_MAIN,
            project_root=ROOT,
        ),
    )


def test_first_attempt_consumes_authority_and_second_attempt_is_forbidden() -> None:
    admitted = _admit()
    consumed = _consume(admitted)
    assert consumed.payload["authorization_consumed"] is True
    assert consumed.payload["authorization_invalidated_for_further_attempts"] is True
    assert consumed.payload["project_mutation_count"] == 1
    assert consumed.payload["cloud_backtest_attempt_count"] == 1
    assert consumed.payload["orders"] == consumed.payload["fills"] == 0
    assert consumed.payload["content_sha256"] == consumed.content_sha256

    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_AUTHORIZATION_ALREADY_CONSUMED",
        lambda: consume_on_first_run_attempt(
            admission=admitted,
            attempted_at_utc="2026-08-17T13:11:00Z",
            project_id=34808569,
            project_code_lf_sha256=PROJECT_CODE_SHA256,
            backtest_id="backtest-2532-second",
            attempt_status="SUBMITTED",
            prior_consumption=consumed,
        ),
    )


@pytest.mark.parametrize(
    ("overrides", "expected_code"),
    [
        (
            {"attempted_at_utc": EXPIRY},
            "SESSION_FINALIZATION_EXTERNAL_AUTHORIZATION_EXPIRED",
        ),
        (
            {"project_id": 1},
            "SESSION_FINALIZATION_EXTERNAL_PROJECT_MISMATCH",
        ),
        (
            {"project_code_lf_sha256": "0" * 64},
            "SESSION_FINALIZATION_EXTERNAL_PROJECT_CODE_MISMATCH",
        ),
        (
            {"backtest_id": ""},
            "SESSION_FINALIZATION_EXTERNAL_RUN_ATTEMPT_INVALID",
        ),
    ],
)
def test_consumption_rejects_expiry_and_execution_drift(
    overrides: dict[str, Any],
    expected_code: str,
) -> None:
    admitted = _admit()
    arguments: dict[str, Any] = {
        "admission": admitted,
        "attempted_at_utc": "2026-08-17T13:10:00Z",
        "project_id": 34808569,
        "project_code_lf_sha256": PROJECT_CODE_SHA256,
        "backtest_id": "backtest-2532-once",
        "attempt_status": "SUBMITTED",
    }
    arguments.update(overrides)
    _assert_code(expected_code, lambda: consume_on_first_run_attempt(**arguments))


def test_valid_export_safe_result_builds_bound_evidence_ledger_and_manifest() -> None:
    admitted = _admit()
    consumed = _consume(admitted)
    raw = _result_bytes()
    evidence = validate_results_json(
        result_bytes=raw,
        admission=admitted,
        consumption=consumed,
        collected_at_utc="2026-08-17T13:20:00Z",
        backtest_id="backtest-2532-once",
        project_root=ROOT,
    )
    assert evidence.payload["observed_session_count"] == 1202
    assert evidence.payload["orders"] == evidence.payload["fills"] == 0
    assert evidence.payload["raw_rows_collected"] is False
    assert evidence.payload["logs_as_data_collected"] is False
    assert evidence.payload["object_store_used"] is False
    assert evidence.payload["source_result_file_sha256"] == hashlib.sha256(raw).hexdigest()

    ledger = build_external_action_ledger(
        admission=admitted,
        login_observed_at_utc="2026-08-17T13:06:00Z",
        mutation_started_at_utc="2026-08-17T13:07:00Z",
        mutation_verified_at_utc="2026-08-17T13:08:00Z",
        consumption=consumed,
        evidence=evidence,
    )
    assert [item["ordinal"] for item in ledger.payload["actions"]] == [1, 2, 3, 4, 5]
    assert ledger.payload["status"] == "EXTERNAL_ACTION_LIFECYCLE_COMPLETE"
    manifest = build_execution_evidence_manifest(
        admission=admitted,
        consumption=consumed,
        action_ledger=ledger,
        evidence=evidence,
    )
    assert manifest.payload["status"] == "EXECUTION_EVIDENCE_COMPLETE"
    assert manifest.payload["artifact_count"] == 4
    assert manifest.payload["raw_result_committed"] is False
    assert manifest.payload["content_sha256"] == manifest.content_sha256


def test_published_execution_package_is_sealed_bound_and_export_safe() -> None:
    package = (
        ROOT
        / "inputs"
        / "research"
        / "qqq_options"
        / "trading_2532_session_finalization_v2_external_validation_execution_v1"
    )
    assert {item.name for item in package.iterdir()} == {
        "authorization_admission.json",
        "run_attempt_consumption_receipt.json",
        "export_safe_aggregate_evidence.json",
        "external_action_ledger.json",
        "execution_evidence_manifest.json",
    }
    admission_payload = json.loads((package / "authorization_admission.json").read_text())
    consumption_payload = json.loads((package / "run_attempt_consumption_receipt.json").read_text())
    evidence_payload = json.loads((package / "export_safe_aggregate_evidence.json").read_text())
    ledger_payload = json.loads((package / "external_action_ledger.json").read_text())
    manifest_payload = json.loads((package / "execution_evidence_manifest.json").read_text())

    admission = OwnerAuthorizationAdmissionReceipt(admission_payload)
    consumption = RunAttemptConsumptionReceipt(consumption_payload)
    evidence = ExportSafeSessionFinalizationEvidence(evidence_payload)
    ledger = ExternalActionLedger(ledger_payload)
    manifest = ExecutionEvidenceManifest(manifest_payload)
    for label, payload in (
        ("authorization admission", admission.payload),
        ("run consumption", consumption.payload),
        ("export-safe evidence", evidence.payload),
        ("external action ledger", ledger.payload),
        ("execution evidence manifest", manifest.payload),
    ):
        admission_module._verify_seal(payload, label=label)

    assert evidence.payload["source_result_file_sha256"] == (
        "5d3220342c96217f2c4a4d624b0dc7fbbcad98427de728e749dc2e4f3168d50d"
    )
    assert evidence.payload["source_result_byte_count"] == 814999
    assert evidence.payload["backtest_id"] == "acf111f24d09a41870f9a23e93fcbe3b"
    assert evidence.payload["observed_session_count"] == 1202
    assert evidence.payload["orders"] == evidence.payload["fills"] == 0
    assert evidence.payload["raw_rows_collected"] is False
    assert evidence.payload["logs_as_data_collected"] is False
    assert evidence.payload["object_store_used"] is False
    assert ledger.payload["cloud_backtest_attempt_count"] == 1
    assert ledger.payload["second_attempt_authorized"] is False
    assert manifest.payload["status"] == "EXECUTION_EVIDENCE_COMPLETE"
    assert manifest.payload["raw_result_committed"] is False
    assert manifest.payload["artifacts"] == {
        "authorization_admission.json": admission.content_sha256,
        "export_safe_aggregate_evidence.json": evidence.content_sha256,
        "external_action_ledger.json": ledger.content_sha256,
        "run_attempt_consumption_receipt.json": consumption.content_sha256,
    }


def _mutate_axis_total(payload: dict[str, Any]) -> None:
    payload["runtimeStatistics"]["TRADING2531_BID_ASK_QUOTE_PRESENT_SESSIONS"] = "1201"


def _mutate_extra_aggregate(payload: dict[str, Any]) -> None:
    payload["runtimeStatistics"]["TRADING2531_UNDECLARED"] = "1"


def _mutate_identity(payload: dict[str, Any]) -> None:
    payload["runtimeStatistics"]["TRADING2531_IDENTITY"] = "wrong"


def _mutate_terminal(payload: dict[str, Any]) -> None:
    payload["runtimeStatistics"]["TRADING2531_TERMINAL"] = payload["runtimeStatistics"][
        "TRADING2531_TERMINAL"
    ].replace("observed_sessions=1202", "observed_sessions=1201")


def _mutate_diagnostic(payload: dict[str, Any]) -> None:
    payload["runtimeStatistics"]["TRADING2531_SESSIONS_RECOVERED_AFTER_CHAINLESS"] = "6"


def _mutate_order(payload: dict[str, Any]) -> None:
    payload["orders"] = {"order-1": {"quantity": 1}}


def _mutate_logs(payload: dict[str, Any]) -> None:
    payload["logs"] = ["not export safe"]


def _mutate_range(payload: dict[str, Any]) -> None:
    payload["algorithmConfiguration"]["startDate"] = "2022-12-01T00:00:00Z"


@pytest.mark.parametrize(
    ("mutator", "expected_code"),
    [
        (_mutate_axis_total, "SESSION_FINALIZATION_RESULTS_AXIS_TOTAL_INVALID"),
        (
            _mutate_extra_aggregate,
            "SESSION_FINALIZATION_RESULTS_AGGREGATE_KEYSET_INVALID",
        ),
        (_mutate_identity, "SESSION_FINALIZATION_RESULTS_IDENTITY_INVALID"),
        (_mutate_terminal, "SESSION_FINALIZATION_RESULTS_TERMINAL_INVALID"),
        (
            _mutate_diagnostic,
            "SESSION_FINALIZATION_RESULTS_DIAGNOSTIC_INVARIANT_INVALID",
        ),
        (_mutate_order, "SESSION_FINALIZATION_RESULTS_ORDERS_NOT_EMPTY"),
        (_mutate_logs, "SESSION_FINALIZATION_RESULTS_PROHIBITED_CARRIER"),
        (_mutate_range, "SESSION_FINALIZATION_RESULTS_RANGE_INVALID"),
    ],
)
def test_result_parser_fails_closed_on_unsafe_or_inconsistent_payload(
    mutator: Any,
    expected_code: str,
) -> None:
    admitted = _admit()
    consumed = _consume(admitted)
    payload = copy.deepcopy(_result_payload())
    mutator(payload)
    _assert_code(
        expected_code,
        lambda: validate_results_json(
            result_bytes=_result_bytes(payload),
            admission=admitted,
            consumption=consumed,
            collected_at_utc="2026-08-17T13:20:00Z",
            backtest_id="backtest-2532-once",
            project_root=ROOT,
        ),
    )


def test_pending_ledger_and_manifest_do_not_claim_result_completion() -> None:
    admitted = _admit()
    consumed = _consume(admitted)
    ledger = build_external_action_ledger(
        admission=admitted,
        login_observed_at_utc="2026-08-17T13:06:00Z",
        mutation_started_at_utc="2026-08-17T13:07:00Z",
        mutation_verified_at_utc="2026-08-17T13:08:00Z",
        consumption=consumed,
    )
    assert ledger.payload["status"] == ("EXTERNAL_ACTION_ATTEMPT_CONSUMED_EVIDENCE_PENDING")
    manifest = build_execution_evidence_manifest(
        admission=admitted,
        consumption=consumed,
        action_ledger=ledger,
    )
    assert manifest.payload["status"] == "RUN_ATTEMPT_CONSUMED_RESULT_EVIDENCE_PENDING"
    assert manifest.payload["artifact_count"] == 3
    assert "export_safe_aggregate_evidence.json" not in manifest.payload["artifacts"]


def test_action_ledger_rejects_time_and_evidence_binding_drift() -> None:
    admitted = _admit()
    consumed = _consume(admitted)
    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_ACTION_ORDER_INVALID",
        lambda: build_external_action_ledger(
            admission=admitted,
            login_observed_at_utc="2026-08-17T13:09:00Z",
            mutation_started_at_utc="2026-08-17T13:07:00Z",
            mutation_verified_at_utc="2026-08-17T13:08:00Z",
            consumption=consumed,
        ),
    )

    other_payload = {
        **consumed.payload,
        "authorization_admission_content_sha256": "0" * 64,
    }
    other_payload["content_sha256"] = admission_module._content_sha256(other_payload)
    other = RunAttemptConsumptionReceipt(other_payload)
    _assert_code(
        "SESSION_FINALIZATION_EXTERNAL_ACTION_BINDING_INVALID",
        lambda: build_external_action_ledger(
            admission=admitted,
            login_observed_at_utc="2026-08-17T13:06:00Z",
            mutation_started_at_utc="2026-08-17T13:07:00Z",
            mutation_verified_at_utc="2026-08-17T13:08:00Z",
            consumption=other,
        ),
    )
