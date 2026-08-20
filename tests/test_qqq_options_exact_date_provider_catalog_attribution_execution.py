from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    exact_date_provider_catalog_attribution_execution as execution,
)

ROOT = Path(__file__).resolve().parents[1]
MAIN_SHA = "02a3a9b75f9e3e25f0c811cbdb974c1eb5727eb3"


def _token() -> str:
    policy = execution._policy(ROOT)
    expected = execution._expected_token_values(policy)
    return "\n".join(f"{key}:{expected[key]}" for key in execution._TOKEN_KEYS)


def _admission() -> execution.SealedRecord:
    return execution.admit_owner_token(
        token=_token(),
        source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        admitted_at_utc="2026-08-20T17:00:00Z",
        local_main_sha=MAIN_SHA,
        origin_main_sha=MAIN_SHA,
        project_root=ROOT,
    )


def _lifecycle() -> tuple[
    execution.SealedRecord, execution.SealedRecord, execution.SealedRecord
]:
    admission = _admission()
    mutation = execution.build_mutation_consumption_receipt(
        admission=admission,
        mutated_at_utc="2026-08-20T17:10:00Z",
        project_id=34808569,
        project_code_lf_sha256=(
            "86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4"
        ),
    )
    run = execution.build_run_attempt_receipt(
        admission=admission,
        mutation=mutation,
        attempted_at_utc="2026-08-20T17:11:00Z",
        backtest_id="a" * 32,
    )
    return admission, mutation, run


def _runtime() -> dict[str, str]:
    return {
        "TRADING2537_TARGET_SESSION_COUNT": "1",
        "TRADING2537_TARGET_SESSION_DATE": "2024-07-17",
        "TRADING2537_TARGET_SESSION_POSITION": "INTERIOR",
        "TRADING2537_TARGET_EQUITY_SLICE_PRESENT": "true",
        "TRADING2537_TARGET_SUBSCRIBED_CHAIN_EVENT_COUNT": "0",
        "TRADING2537_PROVIDER_PROBE_STATUS": "EXACT_DATE_AVAILABLE",
        "TRADING2537_PROVIDER_QUERY_ATTEMPT_COUNT": "1",
        "TRADING2537_EXACT_DATE_RECORD_COUNT": "1",
        "TRADING2537_EXACT_DATE_CONTRACT_COUNT": "314",
        "TRADING2537_NON_TARGET_RECORD_COUNT": "0",
        "TRADING2537_CROSS_DATE_FALLBACK_DETECTED": "false",
        "TRADING2537_ATTRIBUTION": (
            "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING"
        ),
        "TRADING2537_ATTRIBUTION_TERMINAL": "RESOLVED",
        "TRADING2537_IDENTITY": (
            "schema=qc_qqq_options_exact_date_provider_catalog_attribution_correction_runtime.v1"
            "|source=ffa9faafd1d480282bcfe1c07c896f538f26d2b23d7d7d8356460bc881e0bc49"
            "|admission=58a80cf8c0c7678dd1eab0cc8b3297fc1c27a6aace45f46d6789efc2446d7c0a"
            "|staged_policy=35e0455bc8f7e1b2660ffdbac5b508286a28671ca225872f4a86b7671ac14f2d"
            "|predecessor=3978c94ad4a5fa00ef77ae9325bec727bc20df0bc722e123916f22e821b927c1"
        ),
        "TRADING2537_EXECUTION_TERMINAL": (
            "status=COMPLETE|expected_sessions=1202|observed_sessions=1202"
            "|requested_range=2021-02-22..2025-12-02"
            "|evaluated_range=2021-02-22..2025-12-02|orders=0|fills=0"
            "|portfolio_invested=false|raw_rows=false"
            "|contract_identifiers_exported=false|individual_fields_exported=false"
            "|logs_as_data=false|object_store=false"
        ),
    }


def _results(runtime: dict[str, str] | None = None) -> bytes:
    return json.dumps(
        {
            "state": {"Status": "Completed", "RuntimeError": None, "OrderCount": 0},
            "orders": {},
            "statistics": {"Total Orders": "0", "Total Fees": "$0.00"},
            "algorithmConfiguration": {
                "startDate": "2021-02-22T00:00:00Z",
                "endDate": "2025-12-02T23:59:59Z",
            },
            "runtimeStatistics": runtime or _runtime(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _validate(result: bytes) -> execution.SealedRecord:
    admission, mutation, run = _lifecycle()
    return execution.validate_results_json(
        result_bytes=result,
        admission=admission,
        mutation=mutation,
        run_attempt=run,
        collected_at_utc="2026-08-20T17:12:00Z",
        backtest_id="a" * 32,
        project_root=ROOT,
    )


def test_exact_owner_token_and_package_admit_unused() -> None:
    receipt = _admission()
    assert receipt.payload["status"] == "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
    assert receipt.payload["owner_token_byte_count"] == 2519
    assert receipt.payload["authorization_consumed"] is False


@pytest.mark.parametrize("mutation", ["append", "reorder", "crlf"])
def test_owner_token_noncanonical_or_tampered_fails(mutation: str) -> None:
    token = _token()
    if mutation == "append":
        token += "x"
    elif mutation == "reorder":
        lines = token.split("\n")
        lines[1], lines[2] = lines[2], lines[1]
        token = "\n".join(lines)
    else:
        token = token.replace("\n", "\r\n")
    with pytest.raises(execution.ExactDateAttributionExecutionError):
        execution.admit_owner_token(
            token=token,
            source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            admitted_at_utc="2026-08-20T17:00:00Z",
            local_main_sha=MAIN_SHA,
            origin_main_sha=MAIN_SHA,
            project_root=ROOT,
        )


def test_expired_token_and_ref_drift_fail() -> None:
    for admitted_at, local_sha in (
        ("2026-08-21T16:45:19Z", MAIN_SHA),
        ("2026-08-20T17:00:00Z", "0" * 40),
    ):
        with pytest.raises(execution.ExactDateAttributionExecutionError):
            execution.admit_owner_token(
                token=_token(),
                source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
                admitted_at_utc=admitted_at,
                local_main_sha=local_sha,
                origin_main_sha=MAIN_SHA,
                project_root=ROOT,
            )


def test_single_mutation_and_run_receipts_are_bound_and_consumed() -> None:
    admission, mutation, run = _lifecycle()
    assert mutation.payload["project_mutation_count"] == 1
    assert mutation.payload["additional_project_mutation_authorized"] is False
    assert run.payload["cloud_backtest_attempt_count"] == 1
    assert run.payload["second_cloud_run_authorized"] is False
    assert run.payload["authorization_admission_content_sha256"] == (
        admission.content_sha256
    )


def test_exact_date_available_result_validates_export_safe() -> None:
    evidence = _validate(_results())
    assert evidence.payload["target_session_date"] == "2024-07-17"
    assert evidence.payload["exact_date_contract_count"] == 314
    assert evidence.payload["attribution_terminal"] == "RESOLVED"
    assert evidence.payload["orders"] == evidence.payload["fills"] == 0


def test_prior_date_only_fallback_is_not_catalog_available() -> None:
    runtime = _runtime()
    runtime.update(
        {
            "TRADING2537_PROVIDER_PROBE_STATUS": "CROSS_DATE_FALLBACK",
            "TRADING2537_EXACT_DATE_RECORD_COUNT": "0",
            "TRADING2537_EXACT_DATE_CONTRACT_COUNT": "0",
            "TRADING2537_NON_TARGET_RECORD_COUNT": "1",
            "TRADING2537_CROSS_DATE_FALLBACK_DETECTED": "true",
            "TRADING2537_ATTRIBUTION": "NO_EXACT_DATE_PROVIDER_EVIDENCE",
            "TRADING2537_ATTRIBUTION_TERMINAL": "INDETERMINATE",
        }
    )
    evidence = _validate(_results(runtime))
    assert evidence.payload["cross_date_fallback_detected"] is True
    assert evidence.payload["attribution"] == "NO_EXACT_DATE_PROVIDER_EVIDENCE"


def test_cross_date_available_misclassification_fails() -> None:
    runtime = _runtime()
    runtime["TRADING2537_CROSS_DATE_FALLBACK_DETECTED"] = "true"
    runtime["TRADING2537_NON_TARGET_RECORD_COUNT"] = "1"
    with pytest.raises(execution.ExactDateAttributionExecutionError):
        _validate(_results(runtime))


@pytest.mark.parametrize("defect", ["raw_logs", "nonzero_orders", "missing_runtime"])
def test_prohibited_or_incomplete_result_fails(defect: str) -> None:
    payload = json.loads(_results())
    if defect == "raw_logs":
        payload["logs"] = ["raw"]
    elif defect == "nonzero_orders":
        payload["state"]["OrderCount"] = 1
    else:
        payload["runtimeStatistics"].pop("TRADING2537_TARGET_SESSION_DATE")
    with pytest.raises(execution.ExactDateAttributionExecutionError):
        _validate(json.dumps(payload).encode())


def test_ledger_and_manifest_seal_complete_lifecycle() -> None:
    admission, mutation, run = _lifecycle()
    evidence = _validate(_results())
    ledger = execution.build_external_action_ledger(
        admission=admission,
        login_observed_at_utc="2026-08-20T17:09:00Z",
        mutation=mutation,
        run_attempt=run,
        evidence=evidence,
    )
    records = {
        "authorization_admission.json": admission,
        "mutation_consumption_receipt.json": mutation,
        "run_attempt_receipt.json": run,
        "export_safe_attribution_evidence.json": evidence,
        "external_action_ledger.json": ledger,
    }
    manifest = execution.build_execution_manifest(records)
    assert manifest.payload["status"] == "EXECUTION_EVIDENCE_COMPLETE"
    assert manifest.payload["artifact_count"] == 5
    execution.verify_sealed_record(manifest, label="manifest")


def test_failed_mutation_attempt_consumes_authorization_and_blocks_run() -> None:
    admission, mutation, _ = _lifecycle()
    incident = execution.build_failed_mutation_attempt_incident(
        admission=admission,
        preliminary_mutation_receipt=mutation,
        attempted_at_utc="2026-08-20T17:10:00Z",
        verification_failed_at_utc="2026-08-20T17:10:05Z",
        observed_code_marker="schema=prior_runtime.v1",
        screenshot_byte_count=100,
        screenshot_sha256="b" * 64,
    )
    ledger = execution.build_failed_mutation_action_ledger(
        admission=admission,
        incident=incident,
        login_observed_at_utc="2026-08-20T17:09:00Z",
    )
    records = {
        "authorization_admission.json": admission,
        "mutation_consumption_receipt.json": mutation,
        "mutation_attempt_incident.json": incident,
        "external_action_ledger.json": ledger,
    }
    manifest = execution.build_blocked_execution_manifest(records=records)
    assert incident.payload["verified_project_mutation_count"] == 0
    assert incident.payload["retry_authorized"] is False
    assert ledger.payload["cloud_backtest_attempt_count"] == 0
    assert manifest.payload["status"] == (
        "EXECUTION_BLOCKED_AUTHORIZATION_CONSUMED_NO_CLOUD_RUN"
    )
