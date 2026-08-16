from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    daily_transport_per_axis_collection_authorization_admission as admission_module,
)

PerAxisCollectionAuthorizationAdmissionError = (
    admission_module.PerAxisCollectionAuthorizationAdmissionError
)
admit_per_axis_collection_owner_authorization = (
    admission_module.admit_per_axis_collection_owner_authorization
)
build_per_axis_collection_execution_evidence_manifest = (
    admission_module.build_per_axis_collection_execution_evidence_manifest
)
build_per_axis_external_action_ledger = admission_module.build_per_axis_external_action_ledger
build_per_axis_result_download_delivery_incident = (
    admission_module.build_per_axis_result_download_delivery_incident
)
consume_on_first_cloud_run_attempt = admission_module.consume_on_first_cloud_run_attempt
validate_per_axis_results_json = admission_module.validate_per_axis_results_json

ROOT = Path(__file__).resolve().parents[1]
MAIN_SHA = "cd8a89fdb5052e908c5f8b010b27f92a95645689"


def _owner_token() -> bytes:
    policy = admission_module._load_policy(ROOT)
    package = admission_module.load_per_axis_collection_proposal_package(project_root=ROOT)
    fields = admission_module._expected_fields(policy=policy, package=package)
    lines = [
        admission_module._OWNER_DECISION,
        *(f"{key}:{fields[key]}" for key in admission_module._FIELD_ORDER),
    ]
    return "\n".join(lines).encode("utf-8")


TOKEN = _owner_token()


def _admit():
    return admit_per_axis_collection_owner_authorization(
        owner_token_bytes=TOKEN,
        owner_token_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        reviewed_at_utc="2026-08-16T13:10:02Z",
        local_main_sha=MAIN_SHA,
        origin_main_sha=MAIN_SHA,
        project_root=ROOT,
    )


def _consume(admission):
    return consume_on_first_cloud_run_attempt(
        admission=admission,
        attempted_at_utc="2026-08-16T13:20:00Z",
        project_id=34808569,
        project_code_lf_sha256=admission.payload["project_code_lf_sha256"],
        backtest_id="example-backtest-id",
        attempt_status="SUBMITTED",
    )


def _result_payload() -> dict[str, object]:
    policy = admission_module._load_policy(ROOT)
    package = admission_module.load_per_axis_collection_proposal_package(project_root=ROOT)
    runtime_identity = (
        "schema=qc_qqq_options_daily_transport_per_axis_runtime.v1"
        f"|scope={package.run_scope.content_sha256}"
        f"|repository={policy['registration_base_repository_code_sha']}"
        f"|source_diagnostic={package.proposal.source_diagnostic_content_sha256}"
    )
    runtime: dict[str, object] = {"Equity": "$100,000.00"}
    for axis in admission_module._AXES:
        for status in admission_module._STATUSES:
            runtime[f"TRADING2529_{axis}_{status}_SESSIONS"] = (
                "1202" if status == "PRESENT" else "0"
            )
    runtime["TRADING2529_IDENTITY"] = runtime_identity
    runtime["TRADING2529_TERMINAL"] = (
        "status=COMPLETE|expected_sessions=1202|observed_sessions=1202"
        "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
        "|logs_as_data=false|object_store=false"
    )
    return {
        "orders": {},
        "statistics": {"Total Orders": "0", "Total Fees": "$0.00"},
        "runtimeStatistics": runtime,
        "state": {
            "Status": "Completed",
            "RuntimeError": "",
            "OrderCount": "0",
            "Hostname": "BACKTESTING-example-backtest-id",
        },
        "algorithmConfiguration": {
            "startDate": "2021-02-22T00:00:00Z",
            "endDate": "2025-12-02T23:59:59Z",
        },
    }


def _result_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def test_exact_owner_token_admits_unused_and_replays() -> None:
    receipt = _admit()
    assert receipt.payload["status"] == "OWNER_AUTHORIZATION_ADMITTED_UNUSED"
    assert receipt.payload["authorization_consumed"] is False
    assert receipt.payload["external_action_performed"] is False
    assert receipt.payload["maximum_cloud_backtests"] == 1
    assert receipt.payload["maximum_orders"] == 0
    assert receipt.content_sha256 == receipt.payload["content_sha256"]


@pytest.mark.parametrize(
    ("token", "source", "local_sha", "origin_sha", "reviewed", "code"),
    [
        (
            TOKEN.replace(b"1202", b"1201", 1),
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            MAIN_SHA,
            MAIN_SHA,
            "2026-08-16T13:10:02Z",
            "PER_AXIS_AUTHORIZATION_TOKEN_IDENTITY_MISMATCH",
        ),
        (
            TOKEN.replace(b"\n", b"\r\n", 1),
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            MAIN_SHA,
            MAIN_SHA,
            "2026-08-16T13:10:02Z",
            "PER_AXIS_AUTHORIZATION_TOKEN_CANONICAL_LF_REQUIRED",
        ),
        (
            TOKEN,
            "LOCAL_DRY_RUN",
            MAIN_SHA,
            MAIN_SHA,
            "2026-08-16T13:10:02Z",
            "PER_AXIS_AUTHORIZATION_SOURCE_INVALID",
        ),
        (
            TOKEN,
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            "0" * 40,
            MAIN_SHA,
            "2026-08-16T13:10:02Z",
            "PER_AXIS_AUTHORIZATION_PUBLISHED_MAIN_MISMATCH",
        ),
        (
            TOKEN,
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            MAIN_SHA,
            "0" * 40,
            "2026-08-16T13:10:02Z",
            "PER_AXIS_AUTHORIZATION_PUBLISHED_MAIN_MISMATCH",
        ),
        (
            TOKEN,
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            MAIN_SHA,
            MAIN_SHA,
            "2026-08-17T13:02:48Z",
            "PER_AXIS_AUTHORIZATION_REVIEW_TIME_OUTSIDE_WINDOW",
        ),
    ],
)
def test_token_admission_fails_closed(token, source, local_sha, origin_sha, reviewed, code) -> None:
    with pytest.raises(PerAxisCollectionAuthorizationAdmissionError, match=code):
        admit_per_axis_collection_owner_authorization(
            owner_token_bytes=token,
            owner_token_source=source,
            reviewed_at_utc=reviewed,
            local_main_sha=local_sha,
            origin_main_sha=origin_sha,
            project_root=ROOT,
        )


def test_first_run_attempt_consumes_and_second_attempt_is_forbidden() -> None:
    admission = _admit()
    first = consume_on_first_cloud_run_attempt(
        admission=admission,
        attempted_at_utc="2026-08-16T13:20:00Z",
        project_id=34808569,
        project_code_lf_sha256=admission.payload["project_code_lf_sha256"],
        backtest_id="example-backtest-id",
        attempt_status="SUBMITTED",
    )
    assert first.payload["authorization_consumed"] is True
    assert first.payload["second_cloud_run_authorized"] is False
    with pytest.raises(
        PerAxisCollectionAuthorizationAdmissionError,
        match="PER_AXIS_AUTHORIZATION_ALREADY_CONSUMED",
    ):
        consume_on_first_cloud_run_attempt(
            admission=admission,
            attempted_at_utc="2026-08-16T13:21:00Z",
            project_id=34808569,
            project_code_lf_sha256=admission.payload["project_code_lf_sha256"],
            backtest_id="second-backtest-id",
            attempt_status="SUBMITTED",
            prior_consumption=first,
        )


def test_run_attempt_rejects_project_code_and_expiry_drift() -> None:
    admission = _admit()
    with pytest.raises(PerAxisCollectionAuthorizationAdmissionError):
        consume_on_first_cloud_run_attempt(
            admission=admission,
            attempted_at_utc="2026-08-17T13:02:48Z",
            project_id=34808569,
            project_code_lf_sha256=admission.payload["project_code_lf_sha256"],
            backtest_id="late",
            attempt_status="SUBMITTED",
        )
    with pytest.raises(PerAxisCollectionAuthorizationAdmissionError):
        consume_on_first_cloud_run_attempt(
            admission=admission,
            attempted_at_utc="2026-08-16T13:20:00Z",
            project_id=34808569,
            project_code_lf_sha256="0" * 64,
            backtest_id="wrong-code",
            attempt_status="SUBMITTED",
        )


def test_results_json_extracts_only_export_safe_per_axis_aggregates() -> None:
    admission = _admit()
    consumption = _consume(admission)
    evidence = validate_per_axis_results_json(
        result_bytes=_result_bytes(_result_payload()),
        admission=admission,
        consumption=consumption,
        collected_at_utc="2026-08-16T13:30:00Z",
        backtest_id="example-backtest-id",
        project_root=ROOT,
    )
    assert evidence.payload["status"] == "EXPORT_SAFE_PER_AXIS_AGGREGATES_COLLECTED"
    assert len(evidence.payload["per_axis_status_session_counts"]) == 32
    assert set(evidence.payload["per_axis_totals"].values()) == {1202}
    assert evidence.payload["orders"] == evidence.payload["fills"] == 0
    assert evidence.payload["investment_conclusion_authorized"] is False
    assert evidence.content_sha256 == evidence.payload["content_sha256"]

    ledger = build_per_axis_external_action_ledger(
        admission=admission,
        login_observed_at_utc="2026-08-16T13:10:00Z",
        mutation_started_at_utc="2026-08-16T13:11:00Z",
        mutation_verified_at_utc="2026-08-16T13:12:00Z",
        mutation_lf_byte_count=24420,
        consumption=consumption,
        evidence=evidence,
    )
    assert ledger.payload["status"] == "EXTERNAL_ACTION_LIFECYCLE_COMPLETE"
    assert [action["ordinal"] for action in ledger.payload["actions"]] == [1, 2, 3, 4]
    assert ledger.payload["project_mutation_count"] == 1
    assert ledger.payload["cloud_backtest_attempt_count"] == 1
    assert ledger.payload["orders"] == ledger.payload["fills"] == 0
    assert ledger.content_sha256 == ledger.payload["content_sha256"]


def test_external_action_ledger_rejects_out_of_order_actions() -> None:
    admission = _admit()
    consumption = _consume(admission)
    with pytest.raises(
        PerAxisCollectionAuthorizationAdmissionError,
        match="PER_AXIS_EXTERNAL_ACTION_ORDER_INVALID",
    ):
        build_per_axis_external_action_ledger(
            admission=admission,
            login_observed_at_utc="2026-08-16T13:19:00Z",
            mutation_started_at_utc="2026-08-16T13:11:00Z",
            mutation_verified_at_utc="2026-08-16T13:12:00Z",
            mutation_lf_byte_count=24420,
            consumption=consumption,
        )


def test_identical_duplicate_download_incident_is_disclosed_and_bound() -> None:
    admission = _admit()
    consumption = _consume(admission)
    result_bytes = _result_bytes(_result_payload())
    evidence = validate_per_axis_results_json(
        result_bytes=result_bytes,
        admission=admission,
        consumption=consumption,
        collected_at_utc="2026-08-16T13:30:00Z",
        backtest_id="example-backtest-id",
        project_root=ROOT,
    )
    carriers = tuple(
        {
            "file_name": name,
            "downloaded_at_utc": downloaded_at,
            "byte_count": len(result_bytes),
            "sha256": evidence.payload["source_result_file_sha256"],
        }
        for name, downloaded_at in (
            ("result.json", "2026-08-16T13:30:00Z"),
            ("result (1).json", "2026-08-16T13:31:00Z"),
            ("result (2).json", "2026-08-16T13:32:00Z"),
        )
    )
    incident = build_per_axis_result_download_delivery_incident(
        admission=admission,
        consumption=consumption,
        selected_file_name="result.json",
        download_carriers=carriers,
        browser_download_event_acknowledged=False,
    )
    assert incident.payload["download_trigger_count"] == 3
    assert incident.payload["identical_duplicate_file_count"] == 2
    assert incident.payload["all_downloaded_files_identical"] is True
    assert incident.content_sha256 == incident.payload["content_sha256"]

    ledger = build_per_axis_external_action_ledger(
        admission=admission,
        login_observed_at_utc="2026-08-16T13:10:00Z",
        mutation_started_at_utc="2026-08-16T13:11:00Z",
        mutation_verified_at_utc="2026-08-16T13:12:00Z",
        mutation_lf_byte_count=24420,
        consumption=consumption,
        evidence=evidence,
        download_incident=incident,
    )
    assert (
        ledger.payload["status"]
        == "EXTERNAL_ACTION_LIFECYCLE_COMPLETE_WITH_DISCLOSED_DOWNLOAD_DUPLICATES"
    )
    assert ledger.payload["result_download_trigger_count"] == 3
    assert ledger.payload["result_identical_duplicate_file_count"] == 2
    assert (
        ledger.payload["actions"][-1]["download_delivery_incident_content_sha256"]
        == incident.content_sha256
    )

    manifest = build_per_axis_collection_execution_evidence_manifest(
        admission=admission,
        consumption=consumption,
        evidence=evidence,
        download_incident=incident,
        action_ledger=ledger,
    )
    assert manifest.payload["artifact_count"] == 5
    assert manifest.payload["raw_result_committed"] is False
    assert manifest.payload["download_trigger_count"] == 3
    assert manifest.payload["cloud_backtest_attempt_count"] == 1
    assert manifest.content_sha256 == manifest.payload["content_sha256"]


def test_duplicate_download_incident_rejects_non_identical_files() -> None:
    admission = _admit()
    consumption = _consume(admission)
    with pytest.raises(
        PerAxisCollectionAuthorizationAdmissionError,
        match="PER_AXIS_DOWNLOAD_INCIDENT_NON_IDENTICAL_CARRIER",
    ):
        build_per_axis_result_download_delivery_incident(
            admission=admission,
            consumption=consumption,
            selected_file_name="result.json",
            download_carriers=(
                {
                    "file_name": "result.json",
                    "downloaded_at_utc": "2026-08-16T13:30:00Z",
                    "byte_count": 10,
                    "sha256": "a" * 64,
                },
                {
                    "file_name": "result (1).json",
                    "downloaded_at_utc": "2026-08-16T13:31:00Z",
                    "byte_count": 11,
                    "sha256": "b" * 64,
                },
            ),
            browser_download_event_acknowledged=False,
        )


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("extra_key", "PER_AXIS_RESULTS_AGGREGATE_KEYSET_INVALID"),
        ("bad_total", "PER_AXIS_RESULTS_AXIS_TOTAL_INVALID"),
        ("order", "PER_AXIS_RESULTS_ORDERS_NOT_EMPTY"),
        ("logs", "PER_AXIS_RESULTS_PROHIBITED_CARRIER"),
        ("bad_range", "PER_AXIS_RESULTS_RANGE_INVALID"),
        ("bad_terminal", "PER_AXIS_RESULTS_TERMINAL_INVALID"),
    ],
)
def test_results_json_fails_closed_on_unsafe_or_drifted_carrier(mutation: str, code: str) -> None:
    admission = _admit()
    consumption = _consume(admission)
    payload = _result_payload()
    runtime = payload["runtimeStatistics"]
    assert isinstance(runtime, dict)
    if mutation == "extra_key":
        runtime["TRADING2529_UNDECLARED"] = "0"
    elif mutation == "bad_total":
        runtime["TRADING2529_VOLUME_PRESENT_SESSIONS"] = "1201"
    elif mutation == "order":
        payload["orders"] = {"1": {}}
    elif mutation == "logs":
        payload["logs"] = ["not accepted"]
    elif mutation == "bad_range":
        algorithm = payload["algorithmConfiguration"]
        assert isinstance(algorithm, dict)
        algorithm["startDate"] = "2022-12-01T00:00:00Z"
    else:
        runtime["TRADING2529_TERMINAL"] = str(runtime["TRADING2529_TERMINAL"]).replace(
            "status=COMPLETE", "status=INVALID"
        )
    with pytest.raises(PerAxisCollectionAuthorizationAdmissionError, match=code):
        validate_per_axis_results_json(
            result_bytes=_result_bytes(payload),
            admission=admission,
            consumption=consumption,
            collected_at_utc="2026-08-16T13:30:00Z",
            backtest_id="example-backtest-id",
            project_root=ROOT,
        )
