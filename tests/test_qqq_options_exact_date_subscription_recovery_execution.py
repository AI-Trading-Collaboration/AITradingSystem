from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = (
    ROOT
    / "config/research/qc_qqq_options_exact_date_subscription_recovery_execution_v1.yaml"
)
EXECUTION_ROOT = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v1"
)
CANDIDATE_PATH = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_v1/main.py"
)
V2_POLICY_PATH = (
    ROOT
    / "config/research/qc_qqq_options_exact_date_subscription_recovery_execution_v2.yaml"
)
V2_EXECUTION_ROOT = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v2"
)
V3_POLICY_PATH = (
    ROOT
    / "config/research/qc_qqq_options_exact_date_subscription_recovery_execution_v3.yaml"
)
V3_EXECUTION_ROOT = (
    ROOT
    / "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v3"
)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _load_json(name: str) -> dict[str, Any]:
    value = json.loads((EXECUTION_ROOT / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _load_json_from(root: Path, name: str) -> dict[str, Any]:
    value = json.loads((root / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _content_sha256(payload: dict[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("content_sha256", None)
    raw = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256(raw)


def test_s3_execution_policy_freezes_one_zero_order_standing_scope() -> None:
    policy = yaml.safe_load(POLICY_PATH.read_text(encoding="utf-8"))

    assert policy["status"] == "REVIEWED_READY_UNUSED"
    assert policy["authorization"] == {
        "risk_tier": "R1_BOUNDED_RESEARCH_SANDBOX",
        "authorization_state": "STANDING_OWNER_SCOPE",
        "owner_intent_source": (
            "PROJECT_OWNER_CURRENT_CODEX_DIALOG_CONTINUE_ENGINEERING_REPAIR"
        ),
        "standing_scope_policy": (
            "config/governance/risk_tiered_external_action_evidence_admission_v1.yaml"
        ),
        "preformatted_exact_token_required": False,
        "consumed_on_backtest_dispatch": True,
        "automatic_retry_allowed": False,
    }
    assert policy["repository"]["ordinary_pushed_main_sha"] == (
        "d4dd62cd967404643ff6931f3b2451aeb67124e1"
    )
    assert policy["target"] == {
        "service": "QuantConnect Free Web IDE",
        "clone_project_id": 35444189,
        "original_project_id": 34808569,
        "target_file_name": "main.py",
        "original_project_mutations_allowed": 0,
        "maximum_new_clones": 0,
    }
    assert policy["run"]["maximum_additional_clone_project_mutations"] == 1
    assert policy["run"]["maximum_additional_saves"] == 1
    assert policy["run"]["maximum_additional_automatic_cloud_builds"] == 1
    assert policy["run"]["maximum_zero_order_cloud_backtests"] == 1
    assert policy["run"]["maximum_provider_queries"] == 1
    assert policy["run"]["maximum_orders"] == 0
    assert policy["run"]["maximum_fills"] == 0
    assert policy["run"]["cross_date_fallback_allowed"] is False
    assert all(value is False for value in policy["safety"].values() if isinstance(value, bool))
    assert policy["safety"]["broker_action"] == "none"
    assert policy["safety"]["production_effect"] == "none"


def test_execution_json_artifacts_are_self_sealed_and_ready_unused() -> None:
    admission = _load_json("standing_scope_admission.json")
    run_scope = _load_json("run_scope.json")
    manifest = _load_json("execution_manifest.json")

    assert admission["content_sha256"] == _content_sha256(admission)
    assert run_scope["content_sha256"] == _content_sha256(run_scope)
    assert manifest["content_sha256"] == _content_sha256(manifest)
    assert admission["scope_status"] == "READY_UNUSED"
    assert admission["manifest_replay_state"] == "NOT_EXECUTED"
    assert manifest["status"] == "READY_UNUSED"
    assert manifest["technical_validation_state"] == "NOT_EXECUTED"
    assert set(manifest["actual_counters"].values()) == {0}


def test_execution_manifest_replays_every_bound_artifact() -> None:
    manifest = _load_json("execution_manifest.json")

    for artifact in manifest["artifacts"]:
        path = ROOT / artifact["path"]
        raw = path.read_bytes()
        assert len(raw) == artifact["byte_count"]
        assert _sha256(raw) == artifact["sha256"]

    candidate = CANDIDATE_PATH.read_bytes().replace(b"\r\n", b"\n")
    assert len(candidate) == manifest["candidate_lf_byte_count"] == 31720
    assert _sha256(candidate) == manifest["candidate_lf_sha256"] == (
        "d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3"
    )


def test_run_scope_requires_exact_date_recovery_terminal() -> None:
    scope = _load_json("run_scope.json")

    assert scope["acceptance"] == {
        "delivery_path": "EXACT_DATE_PROVIDER_HISTORY_RECOVERY",
        "exact_availability_identity_required": True,
        "exact_source_date_match_required": True,
        "expected_normal_slice_session_count": 1201,
        "expected_recovered_session_count": 1,
        "expected_session_count": 1202,
        "expected_unresolved_session_count": 0,
        "execution_terminal_status": "COMPLETE",
        "recovery_status": "ACCEPTED",
        "target_source_date": "2022-08-26",
    }
    assert scope["cross_date_fallback_allowed"] is False
    assert scope["maximum_provider_queries"] == 1
    assert scope["maximum_zero_order_cloud_backtests"] == 1
    assert scope["orders"] == scope["fills"] == 0
    assert set(scope["evidence_output"].values()) == {False, True}
    assert scope["evidence_output"]["terminal_statistics_only"] is True


def test_predispatch_environment_blocker_preserves_unused_execution_scope() -> None:
    evidence = _load_json("predispatch_environment_evidence.json")

    assert evidence["content_sha256"] == _content_sha256(evidence)
    assert evidence["blocker_code"] == (
        "QC_FREE_CODING_SESSION_UNAVAILABLE_PRE_DISPATCH"
    )
    assert evidence["candidate_dispatch_state"] == "NOT_DISPATCHED"
    assert evidence["authorization_state"] == "STANDING_OWNER_SCOPE"
    assert evidence["authorization_consumption_state"] == (
        "UNCONSUMED_NO_BACKTEST_DISPATCH"
    )
    assert evidence["technical_validation_state"] == (
        "BLOCKED_PRE_DISPATCH_NOT_EXECUTED"
    )
    assert evidence["browser_observations"]["target_clone_project_id"] == 35444189
    assert evidence["browser_observations"]["final_heading"] == (
        "No Coding Session Available"
    )
    assert evidence["browser_observations"]["official_coding_session_retry_attempts"] == 1
    assert set(evidence["actual_manifest_counters"].values()) == {0}


def test_s3_v2_separates_startup_and_candidate_build_budgets() -> None:
    policy = yaml.safe_load(V2_POLICY_PATH.read_text(encoding="utf-8"))
    evidence = _load_json_from(V2_EXECUTION_ROOT, "environment_startup_evidence.json")
    admission = _load_json_from(V2_EXECUTION_ROOT, "standing_scope_admission.json")
    run_scope = _load_json_from(V2_EXECUTION_ROOT, "run_scope.json")
    manifest = _load_json_from(V2_EXECUTION_ROOT, "execution_manifest.json")

    assert policy["status"] == "REVIEWED_READY_UNUSED"
    assert policy["startup_build_accounting"] == {
        "environment_startup_build_id": "11e9d4-8b195b",
        "environment_startup_build_engine": "2.5.0.0.18024",
        "environment_startup_build_code_identity": "CURRENT_CLONE_TRADING_2537_V2",
        "observed_environment_startup_builds": 1,
        "maximum_environment_startup_builds": 1,
        "maximum_candidate_builds": 1,
        "maximum_total_additional_automatic_cloud_builds": 2,
        "remaining_candidate_builds": 1,
    }
    assert policy["run"]["maximum_candidate_automatic_cloud_builds"] == 1
    assert evidence["content_sha256"] == _content_sha256(evidence)
    assert evidence["current_clone_lf_byte_count"] == 26587
    assert evidence["current_clone_lf_sha256"] == (
        "06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2"
    )
    assert evidence["environment_startup_build"]["build_id"] == "11e9d4-8b195b"
    assert evidence["candidate_dispatch_state"] == "NOT_DISPATCHED"
    assert admission["content_sha256"] == _content_sha256(admission)
    assert admission["policy_file_sha256_at_admission"] == _sha256(
        V2_POLICY_PATH.read_bytes()
    )
    assert admission["declared_maxima"]["total_additional_automatic_cloud_builds"] == 2
    assert admission["declared_maxima"]["candidate_automatic_cloud_builds"] == 1
    assert run_scope["content_sha256"] == _content_sha256(run_scope)
    assert run_scope["automatic_build_accounting"] == {
        "candidate_maximum": 1,
        "environment_startup_observed": 1,
        "environment_startup_maximum": 1,
        "total_maximum": 2,
    }
    assert policy["authorization"]["automatic_retry_allowed"] is False
    assert policy["run"]["maximum_orders"] == policy["run"]["maximum_fills"] == 0
    assert manifest["content_sha256"] == _content_sha256(manifest)
    assert manifest["status"] == "READY_CONTINUATION_UNUSED"
    assert manifest["manifest_replay_state"] == "NOT_EXECUTED"
    assert manifest["technical_validation_state"] == "NOT_EXECUTED"
    assert manifest["actual_counters"] == {
        "candidate_automatic_cloud_builds": 0,
        "environment_startup_automatic_cloud_builds": 1,
        "additional_clone_project_mutations": 0,
        "additional_saves": 0,
        "fills": 0,
        "new_clones": 0,
        "orders": 0,
        "original_project_mutations": 0,
        "provider_queries": 0,
        "total_additional_automatic_cloud_builds": 1,
        "zero_order_cloud_backtests": 0,
    }
    for artifact in manifest["artifacts"]:
        raw = (ROOT / artifact["path"]).read_bytes()
        if artifact["kind"] == "PROJECT_CODE":
            raw = raw.replace(b"\r\n", b"\n")
        assert len(raw) == artifact["byte_count"]
        assert _sha256(raw) == artifact["sha256"]


def test_s3_v3_seals_two_startup_builds_and_one_remaining_candidate_build() -> None:
    policy = yaml.safe_load(V3_POLICY_PATH.read_text(encoding="utf-8"))
    evidence = _load_json_from(V3_EXECUTION_ROOT, "environment_startup_evidence.json")
    admission = _load_json_from(V3_EXECUTION_ROOT, "standing_scope_admission.json")
    run_scope = _load_json_from(V3_EXECUTION_ROOT, "run_scope.json")
    manifest = _load_json_from(V3_EXECUTION_ROOT, "execution_manifest.json")

    accounting = policy["startup_build_accounting"]
    assert accounting["environment_startup_build_ids"] == [
        "11e9d4-8b195b",
        "684f9c-8b195b",
    ]
    assert accounting["observed_environment_startup_builds"] == 2
    assert accounting["maximum_environment_startup_builds"] == 2
    assert accounting["maximum_candidate_builds"] == 1
    assert accounting["maximum_total_additional_automatic_cloud_builds"] == 3
    assert accounting["remaining_candidate_builds"] == 1
    assert evidence["content_sha256"] == _content_sha256(evidence)
    assert [item["build_id"] for item in evidence["environment_startup_builds"]] == [
        "11e9d4-8b195b",
        "684f9c-8b195b",
    ]
    assert evidence["candidate_dispatch_state"] == "NOT_DISPATCHED"
    assert admission["content_sha256"] == _content_sha256(admission)
    assert admission["declared_maxima"]["total_additional_automatic_cloud_builds"] == 3
    assert admission["declared_maxima"]["candidate_automatic_cloud_builds"] == 1
    assert run_scope["content_sha256"] == _content_sha256(run_scope)
    assert run_scope["automatic_build_accounting"] == {
        "candidate_maximum": 1,
        "environment_startup_observed": 2,
        "environment_startup_maximum": 2,
        "total_maximum": 3,
    }
    assert policy["authorization"]["automatic_retry_allowed"] is False
    assert policy["run"]["maximum_orders"] == policy["run"]["maximum_fills"] == 0
    assert admission["policy_file_sha256_at_admission"] == _sha256(
        V3_POLICY_PATH.read_bytes()
    )
    assert manifest["content_sha256"] == _content_sha256(manifest)
    assert manifest["status"] == "READY_CONTINUATION_UNUSED"
    assert manifest["manifest_replay_state"] == "NOT_EXECUTED"
    assert manifest["actual_counters"]["environment_startup_automatic_cloud_builds"] == 2
    assert manifest["actual_counters"]["candidate_automatic_cloud_builds"] == 0
    assert manifest["actual_counters"]["total_additional_automatic_cloud_builds"] == 2
    for artifact in manifest["artifacts"]:
        raw = (ROOT / artifact["path"]).read_bytes()
        if artifact["kind"] == "PROJECT_CODE":
            raw = raw.replace(b"\r\n", b"\n")
        assert len(raw) == artifact["byte_count"]
        assert _sha256(raw) == artifact["sha256"]


def test_s3_v3_terminal_evidence_accepts_exact_date_recovery() -> None:
    evidence = _load_json_from(V3_EXECUTION_ROOT, "export_safe_terminal_evidence.json")

    assert evidence["content_sha256"] == _content_sha256(evidence)
    assert evidence["authorization_state"] == "STANDING_OWNER_SCOPE"
    assert evidence["authorization_consumption_state"] == (
        "CONSUMED_ON_SINGLE_BACKTEST_DISPATCH"
    )
    assert evidence["technical_validation_state"] == "PASS"
    assert evidence["project_code_lf_byte_count"] == 31720
    assert evidence["project_code_lf_sha256"] == (
        "d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3"
    )
    assert evidence["cloud_readback_exact_candidate_match"] is True
    assert evidence["candidate_build_id"] == "d65491-f6b483"
    assert evidence["backtest_id"] == "8142b39f1c76a10471a355fc1eb27a1d"
    assert evidence["lean_engine_version"] == "2.5.0.0.18024"
    assert evidence["requested_range"] == evidence["evaluated_range"] == (
        "2021-02-22..2025-12-02"
    )
    assert evidence["expected_session_count"] == 1202
    assert evidence["observed_session_count"] == 1202
    assert evidence["target_source_date"] == "2022-08-26"
    assert evidence["recovery_source_date"] == "2022-08-26"
    assert evidence["recovery_availability_date"] == "2022-08-27"
    assert evidence["recovery_status"] == "ACCEPTED"
    assert evidence["delivery_path"] == "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
    assert evidence["provider_query_attempt_count"] == 1
    assert evidence["exact_date_record_count"] == 1
    assert evidence["exact_date_contract_count"] == 6496
    assert evidence["non_target_record_count"] == 0
    assert evidence["invalid_availability_record_count"] == 0
    assert evidence["normal_slice_session_count"] == 1201
    assert evidence["recovered_session_count"] == 1
    assert evidence["unresolved_session_count"] == 0
    assert evidence["execution_terminal"] == "COMPLETE"
    assert evidence["actual_counters"] == {
        "additional_clone_project_mutations": 1,
        "additional_saves": 1,
        "candidate_automatic_cloud_builds": 1,
        "environment_startup_automatic_cloud_builds": 2,
        "fills": 0,
        "new_clones": 0,
        "orders": 0,
        "original_project_mutations": 0,
        "provider_queries": 1,
        "total_additional_automatic_cloud_builds": 3,
        "zero_order_cloud_backtests": 1,
    }
    assert evidence["orders"] == evidence["fills"] == 0
    assert evidence["portfolio_invested"] is False
    assert evidence["production_effect"] == "none"
    assert evidence["broker_action"] == "none"
