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


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _load_json(name: str) -> dict[str, Any]:
    value = json.loads((EXECUTION_ROOT / name).read_text(encoding="utf-8"))
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
