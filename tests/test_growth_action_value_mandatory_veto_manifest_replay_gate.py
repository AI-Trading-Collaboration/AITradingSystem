from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_manifest_replay_gate as replay_gate,
)


def _context(sha: str = "a" * 64) -> replay_gate.RepositoryReplayContext:
    return replay_gate.RepositoryReplayContext(
        candidate_sha=sha,
        local_main_sha=sha,
        origin_main_sha=sha,
        branch_name="main",
        worktree_audit_status="PASS",
    )


def test_manifest_replay_gate_loads_exact_bound_authorities_and_sessions() -> None:
    loaded = replay_gate.load_mandatory_veto_manifest_replay_gate()

    assert loaded.file_sha256 == hashlib.sha256(loaded.path.read_bytes()).hexdigest()
    assert loaded.canonical_sha256 == loaded.policy.canonical_sha256
    assert loaded.execution_v2.policy.policy_id.endswith("execution_v2")
    assert loaded.successor_architecture.architecture.policy_id.endswith("architecture_v1")
    assert loaded.s8_contract.policy.policy_id.endswith("pit_receipt_adapter_contract_v1")
    assert len(loaded.session_ids) == 1202
    assert loaded.session_ids[0] == "2021-02-22"
    assert loaded.session_ids[-1] == "2025-12-02"
    assert loaded.terminal == (
        "MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE"
    )


def test_manifest_replay_separates_authorization_from_technical_blocker() -> None:
    report = replay_gate.run_mandatory_veto_manifest_replay(repository_context=_context())

    assert report.authorization_state == "STANDING_OWNER_SCOPE"
    assert report.authority_identity_replay_state == "PASS"
    assert report.exact_session_inventory_replay_state == "PASS"
    assert report.successor_compatibility_replay_state == "PASS"
    assert report.source_capability_replay_state == "BLOCKED"
    assert report.manifest_replay_state == "BLOCKED"
    assert report.technical_validation_state == "BLOCKED"


def test_manifest_replay_has_five_typed_pre_query_blockers() -> None:
    report = replay_gate.run_mandatory_veto_manifest_replay(repository_context=_context())

    assert report.blocker_reason_codes == (
        "FMP_HISTORICAL_ROW_AVAILABLE_AT_UNPROVEN",
        "CBOE_VIX_HISTORICAL_PUBLICATION_VINTAGE_UNPROVEN",
        "FED_FOMC_REVISION_LEDGER_UNAVAILABLE",
        "BLS_RELEASE_SCHEDULE_REVISION_LEDGER_UNAVAILABLE",
        "BEA_FROZEN_ENDPOINT_NOT_SCHEDULE_REVISION_AUTHORITY",
    )


def test_manifest_replay_preserves_zero_effect_boundary() -> None:
    loaded = replay_gate.load_mandatory_veto_manifest_replay_gate()
    counters = loaded.policy.actual_counters.model_dump()

    assert set(counters.values()) == {0}
    assert loaded.policy.safety.provider_query_allowed is False
    assert loaded.policy.safety.real_dq_allowed is False
    assert loaded.policy.safety.backtest_allowed is False
    assert loaded.policy.safety.orders_allowed is False
    assert loaded.policy.safety.fills_allowed is False
    assert loaded.policy.safety.positions_allowed is False
    assert loaded.policy.safety.production_effect == "none"
    assert loaded.policy.safety.broker_action == "none"


def test_successor_bridge_removes_tqqq_from_market_state_conjunction() -> None:
    bridge = (
        replay_gate.load_mandatory_veto_manifest_replay_gate().policy.successor_compatibility_bridge
    )

    assert bridge.successor_mandatory_veto_ids == (
        "broad_market_risk_off_veto",
        "realized_volatility_veto",
        "scheduled_event_risk_veto",
        "underlying_trend_break_veto",
    )
    assert bridge.legacy_non_market_field == "tqqq_veto"
    assert bridge.successor_non_market_guard == "NO_LEVERAGE_ETF_ACTION_GUARD"
    assert bridge.tqqq_veto_in_market_state_conjunction is False


def test_repository_context_fails_closed_on_unpublished_candidate() -> None:
    with pytest.raises(ValidationError, match="must be identical"):
        replay_gate.RepositoryReplayContext(
            candidate_sha="a" * 64,
            local_main_sha="b" * 64,
            origin_main_sha="b" * 64,
            branch_name="main",
            worktree_audit_status="PASS",
        )


def test_repository_context_fails_closed_off_main() -> None:
    with pytest.raises(ValidationError):
        replay_gate.RepositoryReplayContext(
            candidate_sha="a" * 64,
            local_main_sha="a" * 64,
            origin_main_sha="a" * 64,
            branch_name="codex/task",
            worktree_audit_status="PASS",
        )


def test_policy_rejects_query_permission_drift() -> None:
    policy = replay_gate.load_mandatory_veto_manifest_replay_gate().policy
    payload = copy.deepcopy(policy.model_dump(mode="json"))
    payload["source_capability_gate"]["rows"][0]["provider_query_allowed"] = True

    with pytest.raises(ValidationError):
        replay_gate.MandatoryVetoManifestReplayGate.model_validate(payload)


def test_policy_rejects_frozen_endpoint_replacement() -> None:
    policy = replay_gate.load_mandatory_veto_manifest_replay_gate().policy
    payload = copy.deepcopy(policy.model_dump(mode="json"))
    payload["source_capability_gate"]["rows"][4]["endpoint"] = "https://www.bea.gov/news/schedule"

    with pytest.raises(ValidationError, match="source capability blocker surface drifted"):
        replay_gate.MandatoryVetoManifestReplayGate.model_validate(payload)


def test_replay_executor_rejects_code_sha_drift() -> None:
    binding = (
        replay_gate.load_mandatory_veto_manifest_replay_gate().policy.replay_executor
    ).model_copy(update={"file_sha256": "0" * 64})

    with pytest.raises(ValueError, match="executor file SHA-256 mismatch"):
        replay_gate._validate_replay_executor(binding, project_root=PROJECT_ROOT)


def test_policy_rejects_tqqq_reintroduced_as_market_veto() -> None:
    policy = replay_gate.load_mandatory_veto_manifest_replay_gate().policy
    payload = copy.deepcopy(policy.model_dump(mode="json"))
    payload["successor_compatibility_bridge"]["successor_mandatory_veto_ids"][0] = "tqqq_veto"

    with pytest.raises(ValidationError, match="successor mandatory veto bridge drifted"):
        replay_gate.MandatoryVetoManifestReplayGate.model_validate(payload)


def test_exact_session_validator_rejects_ordered_inventory_drift(tmp_path: Path) -> None:
    loaded = replay_gate.load_mandatory_veto_manifest_replay_gate()
    inventory = loaded.policy.exact_session_inventory
    source = PROJECT_ROOT / inventory.path
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["session_ids"][1], payload["session_ids"][2] = (
        payload["session_ids"][2],
        payload["session_ids"][1],
    )
    target = tmp_path / "run_scope.json"
    target.write_text(json.dumps(payload), encoding="utf-8")
    mutated = inventory.model_copy(
        update={
            "path": str(target),
            "file_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
    )

    with pytest.raises(ValueError, match="unique and ordered"):
        replay_gate._validate_exact_sessions(mutated, project_root=tmp_path)


def test_report_canonical_bytes_are_deterministic() -> None:
    report = replay_gate.run_mandatory_veto_manifest_replay(repository_context=_context())

    assert report.canonical_bytes.endswith(b"\n")
    assert hashlib.sha256(report.canonical_bytes).hexdigest() == report.canonical_sha256
