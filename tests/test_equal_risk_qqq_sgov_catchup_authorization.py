from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "trading2563_equal_risk_catchup.py"
MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "research" / "equal_risk_qqq_sgov_catchup_run_authorization_v1.yaml"
)
PROPOSAL_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "equal_risk_qqq_sgov_catchup_dq_path_failure_fix_proposal_v1.yaml"
)


def _module():
    spec = importlib.util.spec_from_file_location("trading2563_equal_risk_catchup", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_manifest_freezes_exact_owner_envelope_and_source_identity() -> None:
    module = _module()
    payload = module.load_manifest(MANIFEST_PATH)
    module.validate_manifest_shape(payload, require_frozen=True)
    assert payload["status"] == "OWNER_EXACT_AUTHORIZED_NOT_YET_CONSUMED"
    assert payload["code_identity"]["exact_source_commit"] == (
        "8286392a5e5e5fa1ecd5aea6fb76fbd551854105"
    )
    assert payload["execution_scope"]["as_of"] == "2026-09-03"
    assert payload["execution_scope"]["observation_decision_dates"] == [
        "2026-06-22",
        "2026-06-24",
    ]


def test_manifest_rejects_any_forbidden_action() -> None:
    module = _module()
    payload = module.load_manifest(MANIFEST_PATH)
    payload["run_envelope"]["data_downloads"] = 1
    with pytest.raises(module.ManifestReplayError, match="forbidden action enabled"):
        module.validate_manifest_shape(payload, require_frozen=False)


def test_manifest_rejects_target_drift() -> None:
    module = _module()
    payload = module.load_manifest(MANIFEST_PATH)
    payload["execution_scope"]["observation_decision_dates"].append("2026-06-25")
    with pytest.raises(module.ManifestReplayError, match="observation target drift"):
        module.validate_manifest_shape(payload, require_frozen=False)


def test_failure_fix_proposal_only_allows_one_contained_root_dq_retry() -> None:
    module = _module()
    proposal = module.load_manifest(PROPOSAL_PATH)
    assert proposal["status"] == "OWNER_EXACT_RETRY_AUTHORIZATION_REQUIRED"
    assert proposal["best_fix"]["execution_root"] == "D:\\Work\\AITradingSystem"
    assert proposal["best_fix"]["retry_scope"] == {
        "corrected_canonical_dq_dispatches": 1,
        "completed_canonical_dq_executions_max": 1,
        "manifest_replays": 0,
        "isolated_bounded_rehearsals": 0,
        "canonical_maturity_updates": 0,
        "scoreboard_generations": 0,
        "continuity_audits": 0,
    }
    assert proposal["best_fix"]["stop_after_dq_result"] is True
    assert all(value == 0 for value in proposal["forbidden_actions"].values())
