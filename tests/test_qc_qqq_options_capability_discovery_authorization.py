from __future__ import annotations

import hashlib
import shutil
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.qc_qqq_options_capability_discovery_authorization import (
    CAPABILITY_DISCOVERY_ALLOWED_ACTIONS,
    CAPABILITY_DISCOVERY_PROHIBITED_ACTIONS,
    QCCapabilityDiscoveryAuthorizationContractError,
)
from ai_trading_system.qqq_options_capability_discovery_authorization import (
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH,
    load_qc_qqq_options_capability_discovery_authorization,
)

AUTHORIZATION_ID = (
    "owner_decision:TRADING-2480:2026-08-04:"
    "authorize_single_no_order_qc_capability_discovery_run_v1"
)
BOUND_PATHS = (
    Path("config/research/qc_qqq_options_capability_admission_v1.yaml"),
    Path("inputs/external_validation/qc_qqq_options_capability_evidence_20260803.json"),
    Path(
        "inputs/external_validation/"
        "qc_qqq_options_admission_"
        "e3a987b2b671e922175b35783dded6f4bbfa51dd5aaa523f415547026434ba04.json"
    ),
    Path("config/data/us_equity_special_closure_registry.yaml"),
    Path("config/research/dynamic_walk_forward_policy.yaml"),
)


def test_tracked_capability_discovery_authorization_is_exact_and_cash_preserving() -> None:
    first = load_qc_qqq_options_capability_discovery_authorization()
    second = load_qc_qqq_options_capability_discovery_authorization()
    authorization = first.authorization

    assert first == second
    assert authorization.owner_authorization_id == AUTHORIZATION_ID
    assert authorization.scope.requested_start == date(2025, 12, 2)
    assert authorization.scope.requested_end == date(2025, 12, 2)
    assert authorization.scope.maximum_runtime_minutes == 10
    assert authorization.scope.maximum_projects == 1
    assert authorization.scope.maximum_cloud_backtests == 1
    assert authorization.scope.maximum_order_count == 0
    assert authorization.scope.maximum_contract_quantity == 0
    assert authorization.actors.collector_id == "codex_pilot_coordinator"
    assert authorization.actors.independent_reviewer_id == "project_owner"
    assert authorization.allowed_actions == CAPABILITY_DISCOVERY_ALLOWED_ACTIONS
    assert authorization.prohibited_actions == CAPABILITY_DISCOVERY_PROHIBITED_ACTIONS
    assert authorization.safety.cloud_backtest_authorized is True
    assert authorization.safety.orders_allowed is False
    assert authorization.safety.fills_allowed is False
    assert authorization.safety.positions_allowed is False
    assert authorization.safety.raw_options_data_download_allowed is False
    assert authorization.safety.production_effect == "none"
    assert authorization.safety.broker_action == "none"
    assert first.prior_receipt.decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert first.prior_receipt.bounded_pilot_preparation_allowed is False
    assert first.prior_receipt.confirmed_item_count == 7
    assert first.prior_receipt.confirmed_field_count == 3
    assert (
        first.authorization_policy_sha256
        == hashlib.sha256(
            (
                PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
            ).read_bytes()
        ).hexdigest()
    )
    assert first.authorization_canonical_sha256 == authorization.canonical_sha256


@pytest.mark.parametrize(
    "mutate",
    (
        lambda payload: payload.update(
            {"owner_authorization_id": ("owner_decision:TRADING-2480:2026-08-04:forged")}
        ),
        lambda payload: payload["scope"].update({"maximum_order_count": 1}),
        lambda payload: payload["scope"].update({"maximum_cloud_backtests": 2}),
        lambda payload: payload["scope"].update({"maximum_runtime_minutes": 11}),
        lambda payload: payload["actors"].update(
            {"independent_reviewer_id": "codex_pilot_coordinator"}
        ),
        lambda payload: payload["allowed_actions"].pop(),
        lambda payload: payload["safety"].update({"raw_rows_may_be_logged": True}),
        lambda payload: payload["safety"].update({"production_allowed": True}),
    ),
)
def test_forged_scope_actor_action_and_safety_expansion_fail_closed(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    root = _copy_authorization_tree(tmp_path)
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    mutate(payload)
    policy_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(QCCapabilityDiscoveryAuthorizationContractError) as raised:
        load_qc_qqq_options_capability_discovery_authorization(project_root=root)
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID"


def test_closed_or_out_of_window_date_fails_reviewed_calendar_gate(tmp_path: Path) -> None:
    root = _copy_authorization_tree(tmp_path)
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    payload["scope"]["requested_start"] = date(2025, 12, 25)
    payload["scope"]["requested_end"] = date(2025, 12, 25)
    policy_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(QCCapabilityDiscoveryAuthorizationContractError) as raised:
        load_qc_qqq_options_capability_discovery_authorization(project_root=root)
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID"
    assert "not a normal XNYS session" in raised.value.message


def test_bound_receipt_or_window_policy_tamper_fails_closed(tmp_path: Path) -> None:
    for relative_path in (
        BOUND_PATHS[2],
        Path("config/research/dynamic_walk_forward_policy.yaml"),
    ):
        root = _copy_authorization_tree(tmp_path / relative_path.name)
        target = root / relative_path
        target.write_bytes(target.read_bytes() + b"\n")
        with pytest.raises(QCCapabilityDiscoveryAuthorizationContractError) as raised:
            load_qc_qqq_options_capability_discovery_authorization(project_root=root)
        assert raised.value.code == "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID"
        assert "SHA-256 mismatch" in raised.value.message


def test_path_escape_and_symlink_authority_fail_closed(tmp_path: Path) -> None:
    root = _copy_authorization_tree(tmp_path / "escape")
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    payload["prior_evidence_path"] = "../outside.json"
    policy_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(QCCapabilityDiscoveryAuthorizationContractError) as escaped:
        load_qc_qqq_options_capability_discovery_authorization(project_root=root)
    assert escaped.value.code == "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID"
    assert "escapes the project root" in escaped.value.message

    symlink_root = _copy_authorization_tree(tmp_path / "symlink")
    real_policy = symlink_root / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
    linked_policy = real_policy.with_name("authorization_link.yaml")
    try:
        linked_policy.symlink_to(real_policy)
    except OSError:
        pytest.skip("symlink creation is unavailable in this Windows environment")
    with pytest.raises(QCCapabilityDiscoveryAuthorizationContractError) as linked:
        load_qc_qqq_options_capability_discovery_authorization(
            linked_policy,
            project_root=symlink_root,
        )
    assert linked.value.code == "QC_CAPABILITY_DISCOVERY_AUTHORIZATION_INVALID"
    assert "cannot use a symlink" in linked.value.message


def test_yaml_key_permutation_preserves_canonical_authorization_identity(
    tmp_path: Path,
) -> None:
    root = _copy_authorization_tree(tmp_path)
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH
    payload = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    policy_path.write_text(
        yaml.safe_dump(dict(reversed(tuple(payload.items()))), sort_keys=False),
        encoding="utf-8",
        newline="\n",
    )

    tracked = load_qc_qqq_options_capability_discovery_authorization()
    permuted = load_qc_qqq_options_capability_discovery_authorization(project_root=root)
    assert permuted.authorization == tracked.authorization
    assert permuted.authorization_canonical_sha256 == (tracked.authorization_canonical_sha256)
    assert permuted.authorization_policy_sha256 != tracked.authorization_policy_sha256


def _copy_authorization_tree(tmp_path: Path) -> Path:
    root = tmp_path / "repository"
    paths = (
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_AUTHORIZATION_PATH,
        *BOUND_PATHS,
    )
    for relative_path in paths:
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(PROJECT_ROOT / relative_path, target)
    return root
