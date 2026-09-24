"""GOV-007 sibling lease-store migration admission checks.

These tests exercise the pure admission rules of the entry point on synthetic
values. They never run the live fence, touch a real lease store, or migrate
anything; the migration helper itself is covered by test_arch_005_lease_arbiter.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/architecture_arch005_sibling_lease_store_migration.py"
CANDIDATE = "c" * 40
OWNER_SHA = "a" * 64
DEVX_014_OWNER_REF = "owner_instruction:DEVX-014:2026-09-06:arbiter-safety-fix"


def _cli() -> Any:
    spec = importlib.util.spec_from_file_location("synthetic_sibling_migration_cli", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve their defining module through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "authorization_id": "synthetic-root-v1",
        "task_id": "GOV-007_PRE_MIGRATION_CONVERGENCE_PROGRAM",
        "owner_instruction_ref": "owner_decision:SYNTHETIC:root",
        "target_checkout_root": "D:/Synthetic/Target",
        "expected_legacy_owner_sha256": OWNER_SHA,
        "rationale": "synthetic",
        "review_or_expiry": "synthetic",
    }
    row.update(overrides)
    return row


def _write(tmp_path: Path, rows: list[dict[str, Any]]) -> Path:
    import json

    path = tmp_path / "authorizations.yaml"
    document = {
        "schema_version": "lease_arbiter_sibling_migration_authorizations.v1",
        "authorizations": rows,
    }
    # JSON is a YAML subset; the strict loader reads it unchanged.
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_repository_authorizations_register_only_the_owner_approved_main_root() -> None:
    cli = _cli()
    authorization = cli.load_authorization(cli.AUTHORIZATIONS_PATH, "gov-007-main-checkout-root-v1")
    assert authorization.task_id == "GOV-007_PRE_MIGRATION_CONVERGENCE_PROGRAM"
    assert authorization.owner_instruction_ref == (
        "owner_decision:GOV-007:2026-09-24:main_checkout_lease_arbiter_migration_v1"
    )
    assert authorization.target_checkout_root == Path("D:/Work/AITradingSystem")
    # DEVX-014's own scope is never reused as authority for another root.
    assert authorization.owner_instruction_ref != DEVX_014_OWNER_REF


def test_unknown_authorization_is_rejected(tmp_path: Path) -> None:
    cli = _cli()
    path = _write(tmp_path, [_row()])
    with pytest.raises(cli.SiblingMigrationError, match="AUTHORIZATION_UNKNOWN"):
        cli.load_authorization(path, "not-registered")


@pytest.mark.parametrize(
    "rows",
    [
        [_row(target_checkout_root="relative/root")],
        [_row(expected_legacy_owner_sha256="not-a-digest")],
        [_row(task_id=" ")],
        [_row(), _row()],
        [{**_row(), "extra": "key"}],
    ],
)
def test_malformed_authorization_lists_fail_closed(
    tmp_path: Path, rows: list[dict[str, Any]]
) -> None:
    cli = _cli()
    path = _write(tmp_path, rows)
    with pytest.raises(cli.SiblingMigrationError, match="AUTHORIZATIONS_INVALID"):
        cli.load_authorization(path, "synthetic-root-v1")


def _identity(**overrides: str) -> dict[str, str]:
    values = {"branch": "main", "head": CANDIDATE, "main": CANDIDATE, "origin_main": CANDIDATE}
    values.update(overrides)
    return values


@pytest.mark.parametrize(
    ("overrides", "accepted"),
    [
        ({}, True),
        ({"branch": "codex/x"}, False),
        ({"head": "d" * 40}, False),
        ({"origin_main": "d" * 40}, False),
    ],
)
def test_coordinator_must_sit_on_the_published_candidate(
    overrides: dict[str, str], accepted: bool
) -> None:
    cli = _cli()
    values = _identity(**overrides)
    if accepted:
        cli.check_published_identity(values, CANDIDATE)
    else:
        with pytest.raises(cli.SiblingMigrationError):
            cli.check_published_identity(values, CANDIDATE)


def test_missing_candidate_is_rejected() -> None:
    cli = _cli()
    with pytest.raises(cli.SiblingMigrationError, match="CANDIDATE_NOT_PUBLISHED"):
        cli.check_published_identity(_identity(), None)


def _authorization(cli: Any, root: Path) -> Any:
    return cli.Authorization(
        authorization_id="synthetic-root-v1",
        task_id="GOV-007_PRE_MIGRATION_CONVERGENCE_PROGRAM",
        owner_instruction_ref="owner_decision:SYNTHETIC:root",
        target_checkout_root=root,
        expected_legacy_owner_sha256=OWNER_SHA,
    )


def test_target_root_must_be_exact_sibling_in_same_repository(tmp_path: Path) -> None:
    cli = _cli()
    target, coordinator, common = tmp_path / "target", tmp_path / "coord", tmp_path / "common"
    for path in (target, coordinator, common):
        path.mkdir()
    authorization = _authorization(cli, target)
    cli.check_target_root(
        authorization,
        target_root=target,
        coordinator_root=coordinator,
        target_common=common,
        coordinator_common=common,
    )
    with pytest.raises(cli.SiblingMigrationError, match="TARGET_ROOT_NOT_AUTHORIZED"):
        cli.check_target_root(
            authorization,
            target_root=coordinator,
            coordinator_root=target,
            target_common=common,
            coordinator_common=common,
        )
    with pytest.raises(cli.SiblingMigrationError, match="TARGET_IS_COORDINATOR"):
        cli.check_target_root(
            authorization,
            target_root=target,
            coordinator_root=target,
            target_common=common,
            coordinator_common=common,
        )
    with pytest.raises(cli.SiblingMigrationError, match="TARGET_NOT_SAME_REPOSITORY"):
        cli.check_target_root(
            authorization,
            target_root=target,
            coordinator_root=coordinator,
            target_common=tmp_path / "foreign",
            coordinator_common=common,
        )


def test_target_store_with_active_or_invalid_leases_is_rejected() -> None:
    cli = _cli()
    cli.check_target_leases([], "PASS")
    with pytest.raises(cli.SiblingMigrationError, match="TARGET_HAS_ACTIVE_LEASE"):
        cli.check_target_leases(["lease-synthetic"], "PASS")
    with pytest.raises(cli.SiblingMigrationError, match="TARGET_LEASE_REPLAY_INVALID"):
        cli.check_target_leases([], "FAIL")


@pytest.mark.parametrize(
    "field",
    [
        "store_root",
        "actor",
        "owner_instruction_ref",
        "publication_transaction_id",
        "publication_transaction_sha256",
        "observed_owner_sha256",
    ],
)
def test_quiescence_must_bind_every_admission_fact(tmp_path: Path, field: str) -> None:
    cli = _cli()
    store = tmp_path / "leases"
    authorization = _authorization(cli, tmp_path)
    binding = {"transaction_id": "synthetic-tx", "transaction_sha256": "b" * 64}
    receipt = {
        "store_root": store.as_posix(),
        "actor": cli.ACTOR,
        "owner_instruction_ref": authorization.owner_instruction_ref,
        "publication_transaction_id": "synthetic-tx",
        "publication_transaction_sha256": "b" * 64,
        "observed_owner_sha256": OWNER_SHA,
    }
    cli.check_quiescence(receipt, store_root=store, authorization=authorization, binding=binding)
    receipt[field] = "tampered"
    with pytest.raises(cli.SiblingMigrationError, match="QUIESCENCE_BINDING_MISMATCH"):
        cli.check_quiescence(
            receipt, store_root=store, authorization=authorization, binding=binding
        )


def test_devx_014_entry_point_keeps_its_exact_single_scope() -> None:
    source = (ROOT / "scripts/architecture_arch005_lease_arbiter.py").read_text(encoding="utf-8")
    assert f'OWNER_REF = "{DEVX_014_OWNER_REF}"' in source
    assert "lease_arbiter_sibling_migration_authorizations" not in source
