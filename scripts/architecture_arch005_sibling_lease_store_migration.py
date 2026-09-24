"""GOV-007: migrate an owner-authorized sibling checkout's legacy lease arbiter.

DEVX-014's entry point only covers its own task root and explicitly excludes the
physical main checkout. This entry point accepts only exact roots registered in
``config/architecture/lease_arbiter_sibling_migration_authorizations.yaml``. It
reuses the unchanged ``lease_arbiter.migrate_legacy_arbiter`` helper, so the
migration still only changes the arbiter runtime protocol and never mutates
business lease events.

Admission requires the coordinator's live publication transaction for the
authorized task at ``CLEANUP_PRE`` with candidate = HEAD = main = origin/main,
committed code identity for this entry point and its helpers, the same Git
common directory, no active lease in the target store, the exact legacy owner
bytes, and a quiescence receipt bound to all of the above.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ai_trading_system.platform.architecture import (  # noqa: E402
    checkout_guard,
    lease_arbiter,
)
from ai_trading_system.platform.architecture.checkout_guard import (  # noqa: E402
    CheckoutGuardError,
    CheckoutLeaseGuard,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (  # noqa: E402
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.parallel_control import (  # noqa: E402
    ParallelControlError,
)
from ai_trading_system.platform.artifacts.json_contract import (  # noqa: E402
    load_strict_json_text,
)
from ai_trading_system.yaml_loader import safe_load_yaml_text  # noqa: E402

AUTHORIZATIONS_PATH = (
    PROJECT_ROOT / "config/architecture/lease_arbiter_sibling_migration_authorizations.yaml"
)
AUTHORIZATIONS_SCHEMA = "lease_arbiter_sibling_migration_authorizations.v1"
ACTOR = "integration-coordinator"
RUNTIME_PARENT = "outputs/validation_runtime/lease-arbiter-sibling-migration"
CODE_PATHS = (
    "scripts/architecture_arch005_sibling_lease_store_migration.py",
    "src/ai_trading_system/platform/architecture/lease_arbiter.py",
    "src/ai_trading_system/platform/architecture/checkout_guard.py",
    "config/architecture/lease_arbiter_sibling_migration_authorizations.yaml",
)
_AUTHORIZATION_KEYS = {
    "authorization_id",
    "task_id",
    "owner_instruction_ref",
    "target_checkout_root",
    "expected_legacy_owner_sha256",
    "rationale",
    "review_or_expiry",
}
_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,95}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
# Engineering resource bound for the small receipt JSON, not a model rule.
MAX_RECEIPT_BYTES = 65536


class SiblingMigrationError(ValueError):
    """Typed admission refusal; the message is the reason code plus detail."""


def _fail(code: str, detail: str) -> SiblingMigrationError:
    return SiblingMigrationError(f"SIBLING_MIGRATION_{code}: {detail}")


@dataclass(frozen=True)
class Authorization:
    authorization_id: str
    task_id: str
    owner_instruction_ref: str
    target_checkout_root: Path
    expected_legacy_owner_sha256: str


def load_authorization(path: Path, authorization_id: str) -> Authorization:
    document = safe_load_yaml_text(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema_version") != (AUTHORIZATIONS_SCHEMA):
        raise _fail("AUTHORIZATIONS_INVALID", "unsupported authorization schema")
    rows = document.get("authorizations")
    if not isinstance(rows, list) or not rows:
        raise _fail("AUTHORIZATIONS_INVALID", "authorization list required")
    seen: set[str] = set()
    match: Authorization | None = None
    for row in rows:
        if not isinstance(row, dict) or set(row) != _AUTHORIZATION_KEYS:
            raise _fail("AUTHORIZATIONS_INVALID", "authorization row keys mismatch")
        identifier = str(row["authorization_id"])
        if _ID.fullmatch(identifier) is None or identifier in seen:
            raise _fail("AUTHORIZATIONS_INVALID", f"invalid or duplicate id {identifier}")
        seen.add(identifier)
        root = Path(str(row["target_checkout_root"]))
        if not root.is_absolute():
            raise _fail("AUTHORIZATIONS_INVALID", "target root must be absolute")
        if _SHA256.fullmatch(str(row["expected_legacy_owner_sha256"])) is None:
            raise _fail("AUTHORIZATIONS_INVALID", "legacy owner SHA-256 required")
        for key in ("task_id", "owner_instruction_ref", "rationale", "review_or_expiry"):
            if not isinstance(row[key], str) or not row[key].strip():
                raise _fail("AUTHORIZATIONS_INVALID", f"{key} must be non-empty text")
        if identifier == authorization_id:
            match = Authorization(
                authorization_id=identifier,
                task_id=row["task_id"],
                owner_instruction_ref=row["owner_instruction_ref"],
                target_checkout_root=root,
                expected_legacy_owner_sha256=row["expected_legacy_owner_sha256"],
            )
    if match is None:
        raise _fail("AUTHORIZATION_UNKNOWN", authorization_id)
    return match


def _git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "--no-pager", "--no-optional-locks", "-C", str(root), *args],
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode("utf-8").strip()


def _git_bytes(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "--no-pager", "--no-optional-locks", "-C", str(root), *args],
        capture_output=True,
        check=True,
    ).stdout


def check_published_identity(values: Mapping[str, str], candidate: str | None) -> None:
    """Require the coordinator to sit on main at the published candidate."""
    if values.get("branch") != "main":
        raise _fail("COORDINATOR_NOT_MAIN", str(values.get("branch")))
    if not candidate or any(
        values.get(key) != candidate for key in ("head", "main", "origin_main")
    ):
        raise _fail("CANDIDATE_NOT_PUBLISHED", json.dumps(dict(values), sort_keys=True))


def check_target_root(
    authorization: Authorization,
    *,
    target_root: Path,
    coordinator_root: Path,
    target_common: Path,
    coordinator_common: Path,
) -> None:
    if target_root.resolve() != authorization.target_checkout_root.resolve():
        raise _fail("TARGET_ROOT_NOT_AUTHORIZED", target_root.as_posix())
    if target_root.resolve() == coordinator_root.resolve():
        raise _fail("TARGET_IS_COORDINATOR", target_root.as_posix())
    if target_common.resolve() != coordinator_common.resolve():
        raise _fail("TARGET_NOT_SAME_REPOSITORY", target_common.as_posix())


def check_target_leases(active_lease_ids: list[str], status: str) -> None:
    if status != "PASS":
        raise _fail("TARGET_LEASE_REPLAY_INVALID", status)
    if active_lease_ids:
        raise _fail("TARGET_HAS_ACTIVE_LEASE", ",".join(sorted(active_lease_ids)))


def check_quiescence(
    receipt: Mapping[str, Any],
    *,
    store_root: Path,
    authorization: Authorization,
    binding: Mapping[str, Any],
) -> None:
    expected = {
        "store_root": store_root.as_posix(),
        "actor": ACTOR,
        "owner_instruction_ref": authorization.owner_instruction_ref,
        "publication_transaction_id": binding["transaction_id"],
        "publication_transaction_sha256": binding["transaction_sha256"],
        "observed_owner_sha256": authorization.expected_legacy_owner_sha256,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise _fail("QUIESCENCE_BINDING_MISMATCH", key)


def _code_identity(candidate: str) -> dict[str, str]:
    loaded = {
        "scripts/architecture_arch005_sibling_lease_store_migration.py": Path(__file__),
        "src/ai_trading_system/platform/architecture/lease_arbiter.py": Path(
            lease_arbiter.__file__
        ),
        "src/ai_trading_system/platform/architecture/checkout_guard.py": Path(
            checkout_guard.__file__
        ),
        "config/architecture/lease_arbiter_sibling_migration_authorizations.yaml": (
            AUTHORIZATIONS_PATH
        ),
    }
    observed: dict[str, str] = {}
    for relative in CODE_PATHS:
        target = PROJECT_ROOT / relative
        if loaded[relative].resolve() != target.resolve():
            raise _fail("CODE_FROM_OTHER_CHECKOUT", relative)
        raw = target.read_bytes()
        if raw.replace(b"\r\n", b"\n") != _git_bytes(
            PROJECT_ROOT, "cat-file", "blob", f"{candidate}:{relative}"
        ):
            raise _fail("CODE_NOT_PUBLISHED_CANDIDATE", relative)
        observed[relative] = hashlib.sha256(raw).hexdigest()
    return observed


def _receipt(path: Path) -> tuple[dict[str, Any], bytes]:
    checked = path.absolute()
    checked.relative_to(PROJECT_ROOT / "outputs/validation_runtime")
    info = checked.lstat()
    if checked.suffix != ".json" or not stat.S_ISREG(info.st_mode):
        raise _fail("QUIESCENCE_LOCATOR_INVALID", checked.as_posix())
    if info.st_size > MAX_RECEIPT_BYTES:
        raise _fail("QUIESCENCE_LOCATOR_INVALID", "receipt exceeds byte budget")
    raw = checked.read_bytes()
    value = load_strict_json_text(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise _fail("QUIESCENCE_LOCATOR_INVALID", "JSON object required")
    return value, raw


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("xb") as stream:
        stream.write(
            (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        )


def _admit(args: argparse.Namespace) -> tuple[Path, Authorization, dict[str, Any]]:
    if _ID.fullmatch(args.migration_id) is None:
        raise _fail("MIGRATION_ID_INVALID", args.migration_id)
    authorization = load_authorization(AUTHORIZATIONS_PATH, args.authorization_id)
    if args.owner_instruction_ref != authorization.owner_instruction_ref:
        raise _fail("OWNER_REF_MISMATCH", args.owner_instruction_ref)
    transaction = args.publication_transaction.absolute()
    transaction.relative_to(
        PROJECT_ROOT / "outputs/architecture/arch_005_integration_publication_fence/transactions"
    )
    fence = IntegrationPublicationFence(project_root=PROJECT_ROOT)
    binding = fence.validate(
        transaction,
        task_id=authorization.task_id,
        exact_phase="CLEANUP_PRE",
        require_candidate=True,
    )
    replay = fence.replay(transaction)
    if replay.transaction["actor"] != ACTOR:
        raise _fail("ACTOR_MISMATCH", str(replay.transaction["actor"]))
    identity = {
        "branch": _git(PROJECT_ROOT, "branch", "--show-current"),
        "head": _git(PROJECT_ROOT, "rev-parse", "HEAD"),
        "main": _git(PROJECT_ROOT, "rev-parse", "refs/heads/main"),
        "origin_main": _git(PROJECT_ROOT, "rev-parse", "refs/remotes/origin/main"),
    }
    check_published_identity(identity, replay.candidate_sha)
    codes = _code_identity(str(replay.candidate_sha))
    coordinator_leases = fence.guard.replay()
    if coordinator_leases.status != "PASS" or {
        lease.lease_id for lease in coordinator_leases.active_leases
    } != {binding["lease_id"]}:
        raise _fail("COORDINATOR_LEASES_UNEXPECTED", "only the bound lease may be active")
    target_root = Path(args.target_checkout_root)
    check_target_root(
        authorization,
        target_root=target_root,
        coordinator_root=PROJECT_ROOT,
        target_common=Path(
            _git(target_root, "rev-parse", "--path-format=absolute", "--git-common-dir")
        ),
        coordinator_common=Path(
            _git(PROJECT_ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir")
        ),
    )
    # Coordinator policies, target runtime root: replay only reads event files.
    target_guard = CheckoutLeaseGuard(project_root=target_root)
    target_replay = target_guard.replay()
    check_target_leases(
        [lease.lease_id for lease in target_replay.active_leases], target_replay.status
    )
    store = target_guard.store.root
    legacy_owner = store / "arbiter.lock" / "owner.json"
    owner_sha = hashlib.sha256(legacy_owner.read_bytes()).hexdigest()
    if owner_sha != authorization.expected_legacy_owner_sha256:
        raise _fail("LEGACY_OWNER_MISMATCH", owner_sha)
    receipt, raw = _receipt(args.quiescence_receipt)
    if hashlib.sha256(raw).hexdigest() != args.quiescence_receipt_sha256:
        raise _fail("QUIESCENCE_SHA_MISMATCH", args.quiescence_receipt_sha256)
    check_quiescence(receipt, store_root=store, authorization=authorization, binding=binding)
    return (
        store,
        authorization,
        {
            "schema_version": "lease_arbiter_sibling_migration_admission.v1",
            "migration_id": args.migration_id,
            "authorization_id": authorization.authorization_id,
            "authorization_state": "EXACT_PREAUTHORIZED",
            "owner_instruction_ref": authorization.owner_instruction_ref,
            "publication": binding,
            "coordinator_identity": identity,
            "target_checkout_root": target_root.as_posix(),
            "store_root": store.as_posix(),
            "target_lease_replay": {
                "status": target_replay.status,
                "lease_head_count": len(target_replay.lease_heads),
                "active_lease_count": len(target_replay.active_leases),
            },
            "published_code_sha256": codes,
            "quiescence_receipt_sha256": hashlib.sha256(raw).hexdigest(),
            "admitted_at": datetime.now(UTC).isoformat(),
            "production_effect": "none",
            "broker_action": "none",
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GOV-007 已授权 sibling checkout 的旧 arbiter 迁移"
    )
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--target-checkout-root", required=True)
    parser.add_argument("--publication-transaction", required=True, type=Path)
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--owner-instruction-ref", required=True)
    parser.add_argument("--quiescence-receipt", required=True, type=Path)
    parser.add_argument("--quiescence-receipt-sha256", required=True)
    args = parser.parse_args()
    run: Path | None = None
    try:
        store, authorization, admission = _admit(args)
        parent = PROJECT_ROOT / RUNTIME_PARENT
        parent.mkdir(parents=True, exist_ok=True)
        run = parent / args.migration_id
        run.mkdir(exist_ok=False)
        _write_new(run / "admission.json", admission)
        result = lease_arbiter.migrate_legacy_arbiter(
            store,
            migration_id=args.migration_id,
            actor=ACTOR,
            now=datetime.now(UTC),
            expected_owner_sha256=authorization.expected_legacy_owner_sha256,
            owner_instruction_ref=authorization.owner_instruction_ref,
            quiescence_receipt_path=args.quiescence_receipt.absolute(),
            quiescence_receipt_sha256=args.quiescence_receipt_sha256,
        )
        after = CheckoutLeaseGuard(project_root=Path(args.target_checkout_root)).replay()
        before = admission["target_lease_replay"]
        if after.status != "PASS" or len(after.lease_heads) != before["lease_head_count"]:
            raise _fail("POST_MIGRATION_REPLAY_DRIFT", after.status)
        _write_new(run / "result.json", result)
    except (
        SiblingMigrationError,
        ParallelControlError,
        PublicationFenceError,
        CheckoutGuardError,
        subprocess.CalledProcessError,
        OSError,
        ValueError,
    ) as exc:
        failure = {
            "schema_version": "lease_arbiter_sibling_migration_command_result.v1",
            "status": "BLOCKED",
            "reason_code": getattr(exc, "code", type(exc).__name__),
            "detail": str(exc),
            "production_effect": "none",
            "broker_action": "none",
        }
        if run is not None:
            _write_new(run / "failure.json", failure)
        print(json.dumps(failure, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
