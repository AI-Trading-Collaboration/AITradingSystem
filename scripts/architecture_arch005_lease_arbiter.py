"""DEVX-014: explicitly migrate the existing arbiter after coordinator quiescence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ai_trading_system.platform.architecture import (  # noqa: E402
    lease_arbiter,
    parallel_control_kernel,
    source_preservation,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (  # noqa: E402
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.parallel_control import (  # noqa: E402
    ParallelControlError,
)
from ai_trading_system.platform.architecture.source_preservation import (  # noqa: E402
    SourcePreservation,
    SourcePreservationError,
)
from ai_trading_system.platform.artifacts.json_contract import (  # noqa: E402
    load_strict_json_text,
)

TASK_ID = "DEVX-014_DIRTY_SOURCE_PRESERVATION_RECOVERY_V1"
OWNER_REF = "owner_instruction:DEVX-014:2026-09-06:arbiter-safety-fix"
ACTOR = "integration-coordinator"
CODE_PATHS = (
    "scripts/architecture_arch005_lease_arbiter.py",
    "src/ai_trading_system/platform/architecture/lease_arbiter.py",
    "src/ai_trading_system/platform/architecture/parallel_control_kernel.py",
    "src/ai_trading_system/platform/architecture/source_preservation.py",
)
# Engineering admission JSON only; this is a resource bound, not a model rule.
MAX_ADMISSION_BYTES = 262144


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_path(path: Path, base: Path, *, must_exist: bool = True) -> Path:
    absolute = path.absolute()
    absolute.relative_to(base)
    for item in (absolute, *absolute.parents):
        if not item.exists() and not item.is_symlink():
            continue
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or (
            getattr(info, "st_file_attributes", 0)
            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        ):
            raise ValueError("迁移 admission 路径不允许 symlink/reparse")
    absolute.resolve(strict=must_exist).relative_to(base)
    return absolute


def _runtime_json(path: Path) -> tuple[dict[str, Any], bytes]:
    checked = _safe_path(path, PROJECT_ROOT / "outputs/validation_runtime")
    if checked.suffix != ".json" or not stat.S_ISREG(checked.stat().st_mode):
        raise ValueError("admission 输入必须为 trusted runtime 的普通 JSON")
    if checked.stat().st_size > MAX_ADMISSION_BYTES:
        raise ValueError("admission 输入超过工程字节预算")
    raw = checked.read_bytes()
    if len(raw) > MAX_ADMISSION_BYTES:
        raise ValueError("admission 输入在读取期间超过工程字节预算")
    value = load_strict_json_text(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("admission 输入必须为 JSON object")
    return value, raw


def _write_new(path: Path, payload: dict[str, Any]) -> None:
    _safe_path(path, PROJECT_ROOT / "outputs/validation_runtime", must_exist=False)
    with path.open("xb") as stream:
        stream.write(
            (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        )


def _code_admission(path: Path) -> dict[str, str]:
    manifest, _ = _runtime_json(path)
    if set(manifest) != {"schema_version", "files"} or manifest["schema_version"] != (
        "lease_arbiter_reviewed_code.v1"
    ):
        raise ValueError("reviewed code manifest schema 不符")
    expected = manifest["files"]
    if not isinstance(expected, dict) or set(expected) != set(CODE_PATHS):
        raise ValueError("reviewed code manifest 必须精确绑定四项新增或修改实现")
    origins = (
        Path(__file__),
        Path(lease_arbiter.__file__),
        Path(parallel_control_kernel.__file__),
        Path(source_preservation.__file__),
    )
    observed = {}
    for relative, origin in zip(CODE_PATHS, origins, strict=True):
        target = _safe_path(PROJECT_ROOT / relative, PROJECT_ROOT)
        if origin.resolve() != target.resolve():
            raise ValueError("实际 loaded code 来自其它 checkout")
        digest = _sha(target.read_bytes())
        if digest != expected[relative]:
            raise ValueError("实际实现与 reviewed SHA 不同")
        observed[relative] = digest
    return observed


def _check_workspace_identity(
    identity: dict[str, Any],
    *,
    phase: str,
    current_head: str,
    current_branch: str,
    current_common: Path,
) -> None:
    # The existing publication protocol switches this same checkout to main for
    # the ordinary push. Never accept an arbitrary branch after that handoff.
    if phase not in {"TASK_SOURCE_PRE_WRITE", "CLEANUP_PRE"}:
        raise ValueError("迁移不接受其它 publication phase")
    expected_branch = identity["branch_name"] if phase == "TASK_SOURCE_PRE_WRITE" else "main"
    if (
        Path(identity["checkout_root"]).resolve() != PROJECT_ROOT
        or Path(identity["git_common_dir"]).resolve() != current_common
        or expected_branch != current_branch
        or phase == "TASK_SOURCE_PRE_WRITE"
        and identity["head_commit"] != current_head
    ):
        raise ValueError("当前 checkout identity 与活动事务阶段不一致")


def _admit(args: argparse.Namespace) -> tuple[Path, dict[str, Any]]:
    if args.owner_instruction_ref != OWNER_REF:
        raise ValueError("本迁移仅接受 DEVX-014 S1a 已批准的精确工程范围")
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", args.migration_id) is None:
        raise ValueError("migration id 不合法")
    codes = _code_admission(args.reviewed_code)
    preservation = SourcePreservation(PROJECT_ROOT)
    request: dict[str, Any] | None = None
    if args.source_request is None:
        environment_root = PROJECT_ROOT
        environment_binding = preservation._environment(environment_root)
    else:
        raw_request, _ = _runtime_json(args.source_request)
        request = preservation._request(raw_request)
        target_root = Path(request["source_root"]).resolve(strict=True)
        environment_root = target_root
        environment_binding = preservation._environment(environment_root)
        # Sharing the common directory never substitutes for separately checking
        # each checkout's effective worktree configuration and its exact bytes.
        commons = [
            Path(
                preservation._git(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
                .decode()
                .strip()
            ).resolve()
            for root in (PROJECT_ROOT, target_root)
        ]
        if commons[0] != commons[1]:
            raise ValueError("迁移源不是同一仓库的原任务工作区")
    transaction = _safe_path(
        args.publication_transaction,
        PROJECT_ROOT / "outputs/architecture/arch_005_integration_publication_fence/transactions",
    )
    preservation._recheck_environment(environment_root, environment_binding)
    fence = IntegrationPublicationFence(project_root=PROJECT_ROOT)
    binding = fence.validate(
        transaction,
        task_id=TASK_ID,
        exact_phase="TASK_SOURCE_PRE_WRITE" if request is None else "CLEANUP_PRE",
        require_candidate=request is not None,
    )
    replay = fence.replay(transaction)
    if replay.transaction["actor"] != ACTOR:
        raise ValueError("必须由当前 integration coordinator 显式迁移")
    declared = set(replay.transaction["owned_paths"]) | set(replay.transaction["shared_paths"])
    if not set(CODE_PATHS).issubset(declared):
        raise ValueError("当前事务未精确声明内核、锁实现及迁移入口")
    identity = replay.transaction["workspace_identity"]
    current_head = preservation._git(PROJECT_ROOT, "rev-parse", "HEAD").decode().strip()
    current_branch = (
        preservation._git(PROJECT_ROOT, "symbolic-ref", "--short", "HEAD").decode().strip()
    )
    current_common = Path(
        preservation._git(PROJECT_ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir")
        .decode()
        .strip()
    ).resolve()
    _check_workspace_identity(
        identity,
        phase=str(binding["phase"]),
        current_head=current_head,
        current_branch=current_branch,
        current_common=current_common,
    )
    source: dict[str, Any] | None = None
    store = fence.guard.store.root
    if args.source_request is not None:
        if not replay.candidate_sha or any(
            preservation._git(PROJECT_ROOT, "rev-parse", reference).decode().strip()
            != replay.candidate_sha
            for reference in ("HEAD", "refs/heads/main", "refs/remotes/origin/main")
        ):
            raise ValueError("必须满足已发布 candidate = HEAD = main = origin/main")
        for relative in CODE_PATHS:
            if (PROJECT_ROOT / relative).read_bytes().replace(b"\r\n", b"\n") != preservation._git(
                PROJECT_ROOT, "cat-file", "blob", f"{replay.candidate_sha}:{relative}"
            ):
                raise ValueError("迁移入口不是已发布候选的 exact code")
        if request is None:
            raise ValueError("缺少已验证 source request")
        source = preservation.inspect_migration_source(request)
        source_common = Path(request["source_common_git_dir"]).resolve(strict=True)
        authority_common = Path(str(replay.transaction["workspace_identity"]["git_common_dir"]))
        if source_common != authority_common.resolve(strict=True):
            raise ValueError("迁移源不是同一仓库的原任务工作区")
        store = Path(source["store_root"])
    quiescence, raw = _runtime_json(args.quiescence_receipt)
    if _sha(raw) != args.quiescence_receipt_sha256:
        raise ValueError("quiescence receipt SHA 不符")
    for key, value in {
        "store_root": store.as_posix(),
        "actor": ACTOR,
        "owner_instruction_ref": OWNER_REF,
        "publication_transaction_id": binding["transaction_id"],
        "publication_transaction_sha256": binding["transaction_sha256"],
        "observed_owner_sha256": args.expected_owner_sha256,
    }.items():
        if quiescence.get(key) != value:
            raise ValueError(f"quiescence 与当前 authority 不符：{key}")
    lease_replay = fence.guard.replay()
    if lease_replay.status != "PASS" or {
        lease.lease_id for lease in lease_replay.active_leases
    } != {binding["lease_id"]}:
        raise ValueError("authority checkout 存在其它活动租约或事件链无效")
    preservation._recheck_environment(environment_root, environment_binding)
    return store, {
        "schema_version": "lease_arbiter_migration_admission.v1",
        "migration_id": args.migration_id,
        "authorization_state": "EXACT_PREAUTHORIZED",
        "owner_instruction_ref": OWNER_REF,
        "publication": binding,
        "store_root": store.as_posix(),
        "reviewed_working_code_sha256": codes,
        "implementation_profile": (
            "COMMITTED_PUBLISHED_SOURCE_GIT_EOL_LF"
            if source is not None
            else "REVIEWED_WORKING_SOURCE_ENGINEERING_ONLY"
        ),
        "source_admission": source,
        "git_configuration": environment_binding,
        "quiescence_receipt_sha256": _sha(raw),
        "admitted_at": datetime.now(UTC).isoformat(),
        "production_effect": "none",
        "broker_action": "none",
    }


def _recheck_admission_environment(admission: dict[str, Any]) -> None:
    source = admission["source_admission"]
    root = Path(source["source_root"]) if source is not None else PROJECT_ROOT
    SourcePreservation(PROJECT_ROOT)._recheck_environment(root, admission["git_configuration"])


def main() -> int:
    parser = argparse.ArgumentParser(description="DEVX-014 显式排空旧调用后迁移唯一 lease arbiter")
    parser.add_argument("--publication-transaction", required=True, type=Path)
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--owner-instruction-ref", required=True)
    parser.add_argument("--expected-owner-sha256", required=True)
    parser.add_argument("--quiescence-receipt", required=True, type=Path)
    parser.add_argument("--quiescence-receipt-sha256", required=True)
    parser.add_argument("--reviewed-code", required=True, type=Path)
    parser.add_argument("--source-request", type=Path)
    args = parser.parse_args()
    run: Path | None = None
    try:
        store, admission = _admit(args)
        parent = _safe_path(
            PROJECT_ROOT / "outputs/validation_runtime/lease-arbiter-migration-admission",
            PROJECT_ROOT / "outputs/validation_runtime",
            must_exist=False,
        )
        _recheck_admission_environment(admission)
        parent.mkdir(exist_ok=True)
        destination = parent / args.migration_id
        destination.mkdir(exist_ok=False)
        run = destination
        _write_new(run / "admission.json", admission)
        _recheck_admission_environment(admission)
        result = lease_arbiter.migrate_legacy_arbiter(
            store,
            migration_id=args.migration_id,
            actor=ACTOR,
            now=datetime.now(UTC),
            expected_owner_sha256=args.expected_owner_sha256,
            owner_instruction_ref=OWNER_REF,
            quiescence_receipt_path=args.quiescence_receipt.absolute(),
            quiescence_receipt_sha256=args.quiescence_receipt_sha256,
        )
        _recheck_admission_environment(admission)
        _write_new(run / "result.json", result)
    except (
        ParallelControlError,
        PublicationFenceError,
        SourcePreservationError,
        OSError,
        ValueError,
    ) as exc:
        result = {
            "schema_version": "lease_arbiter_migration_command_result.v1",
            "status": "BLOCKED",
            "reason_code": getattr(exc, "code", "LEASE_ARBITER_MIGRATION_COMMAND_FAILED"),
            "detail": str(exc),
            "production_effect": "none",
            "broker_action": "none",
        }
        if run is not None:
            _write_new(run / "failure.json", result)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
