"""DEVX-015 task-only source checkpoints, never validation/publication authority.

Reuse the DEVX-014 safe Git context and the existing S4D lease kernel. A released
task mutation intent establishes scope without requiring a failed publication
transaction. Source HEAD/index/config/bytes stay fixed; main/remote observations
are deliberately separate. No user's branch, index or source file is modified.
"""

from __future__ import annotations

import functools
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, TypeVar, cast
from uuid import uuid4

from ai_trading_system.platform.architecture import source_preservation as safe
from ai_trading_system.platform.architecture import (
    workflow_contract,
    workflow_coordination,
    workflow_execution,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CHECKOUT_SOURCE_ONLY_PROFILE,
    CheckoutGuardError,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
    parse_checkout_operation_intent,
)
from ai_trading_system.platform.architecture.integration_publication_fence import (
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    ParallelControlError,
    parse_lease_event,
    replay_lease_events,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_fragment,
)
from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text
from ai_trading_system.yaml_loader import safe_load_yaml_text

MODULE_PATH = "src/ai_trading_system/platform/architecture/task_checkpoint.py"
CLI_PATH = "scripts/architecture_arch005_task_checkpoint.py"
POLICY_PATH = "config/architecture/arch_005_task_checkpoint.yaml"
_EXECUTION_MODULES = (
    workflow_coordination.__name__,
    workflow_execution.__name__,
    workflow_contract.__name__,
)
_EXECUTION_PATHS = tuple("src/" + name.replace(".", "/") + ".py" for name in _EXECUTION_MODULES)
DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[4] / POLICY_PATH
RUNTIME = "outputs/architecture/arch_005_task_checkpoints"
REF_PREFIX = "refs/aits/task-checkpoints/"
PROFILE = "RAW_BYTES_TASK_CHECKPOINT_UNVALIDATED"
# Bounded metadata reads: separate from the reviewed aggregate source-byte cap.
_MAX_METADATA_BYTES = 16 * 1024 * 1024
_PHASES = ("ACQUIRED", "CAPTURED", "OBJECTS_WRITTEN", "REF_CREATED", "VERIFIED", "RELEASED")
_SCOPE_KEYS = {
    "schema_version",
    "checkpoint_id",
    "task_id",
    "actor",
    "thread_id",
    "source_root",
    "source_common_git_dir",
    "source_branch",
    "source_head_sha",
    "frozen_base_sha",
    "scope_intent",
    "paths",
}
_REQUEST_KEYS = (_SCOPE_KEYS - {"paths"}) | {
    "files",
    "source_state",
    "implementation",
    "implementation_sha256",
    "observed_main_sha",
    "observed_origin_main_sha",
}
_POLICY_KEYS = {
    "schema_version",
    "policy_id",
    "version",
    "status",
    "owner_instruction_ref",
    "runtime_root",
    "ref_prefix",
    "max_files",
    "max_total_bytes",
    "source_roots",
}
SAFETY = {
    "profile": PROFILE,
    "inspection_profile": "SOURCE_ONLY_EXPLICIT_PATHS",
    "source_mutation_allowed": False,
    "unscoped_worktree_status": "NOT_INSPECTED",
    "clean_integration_status": "NOT_EVALUATED",
    "task_source_write_allowed": False,
    "generator_allowed": False,
    "formal_validation_allowed": False,
    "full_allowed": False,
    "main_ff_allowed": False,
    "push_allowed": False,
    "research_allowed": False,
    "data_action_allowed": False,
    "trading_allowed": False,
    "production_effect": "none",
    "broker_action": "none",
}


class TaskCheckpointError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise TaskCheckpointError("TASK_CHECKPOINT_" + code, message)


_F = TypeVar("_F", bound=Callable[..., Any])


def _public(function: _F) -> _F:
    @functools.wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except TaskCheckpointError:
            raise
        except safe.SourcePreservationError as exc:
            _fail(exc.code.removeprefix("SOURCE_PRESERVATION_"), exc.message)
        except (CheckoutGuardError, PublicationFenceError) as exc:
            _fail("LEASE", exc.code)
        except (ParallelControlError, workflow_execution.ExecutionContainmentError) as exc:
            _fail("EXECUTION", exc.code)
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            # No arbitrary source, Git output or credential-bearing config text.
            _fail("INVALID", type(exc).__name__)

    return cast(_F, wrapped)


def _bytes(path: Path, limit: int = _MAX_METADATA_BYTES) -> bytes:
    safe._configuration_path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        _fail("PATH", "single-link regular file required")
    if info.st_size > limit:
        _fail("BUDGET", "bounded evidence read exceeds metadata budget")
    try:
        return workflow_contract.bounded_regular_bytes(
            path, budget=limit, expected_identity=(info.st_dev, info.st_ino)
        )
    except workflow_contract.WorkflowContractError as exc:
        if exc.code == "WORKFLOW_ARTIFACT_BUDGET":
            _fail("BUDGET", "bounded evidence read exceeds metadata budget")
        if exc.code in {"WORKFLOW_REPARSE_PATH", "WORKFLOW_NOT_REGULAR_FILE"}:
            _fail("PATH", "single-link regular file required")
        _fail("DRIFT", "frozen file identity changed before content access")


def _json(path: Path) -> dict[str, Any]:
    return safe._object(load_strict_json_text(_bytes(path).decode("utf-8")))


def _policy(content: bytes) -> dict[str, Any]:
    value = safe._object(safe_load_yaml_text(content.decode("utf-8")), _POLICY_KEYS)
    if (
        value["schema_version"] != "task_checkpoint_policy.v1"
        or value["policy_id"] != "ARCH-005-TASK-CHECKPOINT"
        or value["status"] != "OWNER_APPROVED_ENGINEERING_IMPLEMENTATION"
        or value["runtime_root"] != RUNTIME
        or value["ref_prefix"] != REF_PREFIX
    ):
        _fail("POLICY", "unsupported checkpoint policy")
    for key in ("version", "owner_instruction_ref"):
        safe._text(value[key])
    for key in ("max_files", "max_total_bytes"):
        if type(value[key]) is not int or value[key] <= 0:
            _fail("POLICY", "positive engineering budget required")
    roots = value["source_roots"]
    if not isinstance(roots, list) or not roots:
        _fail("POLICY", "explicit engineering source roots required")
    for root in roots:
        safe._relative(root)
        if root.casefold().split("/")[0] in {".git", "outputs", "data", ".env"}:
            _fail("POLICY", "runtime/data/private roots cannot be source authority")
    if len({root.casefold() for root in roots}) != len(roots):
        _fail("POLICY", "duplicate source roots")
    return value


def _source_path(value: object, policy: Mapping[str, Any], exclusions: list[str]) -> str:
    path = safe._relative(value)
    parts = PurePosixPath(path).parts
    name = parts[-1].casefold()
    private = {".git", ".ssh", ".aws", ".azure", ".gnupg"}
    if (
        any(part.casefold() in private for part in parts)
        or name in {".env", "credentials", "credentials.json", "secrets.yaml", "secrets.yml"}
        or name.startswith((".env.", "id_rsa", "id_ed25519"))
        or name.endswith((".pem", ".key", ".p12", ".pfx"))
        or any(safe._under(path, excluded) for excluded in exclusions)
        or not any(safe._under(path, root) for root in policy["source_roots"])
    ):
        _fail("SCOPE", "excluded, private or non-engineering path is not capturable")
    return path


class TaskCheckpoint:
    @_public
    def __init__(self, project_root: Path, policy_path: Path | None = None) -> None:
        # Match the repository's documented/CI runtime before any lease action.
        if sys.version_info[:2] != (3, 11):
            _fail("ENVIRONMENT", "use the repository's reviewed Python 3.11 environment")
        self.project_root = safe._configuration_path(project_root).resolve(strict=True)
        self.policy_path = policy_path or self.project_root / POLICY_PATH
        if not self.policy_path.is_absolute():
            self.policy_path = self.project_root / self.policy_path
        self.policy_bytes = _bytes(self.policy_path)
        self.policy = _policy(self.policy_bytes)
        self.io = safe.SourcePreservation(self.project_root, self.project_root / safe.POLICY_PATH)
        self._bound_implementation: dict[str, Any] | None = None

    def _loaded_origins(self) -> set[str]:
        """Attest normal loaded module locations, never a caller-supplied file set."""
        if (
            Path(__file__).resolve() != self.project_root / MODULE_PATH
            or self.policy_path.resolve() != self.project_root / POLICY_PATH
        ):
            _fail("IDENTITY", "checkpoint implementation/policy is outside trusted checkout")
        paths = {safe.MODULE_PATH, safe.CLI_PATH, safe.POLICY_PATH}
        for name in safe._IMPLEMENTATION_MODULES:
            origin = getattr(sys.modules.get(name), "__file__", None)
            if not isinstance(origin, str):
                _fail("IDENTITY", "required project helper has no physical source origin")
            path = safe._configuration_path(Path(origin))
            stem = self.project_root / "src" / Path(*name.split("."))
            if path not in {stem.with_suffix(".py"), stem / "__init__.py"}:
                _fail("IDENTITY", "loaded project helper belongs to another checkout")
            paths.add(path.relative_to(self.project_root).as_posix())
        return paths

    def _blob_contents(
        self, root: Path, references: list[str], *, limit: int = _MAX_METADATA_BYTES
    ) -> dict[str, bytes]:
        """Two bounded batch calls; sizes and types precede any blob content read."""
        if not references:
            return {}
        if any(not reference or any(c in reference for c in "\r\n\0") for reference in references):
            _fail("IDENTITY", "invalid batch object locator")
        query = ("\n".join(references) + "\n").encode("utf-8")
        headers = self.io._git(root, "cat-file", "--batch-check", content=query).splitlines()
        if len(headers) != len(references):
            _fail("IDENTITY", "historical blob inventory is incomplete")
        total, sizes = 0, []
        for header in headers:
            fields = header.split()
            if len(fields) != 3 or fields[1] != b"blob":
                _fail("IDENTITY", "historical implementation/source blob missing")
            safe._digest(fields[0].decode("ascii"), 40)
            size = int(fields[2])
            if size < 0:
                _fail("IDENTITY", "invalid blob size")
            total += size
            sizes.append(size)
        if total > limit:
            _fail("BUDGET", "historical blob batch exceeds bounded byte budget")
        packed = self.io._git(root, "cat-file", "--batch", content=query)
        position, result = 0, {}
        for reference, header, size in zip(references, headers, sizes, strict=True):
            end = packed.find(b"\n", position)
            if end < 0 or packed[position:end] != header:
                _fail("IDENTITY", "historical blob batch header mismatch")
            position = end + 1
            data = packed[position : position + size]
            position += size
            if len(data) != size or packed[position : position + 1] != b"\n":
                _fail("IDENTITY", "historical blob batch is truncated")
            position += 1
            result[reference] = data
        if position != len(packed):
            _fail("IDENTITY", "historical blob batch contains extra bytes")
        return result

    def _historical_helper_paths(self, commit: str) -> set[str]:
        candidates: dict[str, tuple[str, str]] = {}
        for name in safe._IMPLEMENTATION_MODULES:
            stem = "src/" + name.replace(".", "/")
            candidates[name] = (stem + ".py", stem + "/__init__.py")
        entries = self._entries(
            self.project_root, commit, [path for pair in candidates.values() for path in pair]
        )
        paths = {safe.MODULE_PATH, safe.CLI_PATH, safe.POLICY_PATH}
        for pair in candidates.values():
            present = [path for path in pair if path in entries]
            if len(present) != 1:
                _fail("IDENTITY", "required historical helper has no unique canonical file")
            paths.add(present[0])
        return paths

    def _verify_implementation(self, binding: Mapping[str, Any], *, current: bool = False) -> None:
        """Independently prove schema, required files and real historical Git blobs.

        Historical validation never trusts a self-consistent receipt hash as code
        provenance, and never reads the old implementation checkout's live files.
        Current capture additionally rechecks loaded locations and exact disk bytes.
        """
        value = safe._object(
            binding,
            {"schema_version", "commit", "project_root", "files", "safe_git_implementation"},
        )
        if value["schema_version"] not in {
            "task_checkpoint_implementation.v1",
            "task_checkpoint_implementation.v2",
        }:
            _fail("IDENTITY", "unsupported implementation identity schema")
        execution_bound = value["schema_version"] == "task_checkpoint_implementation.v2"
        if current and not execution_bound:
            _fail("IDENTITY", "historical implementation cannot authorize current execution")
        head = safe._digest(value["commit"], 40)
        if not Path(safe._text(value["project_root"])).is_absolute():
            _fail("IDENTITY", "absolute historical implementation root required")
        if self.io._git(self.project_root, "cat-file", "-t", head).strip() != b"commit":
            _fail("IDENTITY", "implementation must identify a real historical commit")
        inherited = safe._object(
            value["safe_git_implementation"], {"project_root", "commit", "files", "basis"}
        )
        if (
            inherited["project_root"] != value["project_root"]
            or inherited["commit"] != head
            or inherited["basis"] != "COMMITTED_SOURCE_GIT_EOL_LF"
        ):
            _fail("IDENTITY", "historical helper identity does not match checkpoint code")
        helper_paths = self._historical_helper_paths(head)
        if current and (
            Path(value["project_root"]).resolve() != self.project_root
            or self._loaded_origins() != helper_paths
        ):
            _fail("IDENTITY", "loaded implementation paths differ from frozen identity")
        if current:
            for name, relative in zip(_EXECUTION_MODULES, _EXECUTION_PATHS, strict=True):
                origin = getattr(sys.modules.get(name), "__file__", None)
                if (
                    not isinstance(origin, str)
                    or safe._configuration_path(Path(origin)) != self.project_root / relative
                ):
                    _fail("IDENTITY", "loaded execution dependency differs from trusted checkout")
        own_paths = {MODULE_PATH, CLI_PATH, POLICY_PATH}
        if execution_bound:
            own_paths.update(_EXECUTION_PATHS)
        all_rows = []
        for rows, expected in (
            (value["files"], own_paths),
            (inherited["files"], helper_paths),
        ):
            if not isinstance(rows, list) or len(rows) != len(expected):
                _fail("IDENTITY", "complete required implementation file set missing")
            found = []
            for row in rows:
                safe._object(row, {"path", "sha256", "git_blob_content_sha256"})
                found.append(safe._relative(row["path"]))
                safe._digest(row["sha256"])
                safe._digest(row["git_blob_content_sha256"])
            if set(found) != expected or len(set(found)) != len(found):
                _fail("IDENTITY", "implementation file set differs from required authority")
            all_rows.extend(rows)
        references = [f"{head}:{row['path']}" for row in all_rows]
        blobs = self._blob_contents(self.project_root, references)
        for row, reference in zip(all_rows, references, strict=True):
            committed = blobs[reference]
            if safe._sha(committed) != row["git_blob_content_sha256"]:
                _fail("IDENTITY", "historical implementation blob checksum mismatch")
            if current:
                raw = _bytes(self.project_root / row["path"])
                if safe._sha(raw) != row["sha256"] or raw.replace(b"\r\n", b"\n") != committed:
                    _fail("DRIFT", "frozen implementation file changed")

    def _implementation_binding(self, reference: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if reference is not None:
            # A separate capture process resumes the exact plan's historical
            # implementation; it does not silently rebind to a newer main HEAD.
            if self._bound_implementation is not None and self._bound_implementation != reference:
                _fail("DRIFT", "planned implementation differs from initialized identity")
            self._verify_implementation(reference, current=True)
            self._bound_implementation = dict(reference)
        if self._bound_implementation is None:
            inherited = self.io._implementation_binding()
            head = inherited["commit"]
            own_paths = (MODULE_PATH, CLI_PATH, POLICY_PATH, *_EXECUTION_PATHS)
            references = [f"{head}:{path}" for path in own_paths]
            blobs = self._blob_contents(self.project_root, references)
            files = []
            for path, reference_path in zip(own_paths, references, strict=True):
                raw = _bytes(self.project_root / path)
                files.append(
                    {
                        "path": path,
                        "sha256": safe._sha(raw),
                        "git_blob_content_sha256": safe._sha(blobs[reference_path]),
                    }
                )
            self._bound_implementation = {
                "schema_version": "task_checkpoint_implementation.v2",
                "commit": head,
                "project_root": self.project_root.as_posix(),
                "files": files,
                "safe_git_implementation": inherited,
            }
        self._verify_implementation(self._bound_implementation, current=True)
        if _bytes(self.policy_path) != self.policy_bytes:
            _fail("DRIFT", "checkpoint policy changed after initialization")
        return self._bound_implementation

    def _scope(self, value: Mapping[str, Any]) -> dict[str, Any]:
        scope = safe._object(value, _SCOPE_KEYS)
        if scope["schema_version"] != "task_checkpoint_scope.v1":
            _fail("REQUEST", "unsupported scope schema")
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", safe._text(scope["checkpoint_id"])) is None:
            _fail("REQUEST", "invalid checkpoint id")
        for key in ("task_id", "actor", "thread_id", "source_branch"):
            safe._text(scope[key])
        if not scope["source_branch"].startswith("codex/"):
            _fail("IDENTITY", "non-protected task branch required")
        for key in ("source_root", "source_common_git_dir"):
            if not Path(safe._text(scope[key])).is_absolute():
                _fail("IDENTITY", "absolute task/common root required")
            safe._configuration_path(Path(scope[key]))
        for key in ("source_head_sha", "frozen_base_sha"):
            safe._digest(scope[key], 40)
        grant = safe._object(scope["scope_intent"], {"path", "sha256", "lease_id"})
        safe._relative(grant["path"])
        safe._digest(grant["sha256"])
        if re.fullmatch(r"lease-[a-z0-9]+", safe._text(grant["lease_id"])) is None:
            _fail("REQUEST", "invalid scope lease id")
        paths = scope["paths"]
        if not isinstance(paths, list) or not paths or len(paths) > self.policy["max_files"]:
            _fail("BUDGET", "empty or excessive path count")
        for path in paths:
            _source_path(path, self.policy, [])
        if paths != sorted(paths, key=str.casefold) or len({p.casefold() for p in paths}) != len(
            paths
        ):
            _fail("REQUEST", "unique casefold-sorted exact paths required")
        return scope

    def _request(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = safe._object(value, _REQUEST_KEYS)
        if request["schema_version"] != "task_checkpoint_request.v1":
            _fail("REQUEST", "unsupported request schema")
        files = request["files"]
        if not isinstance(files, list):
            _fail("REQUEST", "file table required")
        scope = {key: request[key] for key in _SCOPE_KEYS - {"schema_version", "paths"}}
        scope.update(
            schema_version="task_checkpoint_scope.v1", paths=[row["path"] for row in files]
        )
        self._scope(scope)
        total = 0
        for raw in files:
            row = safe._object(raw, {"path", "operation", "git_mode", "sha256", "size_bytes"})
            if row["operation"] not in {"ADD", "MODIFY", "DELETE"}:
                _fail("REQUEST", "unsupported file operation")
            if row["git_mode"] not in {"100644", "100755"}:
                _fail("REQUEST", "regular Git file mode required")
            if type(row["size_bytes"]) is not int or row["size_bytes"] < 0:
                _fail("REQUEST", "invalid source size")
            if row["operation"] == "DELETE":
                if row["sha256"] is not None or row["size_bytes"] != 0:
                    _fail("REQUEST", "deleted source has no new bytes")
            else:
                safe._digest(row["sha256"])
            total += row["size_bytes"]
        if total > self.policy["max_total_bytes"]:
            _fail("BUDGET", "aggregate source budget exceeded")
        safe._object(request["source_state"])
        safe._digest(request["implementation_sha256"])
        if (
            safe._sha(safe._json_bytes(request["implementation"]))
            != request["implementation_sha256"]
        ):
            _fail("REQUEST", "implementation identity digest mismatch")
        for key in ("observed_main_sha", "observed_origin_main_sha"):
            if request[key] is not None:
                safe._digest(request[key], 40)
        return request

    def _context(
        self,
        request: Mapping[str, Any],
        *,
        archive: Path | None = None,
    ) -> tuple[IntegrationPublicationFence, dict[str, bytes]]:
        root = safe._configuration_path(Path(request["source_root"]))
        policy_paths: dict[str, Path] = {}
        evidence: dict[str, bytes] = {}
        git_root = root if archive is None else self.project_root
        for name, relative in safe._OLD_POLICIES.items():
            path = root / relative if archive is None else archive / f"policies/{name}.yaml"
            content = _bytes(path)
            committed = self.io._git(
                git_root, "cat-file", "blob", f"{request['source_head_sha']}:{relative}"
            )
            if content.replace(b"\r\n", b"\n") != committed:
                _fail("IDENTITY", "source policies differ from exact source HEAD")
            evidence[f"policies/{name}.yaml"] = content
            policy_paths[name] = path
        authority = safe._object(safe_load_yaml_text(evidence["policies/fence.yaml"].decode()))[
            "authority"
        ]
        if (
            authority["checkout_guard_policy"] != safe._OLD_POLICIES["checkout"]
            or authority["parallel_control_policy"] != safe._OLD_POLICIES["parallel"]
        ):
            _fail("IDENTITY", "unexpected policy helper locators")
        fence = IntegrationPublicationFence(
            project_root=root,
            policy_path=policy_paths["fence"],
            checkout_guard_policy_path=policy_paths["checkout"],
            parallel_control_policy_path=policy_paths["parallel"],
        )
        for name, path in policy_paths.items():
            if _bytes(path) != evidence[f"policies/{name}.yaml"]:
                _fail("DRIFT", "source policies changed while initializing guard")
        return fence, evidence

    def _intent_lease(
        self,
        request: Mapping[str, Any],
        fence: IntegrationPublicationFence,
        intent_bytes: bytes,
        lease_id: str,
        events: Mapping[str, bytes],
        *,
        checkpoint: bool,
        expected_state: str = "RELEASED",
        released_outcome: str = "completed",
    ) -> None:
        if released_outcome not in {"completed", "failed"}:
            _fail("LEASE", "invalid terminal outcome evidence")
        if expected_state not in {"ACTIVE", "RELEASED"} or (
            expected_state == "ACTIVE" and not checkpoint
        ):
            _fail("LEASE", "invalid intent evidence state")
        raw = safe._object(load_strict_json_text(intent_bytes.decode("utf-8")))
        identity = safe._object(raw.get("workspace_identity"))
        if (
            raw.get("schema_version")
            != ("checkout_operation_intent.v2" if checkpoint else "checkout_operation_intent.v1")
            or raw.get("task_id") != request["task_id"]
            or raw.get("actor") != request["actor"]
            or raw.get("base_commit") != request["source_head_sha"]
            or raw.get("operation_class") not in {"domain_mutation", "shared_mutation"}
            or raw.get("known_unrelated_exclusions")
            != [row.to_dict() for row in fence.guard.policy.known_unrelated_exclusions]
            or identity.get("branch_name") != request["source_branch"]
            or identity.get("head_commit") != request["source_head_sha"]
            or Path(identity.get("checkout_root", "")).resolve()
            != Path(request["source_root"]).resolve()
            or Path(identity.get("git_common_dir", "")).resolve()
            != Path(request["source_common_git_dir"]).resolve()
        ):
            _fail("SCOPE", "intent does not bind exact task/source identity")
        intent = parse_checkout_operation_intent(raw)
        if intent.to_dict() != raw:
            _fail("SCOPE", "intent schema or safety fields changed")
        paths = (
            [row["path"] for row in request["files"]] if "files" in request else request["paths"]
        )
        declared = [*intent.owned_paths, *intent.shared_paths]
        if any(not any(safe._under(path, scope) for scope in declared) for path in paths):
            _fail("SCOPE", "file is not declared by the task mutation intent")
        if checkpoint and (
            intent.thread_id != request["thread_id"]
            or intent.inspection_profile != CHECKOUT_SOURCE_ONLY_PROFILE
            or intent.operation_class is not CheckoutOperationClass.SHARED_MUTATION
            or intent.owned_paths
            or list(intent.shared_paths) != sorted([*paths, RUNTIME])
        ):
            _fail("LEASE", "checkpoint lease scope differs from exact files/runtime")
        task, _ = fence.guard._lease_task(intent)
        records = []
        for name, content in events.items():
            event = parse_lease_event(safe._object(load_strict_json_text(content.decode("utf-8"))))
            if name != event.event_id + ".json" or event.lease.lease_id != lease_id:
                _fail("LEASE", "lease event locator mismatch")
            records.append(event)
        replay = replay_lease_events(records)
        if replay.status != "PASS" or len(replay.lease_heads) != 1:
            _fail("LEASE", "complete immutable lease replay required")
        head = replay.lease_heads[0]
        if (
            head.lease_id != lease_id
            or head.state != expected_state
            or head.change_id != "checkout:" + intent.intent_id
            or head.actor != intent.actor
            or head.base_commit != request["source_head_sha"]
            or head.task_id != fence.guard.policy.authority_task_id
            or head.change_manifest_sha256 != task.manifest.sha256
        ):
            _fail("LEASE", "lease does not bind expected state and task scope")
        final_id = dict(replay.head_event_ids)[lease_id]
        final = next(row for row in records if row.event_id == final_id)
        if (
            checkpoint
            and expected_state == "RELEASED"
            and final.reason_codes != ("CHECKOUT_OPERATION_" + released_outcome.upper(),)
        ):
            _fail("LEASE", "checkpoint completion lease was not successful")

    def _lease_bytes(self, fence: IntegrationPublicationFence, lease_id: str) -> dict[str, bytes]:
        folder = fence.guard.store.events_root / lease_id
        safe._configuration_path(folder)
        return {path.name: _bytes(path) for path in sorted(folder.glob("*.json"))}

    def _grant(
        self,
        request: Mapping[str, Any],
        fence: IntegrationPublicationFence,
    ) -> dict[str, bytes]:
        grant = request["scope_intent"]
        path = safe._member(Path(request["source_root"]), grant["path"])
        if path.parent != fence.guard.runtime_root / "intents" or path.suffix != ".json":
            _fail("SCOPE", "exact checkout intent locator required")
        content = _bytes(path)
        if safe._sha(content) != grant["sha256"]:
            _fail("SCOPE", "scope intent checksum changed")
        raw = safe._object(load_strict_json_text(content.decode()))
        if path.name != raw.get("intent_id", "") + ".json":
            _fail("SCOPE", "intent id/path mismatch")
        events = self._lease_bytes(fence, grant["lease_id"])
        self._intent_lease(request, fence, content, grant["lease_id"], events, checkpoint=False)
        return {
            "scope_intent.json": content,
            **{f"scope_lease/{name}": data for name, data in events.items()},
        }

    def _entry(self, root: Path, commit: str, path: str) -> tuple[str, str] | None:
        return self._entries(root, commit, [path]).get(path)

    def _entries(self, root: Path, commit: str, paths: list[str]) -> dict[str, tuple[str, str]]:
        """Read immutable tree metadata in bounded command-line batches."""
        result: dict[str, tuple[str, str]] = {}
        # Windows command-line bound; all selectors are exact literal pathspecs.
        groups: list[list[str]] = [[]]
        length = 0
        for path in paths:
            safe._relative(path)
            if len(path) > 4096:
                _fail("PATH", "path exceeds supported metadata selector length")
            if length + len(path) + 24 > 7000 and groups[-1]:
                groups.append([])
                length = 0
            groups[-1].append(path)
            length += len(path) + 24
        for group in groups:
            if not group:
                continue
            raw = self.io._git(
                root, "ls-tree", "-z", commit, "--", *(f":(literal){p}" for p in group)
            )
            for row in raw.split(b"\0"):
                if not row:
                    continue
                metadata, found = row.split(b"\t", 1)
                mode, kind, oid = metadata.decode("ascii").split()
                path = found.decode("utf-8")
                if (
                    path not in group
                    or path in result
                    or mode not in {"100644", "100755"}
                    or kind != "blob"
                ):
                    _fail("UNSUPPORTED_CHANGE", "unique exact regular Git blob required")
                result[path] = (mode, safe._digest(oid, 40))
        return result

    def _require_supported_index(self, root: Path) -> None:
        """Inspect index metadata only; V3 permits ordinary staged changes.

        ls-files without --others/status does not hash/open worktree contents or
        consult .gitignore/.gitattributes. Preserve the original index verbatim;
        build the snapshot from HEAD plus requested raw working-tree bytes in a
        private index. Staged entries are not source-selection authority. Only
        unresolved or sparse-directory entries are unsupported here.
        """
        index = {}
        for row in self.io._git(root, "ls-files", "--stage", "--sparse", "-z").split(b"\0"):
            if row:
                metadata, path = row.split(b"\t", 1)
                mode, oid, stage = metadata.split()
                if stage != b"0" or path in index or mode == b"040000":
                    _fail("UNSUPPORTED_CHANGE", "unmerged or sparse-directory index unsupported")
                index[path] = (mode, oid)

    def _history(
        self, root: Path, head: str, row: Mapping[str, Any], content: bytes | None
    ) -> None:
        path = row["path"]
        if not path.startswith("registry/development_tasks/"):
            return
        if row["operation"] == "DELETE":
            _fail("HISTORY", "canonical task history cannot be deleted")
        if content is None:
            _fail("HISTORY", "canonical fragment bytes missing")
        if row["operation"] == "MODIFY":
            self.io._history(root, head, path, content)
        else:
            fragment = safe._object(safe_load_yaml_text(content.decode("utf-8")))
            validate_canonical_fragment(fragment)

    def _state(
        self,
        request: Mapping[str, Any],
        fence: IntegrationPublicationFence,
    ) -> tuple[dict[str, Any], dict[str, bytes], dict[str, Any]]:
        root = safe._configuration_path(Path(request["source_root"]))
        configuration = self.io._environment(root)

        def get(*args: str) -> str:
            return self.io._git(root, *args).decode().strip()

        head = get("rev-parse", "--verify", "HEAD")
        branch = get("symbolic-ref", "--short", "HEAD")
        common = Path(get("rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
        trusted_common = Path(
            self.io._git(
                self.project_root, "rev-parse", "--path-format=absolute", "--git-common-dir"
            )
            .decode()
            .strip()
        ).resolve()
        if (
            head != request["source_head_sha"]
            or branch != request["source_branch"]
            or Path(get("rev-parse", "--show-toplevel")).resolve() != root
            or common != Path(request["source_common_git_dir"]).resolve()
            or common != trusted_common
        ):
            _fail("IDENTITY", "source branch/HEAD/common root drift")
        self.io._git(root, "merge-base", "--is-ancestor", request["frozen_base_sha"], head)
        exclusions = [row.path for row in fence.guard.policy.known_unrelated_exclusions]
        paths = request.get("paths") or [row["path"] for row in request["files"]]
        # Scope and known-unrelated rejection precedes attr evaluation/content reads.
        for path in paths:
            _source_path(path, self.policy, exclusions)
        self._require_supported_index(root)
        existing_entries = self._entries(root, head, paths)
        rows: list[dict[str, Any]] = []
        total = 0
        # Complete metadata pass before any source content capture; no partial budget bypass.
        for path in paths:
            existing = existing_entries.get(path)
            actual = safe._member(root, path)
            safe._configuration_path(actual)
            try:
                info = actual.lstat()
            except FileNotFoundError:
                info = None
            if info is None:
                if existing is None:
                    _fail(
                        "UNSUPPORTED_CHANGE", "requested path exists in neither HEAD nor worktree"
                    )
                operation = "DELETE"
                if path.startswith("registry/development_tasks/"):
                    _fail("HISTORY", "canonical task history cannot be deleted")
                size, mode = 0, existing[0] if existing else "100644"
            else:
                operation = "ADD" if existing is None else "MODIFY"
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    _fail("PATH", "single-link regular source required")
                size = info.st_size
                mode = (
                    existing[0]
                    if existing
                    else ("100755" if os.name != "nt" and info.st_mode & 0o111 else "100644")
                )
                total += size
            rows.append(
                {
                    "path": path,
                    "operation": operation,
                    "git_mode": mode,
                    "size_bytes": size,
                    "sha256": None,
                }
            )
        if len(rows) > self.policy["max_files"] or total > self.policy["max_total_bytes"]:
            _fail("BUDGET", "source metadata exceeds reviewed engineering budgets")
        # A second complete pass catches replacement/size growth before opening
        # any requested source file. Capture then uses no filters/attributes.
        for row in rows:
            actual = safe._configuration_path(safe._member(root, row["path"]))
            try:
                info = actual.lstat()
            except FileNotFoundError:
                info = None
            if row["operation"] == "DELETE":
                if info is not None:
                    _fail("DRIFT", "deleted path appeared before capture")
            elif (
                info is None
                or not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_size != row["size_bytes"]
            ):
                _fail("DRIFT", "requested source metadata changed before capture")
        captures = {}
        for row in rows:
            path = row["path"]
            if row["operation"] != "DELETE":
                data = _bytes(safe._member(root, path), self.policy["max_total_bytes"])
                if len(data) != row["size_bytes"]:
                    _fail("DRIFT", "file changed after metadata budget pass")
                row["sha256"] = safe._sha(data)
                captures[path] = data
                self._history(root, head, row, data)
        index_path = Path(get("rev-parse", "--path-format=absolute", "--git-path", "index"))
        index = _bytes(index_path)
        state = {
            "head": head,
            "branch": branch,
            "common_git_dir": common.as_posix(),
            "frozen_base_sha": request["frozen_base_sha"],
            "index_sha256": safe._sha(index),
            "index_size_bytes": len(index),
            "git_configuration": configuration,
            "unscoped_worktree_status": "NOT_INSPECTED",
            "source_mutation_allowed": False,
            "clean_integration_status": "NOT_EVALUATED",
            "files": rows,
        }
        refs = {}
        for name, ref in (("main", "refs/heads/main"), ("origin_main", "refs/remotes/origin/main")):
            refs[name] = (
                self.io._git(root, "rev-parse", "--verify", "--quiet", ref, allowed=(0, 1))
                .decode()
                .strip()
                or None
            )
        return state, captures, refs

    @_public
    def plan(self, scope: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._scope(scope)
        self.io._environment(Path(checked["source_root"]))
        implementation = self._implementation_binding()
        fence, _ = self._context(checked)
        self._grant(checked, fence)
        state, _, refs = self._state(checked, fence)
        body = {key: checked[key] for key in _SCOPE_KEYS - {"schema_version", "paths"}}
        return {
            **body,
            "schema_version": "task_checkpoint_request.v1",
            "files": state["files"],
            "source_state": state,
            "implementation": implementation,
            "implementation_sha256": safe._sha(safe._json_bytes(implementation)),
            "observed_main_sha": refs["main"],
            "observed_origin_main_sha": refs["origin_main"],
        }

    def _event(
        self, run: Path, events: list[dict[str, Any]], phase: str, payload: dict[str, Any]
    ) -> None:
        if len(events) >= len(_PHASES) or phase != _PHASES[len(events)]:
            _fail("PHASE", "checkpoint event sequence mismatch")
        body = {
            "schema_version": "task_checkpoint_event.v1",
            "checkpoint_id": run.name,
            "sequence": len(events) + 1,
            "phase": phase,
            "occurred_at": datetime.now(UTC).isoformat(),
            "previous_event_id": events[-1]["event_id"] if events else None,
            "payload": payload,
        }
        event = {**body, "event_id": safe._sha(safe._json_bytes(body))}
        safe._write_once(
            run / "events" / f"{len(events) + 1:02d}-{phase}.json", safe._json_bytes(event)
        )
        events.append(event)

    def _unchanged(
        self,
        request: dict[str, Any],
        fence: IntegrationPublicationFence,
        implementation: dict[str, Any],
    ) -> dict[str, Any]:
        state, _, refs = self._state(request, fence)
        if state != request["source_state"] or state["files"] != request["files"]:
            _fail("DRIFT", "source HEAD/index/config/bytes changed since plan")
        if self._implementation_binding() != implementation:
            _fail("DRIFT", "trusted implementation changed during checkpoint")
        return refs

    def _snapshot(
        self,
        root: Path,
        request: dict[str, Any],
        snapshot: dict[str, Any],
        *,
        require_ref: bool = True,
    ) -> None:
        safe._object(snapshot, {"commit", "parent", "tree", "ref", "files"})
        commit, tree = safe._digest(snapshot["commit"], 40), safe._digest(snapshot["tree"], 40)
        if (
            snapshot["parent"] != request["source_head_sha"]
            or snapshot["ref"] != REF_PREFIX + request["checkpoint_id"]
        ):
            _fail("SNAPSHOT", "source checkpoint identity mismatch")
        if self.io._git(root, "rev-list", "--parents", "-n", "1", commit).decode().split() != [
            commit,
            snapshot["parent"],
        ]:
            _fail("SNAPSHOT", "exact single source parent required")
        if self.io._git(root, "rev-parse", commit + "^{tree}").decode().strip() != tree:
            _fail("SNAPSHOT", "tree binding mismatch")
        if (
            require_ref
            and self.io._git(root, "rev-parse", "--verify", snapshot["ref"]).decode().strip()
            != commit
        ):
            _fail("SNAPSHOT", "checkpoint ref mismatch")
        message = self.io._git(root, "show", "-s", "--format=%B", commit).decode().strip()
        if message != self._message(request).decode().strip():
            _fail("SNAPSHOT", "immutable commit does not bind request")
        if len(snapshot["files"]) != len(request["files"]):
            _fail("SNAPSHOT", "file manifest mismatch")
        paths = [row["path"] for row in request["files"]]
        old_entries = self._entries(root, snapshot["parent"], paths)
        new_entries = self._entries(root, commit, paths)
        references = [
            f"{commit}:{row['path']}" for row in request["files"] if row["operation"] != "DELETE"
        ]
        contents = self._blob_contents(root, references, limit=self.policy["max_total_bytes"])
        expected_changes = []
        for expected, actual in zip(request["files"], snapshot["files"], strict=True):
            safe._object(actual, set(expected) | {"blob_oid"})
            if {key: actual[key] for key in expected} != expected:
                _fail("SNAPSHOT", "operation/byte manifest mismatch")
            old = old_entries.get(expected["path"])
            new = new_entries.get(expected["path"])
            if expected["operation"] == "DELETE":
                if (
                    old is None
                    or new is not None
                    or actual["blob_oid"] is not None
                    or old[0] != expected["git_mode"]
                ):
                    _fail("SNAPSHOT", "delete not represented exactly")
                expected_changes.append(expected["path"])
                continue
            oid = safe._digest(actual["blob_oid"], 40)
            if new != (expected["git_mode"], oid) or (
                (expected["operation"] == "ADD") != (old is None)
            ):
                _fail("SNAPSHOT", "add/modify tree entry mismatch")
            content = contents[f"{commit}:{expected['path']}"]
            if len(content) != expected["size_bytes"]:
                _fail("SNAPSHOT", "blob size mismatch")
            if safe._sha(content) != expected["sha256"]:
                _fail("SNAPSHOT", "raw blob bytes mismatch")
            self._history(root, snapshot["parent"], expected, content)
            if old != new:
                expected_changes.append(expected["path"])
        changes = self.io._git(
            root,
            "diff-tree",
            "--no-commit-id",
            "--name-only",
            "--no-renames",
            "--no-ext-diff",
            "-r",
            "-z",
            snapshot["parent"],
            commit,
        ).split(b"\0")
        actual_changes = sorted(
            (path.decode("utf-8") for path in changes if path), key=str.casefold
        )
        if actual_changes != sorted(expected_changes, key=str.casefold):
            _fail("SNAPSHOT", "snapshot changes undeclared source paths")

    def _message(self, request: Mapping[str, Any]) -> bytes:
        return (
            f"Task-only checkpoint {request['checkpoint_id']}\n\nprofile={PROFILE}\n"
            f"request_sha256={safe._sha(safe._json_bytes(request))}\n"
        ).encode()

    def _persist_capture_bundle(
        self,
        run: Path,
        request: dict[str, Any],
        captures: Mapping[str, bytes],
    ) -> None:
        records = []
        for number, row in enumerate(request["files"]):
            storage = None if row["operation"] == "DELETE" else f"capture/{number:06d}.bin"
            if storage is not None:
                content = captures[row["path"]]
                if len(content) != row["size_bytes"] or safe._sha(content) != row["sha256"]:
                    _fail("CAPTURE_BUNDLE", "in-memory source differs from frozen request")
                safe._write_once(safe._member(run, storage), content)
            records.append({"file": row, "storage_path": storage})
        safe._write_once(
            run / "capture_manifest.json",
            safe._json_bytes(
                {
                    "schema_version": "task_checkpoint_capture_bundle.v1",
                    "request_sha256": safe._sha(safe._json_bytes(request)),
                    "records": records,
                }
            ),
        )
        self._read_capture_bundle(run, request)

    def _read_capture_bundle(self, run: Path, request: dict[str, Any]) -> dict[str, bytes]:
        manifest = _json(run / "capture_manifest.json")
        expected = {
            "schema_version": "task_checkpoint_capture_bundle.v1",
            "request_sha256": safe._sha(safe._json_bytes(request)),
            "records": [
                {
                    "file": row,
                    "storage_path": (
                        None if row["operation"] == "DELETE" else f"capture/{number:06d}.bin"
                    ),
                }
                for number, row in enumerate(request["files"])
            ],
        }
        if manifest != expected:
            _fail("CAPTURE_BUNDLE", "durable source mapping differs from frozen request")
        result = {}
        budget = self.policy["max_total_bytes"]
        for record in expected["records"]:
            if record["storage_path"] is None:
                continue
            row = record["file"]
            content = _bytes(safe._member(run, record["storage_path"]), limit=budget)
            if len(content) != row["size_bytes"] or safe._sha(content) != row["sha256"]:
                _fail("CAPTURE_BUNDLE", "durable source bytes differ from frozen request")
            budget -= len(content)
            result[row["path"]] = content
        return result

    def _object_intent(self, request: dict[str, Any], tree: str, timestamp: str) -> dict[str, Any]:
        return {
            "schema_version": "task_checkpoint_object_intent.v1",
            "request_sha256": safe._sha(safe._json_bytes(request)),
            "parent": request["source_head_sha"],
            "tree": tree,
            "ref": REF_PREFIX + request["checkpoint_id"],
            "message_sha256": safe._sha(self._message(request)),
            "timestamp": timestamp,
            "author_name": "Source preservation coordinator",
            "author_email": "source-preservation@localhost",
            "committer_name": "Source preservation coordinator",
            "committer_email": "source-preservation@localhost",
        }

    def _validate_object_intent(
        self,
        run: Path,
        request: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> None:
        intent = _json(run / "object_intent.json")
        instant = datetime.fromisoformat(intent["timestamp"])
        if instant.tzinfo is None or intent != self._object_intent(
            request,
            snapshot["tree"],
            intent["timestamp"],
        ):
            _fail("OBJECT_INTENT", "frozen commit intent mismatch")
        actual = (
            self.io._git(
                self.project_root,
                "show",
                "-s",
                "--format=%an%x00%ae%x00%cn%x00%ce%x00%at%x00%ct",
                snapshot["commit"],
            )
            .decode()
            .strip()
            .split("\0")
        )
        if actual != [
            intent["author_name"],
            intent["author_email"],
            intent["committer_name"],
            intent["committer_email"],
            str(int(instant.timestamp())),
            str(int(instant.timestamp())),
        ]:
            _fail("OBJECT_INTENT", "actual commit identity differs from durable intent")

    def _capture_events(self, run: Path) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for number, phase in enumerate(_PHASES, 1):
            path = run / "events" / f"{number:02d}-{phase}.json"
            if not path.exists():
                break
            event = _json(path)
            safe._object(
                event,
                {
                    "schema_version",
                    "checkpoint_id",
                    "sequence",
                    "phase",
                    "occurred_at",
                    "previous_event_id",
                    "payload",
                    "event_id",
                },
            )
            body = {key: value for key, value in event.items() if key != "event_id"}
            instant = datetime.fromisoformat(event["occurred_at"])
            if (
                event["schema_version"] != "task_checkpoint_event.v1"
                or event["checkpoint_id"] != run.name
                or event["sequence"] != number
                or event["phase"] != phase
                or instant.tzinfo is None
                or event["previous_event_id"] != (events[-1]["event_id"] if events else None)
                or event["event_id"] != safe._sha(safe._json_bytes(body))
                or (events and instant < datetime.fromisoformat(events[-1]["occurred_at"]))
            ):
                _fail("PHASE", "capture event prefix is invalid")
            events.append(event)
        return events

    def _attempt(
        self, run: Path, request: dict[str, Any], *, require_producer: bool = False
    ) -> dict[str, Any]:
        attempt = _json(run / "attempt.json")
        version = attempt.get("schema_version")
        keys = {"schema_version", "checkpoint_id", "request_sha256", "intent_id", "created_at"}
        if version == "task_checkpoint_attempt.v2":
            keys.add("producer")
        elif version != "task_checkpoint_attempt.v1" or require_producer:
            _fail("RECOVERY_PRODUCER", "original producer identity is unavailable")
        safe._object(attempt, keys)
        instant = datetime.fromisoformat(attempt["created_at"])
        if (
            attempt["checkpoint_id"] != request["checkpoint_id"]
            or attempt["request_sha256"] != safe._sha(safe._json_bytes(request))
            or not isinstance(attempt["intent_id"], str)
            or re.fullmatch(
                r"task-checkpoint-[0-9a-f]{64}"
                if version == "task_checkpoint_attempt.v2"
                else r"task-checkpoint-[0-9a-f]{32}",
                attempt["intent_id"],
            )
            is None
            or instant.tzinfo is None
        ):
            _fail("RECEIPT", "create-only attempt/request identity mismatch")
        if version == "task_checkpoint_attempt.v2":
            workflow_coordination._process(attempt["producer"])
            bound = {key: value for key, value in attempt.items() if key != "intent_id"}
            if attempt["intent_id"] != "task-checkpoint-" + safe._sha(safe._json_bytes(bound)):
                _fail("RECOVERY_PRODUCER", "producer/request differs from frozen intent identity")
        return attempt

    def _install_attempt(self, run: Path, request: dict[str, Any]) -> dict[str, Any]:
        """Publish complete identity before acquire; partial stages own no lease."""
        if os.name != "nt":
            _fail("ENVIRONMENT", "atomic attempt installation requires Windows")
        attempt = {
            "schema_version": "task_checkpoint_attempt.v2",
            "checkpoint_id": request["checkpoint_id"],
            "request_sha256": safe._sha(safe._json_bytes(request)),
            "created_at": datetime.now(UTC).isoformat(),
            "producer": workflow_execution.current_process_identity(),
        }
        # The real lease's immutable change_id binds this intent id. A resealed
        # producer cannot keep ownership of an already acquired original lease.
        attempt["intent_id"] = "task-checkpoint-" + safe._sha(safe._json_bytes(attempt))
        safe._configuration_path(run)
        run.parent.mkdir(parents=True, exist_ok=True)
        stage = run.with_name(f"{run.name}.staging-{uuid4().hex}")
        stage.mkdir(exist_ok=False)
        safe._write_once(stage / "attempt.json", safe._json_bytes(attempt))
        safe._write_once(stage / "request.json", safe._json_bytes(request))
        if self._attempt(stage, request, require_producer=True) != attempt or _bytes(
            stage / "request.json"
        ) != safe._json_bytes(request):
            _fail("DRIFT", "staged attempt changed before atomic installation")
        safe._configuration_path(run)
        # Windows rename is create-only, including directory destinations. An
        # abandoned stage is preserved but is never a request/execution authority.
        os.rename(stage, run)
        return attempt

    def _execution_evidence(self, run: Path) -> dict[str, str]:
        return {
            "request_sha256": safe._sha(_bytes(run / "execution_request.json")),
            "result_sha256": safe._sha(_bytes(run / "worker_result.json")),
        }

    def _validate_capture_execution(
        self, run: Path, request: dict[str, Any], receipt: Mapping[str, Any]
    ) -> None:
        records = [
            parse_lease_event(_json(path))
            for path in sorted((run / "authority/checkpoint_lease").glob("*.json"))
        ]
        replay = replay_lease_events(records)
        if replay.status != "PASS" or len(replay.lease_heads) != 1:
            _fail("EXECUTION", "complete checkpoint execution lease required")
        head = replay.lease_heads[0]
        execution = head.execution
        attempt = self._attempt(run, request)
        if (
            execution is not None
            and "producer" in attempt
            and attempt["producer"] != execution["launcher"]
        ):
            _fail("RECOVERY_PRODUCER", "attempt producer differs from actual execution launcher")
        if execution is None:
            if receipt["schema_version"] != "task_checkpoint_receipt.v1":
                _fail("EXECUTION", "supervised receipt has no execution evidence")
            return
        if receipt["schema_version"] != "task_checkpoint_receipt.v2":
            _fail("EXECUTION", "execution receipt cannot downgrade to historical schema")
        frozen = workflow_coordination._request(_json(run / "execution_request.json"))
        original_root = Path(request["implementation"]["project_root"])
        expected = {
            **self._execution_subject(request, _bytes(run / "checkpoint_intent.json")),
            "lease_id": receipt["lease_id"],
            "cwd": original_root.as_posix(),
            "stdout_path": (run / "worker.stdout.log").as_posix(),
            "result_path": (run / "worker_result.json").as_posix(),
            "argv": [
                frozen["argv"][0],
                str(original_root / CLI_PATH),
                "capture-worker",
                "--execution-request",
                str(run / "execution_request.json"),
            ],
        }
        if (
            frozen != execution["request"]
            or any(frozen.get(key) != value for key, value in expected.items())
            or execution["state"] != "RESULT_RECORDED"
            or execution["exit"]["basis"] != "LIVE_CONTAINED_HANDLE"
            or execution["exit"]["returncode"] != 0
            or execution["exit"]["job_state"] != "EMPTY"
            or execution["result"]["status"] != "PASS"
            or receipt["execution"] != self._execution_evidence(run)
        ):
            _fail("EXECUTION", "supervised checkpoint has no exact successful exit/result chain")
        result = _json(run / "worker_result.json")
        safe._object(
            result,
            set(workflow_coordination._result_binding(frozen))
            | {
                "status",
                "snapshot",
                "source_state",
                "observed_refs_before",
                "observed_refs_after",
                "head_event_id",
                "worker_process",
            },
        )
        expected_result = {
            **workflow_coordination._result_binding(frozen),
            "status": "PASS",
            "snapshot": receipt["snapshot"],
            "source_state": request["source_state"],
            "observed_refs_before": receipt["observed_refs_before"],
        }
        verified = _json(run / "events/05-VERIFIED.json")
        if (
            any(result.get(key) != value for key, value in expected_result.items())
            or result["head_event_id"] != verified["event_id"]
            or result["observed_refs_after"] != verified["payload"]["observed_refs"]
            or execution["result"]["artifact"]
            != {
                "path": frozen["result_path"],
                "sha256": receipt["execution"]["result_sha256"],
            }
        ):
            _fail("EXECUTION", "worker result bytes or source proof changed")
        workflow_coordination._process(result["worker_process"])
        observation = _json(run / "worker_observation.json")
        if (
            observation
            != {
                "status": "PASS",
                "lease_id": head.lease_id,
                "execution_request_sha256": execution["request_sha256"],
                "worker_process": result["worker_process"],
                "effect_authority": "SAME_SOURCE_ONLY_LEASE",
            }
            or result["worker_process"] == execution["launcher"]
        ):
            _fail("EXECUTION", "worker observation is not bound to the contained execution")

    def _execution_locations(self, run: Path) -> dict[str, Any]:
        return {
            "argv": [
                sys.executable,
                str(self.project_root / CLI_PATH),
                "capture-worker",
                "--execution-request",
                str(run / "execution_request.json"),
            ],
            "cwd": self.project_root.as_posix(),
            "stdout_path": (run / "worker.stdout.log").as_posix(),
            "result_path": (run / "worker_result.json").as_posix(),
        }

    def _execution_subject(self, checked: dict[str, Any], intent_bytes: bytes) -> dict[str, Any]:
        intent = parse_checkout_operation_intent(load_strict_json_text(intent_bytes.decode()))
        return {
            "execution_kind": "TASK_SOURCE_CAPTURE",
            "source_root": checked["source_root"],
            "source_head_sha": checked["source_head_sha"],
            "checkpoint_id": checked["checkpoint_id"],
            "checkpoint_task_id": checked["task_id"],
            "checkpoint_thread_id": checked["thread_id"],
            "checkpoint_request_sha256": safe._sha(safe._json_bytes(checked)),
            "checkpoint_intent_id": intent.intent_id,
            "checkpoint_intent_sha256": safe._sha(intent_bytes),
            "scope_intent_sha256": checked["scope_intent"]["sha256"],
        }

    def _execution_host(self, fence: IntegrationPublicationFence) -> dict[str, str]:
        binding = fence.guard.store.coordination_binding
        if binding is not None:
            binding.assert_current(operation="observe")
            return {"host_id": binding.host_id, "writer_epoch": binding.epoch}
        return {
            "host_id": workflow_coordination.machine_host_id(),
            "writer_epoch": "UNENROLLED_LEGACY",
        }

    @_public
    def capture_worker(self, execution_request_path: Path) -> dict[str, Any]:
        """Only the frozen contained child may execute the source effects."""
        path = safe._configuration_path(execution_request_path.absolute())
        if path.name != "execution_request.json" or len(path.parents) < 5:
            _fail("EXECUTION", "exact execution request locator required")
        run, root = path.parent, path.parents[4]
        if path != safe._member(root, f"{RUNTIME}/{run.name}/execution_request.json"):
            _fail("EXECUTION", "execution request is outside checkpoint runtime")
        execution = workflow_coordination._request(_json(path))
        checked = self._request(_json(run / "request.json"))
        if Path(checked["source_root"]).resolve() != root or checked["checkpoint_id"] != run.name:
            _fail("EXECUTION", "worker request/source locator mismatch")
        implementation = self._implementation_binding(checked["implementation"])
        fence, _ = self._context(checked)
        intent_bytes = _bytes(run / "checkpoint_intent.json")
        expected = {
            **self._execution_subject(checked, intent_bytes),
            **self._execution_locations(run),
            **self._execution_host(fence),
        }
        if any(execution.get(key) != value for key, value in expected.items()):
            _fail("EXECUTION", "worker does not bind exact source and trusted entrypoint")
        lifecycle = fence.guard.store.execution_lifecycle()
        witness = lifecycle.require_checkpoint_worker(execution, actor=checked["actor"])
        self._intent_lease(
            checked,
            fence,
            intent_bytes,
            execution["lease_id"],
            self._lease_bytes(fence, execution["lease_id"]),
            checkpoint=True,
            expected_state="ACTIVE",
        )
        grant = self._grant(checked, fence)
        configuration = self.io._environment(root)
        # Retained observation, never a reusable admission capability.
        safe._write_once(run / "worker_observation.json", safe._json_bytes(witness))
        state, captures, before_refs = self._state(checked, fence)
        if state != checked["source_state"] or state["files"] != checked["files"]:
            _fail("DRIFT", "source changed before contained capture")
        events = self._capture_events(run)
        if len(events) != 1 or events[0]["payload"] != {"lease_id": execution["lease_id"]}:
            _fail("PARTIAL", "worker requires one original ACQUIRED event; no redispatch")

        def effect_boundary() -> None:
            lifecycle.require_checkpoint_worker(execution, actor=checked["actor"])
            self._unchanged(checked, fence, implementation)
            self.io._recheck_environment(root, configuration)
            fence.guard.store.heartbeat(
                execution["lease_id"], actor=checked["actor"], now=datetime.now(UTC)
            )

        effect_boundary()
        self._persist_capture_bundle(run, checked, captures)
        self._event(run, events, "CAPTURED", {"source_state": state, "observed_refs": before_refs})
        index = run / "private.index"
        self.io._git(root, "read-tree", checked["source_head_sha"], index=index)
        blobs, index_entries = [], []
        for row in checked["files"]:
            if row["operation"] == "DELETE":
                oid = None
                index_entries.append(f"0 {'0' * 40}\t{row['path']}\0".encode())
            else:
                oid = (
                    self.io._git(
                        root,
                        "hash-object",
                        "-w",
                        "--stdin",
                        "--no-filters",
                        content=captures[row["path"]],
                    )
                    .decode()
                    .strip()
                )
                safe._digest(oid, 40)
                index_entries.append(f"{row['git_mode']} {oid}\t{row['path']}\0".encode())
            blobs.append({**row, "blob_oid": oid})
        self.io._git(
            root, "update-index", "-z", "--index-info", content=b"".join(index_entries), index=index
        )
        tree = self.io._git(root, "write-tree", index=index).decode().strip()
        object_intent = self._object_intent(checked, tree, datetime.now(UTC).isoformat())
        safe._write_once(run / "object_intent.json", safe._json_bytes(object_intent))
        commit = (
            self.io._git(
                root,
                "commit-tree",
                tree,
                "-p",
                checked["source_head_sha"],
                content=self._message(checked),
                timestamp=object_intent["timestamp"],
            )
            .decode()
            .strip()
        )
        snapshot = {
            "commit": commit,
            "parent": checked["source_head_sha"],
            "tree": tree,
            "ref": REF_PREFIX + checked["checkpoint_id"],
            "files": blobs,
        }
        self._snapshot(root, checked, snapshot, require_ref=False)
        self._event(run, events, "OBJECTS_WRITTEN", {"snapshot": snapshot})
        effect_boundary()
        if self._grant(checked, fence) != grant:
            _fail("DRIFT", "scope evidence changed before ref publication")
        self.io._git(root, "update-ref", snapshot["ref"], commit, "0" * 40)
        self._event(run, events, "REF_CREATED", {"ref": snapshot["ref"], "commit": commit})
        self._snapshot(root, checked, snapshot)
        after_refs = self._unchanged(checked, fence, implementation)
        lifecycle.require_checkpoint_worker(execution, actor=checked["actor"])
        self._event(run, events, "VERIFIED", {"source_state": state, "observed_refs": after_refs})
        result = {
            **workflow_coordination._result_binding(execution),
            "status": "PASS",
            "snapshot": snapshot,
            "source_state": state,
            "observed_refs_before": before_refs,
            "observed_refs_after": after_refs,
            "head_event_id": events[-1]["event_id"],
            "worker_process": witness["worker_process"],
        }
        safe._write_once(Path(execution["result_path"]), safe._json_bytes(result))
        return result

    def _supervised_capture(
        self,
        checked: dict[str, Any],
        run: Path,
        fence: IntegrationPublicationFence,
        handle: CheckoutLeaseHandle,
        intent_bytes: bytes,
    ) -> dict[str, Any]:
        lifecycle = fence.guard.store.execution_lifecycle()
        replay = fence.guard.store.replay()
        if replay.status != "PASS":
            _fail("LEASE", "invalid execution lease replay")
        lease = next(row for row in replay.lease_heads if row.lease_id == handle.lease_id)
        environment = {
            **os.environ,
            "PYTHONPATH": str(self.project_root / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        execution = {
            "schema_version": "workflow_execution_request.v2",
            "request_id": uuid4().hex,
            "lease_id": lease.lease_id,
            "manifest_sha256": lease.change_manifest_sha256,
            "subject_task_id": lease.task_id,
            **self._execution_subject(checked, intent_bytes),
            **self._execution_locations(run),
            **self._execution_host(fence),
            "environment_sha256": workflow_execution.execution_environment_sha256(environment),
            "job_name": "Local\\AITS-DEVX015-checkpoint-" + uuid4().hex,
        }
        with fence.guard.store.atomic(
            actor=checked["actor"], now=datetime.now(UTC), operation="checkpoint-reserve"
        ):
            attempt = self._attempt(run, checked, require_producer=True)
            if (
                attempt["producer"] != workflow_execution.current_process_identity()
                or lease.change_id != "checkout:" + attempt["intent_id"]
                or _bytes(run / "request.json") != safe._json_bytes(checked)
                or (run / "interrupted_recovery.json").exists()
            ):
                _fail("RECOVERY_PRODUCER", "original attempt no longer owns launch")
            safe._write_once(run / "execution_request.json", safe._json_bytes(execution))
            reservation = lifecycle.reserve(execution, actor=checked["actor"])
        if reservation["dispatch_allowed"] is not True:
            _fail("PARTIAL", "existing execution cannot dispatch again")
        process = None
        result: dict[str, Any] | None = None
        primary_error: BaseException | None = None
        cleanup_errors: list[dict[str, str]] = []
        try:
            process = workflow_execution.WindowsJobProcess.create(
                argv=execution["argv"],
                cwd=self.project_root,
                environment=environment,
                stdout_path=Path(execution["stdout_path"]),
                job_name=execution["job_name"],
            )
            lifecycle.bind(lease.lease_id, process, actor=checked["actor"])
            lifecycle.resume(lease.lease_id, process, actor=checked["actor"])
            # Engineering hang bound, not a lease-expiry or recovery decision.
            code = process.wait(timeout=300)
            lifecycle.confirm_exit(lease.lease_id, process, actor=checked["actor"])
            if code != 0:
                _fail("EXECUTION", "contained capture did not exit successfully")
            result = _json(Path(execution["result_path"]))
            if result.get("status") != "PASS":
                _fail("EXECUTION", "contained capture has no successful result")
            expected_binding = workflow_coordination._result_binding(execution)
            safe._object(
                result,
                set(expected_binding)
                | {
                    "status",
                    "snapshot",
                    "source_state",
                    "observed_refs_before",
                    "observed_refs_after",
                    "head_event_id",
                    "worker_process",
                },
            )
            if any(result[key] != value for key, value in expected_binding.items()):
                _fail("EXECUTION", "contained result request binding differs")
            workflow_coordination._process(result["worker_process"])
            self._snapshot(Path(checked["source_root"]), checked, result["snapshot"])
            events = self._capture_events(run)
            expected_payloads = [
                {"lease_id": lease.lease_id},
                {
                    "source_state": checked["source_state"],
                    "observed_refs": result["observed_refs_before"],
                },
                {"snapshot": result["snapshot"]},
                {"ref": result["snapshot"]["ref"], "commit": result["snapshot"]["commit"]},
                {
                    "source_state": checked["source_state"],
                    "observed_refs": result["observed_refs_after"],
                },
            ]
            if (
                len(events) != 5
                or result.get("head_event_id") != events[-1]["event_id"]
                or result.get("source_state") != checked["source_state"]
                or [event["payload"] for event in events] != expected_payloads
                or result["worker_process"] == workflow_execution.current_process_identity()
                or _json(run / "worker_observation.json")
                != {
                    "status": "PASS",
                    "lease_id": lease.lease_id,
                    "execution_request_sha256": workflow_contract.canonical_digest(execution),
                    "worker_process": result["worker_process"],
                    "effect_authority": "SAME_SOURCE_ONLY_LEASE",
                }
            ):
                _fail("EXECUTION", "contained result is not bound to verified source events")
            lifecycle.record_result(
                lease.lease_id, actor=checked["actor"], result_path=Path(execution["result_path"])
            )
        except BaseException as exc:
            primary_error = exc
            if process is not None:
                # Failure cannot release the source lease while any child remains.
                operation = "terminate"
                try:
                    process.terminate()
                    operation = "confirm_exit"
                    lifecycle.confirm_exit(lease.lease_id, process, actor=checked["actor"])
                    operation = "record_incomplete_result"
                    lifecycle.record_incomplete_result(lease.lease_id, actor=checked["actor"])
                except BaseException as cleanup:
                    cleanup_errors.append(
                        {
                            "operation": operation,
                            "error_code": str(getattr(cleanup, "code", type(cleanup).__name__)),
                        }
                    )
        finally:
            if process is not None:
                try:
                    process.close()
                except BaseException as cleanup:
                    cleanup_errors.append(
                        {
                            "operation": "close",
                            "error_code": str(getattr(cleanup, "code", type(cleanup).__name__)),
                        }
                    )
                    if primary_error is None:
                        primary_error = cleanup
        if primary_error is not None:
            job_diagnostics = workflow_execution._job_process_list_diagnostics(primary_error)
            try:
                safe._write_once(
                    run / "execution_failure.json",
                    safe._json_bytes(
                        {
                            "schema_version": "task_checkpoint_execution_failure.v1",
                            "lease_id": lease.lease_id,
                            "checkpoint_request_sha256": execution["checkpoint_request_sha256"],
                            "primary_error_code": str(
                                getattr(primary_error, "code", type(primary_error).__name__)
                            ),
                            "cleanup_errors": cleanup_errors,
                            **({"job_process_list_diagnostics": job_diagnostics}
                               if job_diagnostics is not None else {}),
                        }
                    ),
                )
            except (OSError, ValueError):
                pass  # Diagnostic storage must not replace either execution failure.
            raise primary_error
        if result is None:
            _fail("EXECUTION", "contained result missing after exit")
        return result

    @_public
    def capture(self, request: Mapping[str, Any]) -> dict[str, Any]:
        checked = self._request(request)
        root = safe._configuration_path(Path(checked["source_root"]))
        run = safe._member(root, f"{RUNTIME}/{checked['checkpoint_id']}")
        receipt_path = run / "receipt.json"
        if run.exists():
            if (run / "failure.json").exists() or not receipt_path.is_file():
                _fail("PARTIAL", "incomplete checkpoint exists; automatic retry is forbidden")
            existing = _json(receipt_path)
            if existing.get("request_sha256") != safe._sha(safe._json_bytes(checked)):
                _fail("REF_EXISTS", "checkpoint id already belongs to another request")
            self.validate(receipt_path)
            return existing
        if os.name != "nt":
            _fail("ENVIRONMENT", "contained capture requires the reviewed Windows runtime")
        configuration = self.io._environment(root)
        implementation = self._implementation_binding(checked["implementation"])
        if safe._sha(safe._json_bytes(implementation)) != checked["implementation_sha256"]:
            _fail("DRIFT", "planned implementation differs")
        fence, source_policies = self._context(checked)
        grant = self._grant(checked, fence)
        state, captures, before_refs = self._state(checked, fence)
        if state != checked["source_state"] or state["files"] != checked["files"]:
            _fail("DRIFT", "source changed since plan")
        ref = REF_PREFIX + checked["checkpoint_id"]
        if self.io._git(root, "rev-parse", "--verify", "--quiet", ref, allowed=(0, 1)):
            _fail("REF_EXISTS", "create-only checkpoint ref already exists")
        handle: CheckoutLeaseHandle | None = None
        owned_run = False
        acquisition_started = False
        intent_id = ""
        events: list[dict[str, Any]] = []
        try:
            self.io._recheck_environment(root, configuration)
            attempt = self._install_attempt(run, checked)
            intent_id = attempt["intent_id"]
            owned_run = True
            # The kernel may persist ACTIVE and then raise before returning a
            # handle. The create-only attempt must already identify that lease.
            with fence.guard.store.atomic(
                actor=checked["actor"], now=datetime.now(UTC), operation="checkpoint-acquire"
            ):
                if (
                    self._attempt(run, checked, require_producer=True) != attempt
                    or attempt["producer"] != workflow_execution.current_process_identity()
                    or _bytes(run / "request.json") != safe._json_bytes(checked)
                    or (run / "interrupted_recovery.json").exists()
                ):
                    _fail("RECOVERY_PRODUCER", "original producer/request no longer owns acquire")
                acquisition_started = True
                decision, handle = fence.guard.acquire(
                    intent_id=intent_id,
                    task_id=checked["task_id"],
                    thread_id=checked["thread_id"],
                    actor=checked["actor"],
                    operation_class=CheckoutOperationClass.SHARED_MUTATION,
                    inspection_profile=CHECKOUT_SOURCE_ONLY_PROFILE,
                    shared_paths=tuple([row["path"] for row in checked["files"]] + [RUNTIME]),
                    base_commit=checked["source_head_sha"],
                )
            if decision.status != "PASS" or handle is None:
                _fail("LEASE", "task source lease unavailable")
            self.io._recheck_environment(root, configuration)
            self._event(run, events, "ACQUIRED", {"lease_id": handle.lease_id})
            safe._write_once(run / "checkpoint_policy.yaml", self.policy_bytes)
            intent_bytes = _bytes(decision.intent_path)
            safe._write_once(run / "checkpoint_intent.json", intent_bytes)
            index_path = Path(
                self.io._git(root, "rev-parse", "--path-format=absolute", "--git-path", "index")
                .decode()
                .strip()
            )
            original_index = _bytes(index_path)
            if safe._sha(original_index) != state["index_sha256"]:
                _fail("DRIFT", "source index changed during acquisition")
            safe._write_once(run / "source.index", original_index)
            for relative, content in {**source_policies, **grant}.items():
                safe._write_once(safe._member(run / "authority", relative), content)
            self._unchanged(checked, fence, implementation)
            worker_result = self._supervised_capture(checked, run, fence, handle, intent_bytes)
            snapshot = worker_result["snapshot"]
            before_refs = worker_result["observed_refs_before"]
            events = self._capture_events(run)
            after_refs = self._unchanged(checked, fence, implementation)
            lease_id = handle.lease_id
            self.io._recheck_environment(root, configuration)
            handle.release(outcome="completed", evidence_refs=((run / "events").as_posix(),))
            handle = None
            own_events = self._lease_bytes(fence, lease_id)
            self._intent_lease(checked, fence, intent_bytes, lease_id, own_events, checkpoint=True)
            for name, content in own_events.items():
                safe._write_once(run / "authority" / "checkpoint_lease" / name, content)
            after_refs = self._unchanged(checked, fence, implementation)
            self._event(
                run,
                events,
                "RELEASED",
                {"lease_id": lease_id, "lease_state": "RELEASED", "observed_refs": after_refs},
            )
            evidence = self._evidence_manifest(run)
            body = {
                "schema_version": "task_checkpoint_receipt.v2",
                "execution": self._execution_evidence(run),
                "status": "PASS",
                "checkpoint_id": checked["checkpoint_id"],
                "request_sha256": safe._sha(safe._json_bytes(checked)),
                "receipt_path": receipt_path.as_posix(),
                "implementation": implementation,
                "policy_sha256": safe._sha(self.policy_bytes),
                "lease_id": lease_id,
                "source_state_before": state,
                "source_state_after": state,
                "observed_refs_before": before_refs,
                "observed_refs_after": after_refs,
                "snapshot": snapshot,
                "head_event_id": events[-1]["event_id"],
                "evidence": evidence,
                "safety": dict(SAFETY),
            }
            receipt = {**body, "receipt_sha256": safe._sha(safe._json_bytes(body))}
            safe._write_once(receipt_path, safe._json_bytes(receipt))
            self.validate(receipt_path)
            return receipt
        except BaseException as exc:
            disposition, release_error = "NOT_ATTEMPTED", None
            if owned_run:
                try:
                    events = self._capture_events(run)
                except Exception:
                    pass  # Never replace the original failure with a diagnostic read.
            failed_lease_id = handle.lease_id if handle is not None else None
            if handle is not None:
                try:
                    self.io._recheck_environment(root, configuration)
                except Exception as error:
                    disposition, release_error = (
                        "DEFERRED_CONFIGURATION_RECHECK_FAILED",
                        getattr(error, "code", type(error).__name__),
                    )
                else:
                    try:
                        handle.release(outcome="failed")
                    except Exception as error:
                        disposition, release_error = (
                            "RELEASE_REJECTED_STATE_NOT_ASSERTED",
                            getattr(error, "code", type(error).__name__),
                        )
                    else:
                        disposition = "RELEASED"
            lease_state = "NOT_ATTEMPTED"
            replay_error = None
            if acquisition_started:
                lease_state = "UNKNOWN"
                if handle is None:
                    disposition = "DEFERRED_ACQUISITION_FAILED"
                try:
                    replay = fence.guard.replay()
                    matches = [
                        lease
                        for lease in replay.lease_heads
                        if lease.change_id == "checkout:" + intent_id
                    ]
                    if replay.status != "PASS" or len(matches) > 1:
                        _fail("LEASE", "failed attempt lease replay is ambiguous")
                    if matches:
                        lease = matches[0]
                        failed_lease_id = lease.lease_id
                        if (
                            lease.actor != checked["actor"]
                            or lease.base_commit != checked["source_head_sha"]
                            or lease.task_id != fence.guard.policy.authority_task_id
                        ):
                            _fail("LEASE", "failed attempt lease identity mismatch")
                        lease_state = lease.state
                        if lease_state == "RELEASED":
                            disposition = "RELEASED"
                    else:
                        lease_state = "NO_MATCHING_LEASE"
                        if handle is None:
                            disposition = "NO_MATCHING_LEASE_AT_REPLAY"
                except Exception as error:
                    replay_error = getattr(error, "code", type(error).__name__)
            if owned_run and not (run / "failure.json").exists():
                try:
                    safe._write_once(
                        run / "failure.json",
                        safe._json_bytes(
                            {
                                "schema_version": "task_checkpoint_failure.v1",
                                "status": "FAIL",
                                "request_sha256": safe._sha(safe._json_bytes(checked)),
                                "completed_phases": [event["phase"] for event in events],
                                "error_code": getattr(exc, "code", type(exc).__name__),
                                "attempt_intent_id": intent_id,
                                "lease_id": failed_lease_id,
                                "lease_state": lease_state,
                                "lease_replay_error_code": replay_error,
                                "release_disposition": disposition,
                                "release_error_code": release_error,
                                "safety": dict(SAFETY),
                            }
                        ),
                    )
                except (OSError, ValueError):
                    pass  # Preserve original cause if evidence storage itself failed.
            raise

    def _evidence_manifest(self, run: Path, *, partial: bool = False) -> list[dict[str, Any]]:
        paths = [
            run / "attempt.json",
            run / "request.json",
            run / "checkpoint_policy.yaml",
            run / "checkpoint_intent.json",
            run / "source.index",
        ]
        for name in (
            "capture_manifest.json",
            "object_intent.json",
            "execution_request.json",
            "worker_result.json",
            "worker.stdout.log",
            "worker_observation.json",
        ):
            if (run / name).exists():
                paths.append(run / name)
        folders = [run / "authority", run / "events"]
        if (run / "capture").exists():
            folders.append(run / "capture")
        for folder in folders:
            safe._configuration_path(folder)
            paths.extend(path for path in folder.rglob("*") if path.is_file())
        if partial:
            paths = [path for path in paths if path.exists()]
            paths.extend(
                run / name
                for name in ("failure.json", "execution_failure.json", "receipt.json")
                if (run / name).exists()
            )
        return [
            {
                "path": path.relative_to(run).as_posix(),
                "sha256": safe._sha(_bytes(path)),
                "size_bytes": path.stat().st_size,
            }
            for path in sorted(paths)
        ]

    @_public
    def validate(self, receipt_path: Path) -> dict[str, Any]:
        safe._configuration_path(receipt_path)
        if receipt_path.name == "terminal_recovery.json":
            return self._validate_terminal_recovery(receipt_path)
        if receipt_path.name == "interrupted_recovery.json":
            return self._validate_interrupted_recovery(receipt_path)
        if (
            receipt_path.name != "receipt.json"
            or receipt_path.parent.parent.name != "arch_005_task_checkpoints"
        ):
            _fail("RECEIPT", "exact checkpoint receipt locator required")
        run = receipt_path.parent
        if (run / "failure.json").exists():
            _fail("PARTIAL", "failed checkpoint is not a complete result")
        receipt = _json(receipt_path)
        return self._validate_receipt_payload(receipt_path, receipt)

    def _validate_receipt_payload(
        self, receipt_path: Path, receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Verify original terminal evidence without writing a replacement receipt.

        Recovery uses the same historical verifier, not the producer's PASS or
        a temporary file at the original receipt locator. A failure diagnostic
        remains intact; only independently complete terminal evidence can pass.
        """
        safe._configuration_path(receipt_path)
        if (
            receipt_path.name != "receipt.json"
            or receipt_path.parent.parent.name != "arch_005_task_checkpoints"
        ):
            _fail("RECEIPT", "exact checkpoint receipt locator required")
        run = receipt_path.parent
        expected_keys = {
            "schema_version",
            "status",
            "checkpoint_id",
            "request_sha256",
            "receipt_sha256",
            "receipt_path",
            "implementation",
            "policy_sha256",
            "lease_id",
            "source_state_before",
            "source_state_after",
            "observed_refs_before",
            "observed_refs_after",
            "snapshot",
            "head_event_id",
            "evidence",
            "safety",
        }
        if receipt.get("schema_version") == "task_checkpoint_receipt.v2":
            expected_keys.add("execution")
        safe._object(receipt, expected_keys)
        body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        if (
            receipt["schema_version"]
            not in {"task_checkpoint_receipt.v1", "task_checkpoint_receipt.v2"}
            or receipt["status"] != "PASS"
            or receipt["safety"] != SAFETY
            or safe._sha(safe._json_bytes(body)) != receipt["receipt_sha256"]
            or receipt["checkpoint_id"] != run.name
            or Path(receipt["receipt_path"]).resolve() != receipt_path.resolve()
        ):
            _fail("RECEIPT", "receipt schema/digest/identity/safety mismatch")
        request = self._request(_json(run / "request.json"))
        attempt = self._attempt(run, request)
        attempt_at = datetime.fromisoformat(attempt["created_at"])
        checkpoint_intent = parse_checkout_operation_intent(_json(run / "checkpoint_intent.json"))
        if (
            attempt["intent_id"] != checkpoint_intent.intent_id
            or attempt_at > checkpoint_intent.created_at
        ):
            _fail("RECEIPT", "create-only attempt/request/intent binding mismatch")
        if (
            receipt["request_sha256"] != safe._sha(safe._json_bytes(request))
            or receipt["source_state_before"] != request["source_state"]
            or receipt["source_state_after"] != request["source_state"]
            or receipt["snapshot"]["files"] is None
            or safe._sha(safe._json_bytes(receipt["implementation"]))
            != request["implementation_sha256"]
            or receipt["implementation"] != request["implementation"]
            or receipt["checkpoint_id"] != request["checkpoint_id"]
            or run.resolve()
            != (Path(request["source_root"]) / RUNTIME / request["checkpoint_id"]).resolve()
        ):
            _fail("RECEIPT", "request/source/implementation binding mismatch")
        if self._evidence_manifest(run) != receipt["evidence"]:
            _fail("RECEIPT", "frozen evidence inventory/bytes changed")
        policy_bytes = _bytes(run / "checkpoint_policy.yaml")
        if (
            safe._sha(policy_bytes) != receipt["policy_sha256"]
            or _policy(policy_bytes) != self.policy
        ):
            _fail("POLICY", "receipt requires its exact supported checkpoint policy")
        index = _bytes(run / "source.index")
        if safe._sha(index) != request["source_state"].get("index_sha256") or len(index) != request[
            "source_state"
        ].get("index_size_bytes"):
            _fail("RECEIPT", "original index evidence mismatch")
        # Execute all Git verification from the trusted checkout. The original task
        # may have continued editing; its current HEAD/config/bytes are not evidence.
        self.io._environment(self.project_root)
        self._verify_implementation(receipt["implementation"])
        common = Path(
            self.io._git(
                self.project_root, "rev-parse", "--path-format=absolute", "--git-common-dir"
            )
            .decode()
            .strip()
        ).resolve()
        if common != Path(request["source_common_git_dir"]).resolve():
            _fail("IDENTITY", "snapshot is not in the trusted common repository")
        fence, _ = self._context(request, archive=run / "authority")
        excluded = [row.path for row in fence.guard.policy.known_unrelated_exclusions]
        for row in request["files"]:
            _source_path(row["path"], self.policy, excluded)
        scope_bytes = _bytes(run / "authority/scope_intent.json")
        if safe._sha(scope_bytes) != request["scope_intent"]["sha256"]:
            _fail("SCOPE", "frozen scope grant mismatch")
        for checkpoint, lease_id, intent_bytes, folder in (
            (False, request["scope_intent"]["lease_id"], scope_bytes, "scope_lease"),
            (True, receipt["lease_id"], _bytes(run / "checkpoint_intent.json"), "checkpoint_lease"),
        ):
            lease_events = {
                path.name: _bytes(path)
                for path in sorted((run / "authority" / folder).glob("*.json"))
            }
            self._intent_lease(
                request, fence, intent_bytes, lease_id, lease_events, checkpoint=checkpoint
            )
        self._validate_capture_execution(run, request, receipt)
        events: list[dict[str, Any]] = []
        for number, phase in enumerate(_PHASES, 1):
            event = _json(run / "events" / f"{number:02d}-{phase}.json")
            safe._object(
                event,
                {
                    "schema_version",
                    "checkpoint_id",
                    "sequence",
                    "phase",
                    "occurred_at",
                    "previous_event_id",
                    "payload",
                    "event_id",
                },
            )
            content = {key: value for key, value in event.items() if key != "event_id"}
            instant = datetime.fromisoformat(event["occurred_at"])
            if (
                event["schema_version"] != "task_checkpoint_event.v1"
                or event["checkpoint_id"] != run.name
                or event["sequence"] != number
                or event["phase"] != phase
                or event["previous_event_id"] != (events[-1]["event_id"] if events else None)
                or event["event_id"] != safe._sha(safe._json_bytes(content))
                or instant.tzinfo is None
                or instant < attempt_at
                or (events and instant < datetime.fromisoformat(events[-1]["occurred_at"]))
            ):
                _fail("RECEIPT", "checkpoint event chronology/hash chain mismatch")
            events.append(event)
        expected_payloads = [
            {"lease_id": receipt["lease_id"]},
            {
                "source_state": request["source_state"],
                "observed_refs": receipt["observed_refs_before"],
            },
            {"snapshot": receipt["snapshot"]},
            {"ref": receipt["snapshot"]["ref"], "commit": receipt["snapshot"]["commit"]},
        ]
        if any(
            event["payload"] != payload
            for event, payload in zip(events[:4], expected_payloads, strict=True)
        ):
            _fail("RECEIPT", "event payload does not bind checkpoint")
        if (
            events[4]["payload"].get("source_state") != request["source_state"]
            or events[5]["payload"]
            != {
                "lease_id": receipt["lease_id"],
                "lease_state": "RELEASED",
                "observed_refs": receipt["observed_refs_after"],
            }
            or receipt["head_event_id"] != events[-1]["event_id"]
        ):
            _fail("RECEIPT", "verification/release proof mismatch")
        self._snapshot(self.project_root, request, receipt["snapshot"])
        # Old completed receipts remain independently verifiable. Recovery must
        # require these durable materials explicitly; their absence grants no
        # recapture, rerun, task writer or publication capability.
        if (run / "capture_manifest.json").exists():
            self._read_capture_bundle(run, request)
        if (run / "object_intent.json").exists():
            self._validate_object_intent(run, request, receipt["snapshot"])
        return {
            "schema_version": "task_checkpoint_validation.v1",
            "status": "PASS",
            "checkpoint_id": request["checkpoint_id"],
            "receipt_sha256": receipt["receipt_sha256"],
            "snapshot_commit": receipt["snapshot"]["commit"],
            "safety": dict(SAFETY),
        }

    def _terminal_receipt(self, run: Path, request: dict[str, Any]) -> dict[str, Any]:
        names = {f"{number:02d}-{phase}.json" for number, phase in enumerate(_PHASES, 1)}
        folder = safe._configuration_path(run / "events")
        if not folder.is_dir() or {path.name for path in folder.iterdir()} != names:
            _fail("RECOVERY_NOT_TERMINAL", "all six exact original terminal events required")
        if not all(
            (run / name).is_file() for name in ("capture_manifest.json", "object_intent.json")
        ):
            _fail("RECOVERY_MATERIAL_MISSING", "durable capture and object intent required")
        events = [
            _json(folder / f"{number:02d}-{phase}.json") for number, phase in enumerate(_PHASES, 1)
        ]
        body = {
            "schema_version": "task_checkpoint_receipt.v1",
            "status": "PASS",
            "checkpoint_id": request["checkpoint_id"],
            "request_sha256": safe._sha(safe._json_bytes(request)),
            "receipt_path": (run / "receipt.json").as_posix(),
            "implementation": request["implementation"],
            "policy_sha256": safe._sha(_bytes(run / "checkpoint_policy.yaml")),
            "lease_id": events[0]["payload"]["lease_id"],
            "source_state_before": request["source_state"],
            "source_state_after": request["source_state"],
            "observed_refs_before": events[1]["payload"]["observed_refs"],
            "observed_refs_after": events[5]["payload"]["observed_refs"],
            "snapshot": events[2]["payload"]["snapshot"],
            "head_event_id": events[-1]["event_id"],
            "evidence": self._evidence_manifest(run),
            "safety": dict(SAFETY),
        }
        if (run / "execution_request.json").exists():
            body["schema_version"] = "task_checkpoint_receipt.v2"
            body["execution"] = self._execution_evidence(run)
        receipt = {**body, "receipt_sha256": safe._sha(safe._json_bytes(body))}
        self._validate_receipt_payload(run / "receipt.json", receipt)
        return receipt

    def _validate_terminal_recovery(self, path: Path) -> dict[str, Any]:
        if path.parent.parent.name != "arch_005_task_checkpoints":
            _fail("RECEIPT", "exact terminal recovery locator required")
        value = _json(path)
        safe._object(
            value,
            {
                "schema_version",
                "status",
                "action",
                "actor",
                "recovery_path",
                "recovered_receipt",
                "safety",
                "recovery_sha256",
            },
        )
        body = {key: item for key, item in value.items() if key != "recovery_sha256"}
        request = self._request(_json(path.parent / "request.json"))
        if (
            value["schema_version"] != "task_checkpoint_terminal_recovery.v1"
            or value["status"] != "PASS"
            or value["action"] != "RECEIPT_RECONSTRUCTION_ONLY"
            or value["actor"] != request["actor"]
            or value["safety"] != SAFETY
            or value["recovery_path"] != path.as_posix()
            or value["recovery_sha256"] != safe._sha(safe._json_bytes(body))
            or value["recovered_receipt"] != self._terminal_receipt(path.parent, request)
        ):
            _fail("RECOVERY_RECEIPT", "recovery identity or original terminal evidence differs")
        verified = self._validate_receipt_payload(
            path.parent / "receipt.json", value["recovered_receipt"]
        )
        return {
            **verified,
            "schema_version": "task_checkpoint_terminal_recovery_validation.v1",
            "action": value["action"],
            "recovery_path": path.as_posix(),
            "recovery_sha256": value["recovery_sha256"],
        }

    @staticmethod
    def _install_terminal_recovery(path: Path, content: bytes) -> None:
        # Windows rename refuses an existing destination. Do not substitute
        # os.replace, shutil.move, or the overwriting POSIX rename semantics.
        # https://docs.python.org/3.11/library/os.html#os.rename
        if os.name != "nt":
            _fail("RECOVERY_PLATFORM", "create-only installation is validated on Windows only")
        if len(content) > _MAX_METADATA_BYTES:
            _fail("BUDGET", "terminal recovery metadata exceeds bounded size")
        safe._configuration_path(path)
        staging = path.with_name(f"{path.stem}.staging-{uuid4().hex}.json")
        safe._write_once(staging, content)
        if _bytes(staging) != content:
            _fail("RECOVERY_DRIFT", "staged recovery bytes changed before installation")
        safe._configuration_path(path)
        # An interrupted staging file is non-authoritative diagnostic evidence.
        # A later explicit call creates one new stage; it never overwrites,
        # interprets, or deletes partial bytes from an interrupted invocation.
        os.rename(staging, path)

    @_public
    def recover_terminal(self, request: Mapping[str, Any], *, actor: str) -> dict[str, Any]:
        """Reconstruct only a separately stored terminal receipt, never rerun.

        No PID/TTL inference is needed for this path: the original successful
        lease release and all source/ref side effects are already independently
        proved. Even a producer still returning from its last event cannot have
        its receipt or failure overwritten here. Earlier phases are not admitted.
        """
        checked = self._request(request)
        if actor != checked["actor"]:
            _fail("RECOVERY_ACTOR", "exact original actor required")
        run = safe._member(Path(checked["source_root"]), f"{RUNTIME}/{checked['checkpoint_id']}")
        if self._request(_json(run / "request.json")) != checked:
            _fail("RECOVERY_REQUEST", "complete original request required")
        self._implementation_binding()
        receipt = self._terminal_receipt(run, checked)
        path = run / "terminal_recovery.json"
        body = {
            "schema_version": "task_checkpoint_terminal_recovery.v1",
            "status": "PASS",
            "action": "RECEIPT_RECONSTRUCTION_ONLY",
            "actor": actor,
            "recovery_path": path.as_posix(),
            "recovered_receipt": receipt,
            "safety": dict(SAFETY),
        }
        value = {**body, "recovery_sha256": safe._sha(safe._json_bytes(body))}
        fence, _ = self._context(checked)
        # Reuse the actual store arbiter. This does not acquire/expire/release
        # a lease, alter events, or dispatch source capture or Git mutation.
        try:
            with fence.guard.store.atomic(
                actor=actor, now=datetime.now(UTC), operation="checkpoint-terminal-recovery"
            ):
                live = self._lease_bytes(fence, receipt["lease_id"])
                archive = {
                    item.name: _bytes(item)
                    for item in (run / "authority" / "checkpoint_lease").glob("*.json")
                }
                if not live or live != archive:
                    _fail(
                        "RECOVERY_LEASE",
                        "actual successful lease history must match archived proof",
                    )
                if self._evidence_manifest(run) != receipt["evidence"]:
                    _fail("RECOVERY_DRIFT", "original evidence changed before recovery write")
                if path.exists():
                    if _json(path) != value:
                        _fail(
                            "RECOVERY_CONFLICT", "existing recovery belongs to different evidence"
                        )
                else:
                    self._install_terminal_recovery(path, safe._json_bytes(value))
        except ParallelControlError as exc:
            if exc.code != "LEASE_ARBITER_BUSY":
                raise
            # This is an observation of the real OS arbiter, not a claim that
            # another recovery of this request is running or that it succeeded.
            return {
                "schema_version": "task_checkpoint_terminal_recovery_wait.v1",
                "status": "WAITING_FOR_ARBITER",
                "action": "NONE",
                "request_sha256": safe._sha(safe._json_bytes(checked)),
                "recovery_path": path.as_posix(),
                "next_action": "RETRY_SAME_RECOVERY_REQUEST_AFTER_ARBITER_RELEASE",
                "safety": dict(SAFETY),
            }
        return self._validate_terminal_recovery(path)

    def _interrupted_lease(
        self,
        run: Path,
        request: dict[str, Any],
        attempt: dict[str, Any],
        fence: Any,
        *,
        check_unmatched_active: bool = True,
    ) -> tuple[Any, dict[str, bytes]]:
        replay = fence.guard.store.replay()
        if replay.status != "PASS":
            _fail("RECOVERY_LEASE", "actual lease history is invalid")
        matches = [
            head
            for head in replay.lease_heads
            if head.change_id == "checkout:" + attempt["intent_id"]
        ]
        if len(matches) > 1:
            _fail("RECOVERY_LEASE", "original attempt has ambiguous lease generations")
        if not matches:
            # A changed attempt cannot hide an already acquired checkpoint lease.
            # The shared runtime scope makes these captures mutually exclusive.
            binding = fence.guard.store.coordination_binding
            runtime_scopes = (RUNTIME,) if binding is None else binding.scoped_paths(RUNTIME)
            potential_original = any(
                head.change_id.startswith("checkout:task-checkpoint-")
                and any(
                    claim.kind == "path"
                    and any(
                        safe._under(claim.resource_id.casefold(), scope.casefold())
                        or safe._under(scope.casefold(), claim.resource_id.casefold())
                        for scope in runtime_scopes
                    )
                    for claim in head.resources
                )
                for head in replay.active_leases
            )
            if (run / "events/01-ACQUIRED.json").exists() or (
                check_unmatched_active and potential_original
            ):
                _fail("RECOVERY_LEASE", "no-match observation conflicts with capture authority")
            return None, {}
        head = matches[0]
        if head.state not in {"ACTIVE", "RELEASED"}:
            _fail("RECOVERY_LEASE", "expiry/reassignment is not an original release proof")
        intent_path = fence.guard.runtime_root / "intents" / f"{attempt['intent_id']}.json"
        intent_bytes = _bytes(intent_path)
        intent = parse_checkout_operation_intent(_json(intent_path))
        if (
            intent.intent_id != attempt["intent_id"]
            or datetime.fromisoformat(attempt["created_at"]) > intent.created_at
            or (run / "checkpoint_intent.json").exists()
            and _bytes(run / "checkpoint_intent.json") != intent_bytes
        ):
            _fail("RECOVERY_LEASE", "original attempt/intent binding differs")
        events = self._lease_bytes(fence, head.lease_id)
        terminal = next(
            parse_lease_event(load_strict_json_text(content.decode("utf-8")))
            for name, content in events.items()
            if name == dict(replay.head_event_ids)[head.lease_id] + ".json"
        )
        outcome = "completed"
        if head.state == "RELEASED":
            if terminal.reason_codes == ("CHECKOUT_OPERATION_FAILED",):
                outcome = "failed"
            elif terminal.reason_codes != ("CHECKOUT_OPERATION_COMPLETED",):
                _fail("RECOVERY_LEASE", "unsupported original terminal reason")
        self._intent_lease(
            request,
            fence,
            intent_bytes,
            head.lease_id,
            events,
            checkpoint=True,
            expected_state=head.state,
            released_outcome=outcome,
        )
        captured_events, _ = self._recovery_capture_events(run)
        if captured_events and captured_events[0]["payload"] != {"lease_id": head.lease_id}:
            _fail("RECOVERY_LEASE", "original capture event names another lease")
        if head.execution is not None:
            frozen = workflow_coordination._request(_json(run / "execution_request.json"))
            expected = {
                **self._execution_subject(request, intent_bytes),
                "lease_id": head.lease_id,
                "manifest_sha256": head.change_manifest_sha256,
                "subject_task_id": head.task_id,
            }
            if (
                head.execution["launcher"] != attempt["producer"]
                or head.execution["request"] != frozen
                or any(frozen.get(key) != value for key, value in expected.items())
            ):
                _fail(
                    "RECOVERY_PRODUCER", "actual execution does not bind original producer/request"
                )
        return head, events

    def _interrupted_body(
        self, run: Path, request: dict[str, Any], attempt: dict[str, Any], fence: Any
    ) -> dict[str, Any]:
        # Later unrelated active captures cannot invalidate an already terminal
        # original identity. New recovery admission performs the scoped check.
        head, events = self._interrupted_lease(
            run, request, attempt, fence, check_unmatched_active=False
        )
        if head is not None and (
            head.state != "RELEASED"
            or head.execution is not None
            and head.execution["state"] != "RESULT_RECORDED"
        ):
            _fail("RECOVERY_NOT_TERMINAL", "original lease/execution is not terminal")
        body = {
            "schema_version": "task_checkpoint_interrupted_recovery.v1",
            "status": "INSUFFICIENT",
            "checkpoint_status": "INSUFFICIENT",
            "action": "FAILED_ATTEMPT_TERMINAL_ONLY",
            "dispatch_allowed": False,
            "actor": request["actor"],
            "request_sha256": safe._sha(safe._json_bytes(request)),
            "attempt_sha256": safe._sha(_bytes(run / "attempt.json")),
            "producer": attempt["producer"],
            "reason": "NO_ACQUIRED_LEASE" if head is None else "ORIGINAL_ATTEMPT_TERMINAL",
            "lease_id": None if head is None else head.lease_id,
            "lease_state": None if head is None else head.state,
            "lease_events": [
                {"name": name, "sha256": safe._sha(content)}
                for name, content in sorted(events.items())
            ],
            "execution_result": (
                None if head is None or head.execution is None else head.execution["result"]
            ),
            "evidence": self._evidence_manifest(run, partial=True),
            "capture_events": self._recovery_capture_events(run)[1],
            "recovery_path": (run / "interrupted_recovery.json").as_posix(),
            "safety": dict(SAFETY),
        }
        return {**body, "recovery_sha256": safe._sha(safe._json_bytes(body))}

    def _recovery_capture_events(self, run: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        try:
            events = self._capture_events(run)
        except (ValueError, OSError, KeyError, TypeError) as exc:
            # Optional partial capture evidence is preserved byte-for-byte by
            # the recovery inventory. It cannot supply a successful phase or
            # lease identity; only the actual attempt/intent/store can release.
            return [], {
                "status": "INVALID",
                "completed_phases": [],
                "reason_code": str(getattr(exc, "code", type(exc).__name__)),
            }
        return events, {
            "status": "VALID_PREFIX",
            "completed_phases": [event["phase"] for event in events],
            "reason_code": None,
        }

    def _validate_interrupted_recovery(self, path: Path) -> dict[str, Any]:
        if path.parent.parent.name != "arch_005_task_checkpoints":
            _fail("RECEIPT", "exact interrupted recovery locator required")
        request = self._request(_json(path.parent / "request.json"))
        self._verify_implementation(request["implementation"])
        attempt = self._attempt(path.parent, request, require_producer=True)
        if workflow_execution.observe_process(**attempt["producer"])["state"] not in {
            "EXITED",
            "REUSED",
        }:
            _fail("RECOVERY_PRODUCER", "original producer exit is not independently proved")
        fence, _ = self._context(request)
        expected = self._interrupted_body(path.parent, request, attempt, fence)
        if _json(path) != expected:
            _fail("RECOVERY_CONFLICT", "original interrupted recovery evidence differs")
        return expected

    @_public
    def recover_interrupted(
        self, request: Mapping[str, Any], *, actor: str, action: str = "observe"
    ) -> dict[str, Any]:
        """Close a failed attempt without capture, ref mutation or redispatch."""
        checked = self._request(request)
        if actor != checked["actor"]:
            _fail("RECOVERY_ACTOR", "exact original actor required")
        if action not in {"observe", "terminate_frozen_job"}:
            _fail("RECOVERY_ACTION", "unsupported interrupted recovery action")
        run = safe._member(Path(checked["source_root"]), f"{RUNTIME}/{checked['checkpoint_id']}")
        if self._request(_json(run / "request.json")) != checked:
            _fail("RECOVERY_REQUEST", "complete original request required")
        self._implementation_binding()
        self._verify_implementation(checked["implementation"])
        if (run / "receipt.json").exists() and not (run / "failure.json").exists():
            return self.validate(run / "receipt.json")
        if len(self._recovery_capture_events(run)[0]) == len(_PHASES):
            return self.recover_terminal(checked, actor=actor)
        attempt = self._attempt(run, checked, require_producer=True)
        producer = workflow_execution.observe_process(**attempt["producer"])
        waiting = {
            "schema_version": "task_checkpoint_interrupted_recovery_wait.v1",
            "status": "OBSERVE_ONLY",
            "action": "NONE",
            "dispatch_allowed": False,
            "request_sha256": safe._sha(safe._json_bytes(checked)),
            "safety": dict(SAFETY),
        }
        # Observing a live producer does not need its arbiter and grants no write.
        if producer["state"] not in {"EXITED", "REUSED"}:
            return {**waiting, "producer": producer}
        fence, _ = self._context(checked)
        path = run / "interrupted_recovery.json"

        def recheck() -> None:
            if (
                self._attempt(run, checked, require_producer=True) != attempt
                or _bytes(run / "request.json") != safe._json_bytes(checked)
                or workflow_execution.observe_process(**attempt["producer"])["state"]
                not in {"EXITED", "REUSED"}
            ):
                _fail("RECOVERY_PRODUCER", "producer/request changed before recovery effect")

        try:
            with fence.guard.store.atomic(
                actor=actor, now=datetime.now(UTC), operation="execution_complete"
            ):
                recheck()
                if path.exists():
                    return self._validate_interrupted_recovery(path)
                head, _ = self._interrupted_lease(run, checked, attempt, fence)
                evidence = self._evidence_manifest(run, partial=True)
            if head is not None and head.execution is not None and head.state == "ACTIVE":
                recovered = fence.guard.store.execution_lifecycle().recover(
                    head.lease_id, actor=actor, action=action
                )
                if recovered["status"] not in {"REPLAY_ONLY", "RECOVERED_TERMINAL"}:
                    return {**waiting, "execution_observation": recovered}
            with fence.guard.store.atomic(
                actor=actor, now=datetime.now(UTC), operation="execution_complete"
            ):
                recheck()
                head, _ = self._interrupted_lease(run, checked, attempt, fence)
                if self._evidence_manifest(run, partial=True) != evidence:
                    _fail("RECOVERY_DRIFT", "original capture evidence changed during recovery")
                if head is not None and head.state == "ACTIVE":
                    if head.execution is not None and head.execution["state"] != "RESULT_RECORDED":
                        _fail("RECOVERY_NOT_TERMINAL", "actual execution is not terminal")
                    fence.guard.release(head.lease_id, actor=actor, outcome="failed")
                value = self._interrupted_body(run, checked, attempt, fence)
                if path.exists():
                    if _json(path) != value:
                        _fail(
                            "RECOVERY_CONFLICT", "existing recovery belongs to different evidence"
                        )
                else:
                    self._install_terminal_recovery(path, safe._json_bytes(value))
        except ParallelControlError as exc:
            if exc.code != "LEASE_ARBITER_BUSY":
                raise
            return {**waiting, "status": "WAITING_FOR_ARBITER"}
        return self._validate_interrupted_recovery(path)
