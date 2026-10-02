"""DEVX-015 Windows execution containment, not a second lease authority.

The existing lease store reserves execution before this backend is called.
JOB_LIST binds the process at creation, before any thread can run; a later
AssignProcessToJobObject would leave a create/assign crash window. No breakaway
or ordinary-Popen fallback is allowed. See Microsoft's UpdateProcThreadAttribute
and Job Objects contracts. Only actual Windows/Python 3.11 is supported here.
"""

from __future__ import annotations

import ast
import ctypes
import functools
import gc
import hashlib
import importlib.metadata
import importlib.util
import json
import marshal
import math
import os
import re
import stat
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from ctypes import wintypes as w
from pathlib import Path
from types import CodeType, FunctionType, ModuleType
from typing import TYPE_CHECKING, Any, BinaryIO

if TYPE_CHECKING:
    from ai_trading_system.platform.architecture.source_preservation import HeldGitConfiguration
    from ai_trading_system.platform.architecture.workflow_contract import _BoundReadFileCustody

# Win32 ABI constants, not configurable policy or investment thresholds.
_CREATE_SUSPENDED = 0x4
_CREATE_NO_WINDOW = 0x08000000
_EXTENDED_STARTUPINFO_PRESENT = 0x80000
_CREATE_UNICODE_ENVIRONMENT = 0x400
_PROC_THREAD_ATTRIBUTE_JOB_LIST = 0x2000D
_PROC_THREAD_ATTRIBUTE_HANDLE_LIST = 0x20002
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
_JOB_QUERY_TERMINATE = 0x0004 | 0x0008
_PROCESS_QUERY_SYNCHRONIZE = 0x1000 | 0x100000
_WAIT_TIMEOUT = 258
_INFINITE = 0xFFFFFFFF
_STILL_ACTIVE = 259
_ABORTED = 1067

# Bounded wait for the OS to confirm that a terminated Job/process tree is gone. This is
# a hang detector, not a permission to skip termination: on expiry the caller still fails
# closed with CLEANUP_UNCONFIRMED. DEVX-018: after TerminateJobObject the active-process
# count reached 0 while retained process handles stayed unsignaled for more than the
# former 10s under formal-Full load (16 outer workers plus nested Fulls, Defender I/O).
# Provisional load calibration pending owner review; exit condition and rationale are in
# docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md (v15 pilot section).
CONTAINED_TERMINATION_CONFIRMATION_SECONDS = 180.0


class _CredentialBlob(ctypes.Structure):
    _fields_ = [("length", w.DWORD), ("data", ctypes.c_void_p)]


@contextmanager
def decrypted_worker_password(encrypted: bytes, binding: bytes) -> Iterator[Any]:
    """Hold DPAPI plaintext only in a native allocation, then wipe and free it.

    The caller must hold the confidential file and protected identity custody.
    This primitive neither authorizes an identity nor reads a credential store.
    The yielded writable array must never escape the context or be serialized.
    """
    if (type(encrypted) is not bytes or not 0 < len(encrypted) <= 16384
            or type(binding) is not bytes or len(binding) != 32):
        raise ExecutionContainmentError("CREDENTIAL_ENVELOPE_INVALID")
    crypt = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    crypt.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_CredentialBlob), ctypes.c_void_p,
        ctypes.POINTER(_CredentialBlob), ctypes.c_void_p, ctypes.c_void_p,
        w.DWORD, ctypes.POINTER(_CredentialBlob),
    ]
    crypt.CryptUnprotectData.restype = w.BOOL
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    source = ctypes.create_string_buffer(encrypted)
    entropy = ctypes.create_string_buffer(binding)
    incoming = _CredentialBlob(len(encrypted), ctypes.addressof(source))
    salt = _CredentialBlob(len(binding), ctypes.addressof(entropy))
    outgoing = _CredentialBlob()
    try:
        if not crypt.CryptUnprotectData(
            ctypes.byref(incoming), None, ctypes.byref(salt), None, None, 1,
            ctypes.byref(outgoing),
        ):
            raise ExecutionContainmentError("CREDENTIAL_DECRYPT_FAILED")
        if (not outgoing.data or not 4 <= outgoing.length <= 2050
                or outgoing.length % ctypes.sizeof(ctypes.c_wchar)):
            raise ExecutionContainmentError("CREDENTIAL_PLAINTEXT_INVALID")
        secret = (ctypes.c_wchar * (outgoing.length // ctypes.sizeof(ctypes.c_wchar))
                  ).from_address(outgoing.data)
        if secret[-1] != "\0" or any(secret[i] == "\0" for i in range(len(secret) - 1)):
            raise ExecutionContainmentError("CREDENTIAL_PLAINTEXT_INVALID")
        yield secret
    finally:
        if outgoing.data:
            ctypes.memset(outgoing.data, 0, outgoing.length)
            kernel.LocalFree(outgoing.data)


class ExecutionContainmentError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = "WORKFLOW_EXECUTION_" + code
        self._job_process_list_diagnostics: dict[str, Any] | None = None
        super().__init__(f"{self.code}: {detail}")


def _job_process_list_diagnostics(error: BaseException) -> dict[str, Any] | None:
    """Return only bounded native counters, never arbitrary exception text."""
    if (type(error) is not ExecutionContainmentError
            or error.code != "WORKFLOW_EXECUTION_JOB_PROCESS_LIST_BUDGET"):
        return None
    value = getattr(error, "_job_process_list_diagnostics", None)
    base_fields = {"queries", "retained_process_count"}
    if type(value) is not dict or set(value) not in (
        base_fields | {"active_process_count"}, base_fields | {"accounting_error_code"},
    ):
        return None

    def native_count(item: object) -> bool:
        return type(item) is int and 0 <= item <= 0xFFFFFFFF

    # Capacity doubles from 16 through 65536: at most 13 observations.
    queries = value["queries"]
    if (type(queries) is not list or not 1 <= len(queries) <= 13
            or not native_count(value["retained_process_count"])):
        return None
    for row in queries:
        if (type(row) is not dict
                or set(row) != {"capacity", "ok", "winerror", "assigned", "count"}
                or type(row["ok"]) is not bool
                or any(not native_count(row[key])
                       for key in ("capacity", "winerror", "assigned", "count"))
                or not 16 <= row["capacity"] <= 65536):
            return None
    if "active_process_count" in value:
        if not native_count(value["active_process_count"]):
            return None
    elif (type(value["accounting_error_code"]) is not str
          or value["accounting_error_code"] != "WORKFLOW_EXECUTION_JOB_QUERY"):
        return None
    return {**value, "queries": [dict(row) for row in queries], "observation_only": True}


DEVX015_ACCEPTANCE_TASK = "DEVX-015_TASK_CHECKPOINT_AND_PUBLICATION_SEPARATION_V2"
_ACCEPTANCE_MANIFEST = "config/architecture/devx_015_workflow_acceptance.v1.json"
# Independent approved V3 inventory commitment, not a checksum supplied by the manifest.
_ACCEPTANCE_VARIANTS_SHA256 = "2644840676db743e4aa8aa4b8a3d05b8811654e7525b42b0be4b2892625d8d67"
MANDATORY_ACCEPTANCE_REQUEST_ENV = "AITS_MANDATORY_ACCEPTANCE_REQUEST"
# These are pytest/xdist observations, not configuration inputs. Versions are bound below.
_ACCEPTANCE_OBSERVATION_ENV = frozenset(
    {
        MANDATORY_ACCEPTANCE_REQUEST_ENV,
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "PYTEST_XDIST_TESTRUNUID",
    }
)


def acceptance_runtime_identity(
    environment: Mapping[str, str] | None = None,
    *,
    captured_dependencies: dict[Path, bytes] | None = None,
    observe_dependency: Callable[[Path, bytes], None] | None = None,
) -> dict[str, Any]:
    """Bind interpreter, installed code bytes, authored loaded code and effective env.

    Runtime-generated assignments and native in-memory implementation evidence
    remain distinct from these authored Python declaration checks. An optional
    observer receives every actually read interpreter/distribution input on this
    calling thread. It does not itself attest retention, Full, or execution rights.
    """
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
    )

    if observe_dependency is not None and not callable(observe_dependency):
        raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_OBSERVER")
    executable = Path(sys.executable).absolute()
    try:
        content = bounded_regular_bytes(executable)
        if os.name != "nt" or not hasattr(sys, "dllhandle"):
            raise ExecutionContainmentError("ACCEPTANCE_INTERPRETER_PLATFORM")
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetModuleFileNameW.argtypes = [w.HMODULE, w.LPWSTR, w.DWORD]
        kernel.GetModuleFileNameW.restype = w.DWORD
        buffer = ctypes.create_unicode_buffer(32768)
        length = kernel.GetModuleFileNameW(sys.dllhandle, buffer, len(buffer))
        if not 0 < length < len(buffer):
            raise ExecutionContainmentError("ACCEPTANCE_INTERPRETER_MODULE")
        engine = Path(buffer.value).absolute()
        engine_content = bounded_regular_bytes(engine)
    except (OSError, WorkflowContractError) as exc:
        raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CUSTODY", str(exc)) from exc
    if observe_dependency is not None:
        observe_dependency(executable, content)
        observe_dependency(engine, engine_content)
    distributions = list(importlib.metadata.distributions())
    packages = sorted(
        (str(item.metadata["Name"]), item.version, str(Path(str(item.locate_file(""))).absolute()))
        for item in distributions
    )
    source_inputs: dict[Path, bytes] = (
        {} if captured_dependencies is None else captured_dependencies
    )
    source_inputs.clear()
    module_origins: dict[str, set[Path]] = {}
    dependency_code = _acceptance_distribution_code(
        distributions, source_inputs=source_inputs, module_origins=module_origins,
        observe_dependency=observe_dependency,
    )
    _verify_loaded_python_sources(
        {path: raw for path, raw in source_inputs.items() if path.suffix == ".py"},
        module_origins=module_origins,
        cache_inputs={path: raw for path, raw in source_inputs.items() if path.suffix == ".pyc"},
    )
    selected = os.environ if environment is None else environment
    return {
        "schema_version": "acceptance_runtime_identity.v1",
        "executable": str(executable),
        "executable_sha256": hashlib.sha256(content).hexdigest(),
        "engine": str(engine),
        "engine_sha256": hashlib.sha256(engine_content).hexdigest(),
        "python_version": sys.version,
        "implementation": sys.implementation.name,
        "prefix": str(Path(sys.prefix).absolute()),
        "base_prefix": str(Path(sys.base_prefix).absolute()),
        "platform": sys.platform,
        "distribution_inventory_sha256": hashlib.sha256(json.dumps(packages).encode()).hexdigest(),
        "distribution_count": len(packages),
        "distribution_code": dependency_code,
        "environment_sha256": execution_environment_sha256(
            {
                name: value
                for name, value in selected.items()
                if name not in _ACCEPTANCE_OBSERVATION_ENV
            }
        ),
    }


@contextmanager
def hold_acceptance_runtime_identity(
    environment: Mapping[str, str] | None = None,
) -> Iterator[tuple[dict[str, Any], tuple[_BoundReadFileCustody, ...]]]:
    """Retain every original runtime read with native file/parent handles until exit.

    Roots come only from this interpreter and its installed distribution origins.
    The caller must still compare the identity with the original Full and bind
    its own live Job/attempt. Neither the returned identity nor tuple grants that
    authority; serialized bindings cannot replace these live custody objects.
    """
    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_read_file
    from ai_trading_system.platform.architecture.workflow_coordination import directory_identity

    roots = {Path(sys.prefix).absolute(), Path(sys.base_prefix).absolute(), *[
        Path(str(distribution.locate_file(""))).absolute()
        for distribution in importlib.metadata.distributions()
    ]}
    ordered_roots = sorted(roots, key=lambda root: (-len(root.parts), str(root)))
    # These are expected identities, not a read/authority cache. Each native hold
    # rechecks them through RootDirectory-relative handles; the first successful
    # hold also retains the chain while later files are acquired.
    identities: dict[Path, tuple[int, int]] = {}
    custodies: list[_BoundReadFileCustody] = []

    def directory(path: Path) -> tuple[int, int]:
        if path not in identities:
            info = directory_identity(path)
            identities[path] = (info["device"], info["file_id"])
        return identities[path]

    with ExitStack() as stack:
        def retain(path: Path, raw: bytes) -> None:
            matches = [root for root in ordered_roots if path.is_relative_to(root)]
            if not matches:
                raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CUSTODY_ROOT")
            root = matches[0]
            relative = path.relative_to(root)
            parents = {
                "/".join(relative.parts[:position]): directory(
                    root.joinpath(*relative.parts[:position])
                )
                for position in range(1, len(relative.parts))
            }
            info = path.stat()
            custodies.append(stack.enter_context(hold_bound_read_file(
                root, relative.as_posix(), expected=raw,
                expected_identity=(info.st_dev, info.st_ino),
                expected_root_identity=directory(root), expected_parent_identities=parents,
                budget=64 * 1024 * 1024,
            )))

        identity = acceptance_runtime_identity(environment, observe_dependency=retain)
        if len(custodies) != identity["distribution_code"]["file_count"] + 2:
            raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CUSTODY_INVENTORY")
        yield identity, tuple(custodies)


def _acceptance_distribution_code(
    distributions: list[importlib.metadata.Distribution],
    *,
    source_inputs: dict[Path, bytes] | None = None,
    module_origins: dict[str, set[Path]] | None = None,
    observe_dependency: Callable[[Path, bytes], None] | None = None,
) -> dict[str, Any]:
    """Commit installed executable sources/binaries and installation metadata bytes.

    RECORD supplies names, never trusted content hashes. Recorded bytecode caches,
    authored Python, native binaries, import path configuration and distribution
    metadata are read through the existing native custody gate. This is not a
    claim about all package data or native in-memory implementations.
    """
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
    )

    if observe_dependency is not None and not callable(observe_dependency):
        raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_OBSERVER")
    paths: set[Path] = set()
    interpreter_root = Path(sys.prefix).absolute()
    for distribution in distributions:
        files = distribution.files
        if not files:
            raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_INVENTORY")
        location = Path(str(distribution.locate_file(""))).absolute()
        for entry in files:
            relative = Path(str(entry))
            metadata = (
                relative.parts[0].endswith((".dist-info", ".egg-info"))
                and ".." not in relative.parts
            )
            if not metadata and relative.suffix.lower() not in {
                ".py", ".pyi", ".pyc", ".pyd", ".dll", ".exe", ".so", ".dylib", ".pth",
            }:
                continue
            # Normalize RECORD's legitimate ../../../Scripts entries but reject
            # escape from both the installation and its interpreter prefix.
            path = Path(os.path.abspath(str(distribution.locate_file(entry))))
            if not path.is_relative_to(location) and not (
                location.is_relative_to(interpreter_root) and path.is_relative_to(interpreter_root)
            ):
                raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_PATH", str(entry))
            paths.add(path)
            if not metadata and module_origins is not None:
                parts = [*relative.parts[:-1], relative.name.split(".", 1)[0]]
                if parts[-1] == "__init__":
                    parts.pop()
                if parts and all(part.isidentifier() for part in parts):
                    module_origins.setdefault(".".join(parts), set()).add(path)
    # Engineering resource bounds, not configurable evidence-admission shortcuts.
    if len(paths) > 20000:
        raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_BUDGET")
    digest = hashlib.sha256()
    total = 0

    def read(path: Path) -> bytes:
        # Every task uses the original metadata-before-content custody reader.
        # No ancestor/stat or byte cache is shared between files or observations.
        budget = min(path.lstat().st_size, 64 * 1024 * 1024)
        return bounded_regular_bytes(path, budget=budget)

    remaining = iter(sorted(paths, key=str))
    pending: deque[tuple[Path, Future[bytes]]] = deque()
    # A bounded I/O window, not a second execution authority. Four in-flight
    # reads bound prefetched bytes by 4 * 64 MiB, unlike eager map over 20k files.
    # Context exit waits for all owned readers, including on a failed capture.
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="acceptance-read") as pool:
        for _ in range(4):
            next_path = next(remaining, None)
            if next_path is not None:
                pending.append((next_path, pool.submit(read, next_path)))
        while pending:
            path, future = pending.popleft()
            try:
                raw = future.result()
            except (OSError, WorkflowContractError) as exc:
                raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_CUSTODY", str(path)) from exc
            total += len(raw)
            if total > 512 * 1024 * 1024:
                raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_BUDGET")
            if observe_dependency is not None:
                observe_dependency(path, raw)
            if source_inputs is not None and path.suffix in {".py", ".pyc"}:
                source_inputs[path] = raw
            digest.update(json.dumps(
                [str(path), len(raw), hashlib.sha256(raw).hexdigest()], separators=(",", ":")
            ).encode())
            digest.update(b"\n")
            next_path = next(remaining, None)
            if next_path is not None:
                pending.append((next_path, pool.submit(read, next_path)))
    return {"sha256": digest.hexdigest(), "file_count": len(paths), "size_bytes": total}


def bind_mandatory_acceptance(repo_root: Path, candidate_sha: str) -> dict[str, Any]:
    """Bind complete V3 node mapping to actual committed blobs before Full claim.

    Mapping completeness is necessary, not proof of assertion adequacy, execution,
    mutant kills or publication. This function never writes or runs pytest.
    """
    if not re.fullmatch(r"[0-9a-f]{40}", candidate_sha):
        raise ExecutionContainmentError("ACCEPTANCE_CANDIDATE")

    def git(*args: str) -> bytes:
        from ai_trading_system.platform.architecture.source_preservation import (
            inspection_git_result,
        )

        protected = inspection_git_result(repo_root, *args)
        if protected is not None:
            if protected.returncode:
                raise ExecutionContainmentError("ACCEPTANCE_GIT", "protected object read failed")
            return protected.stdout
        result = subprocess.run(
            ["git", "--no-optional-locks", *args],
            cwd=repo_root,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode:
            raise ExecutionContainmentError(
                "ACCEPTANCE_GIT", result.stderr.decode(errors="replace")
            )
        return result.stdout

    if git("rev-parse", "HEAD").decode().strip() != candidate_sha:
        raise ExecutionContainmentError("ACCEPTANCE_CANDIDATE_CHANGED")

    def committed(path: str) -> tuple[bytes, dict[str, str]]:
        # Only fixed authority or validated tests paths may reach Git or filesystem reads.
        entry = git("ls-tree", candidate_sha, "--", path).decode().strip()
        fields = entry.split()
        if len(fields) != 4 or fields[0] not in {"100644", "100755"} or fields[1] != "blob":
            raise ExecutionContainmentError("ACCEPTANCE_BLOB_TYPE", path)
        content = git("cat-file", "blob", fields[2])
        return content, {
            "path": path,
            "mode": fields[0],
            "blob": fields[2],
            "sha256": hashlib.sha256(content).hexdigest(),
        }

    raw, manifest_identity = committed(_ACCEPTANCE_MANIFEST)
    try:
        manifest = json.loads(raw)
        if (
            manifest["schema_version"] != "devx_015_workflow_acceptance.v1"
            or manifest["task_id"] != DEVX015_ACCEPTANCE_TASK
            or manifest["mapping_state"] != "COMPLETE_REVIEWED"
        ):
            raise ValueError("manifest identity or mapping state")
        pairs = sorted(
            (case["id"], variant) for case in manifest["cases"] for variant in case["variants"]
        )
        digest = hashlib.sha256(json.dumps(pairs, separators=(",", ":")).encode()).hexdigest()
        if digest != _ACCEPTANCE_VARIANTS_SHA256:
            raise ValueError("approved variant inventory changed")
        mappings = manifest["variant_node_mapping"]
        mapped = [(row["case_id"], row["variant"]) for row in mappings]
        if sorted(mapped) != pairs:
            raise ValueError("missing, duplicate or unknown variant mapping")
        nodes: set[str] = set()
        for row in mappings:
            selected = row["node_ids"]
            if (
                not isinstance(selected, list)
                or not selected
                or len(selected) != len(set(selected))
            ):
                raise ValueError("empty or duplicate node selection")
            for node in selected:
                if not isinstance(node, str) or "::" not in node:
                    raise ValueError("invalid node id")
                path, test = node.split("::", 1)
                if (
                    not re.fullmatch(r"tests/(?:[A-Za-z0-9_]+/)*test_[A-Za-z0-9_]+\.py", path)
                    or not test
                    or any(char in node for char in "\r\n\x00")
                ):
                    raise ValueError("invalid test path or node id")
                nodes.add(node)
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionContainmentError("ACCEPTANCE_MAPPING_INVALID", str(exc)) from exc
    test_blobs = [committed(path)[1] for path in sorted({node.split("::", 1)[0] for node in nodes})]
    if git("rev-parse", "HEAD").decode().strip() != candidate_sha:
        raise ExecutionContainmentError("ACCEPTANCE_CANDIDATE_CHANGED")
    return {
        "schema_version": "mandatory_acceptance_binding.v1",
        "candidate_sha": candidate_sha,
        "task_id": DEVX015_ACCEPTANCE_TASK,
        "manifest": manifest_identity,
        "test_blobs": test_blobs,
        "required_nodes": sorted(nodes),
        "variant_count": len(pairs),
        "execution_status": "NOT_EXECUTED",
    }


def _acceptance_code_identity(code: CodeType) -> Any:
    """Stable code structure, independent of marshal's intern/reference encoding."""

    def constant(value: Any) -> Any:
        if isinstance(value, CodeType):
            return ["code", _acceptance_code_identity(value)]
        if isinstance(value, (tuple, frozenset)):
            items = [constant(item) for item in value]
            return [
                type(value).__name__,
                sorted(items, key=repr) if isinstance(value, frozenset) else items,
            ]
        return [type(value).__name__, repr(value)]

    return {
        key: getattr(code, key)
        for key in (
            "co_argcount",
            "co_posonlyargcount",
            "co_kwonlyargcount",
            "co_nlocals",
            "co_stacksize",
            "co_flags",
            "co_names",
            "co_varnames",
            "co_filename",
            "co_name",
            "co_qualname",
            "co_firstlineno",
            "co_freevars",
            "co_cellvars",
        )
    } | {
        "bytecode": code.co_code.hex(),
        "linetable": code.co_linetable.hex(),
        "exceptiontable": code.co_exceptiontable.hex(),
        "constants": [constant(value) for value in code.co_consts],
    }


def _capture_acceptance_sources(
    root: Path, candidate_sha: str, *, git_context: HeldGitConfiguration | None = None,
) -> tuple[dict[Path, bytes], list[dict[str, str]]]:
    """Read committed candidate sources without importing or executing them."""
    from ai_trading_system.platform.architecture import workflow_contract

    root = root.absolute()
    if git_context is None:
        from ai_trading_system.platform.architecture.source_preservation import (
            inspection_git_result,
        )

        tree = inspection_git_result(
            root, "ls-tree", "-r", "-z", candidate_sha, "--", "src", "scripts",
        )
        if tree is None:
            tree = subprocess.run(
                ["git", "--no-optional-locks", "ls-tree", "-r", "-z", candidate_sha,
                 "--", "src", "scripts"],
                cwd=root, capture_output=True, timeout=30,
            )
        if tree.returncode:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_TREE")
        tree_bytes = tree.stdout
    else:
        from ai_trading_system.platform.architecture.source_preservation import (
            HeldGitConfiguration,
            SourcePreservationError,
        )

        if type(git_context) is not HeldGitConfiguration:
            raise ExecutionContainmentError("ACCEPTANCE_GIT_CONTEXT_REQUIRED")
        try:
            tree_bytes = git_context.candidate_source_tree(root, candidate_sha)
        except SourcePreservationError as exc:
            raise ExecutionContainmentError("ACCEPTANCE_GIT_CONTEXT", str(exc)) from exc
    sources: dict[Path, bytes] = {}
    rows: list[dict[str, str]] = []
    # Bounded engineering inventory, restricted to Python sources, never docs,
    # research exclusions, arbitrary cwd traversal or an independent authority.
    total = 0
    for entry in tree_bytes.split(b"\0"):
        if not entry:
            continue
        header, raw_path = entry.split(b"\t", 1)
        relative = raw_path.decode("utf-8")
        if not relative.endswith(".py"):
            continue
        mode, kind, oid = header.decode("ascii").split()
        if mode not in {"100644", "100755"} or kind != "blob":
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_TYPE", relative)
        if ".." in Path(relative).parts or not relative.startswith(("src/", "scripts/")):
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_PATH", relative)
        origin = root / relative
        try:
            raw = workflow_contract.bounded_regular_bytes(origin)
        except (OSError, workflow_contract.WorkflowContractError) as exc:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_CUSTODY", str(exc)) from exc
        total += len(raw)
        if total > 256 * 1024 * 1024 or len(sources) >= 10000:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_BUDGET")
        digests = {
            hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
            for content in (raw, raw.replace(b"\r\n", b"\n"))
        }
        if oid not in digests:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_NOT_CANDIDATE", relative)
        sources[origin] = raw
        rows.append({"path": relative, "sha256": hashlib.sha256(raw).hexdigest(), "git_blob": oid})
    if not sources:
        raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_TREE")
    return sources, sorted(rows, key=lambda row: row["path"])


def capture_acceptance_implementation(
    root: Path, candidate_sha: str, *, git_context: HeldGitConfiguration | None = None,
) -> list[dict[str, str]]:
    """Data-only source proof for an independent verifier, never execution admission."""
    return _capture_acceptance_sources(root, candidate_sha, git_context=git_context)[1]


def capture_matching_inspector_sources(
    inspector_root: Path, candidate_root: Path, candidate_sha: str, *,
    git_context: HeldGitConfiguration,
) -> list[dict[str, Any]]:
    """Match a source-only relocated tree, rejecting extra importable code.

    The caller derives inspector_root from its fixed installed implementation.
    No candidate or inspector source is imported or executed here.
    """
    from ai_trading_system.platform.architecture.source_preservation import (
        HeldGitConfiguration,
        SourcePreservationError,
    )
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

    if type(git_context) is not HeldGitConfiguration:
        raise ExecutionContainmentError("ACCEPTANCE_GIT_CONTEXT_REQUIRED")
    candidate_rows = capture_acceptance_implementation(
        candidate_root, candidate_sha, git_context=git_context,
    )
    inspector_root = inspector_root.absolute()
    for ancestor in (*inspector_root.parents, inspector_root):
        metadata = ancestor.lstat()
        if (stat.S_ISLNK(metadata.st_mode)
                or getattr(metadata, "st_file_attributes", 0) & 0x400):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_REPARSE", str(ancestor))
    expected_sources = {row["path"] for row in candidate_rows}
    observed_sources: set[str] = set()
    # The project copy is source-only. Third-party/stdlib/native dependencies
    # belong to the separately protected interpreter installation, not here.
    forbidden_suffixes = {
        ".pyc", ".pyo", ".pyw", ".pyd", ".dll", ".exe", ".so", ".dylib", ".pth",
        ".zip", ".egg",
    }
    pending = [inspector_root / part for part in ("src", "scripts")]
    visited = 0
    while pending:
        path = pending.pop()
        try:
            info = path.lstat()
        except FileNotFoundError:
            if path.parent == inspector_root:
                continue  # Missing candidate sources are still rejected below.
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_TREE_DRIFT", str(path)) from None
        visited += 1
        if visited > 40000:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_BUDGET")
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_REPARSE", str(path))
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as entries:
                for entry in entries:
                    if len(pending) + visited >= 40000:
                        raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_BUDGET")
                    pending.append(Path(entry.path))
            continue
        if not stat.S_ISREG(info.st_mode):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_TREE_INVALID", str(path))
        relative = path.relative_to(inspector_root).as_posix()
        if path.suffix.lower() in forbidden_suffixes:
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_EXECUTABLE_EXTRA", relative)
        if path.suffix.lower() == ".py":
            if relative not in expected_sources:
                raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_SOURCE_EXTRA", relative)
            observed_sources.add(relative)
    if observed_sources != expected_sources:
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_SOURCE_MISSING")
    inputs: list[dict[str, Any]] = []
    for row in candidate_rows:
        path = inspector_root / row["path"]
        raw = bounded_regular_bytes(path)
        digests = {
            hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
            for content in (raw, raw.replace(b"\r\n", b"\n"))
        }
        if row["git_blob"] not in digests:
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_NOT_CANDIDATE", row["path"])
        inputs.append({"path": path.as_posix(), "sha256": hashlib.sha256(raw).hexdigest(),
                       "size_bytes": len(raw)})
    try:
        git_context.assert_current(
            candidate_root, {Path(row["path"]): row["sha256"] for row in inputs},
        )
    except SourcePreservationError as exc:
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_NOT_HELD", str(exc)) from exc
    return inputs


def bind_acceptance_implementation(
    root: Path, candidate_sha: str, *, include_runner: bool = False,
    runtime_inputs: Mapping[Path, bytes] | None = None,
) -> list[dict[str, str]]:
    """Candidate execution still requires loaded code from that exact candidate."""
    root = root.absolute()
    sources, rows = _capture_acceptance_sources(root, candidate_sha)

    verification_sources = {
        path: raw for path, raw in (runtime_inputs or {}).items() if path.suffix == ".py"
    }
    if any(path in verification_sources and verification_sources[path] != raw
           for path, raw in sources.items()):
        raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CANDIDATE_OVERLAP")
    verification_sources.update(sources)
    _verify_loaded_python_sources(
        verification_sources, root=root, required_prefix="ai_trading_system",
        require_runner=include_runner,
        cache_inputs={path: raw for path, raw in (runtime_inputs or {}).items()
                      if path.suffix == ".pyc"},
    )
    return sorted(rows, key=lambda row: row["path"])


def bind_protected_inspector_runtime(
    candidate_root: Path, candidate_sha: str, *, git_context: HeldGitConfiguration,
) -> dict[str, Any]:
    """Admit this installed inspector, never a caller-selected code/runtime root."""
    from ai_trading_system.platform.architecture.source_preservation import (
        HeldGitConfiguration,
    )

    if type(git_context) is not HeldGitConfiguration:
        raise ExecutionContainmentError("ACCEPTANCE_GIT_CONTEXT_REQUIRED")
    git_context.assert_current(candidate_root, {})
    runtime_root = Path(sys.executable).absolute().parent
    inspector_root = Path(__file__).resolve().parents[4]
    if (not sys.flags.isolated or not sys.flags.no_site or not sys.dont_write_bytecode
            or "site" in sys.modules or Path.cwd() != runtime_root
            or Path(sys.prefix) != runtime_root or Path(sys.base_prefix) != runtime_root
            or not inspector_root.is_relative_to(runtime_root)):
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_STARTUP")
    git_context.assert_runtime_root(runtime_root)
    if not sys.path or any(
        not isinstance(entry, str) or not Path(entry).is_absolute()
        or not Path(entry).resolve().is_relative_to(runtime_root) for entry in sys.path
    ):
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_IMPORT_PATH")
    for module in tuple(sys.modules.values()):
        location = getattr(module, "__file__", None)
        if (isinstance(location, str) and not location.startswith("<")
                and not Path(location).resolve().is_relative_to(runtime_root)):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_MODULE_OUTSIDE", location)
    _verify_inspector_native_modules(runtime_root)
    observed: dict[Path, str] = {}

    def observe(path: Path, raw: bytes) -> None:
        if not path.is_relative_to(runtime_root):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_RUNTIME_OUTSIDE", str(path))
        observed[path] = hashlib.sha256(raw).hexdigest()

    dependencies: dict[Path, bytes] = {}
    runtime = acceptance_runtime_identity(
        captured_dependencies=dependencies, observe_dependency=observe,
    )
    git_context.assert_current(candidate_root, observed)
    sources = capture_matching_inspector_sources(
        inspector_root, candidate_root, candidate_sha, git_context=git_context,
    )
    loaded = bind_inspector_implementation(inspector_root, runtime_inputs=dependencies)
    return {"inspector_root": inspector_root.as_posix(), "runtime_identity": runtime,
            "inspector_sources": sources, "loaded_inspector_sources": loaded}


def _verify_inspector_native_modules(runtime_root: Path) -> None:
    """Reject native code outside the held installation and the OS system directory."""
    if os.name != "nt":
        raise ExecutionContainmentError("ACCEPTANCE_INTERPRETER_PLATFORM")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = w.HANDLE
    kernel.GetSystemDirectoryW.argtypes = [w.LPWSTR, w.UINT]
    kernel.GetSystemDirectoryW.restype = w.UINT
    kernel.GetModuleFileNameW.argtypes = [w.HMODULE, w.LPWSTR, w.DWORD]
    kernel.GetModuleFileNameW.restype = w.DWORD
    buffer = ctypes.create_unicode_buffer(32768)
    length = kernel.GetSystemDirectoryW(buffer, len(buffer))
    if not 0 < length < len(buffer):
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_SYSTEM_DIRECTORY")
    system_root = Path(buffer.value).resolve()
    api = ctypes.WinDLL("psapi", use_last_error=True)
    api.EnumProcessModules.argtypes = [
        w.HANDLE, ctypes.POINTER(w.HMODULE), w.DWORD, ctypes.POINTER(w.DWORD),
    ]
    api.EnumProcessModules.restype = w.BOOL
    modules = (w.HMODULE * 2048)()
    needed = w.DWORD()
    if (not api.EnumProcessModules(kernel.GetCurrentProcess(), modules, ctypes.sizeof(modules),
                                  ctypes.byref(needed)) or needed.value > ctypes.sizeof(modules)):
        raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_NATIVE_INVENTORY")
    for module in modules[:needed.value // ctypes.sizeof(w.HMODULE)]:
        length = kernel.GetModuleFileNameW(module, buffer, len(buffer))
        if not 0 < length < len(buffer):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_NATIVE_INVENTORY")
        path = Path(buffer.value).resolve()
        if not path.is_relative_to(runtime_root) and not path.is_relative_to(system_root):
            raise ExecutionContainmentError("ACCEPTANCE_INSPECTOR_NATIVE_OUTSIDE", str(path))


def bind_inspector_implementation(
    root: Path, *, runtime_inputs: Mapping[Path, bytes],
) -> list[dict[str, Any]]:
    """Verify loaded inspector code against its own fixed implementation files.

    This checks code integrity, not administrator deployment authorization.
    The caller derives root from its installed implementation, never candidate input.
    """
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

    root = root.absolute()
    sources: dict[Path, bytes] = {}
    origins: dict[str, set[Path]] = {}
    total = 0
    for name, module in tuple(sys.modules.items()):
        if name == "ai_trading_system" or name.startswith("ai_trading_system."):
            relative = Path(*name.split("."))
            origins[name] = {root / "src" / relative.with_suffix(".py"),
                             root / "src" / relative / "__init__.py"}
        location = getattr(module, "__file__", None)
        if not isinstance(location, str):
            continue
        path = Path(location).absolute()
        if path.suffix != ".py" or not any(
            path.is_relative_to(root / part) for part in ("src", "scripts")
        ) or path in sources:
            continue
        raw = bounded_regular_bytes(path)
        total += len(raw)
        if total > 256 * 1024 * 1024 or len(sources) >= 10000:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_BUDGET")
        sources[path] = raw
    verification = {path: raw for path, raw in runtime_inputs.items() if path.suffix == ".py"}
    if any(path in verification and verification[path] != raw for path, raw in sources.items()):
        raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CANDIDATE_OVERLAP")
    verification.update(sources)
    _verify_loaded_python_sources(
        verification, root=root, required_prefix="ai_trading_system", require_runner=True,
        module_origins=origins,
        cache_inputs={path: raw for path, raw in runtime_inputs.items() if path.suffix == ".pyc"},
    )
    return [{"path": path.as_posix(), "sha256": hashlib.sha256(raw).hexdigest(),
             "size_bytes": len(raw)} for path, raw in sorted(sources.items())]


def _verify_loaded_python_sources(
    sources: Mapping[Path, bytes],
    *,
    root: Path | None = None,
    required_prefix: str = "",
    require_runner: bool = False,
    module_origins: Mapping[str, set[Path]] | None = None,
    cache_inputs: Mapping[Path, bytes] | None = None,
) -> None:
    """Verify each process against already captured sources, without executing them."""
    from ai_trading_system.platform.architecture import workflow_contract

    catalogs: dict[Path, list[CodeType]] = {}
    cached_catalogs: dict[Path, list[CodeType]] = {}
    rewriting = sys.modules.get("_pytest.assertion.rewrite")
    rewriting_type = getattr(rewriting, "AssertionRewritingHook", None)
    rewrite_loaders: dict[Path, Any] = {}
    for module in tuple(sys.modules.values()):
        location = getattr(module, "__file__", None)
        loader = getattr(module, "__loader__", None)
        if (isinstance(location, str) and rewriting_type is not None
                and type(loader) is rewriting_type):
            rewrite_loaders[Path(location).absolute()] = loader

    def catalog(path: Path) -> list[CodeType]:
        path = path.absolute()
        if path in catalogs:
            return catalogs[path]
        try:
            content = sources.get(path)
            if content is None:
                # A normal stdlib decorator may supply its wrapper. Verify its
                # actual body too; never open an arbitrary co_filename supplied
                # by a replaced code object (including protected repo files).
                external = Path(os.path.abspath(path))
                roots = (Path(sys.base_prefix) / "Lib", Path(sys.prefix) / "Lib")
                if external.suffix != ".py" or not any(
                    external.is_relative_to(base.absolute()) for base in roots
                ):
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_ORIGIN", str(path))
                content = workflow_contract.bounded_regular_bytes(path)
            code = compile(content, str(path), "exec", dont_inherit=True)
        except (OSError, ValueError, SyntaxError, workflow_contract.WorkflowContractError) as exc:
            raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_SOURCE", str(path)) from exc
        codes: list[CodeType] = []

        def collect(code: CodeType) -> None:
            codes.append(code)
            for value in code.co_consts:
                if isinstance(value, CodeType):
                    collect(value)

        collect(code)
        catalogs[path] = codes
        if path in rewrite_loaders:
            producer = getattr(rewriting, "rewrite_asserts", None)
            location = getattr(rewriting, "__file__", None)
            if not isinstance(producer, FunctionType) or not location:
                raise ExecutionContainmentError("ACCEPTANCE_REWRITE_PRODUCER_MISSING")
            producer_path = Path(location).absolute()
            if producer_path == path or producer_path not in sources:
                raise ExecutionContainmentError("ACCEPTANCE_REWRITE_PRODUCER_ORIGIN")
            expected = next((item for item in catalog(producer_path)
                             if item.co_qualname == "rewrite_asserts"), None)
            if expected is None or not matches_code(producer.__code__, expected):
                raise ExecutionContainmentError("ACCEPTANCE_REWRITE_PRODUCER_CHANGED")
            syntax = ast.parse(content, filename=str(path))
            # Reuse the real, source-verified pytest AST producer and its actual
            # bound configuration; do not disable rewriting to make checks pass.
            producer(syntax, content, str(path), rewrite_loaders[path].config)
            rewritten = compile(syntax, str(path), "exec", dont_inherit=True)
            codes.clear()
            collect(rewritten)
        return codes

    def cached_catalog(path: Path) -> list[CodeType]:
        path = path.absolute()
        if path in rewrite_loaders:
            # The ordinary installation cache is not pytest's rewritten code.
            # Rewritten code must match the independently rebuilt AST exactly.
            return []
        if path in cached_catalogs:
            return cached_catalogs[path]
        raw = (cache_inputs or {}).get(Path(importlib.util.cache_from_source(str(path))))
        if raw is None:
            cached_catalogs[path] = []
            return []
        if (
            len(raw) < 16 or raw[:4] != importlib.util.MAGIC_NUMBER
            or int.from_bytes(raw[4:8], "little") not in {0, 1, 3}
        ):
            raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_CACHE_HEADER", str(path))
        try:
            cached = marshal.loads(raw[16:])
        except (EOFError, ValueError, TypeError) as exc:
            raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_CACHE_CODE", str(path)) from exc
        if not isinstance(cached, CodeType):
            raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_CACHE_CODE", str(path))
        original_filename = cached.co_filename

        def normalize(code: CodeType) -> CodeType:
            return code.replace(
                co_filename=(str(path) if code.co_filename == original_filename
                             else code.co_filename),
                co_consts=tuple(normalize(item) if isinstance(item, CodeType) else item
                                for item in code.co_consts),
            )

        def without_linetables(value: Any) -> Any:
            if isinstance(value, dict):
                return {key: without_linetables(item) for key, item in value.items()
                        if key != "linetable"}
            if isinstance(value, (list, tuple)):
                return [without_linetables(item) for item in value]
            return value

        cached = normalize(cached)
        # A timestamp-valid cache is not sufficient. Its complete instructions,
        # constants, flags, names and exception tables must match frozen source.
        # Only line-table encoding may differ, and the *actual loaded* line table
        # must still exactly equal this separately byte-committed cache below.
        if without_linetables(_acceptance_code_identity(cached)) != without_linetables(
            _acceptance_code_identity(catalog(path)[0])
        ):
            raise ExecutionContainmentError("ACCEPTANCE_DEPENDENCY_CACHE_SOURCE", str(path))
        codes: list[CodeType] = []

        def collect(code: CodeType) -> None:
            codes.append(code)
            for item in code.co_consts:
                if isinstance(item, CodeType):
                    collect(item)

        collect(cached)
        cached_catalogs[path] = codes
        return codes

    def matches_code(actual: CodeType, expected: CodeType) -> bool:
        if (actual.co_qualname, actual.co_firstlineno) != (
            expected.co_qualname, expected.co_firstlineno
        ):
            return False
        identity = _acceptance_code_identity(actual)
        if identity == _acceptance_code_identity(expected):
            return True
        return (
            actual.co_qualname == expected.co_qualname
            and actual.co_firstlineno == expected.co_firstlineno
            and any(identity == _acceptance_code_identity(code)
                    for code in cached_catalog(Path(expected.co_filename)))
        )

    # Pytest's early, source-declared compatibility patches are real bindings,
    # not code changes to the old class-body declaration. Derive only literal
    # MonkeyPatch.setattr contributions from a verified initialization hook;
    # never accept a replacement merely because it belongs to some known file.
    extension_bindings: dict[tuple[int, str], list[tuple[Any, CodeType]]] = {}
    monkeypatch_module = sys.modules.get("_pytest.monkeypatch")
    monkeypatch_type = getattr(monkeypatch_module, "MonkeyPatch", None)
    for extension in tuple(sys.modules.values()):
        location = getattr(extension, "__file__", None)
        if not location or Path(location).absolute() not in sources:
            continue
        namespace = vars(extension)
        hook = namespace.get("pytest_load_initial_conftests")
        if not isinstance(hook, FunctionType) or not hasattr(hook, "pytest_impl"):
            continue
        path = Path(location).absolute()
        syntax = ast.parse(sources[path])
        hook_node = next((item for item in syntax.body if isinstance(item, ast.FunctionDef)
                          and item.name == "pytest_load_initial_conftests"), None)
        hook_code = next((code for code in catalog(path)
                          if code.co_qualname == "pytest_load_initial_conftests"), None)
        if hook_node is None or hook_code is None or not matches_code(hook.__code__, hook_code):
            raise ExecutionContainmentError("ACCEPTANCE_EXTENSION_HOOK_CHANGED", str(path))
        patchers: set[str] = set()
        for statement in hook_node.body:
            if (
                isinstance(statement, ast.Assign) and isinstance(statement.value, ast.Call)
                and isinstance(statement.value.func, ast.Name)
                and monkeypatch_type is not None
                and namespace.get(statement.value.func.id) is monkeypatch_type
            ):
                patchers.update(target.id for target in statement.targets
                                if isinstance(target, ast.Name))
            if not isinstance(statement, ast.Expr) or not isinstance(statement.value, ast.Call):
                continue
            call = statement.value
            if not (
                isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name)
                and call.func.value.id in patchers and call.func.attr == "setattr"
                and len(call.args) == 3 and isinstance(call.args[0], ast.Name)
                and isinstance(call.args[1], ast.Constant) and type(call.args[1].value) is str
                and isinstance(call.args[2], ast.Name)
            ):
                continue
            owner = namespace.get(call.args[0].id)
            implementation = namespace.get(call.args[2].id)
            expected = next((code for code in catalog(path)
                             if code.co_qualname == call.args[2].id), None)
            if not isinstance(owner, type) or not isinstance(implementation, FunctionType):
                continue
            if expected is None or not matches_code(implementation.__code__, expected):
                raise ExecutionContainmentError("ACCEPTANCE_EXTENSION_IMPLEMENTATION_CHANGED")
            extension_bindings.setdefault((id(owner), call.args[1].value), []).append(
                (implementation, expected)
            )

    def verify_callable(value: Any, expected: CodeType, decorators: list[Any]) -> bool:
        seen: set[int] = set()
        while id(value) not in seen:
            seen.add(id(value))
            actual = getattr(value, "__code__", None)
            wrapped = getattr(value, "__wrapped__", None)
            if isinstance(actual, CodeType):
                identity = _acceptance_code_identity(actual)
                if matches_code(actual, expected):
                    return True
                if not any(identity == _acceptance_code_identity(code)
                           for code in [*catalog(Path(actual.co_filename)),
                                        *cached_catalog(Path(actual.co_filename))]):
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_CHANGED",
                                                    expected.co_qualname)
                if wrapped is None:
                    candidates = []
                    for cell in getattr(value, "__closure__", None) or ():
                        try:
                            member = cell.cell_contents
                        except ValueError as exc:
                            raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BINDING",
                                                            expected.co_qualname) from exc
                        if (isinstance(member, FunctionType)
                                and matches_code(member.__code__, expected)):
                            candidates.append(member)
                    if len(candidates) != 1:
                        return False
                    wrapped = candidates[0]
                if not any(
                    producer is not None and getattr(producer, "__code__", None) is not None
                    and actual.co_filename == producer.__code__.co_filename
                    and actual.co_qualname.startswith(producer.__qualname__ + ".<locals>.")
                    for decorator in decorators
                    for producer in (decorator, decorator.__call__ if callable(decorator) else None)
                ):
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_CHANGED",
                                                    expected.co_qualname)
                cells = getattr(value, "__closure__", None) or ()
                try:
                    closure_bound = any(cell.cell_contents is wrapped for cell in cells)
                except ValueError as exc:
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BINDING",
                                                    expected.co_qualname) from exc
                if not closure_bound:
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BINDING",
                                                    expected.co_qualname)
            elif isinstance(value, type(functools.lru_cache()(lambda: None))):
                if wrapped is None or not any(item is wrapped for item in gc.get_referents(value)):
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BINDING",
                                                    expected.co_qualname)
            elif callable(value) or any(
                isinstance(decorator, type) and isinstance(value, decorator)
                for decorator in decorators
            ):
                cls = type(value)
                namespace = sys.modules.get(cls.__module__)
                declared: Any = namespace
                for component in cls.__qualname__.split("."):
                    declared = (vars(declared).get(component)
                                if hasattr(declared, "__dict__") else None)
                location = getattr(namespace, "__file__", None)
                if declared is not cls or not location or not any(
                    code.co_qualname == cls.__qualname__ for code in catalog(Path(location))
                ):
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_CLASS",
                                                    expected.co_qualname)
                referents: list[Any] = []
                for item in gc.get_referents(value):
                    if isinstance(item, dict):
                        referents.extend(member for key, member in item.items()
                                         if key != "__wrapped__")
                    else:
                        referents.append(item)
                if len(referents) > 20000:
                    raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BUDGET")
                if wrapped is not None:
                    if not any(member is wrapped for member in referents):
                        raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_BINDING",
                                                        expected.co_qualname)
                else:
                    functions = [member.__func__ if isinstance(member, (classmethod, staticmethod))
                                 else member for member in referents]
                    return any(
                        verify_callable(member, expected, decorators)
                        for member in functions if isinstance(member, FunctionType)
                    )
            else:
                return False
            value = wrapped
        raise ExecutionContainmentError("ACCEPTANCE_LOADED_WRAPPER_CYCLE", expected.co_qualname)

    runner_seen = False
    unknown = object()
    owner_producers: dict[int, list[Any]] = {}
    environment_variables = dict(os.environ)
    for name, module in tuple(sys.modules.items()):
        location = getattr(module, "__file__", None)
        protected = bool(required_prefix) and (
            name == required_prefix or name.startswith(required_prefix + ".")
        )
        expected_origins = (module_origins or {}).get(name)
        if not location:
            if expected_origins:
                raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_ORIGIN", name)
            continue
        path = Path(location).absolute()
        if expected_origins and path not in expected_origins:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_ORIGIN", name)
        if protected and path not in sources:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_ORIGIN", name)
        if path not in sources:
            continue
        runner_seen |= root is not None and path == root / "scripts/run_validation_tier.py"
        codes = catalog(path)
        syntax = ast.parse(sources[path])

        def decorator_object(node: ast.expr, namespace: Any = module) -> Any:
            if isinstance(node, ast.Call):
                return decorator_object(node.func)
            if isinstance(node, ast.Name):
                return vars(namespace).get(node.id)
            if isinstance(node, ast.Attribute):
                return getattr(decorator_object(node.value), node.attr, None)
            return None

        def static_value(node: ast.expr, namespace: Any = module) -> Any:
            if isinstance(node, ast.Constant):
                return node.value
            if (
                isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get" and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "environ"
                and isinstance(node.func.value.value, ast.Name)
                and vars(namespace).get(node.func.value.value.id) is os
                and 1 <= len(node.args) <= 2 and not node.keywords
            ):
                key = static_value(node.args[0])
                default = static_value(node.args[1]) if len(node.args) == 2 else None
                if type(key) is str and default is not unknown:
                    return environment_variables.get(key, default)
                return unknown
            if isinstance(node, ast.Name):
                value = vars(namespace).get(node.id, unknown)
            elif isinstance(node, ast.Attribute):
                owner = static_value(node.value)
                value = (vars(owner).get(node.attr, unknown)
                         if type(owner) is ModuleType else unknown)
            elif isinstance(node, (ast.Tuple, ast.List)):
                items = tuple(static_value(item) for item in node.elts)
                return unknown if any(item is unknown for item in items) else items
            elif isinstance(node, ast.Subscript):
                owner = static_value(node.value)
                if type(owner) not in {tuple, str}:
                    return unknown
                if isinstance(node.slice, ast.Slice):
                    bounds = [static_value(item) if item else None
                              for item in (node.slice.lower, node.slice.upper, node.slice.step)]
                    if any(item is not None and type(item) is not int for item in bounds):
                        return unknown
                    index: Any = slice(*bounds)
                else:
                    index = static_value(node.slice)
                    if type(index) is not int:
                        return unknown
                try:
                    return owner[index]
                except (IndexError, ValueError):
                    return unknown
            else:
                return unknown
            if value is sys.version_info:
                return tuple(value)
            if type(value) in {str, int, bool, float, tuple, type(None), ModuleType}:
                return value
            return unknown

        def static_condition(node: ast.expr) -> bool | None:
            if isinstance(node, ast.BoolOp):
                conditions = [static_condition(item) for item in node.values]
                if isinstance(node.op, ast.And):
                    return False if False in conditions else (None if None in conditions else True)
                return True if True in conditions else (None if None in conditions else False)
            if (
                isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"startswith", "endswith"}
                and len(node.args) == 1 and not node.keywords
            ):
                target, prefix = static_value(node.func.value), static_value(node.args[0])
                if type(target) is str and type(prefix) is str:
                    return (str.startswith(target, prefix) if node.func.attr == "startswith"
                            else str.endswith(target, prefix))
                return None
            if (
                isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "hasattr" and len(node.args) == 2 and not node.keywords
            ):
                target, attribute = (static_value(item) for item in node.args)
                if type(target) is ModuleType and type(attribute) is str:
                    if attribute in vars(target):
                        return True
                    return None if "__getattr__" in vars(target) else False
                return None
            if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
                condition = static_condition(node.operand)
                return None if condition is None else not condition
            if isinstance(node, ast.Compare):
                left = static_value(node.left)
                for operation, expression in zip(node.ops, node.comparators, strict=True):
                    right = static_value(expression)
                    if left is unknown or right is unknown:
                        return None
                    try:
                        if isinstance(operation, ast.Eq):
                            matched = left == right
                        elif isinstance(operation, ast.NotEq):
                            matched = left != right
                        elif isinstance(operation, ast.Lt):
                            matched = left < right
                        elif isinstance(operation, ast.LtE):
                            matched = left <= right
                        elif isinstance(operation, ast.Gt):
                            matched = left > right
                        elif isinstance(operation, ast.GtE):
                            matched = left >= right
                        else:
                            return None
                    except TypeError:
                        return None
                    if not matched:
                        return False
                    left = right
                return True
            value = static_value(node)
            return bool(value) if type(value) in {bool, int, str, tuple, type(None)} else None

        def check_body(
            body: list[ast.stmt], owner: Any, prefix: str = "", optional: bool = False,
            source_codes: list[CodeType] = codes,
        ) -> None:
            def expand(statements: list[ast.stmt]) -> list[ast.stmt]:
                expanded = []
                for statement in statements:
                    if isinstance(statement, ast.If):
                        choice = static_condition(statement.test)
                        if choice is not None:
                            expanded.extend(expand(statement.body if choice else statement.orelse))
                            continue
                    expanded.append(statement)
                return expanded

            body = expand(body)
            implementations = {
                statement.name: statement for statement in body
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            }
            deleted = {target.id for statement in body if isinstance(statement, ast.Delete)
                       for target in statement.targets if isinstance(target, ast.Name)}
            for node in body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    key = node.name
                    if implementations[key] is not node:
                        continue  # A later active declaration replaces this source binding.
                    if not isinstance(node, ast.ClassDef) and any(
                        decorator_object(item) is getattr(
                            sys.modules.get("typing"), "overload", None
                        )
                        for item in node.decorator_list
                    ):
                        continue  # Typing-only prototypes are not the live implementation.
                    if prefix and key.startswith("__") and not key.endswith("__"):
                        key = "_" + prefix.rstrip(".").split(".")[-1].lstrip("_") + key
                    if key not in vars(owner):
                        if optional or key in deleted:
                            continue
                        raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_MISSING",
                                                        prefix + node.name)
                    value = vars(owner)[key]
                    if isinstance(node, ast.ClassDef):
                        if not isinstance(value, type):
                            raise ExecutionContainmentError("ACCEPTANCE_LOADED_CLASS_CHANGED", key)
                        producers = []
                        metatype: Any = type(value)
                        for metaclass in metatype.__mro__:
                            for method_name in ("__new__", "__init__"):
                                method = vars(metaclass).get(method_name)
                                if isinstance(method, (classmethod, staticmethod)):
                                    method = method.__func__
                                if not isinstance(method, FunctionType):
                                    continue
                                for referenced in method.__code__.co_names:
                                    producer = method.__globals__.get(referenced)
                                    if isinstance(producer, FunctionType):
                                        origin = Path(producer.__code__.co_filename).absolute()
                                        cache = Path(importlib.util.cache_from_source(str(origin)))
                                        if (origin not in sources
                                                and cache not in (cache_inputs or {})):
                                            continue
                                        if not any(matches_code(producer.__code__, code)
                                                   for code in catalog(origin)):
                                            raise ExecutionContainmentError(
                                                "ACCEPTANCE_LOADED_WRAPPER_PRODUCER", referenced
                                            )
                                        producers.append(producer)
                        owner_producers[id(value)] = producers
                        check_body(node.body, value, prefix + node.name + ".")
                        continue
                    first = min([node.lineno, *[item.lineno for item in node.decorator_list]])
                    expected = next((code for code in source_codes
                                     if code.co_qualname == prefix + node.name
                                     and code.co_firstlineno == first), None)
                    if expected is None:
                        raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_SOURCE", key)
                    contributions = extension_bindings.get((id(owner), key), [])
                    for implementation, extension_code in contributions:
                        if value is implementation:
                            expected = extension_code
                            break
                    if isinstance(value, (classmethod, staticmethod)):
                        choices = [value.__func__]
                    elif isinstance(value, property):
                        choices = [item for item in (value.fget, value.fset, value.fdel) if item]
                    elif isinstance(value, functools.cached_property):
                        choices = [value.func]
                    else:
                        choices = [value]
                    decorators = [decorator_object(item) for item in node.decorator_list]
                    decorators.extend(owner_producers.get(id(owner), []))
                    if not any(verify_callable(item, expected, decorators) for item in choices):
                        raise ExecutionContainmentError("ACCEPTANCE_LOADED_CODE_CHANGED",
                                                        prefix + node.name)
                elif isinstance(node, ast.If):
                    condition = static_condition(node.test)
                    if condition is not None:
                        check_body(node.body if condition else node.orelse, owner, prefix, optional)
                    else:
                        check_body(node.body, owner, prefix, optional=True)
                        check_body(node.orelse, owner, prefix, optional=True)
                elif isinstance(node, (ast.Try, ast.With)):
                    check_body(node.body, owner, prefix, optional=True)
                    check_body(getattr(node, "orelse", []), owner, prefix, optional=True)

        check_body(syntax.body, module)
    if require_runner and not runner_seen:
        raise ExecutionContainmentError("ACCEPTANCE_RUNNER_ORIGIN")


def bind_acceptance_checkout(root: Path, binding: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Bind authorized test/manifest working bytes with existing native custody reads."""
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
    )

    rows = []
    try:
        for record in [binding["manifest"], *binding["test_blobs"]]:
            relative = record["path"]
            if relative != _ACCEPTANCE_MANIFEST and not re.fullmatch(
                r"tests/(?:[A-Za-z0-9_]+/)*test_[A-Za-z0-9_]+\.py", relative
            ):
                raise ExecutionContainmentError("ACCEPTANCE_INPUT_PATH")
            path = root / relative
            before = path.lstat()
            identity = (before.st_dev, before.st_ino)
            content = bounded_regular_bytes(path, expected_identity=identity)
            after = path.lstat()
            if identity != (after.st_dev, after.st_ino):
                raise ExecutionContainmentError("ACCEPTANCE_INPUT_REPLACED", relative)
            # Same explicit Git LF text basis as existing source-preservation identity.
            # Raw checkout bytes are separately frozen; no raw-byte identity is normalized.
            if record["sha256"] not in {
                hashlib.sha256(content).hexdigest(),
                hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest(),
            }:
                raise ExecutionContainmentError("ACCEPTANCE_INPUT_NOT_CANDIDATE", relative)
            rows.append(
                {
                    "path": relative,
                    "file_identity": list(identity),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "size_bytes": len(content),
                }
            )
    except (OSError, WorkflowContractError) as exc:
        raise ExecutionContainmentError("ACCEPTANCE_INPUT_CUSTODY", str(exc)) from exc
    return rows


class MandatoryAcceptancePlugin:
    """Reject incomplete mandatory pytest evidence, including actual xdist runs.

    This is an execution-result guard, not authority to select the required
    nodes: the acceptance runner must separately freeze the complete reviewed
    variant mapping and code/environment identities. Existing optional tests
    may retain their skips. Never turn an underlying pytest failure into PASS.
    """

    def __init__(self, required_nodes: Sequence[str]) -> None:
        if (
            isinstance(required_nodes, (str, bytes))
            or not required_nodes
            or any(not isinstance(node, str) or "::" not in node for node in required_nodes)
            or len(set(required_nodes)) != len(required_nodes)
        ):
            raise ExecutionContainmentError("ACCEPTANCE_REQUIRED_NODES")
        self.required = frozenset(required_nodes)
        self.collections: list[tuple[str, ...]] = []
        self.reports: dict[str, list[tuple[str, str, bool]]] = {}
        self.deselected: set[str] = set()
        self.result: dict[str, Any] | None = None

    def pytest_collection_finish(self, session: Any) -> None:
        if not session.config.getoption("numprocesses", default=0):
            self.collections.append(tuple(item.nodeid for item in session.items))

    def pytest_xdist_node_collection_finished(self, node: Any, ids: list[str]) -> None:
        self.collections.append(tuple(ids))

    def pytest_deselected(self, items: list[Any]) -> None:
        self.deselected.update(item.nodeid for item in items)

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.nodeid in self.required:
            self.reports.setdefault(report.nodeid, []).append(
                (report.when, report.outcome, hasattr(report, "wasxfail"))
            )

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        reasons: set[str] = set()
        if int(exitstatus) != 0:
            reasons.add("PYTEST_NOT_SUCCESSFUL")
        if not self.collections:
            reasons.add("COLLECTION_MISSING")
        for collected in self.collections:
            if len(collected) != len(set(collected)):
                reasons.add("COLLECTION_DUPLICATE")
            if not self.required.issubset(collected):
                reasons.add("MANDATORY_NODE_MISSING")
        if self.collections and any(row != self.collections[0] for row in self.collections):
            reasons.add("WORKER_COLLECTION_MISMATCH")
        if self.required.intersection(self.deselected):
            reasons.add("MANDATORY_NODE_DESELECTED")
        for node in self.required:
            reports = self.reports.get(node, [])
            if sorted(phase for phase, _, _ in reports) != ["call", "setup", "teardown"]:
                reasons.add("MANDATORY_PHASES_INCOMPLETE_OR_DUPLICATE")
            if any(outcome != "passed" or xfail for _, outcome, xfail in reports):
                reasons.add("MANDATORY_REPORT_NOT_PASS")
        self.result = {
            "schema_version": "mandatory_acceptance_execution.v1",
            "status": "FAIL" if reasons else "PASS",
            "reason_codes": sorted(reasons),
            "required_nodes": sorted(self.required),
            "collection_count": len(self.collections),
            "collections": [
                {
                    "sha256": hashlib.sha256(json.dumps(row).encode()).hexdigest(),
                    "count": len(row),
                    "duplicate_count": len(row) - len(set(row)),
                    "required_nodes": sorted(self.required.intersection(row)),
                }
                for row in self.collections
            ],
            "pytest_exitstatus": int(exitstatus),
            "reports": {node: self.reports.get(node, []) for node in sorted(self.required)},
        }
        if reasons and int(session.exitstatus) == 0:
            session.exitstatus = 1


class _BoundAcceptancePlugin(MandatoryAcceptancePlugin):
    def __init__(
        self,
        root: Path,
        binding: dict[str, Any],
        output: Path,
        checkout_identity: list[dict[str, Any]],
        runtime_identity: dict[str, Any],
        implementation_identity: list[dict[str, str]],
        result_identity: tuple[int, int],
        result_root_identity: tuple[int, int],
    ) -> None:
        super().__init__(binding["required_nodes"])
        self.root, self.binding, self.output = root, binding, output
        self.checkout_identity = checkout_identity
        self.runtime_identity = runtime_identity
        self.implementation_identity = implementation_identity
        self.result_identity, self.result_root_identity = result_identity, result_root_identity
        self.worker_inputs: list[Any] = []

    def pytest_testnodedown(self, node: Any, error: Any) -> None:
        # A crashed xdist worker may never send workeroutput. Preserve a missing
        # identity so sessionfinish rejects the run, without masking its cause
        # with a controller AttributeError (DEVX-015 V3, v79/v80).
        output = getattr(node, "workeroutput", None)
        self.worker_inputs.append(
            output.get("mandatory_input_identity")
            if error is None and isinstance(output, dict) else None
        )

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        super().pytest_sessionfinish(session, exitstatus)
        identity_error = None
        try:
            if bind_mandatory_acceptance(self.root, self.binding["candidate_sha"]) != self.binding:
                identity_error = "ACCEPTANCE_BINDING_CHANGED"
            if bind_acceptance_checkout(self.root, self.binding) != self.checkout_identity:
                identity_error = "ACCEPTANCE_CHECKOUT_CHANGED"
            dependency_inputs: dict[Path, bytes] = {}
            if (
                acceptance_runtime_identity(captured_dependencies=dependency_inputs)
                != self.runtime_identity
            ):
                identity_error = "ACCEPTANCE_RUNTIME_CHANGED"
            if (
                bind_acceptance_implementation(
                    self.root, self.binding["candidate_sha"], runtime_inputs=dependency_inputs
                )
                != self.implementation_identity
            ):
                identity_error = "ACCEPTANCE_IMPLEMENTATION_CHANGED"
            if session.config.getoption("numprocesses", default=0) and (
                len(self.worker_inputs) != len(self.collections)
                or any(
                    row
                    != {
                        "checkout_identity": self.checkout_identity,
                        "origin_valid": True,
                        "runtime_identity": self.runtime_identity,
                        "implementation_identity": self.implementation_identity,
                    }
                    for row in self.worker_inputs
                )
            ):
                identity_error = "ACCEPTANCE_WORKER_INPUT_CHANGED"
        except ExecutionContainmentError as exc:
            identity_error = str(exc)
        if not session.config.getoption("numprocesses", default=0):
            self.worker_inputs = [
                {
                    "checkout_identity": self.checkout_identity,
                    "origin_valid": identity_error is None,
                    "runtime_identity": self.runtime_identity,
                    "implementation_identity": self.implementation_identity,
                }
            ]
        if identity_error:
            session.exitstatus = 1
        # Fill only the exact empty file pre-reserved by the parent. Native I/O
        # holds root/leaf identities, rejects links/replacements and flushes bytes.
        from ai_trading_system.platform.architecture.workflow_contract import apply_bound_file

        content = json.dumps(
            {
                "binding": self.binding,
                "execution": self.result,
                "identity_error": identity_error,
                "checkout_identity": self.checkout_identity,
                "worker_inputs": self.worker_inputs,
                "runtime_identity": self.runtime_identity,
                "implementation_identity": self.implementation_identity,
            },
            sort_keys=True,
        ).encode()
        apply_bound_file(
            self.output.parent,
            self.output.name,
            b"",
            content,
            expected_identity=self.result_identity,
            expected_root_identity=self.result_root_identity,
        )


class _AcceptanceInputObserver:
    def __init__(
        self,
        root: Path,
        binding: dict[str, Any],
        expected: list[dict[str, Any]],
        runtime_identity: dict[str, Any],
        implementation_identity: list[dict[str, str]],
    ) -> None:
        self.root, self.binding, self.expected = root, binding, expected
        self.origin_valid = True
        self.runtime_identity = runtime_identity
        self.implementation_identity = implementation_identity

    def pytest_collection_modifyitems(self, items: list[Any]) -> None:
        for item in items:
            if item.nodeid in self.binding["required_nodes"]:
                expected = self.root / item.nodeid.split("::", 1)[0]
                origin = getattr(getattr(item, "module", None), "__file__", None)
                if not isinstance(origin, str) or Path(origin).absolute() != expected.absolute():
                    self.origin_valid = False
                    raise ExecutionContainmentError("ACCEPTANCE_TEST_ORIGIN")

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        try:
            identity = bind_acceptance_checkout(self.root, self.binding)
            dependency_inputs: dict[Path, bytes] = {}
            runtime = acceptance_runtime_identity(captured_dependencies=dependency_inputs)
            implementation = bind_acceptance_implementation(
                self.root, self.binding["candidate_sha"], runtime_inputs=dependency_inputs
            )
            valid = (
                identity == self.expected
                and self.origin_valid
                and runtime == self.runtime_identity
                and implementation == self.implementation_identity
            )
        except ExecutionContainmentError:
            identity, valid, runtime, implementation = [], False, {}, []
        if not valid:
            session.exitstatus = 1
        if hasattr(session.config, "workeroutput"):
            session.config.workeroutput["mandatory_input_identity"] = {
                "checkout_identity": identity,
                "origin_valid": valid,
                "runtime_identity": runtime,
                "implementation_identity": implementation,
            }


def pytest_configure(config: Any) -> None:
    """Explicit -p entry point; ordinary pytest imports do not activate acceptance."""
    request_path = os.environ.get(MANDATORY_ACCEPTANCE_REQUEST_ENV)
    if request_path is None:
        return
    try:
        from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

        descriptor = json.loads(request_path)
        request_raw = bounded_regular_bytes(
            Path(descriptor["path"]), expected_identity=tuple(descriptor["identity"])
        )
        if hashlib.sha256(request_raw).hexdigest() != descriptor["sha256"]:
            raise ExecutionContainmentError("ACCEPTANCE_REQUEST_CHANGED")
        request = json.loads(request_raw)
        root = Path(request["repo_root"])
        binding = bind_mandatory_acceptance(root, request["binding"]["candidate_sha"])
        if binding != request["binding"] or Path.cwd().resolve() != root.resolve():
            raise ExecutionContainmentError("ACCEPTANCE_BINDING_CHANGED")
        checkout_identity = bind_acceptance_checkout(root, binding)
        if checkout_identity != request["checkout_identity"]:
            raise ExecutionContainmentError("ACCEPTANCE_CHECKOUT_CHANGED")
        dependency_inputs: dict[Path, bytes] = {}
        runtime_identity = acceptance_runtime_identity(captured_dependencies=dependency_inputs)
        if runtime_identity != request["runtime_identity"]:
            changed = sorted(
                key
                for key in runtime_identity
                if runtime_identity[key] != request["runtime_identity"].get(key)
            )
            raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CHANGED", ",".join(changed))
        implementation_identity = bind_acceptance_implementation(
            root, binding["candidate_sha"], runtime_inputs=dependency_inputs
        )
        if implementation_identity != request["implementation_identity"]:
            raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_CHANGED")
        config.pluginmanager.register(
            _AcceptanceInputObserver(
                root, binding, checkout_identity, runtime_identity, implementation_identity
            ),
            "aits-acceptance-inputs",
        )
        # Workers validate identity too; only the controller sees all workers' reports.
        if not hasattr(config, "workerinput"):
            config.pluginmanager.register(
                _BoundAcceptancePlugin(
                    root,
                    binding,
                    Path(request["output"]),
                    checkout_identity,
                    runtime_identity,
                    implementation_identity,
                    tuple(request["result_identity"]),
                    tuple(request["result_root_identity"]),
                ),
                "aits-mandatory-acceptance",
            )
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise ExecutionContainmentError("ACCEPTANCE_REQUEST_INVALID", str(exc)) from exc


def validate_mandatory_acceptance_result(
    raw: bytes, binding: Mapping[str, Any], *, exit_code: int, expected_collections: int
) -> dict[str, Any]:
    """Independently check captured reports rather than trusting child status PASS."""
    try:
        payload = json.loads(raw)
        execution = payload["execution"]
        required = binding["required_nodes"]
        collections = execution["collections"]
        reports = execution["reports"]
        if payload["identity_error"] is not None:
            raise ValueError("execution identity rejected: " + str(payload["identity_error"]))
        if (
            exit_code != 0
            or payload["binding"] != binding
            or payload["identity_error"] is not None
            or execution["schema_version"] != "mandatory_acceptance_execution.v1"
            or execution["status"] != "PASS"
            or execution["reason_codes"] != []
            or execution["pytest_exitstatus"] != 0
            or execution["required_nodes"] != required
            or execution["collection_count"] != expected_collections
            or len(collections) != expected_collections
            or not collections
            or set(reports) != set(required)
        ):
            raise ValueError("missing or unsuccessful bound execution")
        for row in collections:
            if (
                row != collections[0]
                or row["duplicate_count"] != 0
                or row["required_nodes"] != required
                or row["count"] < len(required)
                or not re.fullmatch(r"[0-9a-f]{64}", row["sha256"])
            ):
                raise ValueError("mandatory collection mismatch")
        for node in required:
            if sorted(reports[node]) != [
                ["call", "passed", False],
                ["setup", "passed", False],
                ["teardown", "passed", False],
            ]:
                raise ValueError("mandatory phases incomplete or unsuccessful")
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionContainmentError("ACCEPTANCE_RESULT_INVALID", str(exc)) from exc
    return dict(payload)


class _BasicLimit(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", w.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", w.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", w.DWORD),
        ("SchedulingClass", w.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_uint64)
        for name in (
            "ReadOperationCount",
            "WriteOperationCount",
            "OtherOperationCount",
            "ReadTransferCount",
            "WriteTransferCount",
            "OtherTransferCount",
        )
    ]


class _ExtendedLimit(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimit),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _Accounting(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", ctypes.c_int64),
        ("TotalKernelTime", ctypes.c_int64),
        ("ThisPeriodTotalUserTime", ctypes.c_int64),
        ("ThisPeriodTotalKernelTime", ctypes.c_int64),
        ("TotalPageFaultCount", w.DWORD),
        ("TotalProcesses", w.DWORD),
        ("ActiveProcesses", w.DWORD),
        ("TotalTerminatedProcesses", w.DWORD),
    ]


class _StartupInfo(ctypes.Structure):
    _fields_ = [
        ("cb", w.DWORD),
        ("lpReserved", w.LPWSTR),
        ("lpDesktop", w.LPWSTR),
        ("lpTitle", w.LPWSTR),
        ("dwX", w.DWORD),
        ("dwY", w.DWORD),
        ("dwXSize", w.DWORD),
        ("dwYSize", w.DWORD),
        ("dwXCountChars", w.DWORD),
        ("dwYCountChars", w.DWORD),
        ("dwFillAttribute", w.DWORD),
        ("dwFlags", w.DWORD),
        ("wShowWindow", w.WORD),
        ("cbReserved2", w.WORD),
        ("lpReserved2", ctypes.POINTER(w.BYTE)),
        ("hStdInput", w.HANDLE),
        ("hStdOutput", w.HANDLE),
        ("hStdError", w.HANDLE),
    ]


class _StartupInfoEx(ctypes.Structure):
    _fields_ = [("StartupInfo", _StartupInfo), ("lpAttributeList", ctypes.c_void_p)]


class _ProcessInfo(ctypes.Structure):
    _fields_ = [
        ("hProcess", w.HANDLE),
        ("hThread", w.HANDLE),
        ("dwProcessId", w.DWORD),
        ("dwThreadId", w.DWORD),
    ]


class _ProcessEntry32(ctypes.Structure):
    _fields_ = [
        ("dwSize", w.DWORD), ("cntUsage", w.DWORD), ("th32ProcessID", w.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", w.DWORD),
        ("cntThreads", w.DWORD), ("th32ParentProcessID", w.DWORD),
        ("pcPriClassBase", w.LONG), ("dwFlags", w.DWORD), ("szExeFile", w.WCHAR * 260),
    ]


def _api() -> Any:
    if os.name != "nt" or sys.version_info[:2] != (3, 11):
        raise ExecutionContainmentError("PLATFORM_UNSUPPORTED", "Windows/Python 3.11 required")
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateJobObjectW": ([ctypes.c_void_p, w.LPCWSTR], w.HANDLE),
        "OpenJobObjectW": ([w.DWORD, w.BOOL, w.LPCWSTR], w.HANDLE),
        "SetInformationJobObject": ([w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD], w.BOOL),
        "QueryInformationJobObject": (
            [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.c_void_p],
            w.BOOL,
        ),
        "TerminateJobObject": ([w.HANDLE, w.UINT], w.BOOL),
        "TerminateProcess": ([w.HANDLE, w.UINT], w.BOOL),
        "InitializeProcThreadAttributeList": (
            [ctypes.c_void_p, w.DWORD, w.DWORD, ctypes.POINTER(ctypes.c_size_t)],
            w.BOOL,
        ),
        "UpdateProcThreadAttribute": (
            [
                ctypes.c_void_p,
                w.DWORD,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_void_p,
            ],
            w.BOOL,
        ),
        "DeleteProcThreadAttributeList": ([ctypes.c_void_p], None),
        "CreateProcessW": (
            [
                w.LPCWSTR,
                w.LPWSTR,
                ctypes.c_void_p,
                ctypes.c_void_p,
                w.BOOL,
                w.DWORD,
                ctypes.c_void_p,
                w.LPCWSTR,
                ctypes.c_void_p,
                ctypes.POINTER(_ProcessInfo),
            ],
            w.BOOL,
        ),
        "GetCurrentProcess": ([], w.HANDLE),
        "GetProcessId": ([w.HANDLE], w.DWORD),
        "CreateToolhelp32Snapshot": ([w.DWORD, w.DWORD], w.HANDLE),
        "Process32FirstW": ([w.HANDLE, ctypes.POINTER(_ProcessEntry32)], w.BOOL),
        "Process32NextW": ([w.HANDLE, ctypes.POINTER(_ProcessEntry32)], w.BOOL),
        "DuplicateHandle": (
            [w.HANDLE, w.HANDLE, w.HANDLE, ctypes.POINTER(w.HANDLE), w.DWORD, w.BOOL, w.DWORD],
            w.BOOL,
        ),
        "CloseHandle": ([w.HANDLE], w.BOOL),
        "ResumeThread": ([w.HANDLE], w.DWORD),
        "WaitForSingleObject": ([w.HANDLE, w.DWORD], w.DWORD),
        "GetExitCodeProcess": ([w.HANDLE, ctypes.POINTER(w.DWORD)], w.BOOL),
        "OpenProcess": ([w.DWORD, w.BOOL, w.DWORD], w.HANDLE),
        "IsProcessInJob": ([w.HANDLE, w.HANDLE, ctypes.POINTER(w.BOOL)], w.BOOL),
        "GetProcessTimes": (
            [
                w.HANDLE,
                ctypes.POINTER(w.FILETIME),
                ctypes.POINTER(w.FILETIME),
                ctypes.POINTER(w.FILETIME),
                ctypes.POINTER(w.FILETIME),
            ],
            w.BOOL,
        ),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(api, name)
        function.argtypes, function.restype = arguments, result
    return api


def _checked(result: object, operation: str) -> None:
    if not result:
        raise ExecutionContainmentError(operation, f"winerror={ctypes.get_last_error()}")


def _creation_time(api: Any, handle: Any) -> int:
    created, exited, kernel, user = (w.FILETIME() for _ in range(4))
    _checked(
        api.GetProcessTimes(
            handle,
            ctypes.byref(created),
            ctypes.byref(exited),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ),
        "PROCESS_IDENTITY",
    )
    return (created.dwHighDateTime << 32) | created.dwLowDateTime


def _job_name(value: str) -> str:
    if not re.fullmatch(r"Local\\AITS-DEVX015-[A-Za-z0-9-]{8,96}", value):
        raise ExecutionContainmentError("JOB_NAME", "exact task-identifiable local name required")
    return value


def current_process_identity() -> dict[str, int]:
    api = _api()
    return {"pid": os.getpid(), "creation_time": _creation_time(api, api.GetCurrentProcess())}


def current_job_member(job_name: str) -> dict[str, int]:
    """Prove current process membership in the exact existing Windows Job.

    A venv redirector can be the primary process while this Python interpreter
    is its contained descendant. A caller-supplied PID or job-name string is
    never treated as proof of membership, and no job is created or terminated.
    """
    api = _api()
    job = api.OpenJobObjectW(0x0004, False, _job_name(job_name))  # JOB_OBJECT_QUERY
    if not job:
        raise ExecutionContainmentError("JOB_MEMBERSHIP_UNPROVEN")
    try:
        member = w.BOOL()
        current = api.GetCurrentProcess()
        _checked(api.IsProcessInJob(current, job, ctypes.byref(member)), "JOB_MEMBERSHIP_QUERY")
        if not member.value:
            raise ExecutionContainmentError("JOB_MEMBERSHIP_MISMATCH")
        return {"pid": os.getpid(), "creation_time": _creation_time(api, current)}
    finally:
        _checked(api.CloseHandle(job), "CLOSE_MEMBERSHIP_JOB")


def contained_subprocess_identity(
    process: subprocess.Popen[bytes], job_name: str,
) -> dict[str, int]:
    """Observe a live inherited child through its original Popen kernel handle.

    This grants no launch/publication authority. Its caller must already be in
    the original Job; no breakaway, replacement Job or PID-only adoption occurs.
    """
    current_job_member(job_name)
    if not isinstance(process, subprocess.Popen):
        raise ExecutionContainmentError("SUBPROCESS_HANDLE_REQUIRED")
    api = _api()
    handle = getattr(process, "_handle", None)
    if handle is None or process.poll() is not None:
        raise ExecutionContainmentError("SUBPROCESS_NOT_LIVE")
    pid = int(api.GetProcessId(handle))
    if pid != process.pid or api.WaitForSingleObject(handle, 0) != _WAIT_TIMEOUT:
        raise ExecutionContainmentError("SUBPROCESS_IDENTITY")
    job = api.OpenJobObjectW(0x0004, False, _job_name(job_name))
    _checked(job, "OPEN_SUBPROCESS_JOB")
    try:
        member = w.BOOL()
        _checked(api.IsProcessInJob(handle, job, ctypes.byref(member)), "SUBPROCESS_MEMBERSHIP")
        if not member.value:
            raise ExecutionContainmentError("SUBPROCESS_MEMBERSHIP_MISMATCH")
        return {"pid": pid, "creation_time": _creation_time(api, handle)}
    finally:
        _checked(api.CloseHandle(job), "CLOSE_SUBPROCESS_JOB")


def _process_parent_snapshot(api: Any) -> dict[int, int]:
    """Only a lookup aid; snapshot PIDs are not retained-process identities."""
    snapshot = api.CreateToolhelp32Snapshot(0x2, 0)  # TH32CS_SNAPPROCESS
    if snapshot == ctypes.c_void_p(-1).value:
        raise ExecutionContainmentError("ANCESTRY_SNAPSHOT", str(ctypes.get_last_error()))
    try:
        entry = _ProcessEntry32()
        entry.dwSize = ctypes.sizeof(entry)
        _checked(api.Process32FirstW(snapshot, ctypes.byref(entry)), "ANCESTRY_FIRST")
        parents: dict[int, int] = {}
        while True:
            pid = int(entry.th32ProcessID)
            if pid in parents or len(parents) >= 65536:
                raise ExecutionContainmentError("ANCESTRY_SNAPSHOT_INVALID")
            parents[pid] = int(entry.th32ParentProcessID)
            if not api.Process32NextW(snapshot, ctypes.byref(entry)):
                if ctypes.get_last_error() != 18:  # ERROR_NO_MORE_FILES
                    raise ExecutionContainmentError("ANCESTRY_NEXT")
                return parents
    finally:
        _checked(api.CloseHandle(snapshot), "CLOSE_ANCESTRY_SNAPSHOT")


@contextmanager
def hold_contained_ancestor(
    job_name: str, ancestor: Mapping[str, int],
) -> Iterator[tuple[dict[str, int], ...]]:
    """Hold a live native chain from this hook to the original contained Git.

    The publication lifecycle must supply its own original Git PID/creation-time
    binding. Same-Job membership alone or a caller's serialized PID is not proof.
    All handles remain held through the caller's effect check, are rechecked on
    exit, and are closed without terminating any process or creating a Job.
    This primitive observes origin only; it grants no lease/publication authority.
    """
    if (not isinstance(ancestor, Mapping) or set(ancestor) != {"pid", "creation_time"}
            or any(type(value) is not int or value <= 0 for value in ancestor.values())
            or ancestor["pid"] == os.getpid()):
        raise ExecutionContainmentError("ANCESTRY_BINDING")
    api = _api()
    current = current_job_member(job_name)
    with ExitStack() as stack:
        def keep(handle: Any, operation: str) -> Any:
            _checked(handle, operation)
            stack.callback(lambda: _checked(api.CloseHandle(handle), "CLOSE_ANCESTRY_HANDLE"))
            return handle

        def live_member(handle: Any) -> bool:
            # Never retain a query Job handle across yield: it would defeat the
            # original launcher's kill-on-last-handle containment on parent death.
            job = api.OpenJobObjectW(0x0004, False, _job_name(job_name))
            _checked(job, "ANCESTRY_JOB")
            try:
                member = w.BOOL()
                _checked(api.IsProcessInJob(handle, job, ctypes.byref(member)),
                         "ANCESTRY_MEMBERSHIP")
                return bool(member.value) and api.WaitForSingleObject(handle, 0) == _WAIT_TIMEOUT
            finally:
                _checked(api.CloseHandle(job), "CLOSE_ANCESTRY_JOB")

        parents = _process_parent_snapshot(api)
        retained: list[tuple[Any, dict[str, int]]] = []
        pid = current["pid"]
        for _ in range(8):  # Git launcher, Git, sh, venv redirector and hook fit this bound.
            if not pid or any(row["pid"] == pid for _, row in retained):
                raise ExecutionContainmentError("ANCESTRY_CHAIN")
            handle = keep(api.OpenProcess(0x1000 | 0x100000, False, pid), "ANCESTRY_PROCESS")
            row = {"pid": int(api.GetProcessId(handle)),
                   "creation_time": _creation_time(api, handle)}
            if row["pid"] != pid or not live_member(handle):
                raise ExecutionContainmentError("ANCESTRY_NOT_LIVE_MEMBER")
            if not retained and row != current:
                raise ExecutionContainmentError("ANCESTRY_CURRENT_CHANGED")
            if retained and row["creation_time"] > retained[-1][1]["creation_time"]:
                raise ExecutionContainmentError("ANCESTRY_PARENT_REUSED")
            retained.append((handle, row))
            if pid == ancestor["pid"]:
                if row != dict(ancestor):
                    raise ExecutionContainmentError("ANCESTRY_ORIGINAL_MISMATCH")
                break
            pid = parents.get(pid, 0)
        else:
            raise ExecutionContainmentError("ANCESTRY_DEPTH")

        def recheck() -> None:
            observed_parents = _process_parent_snapshot(api)
            for index, (handle, row) in enumerate(retained):
                if (not live_member(handle)
                        or int(api.GetProcessId(handle)) != row["pid"]
                        or _creation_time(api, handle) != row["creation_time"]
                        or (index + 1 < len(retained) and observed_parents.get(row["pid"])
                            != retained[index + 1][1]["pid"])):
                    raise ExecutionContainmentError("ANCESTRY_CHANGED")

        recheck()
        yield tuple(dict(row) for _, row in retained)
        recheck()


def execution_environment_sha256(environment: Mapping[str, str]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(environment), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _active(api: Any, job: Any) -> int:
    accounting = _Accounting()
    _checked(
        api.QueryInformationJobObject(
            job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None
        ),
        "JOB_QUERY",
    )
    return int(accounting.ActiveProcesses)


# DEVX-018 v21: bounded settle time for an unexplained short Job process list. A member that is
# exiting can be assigned to the Job without being listed for a moment, and under formal Full load
# that window outlasts the former back-to-back retries (v20 gave up within a millisecond and the
# contained Job was terminated). Engineering bound, not a model rule: provisional, owner review
# pending (docs/requirements/DEVX-018_Validation_Runtime_Throughput_V1.md); a gap that persists
# through the whole window still fails closed exactly as before.
JOB_LIST_SETTLE_FIRST_SECONDS = 0.001
JOB_LIST_SETTLE_MAX_SECONDS = 0.512


def _job_list_settle(seconds: float) -> None:
    time.sleep(seconds)


class _JobProcesses:
    """Keep observed kernel identities until their process objects are signaled."""

    def __init__(self, api: Any, job: Any) -> None:
        self.api, self.job = api, job
        self.handles: dict[tuple[int, int], Any] = {}

    def _signaled_retained_count(self) -> int:
        signaled = 0
        for handle in self.handles.values():
            state = self.api.WaitForSingleObject(handle, 0)
            if state not in {0, _WAIT_TIMEOUT}:
                raise ExecutionContainmentError("WAIT_PROCESS", str(state))
            signaled += state == 0
        return signaled

    def _short_list_explained(
        self, assigned: int, count: int, previous: tuple[int, int] | None,
    ) -> bool:
        """GOV-007 F2: a short list is complete only when our own handles explain it.

        A retained handle keeps an exited process object alive, so the job still
        counts it as assigned while the PID list names only active members (v13
        native evidence: assigned=65, listed=64=ActiveProcesses, retained=65).
        Accept only a successful list that repeats the previous successful query
        exactly, equals ActiveProcesses, and whose gap is at most our retained
        signaled identities. Anything else keeps the bounded fail-closed path.
        """
        if previous != (assigned, count):
            return False
        if assigned - count > self._signaled_retained_count():
            return False
        try:
            return _active(self.api, self.job) == count
        except ExecutionContainmentError:
            return False  # Unobservable accounting keeps the original bounded failure.

    def collect(self) -> None:
        capacity = 16
        queries: list[dict[str, int | bool]] = []
        previous_short: tuple[int, int] | None = None
        settles = 0
        # Bounded allocation/retry guard, not a concurrency scheduling policy.
        while capacity <= 65536:

            class ProcessList(ctypes.Structure):
                _fields_ = [
                    ("assigned", w.DWORD),
                    ("count", w.DWORD),
                    ("pids", ctypes.c_size_t * capacity),
                ]

            listing = ProcessList()
            ok = self.api.QueryInformationJobObject(
                self.job, 3, ctypes.byref(listing), ctypes.sizeof(listing), None
            )
            error = ctypes.get_last_error() if not ok else 0
            if not ok and error != 234:
                _checked(ok, "JOB_PROCESS_LIST")
            short = ok and listing.count < listing.assigned
            if short and self._short_list_explained(
                int(listing.assigned), int(listing.count), previous_short,
            ):
                short = False
            elif short:
                previous_short = (int(listing.assigned), int(listing.count))
            else:
                previous_short = None
            if not ok or short:
                queries.append({"capacity": capacity, "ok": bool(ok), "winerror": error,
                                "assigned": int(listing.assigned), "count": int(listing.count)})
                capacity = max(capacity * 2, int(listing.assigned))
                if ok and capacity <= 65536:
                    # The buffer was large enough, so this is an unexplained short list, not
                    # a growth step: give a transient state time to resolve before re-querying.
                    _job_list_settle(min(JOB_LIST_SETTLE_FIRST_SECONDS * 2 ** settles,
                                         JOB_LIST_SETTLE_MAX_SECONDS))
                    settles += 1
                continue
            if listing.count > capacity:
                raise ExecutionContainmentError("JOB_PROCESS_LIST_BOUNDS")
            for pid in listing.pids[: listing.count]:
                process = self.api.OpenProcess(_PROCESS_QUERY_SYNCHRONIZE, False, pid)
                if not process:
                    if ctypes.get_last_error() == 87:
                        continue  # Gone before OpenProcess; no live PID to retain.
                    _checked(process, "JOB_PROCESS_OPEN")
                retained = False
                try:
                    member = w.BOOL()
                    _checked(
                        self.api.IsProcessInJob(process, self.job, ctypes.byref(member)),
                        "JOB_MEMBERSHIP",
                    )
                    identity = (int(pid), _creation_time(self.api, process))
                    if member.value and identity not in self.handles:
                        self.handles[identity] = process
                        retained = True
                finally:
                    if not retained:
                        _checked(self.api.CloseHandle(process), "CLOSE_PROCESS")
            return
        # Keep the same bounded, fail-closed decision. v8 lacked the native
        # counters needed to distinguish buffer exhaustion from a short list
        # during process exit; diagnostics are observations, never exit proof.
        detail: dict[str, Any] = {"queries": queries, "retained_process_count": len(self.handles)}
        try:
            detail["active_process_count"] = _active(self.api, self.job)
        except ExecutionContainmentError as exc:
            detail["accounting_error_code"] = exc.code
        failure = ExecutionContainmentError(
            "JOB_PROCESS_LIST_BUDGET", json.dumps(detail, sort_keys=True),
        )
        failure._job_process_list_diagnostics = detail
        raise failure

    def exited(self) -> bool:
        complete = True
        for handle in self.handles.values():
            state = self.api.WaitForSingleObject(handle, 0)
            if state not in {0, _WAIT_TIMEOUT}:
                raise ExecutionContainmentError("WAIT_PROCESS", str(state))
            complete = complete and state == 0
        return complete

    def timeout_detail(self, primary_exit_code: int | None, active_count: int) -> str:
        # Sequential diagnostic observations, never an exit/release certificate.
        observed = []
        for (pid, created), handle in sorted(self.handles.items()):
            state = int(self.api.WaitForSingleObject(handle, 0))
            observed.append({"pid": pid, "creation_time": created, "wait_status": state})
        return json.dumps(
            {
                "primary_exit_code": primary_exit_code,
                "job_active_process_count": active_count,
                "observed_processes": observed,
                "observation_only": True,
            },
            sort_keys=True,
        )

    def __enter__(self) -> _JobProcesses:
        return self

    def __exit__(self, *_: object) -> None:
        failures = []
        for handle in self.handles.values():
            if not self.api.CloseHandle(handle):
                failures.append(ctypes.get_last_error())
        if failures:
            raise ExecutionContainmentError("CLEANUP_UNCONFIRMED", str(failures))


def observe_process(pid: int, creation_time: int) -> dict[str, object]:
    api = _api()
    if type(pid) is not int or pid <= 0 or type(creation_time) is not int or creation_time <= 0:
        raise ExecutionContainmentError("PROCESS_IDENTITY", "positive pid/creation time required")
    handle = api.OpenProcess(_PROCESS_QUERY_SYNCHRONIZE, False, pid)
    if not handle:
        code = ctypes.get_last_error()
        return {"state": "EXITED" if code == 87 else "UNKNOWN", "winerror": code}
    try:
        if _creation_time(api, handle) != creation_time:
            return {"state": "REUSED"}
        observed = api.WaitForSingleObject(handle, 0)
        return {
            "state": "RUNNING"
            if observed == _WAIT_TIMEOUT
            else ("EXITED" if observed == 0 else "UNKNOWN")
        }
    finally:
        _checked(api.CloseHandle(handle), "CLOSE_PROCESS")


def _timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExecutionContainmentError("TIMEOUT", "finite nonnegative seconds required")
    if not math.isfinite(value) or value < 0:
        raise ExecutionContainmentError("TIMEOUT", "finite nonnegative seconds required")
    return float(value)


def observe_job(
    job_name: str,
    *,
    terminate: bool = False,
    timeout: float = CONTAINED_TERMINATION_CONFIRMATION_SECONDS,
    expected_process: Mapping[str, int] | None = None,
) -> dict[str, object]:
    api = _api()
    timeout = _timeout(timeout)
    name = _job_name(job_name)
    job = api.OpenJobObjectW(_JOB_QUERY_TERMINATE, False, name)
    if not job:
        code = ctypes.get_last_error()
        return {
            "state": "ABSENT" if code == 2 else "UNKNOWN",
            "active_process_count": None,
            "winerror": code,
        }
    try:
        if terminate:
            # A name is not a generation token. Require a still-identifiable
            # process from the frozen execution and its actual Job membership.
            if expected_process is None:
                raise ExecutionContainmentError("JOB_IDENTITY_REQUIRED")
            pid = expected_process.get("pid")
            created = expected_process.get("creation_time")
            if type(pid) is not int or pid <= 0 or type(created) is not int or created <= 0:
                raise ExecutionContainmentError("JOB_IDENTITY_REQUIRED")
            process = api.OpenProcess(_PROCESS_QUERY_SYNCHRONIZE, False, pid)
            if not process:
                raise ExecutionContainmentError("JOB_IDENTITY_UNPROVEN")
            try:
                member = w.BOOL()
                _checked(api.IsProcessInJob(process, job, ctypes.byref(member)), "JOB_MEMBERSHIP")
                if _creation_time(api, process) != created or not member.value:
                    raise ExecutionContainmentError("JOB_IDENTITY_MISMATCH")
            finally:
                _checked(api.CloseHandle(process), "CLOSE_PROCESS")
        with _JobProcesses(api, job) as processes:
            processes.collect()
            if terminate:
                _checked(api.TerminateJobObject(job, _ABORTED), "TERMINATE_JOB")
            deadline = time.monotonic() + timeout
            while True:
                processes.collect()
                count = _active(api, job)
                complete = count == 0 and processes.exited()
                if not terminate or complete or time.monotonic() >= deadline:
                    return {
                        "state": "EMPTY" if complete else "ACTIVE",
                        "active_process_count": count,
                    }
                time.sleep(0.02)
    finally:
        _checked(api.CloseHandle(job), "CLOSE_JOB")


def _token_api() -> Any:
    security = ctypes.WinDLL("advapi32", use_last_error=True)
    security.LogonUserW.argtypes = [
        w.LPCWSTR, w.LPCWSTR, ctypes.c_void_p, w.DWORD, w.DWORD,
        ctypes.POINTER(w.HANDLE),
    ]
    security.LogonUserW.restype = w.BOOL
    security.OpenProcessToken.argtypes = [w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)]
    security.OpenProcessToken.restype = w.BOOL
    security.GetTokenInformation.argtypes = [
        w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.POINTER(w.DWORD),
    ]
    security.GetTokenInformation.restype = w.BOOL
    security.ConvertSidToStringSidW.argtypes = [ctypes.c_void_p, ctypes.POINTER(w.LPWSTR)]
    security.ConvertSidToStringSidW.restype = w.BOOL
    security.DuplicateTokenEx.argtypes = [
        w.HANDLE, w.DWORD, ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.POINTER(w.HANDLE),
    ]
    security.DuplicateTokenEx.restype = w.BOOL
    security.CreateProcessAsUserW.argtypes = [
        w.HANDLE, w.LPCWSTR, w.LPWSTR, ctypes.c_void_p, ctypes.c_void_p, w.BOOL,
        w.DWORD, ctypes.c_void_p, w.LPCWSTR, ctypes.c_void_p, ctypes.POINTER(_ProcessInfo),
    ]
    security.CreateProcessAsUserW.restype = w.BOOL
    return security


def _token_dword(security: Any, token: int | w.HANDLE | None, kind: int) -> int:
    value, size = w.DWORD(), w.DWORD()
    _checked(
        security.GetTokenInformation(
            token, kind, ctypes.byref(value), ctypes.sizeof(value), ctypes.byref(size),
        ),
        "TOKEN_INFORMATION",
    )
    if size.value != ctypes.sizeof(value):
        raise ExecutionContainmentError("TOKEN_INFORMATION_SIZE")
    return int(value.value)


def _token_identity(api: Any, security: Any, token: int | w.HANDLE | None) -> dict[str, object]:
    api.LocalFree.argtypes = [ctypes.c_void_p]
    api.LocalFree.restype = ctypes.c_void_p
    size = w.DWORD()
    ctypes.set_last_error(0)
    sized = security.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
    if sized or ctypes.get_last_error() != 122 or not 0 < size.value <= 65536:
        raise ExecutionContainmentError("TOKEN_USER_SIZE")
    user = ctypes.create_string_buffer(size.value)
    _checked(
        security.GetTokenInformation(token, 1, user, size.value, ctypes.byref(size)),
        "TOKEN_USER",
    )
    # TOKEN_USER begins with SID_AND_ATTRIBUTES.Sid (pointer-width aligned).
    sid_pointer = ctypes.cast(user, ctypes.POINTER(ctypes.c_void_p))[0]
    sid_text = w.LPWSTR()
    _checked(security.ConvertSidToStringSidW(sid_pointer, ctypes.byref(sid_text)), "TOKEN_SID")
    try:
        sid = sid_text.value
    finally:
        if api.LocalFree(ctypes.cast(sid_text, ctypes.c_void_p)):
            raise ExecutionContainmentError("TOKEN_SID_FREE")
    elevated = _token_dword(security, token, 20)
    if not sid or elevated not in {0, 1}:
        raise ExecutionContainmentError("TOKEN_IDENTITY_INVALID")
    return {"sid": sid, "elevated": bool(elevated)}


def _process_primary_token(api: Any, process: int) -> dict[str, object]:
    """Read the actual process token, never the calling thread's impersonation token."""
    security = _token_api()
    token = w.HANDLE()
    _checked(security.OpenProcessToken(process, 0x0008, ctypes.byref(token)), "OPEN_PROCESS_TOKEN")
    try:
        return _token_identity(api, security, token)
    finally:
        _checked(api.CloseHandle(token), "CLOSE_PROCESS_TOKEN")


class WindowsWorkerToken:
    """Owned native capability supplied by a trusted launcher, never by request JSON.

    Adoption is not enrollment or account approval. The trusted entrypoint must
    obtain the handle and expected SID from its protected identity configuration.
    No password, token handle or live capability is persisted in workflow receipts.
    """

    _api: Any
    _security: Any
    _handle: int | None
    _sid: str
    _session: int
    _owner: tuple[int, int]

    def __init__(self) -> None:
        raise ExecutionContainmentError("WORKER_TOKEN_FACTORY_REQUIRED")

    @classmethod
    @contextmanager
    def logon_registered(
        cls, *, candidate_root: Path, candidate_sha: str,
        git_context: HeldGitConfiguration, password_buffer: Any = None,
    ) -> Iterator[WindowsWorkerToken]:
        """Consume a fixed administrator-owned identity under continuous custody.

        This local identity binding supplements, never replaces, machine/repository
        registration. It stores no secret and grants no launch/publication right.
        Installation of the binding and account changes are separate operations.
        With no buffer, consume fixed worker-credential.dpapi under confidential
        custody, bound to SHA256 of the exact held identity bytes. No fallback.
        """
        if password_buffer is not None and (not isinstance(password_buffer, ctypes.Array)
                or password_buffer._type_ is not ctypes.c_wchar):
            raise ExecutionContainmentError("WORKER_PASSWORD_BUFFER_REQUIRED")
        try:
            from ai_trading_system.platform.architecture.workflow_contract import (
                bounded_regular_bytes,
            )
            from ai_trading_system.platform.architecture.workflow_coordination import (
                _WindowsEnrollmentAdministrator,
                machine_host_id,
            )
            from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

            administrator = _WindowsEnrollmentAdministrator()
            bind_protected_inspector_runtime(candidate_root, candidate_sha, git_context=git_context)
            path = Path(sys.executable).absolute().parent.parent / "worker-identity.json"
            administrator.assert_protected(path.parent)
            administrator.assert_protected(path)
            # Identity is four small fields; bound allocation before parsing.
            raw = bounded_regular_bytes(path, budget=16 * 1024)
            digest = hashlib.sha256(raw).hexdigest()
            with administrator.hold_protected_files({path: digest}):
                identity = load_strict_json_text(raw.decode("utf-8"))
                if (not isinstance(identity, dict)
                        or set(identity) != {"schema_version", "host_id", "account", "sid"}
                        or identity["schema_version"] != "devx015_worker_identity.v1"
                        or identity["host_id"] != machine_host_id()):
                    raise ExecutionContainmentError("WORKER_REGISTERED_IDENTITY_INVALID")
                git_context.assert_current(candidate_root, {})
                with ExitStack() as secrets:
                    active_password = password_buffer
                    if active_password is None:
                        encrypted = secrets.enter_context(administrator.hold_confidential_file(
                            path.parent / "worker-credential.dpapi",
                        ))
                        active_password = secrets.enter_context(decrypted_worker_password(
                            encrypted, hashlib.sha256(raw).digest(),
                        ))
                    with cls.logon_local(
                        account=identity["account"], expected_sid=identity["sid"],
                        password_buffer=active_password,
                    ) as worker:
                        yield worker
                git_context.assert_current(candidate_root, {})
        finally:
            if password_buffer is not None:
                ctypes.memset(ctypes.addressof(password_buffer), 0, ctypes.sizeof(password_buffer))

    @classmethod
    @contextmanager
    def logon_local(
        cls, *, account: str, password_buffer: Any, expected_sid: str,
    ) -> Iterator[WindowsWorkerToken]:
        """Acquire a local worker token for an already authorized trusted caller.

        Account/SID must come from protected identity configuration. The caller
        supplies an exclusively owned, NUL-terminated ctypes wchar array, never
        a password string or request JSON. Its bytes are consumed and zeroed,
        including on rejection, before yielding. This does not enroll/enable an
        account, grant privileges, load a profile, or authorize a process launch.
        LOGON32_LOGON_INTERACTIVE (2), DEFAULT (0), local domain (.) deliberately
        match the reviewed local-account canary; there is no retry or fallback.
        See Microsoft LogonUserW's token and password-buffer lifetime contract.
        """
        if (not isinstance(password_buffer, ctypes.Array)
                or password_buffer._type_ is not ctypes.c_wchar):
            raise ExecutionContainmentError("WORKER_PASSWORD_BUFFER_REQUIRED")
        try:
            if (len(password_buffer) < 2 or password_buffer[-1] != "\0"
                    or any(password_buffer[index] == "\0"
                           for index in range(len(password_buffer) - 1))):
                raise ExecutionContainmentError("WORKER_PASSWORD_BUFFER_INVALID")
            if (not isinstance(account, str) or not account or account != account.strip()
                    or any(character in account for character in "\\/@\0")
                    or not isinstance(expected_sid, str)
                    or re.fullmatch(r"S-1-5-21-(\d+-){3}\d+", expected_sid) is None):
                raise ExecutionContainmentError("WORKER_LOCAL_IDENTITY_INVALID")
            api, security = _api(), _token_api()
            original = w.HANDLE()
            try:
                try:
                    _checked(security.LogonUserW(
                        account, ".", ctypes.cast(password_buffer, ctypes.c_void_p),
                        2, 0, ctypes.byref(original),
                    ), "WORKER_LOGON")
                finally:
                    ctypes.memset(ctypes.addressof(password_buffer), 0,
                                  ctypes.sizeof(password_buffer))
                if original.value is None:
                    raise ExecutionContainmentError("WORKER_LOGON_TOKEN_MISSING")
                with cls.from_primary_handle(original.value, expected_sid=expected_sid) as worker:
                    worker.validate_launcher()
                    yield worker
            finally:
                if original.value:
                    _checked(api.CloseHandle(original), "CLOSE_WORKER_LOGON_TOKEN")
        finally:
            ctypes.memset(ctypes.addressof(password_buffer), 0, ctypes.sizeof(password_buffer))

    @classmethod
    def from_primary_handle(cls, handle: int, *, expected_sid: str) -> WindowsWorkerToken:
        api, security = _api(), _token_api()
        if not isinstance(expected_sid, str) or not re.fullmatch(
            r"S-1-5-21-(\d+-){3}\d+", expected_sid,
        ):
            raise ExecutionContainmentError("WORKER_SID_INVALID")
        if _token_dword(security, handle, 8) != 1:  # TokenType must already be primary.
            raise ExecutionContainmentError("WORKER_PRIMARY_TOKEN_REQUIRED")
        duplicated = w.HANDLE()
        # TOKEN_ASSIGN_PRIMARY | TOKEN_DUPLICATE | TOKEN_QUERY, non-inheritable.
        _checked(security.DuplicateTokenEx(handle, 0x000B, None, 2, 1, ctypes.byref(duplicated)),
                 "DUPLICATE_WORKER_TOKEN")
        try:
            identity = _token_identity(api, security, duplicated.value)
            if identity != {"sid": expected_sid, "elevated": False}:
                raise ExecutionContainmentError("WORKER_TOKEN_IDENTITY")
            instance = object.__new__(cls)
            instance._api = api
            instance._security = security
            instance._handle = duplicated.value
            instance._sid = expected_sid
            instance._session = _token_dword(security, duplicated.value, 12)
            instance._owner = (os.getpid(), threading.get_ident())
            return instance
        except BaseException:
            _checked(api.CloseHandle(duplicated), "CLOSE_WORKER_TOKEN")
            raise

    def _checked_handle(self) -> int:
        if not self._handle or self._owner != (os.getpid(), threading.get_ident()):
            raise ExecutionContainmentError("WORKER_TOKEN_OWNER")
        if (
            _token_identity(self._api, self._security, self._handle)
            != {"sid": self._sid, "elevated": False}
            or _token_dword(self._security, self._handle, 8) != 1
            or _token_dword(self._security, self._handle, 12) != self._session
        ):
            raise ExecutionContainmentError("WORKER_TOKEN_CHANGED")
        return self._handle

    def binding(self) -> dict[str, object]:
        """Observed metadata only; this mapping cannot replace the live capability."""
        self._checked_handle()
        return {"sid": self._sid, "elevated": False, "session_id": self._session}

    def validate_launcher(self) -> dict[str, object]:
        binding = self.binding()
        api, security = self._api, self._security
        launcher = _process_primary_token(api, api.GetCurrentProcess())
        if launcher["sid"] == self._sid:
            raise ExecutionContainmentError("WORKER_PRINCIPAL_NOT_SEPARATE")
        caller = w.HANDLE()
        _checked(security.OpenProcessToken(api.GetCurrentProcess(), 0x0008,
                                          ctypes.byref(caller)), "OPEN_PROCESS_TOKEN")
        try:
            if _token_dword(security, caller.value, 12) != self._session:
                raise ExecutionContainmentError("WORKER_SESSION_MISMATCH")
        finally:
            _checked(api.CloseHandle(caller), "CLOSE_PROCESS_TOKEN")
        return binding

    def close(self) -> None:
        if self._owner != (os.getpid(), threading.get_ident()):
            raise ExecutionContainmentError("WORKER_TOKEN_OWNER")
        if self._handle:
            _checked(self._api.CloseHandle(self._handle), "CLOSE_WORKER_TOKEN")
            self._handle = None

    def __enter__(self) -> WindowsWorkerToken:
        self._checked_handle()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class WindowsJobProcess:
    """A live handle capability; receipts/serialized dictionaries cannot resume it."""

    _api: Any
    _job: int | None
    _process: int | None
    _thread: int | None
    _files: list[BinaryIO]
    _resumed: bool
    _closed: bool
    _owner_pid: int
    _owner_thread: int
    _identity: dict[str, object]
    _launch_binding: dict[str, object]
    _owns_job: bool

    @classmethod
    def create(
        cls,
        *,
        argv: Sequence[str],
        cwd: Path,
        environment: Mapping[str, str],
        stdout_path: Path,
        job_name: str,
    ) -> WindowsJobProcess:
        return cls._create(
            argv=argv, cwd=cwd, environment=environment, stdout_path=stdout_path, job_name=job_name,
        )

    @classmethod
    def create_as_worker(
        cls, *, worker_token: WindowsWorkerToken, argv: Sequence[str], cwd: Path,
        environment: Mapping[str, str], stdout_path: Path, job_name: str,
    ) -> WindowsJobProcess:
        if type(worker_token) is not WindowsWorkerToken:
            raise ExecutionContainmentError("WORKER_TOKEN_CAPABILITY_REQUIRED")
        return cls._create(
            argv=argv, cwd=cwd, environment=environment, stdout_path=stdout_path, job_name=job_name,
            worker_token=worker_token,
        )

    @classmethod
    def _create(
        cls, *, argv: Sequence[str], cwd: Path, environment: Mapping[str, str],
        stdout_path: Path, job_name: str, inherited_job: bool = False,
        directory_handles: Sequence[int] = (),
        worker_token: WindowsWorkerToken | None = None,
    ) -> WindowsJobProcess:
        api = _api()
        if worker_token is None:
            launcher_token = _process_primary_token(api, api.GetCurrentProcess())
            expected_token = launcher_token
            if launcher_token["elevated"] or launcher_token["sid"] in {
                "S-1-5-18", "S-1-5-19", "S-1-5-20",
            }:
                raise ExecutionContainmentError("PRIVILEGED_CANDIDATE_LAUNCH")
        else:
            if type(worker_token) is not WindowsWorkerToken or inherited_job or directory_handles:
                raise ExecutionContainmentError("WORKER_TOKEN_CAPABILITY_REQUIRED")
            worker_token.validate_launcher()
            expected_token = {"sid": worker_token._sid, "elevated": False}
        argv = tuple(argv)
        environment = dict(environment)
        if (
            not argv
            or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
            or not Path(argv[0]).is_absolute()
            or not Path(argv[0]).is_file()
            or not cwd.is_absolute()
            or not cwd.is_dir()
        ):
            raise ExecutionContainmentError("REQUEST", "absolute executable and cwd required")
        if any(
            not isinstance(k, str)
            or not k
            or "=" in k
            or "\0" in k
            or not isinstance(v, str)
            or "\0" in v
            for k, v in environment.items()
        ):
            raise ExecutionContainmentError("ENVIRONMENT", "invalid explicit environment")
        if len({key.casefold() for key in environment}) != len(environment):
            raise ExecutionContainmentError("ENVIRONMENT", "case-aliased environment keys")
        name = _job_name(job_name)
        if inherited_job:
            current_job_member(name)  # Before creating stdout or any child process.
        instance = cls()
        instance._api = api
        instance._job = None
        instance._process = None
        instance._thread = None
        instance._files = []
        instance._resumed = False
        instance._closed = False
        instance._owner_pid = os.getpid()
        instance._owner_thread = threading.get_ident()
        instance._identity = {}
        instance._launch_binding = {
            "argv": list(argv),
            "cwd": cwd.resolve().as_posix(),
            "environment_sha256": execution_environment_sha256(environment),
            "stdout_path": stdout_path.absolute().as_posix(),
        }
        instance._owns_job = not inherited_job
        inherited: list[Any] = []
        attributes = None
        failure: BaseException | None = None
        cleanup_errors: list[str] = []
        try:
            ctypes.set_last_error(0)
            if inherited_job:
                # Query only: this capability never owns or terminates the parent Job.
                instance._job = api.OpenJobObjectW(0x0004, False, name)
                _checked(instance._job, "OPEN_INHERITED_JOB")
            else:
                instance._job = api.CreateJobObjectW(None, name)
                _checked(instance._job, "CREATE_JOB")
                if ctypes.get_last_error() == 183:
                    # Never terminate or adopt someone else's preexisting job.
                    api.CloseHandle(instance._job)
                    instance._job = None
                    raise ExecutionContainmentError("JOB_EXISTS", name)
                limits = _ExtendedLimit()
                limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                _checked(
                    api.SetInformationJobObject(
                        instance._job, 9, ctypes.byref(limits), ctypes.sizeof(limits)
                    ),
                    "JOB_LIMITS",
                )
            output = stdout_path.open("xb")
            instance._files.append(output)
            null_input = open(os.devnull, "rb")
            instance._files.append(null_input)
            import msvcrt

            current = api.GetCurrentProcess()
            for stream in (null_input, output):
                duplicate = w.HANDLE()
                _checked(
                    api.DuplicateHandle(
                        current,
                        msvcrt.get_osfhandle(stream.fileno()),
                        current,
                        ctypes.byref(duplicate),
                        0,
                        True,
                        2,
                    ),
                    "DUPLICATE_STDIO",
                )
                inherited.append(duplicate.value)
            size = ctypes.c_size_t()
            attribute_count = 1 if inherited_job else 2
            api.InitializeProcThreadAttributeList(None, attribute_count, 0, ctypes.byref(size))
            if size.value == 0 or ctypes.get_last_error() != 122:
                raise ExecutionContainmentError("ATTRIBUTE_SIZE")
            buffer = ctypes.create_string_buffer(size.value)
            pending_attributes = ctypes.cast(buffer, ctypes.c_void_p)
            _checked(
                api.InitializeProcThreadAttributeList(
                    pending_attributes, attribute_count, 0, ctypes.byref(size),
                ),
                "ATTRIBUTE_INITIALIZE",
            )
            attributes = pending_attributes
            jobs = (w.HANDLE * 1)(instance._job)
            allowed_handles = [*inherited, *directory_handles]
            handles = (w.HANDLE * len(allowed_handles))(*allowed_handles)
            pairs = [(_PROC_THREAD_ATTRIBUTE_HANDLE_LIST, handles)]
            if not inherited_job:
                pairs.append((_PROC_THREAD_ATTRIBUTE_JOB_LIST, jobs))
            for key, values in pairs:
                _checked(
                    api.UpdateProcThreadAttribute(
                        attributes, 0, key, values, ctypes.sizeof(values), None, None
                    ),
                    "ATTRIBUTE_BIND",
                )
            startup = _StartupInfoEx()
            startup.StartupInfo.cb = ctypes.sizeof(startup)
            startup.StartupInfo.dwFlags = 0x100  # STARTF_USESTDHANDLES
            startup.StartupInfo.hStdInput = inherited[0]
            startup.StartupInfo.hStdOutput = inherited[1]
            startup.StartupInfo.hStdError = inherited[1]
            startup.lpAttributeList = attributes
            info = _ProcessInfo()
            command = ctypes.create_unicode_buffer(subprocess.list2cmdline(argv))
            env = ctypes.create_unicode_buffer(
                "\0".join(
                    f"{k}={v}"
                    for k, v in sorted(environment.items(), key=lambda pair: pair[0].upper())
                )
                + "\0\0"
            )
            flags = (
                _CREATE_SUSPENDED
                | _CREATE_NO_WINDOW
                | _EXTENDED_STARTUPINFO_PRESENT
                | _CREATE_UNICODE_ENVIRONMENT
            )
            creator = api.CreateProcessW
            prefix: tuple[int, ...] = ()
            if worker_token is not None:
                creator = worker_token._security.CreateProcessAsUserW
                prefix = (worker_token._checked_handle(),)
            _checked(
                creator(
                    *prefix,
                    argv[0],
                    command,
                    None,
                    None,
                    True,
                    flags,
                    env,
                    str(cwd),
                    ctypes.byref(startup),
                    ctypes.byref(info),
                ),
                "CREATE_CONTAINED_PROCESS",
            )
            instance._process, instance._thread = info.hProcess, info.hThread
            # Inspect the real, still-suspended child before any candidate code runs.
            if _process_primary_token(api, info.hProcess) != expected_token:
                raise ExecutionContainmentError("CANDIDATE_TOKEN_CHANGED")
            if inherited_job:
                member = w.BOOL()
                _checked(
                    api.IsProcessInJob(info.hProcess, instance._job, ctypes.byref(member)),
                    "INHERITED_CHILD_MEMBERSHIP_QUERY",
                )
                if not member.value:
                    raise ExecutionContainmentError("INHERITED_CHILD_MEMBERSHIP")
            instance._identity = {
                "pid": int(info.dwProcessId),
                "creation_time": _creation_time(api, info.hProcess),
                "job_name": name,
                "launcher_pid": os.getpid(),
                "launcher_creation_time": _creation_time(api, current),
            }
            if inherited_job:
                # Even a query-only Job handle delays KILL_ON_JOB_CLOSE. Never
                # retain it in the worker after the suspended child is verified.
                _checked(api.CloseHandle(instance._job), "CLOSE_INHERITED_JOB_QUERY")
                instance._job = None
        except BaseException as exc:
            failure = exc
        finally:
            if attributes is not None:
                try:
                    api.DeleteProcThreadAttributeList(attributes)
                except BaseException as exc:
                    cleanup_errors.append(f"attributes:{type(exc).__name__}")
            for handle in inherited:
                try:
                    _checked(api.CloseHandle(handle), "CLOSE_INHERITABLE_DUPLICATE")
                except BaseException as exc:
                    cleanup_errors.append(f"stdio:{type(exc).__name__}")
        # Never return before temporary cleanup. Otherwise a finally exception
        # can strand a successfully-created suspended process with no caller.
        if failure is not None or cleanup_errors:
            try:
                instance.close()
            except BaseException as exc:
                cleanup_errors.append(f"instance:{type(exc).__name__}")
            if failure is not None:
                if cleanup_errors:
                    failure.add_note("cleanup unconfirmed: " + ",".join(cleanup_errors))
                raise failure
            raise ExecutionContainmentError("CREATE_CLEANUP", ",".join(cleanup_errors))
        return instance

    def _require_owner(self) -> None:
        if (
            self._closed
            or os.getpid() != self._owner_pid
            or threading.get_ident() != self._owner_thread
        ):
            raise ExecutionContainmentError("HANDLE_OWNER", "not the live launcher")

    def identity(self) -> dict[str, object]:
        self._require_owner()
        return dict(self._identity)

    def launch_binding(self) -> dict[str, object]:
        self._require_owner()
        return dict(json.loads(json.dumps(self._launch_binding)))

    def resume(self) -> None:
        self._require_owner()
        if self._resumed:
            raise ExecutionContainmentError("ALREADY_RESUMED")
        # Freeze resume intent in the lease store BEFORE calling this method.
        self._resumed = True
        if self._api.ResumeThread(self._thread) == _INFINITE:
            raise ExecutionContainmentError("RESUME", f"winerror={ctypes.get_last_error()}")

    def poll(self) -> int | None:
        self._require_owner()
        observed = self._api.WaitForSingleObject(self._process, 0)
        if observed == _WAIT_TIMEOUT:
            return None
        if observed != 0:
            raise ExecutionContainmentError("WAIT_PROCESS", str(observed))
        code = w.DWORD()
        _checked(self._api.GetExitCodeProcess(self._process, ctypes.byref(code)), "EXIT_CODE")
        return int(code.value)

    def active_process_count(self) -> int:
        self._require_owner()
        return _active(self._api, self._job)

    def observed_members(self) -> list[dict[str, int]]:
        """Kernel identities proved in this Job during the completed wait."""
        self._require_owner()
        return [
            {"pid": pid, "creation_time": created}
            for pid, created in sorted(getattr(self, "_wait_members", ()))
        ]

    def wait(self, *, timeout: float) -> int:
        self._require_owner()
        timeout = _timeout(timeout)
        deadline = time.monotonic() + timeout
        if not self._owns_job:
            # Only direct-child exit. The original owner still owns/drains the whole Job.
            while True:
                code = self.poll()
                if code is not None:
                    return code
                if time.monotonic() >= deadline:
                    raise TimeoutError("inherited direct child has not exited")
                time.sleep(0.02)
        with _JobProcesses(self._api, self._job) as processes:
            while True:
                processes.collect()
                code = self.poll()
                if code is not None and self.active_process_count() == 0 and processes.exited():
                    self._wait_members = tuple(processes.handles)
                    return code
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "contained process tree is not empty: "
                        + processes.timeout_detail(code, self.active_process_count())
                    )
                time.sleep(0.02)

    def terminate(self, *, timeout: float = CONTAINED_TERMINATION_CONFIRMATION_SECONDS) -> int:
        self._require_owner()
        timeout = _timeout(timeout)
        if not self._owns_job:
            if self.poll() is None:
                _checked(self._api.TerminateProcess(self._process, _ABORTED), "TERMINATE_CHILD")
            return self.wait(timeout=timeout)
        deadline = time.monotonic() + timeout
        with _JobProcesses(self._api, self._job) as processes:
            processes.collect()
            _checked(self._api.TerminateJobObject(self._job, _ABORTED), "TERMINATE_JOB")
            while True:
                processes.collect()
                code = self.poll()
                if code is not None and self.active_process_count() == 0 and processes.exited():
                    return code
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "contained process termination unconfirmed: "
                        + processes.timeout_detail(code, self.active_process_count())
                    )
                time.sleep(0.02)

    def close(self) -> None:
        if self._closed:
            return
        self._require_owner()
        failures: list[BaseException] = []
        try:
            if self._process:
                if self._owns_job:
                    if self._job and _active(self._api, self._job):
                        self.terminate()
                elif self.poll() is None:
                    self.terminate()
        except BaseException as exc:
            failures.append(exc)
        finally:
            for attribute in ("_thread", "_process", "_job"):
                handle = getattr(self, attribute)
                if handle:
                    try:
                        _checked(self._api.CloseHandle(handle), "CLOSE_HANDLE")
                    except BaseException as exc:
                        failures.append(exc)
                    else:
                        setattr(self, attribute, None)
            for stream in self._files:
                try:
                    stream.close()
                except BaseException as exc:
                    failures.append(exc)
            # A closed buffered stream can still retain a native lock handle.
            # Drop successfully closed streams now, not when this process
            # wrapper is eventually collected; preserve failed streams to retry.
            self._files = [stream for stream in self._files if not stream.closed]
            self._closed = not any((self._thread, self._process, self._job, self._files))
        if failures:
            raise ExecutionContainmentError(
                "CLEANUP_UNCONFIRMED", ",".join(type(exc).__name__ for exc in failures)
            ) from failures[0]

    def __enter__(self) -> WindowsJobProcess:
        self._require_owner()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class InheritedJobChild:
    """An exact suspended child, not a Full executor or ownership of its parent Job.

    Direct-child exit never establishes that descendants or the original Job
    are empty. The original launcher retains that separate cleanup obligation.
    """

    def __init__(self, holder: WindowsJobProcess) -> None:
        if not isinstance(holder, WindowsJobProcess) or holder._owns_job:
            raise ExecutionContainmentError("INHERITED_CHILD_HANDLE_REQUIRED")
        holder._require_owner()
        self._holder = holder
        self._read_file_bindings: list[dict[str, Any]] = []

    @classmethod
    def create(
        cls, *, argv: Sequence[str], cwd: Path, environment: Mapping[str, str],
        stdout_path: Path, job_name: str, directory_custody: object | None = None,
        file_custodies: Sequence[object] = (),
    ) -> InheritedJobChild:
        from ai_trading_system.platform.architecture.workflow_contract import (
            _BoundDirectoryCustody,
            _BoundReadFileCustody,
        )

        current_job_member(job_name)
        files = tuple(file_custodies)
        if any(not isinstance(item, _BoundReadFileCustody) for item in files):
            raise ExecutionContainmentError("INHERITED_CHILD_FILE_CUSTODY")
        holder = None
        try:
            with ExitStack() as stack:
                directory_handles: tuple[int, ...] = ()
                read_file_bindings: list[dict[str, Any]] = []
                if directory_custody is not None:
                    if (not isinstance(directory_custody, _BoundDirectoryCustody)
                            or isinstance(directory_custody, _BoundReadFileCustody)):
                        raise ExecutionContainmentError("INHERITED_CHILD_DIRECTORY_CUSTODY")
                    startup = stack.enter_context(directory_custody.subprocess_inheritance())
                    directory_handles = tuple(startup.lpAttributeList["handle_list"])
                for item in files:
                    assert isinstance(item, _BoundReadFileCustody)
                    # Freeze the proof from the same live custody whose handles are duplicated.
                    read_file_bindings.append(item.binding())
                    startup = stack.enter_context(item.subprocess_inheritance())
                    directory_handles += tuple(startup.lpAttributeList["handle_list"])
                holder = WindowsJobProcess._create(
                    argv=argv, cwd=cwd, environment=environment, stdout_path=stdout_path,
                    job_name=job_name, inherited_job=True, directory_handles=directory_handles,
                )
            result = cls(holder)
            result._read_file_bindings = read_file_bindings
            return result
        except BaseException:
            if holder is not None:
                holder.close()
            raise

    def identity(self) -> dict[str, object]:
        return {**self._holder.identity(), "scope": "INHERITED_JOB_CHILD"}

    def pre_resume_binding(self) -> dict[str, object]:
        """Observe original live handles before this owner has resumed the child.

        This is not a lease/Full capability or a query of an external actor's
        thread suspend count. Publication must append and recheck the snapshot
        in its original lease before its separately admitted resume action.
        """
        holder = self._holder
        holder._require_owner()
        if holder._resumed:
            raise ExecutionContainmentError("PRE_RESUME_REQUIRED")
        identity = holder.identity()
        name = str(identity["job_name"])
        worker = current_job_member(name)
        if (worker["pid"] != identity["launcher_pid"]
                or worker["creation_time"] != identity["launcher_creation_time"]):
            raise ExecutionContainmentError("PRE_RESUME_OWNER_CHANGED")
        api = holder._api
        if (holder.poll() is not None
                or int(api.GetProcessId(holder._process)) != identity["pid"]
                or _creation_time(api, holder._process) != identity["creation_time"]):
            raise ExecutionContainmentError("PRE_RESUME_NOT_LIVE")
        job = api.OpenJobObjectW(0x0004, False, name)
        _checked(job, "OPEN_PRE_RESUME_JOB")
        try:
            member = w.BOOL()
            _checked(api.IsProcessInJob(holder._process, job, ctypes.byref(member)),
                     "PRE_RESUME_MEMBERSHIP_QUERY")
            if not member.value:
                raise ExecutionContainmentError("PRE_RESUME_MEMBERSHIP")
            if api.WaitForSingleObject(holder._process, 0) != _WAIT_TIMEOUT:
                raise ExecutionContainmentError("PRE_RESUME_NOT_LIVE")
            return {
                "schema_version": "workflow_inherited_child_pre_resume.v2",
                "owner_resume_state": "NOT_RESUMED",
                "process": {"pid": identity["pid"], "creation_time": identity["creation_time"]},
                "worker_process": worker, "job_name": name,
                "launch_binding": holder.launch_binding(),
                "read_file_custodies": json.loads(json.dumps(self._read_file_bindings)),
                "dispatch_allowed": False, "publication_allowed": False,
            }
        finally:
            # A leaked query handle would keep the original kill-on-close Job alive.
            _checked(api.CloseHandle(job), "CLOSE_PRE_RESUME_JOB")

    def launch_binding(self) -> dict[str, object]:
        return self._holder.launch_binding()

    def resume(self) -> None:
        self._holder.resume()

    def poll(self) -> int | None:
        return self._holder.poll()

    def wait_exit(self, *, timeout: float) -> int:
        return self._holder.wait(timeout=timeout)

    def terminate_process(
        self, *, timeout: float = CONTAINED_TERMINATION_CONFIRMATION_SECONDS
    ) -> int:
        return self._holder.terminate(timeout=timeout)

    def close(self) -> None:
        self._holder.close()

    def __enter__(self) -> InheritedJobChild:
        self._holder._require_owner()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
