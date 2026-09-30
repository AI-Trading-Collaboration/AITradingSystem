"""DEVX-015 controlled merge planning from external current canonical authority.

The historical integration_revalidation_plan.v1 remains unchanged. This plan
records Git object states and complete attributed source history, not heuristic
semantic equivalence. Only a separately frozen coordinator review can resolve
real residuals; a clean textual merge is not approval.
"""

from __future__ import annotations

import builtins
import fnmatch
import hashlib
import io
import json
import os
import re
import shlex
import stat
import struct
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

if TYPE_CHECKING:
    from ai_trading_system.platform.architecture.source_preservation import SourcePreservation

from ai_trading_system.platform.architecture.workflow_contract import (
    WorkflowContractError,
    apply_bound_file,
    bounded_regular_bytes,
    canonical_digest,
    create_bound_recoverable_directory,
    create_bound_recoverable_file,
    load_current_task_authority,
    load_current_task_structure,
    portable_path,
    read_bound_json,
    remove_bound_empty_directory,
    repository_identity,
    verify_bound_directory,
    write_bound_once,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path, safe_load_yaml_text

_ZERO = "0" * 40
_DISPOSITIONS = {
    "ALREADY_ABSORBED",
    "KEEP_CURRENT_AUTHORITY",
    "REGENERATE",
    "MERGE_REVIEWED",
    "CONTRACT_SEMANTICS_UNRESOLVED",
}


class GeneratedArtifactStage:
    """Private rendering for the known official generators, not a code sandbox.

    Input authority is supplied by the enclosing source worker, never learned
    from a first read. Temporary bindings require an isolated, single-threaded
    worker. This object neither installs artifacts nor grants execution rights.
    """

    def __init__(
        self,
        root: Path,
        expected_inputs: Mapping[str, Mapping[str, Any]],
        *,
        output_paths: set[str],
        deletion_paths: set[str] | None = None,
        prior_outputs: Mapping[str, bytes | None] | None = None,
    ) -> None:
        self.root = root.absolute()
        self.expected = {portable_path(k): dict(v) for k, v in expected_inputs.items()}
        self.output_paths = {portable_path(k) for k in output_paths}
        self.deletion_paths = {portable_path(k) for k in deletion_paths or set()}
        if self.output_paths & self.deletion_paths:
            _fail("GENERATOR_OUTPUT_OVERLAP")
        for name, value in self.expected.items():
            if value != _state(value.get("mode") or "000000", value.get("oid") or _ZERO):
                _fail("GENERATOR_INPUT_OBJECT", name)
        self.prior = {portable_path(k): v for k, v in (prior_outputs or {}).items()}
        self.outputs: dict[str, bytes | None] = {}
        self.reads: dict[str, dict[str, Any]] = {}
        self._physical_reads: dict[str, dict[str, Any]] = {}
        self.git_reads: list[dict[str, Any]] = []
        self._queries: dict[tuple[str, str], bool] = {}
        self._scans: dict[str, tuple[tuple[str, int, int], ...]] = {}
        self._scan_original = os.scandir
        self._bindings: list[tuple[Any, str, Any]] = []
        self._internal = 0
        self._active = False
        self._modes = self._index_modes()
        self._excluded: tuple[str, ...] = ()
        guard = safe_load_yaml_text(
            self._content("config/architecture/arch_005_s4d_checkout_guard.yaml").decode("utf-8")
        )
        self._excluded = tuple(
            portable_path(row["path"]) for row in guard["known_unrelated_exclusions"]
        )
        for name, content in self.prior.items():
            if name not in self.expected:
                _fail("GENERATOR_INPUT_UNDECLARED", name)
            if content is not None and not isinstance(content, bytes):
                _fail("GENERATOR_OUTPUT_BYTES", name)
            observed = _state("000000", _ZERO) if content is None else self._object(name, content)
            if observed != self.expected[name]:
                _fail("GENERATOR_INPUT_DRIFT", name)

    def _index_modes(self) -> dict[str, str]:
        result = {}
        for row in _git(self.root, "ls-files", "--stage", "-z").split(b"\0"):
            if not row:
                continue
            metadata, name = row.split(b"\t", 1)
            mode, _oid, stage = metadata.decode().split()
            if stage != "0":
                _fail("INDEX_CONFLICT")
            result[name.decode()] = mode
        return result

    def _name(self, path: Any) -> str:
        if isinstance(path, int):
            _fail("GENERATOR_DESCRIPTOR_READ")
        target = Path(path).absolute()
        if target == self.root:
            return ""
        try:
            return portable_path(target.relative_to(self.root).as_posix())
        except ValueError:
            _fail("GENERATOR_PATH_OUTSIDE", str(target))

    def _object(self, name: str, content: bytes) -> dict[str, Any]:
        oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
        return _state(self._modes.get(name, "100644"), oid)

    def _is_excluded(self, name: str) -> bool:
        return any(name == item or name.startswith(item + "/") for item in self._excluded)

    def _physical(self, name: str) -> tuple[bytes, dict[str, Any]]:
        self._internal += 1
        try:
            content = bounded_regular_bytes(self.root / name)
        finally:
            self._internal -= 1
        return content, self._object(name, content)

    def _content(self, name: str) -> bytes:
        if self._is_excluded(name):
            _fail("GENERATOR_INPUT_EXCLUDED", name)
        if name in self.outputs:
            content = self.outputs[name]
            origin = "CURRENT_GENERATOR_OUTPUT"
        else:
            if name not in self.expected:
                _fail("GENERATOR_INPUT_UNDECLARED", name)
            if name in self.prior:
                content = self.prior[name]
                origin = "PRIOR_GENERATOR_OUTPUT"
            else:
                content, observed = self._physical(name)
                self._physical_reads[name] = observed
                origin = "WORKTREE_INPUT"
            if content is not None and self._object(name, content) != self.expected[name]:
                _fail("GENERATOR_INPUT_DRIFT", name)
        if content is None:
            raise FileNotFoundError(str(self.root / name))
        if not isinstance(content, bytes):
            _fail("GENERATOR_OUTPUT_BYTES", name)
        previous = self.reads.get(name, {})
        versions = previous.get("versions", [])
        version = {
            "object": self._object(name, content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "origin": origin,
            "read_count": 1,
        }
        for known in versions:
            if known["object"] == version["object"] and known["origin"] == origin:
                known["read_count"] += 1
                break
        else:
            versions.append(version)
        self.reads[name] = {
            "object": self._object(name, content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "origin": origin,
            "read_count": previous.get("read_count", 0) + 1,
            "versions": versions,
        }
        return content

    def _open(
        self,
        original: Any,
        path: Any,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: Any = None,
    ) -> Any:
        if self._internal:
            return original(path, mode, buffering, encoding, errors, newline, closefd, opener)
        name = self._name(path)
        if mode not in {"r", "rt", "rb"} or opener is not None or not closefd:
            _fail("GENERATOR_DIRECT_IO_DENIED", name)
        stream = io.BytesIO(self._content(name))
        if "b" in mode:
            if encoding is not None or errors is not None or newline is not None:
                raise ValueError("binary mode does not take text arguments")
            return stream
        return io.TextIOWrapper(stream, encoding=encoding, errors=errors, newline=newline)

    def _write(self, path: Path, content: bytes, **kwargs: Any) -> Any:
        from ai_trading_system.platform.artifacts.writer import ArtifactWriteResult

        name = self._name(path)
        if (
            self._is_excluded(name)
            or name not in self.output_paths
            or not isinstance(content, bytes)
        ):
            _fail("GENERATOR_OUTPUT_UNDECLARED", name)
        if len(content) > 16 * 1024 * 1024:
            _fail("GENERATOR_OUTPUT_BUDGET", name)
        self.outputs[name] = content
        return ArtifactWriteResult(
            Path(path),
            kwargs.get("artifact_type", "bytes"),
            hashlib.sha256(content).hexdigest(),
            len(content),
            atomic=False,
        )

    def _unlink(self, original: Callable[..., None], path: Path, missing_ok: bool = False) -> None:
        if self._internal:
            return original(path, missing_ok=missing_ok)
        name = self._name(path)
        if self._is_excluded(name) or name not in self.deletion_paths or name not in self.expected:
            _fail("GENERATOR_DELETE_UNDECLARED", name)
        self._content(name)
        self.outputs[name] = None

    def _query(self, original: Callable[[Path], bool], kind: str, path: Path) -> bool:
        if self._internal:
            return original(path)
        name = self._name(path)
        if self._is_excluded(name):
            _fail("GENERATOR_INPUT_EXCLUDED", name)
        overlay = self.outputs if name in self.outputs else self.prior
        if name in overlay:
            return overlay[name] is not None and kind != "is_dir"
        if kind in {"exists", "is_dir"} and any(
            key.startswith(name + "/") and value is not None
            for key, value in {**self.prior, **self.outputs}.items()
        ):
            return True
        self._internal += 1
        try:
            observed = original(path)
            if observed:
                info = path.lstat()
                if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                    _fail("GENERATOR_REPARSE", name)
        finally:
            self._internal -= 1
        # The enclosing worker freezes the complete candidate file namespace.
        # Absence is the complement of that namespace, not a first-observation
        # authority. Directories are structural prefixes, never extra inputs.
        is_directory = not name or any(
            key.startswith(name + "/") and value["exists"] for key, value in self.expected.items()
        )
        is_file = bool(self.expected.get(name, {}).get("exists"))
        expected = {"exists": is_directory or is_file, "is_file": is_file, "is_dir": is_directory}[
            kind
        ]
        if observed != expected:
            if observed and not is_directory and name not in self.expected:
                _fail("GENERATOR_INPUT_UNDECLARED", name)
            _fail("GENERATOR_METADATA_DRIFT", name)
        key = kind, name
        if key in self._queries and self._queries[key] != observed:
            _fail("GENERATOR_METADATA_DRIFT", name)
        self._queries[key] = observed
        return observed

    @staticmethod
    def _matches(parts: tuple[str, ...], pattern: tuple[str, ...]) -> bool:
        if not pattern:
            return not parts
        if pattern[0] == "**":
            return GeneratedArtifactStage._matches(parts, pattern[1:]) or bool(
                parts and GeneratedArtifactStage._matches(parts[1:], pattern)
            )
        return bool(
            parts
            and fnmatch.fnmatchcase(parts[0], pattern[0])
            and GeneratedArtifactStage._matches(parts[1:], pattern[1:])
        )

    def _scan(self, path: Any) -> Any:
        if self._internal:
            return self._scan_original(path)
        name = self._name(path)
        if self._is_excluded(name):
            _fail("GENERATOR_INPUT_EXCLUDED", name)
        target = self.root / name
        virtual_directory = any(
            key.startswith(name + "/") and content is not None
            for key, content in {**self.prior, **self.outputs}.items()
        )
        directory_info = None
        for ancestor in (*reversed(target.parents), target):
            try:
                info = ancestor.lstat()
            except FileNotFoundError:
                if virtual_directory:
                    continue
                raise
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                _fail("GENERATOR_REPARSE", name)
            if ancestor == target:
                directory_info = info
        entries = []
        observed = []
        if directory_info is not None:
            observed.append(("", directory_info.st_mode, directory_info.st_ino))
            with self._scan_original(path) as scan:
                entries = [
                    entry for entry in scan if not self._is_excluded(self._name(Path(entry.path)))
                ]
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                _fail("GENERATOR_REPARSE", entry.name)
            observed.append((entry.name, info.st_mode, info.st_ino))
        frozen = tuple(sorted(observed))
        if name in self._scans and self._scans[name] != frozen:
            _fail("GENERATOR_SCAN_DRIFT", name)
        self._scans[name] = frozen

        class Entries:
            def __init__(self) -> None:
                self.iterator = iter(entries)

            def __iter__(self) -> Any:
                return self

            def __next__(self) -> Any:
                return next(self.iterator)

            def __enter__(self) -> Any:
                return self

            def __exit__(self, *args: Any) -> None:
                pass

            def close(self) -> None:
                pass

        return Entries()

    def _glob(self, original: Any, recursive: bool, path: Path, pattern: str) -> Any:
        if self._internal:
            return original(path, pattern)
        parent = self._name(path)
        portable_path(pattern)
        actual = {self._name(item): item for item in original(path, pattern)}
        overlay = {**self.prior, **self.outputs}
        for name, content in overlay.items():
            if content is None:
                actual.pop(name, None)
        prefix = parent + "/" if parent else ""
        match = tuple((["**"] if recursive else []) + pattern.split("/"))
        virtual = {name for name, content in overlay.items() if content is not None}
        virtual.update(
            parent.as_posix()
            for name in tuple(virtual)
            for parent in Path(name).parents
            if parent.as_posix() != "."
        )
        for name in virtual:
            if name.startswith(prefix) and not self._is_excluded(name):
                relative = name[len(prefix) :]
                if self._matches(tuple(relative.split("/")), match):
                    actual[name] = self.root / name
        return iter(actual[name] for name in sorted(actual))

    def _regular_path(
        self, original: Callable[[Path, str, str], Path], root: Path, portable: str, label: str
    ) -> Path:
        name = self._name(root / portable_path(portable))
        overlay = self.outputs if name in self.outputs else self.prior
        if name not in overlay:
            return original(root, portable, label)
        if overlay[name] is None:
            raise FileNotFoundError(str(self.root / name))
        # The exact memory artifact is already bound; there is deliberately no
        # global resolve(strict=True) relaxation for unrelated filesystem paths.
        target = self.root / name
        for ancestor in (*reversed(target.parents), target):
            try:
                info = ancestor.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                _fail("GENERATOR_REPARSE", name)
            if ancestor != target and not stat.S_ISDIR(info.st_mode):
                _fail("GENERATOR_PARENT_NOT_DIRECTORY", name)
        return target

    def _deny_os(self, original: Any, *args: Any, **kwargs: Any) -> Any:
        if self._internal:
            return original(*args, **kwargs)
        _fail("GENERATOR_DIRECT_IO_DENIED", "direct OS primitive")

    def _bind(self, owner: Any, name: str, replacement: Any) -> None:
        self._bindings.append((owner, name, getattr(owner, name)))
        setattr(owner, name, replacement)

    def _historical_read(self, kind: str, root: Path, *args: Any, **kwargs: Any) -> Any:
        if root.absolute() != self.root:
            _fail("GENERATOR_GIT_ROOT")
        policy = safe_load_yaml_text(
            self._content("config/architecture/arch_005_s4d_checkout_guard.yaml").decode("utf-8")
        )
        excluded = [portable_path(row["path"]) for row in policy["known_unrelated_exclusions"]]
        self._internal += 1
        try:
            if kind == "lines":
                arguments = args[0]
                if (
                    len(arguments) != 9
                    or arguments[:3] != ["grep", "-l", "-F"]
                    or arguments[5:] != ["--", "src", "scripts", "tests"]
                ):
                    _fail("GENERATOR_GIT_COMMAND")
                commit = _commit(self.root, arguments[4])
                pathspec = [*arguments[6:], *(":(exclude,literal)" + name for name in excluded)]
                environment = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
                environment.update(
                    GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0"
                )
                result = subprocess.run(
                    [
                        "git",
                        "-c",
                        "core.quotepath=false",
                        "-C",
                        str(self.root),
                        *arguments[:6],
                        *pathspec,
                    ],
                    capture_output=True,
                    env=environment,
                    timeout=60,
                )
                if result.returncode not in ({0, 1} if kwargs.get("allow_no_match") else {0}):
                    _fail("GENERATOR_GIT_COMMAND")
                if len(result.stdout) > 16 * 1024 * 1024:
                    _fail("GENERATOR_GIT_BUDGET")
                self.git_reads.append(
                    {
                        "commit": commit,
                        "tree": _git(self.root, "rev-parse", commit + "^{tree}").decode().strip(),
                        "search_text": arguments[3],
                        "pathspec": pathspec,
                        "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
                    }
                )
                return [
                    line.strip()
                    for line in result.stdout.decode("utf-8").splitlines()
                    if line.strip()
                ]
            commit, name = args
            commit = _commit(self.root, commit)
            name = portable_path(name)
            if any(name == item or name.startswith(item + "/") for item in excluded):
                _fail("GENERATOR_GIT_EXCLUDED", name)
            state = _at(self.root, commit, name)
            self.git_reads.append({"commit": commit, "path": name, "object": state})
            if not state["exists"]:
                if kind == "text":
                    return ""
                _fail("GENERATOR_GIT_OBJECT_MISSING", name)
            if state["type"] != "blob":
                _fail("GENERATOR_GIT_OBJECT_TYPE", name)
            content = _git(self.root, "cat-file", "blob", state["oid"])
            self.git_reads[-1]["sha256"] = hashlib.sha256(content).hexdigest()
            return content.decode("utf-8") if kind == "text" else content
        finally:
            self._internal -= 1

    def __enter__(self) -> GeneratedArtifactStage:
        if self._active or self._bindings:
            _fail("GENERATOR_STAGE_REENTRY")
        from ai_trading_system.platform.architecture import (
            compatibility_authority,
            report_catalog_flow_authority,
            task_registry_canonical,
        )
        from ai_trading_system.platform.artifacts import writer

        self._active = True
        for module in (
            writer,
            task_registry_canonical,
            report_catalog_flow_authority,
            compatibility_authority,
        ):
            self._bind(module, "write_bytes_atomic", self._write)
        for module in (report_catalog_flow_authority, compatibility_authority):
            original = module._regular_path
            self._bind(
                module,
                "_regular_path",
                lambda *a, _original=original, **k: self._regular_path(_original, *a, **k),
            )
        for owner in (builtins, io):
            original = owner.open
            self._bind(
                owner, "open", lambda *a, _original=original, **k: self._open(_original, *a, **k)
            )
        original_unlink = Path.unlink
        self._bind(
            Path,
            "unlink",
            lambda path, missing_ok=False: self._unlink(original_unlink, path, missing_ok),
        )
        for kind in ("exists", "is_file", "is_dir"):
            original = getattr(Path, kind)
            self._bind(
                Path,
                kind,
                lambda path, _original=original, _kind=kind: self._query(_original, _kind, path),
            )
        self._bind(os, "scandir", self._scan)
        for kind in ("glob", "rglob"):
            original = getattr(Path, kind)
            self._bind(
                Path,
                kind,
                lambda path, pattern, _original=original, _kind=kind: self._glob(
                    _original, _kind == "rglob", path, pattern
                ),
            )
        for kind in ("bytes", "lines", "text"):
            self._bind(
                compatibility_authority,
                "_git_" + kind,
                lambda *a, _kind=kind, **k: self._historical_read(_kind, *a, **k),
            )
        for kind in (
            "open",
            "rename",
            "replace",
            "remove",
            "unlink",
            "mkdir",
            "rmdir",
            "link",
            "symlink",
            "chmod",
            "utime",
            "truncate",
        ):
            original = getattr(os, kind)
            self._bind(
                os, kind, lambda *a, _original=original, **k: self._deny_os(_original, *a, **k)
            )
        return self

    def finish(self) -> dict[str, Any]:
        if not self._active:
            _fail("GENERATOR_STAGE_INACTIVE")
        if set(self.outputs) != self.output_paths | self.deletion_paths:
            _fail("GENERATOR_OUTPUT_SET")
        self.validate_observations()
        return {
            "status": "RENDERED_PRIVATE_ARTIFACTS",
            "materialization_allowed": False,
            "publication_allowed": False,
            "worktree_mutated": False,
            "inputs": json.loads(json.dumps(self.reads)),
            "git_inputs": json.loads(json.dumps(self.git_reads)),
            "metadata_queries": [
                {"kind": kind, "path": name, "result": observed}
                for (kind, name), observed in sorted(self._queries.items())
            ],
            "scanned_directories": sorted(self._scans),
            "outputs": [
                {"path": k, "sha256": hashlib.sha256(v).hexdigest(), "byte_count": len(v)}
                for k, v in sorted(self.outputs.items())
                if v is not None
            ],
            "deletions": sorted(k for k, v in self.outputs.items() if v is None),
        }

    def validate_observations(self) -> None:
        """Recheck all physical observations after later generator steps finish."""
        for name in tuple(self._scans):
            self._scan(self.root / name)
        self._internal += 1
        try:
            if self._index_modes() != self._modes:
                _fail("GENERATOR_INDEX_DRIFT")
            for name, expected in self._physical_reads.items():
                _content, observed = self._physical(name)
                if observed != expected:
                    _fail("GENERATOR_INPUT_DRIFT", name)
            for (kind, name), expected_query in self._queries.items():
                if getattr(self.root / name, kind)() != expected_query:
                    _fail("GENERATOR_METADATA_DRIFT", name)
        finally:
            self._internal -= 1

    def __exit__(self, *args: Any) -> None:
        for owner, name, original in reversed(self._bindings):
            setattr(owner, name, original)
        self._bindings.clear()
        self._active = False


def _fail(code: str, detail: str = "") -> NoReturn:
    raise WorkflowContractError("MERGE_" + code, detail)


_SOURCE_GENERATORS = (
    "canonical-task-source",
    "architecture-manifests",
    "report-flow-authority",
    "compatibility-authority",
)
# Engineering hang protection, not lease TTL or an acceptance deadline. The
# 2026-09-13 real 10,049-path source run finished generator actions near 576s
# and still owed full input revalidation (~143s). Retain every check and allow
# the complete finite chain; see DEVX-015 Workflow Contract V3 runtime evidence.
_SOURCE_WORKER_TIMEOUT_SECONDS = 1200
# Whole-chain evidence combines all four generators and exceeded the ordinary
# 16 MiB per-file reader in the real 2026-09-13 run (20,844,072 compact bytes).
# Keep a finite aggregate ceiling and leave ordinary source/authority limits alone.
_SOURCE_GENERATION_BUDGET = 64 * 1024 * 1024


def _encode_source_generation(delta: Mapping[str, Any]) -> bytes:
    from ai_trading_system.platform.architecture import source_preservation as safe

    content = safe._json_bytes(delta)
    if len(content) > _SOURCE_GENERATION_BUDGET:
        _fail("SOURCE_GENERATION_BUDGET")
    return content


def _source_generation_bytes(run: Path) -> bytes:
    return bounded_regular_bytes(run / "generation.json", budget=_SOURCE_GENERATION_BUDGET)


def _read_source_generation(run: Path, expected_sha256: str) -> dict[str, Any]:
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    content = _source_generation_bytes(run)
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise WorkflowContractError("REFERENCE_DRIFT", str(run / "generation.json"))
    payload = load_strict_json_text(content.decode("utf-8"))
    if not isinstance(payload, dict):
        raise WorkflowContractError("REFERENCE_OBJECT", str(run / "generation.json"))
    return payload


def render_source_generators(
    root: Path,
    expected_inputs: Mapping[str, Mapping[str, Any]],
    *,
    main_commit: str,
    generator_order: Any = None,
    progress: Callable[[str, str, str], None] | None = None,
) -> tuple[dict[str, Any], dict[str, bytes | None]]:
    """Execute the four fixed official generators without installing their bytes.

    This is an internal source-worker step, not a task/lease/Job admission API.
    The caller supplies its independently frozen complete candidate namespace.
    Prior bytes below are the actual renderer outputs, not a receipt's hash.
    """
    from ai_trading_system.platform.architecture import (
        build_aggregate_shadow_index,
        build_architecture_fitness,
        build_module_manifest,
        build_test_manifest,
        load_deprecation_policy,
        scan_deprecation_inventory,
        write_generated_architecture_artifact,
    )
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text
    from ai_trading_system.platform.artifacts.writer import write_yaml_atomic

    order = list(_SOURCE_GENERATORS if generator_order is None else generator_order)
    if order != list(_SOURCE_GENERATORS):
        _fail("GENERATOR_ORDER")
    root = root.absolute()
    identity = repository_identity(root)
    main = _commit(root, main_commit)
    heads = _git(root, "rev-parse", "HEAD", "refs/heads/main")
    if heads.decode().splitlines() != [main, main]:
        _fail("GENERATOR_SOURCE_BASE")
    index_path = Path(_git(root, "rev-parse", "--git-path", "index").decode().strip())
    if not index_path.is_absolute():
        index_path = root / index_path
    original_index = bounded_regular_bytes(index_path)
    initial = {portable_path(name): dict(state) for name, state in expected_inputs.items()}
    inputs = json.loads(json.dumps(initial))
    rendered: dict[str, bytes | None] = {}
    stages: list[GeneratedArtifactStage] = []
    steps: list[dict[str, Any]] = []

    def start(generator: str) -> dict[str, Any]:
        step: dict[str, Any] = {
            "generator_id": generator,
            "inputs": {},
            "outputs": [],
            "deletions": [],
            "phases": [],
        }
        steps.append(step)
        return step

    def run(
        step: dict[str, Any],
        phase: str,
        outputs: set[str],
        action: Any,
        deletions: set[str] | None = None,
    ) -> Any:
        if progress is not None:
            progress(step["generator_id"], phase, "START")
        with GeneratedArtifactStage(
            root, inputs, output_paths=outputs, deletion_paths=deletions, prior_outputs=rendered
        ) as stage:
            result = action(stage)
            evidence = stage.finish()
        stages.append(stage)
        step["phases"].append({"phase": phase, **evidence})
        for name, record in evidence["inputs"].items():
            previous = step["inputs"].get(name, {})
            versions = previous.get("versions", [])
            for version in record["versions"]:
                for prior in versions:
                    if (
                        prior["object"] == version["object"]
                        and prior["origin"] == version["origin"]
                    ):
                        prior["read_count"] += version["read_count"]
                        break
                else:
                    versions.append(dict(version))
            step["inputs"][name] = {
                **record,
                "versions": versions,
                "read_count": previous.get("read_count", 0) + record["read_count"],
            }
        by_path = {row["path"]: row for row in step["outputs"]}
        by_path.update({row["path"]: row for row in evidence["outputs"]})
        step["outputs"] = [by_path[name] for name in sorted(by_path)]
        step["deletions"] = sorted(set(step["deletions"]) | set(evidence["deletions"]))
        for name, content in stage.outputs.items():
            rendered[name] = content
            if content is None:
                inputs[name] = _state("000000", _ZERO)
            else:
                oid = hashlib.sha1(
                    b"blob " + str(len(content)).encode() + b"\0" + content
                ).hexdigest()
                inputs[name] = _state(initial.get(name, {}).get("mode") or "100644", oid)
        if progress is not None:
            progress(step["generator_id"], phase, "END")
        return result

    main_objects: dict[str, dict[str, Any]] = {}
    main_bytes: dict[str, bytes] = {}

    def main_content(path: str) -> bytes:
        # Prefetch exact immutable M objects before entering a render context;
        # subprocess pipes never become a general descriptor-read exception.
        if any(path == name or path.startswith(name + "/") for name in exclusions):
            _fail("GENERATOR_MAIN_INPUT_EXCLUDED", path)
        if path not in main_bytes:
            state = _at(root, main, path)
            if state["type"] != "blob":
                _fail("GENERATOR_MAIN_INPUT", path)
            main_objects[path] = state
            main_bytes[path] = _git(root, "cat-file", "blob", state["oid"])
        return main_bytes[path]

    def require_preserved(stage: GeneratedArtifactStage, paths: set[str]) -> None:
        for name in sorted(paths):
            if name not in main_objects or name not in main_bytes:
                _fail("GENERATOR_MAIN_INPUT_NOT_CAPTURED", name)
            if inputs.get(name) != main_objects[name] or stage._content(name) != main_bytes[name]:
                _fail("GENERATOR_MAIN_INPUT_CHANGED", name)

    step = start(order[0])
    policy = run(step, "DISCOVER", set(), lambda _: canonical.load_cutover_policy(root))
    exclusions = stages[0]._excluded
    canonical_outputs = {
        canonical.CANONICAL_INDEX_PATH,
        policy["canonical"]["consumer_inventory_path"],
        policy["generated_views"]["active_path"],
        policy["generated_views"]["completed_path"],
    }
    run(
        step,
        "REFRESH_AND_VALIDATE",
        canonical_outputs,
        lambda _: canonical.refresh_consumer_inventory(project_root=root),
    )

    step = start(order[1])
    names = {
        "module": "inputs/architecture/arch_004e_module_manifest.yaml",
        "test": "inputs/architecture/arch_004e_test_manifest.yaml",
        "aggregate": "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "fitness": "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "deprecation": "inputs/architecture/arch_004g_deprecation_inventory.yaml",
    }

    def architecture(_: GeneratedArtifactStage) -> None:
        ownership = root / "config/architecture/devex_ownership_policy.yaml"
        for name, builder in (
            ("module", build_module_manifest),
            ("test", build_test_manifest),
            ("aggregate", build_aggregate_shadow_index),
        ):
            write_generated_architecture_artifact(
                root / names[name], builder(project_root=root, policy_path=ownership)
            )
        fitness_args = dict(
            project_root=root,
            policy_path=ownership,
            module_manifest_path=root / names["module"],
            test_manifest_path=root / names["test"],
            aggregate_index_path=root / names["aggregate"],
            dependency_policy_path=root / "config/architecture/arch_004c_dependency_policy.yaml",
            direct_writer_baseline_path=root
            / "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
        )
        fitness = build_architecture_fitness(**fitness_args)
        if fitness["status"] != "PASS":
            _fail("GENERATOR_ARCHITECTURE_FITNESS")
        write_generated_architecture_artifact(root / names["fitness"], fitness)
        inventory = scan_deprecation_inventory(
            load_deprecation_policy(root / "config/architecture/arch_004g_deprecation_policy.yaml"),
            project_root=root,
            architecture_fitness_path=root / names["fitness"],
        ).to_dict()
        write_generated_architecture_artifact(root / names["deprecation"], inventory)
        if build_architecture_fitness(**fitness_args) != fitness:
            _fail("GENERATOR_ARCHITECTURE_CHANGED")

    run(step, "GENERATE_AND_VALIDATE", set(names.values()), architecture)

    step = start(order[2])
    report_policy_path = report.DEFAULT_POLICY_PATH.as_posix()
    historical_policy = safe_load_yaml_text(main_content(report_policy_path).decode("utf-8"))
    seal_fields = {"byte_count", "file_sha256", "lf_sha256", "git_blob", "entry_count"}

    def policy_contract(value: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **value,
            "targets": [
                {k: v for k, v in target.items() if k not in seal_fields}
                for target in value["targets"]
            ],
        }

    def report_discovery(stage: GeneratedArtifactStage) -> dict[str, Any]:
        current = report.load_policy(root)
        if policy_contract(current) != policy_contract(historical_policy):
            _fail("REPORT_POLICY_CONTRACT_CHANGED")
        for target in current["targets"]:
            content = stage._content(target["path"])
            entries = (
                report._split_report_registry(content)
                if target["splitter"] == "YAML_REPORT_ITEMS_WITH_PREFIX_V1"
                else report._split_markdown(target["target_id"], content)
            )
            target.update(
                byte_count=len(content),
                file_sha256=hashlib.sha256(content).hexdigest(),
                lf_sha256=hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest(),
                git_blob=report._git_blob_id(content),
                entry_count=len(entries),
            )
            report.split_target_entries(target, content)
        write_yaml_atomic(root / report_policy_path, current, sort_keys=False)
        return {"policy": current, "expected": report.build_repository_authority(root, write=False)}

    discovery = run(step, "SOURCE_SEALS_AND_DISCOVERY", {report_policy_path}, report_discovery)
    policy, expected = discovery["policy"], discovery["expected"]
    prior_index = load_strict_json_text(main_content(policy["index_path"]).decode("utf-8"))
    if not isinstance(prior_index, dict):
        _fail("REPORT_INDEX_SHAPE")
    old_report = {
        row["fragment_path"] for target in prior_index["targets"] for row in target["fragments"]
    }
    # The current index is not the complete retained history: earlier official
    # generations remain in M. Admit only exact M regular-file names, then
    # verify their immutable bytes below; never adopt arbitrary local extras.
    main_report = _main_fragment_inventory(root, main, policy["fragment_root"])
    if not old_report <= main_report:
        _fail("REPORT_INDEX_SHAPE")
    old_report = main_report
    report_fragments = set(expected["fragment_paths"])
    report_outputs = report_fragments | {policy["index_path"], policy["consumer_inventory_path"]}
    for name in old_report - report_fragments:
        main_content(name)

    def report_generate(stage: GeneratedArtifactStage) -> None:
        actual = {
            path.relative_to(root).as_posix()
            for path in (root / policy["fragment_root"]).rglob("*")
            if path.is_file()
        }
        if actual - old_report - report_fragments:
            _fail("REPORT_OUTPUT_SET_CHANGED")
        require_preserved(stage, actual - report_fragments)
        built = report.build_repository_authority(root, write=True)
        if built != expected:
            _fail("GENERATOR_REPORT_DISCOVERY_CHANGED")
        report.validate_repository_authority(root)

    run(step, "GENERATE_AND_VALIDATE", report_outputs, report_generate)

    step = start(order[3])
    compat_policy_bytes = main_content(compatibility.DEFAULT_POLICY_PATH.as_posix())
    main_content(safe_load_yaml_text(compat_policy_bytes.decode("utf-8"))["legacy_prefix"]["path"])

    def compatibility_discovery(stage: GeneratedArtifactStage) -> dict[str, Any]:
        if stage._content(compatibility.DEFAULT_POLICY_PATH.as_posix()) != compat_policy_bytes:
            _fail("COMPATIBILITY_SEALED_DEPENDENCY_CHANGED")
        current = compatibility.load_compatibility_policy(root)
        require_preserved(stage, {current["legacy_prefix"]["path"]})
        return {
            "policy": current,
            "expected": compatibility.build_repository_authority(root, write=False),
        }

    discovery = run(step, "DISCOVER", set(), compatibility_discovery)
    policy, expected = discovery["policy"], discovery["expected"]
    prior_index = load_strict_json_text(main_content(policy["index_path"]).decode("utf-8"))
    if not isinstance(prior_index, dict):
        _fail("COMPATIBILITY_INDEX_SHAPE")
    entries = expected["index"]["entries"]
    old_sections = [row["section_id"] for row in prior_index["entries"]]
    if [row["section_id"] for row in entries][: len(old_sections)] != old_sections or expected[
        "index"
    ]["legacy_prefix"] != prior_index["legacy_prefix"]:
        _fail("COMPATIBILITY_SECTION_HISTORY_CHANGED")
    old_compat = {row["fragment_path"] for row in prior_index["entries"]}
    compat_fragments = {row["fragment_path"] for row in entries}
    obsolete = old_compat - compat_fragments
    for name in obsolete:
        main_content(name)
    compat_outputs = compat_fragments | {policy["index_path"], policy["consumer_inventory_path"]}

    def compatibility_generate(stage: GeneratedArtifactStage) -> None:
        actual = {
            path.relative_to(root).as_posix()
            for path in (root / policy["fragment_root"]).rglob("*")
            if path.is_file()
        }
        if actual - old_compat - compat_fragments:
            _fail("COMPATIBILITY_OUTPUT_SET_CHANGED")
        require_preserved(stage, obsolete)
        built = compatibility.build_repository_authority(root, write=True)
        if built != expected:
            _fail("GENERATOR_COMPATIBILITY_DISCOVERY_CHANGED")
        compatibility.validate_repository_authority(root)

    run(step, "GENERATE_AND_VALIDATE", compat_outputs, compatibility_generate, obsolete)
    for stage in stages:
        stage.validate_observations()
    if (
        _git(root, "rev-parse", "HEAD", "refs/heads/main") != heads
        or bounded_regular_bytes(index_path) != original_index
    ):
        _fail("GENERATOR_SOURCE_CHANGED")
    return (
        {
            "status": "RENDERED_PRIVATE_SOURCE_GENERATION",
            "repository": identity,
            "main_commit": main,
            "generator_order": order,
            "steps": steps,
            "input_namespace_sha256": canonical_digest(initial),
            "main_inputs": [
                {
                    "path": name,
                    "object": main_objects[name],
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
                for name, content in sorted(main_bytes.items())
            ],
            "generator_functions_executed": True,
            "source_job_execution_proven": False,
            "materialization_allowed": False,
            "publication_allowed": False,
            "worktree_mutated": False,
        },
        rendered,
    )


def _git(root: Path, *args: str, content: bytes | None = None) -> bytes:
    environment = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0"
    )
    result = subprocess.run(
        ["git", "-c", "core.quotepath=false", "-C", str(root), *args],
        input=content,
        capture_output=True,
        env=environment,
        timeout=60,
    )
    if result.returncode:
        _fail("GIT", result.stderr.decode("utf-8", errors="replace")[:1000])
    if len(result.stdout) > 16 * 1024 * 1024:
        _fail("INVENTORY_BUDGET")
    return result.stdout


def _commit(root: Path, value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch("[a-f0-9]{40}", value):
        _fail("COMMIT_IDENTITY")
    if _git(root, "rev-parse", "--verify", value + "^{commit}").decode().strip() != value:
        _fail("COMMIT_IDENTITY")
    return value


def _exclusions(root: Path) -> list[str]:
    policy = safe_load_yaml_path(root / "config/architecture/arch_005_s4d_checkout_guard.yaml")
    paths = [portable_path(row["path"]) for row in policy["known_unrelated_exclusions"]]
    if len(paths) != len(set(paths)):
        _fail("EXCLUSIONS")
    return paths


def _delta(root: Path, first: str, second: str, excluded: list[str]) -> list[dict[str, Any]]:
    # No similarity scan/blob reads: rename endpoints are faithfully represented
    # as delete/add operations. Review can explicitly link the pair, never omit it.
    raw = _git(
        root,
        "diff",
        "--raw",
        "--no-abbrev",
        "--no-renames",
        "--no-ext-diff",
        "--no-textconv",
        "-z",
        first,
        second,
        "--",
        ".",
        *(":(exclude,literal)" + name for name in excluded),
    )
    tokens = raw.split(b"\0")
    result = []
    for offset in range(0, len(tokens) - 1, 2):
        fields = tokens[offset].decode("ascii").split()
        if len(fields) != 5 or not fields[0].startswith(":"):
            _fail("RAW_DELTA")
        name = portable_path(tokens[offset + 1].decode("utf-8"))
        result.append(
            {
                "path": name,
                "operation": fields[4],
                "before": _state(fields[0][1:], fields[2]),
                "after": _state(fields[1], fields[3]),
            }
        )
    return sorted(result, key=lambda row: row["path"])


def _state(mode: str, oid: str) -> dict[str, Any]:
    if mode == "000000":
        if oid != _ZERO:
            _fail("ABSENT_OBJECT")
        return {"exists": False, "mode": None, "type": None, "oid": None}
    kind = {"100644": "blob", "100755": "blob", "120000": "blob", "160000": "commit"}.get(mode)
    if kind is None or not re.fullmatch("[a-f0-9]{40}", oid):
        _fail("OBJECT_TYPE_MODE", mode)
    return {"exists": True, "mode": mode, "type": kind, "oid": oid}


def _at(root: Path, commit: str, path: str) -> dict[str, Any]:
    raw = _git(root, "ls-tree", "-z", commit, "--", ":(literal)" + path)
    if not raw:
        return _state("000000", _ZERO)
    rows = raw.rstrip(b"\0").split(b"\0")
    if len(rows) != 1:
        _fail("TREE_ENTRY", path)
    metadata, observed = rows[0].split(b"\t", 1)
    mode, kind, oid = metadata.decode("ascii").split()
    if observed.decode("utf-8") != path:
        _fail("TREE_PATH", path)
    result = _state(mode, oid)
    if result["type"] != kind:
        _fail("TREE_TYPE", path)
    return result


def _scope(authority: Mapping[str, Any]) -> dict[str, Any]:
    scope = authority["scope"]
    if scope.get("schema_version") != "workflow_merge_scope.v1":
        _fail("SCOPE_SCHEMA")
    keys = {
        "schema_version",
        "task_id",
        "decision_id",
        "repository_common",
        "frozen_base",
        "lane_head",
        "latest_main",
        "source_commits",
        "source_paths",
        "keep_current_paths",
        "generated_paths",
        "generator_order",
        "contract_claims",
    }
    if set(scope) != keys or scope["repository_common"] != authority["repository"]["common"]:
        _fail("SCOPE_IDENTITY")
    for key in ("source_paths", "keep_current_paths"):
        paths = scope[key]
        if not isinstance(paths, list) or paths != sorted(set(paths)):
            _fail("SCOPE_PATH_SET", key)
        for path in paths:
            portable_path(path)
    generated = scope["generated_paths"]
    if not isinstance(generated, dict):
        _fail("GENERATORS")
    for path, generator in generated.items():
        portable_path(path)
        if generator not in scope["generator_order"]:
            _fail("GENERATOR_UNDECLARED", path)
    if set(scope["keep_current_paths"]) & set(generated):
        _fail("CONFLICTING_DISPOSITION")
    claims = scope["contract_claims"]
    if not isinstance(claims, list) or not claims:
        _fail("CONTRACT_CLAIMS_MISSING")
    ids = []
    for claim in claims:
        if set(claim) != {"contract_id", "version", "paths", "required_acceptance"}:
            _fail("CONTRACT_CLAIM_FIELDS")
        if not claim["paths"] or not claim["required_acceptance"]:
            _fail("CONTRACT_CLAIM_EMPTY")
        for path in claim["paths"]:
            portable_path(path)
        ids.append(claim["contract_id"])
    if len(ids) != len(set(ids)):
        _fail("DUPLICATE_CONTRACT")
    return dict(scope)


def inventory_merge_sources(root: Path, authority: Mapping[str, Any]) -> dict[str, Any]:
    scope = _scope(authority)
    base, lane, main = (
        _commit(root, scope[key]) for key in ("frozen_base", "lane_head", "latest_main")
    )
    for target in (lane, main):
        _git(root, "merge-base", "--is-ancestor", base, target)
    excluded = _exclusions(root)
    if set(scope["source_paths"]) & set(excluded):
        _fail("EXCLUDED_SCOPE")
    commits = _git(root, "rev-list", "--reverse", base + ".." + lane).decode().splitlines()
    if commits != scope["source_commits"]:
        _fail("SOURCE_HISTORY_IDENTITY")
    # Metadata-only exclusion check: never open/hash/copy excluded file bytes.
    for name in excluded:
        if _git(root, "log", "--format=%H", base + ".." + lane, "--", ":(literal)" + name).strip():
            _fail("EXCLUDED_SOURCE_HISTORY")
    history = []
    for commit in commits:
        parents = _git(root, "rev-list", "--parents", "-n", "1", commit).decode().split()[1:]
        if len(parents) != 1:
            _fail("SOURCE_PARENT_REVIEW_REQUIRED", commit)
        changes = _delta(root, parents[0], commit, excluded)
        extra = sorted({row["path"] for row in changes} - set(scope["source_paths"]))
        if extra:
            _fail("UNATTRIBUTED_HISTORY", ",".join(extra))
        history.append({"commit": commit, "parents": parents, "operations": changes})
    rows = []
    for change in _delta(root, base, lane, excluded):
        path = change["path"]
        if path not in scope["source_paths"]:
            _fail("UNATTRIBUTED_SOURCE", path)
        rows.append(
            {
                "path": path,
                "operation": change["operation"],
                "base": change["before"],
                "lane": change["after"],
                "main": _at(root, main, path),
            }
        )
    return {
        "frozen_base": base,
        "lane_head": lane,
        "latest_main": main,
        "source_history": history,
        "source_entries": rows,
        "main_delta": _delta(root, base, main, excluded),
        "known_unrelated_exclusions": excluded,
    }


def build_controlled_merge_plan(root: Path, task_id: str) -> dict[str, Any]:
    authority = load_current_task_authority(root, task_id)
    return _plan_from_authority(root, task_id, authority)


def _plan_from_authority(root: Path, task_id: str, authority: Mapping[str, Any]) -> dict[str, Any]:
    scope = _scope(authority)
    inventory = inventory_merge_sources(root, authority)
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != inventory["latest_main"]:
        _fail("MAIN_CHANGED")
    rows = []
    for entry in inventory["source_entries"]:
        path = entry["path"]
        if entry["lane"] == entry["main"]:
            disposition = "ALREADY_ABSORBED"
        elif path in scope["keep_current_paths"]:
            disposition = "KEEP_CURRENT_AUTHORITY"
        elif path in scope["generated_paths"]:
            disposition = "REGENERATE"
        else:
            disposition = "CONTRACT_SEMANTICS_UNRESOLVED"
        rows.append(
            {
                **entry,
                "disposition": disposition,
                "contract_ids": sorted(
                    claim["contract_id"]
                    for claim in scope["contract_claims"]
                    if path in claim["paths"]
                ),
                "generator_id": scope["generated_paths"].get(path),
            }
        )
    plan = {
        "schema_version": "controlled_merge_plan.v1",
        "task_id": task_id,
        "decision_id": scope["decision_id"],
        "scope_ref": authority["authority"]["scope_ref"],
        "repository": authority["repository"],
        "inventory": inventory,
        "dispositions": rows,
        "contract_claims": scope["contract_claims"],
        "generator_order": scope["generator_order"],
        "status": "NO_RESIDUAL_SOURCE"
        if all(row["disposition"] == "ALREADY_ABSORBED" for row in rows)
        else "COORDINATOR_REVIEW_REQUIRED",
    }
    plan["plan_sha256"] = canonical_digest(plan)
    return plan


def validate_controlled_merge_plan(
    root: Path, task_id: str, plan: Mapping[str, Any], *, require_review: bool = True
) -> dict[str, Any]:
    expected = build_controlled_merge_plan(root, task_id)
    if dict(plan) != expected:
        _fail("PLAN_IDENTITY_OR_CLAIMS")
    if expected["status"] == "NO_RESIDUAL_SOURCE":
        return {"status": "NO_RESIDUAL_SOURCE", "plan": expected}
    if not require_review:
        return {"status": "COORDINATOR_REVIEW_REQUIRED", "plan": expected}
    authority = load_current_task_authority(root, task_id)
    reference = authority["authority"]["review_ref"]
    if reference is None:
        _fail("REVIEW_NOT_FROZEN")
    review = read_bound_json(root, reference)
    _validate_review(expected, review)
    # A frozen review proves the prior decision, not that its raw source still
    # exists unchanged. Recheck both lane resolutions and additional source
    # captured by the coordinator before advertising current merge readiness.
    # This is not generated-output closure or an atomic materialization grant.
    for row in review["resolutions"]:
        if row["result"] is not None and working_object(root, row["path"]) != row["result"]:
            _fail("REVIEW_WORKING_RESULT_CHANGED", row["path"])
    for row in review["candidate_sources"]:
        if working_object(root, row["path"]) != row["object"]:
            _fail("REVIEW_WORKING_RESULT_CHANGED", row["path"])
    from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard

    observed_paths = _candidate_source_paths(
        CheckoutLeaseGuard(project_root=root).audit_worktree().dirty_paths, authority["scope"]
    )
    if observed_paths != [row["path"] for row in review["candidate_sources"]]:
        _fail("REVIEW_SOURCE_SET_CHANGED")
    return {
        "status": "READY_FOR_CONTROLLED_MERGE",
        "plan": expected,
        "review": review,
        "review_ref": reference,
    }


def _validate_review(plan: Mapping[str, Any], review: Mapping[str, Any]) -> None:
    if set(review) != {
        "schema_version",
        "task_id",
        "plan_sha256",
        "scope_ref",
        "actor",
        "resolutions",
        "contract_resolutions",
        "rename_pairs",
        "candidate_sources",
        "source_transaction_sha256",
    }:
        _fail("REVIEW_FIELDS")
    if (
        review["schema_version"] != "controlled_merge_review.v1"
        or review["task_id"] != plan["task_id"]
        or review["plan_sha256"] != plan["plan_sha256"]
        or review["scope_ref"] != plan["scope_ref"]
        or not review["actor"]
    ):
        _fail("REVIEW_IDENTITY")
    resolutions = review["resolutions"]
    if not isinstance(resolutions, list) or len(resolutions) != len(plan["dispositions"]):
        _fail("RESOLUTION_COVERAGE")
    by_path = {row["path"]: row for row in resolutions}
    if len(by_path) != len(resolutions) or set(by_path) != {
        row["path"] for row in plan["dispositions"]
    }:
        _fail("RESOLUTION_COVERAGE")
    for row in plan["dispositions"]:
        resolution = by_path[row["path"]]
        if set(resolution) != {"path", "disposition", "result", "rationale"}:
            _fail("RESOLUTION_FIELDS")
        disposition = resolution["disposition"]
        if disposition not in _DISPOSITIONS or disposition == "CONTRACT_SEMANTICS_UNRESOLVED":
            _fail("CONTRACT_SEMANTICS_UNRESOLVED", row["path"])
        if row["disposition"] == "CONTRACT_SEMANTICS_UNRESOLVED":
            if disposition != "MERGE_REVIEWED":
                _fail("RESIDUAL_DISCARDED", row["path"])
        elif disposition != row["disposition"]:
            _fail("DISPOSITION_CHANGED", row["path"])
        if not isinstance(resolution["rationale"], str) or not resolution["rationale"]:
            _fail("REVIEW_RATIONALE")
        result = resolution["result"]
        if row["generator_id"] is not None and disposition in {"REGENERATE", "ALREADY_ABSORBED"}:
            # Absorption is a B/L/M source-history fact, not an instruction to
            # freeze an old generated blob into the new candidate. Exact fresh
            # output closure is independently required before materialization.
            if result is not None:
                _fail("OLD_GENERATED_BYTES", row["path"])
        elif disposition in {"ALREADY_ABSORBED", "KEEP_CURRENT_AUTHORITY"}:
            if result != row["main"]:
                _fail("CURRENT_AUTHORITY_REPLACED", row["path"])
        else:
            if not isinstance(result, dict) or set(result) != {"exists", "type", "mode", "oid"}:
                _fail("RESULT_OBJECT")
            checked = _state(result["mode"] or "000000", result["oid"] or _ZERO)
            if result != checked:
                _fail("RESULT_OBJECT")
    contracts = review["contract_resolutions"]
    if not isinstance(contracts, list) or len(contracts) != len(plan["contract_claims"]):
        _fail("CONTRACT_REVIEW_COVERAGE")
    for claim, resolution in zip(plan["contract_claims"], contracts, strict=True):
        if (
            set(resolution) != {"claim", "semantic_decision", "rationale"}
            or resolution["claim"] != claim
            or resolution["semantic_decision"] != "PRESERVE_VALID_RULES"
            or not isinstance(resolution["rationale"], str)
            or not resolution["rationale"]
        ):
            _fail("CONTRACT_SEMANTICS_UNRESOLVED")
    endpoints = set()
    for pair in review["rename_pairs"]:
        if not isinstance(pair, list) or len(pair) != 2 or pair[0] == pair[1]:
            _fail("RENAME_PAIR")
        if any(path not in by_path or path in endpoints for path in pair):
            _fail("RENAME_COVERAGE")
        endpoints.update(pair)
    candidates = review["candidate_sources"]
    if not isinstance(candidates, list):
        _fail("CANDIDATE_SOURCES")
    if [row["path"] for row in candidates] != sorted({row["path"] for row in candidates}):
        _fail("CANDIDATE_SOURCE_COVERAGE")
    for row in candidates:
        if set(row) != {"path", "object"}:
            _fail("CANDIDATE_SOURCE_FIELDS")
        portable_path(row["path"])
        value = row["object"]
        if value != _state(value["mode"] or "000000", value["oid"] or _ZERO):
            _fail("CANDIDATE_SOURCE_OBJECT")
    if not re.fullmatch("[a-f0-9]{64}", str(review["source_transaction_sha256"])):
        _fail("SOURCE_TRANSACTION_IDENTITY")


def working_object(root: Path, path: str) -> dict[str, Any]:
    """Freeze raw bytes without writing Git objects or invoking a clean filter."""
    path = portable_path(path)
    target = root / path
    if not target.exists():
        if target.is_symlink():
            _fail("RESULT_REPARSE", path)
        return _state("000000", _ZERO)
    content = bounded_regular_bytes(target)
    oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
    # Windows worktree bytes alone cannot prove the executable bit. The staged
    # index provides it; callers review exact mode differences separately.
    rows = _git(root, "ls-files", "--stage", "-z", "--", ":(literal)" + path).split(b"\0")
    mode = "100644"
    if rows[0]:
        metadata, observed = rows[0].split(b"\t", 1)
        fields = metadata.decode().split()
        if len(fields) != 3 or fields[2] != "0" or len([row for row in rows if row]) != 1:
            _fail("INDEX_CONFLICT", path)
        if observed.decode() != path:
            _fail("INDEX_PATH", path)
        mode = fields[0]
    return _state(mode, oid)


def inspect_canonical_merge_outputs(root: Path, task_id: str) -> dict[str, Any]:
    """Validate exact current canonical outputs against external main history.

    This read-only inventory is one generator's input to candidate closure, not
    proof of generator execution, a frozen closure, or materialization authority.
    """
    from ai_trading_system.platform.architecture import task_registry_canonical as canonical

    authority = load_current_task_authority(root, task_id)
    main = _scope(authority)["latest_main"]
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    policy = canonical.load_cutover_policy(root)
    registry = canonical.validate_canonical_registry(project_root=root)
    index_path = policy["canonical"]["index_path"]
    prior = safe_load_yaml_text(_git(root, "show", main + ":" + index_path).decode("utf-8"))
    previous = {row["task_id"]: row for row in prior["fragments"]}
    current = {row["task_id"]: row for row in registry.index["fragments"]}
    if not set(previous) <= set(current) or set(current) - set(previous) - {task_id}:
        _fail("CANONICAL_TASK_SET_CHANGED")
    for identity, old in previous.items():
        now = current[identity]
        if old["path"] != now["path"]:
            _fail("CANONICAL_TASK_PATH_CHANGED", identity)
        if identity != task_id and old["file_sha256"] != now["file_sha256"]:
            _fail("CANONICAL_UNRELATED_TASK_CHANGED", identity)
    if task_id in previous:
        old_task = safe_load_yaml_text(
            _git(root, "show", main + ":" + previous[task_id]["path"]).decode("utf-8")
        )
        current_task = safe_load_yaml_path(root / current[task_id]["path"])
        events = old_task["events"]
        if current_task["events"][: len(events)] != events:
            _fail("CANONICAL_EVENT_HISTORY_CHANGED", task_id)
    cycles = prior["governance_cycles"]
    if registry.index["governance_cycles"][: len(cycles)] != cycles:
        _fail("CANONICAL_GOVERNANCE_HISTORY_CHANGED")
    for key in ("templates", "cutover_manifest_sha256", "policy_sha256"):
        if registry.index[key] != prior[key]:
            _fail("CANONICAL_SEALED_DEPENDENCY_CHANGED", key)
    fragment_paths = {row["path"] for row in current.values()}
    fragment_root = root / policy["canonical"]["fragment_root"]
    pending = [fragment_root]
    observed = set()
    while pending:
        directory = pending.pop()
        if getattr(directory.lstat(), "st_file_attributes", 0) & 0x400 or directory.is_symlink():
            _fail("CANONICAL_OUTPUT_REPARSE")
        with os.scandir(directory) as entries:
            for entry in entries:
                metadata = entry.stat(follow_symlinks=False)
                if getattr(metadata, "st_file_attributes", 0) & 0x400 or entry.is_symlink():
                    _fail("CANONICAL_OUTPUT_REPARSE")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                else:
                    observed.add(Path(entry.path).relative_to(root).as_posix())
    if observed != fragment_paths:
        # Unknown files are identified by metadata only; never read or hash them.
        _fail("CANONICAL_OUTPUT_SET_CHANGED", ",".join(sorted(observed ^ fragment_paths)))
    outputs = sorted(
        fragment_paths
        | {
            index_path,
            policy["canonical"]["consumer_inventory_path"],
            policy["generated_views"]["active_path"],
            policy["generated_views"]["completed_path"],
        }
    )
    return {
        "schema_version": "canonical_merge_output_inventory.v1",
        "status": "VALIDATED_CANONICAL_INPUTS",
        "task_id": task_id,
        "main": main,
        "output_paths": outputs,
        "dependency_paths": sorted(
            {policy["canonical"]["manifest_path"], canonical.POLICY_PATH}
            | {row["path"] for row in registry.index["templates"]}
        ),
        "materialization_allowed": False,
    }


def inspect_architecture_merge_outputs(root: Path, task_id: str) -> dict[str, Any]:
    """Recompute all five official architecture outputs without writing them."""
    from ai_trading_system.platform.architecture import (
        build_architecture_fitness,
        load_deprecation_policy,
        scan_deprecation_inventory,
    )

    authority = load_current_task_authority(root, task_id)
    main = _scope(authority)["latest_main"]
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    names = {
        "module": "inputs/architecture/arch_004e_module_manifest.yaml",
        "test": "inputs/architecture/arch_004e_test_manifest.yaml",
        "aggregate": "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "fitness": "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "deprecation": "inputs/architecture/arch_004g_deprecation_inventory.yaml",
    }
    dependencies = [
        "config/architecture/devex_ownership_policy.yaml",
        "config/architecture/arch_004c_dependency_policy.yaml",
        "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
        "config/architecture/arch_004g_deprecation_policy.yaml",
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
    ]
    observed = {
        path: bounded_regular_bytes(root / path) for path in [*names.values(), *dependencies]
    }
    fitness = build_architecture_fitness(
        project_root=root,
        policy_path=root / dependencies[0],
        module_manifest_path=root / names["module"],
        test_manifest_path=root / names["test"],
        aggregate_index_path=root / names["aggregate"],
        dependency_policy_path=root / dependencies[1],
        direct_writer_baseline_path=root / dependencies[2],
    )
    if fitness["status"] != "PASS":
        _fail("ARCHITECTURE_INPUTS_STALE_OR_INVALID")
    if safe_load_yaml_text(observed[names["fitness"]].decode("utf-8")) != fitness:
        _fail("ARCHITECTURE_FITNESS_OUTPUT_STALE")
    inventory = scan_deprecation_inventory(
        load_deprecation_policy(root / dependencies[3]),
        project_root=root,
        architecture_fitness_path=root / names["fitness"],
    ).to_dict()
    if safe_load_yaml_text(observed[names["deprecation"]].decode("utf-8")) != inventory:
        _fail("ARCHITECTURE_DEPRECATION_OUTPUT_STALE")
    if any(bounded_regular_bytes(root / path) != value for path, value in observed.items()):
        _fail("ARCHITECTURE_INPUTS_CHANGED_DURING_INSPECTION")
    return {
        "schema_version": "architecture_merge_output_inventory.v1",
        "status": "VALIDATED_ARCHITECTURE_INPUTS",
        "task_id": task_id,
        "main": main,
        "output_paths": sorted(names.values()),
        "dependency_paths": sorted(dependencies),
        "materialization_allowed": False,
    }


def _fragment_inventory(root: Path, portable: str, *, excluded: tuple[str, ...] = ()) -> set[str]:
    """Inventory names without opening unknown files or following reparse points."""
    excluded_keys = {portable_path(name).casefold() for name in excluded}

    def is_excluded(name: str) -> bool:
        key = name.casefold()
        return any(key == item or key.startswith(item + "/") for item in excluded_keys)

    if is_excluded(portable):
        return set()
    directory = root
    for part in portable_path(portable).split("/"):
        directory /= part
        if not directory.exists():
            return set()
        if getattr(directory.lstat(), "st_file_attributes", 0) & 0x400 or directory.is_symlink():
            _fail("GENERATED_OUTPUT_REPARSE")
    pending = [directory]
    observed: set[str] = set()
    while pending:
        with os.scandir(pending.pop()) as entries:
            for entry in entries:
                relative = Path(entry.path).relative_to(root).as_posix()
                # Policy exclusions are a no-read/no-traversal boundary, not
                # unreviewed candidate inputs. Apply before even inspecting a
                # leaf or descending into an excluded directory/reparse point.
                if is_excluded(relative):
                    continue
                metadata = entry.stat(follow_symlinks=False)
                if getattr(metadata, "st_file_attributes", 0) & 0x400 or entry.is_symlink():
                    _fail("GENERATED_OUTPUT_REPARSE")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                else:
                    observed.add(relative)
    return observed


def _main_fragment_inventory(root: Path, main: str, portable: str) -> set[str]:
    """Complete immutable historical names, not only the latest index references."""
    prefix = portable_path(portable) + "/"
    names: set[str] = set()
    for row in _git(root, "ls-tree", "-r", "-z", main, "--", portable).split(b"\0"):
        if not row:
            continue
        metadata, raw_name = row.split(b"\t", 1)
        mode, kind, _oid = metadata.decode("ascii").split()
        name = portable_path(raw_name.decode("utf-8"))
        if not name.startswith(prefix) or mode not in {"100644", "100755"} or kind != "blob":
            _fail("REPORT_HISTORICAL_OUTPUT_TYPE", name)
        names.add(name)
    return names


def inspect_report_merge_outputs(root: Path, task_id: str) -> dict[str, Any]:
    """Validate official report-flow outputs and preserve only main-known leftovers."""
    from ai_trading_system.platform.architecture import report_catalog_flow_authority as report
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    authority = load_current_task_authority(root, task_id)
    main = _scope(authority)["latest_main"]
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    policy = report.load_policy(root)
    policy_path = report.DEFAULT_POLICY_PATH.as_posix()
    policy_state = _at(root, main, policy_path)
    if policy_state["exists"]:
        prior_policy = safe_load_yaml_text(
            _git(root, "cat-file", "blob", policy_state["oid"]).decode("utf-8")
        )
        # Only lossless source seals/counts are regenerated. Changing target
        # paths, splitters, ownership, status or the contract needs source review.
        seal_fields = {"byte_count", "file_sha256", "lf_sha256", "git_blob", "entry_count"}

        def policy_contract(value: Mapping[str, Any]) -> dict[str, Any]:
            return {
                **value,
                "targets": [
                    {key: item for key, item in row.items() if key not in seal_fields}
                    for row in value["targets"]
                ],
            }

        if policy_contract(policy) != policy_contract(prior_policy):
            _fail("REPORT_POLICY_CONTRACT_CHANGED")
    dependencies = sorted({policy_path} | {row["path"] for row in policy["targets"]})
    inputs = {path: bounded_regular_bytes(root / path) for path in dependencies}
    expected = report.build_repository_authority(root, write=False)
    fragments = set(expected["fragment_paths"])
    prior_paths: set[str] = set()
    index_path = policy["index_path"]
    if _at(root, main, index_path)["exists"]:
        prior = load_strict_json_text(_git(root, "show", main + ":" + index_path).decode("utf-8"))
        if not isinstance(prior, dict):
            _fail("REPORT_INDEX_SHAPE")
        prior_paths = {
            row["fragment_path"] for target in prior["targets"] for row in target["fragments"]
        }
    main_paths = _main_fragment_inventory(root, main, policy["fragment_root"])
    if not prior_paths <= main_paths:
        _fail("REPORT_INDEX_SHAPE")
    prior_paths = main_paths
    observed = _fragment_inventory(root, policy["fragment_root"])
    if observed - fragments - prior_paths or fragments - observed:
        _fail("REPORT_OUTPUT_SET_CHANGED")
    retained = observed - fragments
    retained_bytes = {}
    for path in retained:
        state = _at(root, main, path)
        if state["mode"] not in {"100644", "100755"}:
            _fail("REPORT_HISTORICAL_OUTPUT_TYPE", path)
        retained_bytes[path] = bounded_regular_bytes(root / path)
        if retained_bytes[path] != _git(root, "cat-file", "blob", state["oid"]):
            _fail("REPORT_HISTORICAL_OUTPUT_CHANGED", path)
    outputs = sorted(fragments | {index_path, policy["consumer_inventory_path"], policy_path})
    captured = {path: bounded_regular_bytes(root / path) for path in outputs}
    report.validate_repository_authority(root)
    for path, content in (inputs | captured | retained_bytes).items():
        if bounded_regular_bytes(root / path) != content:
            _fail("REPORT_INPUTS_CHANGED_DURING_INSPECTION", path)
    if _fragment_inventory(root, policy["fragment_root"]) != observed:
        _fail("REPORT_OUTPUT_SET_CHANGED")
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    return {
        "schema_version": "report_merge_output_inventory.v1",
        "status": "VALIDATED_REPORT_INPUTS",
        "task_id": task_id,
        "main": main,
        "output_paths": outputs,
        "retained_main_paths": sorted(retained),
        "dependency_paths": dependencies,
        "materialization_allowed": False,
    }


def inspect_compatibility_merge_outputs(root: Path, task_id: str) -> dict[str, Any]:
    """Check every official compatibility fragment, never just the latest one.

    Section order and immutable prefix are historical constraints, not a claim
    of automatic semantic equivalence. Frozen source review remains mandatory.
    """
    from ai_trading_system.platform.architecture import compatibility_authority as compatibility
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    authority = load_current_task_authority(root, task_id)
    main = _scope(authority)["latest_main"]
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    policy = compatibility.load_compatibility_policy(root)
    sealed_paths = [compatibility.DEFAULT_POLICY_PATH.as_posix(), policy["legacy_prefix"]["path"]]
    sealed = {path: bounded_regular_bytes(root / path) for path in sealed_paths}
    for path, content in sealed.items():
        state = _at(root, main, path)
        if state["exists"] and (
            state["type"] != "blob" or _git(root, "cat-file", "blob", state["oid"]) != content
        ):
            _fail("COMPATIBILITY_SEALED_DEPENDENCY_CHANGED", path)
    expected = compatibility.build_repository_authority(root, write=False)
    entries = expected["index"]["entries"]
    expected_hashes = {row["fragment_path"]: row["fragment_sha256"] for row in entries}
    prior_paths: set[str] = set()
    index_state = _at(root, main, policy["index_path"])
    if index_state["exists"]:
        prior = load_strict_json_text(_git(root, "cat-file", "blob", index_state["oid"]).decode())
        if not isinstance(prior, dict):
            _fail("COMPATIBILITY_INDEX_SHAPE")
        old_sections = [row["section_id"] for row in prior["entries"]]
        if [row["section_id"] for row in entries][: len(old_sections)] != old_sections:
            _fail("COMPATIBILITY_SECTION_HISTORY_CHANGED")
        if expected["index"]["legacy_prefix"] != prior["legacy_prefix"]:
            _fail("COMPATIBILITY_PREFIX_CHANGED")
        prior_paths = {row["fragment_path"] for row in prior["entries"]}
    fragments = set(expected_hashes)
    observed = _fragment_inventory(root, policy["fragment_root"])
    if observed - fragments - prior_paths or fragments - observed:
        _fail("COMPATIBILITY_OUTPUT_SET_CHANGED")
    obsolete = observed - fragments
    obsolete_bytes = {}
    for path in obsolete:
        state = _at(root, main, path)
        if state["mode"] not in {"100644", "100755"}:
            _fail("COMPATIBILITY_HISTORICAL_OUTPUT_TYPE", path)
        obsolete_bytes[path] = bounded_regular_bytes(root / path)
        if obsolete_bytes[path] != _git(root, "cat-file", "blob", state["oid"]):
            _fail("COMPATIBILITY_HISTORICAL_OUTPUT_CHANGED", path)
    outputs = sorted(fragments | {policy["index_path"], policy["consumer_inventory_path"]})
    captured = {path: bounded_regular_bytes(root / path) for path in outputs}
    for path, digest in expected_hashes.items():
        if hashlib.sha256(captured[path]).hexdigest() != digest:
            _fail("COMPATIBILITY_FRAGMENT_STALE", path)
    compatibility.validate_repository_authority(root)
    for path, content in (sealed | captured | obsolete_bytes).items():
        if bounded_regular_bytes(root / path) != content:
            _fail("COMPATIBILITY_INPUTS_CHANGED_DURING_INSPECTION", path)
    if _fragment_inventory(root, policy["fragment_root"]) != observed:
        _fail("COMPATIBILITY_OUTPUT_SET_CHANGED")
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != main:
        _fail("MAIN_CHANGED")
    return {
        "schema_version": "compatibility_merge_output_inventory.v1",
        "status": "VALIDATED_COMPATIBILITY_INPUTS",
        "task_id": task_id,
        "main": main,
        "output_paths": outputs,
        "obsolete_main_paths": sorted(prior_paths - fragments),
        "sealed_dependency_paths": sorted(sealed_paths),
        "source_review_required": True,
        "materialization_allowed": False,
        "deletion_allowed": False,
    }


def _candidate_source_paths(dirty_paths: Any, scope: Mapping[str, Any]) -> list[str]:
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        ACTIVE_VIEW_PATH,
        COMPLETED_VIEW_PATH,
    )

    # These are official regenerated/canonical outputs, not source exclusions.
    # Their final closure is independently required before candidate materialization.
    generated_roots = (
        "inputs/architecture/",
        "registry/development_tasks/",
        "registry/architecture_compatibility_authority/",
        "registry/report_catalog_flow_authority/",
        "outputs/",
    )
    ignored_names = {ACTIVE_VIEW_PATH, COMPLETED_VIEW_PATH, *scope["generated_paths"]}
    return [
        path
        for path in sorted(dirty_paths)
        if path not in ignored_names and not path.startswith(generated_roots)
    ]


def _candidate_scan_admission(root: Path, main: str, review: Mapping[str, Any]) -> set[str]:
    """Metadata-only first gate for the official recursive source scans.

    Existing M names plus frozen reviewed source names are a read ceiling, not
    proof of unchanged bytes. Dynamic policy references and atomic execution
    still need the source worker's separate exact input binding.
    """
    excluded = _exclusions(root)
    known = {
        portable_path(row.decode("utf-8"))
        for row in _git(
            root,
            "ls-tree",
            "-r",
            "--name-only",
            "-z",
            main,
            "--",
            "src",
            "scripts",
            "tests",
            "docs",
            "config",
        ).split(b"\0")
        if row
    }
    # ls-tree does not support exclusion magic. It reads only tree metadata;
    # remove excluded names here before any file-content admission.
    known = {
        path
        for path in known
        if not any(path == name or path.startswith(name + "/") for name in excluded)
    }
    known.update(row["path"] for row in review["candidate_sources"])
    known.update(row["path"] for row in review["resolutions"] if row["result"] is not None)
    names: set[str] = set()
    for top, suffix in (
        ("src", ".py"),
        ("scripts", ".py"),
        ("tests", ".py"),
        ("docs", ".md"),
        ("config", ".yaml"),
    ):
        start = root / top
        if not start.exists():
            continue
        for path in _fragment_inventory(root, top, excluded=tuple(excluded)):
            if path.endswith(suffix):
                if path not in known:
                    _fail("CANDIDATE_DELTA_UNCOVERED", path)
                names.add(path)
    return names


def prepare_source_generation(
    root: Path,
    task_id: str,
    *,
    transaction_path: Path,
    actor: str,
    phase: str = "TASK_SOURCE_PRE_WRITE",
) -> dict[str, Any]:
    """Freeze the external raw namespace before invoking any source generator.

    M supplies names and provenance, not a claim that Windows worktree bytes
    equal Git's normalized blob bytes. Dirty source bytes require the existing
    immutable review; only current, independently validated canonical outputs
    may differ without that source review. Other generators have not run yet.
    The returned observation is not a worker or Git-write authorization.
    """
    from ai_trading_system.platform.architecture.checkout_guard import (
        collect_checkout_dirty_paths,
    )
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root = root.absolute()
    if phase not in {
        "TASK_SOURCE_PRE_WRITE",
        "GENERATED_REBUILD_PRE",
        "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE",
    }:
        _fail("CANDIDATE_SOURCE_PHASE")
    fence = IntegrationPublicationFence(project_root=root)
    fence.validate(transaction_path, exact_phase=phase, task_id=task_id)
    transaction = fence.replay(transaction_path)
    if transaction.transaction["actor"] != actor:
        _fail("CANDIDATE_ACTOR")
    structural = load_current_task_structure(root, task_id)
    scope = _scope(structural)
    reference = structural["authority"]["review_ref"]
    if reference is None:
        _fail("REVIEW_NOT_FROZEN")
    review = read_bound_json(root, reference)
    plan = _plan_from_authority(root, task_id, structural)
    _validate_review(plan, review)
    if review["source_transaction_sha256"] != transaction.transaction["transaction_sha256"]:
        _fail("CANDIDATE_TRANSACTION_CHANGED")
    main = scope["latest_main"]
    heads = _git(root, "rev-parse", "HEAD", "refs/heads/main")
    if (
        heads.decode().splitlines() != [main, main]
        or transaction.transaction["expected_main_sha"] != main
        or transaction.transaction["lane_head_sha"] != main
    ):
        _fail("CANDIDATE_BASE")
    excluded = _exclusions(root)

    def is_excluded(name: str) -> bool:
        return any(name == item or name.startswith(item + "/") for item in excluded)

    dirty = collect_checkout_dirty_paths(root, exclusions=excluded)
    if _candidate_source_paths(dirty, scope) != [
        row["path"] for row in review["candidate_sources"]
    ]:
        _fail("CANDIDATE_DELTA_UNCOVERED", "source set differs from frozen review")
    scan_names = _candidate_scan_admission(root, main, review)
    authority = load_current_task_authority(root, task_id)
    if authority != {
        key: value for key, value in structural.items() if key != "consumer_inventory_checked"
    }:
        _fail("CANDIDATE_AUTHORITY_CHANGED")
    validate_controlled_merge_plan(root, task_id, plan)
    canonical = inspect_canonical_merge_outputs(root, task_id)
    sources = {row["path"]: row["object"] for row in review["candidate_sources"]}
    for row in review["resolutions"]:
        if row["result"] is not None:
            if row["path"] in sources and sources[row["path"]] != row["result"]:
                _fail("CANDIDATE_SOURCE_CONFLICT", row["path"])
            sources[row["path"]] = row["result"]
    allowed_dirty = set(sources) | set(canonical["output_paths"])
    unknown = set(dirty) - allowed_dirty
    if unknown:
        # Do not learn an unknown generated-root file's identity by opening it.
        _fail("CANDIDATE_DELTA_UNCOVERED", ",".join(sorted(unknown)))
    declared = (*transaction.transaction["owned_paths"], *transaction.transaction["shared_paths"])
    for path in dirty:
        if not any(path == item or path.startswith(item + "/") for item in declared):
            _fail("CANDIDATE_DELTA_UNDECLARED", path)
    if any(is_excluded(path) for path in allowed_dirty):
        _fail("GENERATOR_INPUT_EXCLUDED")

    main_inputs: dict[str, dict[str, Any]] = {}
    for row in _git(root, "ls-tree", "-r", "-z", main).split(b"\0"):
        if not row:
            continue
        metadata, raw_path = row.split(b"\t", 1)
        path = portable_path(raw_path.decode("utf-8"))
        if is_excluded(path):
            continue
        mode, kind, oid = metadata.decode("ascii").split()
        state = _state(mode, oid)
        if state["type"] != kind or mode not in {"100644", "100755"}:
            _fail("GENERATOR_INPUT_OBJECT", path)
        main_inputs[path] = state
    names = set(main_inputs) | allowed_dirty
    for path in names - set(main_inputs):
        main_inputs[path] = _state("000000", _ZERO)

    index_path = Path(_git(root, "rev-parse", "--git-path", "index").decode().strip())
    if not index_path.is_absolute():
        index_path = root / index_path
    index_bytes = bounded_regular_bytes(index_path)
    for row in _git(root, "ls-files", "-v", "-z").split(b"\0"):
        if not row:
            continue
        path = portable_path(row[2:].decode("utf-8"))
        if not is_excluded(path) and row[:2] != b"H ":
            # The dirty inventory must not hide worktree changes behind index
            # assume-unchanged or sparse/skip-worktree flags.
            _fail("GENERATOR_INDEX_VISIBILITY", path)
    index_modes: dict[str, str] = {}
    for row in _git(root, "ls-files", "--stage", "-z").split(b"\0"):
        if not row:
            continue
        metadata, raw_path = row.split(b"\t", 1)
        path = portable_path(raw_path.decode("utf-8"))
        if is_excluded(path):
            continue
        mode, _oid, stage = metadata.decode("ascii").split()
        if stage != "0" or mode not in {"100644", "100755"} or path in index_modes:
            _fail("INDEX_CONFLICT", path)
        index_modes[path] = mode

    def raw_namespace() -> dict[str, dict[str, Any]]:
        result = {}
        for path in sorted(names):
            target = root / path
            if not target.exists():
                if target.is_symlink():
                    _fail("RESULT_REPARSE", path)
                result[path] = _state("000000", _ZERO)
            else:
                content = bounded_regular_bytes(target)
                oid = hashlib.sha1(
                    b"blob " + str(len(content)).encode() + b"\0" + content
                ).hexdigest()
                result[path] = _state(index_modes.get(path, "100644"), oid)
            if path in sources and result[path] != sources[path]:
                _fail("REVIEW_WORKING_RESULT_CHANGED", path)
            if path not in sources and not result[path]["exists"]:
                _fail("GENERATOR_INPUT_MISSING", path)
        return result

    expected_inputs = raw_namespace()
    order = [name for name in scope["generator_order"] if name != "atlas-authority"]
    if not order or order != transaction.transaction["generator_ids"]:
        _fail("CANDIDATE_GENERATOR_ORDER")
    if _candidate_scan_admission(root, main, review) != scan_names:
        _fail("CANDIDATE_SCAN_SET_CHANGED")
    if collect_checkout_dirty_paths(root, exclusions=excluded) != dirty:
        _fail("CANDIDATE_DELTA_SET_CHANGED")
    if raw_namespace() != expected_inputs or bounded_regular_bytes(index_path) != index_bytes:
        _fail("GENERATOR_INPUT_DRIFT")
    if inspect_canonical_merge_outputs(root, task_id) != canonical:
        _fail("CANDIDATE_OUTPUT_INVENTORY_CHANGED")
    if load_current_task_authority(root, task_id) != authority:
        _fail("CANDIDATE_AUTHORITY_CHANGED")
    fence.validate(transaction_path, exact_phase=phase, task_id=task_id)
    if (
        _git(root, "rev-parse", "HEAD", "refs/heads/main") != heads
        or collect_checkout_dirty_paths(root, exclusions=excluded) != dirty
        or bounded_regular_bytes(index_path) != index_bytes
    ):
        _fail("CANDIDATE_BASE")
    return {
        "schema_version": "controlled_source_generation_inputs.v1",
        "status": "PREPARED_SOURCE_GENERATION_INPUTS",
        "task_id": task_id,
        "repository": authority["repository"],
        "main": main,
        "lane": scope["lane_head"],
        "frozen_base": scope["frozen_base"],
        "plan_sha256": plan["plan_sha256"],
        "review_ref": reference,
        "authority_sha256": authority["authority_sha256"],
        "current_event_id": authority["current_event_id"],
        "source_transaction_sha256": transaction.transaction["transaction_sha256"],
        "index_sha256": hashlib.sha256(index_bytes).hexdigest(),
        "generator_order": order,
        "dirty_paths": list(dirty),
        "expected_inputs": expected_inputs,
        "main_inputs": main_inputs,
        "canonical_output_paths": canonical["output_paths"],
        "materialization_allowed": False,
        "generator_execution_proven": False,
    }


def render_source_candidate_delta(
    root: Path,
    prepared: Mapping[str, Any],
    *,
    transaction_path: Path,
    actor: str,
    phase: str = "GENERATED_REBUILD_PRE",
    progress: Callable[[str, str, str], None] | None = None,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Render and close the actual private view; no HEAD/index/ref installation.

    This is the contained worker's internal generation step. Execution admission
    and subsequent private Git writes remain separate checks at their effects.
    Only the fixed four-generator route is executable; inspection-only subset
    fixtures cannot become a source candidate through this entry point.
    """
    if phase not in {"GENERATED_REBUILD_PRE", "CANDIDATE_COMMIT_PRE"}:
        _fail("CANDIDATE_SOURCE_PHASE")
    if progress is not None:
        progress("source-candidate", "PREPARE", "START")
    observed = prepare_source_generation(
        root,
        prepared["task_id"],
        transaction_path=transaction_path,
        actor=actor,
        phase=phase,
    )
    if dict(prepared) != observed:
        _fail("SOURCE_REQUEST_INPUTS_CHANGED")
    if prepared["generator_order"] != list(_SOURCE_GENERATORS):
        _fail("CANDIDATE_GENERATOR_ORDER")
    if progress is not None:
        progress("source-candidate", "PREPARE", "END")
        progress("source-candidate", "GENERATORS", "START")
    generation, rendered = render_source_generators(
        root, prepared["expected_inputs"], main_commit=prepared["main"], progress=progress
    )
    if progress is not None:
        progress("source-candidate", "GENERATORS", "END")
        progress("source-candidate", "DELTA_CLOSURE", "START")
    ownership: dict[str, str] = {}
    declared_outputs: dict[str, Mapping[str, Any]] = {}
    deletions: set[str] = set()
    for step in generation["steps"]:
        for row in step["outputs"]:
            name = portable_path(row["path"])
            if name in ownership:
                _fail("CANDIDATE_OUTPUT_OVERLAP", name)
            ownership[name] = step["generator_id"]
            declared_outputs[name] = row
        for name in step["deletions"]:
            name = portable_path(name)
            if name in ownership or step["generator_id"] != "compatibility-authority":
                _fail("CANDIDATE_OUTPUT_OVERLAP", name)
            ownership[name] = step["generator_id"]
            deletions.add(name)
    if set(rendered) != set(ownership):
        _fail("CANDIDATE_OUTPUT_SET_CHANGED")
    for name, content in rendered.items():
        if name in deletions:
            if content is not None or not prepared["main_inputs"].get(name, {}).get("exists"):
                _fail("CANDIDATE_OBSOLETE_INVALID", name)
        elif (
            not isinstance(content, bytes)
            or hashlib.sha256(content).hexdigest() != declared_outputs[name]["sha256"]
            or len(content) != declared_outputs[name]["byte_count"]
        ):
            _fail("CANDIDATE_OUTPUT_BYTES", name)

    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.replay(transaction_path)
    declared = (*transaction.transaction["owned_paths"], *transaction.transaction["shared_paths"])
    captured: dict[str, bytes] = {}
    operations = []
    for name in sorted(set(prepared["dirty_paths"]) | set(rendered)):
        if not any(name == item or name.startswith(item + "/") for item in declared):
            _fail("CANDIDATE_DELTA_UNDECLARED", name)
        previous = prepared["main_inputs"].get(name, _state("000000", _ZERO))
        if name in rendered:
            content = rendered[name]
            mode = previous["mode"] if previous["exists"] else "100644"
        else:
            source = prepared["expected_inputs"][name]
            content = bounded_regular_bytes(root / name) if source["exists"] else None
            mode = source["mode"]
        after = (
            _state("000000", _ZERO)
            if content is None
            else _state(
                mode,
                hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest(),
            )
        )
        if name not in rendered and after != prepared["expected_inputs"][name]:
            _fail("REVIEW_WORKING_RESULT_CHANGED", name)
        if after == previous:
            continue
        if content is not None:
            captured[name] = content
        operations.append(
            {
                "path": name,
                "operation": "D" if content is None else "M" if previous["exists"] else "A",
                "before": previous,
                "after": after,
                "origin": "OFFICIAL_GENERATOR" if name in rendered else "FROZEN_SOURCE",
                "generator_id": ownership.get(name),
            }
        )
    if progress is not None:
        progress("source-candidate", "DELTA_CLOSURE", "END")
        progress("source-candidate", "FINAL_INPUT_RECHECK", "START")
    if prepare_source_generation(
        root,
        prepared["task_id"],
        transaction_path=transaction_path,
        actor=actor,
        phase=phase,
    ) != dict(prepared):
        _fail("SOURCE_REQUEST_INPUTS_CHANGED")
    if progress is not None:
        progress("source-candidate", "FINAL_INPUT_RECHECK", "END")
    return {
        "schema_version": "rendered_source_candidate_delta.v1",
        "status": "RENDERED_SOURCE_CANDIDATE_DELTA",
        "prepared_sha256": canonical_digest(prepared),
        "main": prepared["main"],
        "lane": prepared["lane"],
        "generation": generation,
        "operations": operations,
        "materialization_allowed": False,
        "publication_allowed": False,
    }, captured


def _source_commit_bytes(intent: Mapping[str, Any]) -> bytes:
    """Independent exact Git commit bytes for the frozen UTC source intent."""
    tree, message = intent["tree"], intent["message"]
    if not isinstance(tree, str) or not isinstance(message, str):
        _fail("SOURCE_COMMIT_SHAPE")
    instant = datetime.fromisoformat(intent["timestamp"])
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        _fail("SOURCE_COMMIT_TIMESTAMP")
    identity = (
        "Source preservation coordinator <source-preservation@localhost> "
        + str(int(instant.timestamp()))
        + " +0000"
    )
    return (
        "tree "
        + tree
        + "\n"
        + "".join("parent " + parent + "\n" for parent in intent["parents"])
        + "author "
        + identity
        + "\ncommitter "
        + identity
        + "\n\n"
        + message
    ).encode()


def _source_tree_without_index(
    root: Path,
    io_backend: SourcePreservation,
    main: str,
    operations: list[dict[str, Any]],
) -> str:
    """Compose only exact M entries and already-written captured blob objects.

    The worker has independently checked every changed blob's actual write.
    Construct canonical tree bytes here, not an externally supplied raw tree.
    Neither mktree --missing (object type lookup) nor hash-object's default
    fsck_finish (attributes/modules/symlink blobs) is metadata-only. The literal
    write below therefore follows strict entry validation, with independent
    object hashing and the caller's complete actual Git tree comparison.
    """
    # Git 2.45 reports only its primary algorithm even for the "input" query.
    # A compat algorithm would enter object conversion during literal writes.
    configuration = io_backend._git(root, "config", "--no-includes", "--null", "--list")
    if (
        any(
            row.partition(b"\n")[0].lower() == b"extensions.compatobjectformat"
            for row in configuration.split(b"\0")
        )
        or io_backend._git(root, "rev-parse", "--show-object-format=input").strip() != b"sha1"
    ):
        _fail("SOURCE_TREE_OBJECT_FORMAT")
    nodes: dict[str, Any] = {}
    for record in io_backend._git(root, "ls-tree", "-r", "-t", "-z", main).split(b"\0"):
        if not record:
            continue
        metadata, raw_name = record.split(b"\t", 1)
        mode, kind, oid = metadata.decode("ascii").split()
        parts = raw_name.decode("utf-8").split("/")
        parent = nodes
        for part in parts[:-1]:
            parent = parent[part]
        if parts[-1] in parent:
            _fail("SOURCE_TREE_DUPLICATE_ENTRY")
        parent[parts[-1]] = {} if kind == "tree" else (mode, kind, oid)

    # Deletions precede additions, covering both file->directory and the inverse
    # without depending on the lexical order of independent delta rows.
    for operation in operations:
        if operation["after"]["exists"]:
            continue
        parts = portable_path(operation["path"]).split("/")
        parent = nodes
        ancestry = []
        for part in parts[:-1]:
            ancestry.append((parent, part))
            parent = parent[part]
        del parent[parts[-1]]
        for ancestor, name in reversed(ancestry):
            if ancestor[name]:
                break
            del ancestor[name]
    for operation in operations:
        after = operation["after"]
        if not after["exists"]:
            continue
        parts = portable_path(operation["path"]).split("/")
        parent = nodes
        for part in parts[:-1]:
            parent = parent.setdefault(part, {})
            if not isinstance(parent, dict):
                _fail("SOURCE_TREE_PATH_COLLISION")
        if isinstance(parent.get(parts[-1]), dict) and parent[parts[-1]]:
            _fail("SOURCE_TREE_PATH_COLLISION")
        parent[parts[-1]] = (after["mode"], after["type"], after["oid"])

    def validate_entries(children: dict[str, Any]) -> None:
        for name, entry in children.items():
            if not name or name in {".", ".."} or any(c in name for c in "/\0"):
                _fail("SOURCE_TREE_ENTRY_NAME")
            if isinstance(entry, dict):
                validate_entries(entry)
            elif (
                (entry[0], entry[1])
                not in {
                    ("100644", "blob"),
                    ("100755", "blob"),
                    ("120000", "blob"),
                    ("160000", "commit"),
                }
                or re.fullmatch(r"[0-9a-f]{40}", entry[2]) is None
                or entry[2] == _ZERO
            ):
                _fail("SOURCE_TREE_ENTRY_OBJECT")

    validate_entries(nodes)

    def write_tree(children: dict[str, Any]) -> str:
        entries = []
        ordered = sorted(
            children.items(),
            key=lambda item: (
                item[0].encode("utf-8") + (b"/" if isinstance(item[1], dict) else b"\0")
            ),
        )
        for name, entry in ordered:
            if isinstance(entry, dict):
                entry = ("40000", "tree", write_tree(entry))
            entries.append(
                entry[0].encode("ascii")
                + b" "
                + name.encode("utf-8")
                + b"\0"
                + bytes.fromhex(entry[2])
            )
        content = b"".join(entries)
        expected_oid = hashlib.sha1(
            b"tree " + str(len(content)).encode("ascii") + b"\0" + content
        ).hexdigest()
        actual_oid = (
            io_backend._git(
                root,
                "hash-object",
                "-t",
                "tree",
                "--literally",
                "-w",
                "--stdin",
                content=content,
            )
            .decode()
            .strip()
        )
        if actual_oid != expected_oid:
            _fail("SOURCE_TREE_OBJECT_HASH")
        return actual_oid

    return write_tree(nodes)


def _source_index_bytes(root: Path, candidate_sha: str) -> bytes:
    """Encode the exact candidate metadata as an ordinary SHA-1 index v2.

    No private index, retained blob read, worktree stat, or ref write is needed.
    Stat caches are intentionally invalidated; no assume-valid/skip-worktree or
    extension may hide changes. The installer binds and applies these bytes
    under its durable checkout intent, not through this pure encoder.
    Format: https://git-scm.com/docs/index-format .
    """
    configuration = _git(root, "config", "--no-includes", "--null", "--list")
    if (
        any(
            row.partition(b"\n")[0].lower() == b"extensions.compatobjectformat"
            for row in configuration.split(b"\0")
        )
        or _git(root, "rev-parse", "--show-object-format=input").strip() != b"sha1"
    ):
        _fail("SOURCE_INDEX_OBJECT_FORMAT")
    if (
        not isinstance(candidate_sha, str)
        or re.fullmatch(r"[0-9a-f]{40}", candidate_sha) is None
        or _git(root, "cat-file", "-t", candidate_sha).strip() != b"commit"
    ):
        _fail("SOURCE_INDEX_CANDIDATE")
    entries: list[tuple[bytes, str, str]] = []
    spelling: dict[str, str] = {}
    leaves: set[str] = set()
    directories: set[str] = set()
    devices = {"con", "prn", "aux", "nul"} | {
        prefix + str(number) for prefix in ("com", "lpt") for number in range(1, 10)
    }
    for record in _git(root, "ls-tree", "-r", "-z", candidate_sha).split(b"\0"):
        if not record:
            continue
        try:
            metadata, raw_name = record.split(b"\t", 1)
            mode, kind, oid = metadata.decode("ascii").split()
            name = portable_path(raw_name.decode("utf-8"))
        except (UnicodeError, ValueError) as error:
            _fail("SOURCE_INDEX_ENTRY_NAME", str(error))
        parts = name.split("/")
        if any(
            part.casefold() == ".git"
            or part != part.rstrip(". ")
            or part.split(".")[0].casefold() in devices
            or any(ord(char) < 32 or char in '<>"|?*' for char in part)
            for part in parts
        ):
            _fail("SOURCE_INDEX_ENTRY_NAME", name)
        if (
            (mode, kind)
            not in {
                ("100644", "blob"),
                ("100755", "blob"),
                ("120000", "blob"),
                ("160000", "commit"),
            }
            or re.fullmatch(r"[0-9a-f]{40}", oid) is None
            or oid == _ZERO
        ):
            _fail("SOURCE_INDEX_ENTRY_OBJECT", name)
        for depth in range(1, len(parts) + 1):
            prefix = "/".join(parts[:depth])
            key = prefix.casefold()
            if key in spelling and spelling[key] != prefix:
                _fail("SOURCE_INDEX_PATH_COLLISION", name)
            spelling[key] = prefix
            if depth < len(parts):
                if key in leaves:
                    _fail("SOURCE_INDEX_PATH_COLLISION", name)
                directories.add(key)
            else:
                if key in leaves or key in directories:
                    _fail("SOURCE_INDEX_PATH_COLLISION", name)
                leaves.add(key)
        entries.append((raw_name, mode, oid))
    content = bytearray(struct.pack("!4sII", b"DIRC", 2, len(entries)))
    for name_bytes, mode, oid in sorted(entries):
        # Ten 32-bit stat fields, the SHA-1 object, then 16-bit flags. Git v2
        # entries (including the NUL pathname terminator) align to eight bytes.
        entry = struct.pack("!10I", 0, 0, 0, 0, 0, 0, int(mode, 8), 0, 0, 0)
        entry += bytes.fromhex(oid) + struct.pack("!H", min(len(name_bytes), 0xFFF))
        entry += name_bytes
        entry += b"\0" * (8 - len(entry) % 8)
        content.extend(entry)
    return bytes(content) + hashlib.sha1(content).digest()


def _local_publication_metadata(path: Path, *, contents: bool = False) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path

    checked = _configuration_path(path.absolute())
    try:
        initial = checked.lstat()
    except FileNotFoundError:
        return {"path": checked.as_posix(), "identity": None, "sha256": None, "size": None}
    identity = (initial.st_dev, initial.st_ino)
    raw = bounded_regular_bytes(checked, expected_identity=identity)
    current = checked.lstat()
    if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
        initial.st_dev, initial.st_ino, initial.st_size, initial.st_mtime_ns,
    ):
        _fail("LOCAL_PUBLICATION_METADATA_CHANGED", checked.name)
    result: dict[str, Any] = {
        "path": checked.as_posix(), "identity": list(identity),
        "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw),
    }
    if contents:
        result["bytes_hex"] = raw.hex()
    return result


def _local_publication_checkout(root: Path) -> dict[str, Any]:
    """Git administration only: never scan or open this checkout's working files."""
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path

    root = _configuration_path(root.absolute())
    _configuration_path(root / ".git")
    locations = _git(
        root, "rev-parse", "--path-format=absolute", "--show-toplevel",
        "--git-common-dir", "--absolute-git-dir",
    ).decode("utf-8").splitlines()
    if len(locations) != 3 or Path(locations[0]) != root:
        _fail("LOCAL_PUBLICATION_CHECKOUT_IDENTITY")
    directories: dict[str, dict[str, Any]] = {}
    for label, name in zip(("root", "common", "gitdir"), locations, strict=True):
        checked = _configuration_path(Path(name))
        info = checked.lstat()
        if not stat.S_ISDIR(info.st_mode):
            _fail("LOCAL_PUBLICATION_DIRECTORY_IDENTITY", label)
        directories[label] = {
            "path": checked.as_posix(), "identity": [info.st_dev, info.st_ino],
        }
    gitdir = Path(directories["gitdir"]["path"])
    head = _local_publication_metadata(gitdir / "HEAD", contents=True)
    index = _local_publication_metadata(gitdir / "index")
    if head["identity"] is None or index["identity"] is None:
        _fail("LOCAL_PUBLICATION_ADMIN_MISSING")
    locator = root / ".git"
    locator_info = locator.lstat()
    locator_binding = (
        {"path": locator.as_posix(), "directory": True,
         "identity": [locator_info.st_dev, locator_info.st_ino]}
        if stat.S_ISDIR(locator_info.st_mode)
        else _local_publication_metadata(locator, contents=True)
    )
    return {
        **directories, "head": head, "index": index, "git_locator": locator_binding,
        "commondir_locator": _local_publication_metadata(gitdir / "commondir", contents=True),
        "observed_head": _git(root, "rev-parse", "HEAD").decode("ascii").strip(),
    }


def _local_publication_ref_directories(common: Path) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path

    result = {}
    for relative in ("refs", "refs/heads"):
        path = _configuration_path(common / relative)
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode):
            _fail("LOCAL_PUBLICATION_DIRECTORY_IDENTITY", relative)
        result[relative] = {"path": path.as_posix(), "identity": [info.st_dev, info.st_ino]}
    return result


def _local_publication_worktrees(raw: bytes) -> list[dict[str, str]]:
    records = []
    for group in raw.split(b"\0\0"):
        if not group:
            continue
        record: dict[str, str] = {}
        for field in group.rstrip(b"\0").split(b"\0"):
            key, _, value = field.partition(b" ")
            name = key.decode("ascii")
            if name not in {"worktree", "HEAD", "branch", "bare", "detached", "locked", "prunable"}:
                _fail("LOCAL_PUBLICATION_WORKTREE_SHAPE")
            if name in record:
                _fail("LOCAL_PUBLICATION_WORKTREE_SHAPE")
            record[name] = value.decode("utf-8")
        if "worktree" not in record:
            _fail("LOCAL_PUBLICATION_WORKTREE_SHAPE")
        records.append(record)
    return records


def _require_publication_candidate_index(root: Path, candidate: str) -> None:
    """Compare the complete real index to C without reading peer working files."""
    tree = {}
    for row in _git(root, "ls-tree", "-r", "-z", candidate).split(b"\0"):
        if row:
            metadata, name = row.split(b"\t", 1)
            mode, _kind, oid = metadata.split()
            tree[name] = (mode, oid)
    index = {}
    for row in _git(root, "ls-files", "--stage", "-z").split(b"\0"):
        if row:
            metadata, name = row.split(b"\t", 1)
            mode, oid, stage = metadata.split()
            if stage != b"0" or name in index:
                _fail("LOCAL_PUBLICATION_INDEX_STAGE")
            index[name] = (mode, oid)
    if tree != index:
        _fail("LOCAL_PUBLICATION_INDEX_CANDIDATE_MISMATCH")
    if any(row and not row.startswith(b"H ")
           for row in _git(root, "ls-files", "-v", "-z").split(b"\0")):
        _fail("LOCAL_PUBLICATION_INDEX_HIDDEN")


def inspect_local_publication_topology(
    root: Path, *, candidate: str, expected_main: str,
) -> dict[str, Any]:
    """Read-only P01 input capture, not a publication or recovery capability.

    The future mutating entry must bind this topology to original fence/Full
    custody and recheck under its native locks. A recomputed digest grants no
    rights. Peer working files are deliberately outside this reader's scope.
    """
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path

    _configuration_path(root.absolute() / ".git")
    repository_identity(root)
    candidate, expected_main = _commit(root, candidate), _commit(root, expected_main)
    if _git(root, "rev-parse", "HEAD").decode().strip() != candidate:
        _fail("LOCAL_PUBLICATION_CANDIDATE_CHANGED")
    if _git(root, "rev-parse", "refs/heads/main").decode().strip() != expected_main:
        _fail("LOCAL_PUBLICATION_MAIN_CHANGED")
    if _git(root, "merge-base", expected_main, candidate).decode().strip() != expected_main:
        _fail("LOCAL_PUBLICATION_ANCESTRY")
    raw_worktrees = _git(root, "worktree", "list", "--porcelain", "-z")
    records = _local_publication_worktrees(raw_worktrees)
    main_records = [row for row in records if row.get("branch") == "refs/heads/main"]
    if len(main_records) > 1:
        _fail("LOCAL_PUBLICATION_MULTIPLE_MAIN_CHECKOUTS")
    current = _local_publication_checkout(root)
    if current["observed_head"] != candidate:
        _fail("LOCAL_PUBLICATION_CANDIDATE_CHANGED")
    branch_bytes = bytes.fromhex(current["head"]["bytes_hex"])
    if not branch_bytes.startswith(b"ref: refs/heads/") or not branch_bytes.endswith(b"\n"):
        _fail("LOCAL_PUBLICATION_CANDIDATE_BRANCH")
    branch = portable_path(branch_bytes[5:-1].decode("utf-8"))
    peer = None
    if main_records:
        main_record = main_records[0]
        if "locked" in main_record or "prunable" in main_record:
            _fail("LOCAL_PUBLICATION_MAIN_WORKTREE_UNAVAILABLE")
        peer = _local_publication_checkout(Path(main_record["worktree"]))
        if (
            peer["common"] != current["common"]
            or peer["observed_head"] != expected_main
            or bytes.fromhex(peer["head"]["bytes_hex"]) != b"ref: refs/heads/main\n"
        ):
            _fail("LOCAL_PUBLICATION_MAIN_CHECKOUT_CHANGED")
    _require_publication_candidate_index(root, candidate)
    common = Path(current["common"]["path"])
    ref_directories = _local_publication_ref_directories(common)
    ref = _local_publication_metadata(common / "refs/heads/main", contents=True)
    packed = _local_publication_metadata(common / "packed-refs")
    config = _local_publication_metadata(common / "config")
    if ref["identity"] is not None and bytes.fromhex(ref["bytes_hex"]) != (
        expected_main + "\n"
    ).encode():
        _fail("LOCAL_PUBLICATION_MAIN_STORAGE_CHANGED")
    if (
        _local_publication_checkout(root) != current
        or (peer is not None and _local_publication_checkout(Path(peer["root"]["path"])) != peer)
        or _git(root, "worktree", "list", "--porcelain", "-z") != raw_worktrees
        or _local_publication_metadata(common / "refs/heads/main", contents=True) != ref
        or _local_publication_metadata(common / "packed-refs") != packed
        or _local_publication_metadata(common / "config") != config
        or _local_publication_ref_directories(common) != ref_directories
        or _git(root, "rev-parse", "refs/heads/main").decode().strip() != expected_main
    ):
        _fail("LOCAL_PUBLICATION_CAPTURE_CHANGED")
    result = {
        "schema_version": "workflow_local_publication_topology.v1",
        "candidate_sha": candidate, "expected_main_sha": expected_main,
        "candidate_branch": branch, "candidate_checkout": current, "main_checkout": peer,
        "main_ref": ref, "packed_refs": packed, "git_config": config,
        "ref_directories": ref_directories,
        "worktree_inventory_sha256": hashlib.sha256(raw_worktrees).hexdigest(),
        "candidate_index_matches_tree": True, "peer_worktree_files_read": False,
        "publication_allowed": False, "dispatch_allowed": False, "mutation_performed": False,
    }
    return {**result, "topology_sha256": canonical_digest(result)}


def inspect_publication_switched_heads(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Exact pre-resume handoff observation, not a substitute Full or a permit.

    Only the two original HEAD changes are normalized. Main, source branch,
    every other worktree, both indexes, locators and auxiliary state must still
    be original. Ordinary C-HEAD admission remains unchanged outside this seam.
    """
    return _inspect_publication_head_window(root, request, plan, merge=None)


def inspect_publication_merge_window(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any], merge: Mapping[str, Any],
    *, auto_merge_cleanup: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read actual Git effects against the original native prepared-file records.

    The original fence separately validates merge history and live hook origin.
    This observer grants no authority from a caller-provided record by itself.
    """
    return _inspect_publication_head_window(
        root, request, plan, merge=merge, auto_merge_cleanup=auto_merge_cleanup,
    )


def observe_publication_auto_merge_lock(
    plan: Mapping[str, Any], merge: Mapping[str, Any], stage: str,
) -> dict[str, Any] | None:
    """Observe Git's empty cleanup lock; caller must hold original native hook origin.

    This is not an admission capability. The fence independently proves that
    origin before allowing the observation through its effect-window gate.
    """
    seen = {(row["reference_kind"], row["stage"]) for row in merge["hooks"]}
    if (stage not in {"aborted", "prepared", "committed"} or merge["exit"] is not None
            or ("FAST_FORWARD", "committed") not in seen
            or not any(row["kind"] == "post-merge" for row in merge["hooks"])
            or ("AUTO_MERGE", "committed") in seen):
        _fail("PUBLICATION_AUTO_MERGE_LOCK_PHASE")
    path = Path(plan["topology"]["candidate_checkout"]["common"]["path"]) / "packed-refs.lock"
    previous = [row["cleanup_lock"] for row in merge["hooks"]
                if row.get("cleanup_lock") is not None]
    try:
        info = path.lstat()
    except FileNotFoundError:
        if stage == "prepared":
            _fail("PUBLICATION_AUTO_MERGE_LOCK_MISSING")
        return None
    if (stage == "committed" or info.st_size != 0
            or any(row["identity"] != [info.st_dev, info.st_ino] for row in previous)):
        _fail("PUBLICATION_AUTO_MERGE_LOCK_CHANGED")
    observed = _local_publication_metadata(path, contents=True)
    if (observed.get("bytes_hex") != "" or observed["size"] != 0
            or any(row != observed for row in previous)):
        _fail("PUBLICATION_AUTO_MERGE_LOCK_CHANGED")
    return observed


def inspect_publication_recovery_window(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, recovery: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Observe only original M and recognized before/after HEADs, without writes.

    The owning fence must first prove the original terminal attempt. Prepared
    lock records are not permits until that original Git and its Job are dead.
    Unknown residue is rejected before reading its content, never removed here.
    """
    return _inspect_publication_head_window(
        root, request, plan, merge=merge, recovering=True, recovery=recovery,
    )


def _observe_publication_prepared_lock(row: Mapping[str, Any]) -> dict[str, Any] | None:
    path = Path(row["path"])
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    if [metadata.st_dev, metadata.st_ino] != row["identity"]:
        _fail("PUBLICATION_RECOVERY_LOCK_CHANGED")
    raw = bounded_regular_bytes(path, expected_identity=tuple(row["identity"]))
    if raw.hex() != row["bytes_hex"]:
        _fail("PUBLICATION_RECOVERY_LOCK_CHANGED")
    return dict(row)


def inspect_publication_main_advanced_recovery_window(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, recovery: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Observe a single M->N advance before original FAST_FORWARD preparation.

    This read-only scene grants no recovery or publication authority. The original
    fence must separately prove terminal process custody and bind the observation
    before any inverse write. A handed-off peer must remain detached at M; only
    the candidate may have regained its original branch at C.
    """
    if recovery is not None:
        if recovery.get("schema_version") != "workflow_publication_head_recovery.v2":
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_BINDING")
        validate_publication_main_advanced_recovery_observation(
            recovery["scene_before"], request, plan, merge,
        )
    scene = _inspect_publication_head_window(
        root, request, plan, merge=merge, recovering=True, recovery=recovery,
        main_advanced_recovery=True,
    )
    if recovery is not None:
        before = recovery["scene_before"]
        if (any(scene[key] != before[key] for key in ("main_sha", "main_ref"))
                or scene["auxiliary"]["reflogs"] != before["auxiliary"]["reflogs"]
                or scene["checkouts"]["candidate_head"]["index"]
                != before["checkouts"]["candidate_head"]["index"]):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_BINDING")
    return scene


def validate_publication_recovery_observation(
    scene: Mapping[str, Any], request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, *, restored_orig: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay the original recovery scene and derive its only legal ORIG_HEAD result.

    This pure contract is not an admission API; runtime callers must observe the
    actual original store, native process death and files before any mutation.
    """
    return _validate_publication_recovery_scene(
        scene, request, plan, merge, restored_orig=restored_orig, main_advanced=False,
    )


@contextmanager
def hold_publication_main_advanced_recovery_scene(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, recovery: Mapping[str, Any] | None,
) -> Iterator[dict[str, Any]]:
    """Hold N and both reflogs; custody is not original-fence recovery authority.

    Git can append logs before a failed ref rename. Pin every existing journal
    along with N before allowing the original caller's inverse writes. Missing
    journals cannot silently receive the guarantees of a held file.
    """
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path
    from ai_trading_system.platform.architecture.workflow_contract import hold_bound_read_file

    scene = inspect_publication_main_advanced_recovery_window(root, request, plan, merge, recovery)
    checkout = plan["topology"]["candidate_checkout"]
    rows = [(checkout["common"], scene["main_ref"], True)]
    rows.extend((checkout["gitdir"] if role == "candidate" else checkout["common"], row, False)
                for role, row in scene["auxiliary"]["reflogs"].items())
    with ExitStack() as custody:
        for directory, row, contents in rows:
            if row["identity"] is None:
                _fail("PUBLICATION_RECOVERY_JOURNAL_CUSTODY_UNAVAILABLE")
            base, path = Path(directory["path"]), Path(row["path"])
            relative = path.relative_to(base)
            parents = {}
            for parent in relative.parents:
                if parent == Path("."):
                    continue
                metadata = _configuration_path(base / parent).lstat()
                if not stat.S_ISDIR(metadata.st_mode):
                    _fail("PUBLICATION_RECOVERY_CUSTODY_PARENT")
                parents[parent.as_posix()] = (metadata.st_dev, metadata.st_ino)
            if contents and parents != {
                key: tuple(value["identity"])
                for key, value in plan["topology"]["ref_directories"].items()
            }:
                _fail("PUBLICATION_RECOVERY_CUSTODY_PARENT")
            raw = bounded_regular_bytes(path, expected_identity=tuple(row["identity"]))
            if len(raw) != row["size"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                _fail("PUBLICATION_RECOVERY_CUSTODY_CHANGED")
            custody.enter_context(hold_bound_read_file(
                base, relative.as_posix(), expected=raw, expected_identity=tuple(row["identity"]),
                expected_root_identity=tuple(directory["identity"]),
                expected_parent_identities=parents,
            ))
        if inspect_publication_main_advanced_recovery_window(
            root, request, plan, merge, recovery,
        ) != scene:
            _fail("PUBLICATION_RECOVERY_CUSTODY_CHANGED")
        yield scene
        for _, row, contents in rows:
            if _local_publication_metadata(Path(row["path"]), contents=contents) != row:
                _fail("PUBLICATION_RECOVERY_CUSTODY_CHANGED")


def validate_publication_main_advanced_recovery_observation(
    scene: Mapping[str, Any], request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, *, restored_orig: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay the distinct N scene; no serialized record proves live Git lineage.

    Runtime observation must independently verify ancestry, original reflog
    prefixes and native identities. This validator keeps those observations
    structurally bound without rewriting the original M plan into an N plan.
    """
    return _validate_publication_recovery_scene(
        scene, request, plan, merge, restored_orig=restored_orig, main_advanced=True,
    )


def _validate_publication_recovery_scene(
    scene: Mapping[str, Any], request: Mapping[str, Any], plan: Mapping[str, Any],
    merge: Mapping[str, Any] | None, *, restored_orig: Mapping[str, Any] | None,
    main_advanced: bool,
) -> dict[str, Any]:
    validate_local_publication_checkout_plan(plan, request)
    if (not isinstance(scene, Mapping) or set(scene) != {
        "schema_version", "request_sha256", "plan_sha256", "checkouts", "main_sha", "main_ref",
        "worktree_inventory_sha256", "candidate_index_matches_tree", "auxiliary", "owned_locks",
        "dispatch_allowed", "publication_allowed", "mutation_performed",
    } or scene["schema_version"] != (
        "workflow_publication_main_advanced_recovery_window_observation.v1" if main_advanced
        else "workflow_publication_recovery_window_observation.v1"
    )
            or scene["request_sha256"] != canonical_digest(request)
            or scene["plan_sha256"] != plan["plan_sha256"]
            or (not main_advanced and (
                scene["main_sha"] != request["expected_main_sha"]
                or scene["main_ref"] != plan["topology"]["main_ref"]
            ))
            or scene["candidate_index_matches_tree"] is not True
            or any(scene[key] is not False for key in (
                "dispatch_allowed", "publication_allowed", "mutation_performed",
            ))):
        _fail("PUBLICATION_RECOVERY_SCENE")
    if (not isinstance(scene["worktree_inventory_sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", scene["worktree_inventory_sha256"]) is None):
        _fail("PUBLICATION_RECOVERY_SCENE")
    topology = plan["topology"]
    if main_advanced:
        if (not isinstance(scene["main_sha"], str)
                or re.fullmatch(r"[0-9a-f]{40}", scene["main_sha"]) is None
                or scene["main_sha"] in {request["candidate_sha"], request["expected_main_sha"]}
                or (merge is not None and any(
                    row["reference_kind"] != "ORIG_HEAD" for row in merge["hooks"]
                ))):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_BOUNDARY")
        _publication_plan_metadata(
            scene["main_ref"], Path(topology["main_ref"]["path"]), contents=True,
        )
        if (scene["main_ref"]["identity"] is None
                or bytes.fromhex(scene["main_ref"]["bytes_hex"])
                != (scene["main_sha"] + "\n").encode("ascii")):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REF")
    checkouts = scene["checkouts"]
    if not isinstance(checkouts, Mapping) or set(checkouts) != {
        row["role"] for row in plan["head_transitions"]
    }:
        _fail("PUBLICATION_RECOVERY_CHECKOUT")
    for transition in plan["head_transitions"]:
        role = transition["role"]
        expected = json.loads(json.dumps(
            topology["candidate_checkout"] if role == "candidate_head"
            else topology["main_checkout"]
        ))
        current = checkouts[role]
        if not isinstance(current, Mapping):
            _fail("PUBLICATION_RECOVERY_CHECKOUT")
        if merge is not None and role == "candidate_head":
            index = current.get("index")
            if not isinstance(index, Mapping):
                _fail("PUBLICATION_RECOVERY_INDEX")
            _publication_plan_metadata(index, Path(expected["index"]["path"]))
            if index["identity"] is None or index["size"] <= 0:
                _fail("PUBLICATION_RECOVERY_INDEX")
            expected["index"] = index
        if (current.get("head") != transition["before"]
                or (main_advanced and role == "peer_head")):
            raw = bytes.fromhex(transition["after_hex"])
            expected["head"] = {**transition["before"], "bytes_hex": raw.hex(), "size": len(raw),
                                "sha256": hashlib.sha256(raw).hexdigest()}
            expected["observed_head"] = (
                scene["main_sha"] if main_advanced and role == "candidate_head"
                else request["expected_main_sha"]
            )
        if current != expected:
            _fail("PUBLICATION_RECOVERY_CHECKOUT")
    prepared = {} if merge is None else {
        row["reference_kind"]: row["prepared_file"] for row in merge["hooks"]
        if row["stage"] == "prepared" and row["prepared_file"] is not None
    }
    auxiliary = scene["auxiliary"]
    if (not isinstance(auxiliary, Mapping) or set(auxiliary) != {"orig_head", "reflogs"}
            or (not main_advanced and auxiliary["reflogs"] != plan["reflogs"])):
        _fail("PUBLICATION_RECOVERY_AUXILIARY")
    if main_advanced:
        logs = auxiliary["reflogs"]
        if not isinstance(logs, Mapping) or set(logs) != set(plan["reflogs"]):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG")
        for role, before in plan["reflogs"].items():
            current = logs[role]
            _publication_plan_metadata(current, Path(before["path"]))
            if role == "candidate" and current == before:
                continue
            if (current["identity"] is None or current["size"] <= (before["size"] or 0)
                    or current["sha256"] == before["sha256"]
                    or (before["identity"] is not None
                        and current["identity"] != before["identity"])):
                _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG", role)
    original = plan["orig_head"]
    orig = auxiliary["orig_head"]
    permitted = [original]
    if "ORIG_HEAD" in prepared:
        permitted.append({**prepared["ORIG_HEAD"], "path": original["path"]})
    if restored_orig is not None:
        permitted.append(restored_orig)
    if orig not in permitted:
        _fail("PUBLICATION_RECOVERY_AUXILIARY")
    locks = scene["owned_locks"]
    known = {row["path"]: row for row in prepared.values()}
    if (not isinstance(locks, Mapping)
            or any(known.get(key) != value for key, value in locks.items())):
        _fail("PUBLICATION_RECOVERY_LOCK")
    if orig == original or original["identity"] is None:
        return dict(original)
    # Restoring bytes in the owned replacement preserves its *new* native ID;
    # never fabricate resurrection of the original deleted file object.
    return {**original, "identity": orig["identity"]}


def _inspect_publication_head_window(
    root: Path, request: Mapping[str, Any], plan: Mapping[str, Any], *,
    merge: Mapping[str, Any] | None,
    recovering: bool = False, recovery: Mapping[str, Any] | None = None,
    auto_merge_cleanup: Mapping[str, Any] | None = None,
    main_advanced_recovery: bool = False,
) -> dict[str, Any]:
    validate_local_publication_checkout_plan(plan, request)
    if root.as_posix() != request["cwd"]:
        _fail("PUBLICATION_EFFECT_ROOT")
    original = plan["topology"]
    candidate, main = request["candidate_sha"], request["expected_main_sha"]
    prepared = {} if merge is None else {
        row["reference_kind"]: row["prepared_file"] for row in merge["hooks"]
        if row["stage"] == "prepared" and row["prepared_file"] is not None
    }
    actual_main = _git(root, "rev-parse", "refs/heads/main").decode().strip()
    if main_advanced_recovery:
        if (not recovering or auto_merge_cleanup is not None or actual_main in {main, candidate}
                or (merge is not None and any(
                    row["reference_kind"] != "ORIG_HEAD" for row in merge["hooks"]
                )) or _git(root, "merge-base", main, actual_main).decode().strip() != main):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_BOUNDARY")
    if ((not main_advanced_recovery and actual_main not in (
        {main, candidate} if "FAST_FORWARD" in prepared and not recovering else {main}
    ))
            or _git(root, "rev-parse", original["candidate_branch"]).decode().strip() != candidate
            or _git(root, "merge-base", main, candidate).decode().strip() != main):
        _fail("PUBLICATION_EFFECT_REFS")
    observed = {}
    originals = {"candidate_head": original["candidate_checkout"]}
    if original["main_checkout"] is not None:
        originals["peer_head"] = original["main_checkout"]
    for transition in plan["head_transitions"]:
        role = transition["role"]
        before = originals[role]
        current = _local_publication_checkout(Path(before["root"]["path"]))
        expected = json.loads(json.dumps(before))
        raw = bytes.fromhex(transition["after_hex"])
        expected["head"] = {**transition["before"], "bytes_hex": raw.hex(),
                            "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        expected["observed_head"] = actual_main if role == "candidate_head" else main
        if (recovering and current["head"] == transition["before"]
                and not (main_advanced_recovery and role == "peer_head")):
            expected["head"] = transition["before"]
            expected["observed_head"] = before["observed_head"]
        if merge is not None and role == "candidate_head":
            # Physical index replacement is a normal Git refresh. Its complete
            # mode/OID/stage/hidden-flag correspondence to C is checked below.
            expected["index"] = current["index"]
        if current != expected:
            _fail("PUBLICATION_EFFECT_CHECKOUT", role)
        observed[role] = current
    _require_publication_candidate_index(root, candidate)
    common = Path(original["candidate_checkout"]["common"]["path"])
    main_ref = original["main_ref"]
    if main_advanced_recovery:
        main_ref = _local_publication_metadata(common / "refs/heads/main", contents=True)
        if (main_ref["identity"] is None or bytes.fromhex(main_ref["bytes_hex"])
                != (actual_main + "\n").encode("ascii")):
            _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REF")
    elif actual_main == candidate and candidate != main:
        main_ref = {**prepared["FAST_FORWARD"], "path": main_ref["path"]}
    for key, path, contents in (
        ("main_ref", common / "refs/heads/main", True),
        ("packed_refs", common / "packed-refs", False),
        ("git_config", common / "config", False),
    ):
        expected_admin = main_ref if key == "main_ref" else original[key]
        if _local_publication_metadata(path, contents=contents) != expected_admin:
            _fail("PUBLICATION_EFFECT_ADMIN", key)
    if _local_publication_ref_directories(common) != original["ref_directories"]:
        _fail("PUBLICATION_EFFECT_ADMIN", "ref_directories")
    def auxiliary() -> dict[str, Any]:
        orig = _local_publication_metadata(Path(plan["orig_head"]["path"]), contents=True)
        orig_committed = merge is not None and any(
            row["reference_kind"] == "ORIG_HEAD" and row["stage"] == "committed"
            for row in merge["hooks"]
        )
        permitted_orig = [plan["orig_head"]] if recovering or not orig_committed else []
        if "ORIG_HEAD" in prepared:
            permitted_orig.append({**prepared["ORIG_HEAD"], "path": plan["orig_head"]["path"]})
        if recovering and recovery is not None:
            permitted_orig.append(recovery["restored_orig_head"])
        if orig not in permitted_orig:
            _fail("PUBLICATION_EFFECT_AUXILIARY", "ORIG_HEAD")
        logs = {}
        for role, before in plan["reflogs"].items():
            path = Path(before["path"])
            current = _local_publication_metadata(path)
            if main_advanced_recovery:
                if role == "candidate" and current == before:
                    logs[role] = current
                    continue
                if current["identity"] is None:
                    _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG", role)
                raw = bounded_regular_bytes(path, expected_identity=tuple(current["identity"]))
                length = before["size"] or 0
                prefix, suffix = raw[:length], raw[length:]
                if ((before["identity"] is not None and (
                    current["identity"] != before["identity"]
                    or hashlib.sha256(prefix).hexdigest() != before["sha256"]
                )) or suffix.count(b"\n") != 1 or not suffix.endswith(b"\n")
                        or not suffix.startswith((main + " " + actual_main + " ").encode("ascii"))
                        or b"\t" not in suffix):
                    _fail("PUBLICATION_RECOVERY_MAIN_ADVANCE_REFLOG", role)
            elif actual_main == main:
                if current != before:
                    _fail("PUBLICATION_EFFECT_AUXILIARY", role)
            else:
                if current["identity"] is None:
                    _fail("PUBLICATION_EFFECT_AUXILIARY", role)
                raw = bounded_regular_bytes(path, expected_identity=tuple(current["identity"]))
                length = before["size"] or 0
                prefix, suffix = raw[:length], raw[length:]
                if ((before["identity"] is not None and (
                    current["identity"] != before["identity"]
                    or hashlib.sha256(prefix).hexdigest() != before["sha256"]
                )) or suffix.count(b"\n") != 1 or not suffix.startswith(
                    (main + " " + candidate + " ").encode("ascii")
                ) or not suffix.endswith((
                    "\tmerge " + plan["merge_argv_tail"][-1] + ": Fast-forward\n"
                ).encode("utf-8"))):
                    _fail("PUBLICATION_EFFECT_AUXILIARY", role)
            logs[role] = current
        return {"orig_head": orig, "reflogs": logs}

    auxiliary_before = auxiliary()
    allowed_locks: set[str] = set()
    if merge is not None and merge["exit"] is None and not recovering:
        gitdir = Path(original["candidate_checkout"]["gitdir"]["path"])
        allowed_locks = {(gitdir / name).as_posix() for name in (
            "HEAD.lock", "index.lock", "ORIG_HEAD.lock", "AUTO_MERGE.lock",
        )} | {(common / "refs/heads/main.lock").as_posix()}
    if auto_merge_cleanup is not None:
        if (recovering or merge is None or actual_main != candidate
                or set(auto_merge_cleanup) != {"stage", "metadata"}
                or auto_merge_cleanup["stage"] not in {"aborted", "prepared"}
                or auto_merge_cleanup["metadata"] is None
                or observe_publication_auto_merge_lock(
                    plan, merge, auto_merge_cleanup["stage"],
                ) != auto_merge_cleanup["metadata"]):
            _fail("PUBLICATION_AUTO_MERGE_LOCK_BINDING")
        allowed_locks.add((common / "packed-refs.lock").as_posix())
    owned_locks = {}
    if recovering:
        allowed_locks = {row["path"] for row in prepared.values()}
        # Only original prepared files can even be read as known residue.
        _require_publication_plan_absences([
            name for name in plan["absent_paths"] if name not in allowed_locks
        ])
        for row in prepared.values():
            current_lock = _observe_publication_prepared_lock(row)
            if current_lock is not None:
                owned_locks[row["path"]] = current_lock
    absences = [name for name in plan["absent_paths"] if name not in allowed_locks]
    _require_publication_plan_absences(absences)
    raw_inventory = _git(root, "worktree", "list", "--porcelain", "-z")
    records = _local_publication_worktrees(raw_inventory)
    normalized = []
    seen: set[str] = set()
    for record in records:
        restored = dict(record)
        name = record["worktree"]
        for role, before in originals.items():
            if Path(name) != Path(before["root"]["path"]):
                continue
            if role in seen:
                _fail("PUBLICATION_EFFECT_INVENTORY")
            seen.add(role)
            if recovering and observed[role]["head"] == before["head"]:
                if (record.get("HEAD") != before["observed_head"]
                        or record.get("branch") != (
                            original["candidate_branch"] if role == "candidate_head"
                            else "refs/heads/main"
                        ) or "detached" in record):
                    _fail("PUBLICATION_EFFECT_INVENTORY")
                continue
            if role == "candidate_head":
                if record.get("branch") != "refs/heads/main" or record.get("HEAD") != actual_main:
                    _fail("PUBLICATION_EFFECT_INVENTORY")
                restored["branch"] = original["candidate_branch"]
                restored["HEAD"] = candidate
            else:
                if record.get("detached") != "" or "branch" in record or record.get("HEAD") != main:
                    _fail("PUBLICATION_EFFECT_INVENTORY")
                # Preserve Git's field order while restoring only the original
                # branch marker at the detached marker's exact position.
                restored = {("branch" if key == "detached" else key): (
                    "refs/heads/main" if key == "detached" else item
                ) for key, item in record.items()}
        normalized.append(b"\0".join(
            (key + (" " + item if item else "")).encode("utf-8")
            for key, item in restored.items()
        ) + b"\0\0")
    if (seen != set(originals) or hashlib.sha256(b"".join(normalized)).hexdigest()
            != original["worktree_inventory_sha256"]):
        _fail("PUBLICATION_EFFECT_INVENTORY")
    # Reobserve both actual HEAD/index namespaces and every auxiliary object;
    # this catches capture-time changes without writing or normalizing on disk.
    for role, current in observed.items():
        if _local_publication_checkout(Path(current["root"]["path"])) != current:
            _fail("PUBLICATION_EFFECT_CAPTURE_CHANGED", role)
    if (_git(root, "worktree", "list", "--porcelain", "-z") != raw_inventory
            or _git(root, "rev-parse", "refs/heads/main").decode().strip() != actual_main
            or _git(root, "rev-parse", original["candidate_branch"]).decode().strip() != candidate
            or auxiliary() != auxiliary_before):
        _fail("PUBLICATION_EFFECT_CAPTURE_CHANGED")
    for key, path, contents in (
        ("main_ref", common / "refs/heads/main", True),
        ("packed_refs", common / "packed-refs", False),
        ("git_config", common / "config", False),
    ):
        expected_admin = main_ref if key == "main_ref" else original[key]
        if _local_publication_metadata(path, contents=contents) != expected_admin:
            _fail("PUBLICATION_EFFECT_CAPTURE_CHANGED", key)
    if _local_publication_ref_directories(common) != original["ref_directories"]:
        _fail("PUBLICATION_EFFECT_CAPTURE_CHANGED", "ref_directories")
    _require_publication_plan_absences(absences)
    if recovering:
        for row in prepared.values():
            current_lock = _observe_publication_prepared_lock(row)
            if owned_locks.get(row["path"]) != current_lock:
                _fail("PUBLICATION_EFFECT_CAPTURE_CHANGED", "owned_lock")
    if auto_merge_cleanup is not None and merge is not None and (
        observe_publication_auto_merge_lock(plan, merge, auto_merge_cleanup["stage"])
        != auto_merge_cleanup["metadata"]
    ):
        _fail("PUBLICATION_AUTO_MERGE_LOCK_CHANGED")
    return {"schema_version": (
                "workflow_publication_main_advanced_recovery_window_observation.v1"
                if main_advanced_recovery else
                "workflow_publication_recovery_window_observation.v1" if recovering else
                "workflow_publication_switched_heads_observation.v1" if merge is None else
                "workflow_publication_merge_window_observation.v1"
            ),
            "request_sha256": canonical_digest(request), "plan_sha256": plan["plan_sha256"],
            "checkouts": observed,
            "worktree_inventory_sha256": hashlib.sha256(raw_inventory).hexdigest(),
            "candidate_index_matches_tree": True, "main_sha": actual_main,
            **({"auxiliary": auxiliary_before, "main_ref": main_ref}
               if merge is not None or recovering else {}),
            **({"owned_locks": owned_locks} if recovering else {}),
            "dispatch_allowed": False, "publication_allowed": False, "mutation_performed": False}


_PUBLICATION_MERGE_STATE = (
    "AUTO_MERGE", "MERGE_HEAD", "MERGE_MSG", "MERGE_MODE", "MERGE_RR",
    "MERGE_AUTOSTASH", "SQUASH_MSG",
)


def _publication_plan_absences(topology: Mapping[str, Any]) -> list[str]:
    checkout = topology["candidate_checkout"]
    gitdir = Path(checkout["gitdir"]["path"])
    common = Path(checkout["common"]["path"])
    paths = [gitdir / name for name in (*_PUBLICATION_MERGE_STATE, "HEAD.lock", "index.lock",
                                        "ORIG_HEAD.lock", "AUTO_MERGE.lock")]
    paths += [common / "refs/heads/main.lock", common / "packed-refs.lock"]
    peer = topology["main_checkout"]
    if peer is not None:
        paths.append(Path(peer["gitdir"]["path"]) / "HEAD.lock")
    return sorted({path.as_posix() for path in paths})


def _require_publication_plan_absences(paths: Sequence[str]) -> None:
    from ai_trading_system.platform.architecture.source_preservation import _configuration_path

    for name in paths:
        path = _configuration_path(Path(name))
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        # Unknown locks/in-flight merge state are never read or deleted here.
        _fail("PUBLICATION_CHECKOUT_PLAN_STATE_EXISTS", path.name)


def _publication_plan_metadata(
    value: Mapping[str, Any], path: Path, *, contents: bool = False,
) -> None:
    if not isinstance(value, Mapping) or value.get("path") != path.as_posix():
        _fail("PUBLICATION_CHECKOUT_PLAN_METADATA")
    fields = {"path", "identity", "sha256", "size"}
    identity = value.get("identity")
    if identity is None:
        if set(value) != fields or any(value[key] is not None for key in fields - {"path"}):
            _fail("PUBLICATION_CHECKOUT_PLAN_METADATA")
        return
    if (set(value) != fields | ({"bytes_hex"} if contents else set())
            or not isinstance(identity, list) or len(identity) != 2
            or any(type(item) is not int or item < 0 for item in identity)
            or type(value["size"]) is not int or value["size"] < 0
            or not isinstance(value["sha256"], str)
            or re.fullmatch(r"[a-f0-9]{64}", value["sha256"]) is None):
        _fail("PUBLICATION_CHECKOUT_PLAN_METADATA")
    if contents:
        raw = bytes.fromhex(value["bytes_hex"])
        if (raw.hex() != value["bytes_hex"] or len(raw) != value["size"]
                or hashlib.sha256(raw).hexdigest() != value["sha256"]):
            _fail("PUBLICATION_CHECKOUT_PLAN_METADATA")


def validate_local_publication_checkout_plan(
    plan: Mapping[str, Any], request: Mapping[str, Any],
) -> None:
    """Structural binding only; does not admit Full or authorize a checkout effect."""
    try:
        fields = {"schema_version", "request_sha256", "topology", "head_transitions",
                  "orig_head", "orig_head_after_hex", "reflogs", "absent_paths", "merge_argv_tail",
                  "peer_handoff_required", "dispatch_allowed", "publication_allowed",
                  "resume_allowed", "plan_sha256"}
        if (set(plan) != fields or plan["schema_version"] != "workflow_publication_checkout_plan.v1"
                or plan["request_sha256"] != canonical_digest(request)
                or plan["plan_sha256"] != canonical_digest(
                    {key: value for key, value in plan.items() if key != "plan_sha256"}
                ) or any(plan[key] is not False for key in (
                    "dispatch_allowed", "publication_allowed", "resume_allowed",
                ))):
            _fail("PUBLICATION_CHECKOUT_PLAN_BINDING")
        topology = plan["topology"]
        if (topology["candidate_sha"] != request["candidate_sha"]
                or topology["expected_main_sha"] != request["expected_main_sha"]
                or topology["topology_sha256"] != canonical_digest(
                    {key: value for key, value in topology.items() if key != "topology_sha256"}
                )):
            _fail("PUBLICATION_CHECKOUT_PLAN_TOPOLOGY")
        intent = {
            "schema_version": "integration_publication_local_intent.v1",
            "transaction_sha256": request["publication_transaction_sha256"],
            "lease_id": request["lease_id"], "candidate_sha": request["candidate_sha"],
            "expected_main_sha": request["expected_main_sha"], "topology": topology,
            "dispatch_allowed": False, "publication_allowed": False,
        }
        if canonical_digest(intent) != request["local_publication_intent_sha256"]:
            _fail("PUBLICATION_CHECKOUT_PLAN_ORIGINAL_INTENT")
        checkout = topology["candidate_checkout"]
        gitdir, common = Path(checkout["gitdir"]["path"]), Path(checkout["common"]["path"])
        if Path(checkout["root"]["path"]) != Path(request["cwd"]):
            _fail("PUBLICATION_CHECKOUT_PLAN_ROOT")
        peer = topology["main_checkout"]
        transitions = [{"role": "candidate_head", "before": checkout["head"],
                        "after_hex": b"ref: refs/heads/main\n".hex()}]
        if peer is not None:
            if peer["root"] == checkout["root"]:
                _fail("PUBLICATION_CHECKOUT_PLAN_ALREADY_MAIN")
            transitions.insert(0, {
                "role": "peer_head", "before": peer["head"],
                "after_hex": (request["expected_main_sha"] + "\n").encode().hex(),
            })
        if (plan["head_transitions"] != transitions
                or plan["peer_handoff_required"] is not (peer is not None)
                or plan["absent_paths"] != _publication_plan_absences(topology)
                or plan["merge_argv_tail"] != [
                    "merge", "--ff-only", "--no-edit", topology["candidate_branch"],
                ] or plan["orig_head_after_hex"] != (
                    request["expected_main_sha"] + "\n"
                ).encode().hex()):
            _fail("PUBLICATION_CHECKOUT_PLAN_EFFECTS")
        _publication_plan_metadata(plan["orig_head"], gitdir / "ORIG_HEAD", contents=True)
        if plan["orig_head"]["identity"] is not None and re.fullmatch(
            rb"[a-f0-9]{40}\n", bytes.fromhex(plan["orig_head"]["bytes_hex"]),
        ) is None:
            _fail("PUBLICATION_CHECKOUT_PLAN_ORIG_HEAD")
        if set(plan["reflogs"]) != {"candidate", "main"}:
            _fail("PUBLICATION_CHECKOUT_PLAN_REFLOGS")
        _publication_plan_metadata(plan["reflogs"]["candidate"], gitdir / "logs/HEAD")
        _publication_plan_metadata(plan["reflogs"]["main"], common / "logs/refs/heads/main")
    except WorkflowContractError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        _fail("PUBLICATION_CHECKOUT_PLAN_FIELDS", str(exc))


def classify_publication_prepared_reference_update(
    plan: Mapping[str, Any], request: Mapping[str, Any], updates: bytes,
) -> str:
    """Check Git's locked prepared values, not execution permission or hook identity.

    These are the three exact sets emitted by the supported ordinary ff-only
    merge. In particular, HEAD alone or a main update from another old value
    must never be admitted merely because the new commit equals the candidate.
    """
    validate_local_publication_checkout_plan(plan, request)
    main, candidate = request["expected_main_sha"], request["candidate_sha"]
    zero = "0" * 40
    expected = {
        "ORIG_HEAD": frozenset({f"{zero} {main} ORIG_HEAD"}),
        "FAST_FORWARD": frozenset({f"{main} {candidate} HEAD",
                                    f"{main} {candidate} refs/heads/main"}),
        "AUTO_MERGE": frozenset({f"{zero} {zero} AUTO_MERGE"}),
    }
    # Two SHA1 updates with the longest fixed ref fit this bound. It is a
    # protocol bound, not a caller-selected buffer or an unrestricted ref list.
    if (not isinstance(updates, bytes) or not updates or len(updates) > 256
            or not updates.endswith(b"\n")):
        _fail("PUBLICATION_REFERENCE_UPDATE_FORMAT")
    try:
        lines = updates[:-1].decode("ascii").split("\n")
    except UnicodeDecodeError as exc:
        _fail("PUBLICATION_REFERENCE_UPDATE_FORMAT", str(exc))
    if len(lines) not in {1, 2} or len(set(lines)) != len(lines):
        _fail("PUBLICATION_REFERENCE_UPDATE_FORMAT")
    for kind, allowed in expected.items():
        if frozenset(lines) == allowed:
            return kind
    _fail("PUBLICATION_REFERENCE_UPDATE_DENIED")


def define_local_publication_hook_capsule(
    request: Mapping[str, Any], plan: Mapping[str, Any], *, actor: str, policy_path: Path,
) -> dict[str, Any]:
    """Define fixed hook bytes without creating files or granting execution rights.

    The enclosing original Full/Job lifecycle must supply the actor and policy,
    persist this definition before creation, and retain native file custody.
    A definition, including a valid digest, is never a callable hook capability.
    """
    validate_local_publication_checkout_plan(plan, request)
    try:
        if (request["schema_version"] != "workflow_execution_request.v5"
                or request["execution_kind"] != "CONTROLLED_LOCAL_PUBLICATION"
                or not isinstance(actor, str)
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", actor) is None
                or not isinstance(request["argv"], list) or not request["argv"]):
            _fail("PUBLICATION_HOOK_DEFINITION_CONTEXT")

        def absolute_path(value: Any) -> Path:
            if (not isinstance(value, str) or not value
                    or any(ord(char) < 32 or ord(char) == 127 for char in value)):
                _fail("PUBLICATION_HOOK_DEFINITION_PATH")
            path = Path(value)
            if not path.is_absolute() or ".." in path.parts:
                _fail("PUBLICATION_HOOK_DEFINITION_PATH")
            return path

        root = absolute_path(request["cwd"])
        python = absolute_path(request["argv"][0])
        stdout = absolute_path(request["stdout_path"])
        transaction = absolute_path(request["publication_transaction_path"])
        policy = absolute_path(policy_path.as_posix())
        if not stdout.is_relative_to(root) or not transaction.is_relative_to(root):
            _fail("PUBLICATION_HOOK_DEFINITION_ROOT")
        # Reuse the original attempt's output namespace; no independent temp root.
        output_parent = portable_path(stdout.parent.relative_to(root).as_posix())
        request_sha = canonical_digest(request)
        directory = portable_path(f"{output_parent}/publication-{request_sha}.hooks")
        entrypoint = root / "scripts/architecture_arch005_publication_fence.py"
        prefix = [python.as_posix(), "-B", entrypoint.as_posix(),
                  "--repository", root.as_posix(), "--policy", policy.as_posix(),
                  "local-publication-hook", "--transaction", transaction.as_posix(),
                  "--actor", actor, "--request-sha", request_sha]
        files = []
        for kind in ("reference-transaction", "post-merge"):
            argv = [*prefix, "--kind", kind, "--stage"]
            # Only Git's one stage argument is expanded; stdin passes through to
            # the original CLI. Fail on an unexpected argument count, not "$*".
            raw = ("#!/bin/sh\n[ \"$#\" -eq 1 ] || exit 97\nexec "
                   + shlex.join(argv) + ' "$1"\n').encode("utf-8")
            files.append({"kind": kind, "path": f"{directory}/{kind}", "argv": argv,
                          "bytes_hex": raw.hex(), "size_bytes": len(raw),
                          "sha256": hashlib.sha256(raw).hexdigest()})
        value = {
            "schema_version": "workflow_publication_hook_definition.v1",
            "request_sha256": request_sha, "checkout_plan_sha256": plan["plan_sha256"],
            "root": root.as_posix(), "actor": actor, "policy_path": policy.as_posix(),
            "python_path": python.as_posix(), "entrypoint_path": entrypoint.as_posix(),
            "directory": directory, "files": files,
            "dispatch_allowed": False, "publication_allowed": False, "resume_allowed": False,
        }
        return {**value, "definition_sha256": canonical_digest(value)}
    except WorkflowContractError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        _fail("PUBLICATION_HOOK_DEFINITION_FIELDS", str(exc))


def validate_local_publication_hook_capsule_definition(
    definition: Mapping[str, Any], request: Mapping[str, Any], plan: Mapping[str, Any], *,
    actor: str, policy_path: Path,
) -> None:
    """Re-derive all bytes from original inputs; a rehashed substitute is rejected."""
    expected = define_local_publication_hook_capsule(request, plan, actor=actor,
                                                   policy_path=policy_path)
    if (not isinstance(definition, Mapping) or definition != expected
            or canonical_digest(definition) != canonical_digest(expected)):
        _fail("PUBLICATION_HOOK_DEFINITION_CHANGED")


def prepare_local_publication_checkout_plan(
    root: Path, request: Mapping[str, Any],
) -> dict[str, Any]:
    """Capture the original checkout and Git auxiliary state without changing any file."""
    topology = inspect_local_publication_topology(
        root, candidate=request["candidate_sha"], expected_main=request["expected_main_sha"],
    )
    checkout, peer = topology["candidate_checkout"], topology["main_checkout"]
    gitdir, common = Path(checkout["gitdir"]["path"]), Path(checkout["common"]["path"])
    absences = _publication_plan_absences(topology)
    _require_publication_plan_absences(absences)
    transitions = [{"role": "candidate_head", "before": checkout["head"],
                    "after_hex": b"ref: refs/heads/main\n".hex()}]
    if peer is not None:
        transitions.insert(0, {"role": "peer_head", "before": peer["head"],
                               "after_hex": (request["expected_main_sha"] + "\n").encode().hex()})
    value = {
        "schema_version": "workflow_publication_checkout_plan.v1",
        "request_sha256": canonical_digest(request), "topology": topology,
        "head_transitions": transitions, "absent_paths": absences,
        "orig_head": _local_publication_metadata(gitdir / "ORIG_HEAD", contents=True),
        "orig_head_after_hex": (request["expected_main_sha"] + "\n").encode().hex(),
        "reflogs": {
            "candidate": _local_publication_metadata(gitdir / "logs/HEAD"),
            "main": _local_publication_metadata(common / "logs/refs/heads/main"),
        },
        "merge_argv_tail": ["merge", "--ff-only", "--no-edit", topology["candidate_branch"]],
        "peer_handoff_required": peer is not None, "dispatch_allowed": False,
        "publication_allowed": False, "resume_allowed": False,
    }
    plan = {**value, "plan_sha256": canonical_digest(value)}
    validate_local_publication_checkout_plan(plan, request)
    for record in [value["orig_head"], *value["reflogs"].values()]:
        if _local_publication_metadata(
            Path(record["path"]), contents=record is value["orig_head"],
        ) != record:
            _fail("PUBLICATION_CHECKOUT_PLAN_CAPTURE_CHANGED")
    _require_publication_plan_absences(absences)
    if inspect_local_publication_topology(
        root, candidate=request["candidate_sha"], expected_main=request["expected_main_sha"],
    ) != topology:
        _fail("PUBLICATION_CHECKOUT_PLAN_CAPTURE_CHANGED")
    return plan


def _prepare_source_installation(
    root: Path,
    task_id: str,
    *,
    transaction_path: Path,
    actor: str,
    request_id: str,
    plan_version: str = "controlled_source_installation_plan.v2",
) -> dict[str, Any]:
    """Bind the actual adopted source to exact recoverable checkout mutations.

    This performs no installation and creates no file. Its complete bytes must
    be bound in the original lease's installation reservation before persistence
    or dispatch. Recovery uses that original intent, never a newly prepared plan.
    """
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root = root.absolute()
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,80}", request_id) is None:
        _fail("SOURCE_REQUEST_ID")
    repository_identity(root)
    fence = IntegrationPublicationFence(project_root=root)
    fence.validate(transaction_path, exact_phase="CANDIDATE_COMMIT_PRE", task_id=task_id)
    transaction = fence.replay(transaction_path).transaction
    if transaction["actor"] != actor:
        _fail("SOURCE_TRANSACTION_IDENTITY")
    lease = next(
        item
        for item in fence.guard.store.replay().active_leases
        if item.lease_id == transaction["lease_id"]
    )
    execution = lease.execution
    if (
        execution is None
        or execution["request"]["schema_version"] != "workflow_execution_request.v3"
        or execution["request"]["request_id"] != request_id
        or execution["request"]["cwd"] != root.as_posix()
        or execution["request"]["source_transaction_sha256"] != transaction["transaction_sha256"]
        or execution["state"] != "RESULT_RECORDED"
        or execution["result"]["status"] != "PASS"
        or execution.get("installation_attempts")
    ):
        _fail("INSTALLATION_ADOPTED_SOURCE_REQUIRED")
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    manifest_bytes = bounded_regular_bytes(run / "request.json")
    if hashlib.sha256(manifest_bytes).hexdigest() != execution["request"]["source_request_sha256"]:
        _fail("SOURCE_REQUEST_BINDING")
    manifest = json.loads(manifest_bytes)
    prepared = manifest["prepared"]
    if (
        manifest["actor"] != actor
        or manifest["request_id"] != request_id
        or prepared["task_id"] != task_id
        or prepared
        != prepare_source_generation(
            root,
            task_id,
            transaction_path=transaction_path,
            actor=actor,
            phase="CANDIDATE_COMMIT_PRE",
        )
    ):
        _fail("SOURCE_REQUEST_INPUTS_CHANGED")
    return _capture_source_installation_plan(
        root, manifest, execution, transaction, actor, plan_version=plan_version
    )


def _capture_source_installation_plan(
    root: Path,
    manifest: Mapping[str, Any],
    execution: Mapping[str, Any],
    transaction: Mapping[str, Any],
    actor: str,
    *,
    plan_version: str = "controlled_source_installation_plan.v2",
) -> dict[str, Any]:
    """Pure original-plan calculation; callers supply separately checked custody.

    Recovery may use this calculation only when its complete canonical bytes
    match the already reserved plan digest. It does not grant execution rights.
    """
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    prepared = manifest["prepared"]
    task_id = prepared["task_id"]
    request_id = execution["request"]["request_id"]
    run = root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    repository = repository_identity(root)
    result = read_bound_json(
        root,
        {
            "path": Path(execution["result"]["artifact"]["path"]).relative_to(root).as_posix(),
            "sha256": execution["result"]["artifact"]["sha256"],
        },
    )
    if any(
        result.get(key) != value
        for key, value in coordination._result_binding(execution["request"]).items()
    ):
        _fail("SOURCE_WORKER_RESULT")
    snapshot = result["snapshot"]
    if snapshot["parents"] != [prepared["main"], prepared["lane"]] or _git(
        root, "cat-file", "commit", snapshot["commit"]
    ) != _source_commit_bytes(snapshot):
        _fail("SOURCE_WORKER_RESULT_COMMIT")
    delta = _read_source_generation(run, result["generation_sha256"])
    excluded = _exclusions(root)
    declared = (*transaction["owned_paths"], *transaction["shared_paths"])

    def capture_file(base: Path, name: str, after: bytes | None) -> dict[str, Any]:
        name = portable_path(name)
        path = base / name
        for parent in (path, *path.parents):
            if parent == base:
                break
            try:
                observed = parent.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(observed.st_mode) or getattr(observed, "st_file_attributes", 0) & 0x400:
                _fail("INSTALLATION_REPARSE", name)
        try:
            identity = path.lstat()
        except FileNotFoundError:
            before, file_id = None, None
        else:
            before = bounded_regular_bytes(path)
            current = path.lstat()
            if (identity.st_dev, identity.st_ino, identity.st_size, identity.st_mtime_ns) != (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
            ) or current.st_nlink != 1:
                _fail("INSTALLATION_INPUT_CHANGED", name)
            file_id = [current.st_dev, current.st_ino]
        return {
            "path": name,
            "identity": file_id,
            "before_hex": None if before is None else before.hex(),
            "after_hex": None if after is None else after.hex(),
        }

    files = []
    for position, operation in enumerate(delta["operations"]):
        name = portable_path(operation["path"])
        if any(name == item or name.startswith(item + "/") for item in excluded) or not any(
            name == item or name.startswith(item + "/") for item in declared
        ):
            _fail("INSTALLATION_PATH_NOT_AUTHORIZED", name)
        after = operation["after"]
        content = None
        if after["exists"]:
            if after["mode"] not in {"100644", "100755"} or after["type"] != "blob":
                _fail("INSTALLATION_OBJECT_TYPE", name)
            content = bounded_regular_bytes(run / "capture" / f"{position:06d}.bin")
            if (
                hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
                != after["oid"]
            ):
                _fail("SOURCE_CAPTURE_CHANGED", name)
        row = capture_file(root, name, content)
        before = None if row["before_hex"] is None else bytes.fromhex(row["before_hex"])
        expected = prepared["expected_inputs"].get(name, _state("000000", _ZERO))
        if expected["exists"] != (before is not None) or (
            before is not None
            and hashlib.sha1(b"blob " + str(len(before)).encode() + b"\0" + before).hexdigest()
            != expected["oid"]
        ):
            _fail("INSTALLATION_INPUT_CHANGED", name)
        if row["before_hex"] != row["after_hex"]:
            files.append(row)
    branch = _git(root, "symbolic-ref", "-q", "HEAD").decode().strip()
    if not branch.startswith("refs/heads/") or branch.casefold() == "refs/heads/main":
        _fail("INSTALLATION_SOURCE_BRANCH_REQUIRED")
    portable_path(branch)
    gitdir = Path(_git(root, "rev-parse", "--absolute-git-dir").decode().strip())
    common = Path(repository["common"])
    index = capture_file(gitdir, "index", _source_index_bytes(root, snapshot["commit"]))
    if (
        index["before_hex"] is None
        or hashlib.sha256(bytes.fromhex(index["before_hex"])).hexdigest()
        != prepared["index_sha256"]
    ):
        _fail("INSTALLATION_INDEX_CHANGED")
    head = capture_file(gitdir, "HEAD", ("ref: " + branch + "\n").encode())
    if head["before_hex"] != head["after_hex"]:
        _fail("INSTALLATION_HEAD_CHANGED")
    ref = capture_file(common, branch, (snapshot["commit"] + "\n").encode())
    if ref["before_hex"] is not None and (
        bytes.fromhex(ref["before_hex"]).strip() != prepared["main"].encode()
    ):
        _fail("INSTALLATION_REF_CHANGED")
    if _git(root, "rev-parse", "HEAD", "refs/heads/main").decode().splitlines() != [
        prepared["main"],
        prepared["main"],
    ]:
        _fail("INSTALLATION_REF_CHANGED")
    plan = {
        "schema_version": plan_version,
        "task_id": task_id,
        "actor": actor,
        "lease_id": execution["request"]["lease_id"],
        "source_request_id": request_id,
        "source_request_sha256": execution["request"]["source_request_sha256"],
        "source_transaction_sha256": transaction["transaction_sha256"],
        "source_result_sha256": execution["result"]["artifact"]["sha256"],
        "root_identity": coordination.directory_identity(root),
        "gitdir_identity": coordination.directory_identity(gitdir),
        "common_identity": coordination.directory_identity(common),
        "main": prepared["main"],
        "candidate": snapshot["commit"],
        "tree": snapshot["tree"],
        "branch": branch,
        "files": files,
        "index": index,
        "head": head,
        "ref": ref,
        "expected_inputs": prepared["expected_inputs"],
    }
    if plan_version == "controlled_source_installation_plan.v2":
        directories = []
        for key, name in _installation_directory_names(plan):
            base = Path(plan[key]["path"])
            try:
                info = coordination.directory_identity(base / name)
            except FileNotFoundError:
                identity = None
            else:
                identity = [info["device"], info["file_id"]]
            directories.append({"root_key": key, "path": name, "identity": identity})
        plan["directories"] = directories
    elif plan_version != "controlled_source_installation_plan.v1":
        _fail("INSTALLATION_PLAN_VERSION")
    return plan


def _installation_directory_names(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Exact parents needed by the original targets and Git interoperability locks."""
    names: dict[str, tuple[str, str]] = {}
    for key, targets in (
        ("root_identity", [row["path"] for row in plan["files"]]),
        ("gitdir_identity", ["HEAD", "index", "HEAD.lock", "index.lock"]),
        (
            "common_identity",
            [plan["branch"], plan["branch"] + ".lock", "refs/heads/main.lock", "packed-refs.lock"],
        ),
    ):
        for target in targets:
            parts = portable_path(target).split("/")
            for position in range(1, len(parts)):
                name = "/".join(parts[:position])
                absolute = (Path(plan[key]["path"]) / name).as_posix().casefold()
                item = (key, name)
                if absolute in names and names[absolute] != item:
                    _fail("INSTALLATION_DIRECTORY_ALIAS", name)
                names[absolute] = item
    return sorted(names.values(), key=lambda item: (item[1].count("/"), item))


def _installation_directory_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    version = plan["schema_version"]
    if version == "controlled_source_installation_plan.v1":
        if "directories" in plan:
            _fail("INSTALLATION_DIRECTORY_LEGACY")
        return []
    if version != "controlled_source_installation_plan.v2":
        _fail("INSTALLATION_PLAN_VERSION")
    rows = plan.get("directories")
    if (
        not isinstance(rows, list)
        or any(
            not isinstance(row, dict) or set(row) != {"root_key", "path", "identity"}
            for row in rows
        )
        or [(row["root_key"], row["path"]) for row in rows] != _installation_directory_names(plan)
    ):
        _fail("INSTALLATION_DIRECTORY_SET")
    for row in rows:
        identity = row["identity"]
        if identity is not None and (
            not isinstance(identity, list)
            or len(identity) != 2
            or any(type(item) is not int or item < 0 for item in identity)
            or identity[1] == 0
            or identity[0] != plan[row["root_key"]]["device"]
        ):
            _fail("INSTALLATION_DIRECTORY_IDENTITY", row["path"])
    return rows


def _installation_directory_identity(
    plan: Mapping[str, Any],
    source: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    initial: bool = False,
) -> tuple[int, int] | None:
    root = plan[row["root_key"]]
    identity = row["identity"]
    if not initial:
        for attempt in source.get("installation_attempts", []):
            for created in attempt.get("created_objects", []):
                if created.get("kind") != "directory" or (
                    created["root"].casefold(),
                    created["path"].casefold(),
                ) != (root["path"].casefold(), row["path"].casefold()):
                    continue
                if (
                    row["identity"] is not None
                    or created["root_identity"] != [root["device"], root["file_id"]]
                    or created["plan_sha256"] != attempt["request"]["installation_plan_sha256"]
                ):
                    _fail("INSTALLATION_DIRECTORY_CREATION_BINDING", row["path"])
                identity = created["file_identity"]
    return None if identity is None else tuple(identity)


def _installation_parent_identities(
    plan: Mapping[str, Any],
    source: Mapping[str, Any],
    base: Path,
    relative: str,
    *,
    initial: bool = False,
) -> dict[str, tuple[int, int]] | None:
    if plan["schema_version"] == "controlled_source_installation_plan.v1":
        return None  # Legacy evidence grants no new directory creation permission.
    rows = _installation_directory_rows(plan)
    index = {
        (Path(plan[row["root_key"]]["path"]) / row["path"]).as_posix().casefold(): row
        for row in rows
    }
    parts = portable_path(relative).split("/")
    result = {}
    for position in range(1, len(parts)):
        name = "/".join(parts[:position])
        row = index.get((base / name).as_posix().casefold())
        if row is None:
            _fail("INSTALLATION_DIRECTORY_UNPLANNED", name)
        identity = _installation_directory_identity(plan, source, row, initial=initial)
        if identity is None:
            _fail("INSTALLATION_DIRECTORY_IDENTITY_UNKNOWN", name)
        result[name] = identity
    return result


def _installation_directories(
    plan: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    initial: bool = False,
    restored: bool = False,
    remove: bool = False,
    allow_created: bool = False,
) -> None:
    rows = _installation_directory_rows(plan)
    for row in reversed(rows) if remove else rows:
        binding = plan[row["root_key"]]
        base = Path(binding["path"])
        path = base / row["path"]
        try:
            path.lstat()
        except FileNotFoundError:
            if row["identity"] is not None or (not initial and not restored):
                _fail("INSTALLATION_DIRECTORY_MISSING", row["path"])
            continue
        if initial and row["identity"] is None:
            _fail("INSTALLATION_DIRECTORY_UNEXPECTED", row["path"])
        if restored and not remove and not allow_created and row["identity"] is None:
            _fail("INSTALLATION_DIRECTORY_NOT_RESTORED", row["path"])
        identity = _installation_directory_identity(plan, source, row, initial=initial)
        if identity is None:
            _fail("INSTALLATION_DIRECTORY_IDENTITY_UNKNOWN", row["path"])
        root_identity = (binding["device"], binding["file_id"])
        parent_identities = _installation_parent_identities(
            plan, source, base, row["path"], initial=initial
        )
        if parent_identities is None:
            _fail("INSTALLATION_DIRECTORY_IDENTITY_UNKNOWN", row["path"])
        if remove and row["identity"] is None:
            remove_bound_empty_directory(
                base,
                row["path"],
                expected_identity=identity,
                expected_root_identity=root_identity,
                expected_parent_identities=parent_identities,
            )
        else:
            verify_bound_directory(
                base,
                row["path"],
                expected_identity=identity,
                expected_root_identity=root_identity,
                expected_parent_identities=parent_identities,
            )


def _installation_plan_bytes(run: Path, expected_sha256: str) -> bytes:
    for name in ("installation_plan.json", "installation_plan.recovered.json"):
        path = run / name
        try:
            content = bounded_regular_bytes(path, budget=64 * 1024 * 1024)
        except FileNotFoundError:
            continue
        if hashlib.sha256(content).hexdigest() == expected_sha256:
            return content
    _fail("INSTALLATION_PLAN_CHANGED_OR_MISSING")


def _recover_unstarted_installation_plan(
    root: Path,
    *,
    transaction_path: Path,
    actor: str,
) -> None:
    """Reconstitute only the exact reserved plan of a provably unstarted Job.

    No TTL revival or new before-state is possible: recomputed bytes must match
    the original external digest. The incomplete original file stays untouched.
    """
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture import workflow_execution as execution_backend
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    fence = IntegrationPublicationFence(project_root=root)
    replay = fence.replay(transaction_path)
    if replay.status != "PASS" or replay.phase != "CANDIDATE_COMMIT_PRE":
        _fail("INSTALLATION_TRANSACTION_CHANGED")
    physical = next(
        row
        for row in fence.guard.store.replay().active_leases
        if row.lease_id == replay.transaction["lease_id"]
    )
    source = physical.execution
    if physical.actor != actor or replay.transaction["actor"] != actor or source is None:
        _fail("SOURCE_TRANSACTION_IDENTITY")
    attempts = source.get("installation_attempts", [])
    if not attempts:
        _fail("INSTALLATION_MISSING")
    first = attempts[0]
    request = first["request"]
    # Every prior attempt must also have stopped before Job creation. Once a
    # bound executor exists, reconstruction from the current checkout is denied.
    if any(
        attempt["process"] is not None
        or attempt["state"] != "RESULT_RECORDED"
        or attempt["result"]["status"] == "PASS"
        or attempt["exit"]["basis"] != "DEAD_LAUNCHER_JOB_EMPTY"
        or execution_backend.observe_process(**attempt["launcher"])["state"]
        not in {"EXITED", "REUSED"}
        or execution_backend.observe_job(attempt["request"]["job_name"])["state"]
        not in {"ABSENT", "EMPTY"}
        for attempt in attempts
    ):
        _fail("INSTALLATION_PLAN_RECONSTRUCTION_NOT_UNSTARTED")
    if (
        source["request"]["cwd"] != root.as_posix()
        or source["request"]["source_transaction_sha256"]
        != replay.transaction["transaction_sha256"]
        or source["state"] != "RESULT_RECORDED"
        or source["result"]["status"] != "PASS"
    ):
        _fail("INSTALLATION_SOURCE_BINDING")
    run = (
        root
        / "outputs/architecture/workflow_integration/source_candidates"
        / source["request"]["request_id"]
    )
    manifest = read_bound_json(
        root,
        {
            "path": (run / "request.json").relative_to(root).as_posix(),
            "sha256": source["request"]["source_request_sha256"],
        },
    )
    if (
        manifest["actor"] != actor
        or manifest["prepared"]["task_id"] != replay.transaction["task_id"]
    ):
        _fail("SOURCE_TRANSACTION_IDENTITY")
    plan = _capture_source_installation_plan(root, manifest, source, replay.transaction, actor)
    content = safe._json_bytes(plan)
    if hashlib.sha256(content).hexdigest() != request["installation_plan_sha256"]:
        # Old reserved bytes retain their old schema and no directory permission.
        plan = _capture_source_installation_plan(
            root,
            manifest,
            source,
            replay.transaction,
            actor,
            plan_version="controlled_source_installation_plan.v1",
        )
        content = safe._json_bytes(plan)
    if hashlib.sha256(content).hexdigest() != request["installation_plan_sha256"]:
        _fail("INSTALLATION_ORIGINAL_PLAN_NOT_REPRODUCED")
    destination = run / "installation_plan.recovered.json"
    root_identity = (plan["root_identity"]["device"], plan["root_identity"]["file_id"])
    _complete_recoverable_evidence(
        root,
        destination,
        content,
        root_identity=root_identity,
        error_prefix="INSTALLATION_RECOVERY_PLAN",
    )


def _complete_recoverable_evidence(
    root: Path,
    destination: Path,
    content: bytes,
    *,
    root_identity: tuple[int, int],
    error_prefix: str,
) -> None:
    """Complete reproducible owned evidence with a fixed two-level preservation.

    This helper grants no authority: the caller must bind its deterministic
    bytes to external facts and hold the appropriate original writer boundary.
    It is not a recovery rule for arbitrary new checkout files.
    """
    if destination.exists():
        info = destination.lstat()
        partial = bounded_regular_bytes(destination, budget=64 * 1024 * 1024)
        if partial == content:
            return
        if not content.startswith(partial):
            _fail(error_prefix + "_UNKNOWN_BYTES")
        retained = destination.with_name(
            destination.stem + ".partial-" + hashlib.sha256(partial).hexdigest() + ".json"
        )
        if not retained.exists():
            write_bound_once(
                root,
                retained.relative_to(root).as_posix(),
                partial,
                expected_root_identity=root_identity,
            )
        retained_info = retained.lstat()
        retained_bytes = bounded_regular_bytes(retained, budget=64 * 1024 * 1024)
        if retained_bytes != partial:
            if (
                not partial.startswith(retained_bytes)
                or bounded_regular_bytes(destination, budget=64 * 1024 * 1024) != partial
            ):
                _fail(error_prefix + "_RETENTION_CHANGED")
            # The full original partial still exists. An interrupted retention
            # copy is repairable without another recursive backup-copy chain.
            apply_bound_file(
                root,
                retained.relative_to(root).as_posix(),
                retained_bytes,
                partial,
                expected_identity=(retained_info.st_dev, retained_info.st_ino),
                expected_root_identity=root_identity,
            )
        if bounded_regular_bytes(retained, budget=64 * 1024 * 1024) != partial:
            _fail(error_prefix + "_RETENTION_CHANGED")
        apply_bound_file(
            root,
            destination.relative_to(root).as_posix(),
            partial,
            content,
            expected_identity=(info.st_dev, info.st_ino),
            expected_root_identity=root_identity,
        )
    else:
        write_bound_once(
            root,
            destination.relative_to(root).as_posix(),
            content,
            expected_root_identity=root_identity,
        )


def _installation_context(
    root: Path, request: Mapping[str, Any], source: Mapping[str, Any], actor: str
) -> tuple[dict[str, Any], list[tuple[Path, tuple[int, int], dict[str, Any]]]]:
    """Validate the original plan against independent, adopted source custody.

    This works in a partial checkout: no new source preparation or current index
    parser is consulted. A self-hashed arbitrary plan is not installation proof.
    """
    from ai_trading_system.platform.architecture import workflow_coordination as coordination

    source_request = source["request"]
    run = (
        root
        / "outputs/architecture/workflow_integration/source_candidates"
        / source_request["request_id"]
    )
    if (
        source_request["schema_version"] != "workflow_execution_request.v3"
        or source["state"] != "RESULT_RECORDED"
        or source["result"]["status"] != "PASS"
        or request["cwd"] != root.as_posix()
        or request["source_result_sha256"] != source["result"]["artifact"]["sha256"]
    ):
        _fail("INSTALLATION_ADOPTED_SOURCE_REQUIRED")
    # The first plan is immutable across all recovery attempts. Its location is
    # derived from original custody, not a path supplied inside the plan.
    content = _installation_plan_bytes(run, request["installation_plan_sha256"])
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    plan = load_strict_json_text(content.decode())
    if not isinstance(plan, dict):
        _fail("INSTALLATION_PLAN_SHAPE")
    manifest = read_bound_json(
        root,
        {
            "path": (run / "request.json").relative_to(root).as_posix(),
            "sha256": source_request["source_request_sha256"],
        },
    )
    prepared = manifest["prepared"]
    result = read_bound_json(
        root,
        {
            "path": Path(source["result"]["artifact"]["path"]).relative_to(root).as_posix(),
            "sha256": source["result"]["artifact"]["sha256"],
        },
    )
    snapshot = result["snapshot"]
    if (
        any(
            plan.get(key) != expected
            for key, expected in {
                "actor": actor,
                "task_id": prepared["task_id"],
                "lease_id": request["lease_id"],
                "source_request_id": source_request["request_id"],
                "source_request_sha256": source_request["source_request_sha256"],
                "source_transaction_sha256": source_request["source_transaction_sha256"],
                "source_result_sha256": source["result"]["artifact"]["sha256"],
                "candidate": snapshot["commit"],
                "tree": snapshot["tree"],
                "main": prepared["main"],
                "expected_inputs": prepared["expected_inputs"],
            }.items()
        )
        or request["installed_candidate_sha"] != snapshot["commit"]
    ):
        _fail("INSTALLATION_SOURCE_BINDING")
    roots = {}
    _installation_directory_rows(plan)
    for key in ("root_identity", "gitdir_identity", "common_identity"):
        identity = plan[key]
        base = Path(identity["path"])
        if coordination.directory_identity(base) != identity:
            _fail("INSTALLATION_ROOT_CHANGED", key)
        roots[key] = (base, (identity["device"], identity["file_id"]))
    if roots["root_identity"][0] != root or (
        roots["common_identity"][0].as_posix() != prepared["repository"]["common"]
    ):
        _fail("INSTALLATION_ROOT_CHANGED")
    # Resolving the Git administrative directories does not parse index bytes.
    if roots["gitdir_identity"][0] != Path(
        _git(root, "rev-parse", "--absolute-git-dir").decode().strip()
    ) or roots["common_identity"][0] != Path(
        _git(root, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip()
    ):
        _fail("INSTALLATION_ROOT_CHANGED")
    branch = portable_path(plan["branch"])
    if not branch.startswith("refs/heads/") or branch.casefold() == "refs/heads/main":
        _fail("INSTALLATION_SOURCE_BRANCH_REQUIRED")
    if (
        plan["index"]["path"] != "index"
        or plan["head"]["path"] != "HEAD"
        or plan["ref"]["path"] != branch
        or plan["head"]["before_hex"] != ("ref: " + branch + "\n").encode().hex()
        or plan["head"]["after_hex"] != plan["head"]["before_hex"]
        or plan["ref"]["after_hex"] != (snapshot["commit"] + "\n").encode().hex()
        or plan["ref"]["before_hex"] not in {None, (prepared["main"] + "\n").encode().hex()}
        or hashlib.sha256(bytes.fromhex(plan["index"]["before_hex"])).hexdigest()
        != prepared["index_sha256"]
        or bytes.fromhex(plan["index"]["after_hex"])
        != _source_index_bytes(root, snapshot["commit"])
    ):
        _fail("INSTALLATION_ADMIN_PLAN")
    delta = _read_source_generation(run, result["generation_sha256"])
    expected_rows = {}
    for row in delta["operations"]:
        before = prepared["expected_inputs"].get(row["path"], _state("000000", _ZERO))
        if (before["exists"], before["oid"]) != (row["after"]["exists"], row["after"]["oid"]):
            expected_rows[row["path"]] = (before, row["after"])
    if len(plan["files"]) != len(expected_rows) or {row["path"] for row in plan["files"]} != set(
        expected_rows
    ):
        _fail("INSTALLATION_FILE_SET")
    for row in plan["files"]:
        portable_path(row["path"])
        for field, expected in zip(
            ("before_hex", "after_hex"), expected_rows[row["path"]], strict=True
        ):
            raw = None if row[field] is None else bytes.fromhex(row[field])
            if expected["exists"] != (raw is not None) or (
                raw is not None
                and hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
                != expected["oid"]
            ):
                _fail("INSTALLATION_FILE_BYTES", row["path"])
    targets = [(*roots["root_identity"], row) for row in plan["files"]]
    targets += [
        (*roots["gitdir_identity"], plan["index"]),
        (*roots["common_identity"], plan["ref"]),
    ]
    return plan, targets


def _installation_object_identity(
    source: Mapping[str, Any],
    base: Path,
    root_identity: tuple[int, int],
    row: Mapping[str, Any],
    *,
    initial: bool = False,
) -> tuple[int, int] | None:
    """Resolve original custody, not identity inferred from current bytes."""
    identity = row["identity"]
    if not initial:
        for attempt in source.get("installation_attempts", []):
            for created in attempt.get("created_objects", []):
                if created.get("kind") == "directory":
                    continue
                if (created["root"].casefold(), created["path"].casefold()) != (
                    base.as_posix().casefold(),
                    row["path"].casefold(),
                ):
                    continue
                creation_hex = row[
                    "after_hex"
                    if attempt["request"]["installation_action"] == "INSTALL"
                    else "before_hex"
                ]
                if (
                    created["root_identity"] != list(root_identity)
                    or created["plan_sha256"] != attempt["request"]["installation_plan_sha256"]
                    or creation_hex is None
                    or created["target_sha256"]
                    != hashlib.sha256(bytes.fromhex(creation_hex)).hexdigest()
                ):
                    _fail("INSTALLATION_CREATION_BINDING", row["path"])
                identity = created["file_identity"]
    return None if identity is None else tuple(identity)


def _verify_source_installation(
    root: Path,
    request: Mapping[str, Any],
    source: Mapping[str, Any],
    actor: str,
    *,
    initial: bool = False,
    allow_recovery_directories: bool = False,
) -> dict[str, Any]:
    """Independent actual Git/raw-file verification, never a worker flag."""
    plan, targets = _installation_context(root, request, source, actor)
    restored = initial or request["installation_action"] == "RECOVER"
    field = "before_hex" if restored else "after_hex"
    expected_head = plan["main"] if restored else plan["candidate"]
    _installation_directories(
        plan, source, initial=initial, restored=restored, allow_created=allow_recovery_directories
    )
    for base, root_identity, row in targets:
        path = base / row["path"]
        expected = None if row[field] is None else bytes.fromhex(row[field])
        if expected is None:
            if path.exists() or path.is_symlink():
                _fail("INSTALLATION_STABLE_FILE", row["path"])
        else:
            identity = _installation_object_identity(
                source, base, root_identity, row, initial=initial
            )
            if identity is None:
                _fail("INSTALLATION_RECOVERY_IDENTITY_UNKNOWN", row["path"])
            if bounded_regular_bytes(path, expected_identity=identity) != expected:
                _fail("INSTALLATION_STABLE_FILE", row["path"])
    if _git(root, "symbolic-ref", "-q", "HEAD").decode().strip() != plan["branch"] or _git(
        root, "rev-parse", "HEAD", "refs/heads/main"
    ).decode().splitlines() != [expected_head, plan["main"]]:
        _fail("INSTALLATION_STABLE_REFS")
    overrides = {row["path"]: row[field] for row in plan["files"]}
    for name, expected in plan["expected_inputs"].items():
        if name in overrides:
            continue  # Exact bytes/absence already independently checked above.
        path = root / name
        if not expected["exists"]:
            if path.exists() or path.is_symlink():
                _fail("INSTALLATION_STABLE_INPUT", name)
            continue
        raw = bounded_regular_bytes(path)
        if (
            hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
            != expected["oid"]
        ):
            _fail("INSTALLATION_STABLE_INPUT", name)
    return {
        "stable_state": "SOURCE_RESTORED" if restored else "SOURCE_INSTALLED",
        "observed_head": expected_head,
        "observed_main": plan["main"],
        "installation_plan_sha256": request["installation_plan_sha256"],
    }


def _installation_locks(plan: Mapping[str, Any]) -> list[tuple[Path, tuple[int, int], str]]:
    branch = portable_path(plan["branch"])
    if not branch.startswith("refs/heads/") or branch.casefold() == "refs/heads/main":
        _fail("INSTALLATION_SOURCE_BRANCH_REQUIRED")
    result: list[tuple[Path, tuple[int, int], str]] = []
    for key, names in (
        ("gitdir_identity", ("HEAD.lock", "index.lock")),
        ("common_identity", (plan["branch"] + ".lock", "refs/heads/main.lock", "packed-refs.lock")),
    ):
        identity = plan[key]
        result.extend(
            (Path(identity["path"]), (identity["device"], identity["file_id"]), name)
            for name in names
        )
    # Reject Windows aliases among both administrative targets and locks before
    # creating even the first lock. Directory identity alone cannot prove that
    # two differently cased names do not address the same file.
    paths = [
        Path(plan[key]["path"]) / name
        for key, name in (
            ("gitdir_identity", "HEAD"),
            ("gitdir_identity", "index"),
            ("common_identity", branch),
            ("common_identity", "refs/heads/main"),
            ("common_identity", "packed-refs"),
        )
    ] + [base / name for base, _identity, name in result]
    normalized = [path.as_posix().casefold() for path in paths]
    if len(normalized) != len(set(normalized)):
        _fail("INSTALLATION_ADMIN_PATH_COLLISION")
    return result


def _installation_lock_bytes(request: Mapping[str, Any]) -> bytes:
    digest = request["installation_plan_sha256"]
    if not isinstance(digest, str):
        _fail("INSTALLATION_PLAN_HASH")
    return ("DEVX-015 installation " + digest + "\n").encode()


def source_installation_worker(root: Path, execution_request_path: Path) -> dict[str, Any]:
    """Apply/restore only the original plan inside its actual contained Job."""
    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.workflow_execution import (
        execution_environment_sha256,
    )
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = root.absolute()
    request = coordination._request(
        load_strict_json_text(bounded_regular_bytes(execution_request_path).decode())
    )
    if (
        request["schema_version"] != "workflow_execution_request.v4"
        or request["cwd"] != root.as_posix()
    ):
        _fail("INSTALLATION_REQUEST_BINDING")
    fence = IntegrationPublicationFence(project_root=root)
    physical = next(
        row
        for row in fence.guard.store.replay().active_leases
        if row.lease_id == request["lease_id"]
    )
    actor = physical.actor
    lifecycle = coordination.InstallationLifecycle(fence.guard.store)
    witness = lifecycle.require_installation_worker(request, actor=actor)
    if physical.execution is None:
        _fail("INSTALLATION_MISSING")

    def record_created(descriptor: int) -> None:
        lifecycle.record_created_object(request, descriptor, actor=actor)

    if execution_environment_sha256(dict(os.environ)) != request["environment_sha256"]:
        _fail("INSTALLATION_ENVIRONMENT_CHANGED")
    plan, targets = _installation_context(root, request, physical.execution, actor)
    if (
        execution_request_path.absolute()
        != Path(request["result_path"]).parent / "execution_request.json"
    ):
        _fail("INSTALLATION_REQUEST_PATH")
    source_run = (
        root
        / "outputs/architecture/workflow_integration/source_candidates"
        / plan["source_request_id"]
    )
    manifest = read_bound_json(
        root,
        {
            "path": (source_run / "request.json").relative_to(root).as_posix(),
            "sha256": plan["source_request_sha256"],
        },
    )
    transaction_path = root / manifest["transaction_path"]
    if _source_runtime_binding(root, manifest["prepared"]) != manifest["runtime"]:
        _fail("INSTALLATION_RUNTIME_CHANGED")
    transaction = fence.replay(transaction_path)
    if (
        transaction.phase != "CANDIDATE_COMMIT_PRE"
        or transaction.transaction["transaction_sha256"] != plan["source_transaction_sha256"]
        or transaction.transaction["lease_id"] != request["lease_id"]
        or transaction.transaction["actor"] != actor
    ):
        _fail("INSTALLATION_TRANSACTION_CHANGED")
    restored = request["installation_action"] == "RECOVER"
    if not restored:
        _verify_source_installation(root, request, physical.execution, actor, initial=True)
    locks = _installation_locks(plan)
    for directory_row in _installation_directory_rows(plan):
        binding = plan[directory_row["root_key"]]
        base = Path(binding["path"])
        name = directory_row["path"]
        path = base / name
        root_id = (binding["device"], binding["file_id"])
        expected = _installation_directory_identity(plan, physical.execution, directory_row)
        try:
            path.lstat()
        except FileNotFoundError:
            if directory_row["identity"] is not None:
                _fail("INSTALLATION_DIRECTORY_MISSING", name)
            if restored and not any(
                (lock_base / lock_name).is_relative_to(path) for lock_base, _, lock_name in locks
            ):
                continue
            create_bound_recoverable_directory(
                base,
                name,
                expected_root_identity=root_id,
                expected_parent_identities=_installation_parent_identities(
                    plan, physical.execution, base, name
                ),
                record_created=record_created,
            )
            physical = next(
                item
                for item in fence.guard.store.replay().active_leases
                if item.lease_id == request["lease_id"]
            )
            if physical.execution is None:
                _fail("INSTALLATION_MISSING")
        else:
            if expected is None:
                _fail("INSTALLATION_DIRECTORY_IDENTITY_UNKNOWN", name)
            if physical.execution is None:
                _fail("INSTALLATION_MISSING")
            parent_identities = _installation_parent_identities(
                plan, physical.execution, base, name
            )
            if parent_identities is None:
                _fail("INSTALLATION_DIRECTORY_IDENTITY_UNKNOWN", name)
            verify_bound_directory(
                base,
                name,
                expected_identity=expected,
                expected_root_identity=root_id,
                expected_parent_identities=parent_identities,
            )
    if physical.execution is None:
        _fail("INSTALLATION_MISSING")
    lock_bytes = _installation_lock_bytes(request)
    # Git's existing .lock protocol is interoperability with ordinary Git, not
    # another workflow lease or queue. Keep these until independent adoption.
    for base, lock_identity, name in locks:
        lifecycle.require_installation_worker(request, actor=actor)
        if (base / name).exists():
            if not restored or bounded_regular_bytes(base / name) != lock_bytes:
                _fail("INSTALLATION_GIT_LOCK_EXISTS", name)
        else:
            write_bound_once(
                base,
                name,
                lock_bytes,
                expected_root_identity=lock_identity,
                require_existing_parents=True,
                expected_parent_identities=_installation_parent_identities(
                    plan, physical.execution, base, name
                ),
            )
    journal = Path(request["result_path"]).parent
    # Worktree first, then index, then expected-old branch ref. Main never moves.
    # An interrupted sequence remains protected by the original execution lease.
    ordered = list(reversed(targets)) if restored else targets
    for position, (base, root_identity, row) in enumerate(ordered):
        lifecycle.require_installation_worker(request, actor=actor)
        path = base / row["path"]
        wanted_hex = row["before_hex" if restored else "after_hex"]
        desired = None if wanted_hex is None else bytes.fromhex(wanted_hex)
        original = None if row["before_hex"] is None else bytes.fromhex(row["before_hex"])
        planned = None if row["after_hex"] is None else bytes.fromhex(row["after_hex"])
        expected_identity = _installation_object_identity(
            physical.execution, base, root_identity, row, initial=not restored
        )
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            current, identity = None, None
        else:
            identity = (metadata.st_dev, metadata.st_ino)
            if expected_identity is None or identity != expected_identity:
                _fail(
                    "INSTALLATION_RECOVERY_IDENTITY_UNKNOWN"
                    if restored
                    else "INSTALLATION_INPUT_CHANGED",
                    row["path"],
                )
            current = bounded_regular_bytes(path, expected_identity=expected_identity)
        if current == desired:
            continue
        if not restored:
            if current != original or (identity is not None and list(identity) != row["identity"]):
                _fail("INSTALLATION_INPUT_CHANGED", row["path"])
        elif current is not None and current not in (original, planned):
            # Recognize a bounded torn write made from the two frozen versions;
            # arbitrary new content is not permission to erase someone else's work.
            old, new = original or b"", planned or b""
            if len(current) > max(len(old), len(new)) or any(
                value
                not in {
                    old[index] if index < len(old) else 0,
                    new[index] if index < len(new) else 0,
                }
                for index, value in enumerate(current)
            ):
                _fail("INSTALLATION_RECOVERY_CONTENT_UNKNOWN", row["path"])
        intent = {
            "request_id": request["request_id"],
            "plan_sha256": request["installation_plan_sha256"],
            "position": position,
            "root": base.as_posix(),
            "path": row["path"],
            "file_identity": identity,
            "observed_sha256": None if current is None else hashlib.sha256(current).hexdigest(),
            "target_sha256": None if desired is None else hashlib.sha256(desired).hexdigest(),
        }
        write_bound_once(
            root,
            (journal / f"{position:06d}.intent.json").relative_to(root).as_posix(),
            json.dumps(intent, sort_keys=True).encode(),
            expected_root_identity=(
                plan["root_identity"]["device"],
                plan["root_identity"]["file_id"],
            ),
        )
        if current is None:
            if desired is not None:
                create_bound_recoverable_file(
                    base,
                    row["path"],
                    desired,
                    expected_root_identity=root_identity,
                    expected_parent_identities=_installation_parent_identities(
                        plan, physical.execution, base, row["path"]
                    ),
                    record_created=record_created,
                )
        else:
            assert identity is not None
            apply_bound_file(
                base,
                row["path"],
                current,
                desired,
                expected_identity=identity,
                expected_root_identity=root_identity,
                expected_parent_identities=_installation_parent_identities(
                    plan, physical.execution, base, row["path"]
                ),
            )
    lifecycle.require_installation_worker(request, actor=actor)
    result = {
        **coordination._result_binding(request),
        "status": "PASS",
        "worker_process": witness["worker_process"],
        "stable_state": "SOURCE_RESTORED" if restored else "SOURCE_INSTALLED",
    }
    write_bound_once(
        root,
        Path(request["result_path"]).relative_to(root).as_posix(),
        json.dumps(result, sort_keys=True).encode(),
        expected_root_identity=(plan["root_identity"]["device"], plan["root_identity"]["file_id"]),
    )
    return result


def start_source_installation(
    root: Path,
    task_id: str,
    *,
    transaction_path: Path,
    actor: str,
    source_request_id: str,
    request_id: str,
    action: str = "INSTALL",
) -> dict[str, Any]:
    """Dispatch one explicit installation or original-plan restoration attempt.

    The original source lease owns every attempt. Same IDs only observe/replay;
    recovery uses a new ID linked to an OS-confirmed failed predecessor.
    """
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture import workflow_execution as execution
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.parallel_control_kernel import _canonical_sha256
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = root.absolute()
    if os.name != "nt" or sys.version_info[:2] != (3, 11):
        _fail("SOURCE_RUNTIME_UNSUPPORTED")
    if action not in {"INSTALL", "RECOVER"}:
        _fail("INSTALLATION_ACTION")
    if source_request_id == request_id or any(
        re.fullmatch(r"[a-z0-9][a-z0-9-]{7,80}", value) is None
        for value in (source_request_id, request_id)
    ):
        _fail("SOURCE_REQUEST_ID")
    transaction_path = (
        transaction_path if transaction_path.is_absolute() else root / transaction_path
    )
    fence = IntegrationPublicationFence(project_root=root)
    transaction = fence.replay(transaction_path)
    if (
        transaction.status != "PASS"
        or transaction.transaction["actor"] != actor
        or transaction.transaction["task_id"] != task_id
    ):
        _fail("SOURCE_TRANSACTION_IDENTITY")
    lease_id = transaction.transaction["lease_id"]
    physical = next(
        row for row in fence.guard.store.replay().lease_heads if row.lease_id == lease_id
    )
    source = physical.execution
    if (
        source is None
        or source["request"]["schema_version"] != "workflow_execution_request.v3"
        or source["request"]["request_id"] != source_request_id
        or source["request"]["cwd"] != root.as_posix()
        or source["request"]["source_transaction_sha256"]
        != transaction.transaction["transaction_sha256"]
        or source["state"] != "RESULT_RECORDED"
        or source["result"]["status"] != "PASS"
    ):
        _fail("INSTALLATION_ADOPTED_SOURCE_REQUIRED")
    source_run = (
        root / "outputs/architecture/workflow_integration/source_candidates" / source_request_id
    )
    run = safe._configuration_path(source_run / "installations" / request_id)
    declared = (*transaction.transaction["owned_paths"], *transaction.transaction["shared_paths"])
    if not any(run.relative_to(root).as_posix().startswith(name + "/") for name in declared):
        _fail("SOURCE_RUN_UNDECLARED")
    lifecycle = coordination.InstallationLifecycle(fence.guard.store)
    attempts = source.get("installation_attempts", [])
    for index, attempt in enumerate(attempts):
        saved = attempt["request"]
        if saved["request_id"] != request_id:
            continue
        if saved["installation_action"] != action:
            _fail("INSTALLATION_REQUEST_REUSE_MISMATCH")
        observed = (
            lifecycle.recover(lease_id, actor=actor)
            if index == len(attempts) - 1 and attempt["state"] != "RESULT_RECORDED"
            else {"status": "REPLAY_ONLY", "execution": attempt}
        )
        return {"status": "REPLAY_ONLY", "dispatch_allowed": False, "observation": observed}
    if transaction.phase != "CANDIDATE_COMMIT_PRE" or physical.state != "ACTIVE":
        _fail("INSTALLATION_TRANSACTION_CHANGED")
    if action == "INSTALL":
        if attempts:
            _fail("INSTALLATION_RECOVERY_REQUIRED")
        plan = _prepare_source_installation(
            root,
            task_id,
            transaction_path=transaction_path,
            actor=actor,
            request_id=source_request_id,
        )
        plan_bytes = safe._json_bytes(plan)
        previous_sha = None
    else:
        if not attempts:
            _fail("INSTALLATION_MISSING")
        observation = lifecycle.recover(lease_id, actor=actor)
        physical = next(
            row for row in fence.guard.store.replay().lease_heads if row.lease_id == lease_id
        )
        source = physical.execution
        if source is None:
            _fail("INSTALLATION_MISSING")
        attempts = source["installation_attempts"]
        previous = attempts[-1]
        if previous["state"] != "RESULT_RECORDED":
            return {"status": "OBSERVE_ONLY", "dispatch_allowed": False, "observation": observation}
        if previous["result"]["status"] == "PASS":
            _fail("INSTALLATION_ALREADY_STABLE")
        try:
            plan_bytes = _installation_plan_bytes(
                source_run, previous["request"]["installation_plan_sha256"]
            )
        except WorkflowContractError as error:
            if error.code != "WORKFLOW_MERGE_INSTALLATION_PLAN_CHANGED_OR_MISSING":
                raise
            _recover_unstarted_installation_plan(
                root, transaction_path=transaction_path, actor=actor
            )
            plan_bytes = _installation_plan_bytes(
                source_run, previous["request"]["installation_plan_sha256"]
            )
        plan, _targets = _installation_context(root, previous["request"], source, actor)
        previous_sha = _canonical_sha256(previous)
    if len(plan_bytes) > 64 * 1024 * 1024:
        _fail("INSTALLATION_PLAN_BUDGET")
    manifest = read_bound_json(
        root,
        {
            "path": (source_run / "request.json").relative_to(root).as_posix(),
            "sha256": source["request"]["source_request_sha256"],
        },
    )
    if _source_runtime_binding(root, manifest["prepared"]) != manifest["runtime"]:
        _fail("SOURCE_RUNTIME_CHANGED")
    environment = {
        key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
    }
    environment.update(
        PYTHONPATH=str(root / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_OPTIONAL_LOCKS="0",
    )
    request = {
        **source["request"],
        "schema_version": "workflow_execution_request.v4",
        "execution_kind": "CONTROLLED_SOURCE_INSTALLATION",
        "request_id": request_id,
        "installed_candidate_sha": plan["candidate"],
        "source_result_sha256": source["result"]["artifact"]["sha256"],
        "installation_plan_sha256": hashlib.sha256(plan_bytes).hexdigest(),
        "installation_action": action,
        "installation_attempt": len(attempts) + 1,
        "previous_installation_sha256": previous_sha,
        "stdout_path": (run / "worker.stdout.log").as_posix(),
        "result_path": (run / "worker_result.json").as_posix(),
        "argv": [
            sys.executable,
            str(root / "scripts/architecture_arch005_workflow.py"),
            "source-install-worker",
            "--execution-request",
            str(run / "execution_request.json"),
        ],
        "environment_sha256": execution.execution_environment_sha256(environment),
        "job_name": "Local\\AITS-DEVX015-" + request_id,
    }
    reservation = lifecycle.reserve(request, actor=actor)
    if not reservation["dispatch_allowed"]:
        return reservation
    process = None
    try:
        identity = (plan["root_identity"]["device"], plan["root_identity"]["file_id"])
        if action == "INSTALL":
            write_bound_once(
                root,
                (source_run / "installation_plan.json").relative_to(root).as_posix(),
                plan_bytes,
                expected_root_identity=identity,
            )
        write_bound_once(
            root,
            (run / "execution_request.json").relative_to(root).as_posix(),
            safe._json_bytes(request),
            expected_root_identity=identity,
        )
        process = execution.WindowsJobProcess.create(
            argv=request["argv"],
            cwd=root,
            environment=environment,
            stdout_path=Path(request["stdout_path"]),
            job_name=request["job_name"],
        )
        lifecycle.bind(lease_id, process, actor=actor)
        lifecycle.resume(lease_id, process, actor=actor)
        code = process.wait(timeout=600)
        lifecycle.confirm_exit(lease_id, process, actor=actor)
        if code != 0:
            _fail("INSTALLATION_WORKER_FAILED", str(code))
        result = load_strict_json_text(bounded_regular_bytes(Path(request["result_path"])).decode())
        if not isinstance(result, dict):
            _fail("INSTALLATION_WORKER_RESULT_SHAPE")
        coordination._process(result["worker_process"])
        if result["worker_process"] not in process.observed_members():
            _fail("INSTALLATION_WORKER_PROCESS")
        stable = lifecycle.adopt_stable_result(lease_id, actor=actor)
        return {
            "status": "PASS",
            "request_id": request_id,
            **stable,
            "publication_performed": False,
            "lease_released": False,
        }
    except BaseException as primary:
        if process is not None:
            try:
                process.terminate()
                lifecycle.confirm_exit(lease_id, process, actor=actor)
                lifecycle.record_incomplete_result(lease_id, actor=actor)
            except BaseException as cleanup:
                primary.add_note("Installation custody cleanup: " + repr(cleanup))
        raise
    finally:
        if process is not None:
            process.close()


def finish_source_installation(
    root: Path,
    task_id: str,
    *,
    transaction_path: Path,
    actor: str,
    source_request_id: str,
    request_id: str,
) -> dict[str, Any]:
    """Hand off an independently installed S; never claim final publication.

    The source transaction terminates through the existing administrative FAILED
    route. A subsequent ordinary final transaction must obtain its own Full.
    """
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.architecture.parallel_control_kernel import (
        LeaseEvent,
        _canonical_sha256,
        parse_lease_event,
    )
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = root.absolute()
    if source_request_id == request_id or any(
        re.fullmatch(r"[a-z0-9][a-z0-9-]{7,80}", value) is None
        for value in (source_request_id, request_id)
    ):
        _fail("SOURCE_REQUEST_ID")
    fence = IntegrationPublicationFence(project_root=root)
    replay = fence.replay(transaction_path)
    if (
        replay.status != "PASS"
        or replay.transaction["actor"] != actor
        or replay.transaction["task_id"] != task_id
        or replay.phase not in {"CANDIDATE_COMMIT_PRE", "FAILED"}
    ):
        _fail("SOURCE_HANDOFF_TRANSACTION")
    publication_head = replay.events[-1]["event_id"]
    source_publication_head = publication_head
    if replay.phase == "FAILED":
        if len(replay.events) < 2 or replay.events[-2]["phase"] != "CANDIDATE_COMMIT_PRE":
            _fail("SOURCE_HANDOFF_TERMINAL_PREDECESSOR")
        source_publication_head = replay.events[-2]["event_id"]
    lease_id = replay.transaction["lease_id"]
    lease_replay = fence.guard.store.replay()
    if lease_replay.status != "PASS":
        _fail("SOURCE_HANDOFF_LEASE_REPLAY")
    head = next(row for row in lease_replay.lease_heads if row.lease_id == lease_id)
    observed_lease_head = dict(lease_replay.head_event_ids)[lease_id]
    if head.state not in {"ACTIVE", "RELEASED"} or (
        replay.phase == "FAILED" and head.state != "RELEASED"
    ):
        _fail("SOURCE_HANDOFF_LEASE_STATE")
    source = head.execution
    if (
        source is None
        or source["request"]["schema_version"] != "workflow_execution_request.v3"
        or source["request"]["request_id"] != source_request_id
        or source["request"]["cwd"] != root.as_posix()
        or source["request"]["source_transaction_sha256"]
        != replay.transaction["transaction_sha256"]
        or source["state"] != "RESULT_RECORDED"
        or source["result"]["status"] != "PASS"
        or not source.get("installation_attempts")
    ):
        _fail("SOURCE_HANDOFF_INSTALLED_SOURCE_REQUIRED")
    attempt = source["installation_attempts"][-1]
    request = attempt["request"]
    if (
        request["request_id"] != request_id
        or request["installation_action"] != "INSTALL"
        or attempt["state"] != "RESULT_RECORDED"
        or attempt["result"]["status"] != "PASS"
        or attempt["result"]["reason"] != "INDEPENDENT_SOURCE_INSTALLATION_VERIFIED"
    ):
        _fail("SOURCE_HANDOFF_INDEPENDENT_INSTALL_REQUIRED")
    source_digest = _canonical_sha256(source)
    run = root / "outputs/architecture/workflow_integration/source_candidates" / source_request_id
    source_lease_head = observed_lease_head
    if head.state == "RELEASED":
        # A released lease is historical evidence, not a current writer fence.
        # Only the exact official handoff release may finish its interrupted
        # terminal event/receipt; do not re-observe an unprotected checkout.
        def lease_event(event_id: str) -> LeaseEvent:
            path = fence.guard.store.events_root / lease_id / (event_id + ".json")
            payload = load_strict_json_text(bounded_regular_bytes(path).decode())
            if not isinstance(payload, dict):
                _fail("SOURCE_HANDOFF_RELEASE_EVENT")
            event = parse_lease_event(payload)
            if event.event_id != event_id or event.lease.lease_id != lease_id:
                _fail("SOURCE_HANDOFF_RELEASE_EVENT")
            return event

        released = lease_event(observed_lease_head)
        if (
            released.from_state != "ACTIVE"
            or released.to_state != "RELEASED"
            or released.actor != actor
            or released.reason_codes != ("CHECKOUT_OPERATION_FAILED",)
            or released.lease.to_dict() != head.to_dict()
            or released.previous_event_id is None
        ):
            _fail("SOURCE_HANDOFF_RELEASE_EVENT")
        source_lease_head = released.previous_event_id
        prior = lease_event(source_lease_head)
        prior_body = prior.lease.to_dict()
        prior_body.update(state="RELEASED", evidence_refs=list(head.evidence_refs))
        if prior.to_state != "ACTIVE" or prior_body != head.to_dict():
            _fail("SOURCE_HANDOFF_RELEASE_PREDECESSOR")
    # A heartbeat after interrupted evidence writing gets its own marker. Keep
    # old complete/partial markers, but never consume them for the new head.
    handoff_path = run / ("source_final_handoff." + source_lease_head + ".json")
    declared = (*replay.transaction["owned_paths"], *replay.transaction["shared_paths"])
    if not any(
        handoff_path.relative_to(root).as_posix().startswith(name + "/") for name in declared
    ):
        _fail("SOURCE_RUN_UNDECLARED")
    if head.state == "RELEASED":
        _intent, intent_path = fence.guard._bound_lease_intent(head)
        expected_refs = tuple(
            sorted((intent_path.as_posix(), handoff_path.relative_to(root).as_posix()))
        )
        if head.evidence_refs != expected_refs:
            _fail("SOURCE_HANDOFF_RELEASE_EVENT")
    payload = {
        "schema_version": "controlled_source_final_handoff.v1",
        "task_id": task_id,
        "actor": actor,
        "source_transaction_sha256": replay.transaction["transaction_sha256"],
        "lease_id": lease_id,
        "source_request_id": source_request_id,
        "installation_request_id": request_id,
        "source_execution_sha256": source_digest,
        "observed_lease_head_event_id": source_lease_head,
        "observed_publication_head_event_id": source_publication_head,
        "installation_plan_sha256": request["installation_plan_sha256"],
        "source_candidate_sha": request["installed_candidate_sha"],
        "expected_main_sha": replay.transaction["expected_main_sha"],
        "source_handoff_status": "PASS",
        "publication_outcome": "FAILED",
        "formal_validation_status": "NOT_EXECUTED",
        "stability_observation": "AT_SOURCE_HANDOFF_ONLY",
    }
    encoded = safe._json_bytes(payload)
    if head.state == "ACTIVE":
        plan, _targets = _installation_context(root, request, source, actor)
        _verify_source_installation(root, request, source, actor)
        original = read_bound_json(
            root,
            {
                "path": Path(source["result"]["artifact"]["path"]).relative_to(root).as_posix(),
                "sha256": source["result"]["artifact"]["sha256"],
            },
        )
        manifest = read_bound_json(
            root,
            {
                "path": (run / "request.json").relative_to(root).as_posix(),
                "sha256": source["request"]["source_request_sha256"],
            },
        )
        snapshot = original["snapshot"]
        if snapshot["parents"] != [plan["main"], manifest["prepared"]["lane"]] or _git(
            root, "cat-file", "commit", plan["candidate"]
        ) != _source_commit_bytes(snapshot):
            _fail("SOURCE_WORKER_RESULT_COMMIT")
        if any((base / name).exists() for base, _identity, name in _installation_locks(plan)):
            _fail("SOURCE_HANDOFF_GIT_LOCK_PRESENT")
        fence._require_clean_candidate()
    # The expensive independent observation is outside the short shared arbiter;
    # the original source lease remains the writer boundary until release.
    with fence.guard.store.atomic(actor=actor, now=datetime.now(UTC), operation="terminal"):
        current_leases = fence.guard.store.replay()
        current = next(row for row in current_leases.lease_heads if row.lease_id == lease_id)
        if (
            current_leases.status != "PASS"
            or dict(current_leases.head_event_ids).get(lease_id) != observed_lease_head
            or current.to_dict() != head.to_dict()
        ):
            _fail("SOURCE_HANDOFF_LEASE_CHANGED")
        if current.execution is None or _canonical_sha256(current.execution) != source_digest:
            _fail("SOURCE_HANDOFF_EXECUTION_CHANGED")
        current_replay = fence.replay(transaction_path)
        if (
            current_replay.status != "PASS"
            or current_replay.events[-1]["event_id"] != publication_head
        ):
            _fail("SOURCE_HANDOFF_TRANSACTION_CHANGED")
        if current.state == "ACTIVE":
            _complete_recoverable_evidence(
                root,
                handoff_path,
                encoded,
                root_identity=(plan["root_identity"]["device"], plan["root_identity"]["file_id"]),
                error_prefix="SOURCE_HANDOFF_EVIDENCE",
            )
        elif not handoff_path.exists():
            _fail("SOURCE_HANDOFF_EVIDENCE_MISSING")
        elif bounded_regular_bytes(handoff_path) != encoded:
            _fail("SOURCE_HANDOFF_EVIDENCE_CHANGED")
        receipt = fence.release(
            transaction_path, actor=actor, outcome="failed", evidence_paths=[handoff_path]
        )
    return {
        **payload,
        "publication_receipt": receipt,
        "dispatch_allowed": False,
        "publication_performed": False,
        "lease_released": True,
    }


def source_candidate_worker(root: Path, execution_request_path: Path) -> dict[str, Any]:
    """Contained source worker: generate, close delta, construct private S only."""
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture import workflow_execution as execution
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = root.absolute()
    request_path = safe._configuration_path(execution_request_path.absolute())
    run = request_path.parent
    frozen_execution = coordination._request(
        load_strict_json_text(bounded_regular_bytes(request_path).decode("utf-8"))
    )
    if (
        request_path.name != "execution_request.json"
        or frozen_execution["schema_version"] != "workflow_execution_request.v3"
        or run.parent != root / "outputs/architecture/workflow_integration/source_candidates"
        or run.name != frozen_execution["request_id"]
        or frozen_execution["cwd"] != root.as_posix()
        or frozen_execution["result_path"] != (run / "worker_result.json").as_posix()
        or frozen_execution["argv"]
        != [
            sys.executable,
            str(root / "scripts/architecture_arch005_workflow.py"),
            "source-worker",
            "--execution-request",
            str(request_path),
        ]
    ):
        _fail("SOURCE_WORKER_LOCATOR")
    manifest_bytes = bounded_regular_bytes(run / "request.json")
    manifest = load_strict_json_text(manifest_bytes.decode("utf-8"))
    if not isinstance(manifest, dict):
        _fail("SOURCE_REQUEST_SHAPE")
    if (
        set(manifest)
        != {
            "schema_version",
            "request_id",
            "actor",
            "prepared",
            "transaction_path",
            "timestamp",
            "runtime",
        }
        or manifest["schema_version"] != "controlled_source_candidate_request.v1"
        or hashlib.sha256(manifest_bytes).hexdigest() != frozen_execution["source_request_sha256"]
        or manifest["request_id"] != frozen_execution["request_id"]
    ):
        _fail("SOURCE_REQUEST_BINDING")
    prepared = manifest["prepared"]
    if (
        frozen_execution["source_head_sha"] != prepared["main"]
        or frozen_execution["source_transaction_sha256"] != prepared["source_transaction_sha256"]
        or frozen_execution["task_authority_sha256"] != prepared["authority_sha256"]
        or frozen_execution["review_sha256"] != prepared["review_ref"]["sha256"]
        or execution.execution_environment_sha256(dict(os.environ))
        != frozen_execution["environment_sha256"]
    ):
        _fail("SOURCE_REQUEST_BINDING")
    transaction_path = root / portable_path(manifest["transaction_path"])
    actor = manifest["actor"]
    fence = IntegrationPublicationFence(project_root=root)
    lifecycle = fence.guard.store.execution_lifecycle()

    def admit(phase: str) -> dict[str, Any]:
        witness = lifecycle.require_source_candidate_worker(frozen_execution, actor=actor)
        fence.validate(transaction_path, exact_phase=phase, task_id=prepared["task_id"])
        replay = fence.replay(transaction_path)
        if (
            replay.transaction["transaction_sha256"] != prepared["source_transaction_sha256"]
            or replay.transaction["lease_id"] != frozen_execution["lease_id"]
            or _source_runtime_binding(root, prepared) != manifest["runtime"]
        ):
            _fail("SOURCE_WORKER_AUTHORITY_CHANGED")
        return dict(witness)

    def progress(generator: str, phase: str, boundary: str) -> None:
        # Worker-only diagnostics in its already-bound stdout. No source bytes,
        # authority or candidate result is inferred from these observations.
        print(
            json.dumps(
                {
                    "observation_only": True,
                    "generator": generator,
                    "phase": phase,
                    "boundary": boundary,
                    "observed_at": datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            ),
            flush=True,
        )

    progress("source-candidate", "WORKER_ADMISSION", "START")
    witness = admit("GENERATED_REBUILD_PRE")
    progress("source-candidate", "WORKER_ADMISSION", "END")
    delta, captured = render_source_candidate_delta(
        root, prepared, transaction_path=transaction_path, actor=actor, progress=progress
    )
    generation_content = _encode_source_generation(delta)
    admit("GENERATED_REBUILD_PRE")
    write_bound_once(
        root, (run / "generation.json").relative_to(root).as_posix(), generation_content
    )
    for position, operation in enumerate(delta["operations"]):
        if operation["path"] in captured:
            write_bound_once(
                root,
                (run / "capture" / f"{position:06d}.bin").relative_to(root).as_posix(),
                captured[operation["path"]],
            )
    fence.checkpoint(
        transaction_path,
        phase="GENERATED_REBUILD_POST",
        actor=actor,
        generator_ids=prepared["generator_order"],
        evidence_paths=[run / "generation.json"],
    )
    admit("GENERATED_REBUILD_POST")
    fence.checkpoint(transaction_path, phase="CANDIDATE_COMMIT_PRE", actor=actor)
    admit("CANDIDATE_COMMIT_PRE")
    io_backend = safe.SourcePreservation(root)
    for position, operation in enumerate(delta["operations"]):
        name = operation["path"]
        after = operation["after"]
        if after["exists"]:
            content = bounded_regular_bytes(run / "capture" / f"{position:06d}.bin")
            if content != captured[name]:
                _fail("SOURCE_CAPTURE_CHANGED", name)
            oid = (
                io_backend._git(root, "hash-object", "-w", "--stdin", content=content)
                .decode()
                .strip()
            )
            if oid != after["oid"]:
                _fail("SOURCE_CAPTURE_CHANGED", name)
    tree = _source_tree_without_index(root, io_backend, prepared["main"], delta["operations"])

    def tree_objects(identity: str) -> dict[str, tuple[str, str, str]]:
        result: dict[str, tuple[str, str, str]] = {}
        for record in io_backend._git(root, "ls-tree", "-r", "-z", identity).split(b"\0"):
            if record:
                metadata, name = record.split(b"\t", 1)
                mode, kind, oid = metadata.decode("ascii").split()
                result[name.decode("utf-8")] = (mode, kind, oid)
        return result

    expected_tree = tree_objects(prepared["main"])
    for row in delta["operations"]:
        after = row["after"]
        if after["exists"]:
            expected_tree[row["path"]] = (after["mode"], after["type"], after["oid"])
        else:
            expected_tree.pop(row["path"], None)
    # Complete tree metadata also proves excluded M entries were preserved;
    # excluded blob contents are never opened, hashed, copied, or modified.
    if tree_objects(tree) != expected_tree:
        _fail("SOURCE_PRIVATE_TREE_DELTA")
    intent = {
        "tree": tree,
        "parents": [prepared["main"], prepared["lane"]],
        "timestamp": manifest["timestamp"],
        "message": "DEVX-015 reviewed source integration " + manifest["request_id"] + "\n",
    }
    write_bound_once(
        root,
        (run / "object_intent.json").relative_to(root).as_posix(),
        safe._json_bytes(intent),
    )
    admit("CANDIDATE_COMMIT_PRE")
    if (
        prepare_source_generation(
            root,
            prepared["task_id"],
            transaction_path=transaction_path,
            actor=actor,
            phase="CANDIDATE_COMMIT_PRE",
        )
        != prepared
    ):
        _fail("SOURCE_REQUEST_INPUTS_CHANGED")
    commit = (
        io_backend._git(
            root,
            "commit-tree",
            tree,
            "-p",
            intent["parents"][0],
            "-p",
            intent["parents"][1],
            content=intent["message"].encode(),
            timestamp=intent["timestamp"],
        )
        .decode()
        .strip()
    )
    header = io_backend._git(root, "cat-file", "commit", commit).split(b"\n\n", 1)[0].decode()
    if (
        [line[7:] for line in header.splitlines() if line.startswith("parent ")]
        != intent["parents"]
        or header.splitlines()[0] != "tree " + tree
        or _git(root, "rev-parse", "HEAD", "refs/heads/main").decode().splitlines()
        != [prepared["main"], prepared["main"]]
    ):
        _fail("SOURCE_PRIVATE_COMMIT_IDENTITY")
    admit("CANDIDATE_COMMIT_PRE")
    observed_generation = _source_generation_bytes(run)
    if observed_generation != generation_content:
        _fail("SOURCE_GENERATION_CHANGED")
    result = {
        **coordination._result_binding(frozen_execution),
        "status": "PASS",
        "snapshot": {"commit": commit, **intent},
        "worker_process": witness["worker_process"],
        "generation_sha256": hashlib.sha256(observed_generation).hexdigest(),
        "installation_performed": False,
        "publication_performed": False,
    }
    write_bound_once(
        root,
        Path(frozen_execution["result_path"]).relative_to(root).as_posix(),
        safe._json_bytes(result),
    )
    return result


def _source_runtime_binding(root: Path, prepared: Mapping[str, Any]) -> dict[str, Any]:
    """Bind actual interpreter and loaded project files to the reviewed namespace."""
    import importlib.metadata

    records = []
    for name, module in sorted(sys.modules.items()):
        if name != "ai_trading_system" and not name.startswith("ai_trading_system."):
            continue
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str):
            _fail("SOURCE_LOADED_MODULE", name)
        path = Path(origin).absolute()
        expected = root / "src" / Path(*name.split("."))
        if path not in {expected.with_suffix(".py"), expected / "__init__.py"}:
            _fail("SOURCE_LOADED_MODULE", name)
        relative = path.relative_to(root).as_posix()
        if relative not in prepared["expected_inputs"]:
            _fail("SOURCE_LOADED_MODULE", name)
        content = bounded_regular_bytes(path)
        oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
        if oid != prepared["expected_inputs"][relative]["oid"]:
            _fail("SOURCE_LOADED_MODULE", name)
        records.append(
            {"module": name, "path": relative, "sha256": hashlib.sha256(content).hexdigest()}
        )
    return {
        "interpreter": sys.executable,
        "version": sys.version,
        "interpreter_sha256": hashlib.sha256(
            bounded_regular_bytes(Path(sys.executable))
        ).hexdigest(),
        # Lazy imports may add authorized project modules during generation.
        # Every actual loaded module was checked above; identity binds the full
        # reviewed project namespace rather than an incidental import prefix.
        "project_namespace_sha256": canonical_digest(
            {
                name: state
                for name, state in prepared["expected_inputs"].items()
                if name.startswith("src/") and name.endswith(".py")
            }
        ),
        "dependencies": sorted(
            [dist.metadata["Name"], dist.version] for dist in importlib.metadata.distributions()
        ),
    }


def start_source_candidate(
    root: Path, task_id: str, *, transaction_path: Path, actor: str, request_id: str
) -> dict[str, Any]:
    """Run one contained source candidate attempt under the existing lease."""
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture import workflow_coordination as coordination
    from ai_trading_system.platform.architecture import workflow_execution as execution
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = root.absolute()
    if os.name != "nt" or sys.version_info[:2] != (3, 11):
        _fail("SOURCE_RUNTIME_UNSUPPORTED")
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,80}", request_id) is None:
        _fail("SOURCE_REQUEST_ID")
    run = safe._configuration_path(
        root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    )
    transaction_path = (
        transaction_path if transaction_path.is_absolute() else root / transaction_path
    )
    fence = IntegrationPublicationFence(project_root=root)
    lifecycle = fence.guard.store.execution_lifecycle()
    transaction = fence.replay(transaction_path)
    if transaction.status != "PASS" or transaction.transaction["actor"] != actor:
        _fail("SOURCE_TRANSACTION_IDENTITY")
    if transaction.transaction["task_id"] != task_id:
        _fail("SOURCE_TRANSACTION_IDENTITY")
    declared = (*transaction.transaction["owned_paths"], *transaction.transaction["shared_paths"])
    if not any(run.relative_to(root).as_posix().startswith(name + "/") for name in declared):
        _fail("SOURCE_RUN_UNDECLARED")
    existing_lease = next(
        row
        for row in fence.guard.store.replay().lease_heads
        if row.lease_id == transaction.transaction["lease_id"]
    )
    if existing_lease.execution is not None and (
        not (run / "request.json").exists() or not (run / "execution_request.json").exists()
    ):
        return recover_source_candidate(
            root,
            task_id,
            transaction_path=transaction_path,
            actor=actor,
            request_id=request_id,
            finish_failed=False,
        )
    if run.exists():
        # A partial capture is evidence, never permission to regenerate or
        # choose a new timestamp/commit under the same request id.
        saved_bytes = bounded_regular_bytes(run / "request.json")
        saved = load_strict_json_text(saved_bytes.decode())
        if not isinstance(saved, dict):
            _fail("SOURCE_REQUEST_SHAPE")
        if (
            saved["request_id"] != request_id
            or saved["actor"] != actor
            or saved["prepared"]["task_id"] != task_id
            or saved["prepared"]["source_transaction_sha256"]
            != transaction.transaction["transaction_sha256"]
        ):
            _fail("SOURCE_REQUEST_BINDING")
        saved_execution = coordination._request(
            load_strict_json_text(bounded_regular_bytes(run / "execution_request.json").decode())
        )
        if (
            saved_execution["schema_version"] != "workflow_execution_request.v3"
            or saved_execution["request_id"] != request_id
            or saved_execution["lease_id"] != transaction.transaction["lease_id"]
            or saved_execution["source_request_sha256"] != hashlib.sha256(saved_bytes).hexdigest()
        ):
            _fail("SOURCE_REQUEST_BINDING")
        head = next(
            row
            for row in fence.guard.store.replay().lease_heads
            if row.lease_id == saved_execution["lease_id"]
        )
        if head.execution is None:
            return {
                "status": "INSUFFICIENT",
                "dispatch_allowed": False,
                "reason": "SOURCE_REQUEST_PERSISTED_WITHOUT_EXECUTION",
                "request_id": request_id,
            }
        if head.execution["request"] != saved_execution:
            _fail("SOURCE_REQUEST_BINDING")
        return {
            "status": "REPLAY_ONLY",
            "dispatch_allowed": False,
            "execution": lifecycle.recover(transaction.transaction["lease_id"], actor=actor),
        }
    prepared = prepare_source_generation(
        root, task_id, transaction_path=transaction_path, actor=actor
    )
    if prepared["generator_order"] != list(_SOURCE_GENERATORS):
        _fail("CANDIDATE_GENERATOR_ORDER")
    environment = {
        key: value for key, value in os.environ.items() if not key.upper().startswith("GIT_")
    }
    environment.update(
        PYTHONPATH=str(root / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_OPTIONAL_LOCKS="0",
    )
    manifest = {
        "schema_version": "controlled_source_candidate_request.v1",
        "request_id": request_id,
        "actor": actor,
        "prepared": prepared,
        "transaction_path": transaction_path.relative_to(root).as_posix(),
        "timestamp": datetime.now(UTC).isoformat(),
        "runtime": _source_runtime_binding(root, prepared),
    }
    manifest_bytes = safe._json_bytes(manifest)
    lease = next(
        row
        for row in fence.guard.store.replay().lease_heads
        if row.lease_id == transaction.transaction["lease_id"]
    )
    binding = fence.guard.store.coordination_binding
    if binding is not None:
        binding.assert_current(operation="observe")
    execution_request = {
        "schema_version": "workflow_execution_request.v3",
        "request_id": request_id,
        "execution_kind": "CONTROLLED_SOURCE_CANDIDATE",
        "lease_id": lease.lease_id,
        "manifest_sha256": lease.change_manifest_sha256,
        # The shared publication lease belongs to the checkout guard. The
        # business task is independently bound by prepared authority and fence.
        "subject_task_id": lease.task_id,
        "source_head_sha": prepared["main"],
        "source_request_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "source_transaction_sha256": prepared["source_transaction_sha256"],
        "task_authority_sha256": prepared["authority_sha256"],
        "review_sha256": prepared["review_ref"]["sha256"],
        "cwd": root.as_posix(),
        "stdout_path": (run / "worker.stdout.log").as_posix(),
        "result_path": (run / "worker_result.json").as_posix(),
        "argv": [
            sys.executable,
            str(root / "scripts/architecture_arch005_workflow.py"),
            "source-worker",
            "--execution-request",
            str(run / "execution_request.json"),
        ],
        "environment_sha256": execution.execution_environment_sha256(environment),
        "job_name": "Local\\AITS-DEVX015-" + request_id,
        "host_id": binding.host_id if binding else coordination.machine_host_id(),
        "writer_epoch": binding.epoch if binding else "UNENROLLED_LEGACY",
    }
    coordination._request(execution_request)
    # Persist the complete execution/launcher identity in the existing lease
    # before the first private file. A crash during request persistence then
    # has an OS-verifiable recovery owner, even if no request file survived.
    reservation = lifecycle.reserve(execution_request, actor=actor)
    if reservation["dispatch_allowed"] is not True:
        return dict(reservation)
    process = None
    try:
        write_bound_once(root, (run / "request.json").relative_to(root).as_posix(), manifest_bytes)
        write_bound_once(
            root,
            (run / "execution_request.json").relative_to(root).as_posix(),
            safe._json_bytes(execution_request),
        )
        fence.checkpoint(
            transaction_path,
            phase="GENERATED_REBUILD_PRE",
            actor=actor,
            generator_ids=prepared["generator_order"],
            evidence_paths=[run / "request.json", run / "execution_request.json"],
        )
        process = execution.WindowsJobProcess.create(
            argv=execution_request["argv"],
            cwd=root,
            environment=environment,
            stdout_path=Path(execution_request["stdout_path"]),
            job_name=execution_request["job_name"],
        )
        lifecycle.bind(lease.lease_id, process, actor=actor)
        lifecycle.resume(lease.lease_id, process, actor=actor)
        # Engineering hang bound only; not lease expiry or permission to retry.
        code = process.wait(timeout=_SOURCE_WORKER_TIMEOUT_SECONDS)
        lifecycle.confirm_exit(lease.lease_id, process, actor=actor)
        if code != 0:
            _fail("SOURCE_WORKER_FAILED", str(code))
        result_bytes = bounded_regular_bytes(Path(execution_request["result_path"]))
        result = load_strict_json_text(result_bytes.decode())
        if not isinstance(result, dict):
            _fail("SOURCE_RESULT_SHAPE")
        if (
            result.get("status") != "PASS"
            or result.get("installation_performed") is not False
            or result.get("publication_performed") is not False
            or any(
                result.get(key) != value
                for key, value in coordination._result_binding(execution_request).items()
            )
        ):
            _fail("SOURCE_WORKER_RESULT")
        # Child custody is not adoption. Recompute the final private view in
        # the parent without installing it, then inspect actual Git objects.
        expected_delta, _expected_bytes = render_source_candidate_delta(
            root,
            prepared,
            transaction_path=transaction_path,
            actor=actor,
            phase="CANDIDATE_COMMIT_PRE",
        )
        generation_bytes = _source_generation_bytes(run)
        if (
            load_strict_json_text(generation_bytes.decode()) != expected_delta
            or hashlib.sha256(generation_bytes).hexdigest() != result["generation_sha256"]
        ):
            _fail("SOURCE_WORKER_RESULT_DELTA")
        snapshot = result["snapshot"]
        intent = load_strict_json_text(bounded_regular_bytes(run / "object_intent.json").decode())
        if not isinstance(intent, dict):
            _fail("SOURCE_OBJECT_INTENT_SHAPE")
        if (
            set(snapshot) != {"commit", "tree", "parents", "timestamp", "message"}
            or {key: value for key, value in snapshot.items() if key != "commit"} != intent
            or intent["parents"] != [prepared["main"], prepared["lane"]]
            or intent["timestamp"] != manifest["timestamp"]
            or intent["message"] != "DEVX-015 reviewed source integration " + request_id + "\n"
        ):
            _fail("SOURCE_WORKER_RESULT_COMMIT")
        io_backend = safe.SourcePreservation(root)
        actual_commit = io_backend._git(root, "cat-file", "commit", snapshot["commit"])
        if actual_commit != _source_commit_bytes(intent):
            _fail("SOURCE_WORKER_RESULT_COMMIT")

        def tree_map(identity: str) -> dict[str, tuple[str, ...]]:
            value = {}
            for record in io_backend._git(root, "ls-tree", "-r", "-z", identity).split(b"\0"):
                if record:
                    metadata, name = record.split(b"\t", 1)
                    value[name.decode()] = tuple(metadata.decode().split())
            return value

        expected_tree = tree_map(prepared["main"])
        for operation in expected_delta["operations"]:
            after = operation["after"]
            if after["exists"]:
                expected_tree[operation["path"]] = (after["mode"], after["type"], after["oid"])
            else:
                expected_tree.pop(operation["path"], None)
        if tree_map(snapshot["commit"]) != expected_tree:
            _fail("SOURCE_WORKER_RESULT_TREE")
        coordination._process(result["worker_process"])
        if result["worker_process"] not in process.observed_members():
            _fail("SOURCE_WORKER_RESULT_PROCESS")
        lifecycle.record_result(
            lease.lease_id,
            actor=actor,
            result_path=Path(execution_request["result_path"]),
            expected_sha256=hashlib.sha256(result_bytes).hexdigest(),
        )
        return result
    except BaseException:
        if process is not None:
            process.terminate()
            lifecycle.confirm_exit(lease.lease_id, process, actor=actor)
            lifecycle.record_incomplete_result(lease.lease_id, actor=actor)
        raise
    finally:
        if process is not None:
            process.close()


def recover_source_candidate(
    root: Path,
    task_id: str,
    *,
    transaction_path: Path,
    actor: str,
    request_id: str,
    finish_failed: bool = True,
) -> dict[str, Any]:
    """Finish a dead incomplete source attempt, preserving every private object.

    No materialization or redispatch is possible here. Actual lifecycle and
    publication terminal receipts remain the only stores of recovery authority.
    """
    from ai_trading_system.platform.architecture import source_preservation as safe
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    root = root.absolute()
    if re.fullmatch(r"[a-z0-9][a-z0-9-]{7,80}", request_id) is None:
        _fail("SOURCE_REQUEST_ID")
    run = safe._configuration_path(
        root / "outputs/architecture/workflow_integration/source_candidates" / request_id
    )
    fence = IntegrationPublicationFence(project_root=root)
    replay = fence.replay(transaction_path)
    if (
        replay.status != "PASS"
        or replay.transaction["actor"] != actor
        or replay.transaction["task_id"] != task_id
    ):
        _fail("SOURCE_TRANSACTION_IDENTITY")
    head = next(
        row
        for row in fence.guard.store.replay().lease_heads
        if row.lease_id == replay.transaction["lease_id"]
    )
    if head.execution is None:
        return {
            "status": "INSUFFICIENT",
            "reason": "SOURCE_RESERVATION_MISSING",
            "dispatch_allowed": False,
            "release_performed": False,
        }
    request = head.execution["request"]
    if (
        request["schema_version"] != "workflow_execution_request.v3"
        or request["request_id"] != request_id
        or request["cwd"] != root.as_posix()
        or request["result_path"] != (run / "worker_result.json").as_posix()
        or request["source_transaction_sha256"] != replay.transaction["transaction_sha256"]
        or request["source_head_sha"] != replay.transaction["expected_main_sha"]
    ):
        _fail("SOURCE_REQUEST_BINDING")
    observed = fence.guard.store.execution_lifecycle().recover(head.lease_id, actor=actor)
    current = next(
        row for row in fence.guard.store.replay().lease_heads if row.lease_id == head.lease_id
    )
    execution = current.execution
    if execution is None:
        _fail("SOURCE_EXECUTION_CHANGED")
    terminal_failed = (
        execution["state"] == "RESULT_RECORDED" and execution["result"]["status"] != "PASS"
    )
    if not finish_failed or not terminal_failed:
        return {
            "status": "REPLAY_ONLY",
            "execution": observed,
            "dispatch_allowed": False,
            "release_performed": False,
        }
    # Release is gated again by the existing fence/store. A live or unknown
    # launcher/Job cannot reach terminal_failed and cannot be released here.
    evidence = (
        []
        if replay.phase == "FAILED"
        else [
            path
            for name in ("request.json", "execution_request.json", "worker_result.json")
            if (path := run / name).exists()
        ]
    )
    receipt = fence.release(
        transaction_path,
        actor=actor,
        outcome="failed",
        evidence_paths=evidence,
    )
    return {
        "status": "RECOVERED_FAILED",
        "dispatch_allowed": False,
        "release_performed": True,
        "publication_receipt": receipt,
        "private_evidence_preserved": True,
        "installation_performed": False,
    }


def _capture_controlled_candidate_delta(
    root: Path, task_id: str, *, transaction_path: Path, actor: str
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Close every candidate delta over reviewed sources and exact official outputs.

    This observation deliberately grants no materialization permission. A source
    worker additionally needs frozen real generator inputs and execution custody;
    generator names or these freshness checks cannot substitute for either.
    """
    from ai_trading_system.platform.architecture.checkout_guard import (
        collect_checkout_dirty_paths,
    )
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
    )

    fence = IntegrationPublicationFence(project_root=root)
    fence.validate(transaction_path, task_id=task_id)
    transaction = fence.replay(transaction_path)
    if transaction.phase not in {
        "TASK_SOURCE_PRE_WRITE",
        "GENERATED_REBUILD_POST",
        "CANDIDATE_COMMIT_PRE",
    }:
        _fail("CANDIDATE_SOURCE_PHASE")
    if transaction.transaction["actor"] != actor:
        _fail("CANDIDATE_ACTOR")
    structural = load_current_task_structure(root, task_id)
    scope = _scope(structural)
    reference = structural["authority"]["review_ref"]
    if reference is None:
        _fail("REVIEW_NOT_FROZEN")
    review = read_bound_json(root, reference)
    structural_plan = _plan_from_authority(root, task_id, structural)
    _validate_review(structural_plan, review)
    initial_dirty = collect_checkout_dirty_paths(root, exclusions=_exclusions(root))
    if _candidate_source_paths(initial_dirty, scope) != [
        row["path"] for row in review["candidate_sources"]
    ]:
        _fail("CANDIDATE_DELTA_UNCOVERED", "source set differs from frozen review")
    scan_names = _candidate_scan_admission(root, scope["latest_main"], review)
    authority = load_current_task_authority(root, task_id)
    if authority != {
        key: value for key, value in structural.items() if key != "consumer_inventory_checked"
    }:
        _fail("CANDIDATE_AUTHORITY_CHANGED")
    scope = _scope(authority)
    main = scope["latest_main"]
    if (
        transaction.transaction["expected_main_sha"] != main
        or transaction.transaction["lane_head_sha"] != main
        or _git(root, "rev-parse", "HEAD").decode().strip() != main
    ):
        _fail("CANDIDATE_BASE")
    plan = build_controlled_merge_plan(root, task_id)
    reference = authority["authority"]["review_ref"]
    if reference is None:
        _fail("REVIEW_NOT_FROZEN")
    review = read_bound_json(root, reference)
    _validate_review(plan, review)
    if review["source_transaction_sha256"] != transaction.transaction["transaction_sha256"]:
        _fail("CANDIDATE_TRANSACTION_CHANGED")
    readers = {
        "canonical-task-source": inspect_canonical_merge_outputs,
        "architecture-manifests": inspect_architecture_merge_outputs,
        "report-flow-authority": inspect_report_merge_outputs,
        "compatibility-authority": inspect_compatibility_merge_outputs,
    }
    # The approved source/final split defers Atlas until committed S exists.
    order = [name for name in scope["generator_order"] if name != "atlas-authority"]
    if (
        not order
        or len(order) != len(set(order))
        or any(name not in readers for name in order)
        or order != transaction.transaction["generator_ids"]
    ):
        _fail("CANDIDATE_GENERATOR_ORDER")
    inventories = {name: readers[name](root, task_id) for name in order}
    outputs: dict[str, str] = {}
    obsolete: set[str] = set()
    retained: set[str] = set()
    for generator, inventory in inventories.items():
        if inventory["main"] != main:
            _fail("CANDIDATE_GENERATOR_BASE")
        for path in inventory["output_paths"]:
            if path in outputs:
                _fail("CANDIDATE_OUTPUT_OVERLAP", path)
            outputs[portable_path(path)] = generator
        obsolete.update(inventory.get("obsolete_main_paths", ()))
        retained.update(inventory.get("retained_main_paths", ()))
    if set(outputs) & (obsolete | retained):
        _fail("CANDIDATE_OUTPUT_OVERLAP")
    sources = {row["path"]: row["object"] for row in review["candidate_sources"]}
    for row in review["resolutions"]:
        if row["result"] is not None:
            if row["path"] in sources and sources[row["path"]] != row["result"]:
                _fail("CANDIDATE_SOURCE_CONFLICT", row["path"])
            sources[row["path"]] = row["result"]
    if set(sources) & (set(outputs) | obsolete | retained):
        _fail("CANDIDATE_SOURCE_OUTPUT_OVERLAP")
    excluded = _exclusions(root)
    dirty = collect_checkout_dirty_paths(root, exclusions=excluded)
    allowed = set(sources) | set(outputs) | obsolete | retained
    unknown = set(dirty) - allowed
    if unknown:
        # Names only. Never open/hash an unreviewed path just to classify it.
        _fail("CANDIDATE_DELTA_UNCOVERED", ",".join(sorted(unknown)))
    declared = (*transaction.transaction["owned_paths"], *transaction.transaction["shared_paths"])
    for path in dirty:
        if not any(path == item or path.startswith(item + "/") for item in declared):
            _fail("CANDIDATE_DELTA_UNDECLARED", path)
    validate_controlled_merge_plan(root, task_id, plan)
    objects = {path: working_object(root, path) for path in sorted(allowed)}
    operations = []
    captured: dict[str, bytes] = {}
    for path, observed in objects.items():
        previous = _at(root, main, path)
        if path in sources and observed != sources[path]:
            _fail("REVIEW_WORKING_RESULT_CHANGED", path)
        if path in outputs and (
            not observed["exists"] or observed["mode"] not in {"100644", "100755"}
        ):
            _fail("CANDIDATE_OUTPUT_OBJECT", path)
        if path in outputs and observed["mode"] != (
            previous["mode"] if previous["exists"] else "100644"
        ):
            _fail("CANDIDATE_OUTPUT_MODE", path)
        if path in retained and observed != previous:
            _fail("CANDIDATE_RETAINED_MAIN_CHANGED", path)
        if path in obsolete and observed["exists"]:
            _fail("CANDIDATE_OBSOLETE_NOT_REMOVED", path)
        if observed == previous:
            continue
        if not any(path == item or path.startswith(item + "/") for item in declared):
            _fail("CANDIDATE_DELTA_UNDECLARED", path)
        if observed["exists"]:
            content = bounded_regular_bytes(root / path)
            oid = hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()
            if oid != observed["oid"]:
                _fail("CANDIDATE_OBJECT_CHANGED", path)
            captured[path] = content
        operations.append(
            {
                "path": path,
                "operation": "D" if not observed["exists"] else "M" if previous["exists"] else "A",
                "before": previous,
                "after": observed,
                "origin": "REVIEWED_SOURCE" if path in sources else "OFFICIAL_GENERATOR",
                "generator_id": outputs.get(path)
                or ("compatibility-authority" if path in obsolete else None),
            }
        )
    # Freshness is recomputed after capture; no reader's mutable self-report is
    # treated as an immutable output witness. Recheck modes/presence separately.
    if _candidate_scan_admission(root, main, review) != scan_names:
        _fail("CANDIDATE_SCAN_SET_CHANGED")
    if collect_checkout_dirty_paths(root, exclusions=excluded) != dirty:
        _fail("CANDIDATE_DELTA_SET_CHANGED")
    if {name: readers[name](root, task_id) for name in order} != inventories:
        _fail("CANDIDATE_OUTPUT_INVENTORY_CHANGED")
    if any(working_object(root, path) != value for path, value in objects.items()):
        _fail("CANDIDATE_OBJECT_CHANGED")
    if collect_checkout_dirty_paths(root, exclusions=excluded) != dirty:
        _fail("CANDIDATE_DELTA_SET_CHANGED")
    if load_current_task_authority(root, task_id) != authority:
        _fail("CANDIDATE_AUTHORITY_CHANGED")
    fence.validate(transaction_path, exact_phase=transaction.phase, task_id=task_id)
    if _git(root, "rev-parse", "HEAD").decode().strip() != main:
        _fail("CANDIDATE_BASE")
    result = {
        "schema_version": "controlled_candidate_delta.v1",
        "status": "VALIDATED_CANDIDATE_DELTA",
        "task_id": task_id,
        "main": main,
        "lane": scope["lane_head"],
        "plan_sha256": plan["plan_sha256"],
        "review_ref": reference,
        "authority_sha256": authority["authority_sha256"],
        "current_event_id": authority["current_event_id"],
        "source_transaction_sha256": transaction.transaction["transaction_sha256"],
        "generator_order": order,
        "generator_inventories": inventories,
        "operations": operations,
        "materialization_allowed": False,
        "generator_execution_proven": False,
    }
    return result, captured


def inspect_controlled_candidate_delta(
    root: Path, task_id: str, *, transaction_path: Path, actor: str
) -> dict[str, Any]:
    """Public read-only view; captured raw bytes do not escape as an execution grant."""
    result, _captured = _capture_controlled_candidate_delta(
        root, task_id, transaction_path=transaction_path, actor=actor
    )
    return result


def _candidate_sources(
    root: Path, fence: Any, transaction: Any, scope: Mapping[str, Any]
) -> list[dict[str, Any]]:
    fence._require_dirty_attributed(transaction)
    return [
        {"path": path, "object": working_object(root, path)}
        for path in _candidate_source_paths(fence.guard.audit_worktree().dirty_paths, scope)
    ]


def freeze_controlled_merge_review(
    root: Path,
    task_id: str,
    *,
    proposal: Mapping[str, Any],
    transaction_path: Path,
    actor: str,
    change_id: str,
) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.integration_publication_fence import (
        IntegrationPublicationFence,
        _write_json_exclusive,
    )
    from ai_trading_system.platform.architecture.task_registry_canonical import update_task

    fence = IntegrationPublicationFence(project_root=root)
    now = datetime.now(UTC)
    with fence.guard.store.atomic(actor=actor, now=now):
        fence.validate(transaction_path, exact_phase="TASK_SOURCE_PRE_WRITE", task_id=task_id)
        transaction = fence.replay(transaction_path)
        if transaction.transaction["actor"] != actor:
            _fail("REVIEW_ACTOR")
        plan = build_controlled_merge_plan(root, task_id)
        if plan["status"] == "NO_RESIDUAL_SOURCE":
            return {"status": "NO_RESIDUAL_SOURCE", "plan_sha256": plan["plan_sha256"]}
        authority = load_current_task_authority(root, task_id)
        if set(proposal) != {"resolutions", "contract_resolutions", "rename_pairs"}:
            _fail("PROPOSAL_FIELDS")
        review = {
            "schema_version": "controlled_merge_review.v1",
            "task_id": task_id,
            "plan_sha256": plan["plan_sha256"],
            "scope_ref": plan["scope_ref"],
            "actor": actor,
            **json.loads(json.dumps(proposal)),
            "candidate_sources": _candidate_sources(root, fence, transaction, authority["scope"]),
            "source_transaction_sha256": transaction.transaction["transaction_sha256"],
        }
        _validate_review(plan, review)
        for resolution in review["resolutions"]:
            if resolution["disposition"] == "MERGE_REVIEWED":
                if working_object(root, resolution["path"]) != resolution["result"]:
                    _fail("REVIEW_WORKING_RESULT_CHANGED", resolution["path"])
        destination = (
            root
            / "outputs/architecture/workflow_integration/reviews"
            / (canonical_digest(review) + ".json")
        )
        relative = destination.relative_to(root).as_posix()
        declared = (
            *transaction.transaction["owned_paths"],
            *transaction.transaction["shared_paths"],
        )
        if not any(relative == path or relative.startswith(path + "/") for path in declared):
            _fail("REVIEW_PATH_UNDECLARED")
        if destination.exists():
            content = bounded_regular_bytes(destination)
            if json.loads(content) != review:
                _fail("REVIEW_REPLAY_MISMATCH")
        else:
            _write_json_exclusive(destination, review)
            content = bounded_regular_bytes(destination)
        reference = {"path": relative, "sha256": hashlib.sha256(content).hexdigest()}
        if authority["authority"]["review_ref"] != reference:
            updated_authority = {**authority["authority"], "review_ref": reference}
            update_task(
                project_root=root,
                task_id=task_id,
                actor=actor,
                change_id=change_id,
                occurred_at=now.isoformat(),
                base_commit=_git(root, "rev-parse", "HEAD").decode().strip(),
                workflow_authority=updated_authority,
            )
        # Current canonical authority is the only locator on replay/validation.
        result = validate_controlled_merge_plan(root, task_id, plan)
        return {
            "status": result["status"],
            "plan_sha256": plan["plan_sha256"],
            "review_ref": reference,
        }
