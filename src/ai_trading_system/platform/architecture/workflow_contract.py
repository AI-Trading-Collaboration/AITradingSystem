"""Narrow DEVX-015 current-task authority and bounded artifact reads.

This is not a second task registry. Authority is located from the current
canonical event chain, never from a plan's supplied claims or Markdown rows.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import stat
import subprocess
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text


class WorkflowContractError(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = "WORKFLOW_" + code
        super().__init__(self.code + ": " + detail)


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def portable_path(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or ":" in value
        or "\0" in value
        or PurePosixPath(value).is_absolute()
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise WorkflowContractError("PATH", str(value))
    return value


_CUSTODY_KERNEL_LOCK = threading.Lock()
_CUSTODY_KERNEL: Any = None
# The genuine library factory. Tests inject race/observation wrappers by replacing ctypes.WinDLL;
# such a substitution must stay observable on every read, so the shared binding is only used
# while ctypes.WinDLL is this original object.
_GENUINE_WINDLL = getattr(ctypes, "WinDLL", None)


def _declare_custody_prototypes(api: Any) -> Any:
    from ctypes import wintypes as w

    api.CreateFileW.argtypes = [
        w.LPCWSTR,
        w.DWORD,
        w.DWORD,
        ctypes.c_void_p,
        w.DWORD,
        w.DWORD,
        w.HANDLE,
    ]
    api.CreateFileW.restype = w.HANDLE
    api.ReOpenFile.argtypes = [w.HANDLE, w.DWORD, w.DWORD, w.DWORD]
    api.ReOpenFile.restype = w.HANDLE
    api.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
    api.GetFinalPathNameByHandleW.restype = w.DWORD
    api.CloseHandle.argtypes = [w.HANDLE]
    api.CloseHandle.restype = w.BOOL
    api.GetFileInformationByHandleEx.argtypes = [
        w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD,
    ]
    api.GetFileInformationByHandleEx.restype = w.BOOL
    return api


def _custody_kernel() -> Any:
    """Bind kernel32 for the custody reader once per process (DEVX-018 O3-a).

    The reader used to build a new ctypes library object and re-declare every prototype on each
    file. The prototypes never change, and a bound function pointer is safe to call from several
    threads, so the declaration is done once under a lock and then only read. If ctypes.WinDLL has
    been replaced (race-injection tests), the original per-read binding is kept instead.
    """
    global _CUSTODY_KERNEL
    if ctypes.WinDLL is not _GENUINE_WINDLL:
        return _declare_custody_prototypes(ctypes.WinDLL("kernel32", use_last_error=True))
    api = _CUSTODY_KERNEL
    if api is None:
        with _CUSTODY_KERNEL_LOCK:
            api = _CUSTODY_KERNEL
            if api is None:
                api = _declare_custody_prototypes(
                    ctypes.WinDLL("kernel32", use_last_error=True)
                )
                _CUSTODY_KERNEL = api
    return api


def bounded_regular_bytes(
    path: Path,
    *,
    budget: int = 16 * 1024 * 1024,
    expected_identity: tuple[int, int] | None = None,
    expected_link_count: int = 1,
    verified_ancestors: set[Path] | None = None,
) -> bytes:
    """Read a frozen, caller-authorized name, rejecting reparse/replace aliases.

    Windows first opens metadata-only and verifies identity before requesting
    content access. ReOpenFile binds the read handle to that same object; the
    read handle denies write/delete and is rechecked before consuming bytes.
    Ancestor checks alone leave a check/open race. Opening is not task permission.
    """
    # Ordinary source/artifact callers retain the single-link default. A known
    # installed executable can have installer-created hardlinks, but a reader
    # must freeze both its physical identity and exact link count explicitly.
    # This is read-only custody, never write/alias permission for source capture.
    if (type(expected_link_count) is not int or expected_link_count < 1
            or (expected_link_count != 1 and expected_identity is None)):
        raise WorkflowContractError("ARTIFACT_LINK_IDENTITY")
    path = path.absolute()
    if type(budget) is not int or budget < 0:
        raise WorkflowContractError("ARTIFACT_BUDGET")
    if expected_identity is not None and (
        type(expected_identity) is not tuple
        or len(expected_identity) != 2
        or any(type(value) is not int or value < 0 for value in expected_identity)
    ):
        raise WorkflowContractError("ARTIFACT_IDENTITY")
    chain = (*reversed(path.parents), path)
    for position, entry in enumerate(chain):
        is_ancestor = position < len(chain) - 1
        # DEVX-018 O3-b: one caller-owned set shares ancestor checks inside a single identity
        # computation only. The leaf is always checked, and the final-path comparison after the
        # handle opens still rejects an ancestor that was redirected after its check.
        if is_ancestor and verified_ancestors is not None and entry in verified_ancestors:
            continue
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise WorkflowContractError("REPARSE_PATH", str(path))
        if is_ancestor and verified_ancestors is not None:
            verified_ancestors.add(entry)
    if os.name != "nt":
        raise WorkflowContractError("PLATFORM_UNSUPPORTED", "Windows file identity required")
    import msvcrt
    from ctypes import wintypes as w

    api = _custody_kernel()
    # No data access: a replaced protected file must not be opened for reading
    # merely to discover its identity. Metadata observation shares existing
    # readers/writers; the later read handle establishes deny-write/delete custody.
    handle = api.CreateFileW(str(path), 0, 7, None, 3, 0x200000, None)
    if handle == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), "frozen file open rejected", str(path))
    owned = True
    try:

        class AttributeTag(ctypes.Structure):
            _fields_ = [("attributes", w.DWORD), ("tag", w.DWORD)]

        attributes = AttributeTag()
        if not api.GetFileInformationByHandleEx(
            handle, 9, ctypes.byref(attributes), ctypes.sizeof(attributes)
        ):
            raise WorkflowContractError("HANDLE_ATTRIBUTES_UNPROVEN")
        if attributes.attributes & (0x400 | 0x10):
            raise WorkflowContractError("NOT_REGULAR_FILE")
        name = ctypes.create_unicode_buffer(32768)
        length = api.GetFinalPathNameByHandleW(handle, name, len(name), 0)
        if not length or length >= len(name):
            raise WorkflowContractError("HANDLE_PATH_UNPROVEN")
        observed = name.value
        if observed.startswith("\\\\?\\UNC\\"):
            observed = "\\\\" + observed[8:]
        elif observed.startswith("\\\\?\\"):
            observed = observed[4:]
        if os.path.normcase(observed) != os.path.normcase(str(path)):
            raise WorkflowContractError("HANDLE_PATH_CHANGED")
        descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDONLY | os.O_BINARY)
        owned = False
        with os.fdopen(descriptor, "rb") as stream:
            actual = os.fstat(stream.fileno())
            if (
                expected_identity is not None
                and (actual.st_dev, actual.st_ino) != expected_identity
            ):
                raise WorkflowContractError("HANDLE_EXPECTED_IDENTITY_CHANGED")
            if (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns) != (
                info.st_dev,
                info.st_ino,
                info.st_size,
                info.st_mtime_ns,
            ):
                raise WorkflowContractError("HANDLE_IDENTITY_CHANGED")
            if (
                not stat.S_ISREG(actual.st_mode)
                or actual.st_nlink != expected_link_count
                or getattr(actual, "st_file_attributes", 0) & 0x400
            ):
                raise WorkflowContractError("NOT_REGULAR_FILE")
            if actual.st_size > budget:
                raise WorkflowContractError("ARTIFACT_BUDGET")
            # Reopen the held object, never resolve the possibly replaced name
            # again. Keep metadata ownership until the read handle is closed.
            read_handle = api.ReOpenFile(handle, 0x80000000, 1, 0x200000)
            if read_handle == ctypes.c_void_p(-1).value:
                raise OSError(ctypes.get_last_error(), "frozen read access rejected", str(path))
            read_owned = True
            try:
                read_descriptor = msvcrt.open_osfhandle(
                    int(read_handle), os.O_RDONLY | os.O_BINARY
                )
                read_owned = False
                with os.fdopen(read_descriptor, "rb") as reader:
                    frozen = os.fstat(reader.fileno())
                    if (
                        frozen.st_dev, frozen.st_ino, frozen.st_size, frozen.st_mtime_ns
                    ) != (actual.st_dev, actual.st_ino, actual.st_size, actual.st_mtime_ns) or (
                        frozen.st_nlink != expected_link_count
                    ):
                        raise WorkflowContractError("HANDLE_IDENTITY_CHANGED")
                    content = reader.read(budget + 1)
                    if len(content) > budget:
                        raise WorkflowContractError("ARTIFACT_BUDGET")
                    return content
            finally:
                if read_owned and not api.CloseHandle(read_handle):
                    raise WorkflowContractError("HANDLE_CLOSE_UNCONFIRMED")
    finally:
        if owned and not api.CloseHandle(handle):
            raise WorkflowContractError("HANDLE_CLOSE_UNCONFIRMED")


def write_bound_once(
    root: Path,
    relative: str,
    content: bytes,
    *,
    expected_root_identity: tuple[int, int] | None = None,
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
    require_existing_parents: bool = False,
) -> None:
    """Create a new artifact only; an existing leaf is never overwritten."""
    _bound_file_io(
        root,
        relative,
        content,
        expected_root_identity=expected_root_identity,
        expected_parent_identities=expected_parent_identities,
        require_existing_parents=require_existing_parents,
    )


def apply_bound_file(
    root: Path,
    relative: str,
    before: bytes,
    after: bytes | None,
    *,
    expected_identity: tuple[int, int],
    expected_root_identity: tuple[int, int] | None = None,
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
) -> None:
    """Apply one held-file change bound to an installer's durable before-state.

    No namespace rename is needed. Partial writes require the caller's durable
    installation intent and recovery isolation; this primitive grants neither.
    New files still use write_bound_once, never an open-or-overwrite operation.
    """
    if (
        type(before) is not bytes
        or (after is not None and type(after) is not bytes)
        or type(expected_identity) is not tuple
        or len(expected_identity) != 2
        or any(type(value) is not int or value < 0 for value in expected_identity)
    ):
        raise WorkflowContractError("OUTPUT_EXPECTATION")
    _bound_file_io(
        root,
        relative,
        after,
        existing=(before, expected_identity),
        expected_root_identity=expected_root_identity,
        expected_parent_identities=expected_parent_identities,
    )


def create_bound_recoverable_file(
    root: Path,
    relative: str,
    content: bytes,
    *,
    record_created: Callable[[int], None],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
) -> None:
    """Keep a new file delete-on-close until its held identity is recorded.

    The callback receives the real open descriptor and must durably bind its
    identity to the caller's original authority before returning. This primitive
    grants no authority. Existing parents are required: creating unrecorded
    directories would introduce another crash window. Ordinary artifact writers
    retain their separate write-once semantics.
    """
    if not callable(record_created):
        raise WorkflowContractError("OUTPUT_CREATION_RECORDER")
    if expected_root_identity is None:
        raise WorkflowContractError("OUTPUT_ROOT_IDENTITY_REQUIRED")
    _bound_file_io(
        root,
        relative,
        content,
        expected_root_identity=expected_root_identity,
        record_created=record_created,
        expected_parent_identities=expected_parent_identities,
    )


def create_bound_recoverable_directory(
    root: Path,
    relative: str,
    *,
    record_created: Callable[[int], None],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
) -> None:
    """Create one empty directory with the same durable held-object boundary."""
    if not callable(record_created):
        raise WorkflowContractError("OUTPUT_CREATION_RECORDER")
    if expected_root_identity is None:
        raise WorkflowContractError("OUTPUT_ROOT_IDENTITY_REQUIRED")
    _bound_file_io(
        root,
        relative,
        b"",
        expected_root_identity=expected_root_identity,
        record_created=record_created,
        directory_leaf=True,
        expected_parent_identities=expected_parent_identities,
    )


def remove_bound_empty_directory(
    root: Path,
    relative: str,
    *,
    expected_identity: tuple[int, int],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
) -> None:
    """Remove only the exact empty object; never recursively delete children."""
    if expected_root_identity is None or expected_identity is None:
        raise WorkflowContractError("OUTPUT_ROOT_IDENTITY_REQUIRED")
    _bound_file_io(
        root,
        relative,
        None,
        existing=(b"", expected_identity),
        expected_root_identity=expected_root_identity,
        directory_leaf=True,
        expected_parent_identities=expected_parent_identities,
    )


def verify_bound_directory(
    root: Path,
    relative: str,
    *,
    expected_identity: tuple[int, int],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]],
) -> None:
    """Observe exact opened directory/ancestors without reading children or writing."""
    _bound_file_io(
        root,
        relative,
        b"",
        existing=(b"", expected_identity),
        expected_root_identity=expected_root_identity,
        directory_leaf=True,
        expected_parent_identities=expected_parent_identities,
    )


class _BoundDirectoryCustody:
    """Owned native directory handles, not serialized write authority."""

    def __init__(self) -> None:
        if os.name != "nt":
            raise WorkflowContractError("PLATFORM_UNSUPPORTED")
        from ctypes import wintypes as w

        self._api = ctypes.WinDLL("kernel32", use_last_error=True)
        self._api.GetCurrentProcess.argtypes = []
        self._api.GetCurrentProcess.restype = w.HANDLE
        self._api.DuplicateHandle.argtypes = [
            w.HANDLE, w.HANDLE, w.HANDLE, ctypes.POINTER(w.HANDLE), w.DWORD, w.BOOL, w.DWORD,
        ]
        self._api.DuplicateHandle.restype = w.BOOL
        self._api.CloseHandle.argtypes = [w.HANDLE]
        self._api.CloseHandle.restype = w.BOOL
        self._handles: list[int] = []
        self._owner_pid = os.getpid()
        self._closed = False

    def _close_handles(self, handles: list[int]) -> None:
        failures = [handle for handle in reversed(handles) if not self._api.CloseHandle(handle)]
        if failures:
            raise WorkflowContractError("DIRECTORY_CUSTODY_CLOSE_UNCONFIRMED")

    def _duplicate(self, originals: tuple[int, ...], *, inherit: bool) -> list[int]:
        from ctypes import wintypes as w

        if self._closed or os.getpid() != self._owner_pid:
            raise WorkflowContractError("DIRECTORY_CUSTODY_OWNER")
        current = self._api.GetCurrentProcess()
        copies: list[int] = []
        try:
            for original in originals:
                copied = w.HANDLE()
                if not self._api.DuplicateHandle(
                    current, original, current, ctypes.byref(copied), 0, inherit, 2,
                ):
                    raise OSError(ctypes.get_last_error(), "directory handle duplication rejected")
                if copied.value is None:
                    raise WorkflowContractError("DIRECTORY_CUSTODY_NULL_HANDLE")
                copies.append(copied.value)
        except BaseException:
            self._close_handles(copies)
            raise
        return copies

    def _retain(self, originals: tuple[int, ...]) -> None:
        if self._handles or not originals:
            raise WorkflowContractError("DIRECTORY_CUSTODY_STATE")
        self._handles = self._duplicate(originals, inherit=False)

    @contextmanager
    def subprocess_inheritance(self) -> Iterator[subprocess.STARTUPINFO]:
        """Explicit allowlist only; inherited copies protect a surviving child.

        Use with Popen(close_fds=True). This does not create a process, Job or
        execution permission. The original worker must separately prove its Job.
        """
        if not self._handles:
            raise WorkflowContractError("DIRECTORY_CUSTODY_STATE")
        copies = self._duplicate(tuple(self._handles), inherit=True)
        try:
            yield subprocess.STARTUPINFO(lpAttributeList={"handle_list": copies})
        finally:
            self._close_handles(copies)

    def close(self) -> None:
        if self._closed:
            return
        if os.getpid() != self._owner_pid:
            raise WorkflowContractError("DIRECTORY_CUSTODY_OWNER")
        handles, self._handles = self._handles, []
        self._closed = True
        self._close_handles(handles)


class _BoundReadFileCustody(_BoundDirectoryCustody):
    """Verified read-only file plus parent-chain handles; no write/lease authority."""

    def __init__(self, binding: Mapping[str, Any]) -> None:
        super().__init__()
        self._binding = json.loads(json.dumps(binding))

    def binding(self) -> dict[str, Any]:
        if self._closed or os.getpid() != self._owner_pid or not self._handles:
            raise WorkflowContractError("READ_FILE_CUSTODY_OWNER")
        if self._binding["schema_version"] == "workflow_read_file_custody.v2":
            from ctypes import wintypes as w

            class FileStandardInfo(ctypes.Structure):
                _fields_ = [("allocation", ctypes.c_int64), ("size", ctypes.c_int64),
                            ("links", w.DWORD), ("delete_pending", w.BOOLEAN),
                            ("directory", w.BOOLEAN)]

            query = self._api.GetFileInformationByHandleEx
            query.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
            query.restype = w.BOOL
            info = FileStandardInfo()
            if not query(self._handles[-1], 1, ctypes.byref(info), ctypes.sizeof(info)):
                raise WorkflowContractError("READ_FILE_CUSTODY_LINKS_UNPROVEN")
            if (info.links != self._binding["link_count"] or info.delete_pending or info.directory
                    or info.size != self._binding["size_bytes"]):
                raise WorkflowContractError("READ_FILE_CUSTODY_LINKS_CHANGED")
        return dict(json.loads(json.dumps(self._binding)))


@contextmanager
def hold_bound_read_file(
    root: Path, relative: str, *, expected: bytes, expected_identity: tuple[int, int],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]],
    budget: int = 16 * 1024 * 1024,
    expected_link_count: int = 1,
    allow_parent_updates: bool = False,
) -> Iterator[_BoundReadFileCustody]:
    """Pin existing bytes; optionally allow sibling writes, never leaf mutation.

    Publication receipt replacement needs write sharing on parent directories.
    The pinned leaf still denies write/delete and every ancestor denies delete.
    """
    if (type(budget) is not int or not 0 <= budget <= 64 * 1024 * 1024
            or type(expected) is not bytes or len(expected) > budget
            or type(expected_identity) is not tuple or len(expected_identity) != 2
            or any(type(item) is not int or item < 0 for item in expected_identity)
            or type(expected_link_count) is not int or expected_link_count < 1):
        raise WorkflowContractError("READ_FILE_CUSTODY_EXPECTATION")
    relative = portable_path(relative)
    binding = {
        "schema_version": "workflow_read_file_custody.v1",
        "root": root.absolute().as_posix(), "relative": relative,
        "root_identity": expected_root_identity, "parent_identities": expected_parent_identities,
        "identity": expected_identity, "size_bytes": len(expected),
        "sha256": hashlib.sha256(expected).hexdigest(),
    }
    if expected_link_count != 1:
        binding.update(schema_version="workflow_read_file_custody.v2",
                       link_count=expected_link_count)
    custody = _BoundReadFileCustody(binding)
    try:
        _bound_file_io(
            root, relative, expected, existing=(expected, expected_identity),
            expected_root_identity=expected_root_identity,
            expected_parent_identities=expected_parent_identities,
            retain_read_file_handles=custody._retain,
            read_file_link_count=expected_link_count,
            share_directory_writes=allow_parent_updates,
        )
        if expected_link_count != 1:
            custody.binding()
        yield custody
        if expected_link_count != 1 and not custody._closed:
            custody.binding()
    finally:
        custody.close()


@contextmanager
def hold_bound_directory(
    root: Path,
    relative: str,
    *,
    expected_identity: tuple[int, int],
    expected_root_identity: tuple[int, int],
    expected_parent_identities: Mapping[str, tuple[int, int]],
    allow_child_updates: bool = False,
) -> Iterator[_BoundDirectoryCustody]:
    """Retain the original verified directory chain without reading children.

    Duplicate verified handles before the existing native opener returns; never
    reopen names after checking. Copies preserve deny-delete sharing until the
    context and any explicitly inheriting child release their own handles.
    The explicit publication mode shares directory write access so Git can
    commit child names; it still denies delete/rename of the pinned directory.
    Ordinary file/input custody and the default directory mode stay unchanged.
    """
    custody = _BoundDirectoryCustody()
    try:
        _bound_file_io(
            root, relative, b"", existing=(b"", expected_identity),
            expected_root_identity=expected_root_identity, directory_leaf=True,
            expected_parent_identities=expected_parent_identities,
            retain_directory_handles=custody._retain,
            share_directory_writes=allow_child_updates,
        )
        yield custody
    finally:
        custody.close()


def _bound_file_io(
    root: Path,
    relative: str,
    content: bytes | None,
    *,
    existing: tuple[bytes, tuple[int, int]] | None = None,
    expected_root_identity: tuple[int, int] | None = None,
    record_created: Callable[[int], None] | None = None,
    directory_leaf: bool = False,
    expected_parent_identities: Mapping[str, tuple[int, int]] | None = None,
    require_existing_parents: bool = False,
    retain_directory_handles: Callable[[tuple[int, ...]], None] | None = None,
    retain_read_file_handles: Callable[[tuple[int, ...]], None] | None = None,
    read_file_link_count: int = 1,
    share_directory_writes: bool = False,
) -> None:
    """Create one artifact beneath held, non-reparse directory objects.

    RootDirectory-relative NtCreateFile avoids a check/absolute-open junction
    race. Ordinary directory handles deny write/delete until the file is flushed.
    Explicit publication custody can share directory writes, never deletes.
    This is an I/O primitive, not task permission or an atomic installation.
    """
    if os.name != "nt":
        raise WorkflowContractError("PLATFORM_UNSUPPORTED")
    if (type(share_directory_writes) is not bool
            or (share_directory_writes and retain_directory_handles is None
                and retain_read_file_handles is None)):
        raise WorkflowContractError("DIRECTORY_CUSTODY_SHARE_MODE")
    if (type(read_file_link_count) is not int or read_file_link_count < 1
            or (read_file_link_count != 1 and retain_read_file_handles is None)):
        raise WorkflowContractError("READ_FILE_CUSTODY_BINDING")
    if retain_directory_handles is not None and (
        not callable(retain_directory_handles) or not directory_leaf or existing is None
        or existing[0] != b"" or content != b"" or record_created is not None
        or expected_root_identity is None or expected_parent_identities is None
    ):
        raise WorkflowContractError("DIRECTORY_CUSTODY_BINDING")
    if retain_read_file_handles is not None and (
        not callable(retain_read_file_handles) or directory_leaf or existing is None
        or content != existing[0] or record_created is not None
        or expected_root_identity is None or expected_parent_identities is None
        or retain_directory_handles is not None
    ):
        raise WorkflowContractError("READ_FILE_CUSTODY_BINDING")
    relative = portable_path(relative)
    parts = relative.split("/")
    if expected_parent_identities is not None and (
        set(expected_parent_identities)
        != {"/".join(parts[:index]) for index in range(1, len(parts))}
        or any(
            type(value) is not tuple
            or len(value) != 2
            or any(type(item) is not int or item < 0 for item in value)
            for value in expected_parent_identities.values()
        )
    ):
        raise WorkflowContractError("OUTPUT_PARENT_IDENTITIES")
    devices = {"con", "prn", "aux", "nul"} | {
        prefix + str(number) for prefix in ("com", "lpt") for number in range(1, 10)
    }
    if any(
        part != part.rstrip(". ")
        or part.split(".")[0].casefold() in devices
        or len(part.encode("utf-16-le")) > 65532
        for part in parts
    ) or (type(content) is not bytes and not (content is None and existing is not None)):
        raise WorkflowContractError("OUTPUT_PATH_OR_BYTES")
    import msvcrt
    from ctypes import wintypes as w

    class UnicodeString(ctypes.Structure):
        _fields_ = [("Length", w.USHORT), ("MaximumLength", w.USHORT), ("Buffer", w.LPWSTR)]

    class ObjectAttributes(ctypes.Structure):
        _fields_ = [
            ("Length", w.ULONG),
            ("RootDirectory", w.HANDLE),
            ("ObjectName", ctypes.POINTER(UnicodeString)),
            ("Attributes", w.ULONG),
            ("SecurityDescriptor", ctypes.c_void_p),
            ("SecurityQualityOfService", ctypes.c_void_p),
        ]

    class IoStatus(ctypes.Structure):
        _fields_ = [("StatusOrPointer", ctypes.c_void_p), ("Information", ctypes.c_size_t)]

    class FileInformation(ctypes.Structure):
        _fields_ = [
            ("attributes", w.DWORD),
            ("creation", w.FILETIME),
            ("access", w.FILETIME),
            ("write", w.FILETIME),
            ("volume", w.DWORD),
            ("size_high", w.DWORD),
            ("size_low", w.DWORD),
            ("links", w.DWORD),
            ("index_high", w.DWORD),
            ("index_low", w.DWORD),
        ]

    api = ctypes.WinDLL("kernel32", use_last_error=True)
    native = ctypes.WinDLL("ntdll")
    api.CreateFileW.argtypes = [
        w.LPCWSTR,
        w.DWORD,
        w.DWORD,
        ctypes.c_void_p,
        w.DWORD,
        w.DWORD,
        w.HANDLE,
    ]
    api.CreateFileW.restype = w.HANDLE
    api.CloseHandle.argtypes = [w.HANDLE]
    api.CloseHandle.restype = w.BOOL
    api.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
    api.GetFinalPathNameByHandleW.restype = w.DWORD
    api.GetFileInformationByHandle.argtypes = [w.HANDLE, ctypes.POINTER(FileInformation)]
    api.GetFileInformationByHandle.restype = w.BOOL
    api.SetFileInformationByHandle.argtypes = [w.HANDLE, w.INT, ctypes.c_void_p, w.DWORD]
    api.SetFileInformationByHandle.restype = w.BOOL
    native.NtCreateFile.argtypes = [
        ctypes.POINTER(w.HANDLE),
        w.DWORD,
        ctypes.POINTER(ObjectAttributes),
        ctypes.POINTER(IoStatus),
        ctypes.c_void_p,
        w.ULONG,
        w.ULONG,
        w.ULONG,
        w.ULONG,
        ctypes.c_void_p,
        w.ULONG,
    ]
    native.NtCreateFile.restype = w.LONG
    native.RtlNtStatusToDosError.argtypes = [w.LONG]
    native.RtlNtStatusToDosError.restype = w.ULONG
    handles = []

    def directory(handle: Any) -> FileInformation:
        info = FileInformation()
        if not api.GetFileInformationByHandle(handle, ctypes.byref(info)):
            raise WorkflowContractError("OUTPUT_HANDLE_UNPROVEN")
        if info.attributes & 0x400 or not info.attributes & 0x10:
            raise WorkflowContractError("OUTPUT_DIRECTORY_REPARSE")
        return info

    def create(parent: Any, name: str, *, is_directory: bool, leaf_directory: bool = False) -> Any:
        buffer = ctypes.create_unicode_buffer(name)
        length = len(name.encode("utf-16-le"))
        text = UnicodeString(length, length + 2, ctypes.cast(buffer, w.LPWSTR))
        attributes = ObjectAttributes(
            ctypes.sizeof(ObjectAttributes),
            parent,
            ctypes.pointer(text),
            0x40,
            None,
            None,
        )
        handle, status = w.HANDLE(), IoStatus()
        # Existing-file application opens only: a missing parent must not be
        # created as a side effect of a rejected before-state.
        code = native.NtCreateFile(
            ctypes.byref(handle),
            0x120089
            if retain_read_file_handles is not None
            or (is_directory and retain_directory_handles is not None)
            else 0x13019F
            if leaf_directory
            else 0x120089
            if is_directory
            # In-place content updates need read/write, never DELETE. Keep
            # DELETE for deletion and delete-on-close recoverable creation.
            else 0x12019F
            if existing is not None and content is not None
            else 0x13019F
            if existing is not None or record_created is not None
            else 0x120116,
            ctypes.byref(attributes),
            ctypes.byref(status),
            None,
            0x80,
            3 if is_directory and share_directory_writes else
            1 if is_directory or retain_read_file_handles is not None
            or (existing is None and record_created is None) else 0,
            1
            if existing is not None
            or (is_directory and record_created is not None and not leaf_directory)
            or (is_directory and not leaf_directory and expected_parent_identities is not None)
            or (is_directory and not leaf_directory and require_existing_parents)
            else 3
            if is_directory and not leaf_directory
            else 2,
            0x200020
            | (1 if is_directory else 0x40)
            | (
                0x1000 if (not is_directory or leaf_directory) and record_created is not None else 0
            ),
            None,
            0,
        )
        if code < 0:
            raise OSError(native.RtlNtStatusToDosError(code), "bound output create rejected")
        handles.append(handle)
        if is_directory:
            directory(handle)
        return handle

    root = root.absolute()
    before = root.lstat()
    if expected_root_identity is not None and (
        type(expected_root_identity) is not tuple
        or len(expected_root_identity) != 2
        or any(type(value) is not int or value < 0 for value in expected_root_identity)
        or (before.st_dev, before.st_ino) != expected_root_identity
    ):
        raise WorkflowContractError("OUTPUT_ROOT_IDENTITY_CHANGED")
    try:
        handle = api.CreateFileW(
            str(root), 0x80000000, 3 if share_directory_writes else 1,
            None, 3, 0x02200000, None,
        )
        if handle == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_last_error(), "output root open rejected")
        handles.append(handle)
        info = directory(handle)
        final = ctypes.create_unicode_buffer(32768)
        length = api.GetFinalPathNameByHandleW(handle, final, len(final), 0)
        name = final.value
        if name.startswith("\\\\?\\UNC\\"):
            name = "\\\\" + name[8:]
        elif name.startswith("\\\\?\\"):
            name = name[4:]
        if (
            not length
            or length >= len(final)
            or os.path.normcase(name) != os.path.normcase(str(root))
            or (info.index_high << 32) | info.index_low != before.st_ino
            or info.volume != before.st_dev
            or not stat.S_ISDIR(before.st_mode)
        ):
            raise WorkflowContractError("OUTPUT_ROOT_CHANGED")
        for position, part in enumerate(parts[:-1], 1):
            handle = create(handle, part, is_directory=True)
            if expected_parent_identities is not None:
                opened = directory(handle)
                if (
                    opened.volume,
                    (opened.index_high << 32) | opened.index_low,
                ) != expected_parent_identities["/".join(parts[:position])]:
                    raise WorkflowContractError("OUTPUT_PARENT_IDENTITY_CHANGED")
        leaf = create(handle, parts[-1], is_directory=directory_leaf, leaf_directory=directory_leaf)
        if directory_leaf:
            descriptor = msvcrt.open_osfhandle(int(leaf.value), os.O_RDONLY | os.O_BINARY)
            handles.pop()
            try:
                actual = os.fstat(descriptor)
                if record_created is not None:
                    record_created(descriptor)
                    recorded = os.fstat(descriptor)
                    if (recorded.st_dev, recorded.st_ino) != (actual.st_dev, actual.st_ino):
                        raise WorkflowContractError("OUTPUT_CREATED_HANDLE_CHANGED")
                    flags = w.DWORD(8)
                    if not api.SetFileInformationByHandle(leaf, 21, ctypes.byref(flags), 4):
                        raise OSError(
                            ctypes.get_last_error(), "bound creation disposition rejected"
                        )
                else:
                    if existing is None or (actual.st_dev, actual.st_ino) != existing[1]:
                        raise WorkflowContractError("OUTPUT_FILE_IDENTITY")
                    if content is None:
                        delete = w.BOOLEAN(True)
                        if not api.SetFileInformationByHandle(leaf, 4, ctypes.byref(delete), 1):
                            raise OSError(
                                ctypes.get_last_error(), "bound directory disposition rejected"
                            )
                    if retain_directory_handles is not None:
                        originals: list[int] = []
                        for item in handles:
                            if isinstance(item, int):
                                originals.append(item)
                            elif isinstance(item, w.HANDLE) and item.value is not None:
                                originals.append(item.value)
                            else:
                                raise WorkflowContractError("DIRECTORY_CUSTODY_NULL_HANDLE")
                        originals.append(msvcrt.get_osfhandle(descriptor))
                        retain_directory_handles(tuple(originals))
            finally:
                os.close(descriptor)
            return
        if existing is not None or record_created is not None:
            info = FileInformation()
            if not api.GetFileInformationByHandle(leaf, ctypes.byref(info)):
                raise WorkflowContractError("OUTPUT_HANDLE_UNPROVEN")
            if info.attributes & (0x400 | 0x10) or info.links != read_file_link_count:
                raise WorkflowContractError("OUTPUT_FILE_TYPE_OR_LINKS")
        descriptor = msvcrt.open_osfhandle(
            int(leaf.value),
            (os.O_RDONLY if retain_read_file_handles is not None else os.O_RDWR
             if existing is not None or record_created is not None else os.O_WRONLY)
            | os.O_BINARY,
        )
        handles.pop()  # The descriptor now owns the exact leaf handle.
        try:
            stream = os.fdopen(
                descriptor, "rb" if retain_read_file_handles is not None else "r+b"
                if existing is not None or record_created is not None else "wb"
            )
        except BaseException:
            os.close(descriptor)
            raise
        with stream:
            if record_created is not None:
                created = os.fstat(stream.fileno())
                if created.st_size != 0:
                    raise WorkflowContractError("OUTPUT_CREATED_NOT_EMPTY")
                record_created(stream.fileno())
                recorded = os.fstat(stream.fileno())
                if (recorded.st_dev, recorded.st_ino, recorded.st_size) != (
                    created.st_dev,
                    created.st_ino,
                    0,
                ):
                    raise WorkflowContractError("OUTPUT_CREATED_HANDLE_CHANGED")
                # The recorder may inspect the fd using seek-based APIs. Its
                # cursor is not permission to introduce holes in the payload.
                stream.seek(0)
                # FILE_DISPOSITION_ON_CLOSE without DELETE clears the flag on
                # this exact handle. Ordinary FileDispositionInfo(FALSE) does
                # not cancel a create-time FILE_DELETE_ON_CLOSE.
                flags = w.DWORD(8)
                if not api.SetFileInformationByHandle(leaf, 21, ctypes.byref(flags), 4):
                    raise OSError(ctypes.get_last_error(), "bound creation disposition rejected")
            if existing is not None:
                expected, identity = existing
                actual = os.fstat(stream.fileno())
                if (actual.st_dev, actual.st_ino) != identity:
                    raise WorkflowContractError("OUTPUT_FILE_IDENTITY")
                if stream.read(len(expected) + 1) != expected:
                    raise WorkflowContractError("OUTPUT_FILE_CONTENT_CHANGED")
                if retain_read_file_handles is not None:
                    frozen = os.fstat(stream.fileno())
                    if frozen.st_nlink != read_file_link_count or frozen.st_size != len(expected):
                        raise WorkflowContractError("READ_FILE_CUSTODY_LINKS_CHANGED")
                    read_originals: list[int] = []
                    for item in handles:
                        if isinstance(item, int):
                            read_originals.append(item)
                        elif isinstance(item, w.HANDLE) and item.value is not None:
                            read_originals.append(item.value)
                        else:
                            raise WorkflowContractError("READ_FILE_CUSTODY_NULL_HANDLE")
                    read_originals.append(msvcrt.get_osfhandle(stream.fileno()))
                    retain_read_file_handles(tuple(read_originals))
                    return  # No write, truncate, flush, delete, or pathname reopen.
                if content is None:
                    delete = w.BOOLEAN(True)
                    if not api.SetFileInformationByHandle(leaf, 4, ctypes.byref(delete), 1):
                        raise OSError(ctypes.get_last_error(), "bound disposition rejected")
                else:
                    stream.seek(0)
                    stream.write(content)
                    stream.truncate()
                    stream.flush()
                    os.fsync(stream.fileno())
                    stream.seek(0)
                    if stream.read(len(content) + 1) != content:
                        raise WorkflowContractError("OUTPUT_FILE_WRITE_UNCONFIRMED")
            else:
                assert content is not None
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
                if record_created is not None:
                    stream.seek(0)
                    if stream.read(len(content) + 1) != content:
                        raise WorkflowContractError("OUTPUT_FILE_WRITE_UNCONFIRMED")
    finally:
        failures = [handle for handle in reversed(handles) if not api.CloseHandle(handle)]
        if failures:
            raise WorkflowContractError("OUTPUT_HANDLE_CLOSE_UNCONFIRMED")


def read_bound_json(root: Path, reference: Mapping[str, Any]) -> dict[str, Any]:
    if set(reference) != {"path", "sha256"}:
        raise WorkflowContractError("REFERENCE_FIELDS")
    path = portable_path(reference["path"])
    content = bounded_regular_bytes(root / path)
    if hashlib.sha256(content).hexdigest() != reference["sha256"]:
        raise WorkflowContractError("REFERENCE_DRIFT", path)
    payload = load_strict_json_text(content.decode("utf-8"))
    if not isinstance(payload, dict):
        raise WorkflowContractError("REFERENCE_OBJECT", path)
    return payload


def validate_task_authority(value: Any, *, task_id: str) -> dict[str, Any]:
    keys = {"schema_version", "task_id", "decision_id", "status", "scope_ref", "review_ref"}
    if not isinstance(value, dict) or set(value) != keys:
        raise WorkflowContractError("TASK_AUTHORITY_FIELDS")
    if value["schema_version"] != "workflow_task_authority.v1" or value["task_id"] != task_id:
        raise WorkflowContractError("TASK_AUTHORITY_IDENTITY")
    if value["status"] not in {"ACTIVE", "REVOKED"}:
        raise WorkflowContractError("TASK_AUTHORITY_STATUS")
    if not isinstance(value["decision_id"], str) or not value["decision_id"].startswith(
        "owner_decision:"
    ):
        raise WorkflowContractError("TASK_AUTHORITY_DECISION")
    ref = value["scope_ref"]
    if not isinstance(ref, dict) or set(ref) != {"path", "sha256"}:
        raise WorkflowContractError("TASK_AUTHORITY_REFERENCE")
    path = portable_path(ref["path"])
    if not path.startswith("config/architecture/") or not path.endswith(".json"):
        raise WorkflowContractError("TASK_AUTHORITY_SCOPE_PATH")
    if not isinstance(ref["sha256"], str) or not re.fullmatch("[a-f0-9]{64}", ref["sha256"]):
        raise WorkflowContractError("TASK_AUTHORITY_SCOPE_HASH")
    review = value["review_ref"]
    if review is not None:
        if not isinstance(review, dict) or set(review) != {"path", "sha256"}:
            raise WorkflowContractError("TASK_AUTHORITY_REVIEW")
        if not portable_path(review["path"]).startswith("outputs/architecture/"):
            raise WorkflowContractError("TASK_AUTHORITY_REVIEW_PATH")
        if not isinstance(review["sha256"], str) or not re.fullmatch(
            "[a-f0-9]{64}", review["sha256"]
        ):
            raise WorkflowContractError("TASK_AUTHORITY_REVIEW_HASH")
    return dict(json.loads(json.dumps(value)))


def repository_identity(root: Path) -> dict[str, str]:
    root = root.resolve()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_OPTIONAL_LOCKS="0"
    )

    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            check=True,
            text=True,
            env=environment,
            timeout=30,
        )
        return result.stdout.strip()

    top = Path(git("rev-parse", "--show-toplevel")).resolve()
    common = Path(git("rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
    origin = git("remote", "get-url", "origin")
    if top != root or origin not in {
        "git@github.com:AI-Trading-Collaboration/AITradingSystem.git",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem.git",
        "https://github.com/AI-Trading-Collaboration/AITradingSystem",
        "ssh://git@github.com/AI-Trading-Collaboration/AITradingSystem.git",
    }:
        raise WorkflowContractError("REPOSITORY_IDENTITY")
    for path in (
        "docs/requirements/DEVX-002_Governed_Development_Workflow_Skill.md",
        "scripts/architecture_arch005_checkout_guard.py",
    ):
        if not (root / path).is_file():
            raise WorkflowContractError("REPOSITORY_SENTINEL")
    return {"checkout": root.as_posix(), "common": common.as_posix(), "origin": origin}


def load_current_task_authority(root: Path, task_id: str) -> dict[str, Any]:
    return _load_current_task_authority(root, task_id, inventory_freshness=True)


def load_current_task_structure(root: Path, task_id: str) -> dict[str, Any]:
    """Bootstrap read admission without scanning unapproved source files.

    Checks canonical policy/index/fragments/events/templates/views and bound
    authority, but is explicitly NOT an execution/freshness approval. The caller
    must establish read admission and then run load_current_task_authority.
    """
    result = _load_current_task_authority(root, task_id, inventory_freshness=False)
    return {**result, "consumer_inventory_checked": False}


def _load_current_task_authority(
    root: Path, task_id: str, *, inventory_freshness: bool
) -> dict[str, Any]:
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        validate_canonical_registry,
    )

    identity = repository_identity(root)
    registry = validate_canonical_registry(
        project_root=root,
        require_consumer_cutover=False,
        require_inventory_freshness=inventory_freshness,
    )
    fragment = registry.fragment(task_id)  # Exact equality; never Markdown prefix matching.
    if fragment["projection"]["terminal"]:
        raise WorkflowContractError("TASK_TERMINAL", task_id)
    authority = validate_task_authority(
        fragment["task_record"].get("workflow_authority"), task_id=task_id
    )
    if authority["status"] != "ACTIVE":
        raise WorkflowContractError("TASK_AUTHORITY_REVOKED", task_id)
    event = next(
        (row for row in reversed(fragment["events"]) if "workflow_authority" in row["payload"]),
        None,
    )
    if event is None or event["payload"]["workflow_authority"] != authority:
        raise WorkflowContractError("TASK_AUTHORITY_EVENT")
    scope = read_bound_json(root, authority["scope_ref"])
    if scope.get("task_id") != task_id or scope.get("decision_id") != authority["decision_id"]:
        raise WorkflowContractError("TASK_SCOPE_IDENTITY")
    return {
        "repository": identity,
        "task_id": task_id,
        "authority": authority,
        "authority_event_id": event["event_id"],
        "current_event_id": fragment["last_event_id"],
        "scope": scope,
        "authority_sha256": canonical_digest(authority),
    }
