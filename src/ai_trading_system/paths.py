"""Code root and state root.

The code root is the checkout or release directory that holds src/, config/, docs/ and scripts/.
The state root holds the state directories data/, outputs/ and artifacts/. It is the directory
named by AITS_STATE_ROOT when that is set, else the code root, which is the layout every
checkout used before this module existed.

Relative path strings in config files and reports keep their form ("outputs/reports/x.md"): a
path whose first part is a state directory resolves under the state root, any other path under
the code root. A root other than the code root (a test's temporary directory, a replay mirror)
keeps everything under itself, so a caller that passes its own root sees no change. With
AITS_STATE_ROOT unset every helper returns exactly what the plain pathlib expression returned.

Code outside this module does not build a state path from a root by hand; tests/test_paths.py
enforces that.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import TypeVar

STATE_DIRS = ("data", "outputs", "artifacts")
STATE_ROOT_ENV = "AITS_STATE_ROOT"
CODE_ROOT = Path(__file__).resolve().parents[2]

_P = TypeVar("_P", bound=PurePath)


def state_root() -> Path:
    """The directory that holds data/, outputs/ and artifacts/."""
    value = os.environ.get(STATE_ROOT_ENV, "").strip()
    if not value:
        return CODE_ROOT
    return _absolute_state_root(value)


@lru_cache(maxsize=8)
def _absolute_state_root(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{STATE_ROOT_ENV} must be an absolute path, got {value!r}")
    return path.resolve()


def _separate_state_root() -> Path | None:
    current = state_root()
    return None if current == CODE_ROOT else current


def is_code_root(root: PurePath | str) -> bool:
    """Whether root is this checkout's code root, the only root whose state is redirected."""
    if root == CODE_ROOT:
        return True
    return _resolves_to_code_root(str(root))


@lru_cache(maxsize=256)
def _resolves_to_code_root(text: str) -> bool:
    try:
        return Path(text).resolve() == CODE_ROOT
    except (OSError, RuntimeError):
        return False


def _first_part(rel: PurePath | str) -> str | None:
    pure = PurePath(rel)
    if pure.is_absolute() or not pure.parts:
        return None
    return pure.parts[0]


def is_state_relative(rel: PurePath | str) -> bool:
    """Whether a relative path starts with a state directory (data/, outputs/, artifacts/)."""
    return _first_part(rel) in STATE_DIRS


def state_dir(root: _P, *parts: str) -> _P | Path:
    """root.joinpath(*parts), where parts start with a state directory; under the state root
    when root is the code root."""
    separate = _separate_state_root()
    if separate is not None and is_code_root(root):
        return separate.joinpath(*parts)
    return root.joinpath(*parts)


def join_root(root: _P, rel: PurePath | str) -> _P | Path:
    """root / rel; a relative rel that starts with a state directory comes from the state root
    when root is the code root."""
    separate = _separate_state_root()
    if separate is not None and is_state_relative(rel) and is_code_root(root):
        return separate / rel
    return root / rel


def contained_root(root: Path, relative_path: str) -> Path:
    """The root a root-bound reader or writer should use for relative_path: the state root for a
    state path under the code root, else root itself."""
    separate = _separate_state_root()
    if separate is None or not isinstance(relative_path, str):
        return root
    if is_state_relative(relative_path) and is_code_root(root):
        return separate
    return root


def _stored_pure(text: str) -> PurePath:
    if re.match(r"^(?:[A-Za-z]:[\\/]|\\\\)", text):
        return PureWindowsPath(text)
    if text.startswith("/"):
        return PurePosixPath(text)
    return PurePath(text)


def _is_under(path: PurePath, root: PurePath | str) -> bool:
    parts = path.parts
    root_parts = type(path)(str(root)).parts
    if isinstance(path, PureWindowsPath):
        parts = tuple(part.lower() for part in parts)
        root_parts = tuple(part.lower() for part in root_parts)
    return parts[: len(root_parts)] == root_parts


def stored_state_path(stored: PurePath | str, *roots: PurePath | str) -> Path | None:
    """Resolve a recorded location, not a read instruction, into the current state.

    Strip whitespace and return None for empty text. Parse Windows drive and UNC paths as
    Windows paths, slash-rooted paths as POSIX paths, and others in the native flavour.
    Relative paths use join_root(CODE_ROOT, text). Absolute paths under the state root,
    code root, or a caller's trusted root are kept as written, without resolve(); containment
    compares whole parts (case-insensitively for Windows), including the root itself.
    Otherwise rebase from the first data/outputs/artifacts part onto the state root, lowering
    that anchor only; return None if there is no state-directory anchor.
    """
    text = str(stored).strip()
    if not text:
        return None
    pure = _stored_pure(text)
    if not pure.is_absolute():
        return join_root(CODE_ROOT, text)
    for root in (state_root(), CODE_ROOT, *roots):
        if _is_under(pure, root):
            return Path(text)
    for index, part in enumerate(pure.parts[1:], start=1):
        if part.lower() in STATE_DIRS:
            return state_root().joinpath(part.lower(), *pure.parts[index + 1 :])
    return None


def cli_path(value: PurePath | str) -> Path:
    """Path(value) for a path given on the command line: a relative state path means the file
    under a separate state root; anything else is left as given (relative to the working
    directory)."""
    path = Path(value)
    separate = _separate_state_root()
    if separate is not None and is_state_relative(path):
        return separate / path
    return path


def state_args(args: list[str]) -> list[str]:
    """Command-line arguments with relative state paths made absolute under a separate state
    root, so "data/raw/x.csv" means the same file whatever the working directory is. Unchanged
    when the state root is the code root."""
    separate = _separate_state_root()
    if separate is None:
        return list(args)
    converted = []
    for arg in args:
        if arg.startswith("--"):
            option, sep, value = arg.partition("=")
            if sep and _looks_like_state_path(value):
                arg = f"{option}={separate / value}"
        elif _looks_like_state_path(arg):
            arg = str(separate / arg)
        converted.append(arg)
    return converted


def _looks_like_state_path(value: str) -> bool:
    return ("/" in value or "\\" in value) and is_state_relative(value.replace("\\", "/"))


def relative_to_root(path: PurePath, root: PurePath | str) -> PurePath:
    """path.relative_to(root); a path in the state root's state directories is made relative to
    the state root, so it still reads "outputs/...". Raises ValueError like relative_to."""
    separate = _separate_state_root()
    if separate is not None and is_code_root(root):
        try:
            relative = path.relative_to(separate)
        except ValueError:
            pass
        else:
            if is_state_relative(relative):
                return relative
    return path.relative_to(root)


def glob_root(root: Path, pattern: str) -> Iterator[Path]:
    """root.glob(pattern); state directories come from the state root when root is the code
    root."""
    return _glob(root, pattern, recursive=False)


def rglob_root(root: Path, pattern: str) -> Iterator[Path]:
    """root.rglob(pattern); state directories come from the state root when root is the code
    root."""
    return _glob(root, pattern, recursive=True)


def _glob(root: Path, pattern: str, *, recursive: bool) -> Iterator[Path]:
    search = root.rglob if recursive else root.glob
    separate = _separate_state_root()
    if separate is None or not is_code_root(root):
        return search(pattern)
    first = _first_part(pattern)
    if not recursive and first is not None and not _has_magic(first):
        if first in STATE_DIRS:
            return separate.glob(pattern)
        return search(pattern)
    return _glob_both(root, separate, pattern, recursive=recursive)


def _glob_both(root: Path, separate: Path, pattern: str, *, recursive: bool) -> Iterator[Path]:
    for path in (root.rglob if recursive else root.glob)(pattern):
        if not is_state_relative(path.relative_to(root)):
            yield path
    for path in (separate.rglob if recursive else separate.glob)(pattern):
        if is_state_relative(path.relative_to(separate)):
            yield path


def _has_magic(part: str) -> bool:
    return any(char in part for char in "*?[")
