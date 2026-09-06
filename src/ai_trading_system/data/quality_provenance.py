"""Pure CSV facts and exact manifest-row provenance; no input or policy I/O."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import PurePosixPath, PureWindowsPath
from types import MappingProxyType

MANIFEST_REQUIRED_COLUMNS = (
    "downloaded_at",
    "source_id",
    "provider",
    "endpoint",
    "request_parameters",
    "output_path",
    "row_count",
    "checksum_sha256",
)


def inspect_csv_content(content: bytes) -> tuple[tuple[str, ...], int]:
    """Preserve the legacy runner's CSV facts and unreadable-input semantics."""

    try:
        handle = io.StringIO(content.decode("utf-8-sig"), newline="")
        reader = csv.reader(handle)
        header = next(reader, [])
        row_count = sum(1 for _ in reader)
    except (UnicodeError, csv.Error):
        return (), 0
    if len(header) != len(set(header)):
        return (), row_count
    return tuple(header), row_count


def parse_manifest_content(
    content: bytes,
    *,
    required_columns: Sequence[str] = MANIFEST_REQUIRED_COLUMNS,
) -> tuple[tuple[str, ...], tuple[Mapping[str, str], ...]]:
    """Capture every original string field, including extension columns."""

    try:
        handle = io.StringIO(content.decode("utf-8-sig"), newline="")
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        if (
            not fieldnames
            or len(fieldnames) != len(set(fieldnames))
            or not set(required_columns).issubset(fieldnames)
        ):
            return fieldnames, ()
        rows: list[Mapping[str, str]] = []
        for raw_row in reader:
            if None in raw_row or any(value is None for value in raw_row.values()):
                return fieldnames, ()
            rows.append(MappingProxyType({field: raw_row[field] for field in fieldnames}))
    except (UnicodeError, csv.Error):
        return (), ()
    return fieldnames, tuple(rows)


def manifest_record_ref(row: Mapping[str, str]) -> str:
    """Use the unchanged legacy full-row hash, never a selected-field digest."""

    canonical = json.dumps(
        dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"manifest_record_{sha256(canonical).hexdigest()}"


class NamedManifestBindingError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class NamedManifestRowMatch:
    """A zero-based ordinal into the unchanged original manifest CSV rows."""

    ordinal: int
    row: Mapping[str, str]
    record_ref: str
    source_id: str


def match_named_manifest_member(
    manifest_content: bytes,
    *,
    transaction_id: str,
    role: str,
    original_output_path: str,
    member_sha256: str,
    member_row_count: int,
) -> NamedManifestRowMatch:
    """Bind one immutable member to its transaction's unique original full row.

    The caller binds manifest bytes and the captured member to the validated
    named publication first. ``original_output_path`` comes from the explicit
    original source-root relationship, not from a relocated member's basename.
    """

    _, rows = parse_manifest_content(manifest_content)
    matches: list[tuple[int, Mapping[str, str]]] = []
    for ordinal, row in enumerate(rows):
        try:
            parameters = json.loads(
                row["request_parameters"], object_pairs_hook=_unique_json_object
            )
        except (TypeError, ValueError):
            continue
        if (
            isinstance(parameters, dict)
            and parameters.get("publication_transaction_id") == transaction_id
            and parameters.get("artifact_role") == role
        ):
            matches.append((ordinal, row))
    if len(matches) != 1:
        raise NamedManifestBindingError(
            "DQ_NAMED_MANIFEST_ROW_NOT_UNIQUE",
            f"transaction={transaction_id} role={role} rows={len(matches)}",
        )
    ordinal, row = matches[0]
    if not _same_absolute_path(row["output_path"], original_output_path):
        raise NamedManifestBindingError("DQ_NAMED_MANIFEST_SOURCE_PATH_MISMATCH", f"role={role}")
    if row["checksum_sha256"] != member_sha256:
        raise NamedManifestBindingError("DQ_NAMED_MANIFEST_SHA_MISMATCH", f"role={role}")
    if row["row_count"] != str(member_row_count):
        raise NamedManifestBindingError("DQ_NAMED_MANIFEST_ROW_COUNT_MISMATCH", f"role={role}")
    source_id = row["source_id"]
    if not source_id or source_id != source_id.strip():
        raise NamedManifestBindingError("DQ_SOURCE_ID_UNREVIEWED", f"role={role}")
    return NamedManifestRowMatch(ordinal, row, manifest_record_ref(row), source_id)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _same_absolute_path(observed: str, expected: str) -> bool:
    """Lexical only: explicit original roots may no longer exist on this host."""

    if not observed or observed != observed.strip() or "\x00" in observed:
        return False
    for path_type in (PureWindowsPath, PurePosixPath):
        actual = path_type(observed)
        claimed = path_type(expected)
        if actual.is_absolute() and claimed.is_absolute():
            return ".." not in actual.parts and ".." not in claimed.parts and actual == claimed
    return False
