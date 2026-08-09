from __future__ import annotations

import base64
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from ai_trading_system.platform.artifacts.writer import (
    canonical_json_bytes,
    write_bytes_atomic,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_POLICY_PATH = Path(
    "config/architecture/devx_006d_report_catalog_flow_authority.yaml"
)
INDEX_SCHEMA = "report_catalog_flow_authority_index.v2"
FRAGMENT_SCHEMA = "report_catalog_flow_full_entry_fragment.v2"
INVENTORY_SCHEMA = "report_catalog_flow_consumer_inventory.v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_BLOB_PATTERN = re.compile(r"^[0-9a-f]{40}$")
REPORT_MARKER_PATTERN = re.compile(
    rb"(?m)^  - report_id: ([A-Za-z0-9_.-]+)[ \t]*\r?\n"
)

_POLICY_FIELDS = {
    "schema_version",
    "status",
    "task_id",
    "exact_start_base",
    "owner_decision",
    "partition_count",
    "fragment_root",
    "index_path",
    "consumer_inventory_path",
    "targets",
    "contract",
    "production_effect",
    "broker_action",
}
_TARGET_FIELDS = {
    "target_id",
    "path",
    "format",
    "splitter",
    "byte_count",
    "file_sha256",
    "lf_sha256",
    "git_blob",
    "entry_count",
}
_CONTRACT_FIELDS = {
    "source_of_truth",
    "fragment_shadow_active",
    "aggregate_write_allowed",
    "fragment_identity",
    "partition_identity",
    "index_chain",
    "coverage_required_percent",
    "silent_drop_allowed",
    "rollback_mode",
}
_INDEX_FIELDS = {
    "schema_version",
    "task_id",
    "status",
    "source_of_truth",
    "partition_count",
    "fragment_root",
    "targets",
    "contract",
    "production_effect",
    "broker_action",
}
_INDEX_TARGET_FIELDS = {
    "target_id",
    "path",
    "format",
    "splitter",
    "source_seal",
    "entry_count",
    "coverage_bytes",
    "coverage_percent",
    "fragment_count",
    "fragments",
    "entry_order",
    "entry_order_sha256",
    "final_chain_sha256",
}
_SOURCE_SEAL_FIELDS = {
    "byte_count",
    "file_sha256",
    "lf_sha256",
    "git_blob",
}
_INDEX_ENTRY_FIELDS = {
    "entry_id",
    "raw_sha256",
    "byte_count",
    "partition",
}
_INDEX_FRAGMENT_FIELDS = {
    "fragment_path",
    "fragment_sha256",
    "partition",
    "entry_count",
    "previous_fragment_sha256",
    "chain_sha256",
}
_FRAGMENT_FIELDS = {
    "schema_version",
    "task_id",
    "target_id",
    "partition",
    "entries",
}
_FRAGMENT_ENTRY_FIELDS = {"entry_id", "raw_sha256", "raw_base64"}
_SCANNED_CONSUMER_ROOTS = ("src", "scripts", "tests")
_TARGET_FORMATS = {"YAML_REPORT_REGISTRY", "MARKDOWN"}
_TARGET_SPLITTERS = {
    "YAML_REPORT_ITEMS_WITH_PREFIX_V1",
    "EXACT_BLANK_LINE_BLOCKS_V1",
}


class ReportCatalogFlowAuthorityError(ValueError):
    """Typed fail-closed lossless fragment authority failure."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _fail(code: str, detail: object) -> NoReturn:
    raise ReportCatalogFlowAuthorityError(code, str(detail))


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("RCF_MAPPING_REQUIRED", label)
    return value


def _sequence(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        _fail("RCF_SEQUENCE_REQUIRED", label)
    return value


def _exact(value: Mapping[str, Any], fields: set[str], label: str) -> None:
    if set(value) != fields:
        _fail(
            "RCF_FIELDS_INVALID",
            f"{label}:expected={sorted(fields)} actual={sorted(value)}",
        )


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("RCF_STRING_REQUIRED", label)
    return value


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        _fail("RCF_BOOLEAN_REQUIRED", label)
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        _fail("RCF_INTEGER_INVALID", label)
    return value


def _sha256(value: object, label: str) -> str:
    result = _string(value, label)
    if SHA256_PATTERN.fullmatch(result) is None:
        _fail("RCF_SHA256_INVALID", label)
    return result


def _git_blob(value: object, label: str) -> str:
    result = _string(value, label)
    if GIT_BLOB_PATTERN.fullmatch(result) is None:
        _fail("RCF_GIT_BLOB_INVALID", label)
    return result


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _git_blob_id(content: bytes) -> str:
    header = f"blob {len(content)}\0".encode("ascii")
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def _canonical_json(value: object, *, indent: int | None) -> bytes:
    return canonical_json_bytes(
        value,
        sort_keys=True,
        indent=indent,
        ensure_ascii=False,
    )


def _strict_json(content: bytes, label: str) -> dict[str, Any]:
    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("RCF_JSON_DUPLICATE_KEY", f"{label}:{key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> NoReturn:
        _fail("RCF_JSON_NON_FINITE", f"{label}:{value}")

    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReportCatalogFlowAuthorityError("RCF_UTF8_INVALID", label) from exc
    try:
        parsed = json.loads(
            text,
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except ReportCatalogFlowAuthorityError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise ReportCatalogFlowAuthorityError("RCF_JSON_INVALID", label) from exc
    return _mapping(parsed, label)


def _portable(value: object, label: str) -> str:
    portable = _string(value, label)
    parsed = PurePosixPath(portable)
    if (
        parsed.is_absolute()
        or "\\" in portable
        or not parsed.parts
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or ":" in parsed.parts[0]
    ):
        _fail("RCF_PATH_INVALID", f"{label}:{portable}")
    return portable


def _regular_path(root: Path, portable: str, label: str) -> Path:
    root = root.resolve()
    cursor = root
    for part in PurePosixPath(portable).parts:
        cursor = cursor / part
        if cursor.is_symlink():
            _fail("RCF_PATH_SYMLINK", f"{label}:{portable}")
    try:
        resolved = cursor.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ReportCatalogFlowAuthorityError("RCF_FILE_MISSING", portable) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ReportCatalogFlowAuthorityError("RCF_PATH_ESCAPE", portable) from exc
    if not resolved.is_file():
        _fail("RCF_REGULAR_FILE_REQUIRED", portable)
    return resolved


def load_policy(repository_root: Path = Path(".")) -> dict[str, Any]:
    root = repository_root.resolve()
    path = _regular_path(root, DEFAULT_POLICY_PATH.as_posix(), "policy")
    policy = _mapping(
        load_strict_yaml_text(path.read_text(encoding="utf-8"), label=path.as_posix()),
        "policy",
    )
    _exact(policy, _POLICY_FIELDS, "policy")
    if policy["schema_version"] != "devx_006d_report_catalog_flow_authority_policy.v1":
        _fail("RCF_POLICY_SCHEMA_INVALID", policy["schema_version"])
    if policy["status"] != "INACTIVE_SHADOW":
        _fail("RCF_POLICY_STATUS_INVALID", policy["status"])
    _string(policy["task_id"], "task_id")
    _string(policy["exact_start_base"], "exact_start_base")
    _string(policy["owner_decision"], "owner_decision")
    partition_count = _integer(policy["partition_count"], "partition_count", minimum=1)
    if partition_count != 64:
        _fail("RCF_PARTITION_COUNT_INVALID", partition_count)
    for field in ("fragment_root", "index_path", "consumer_inventory_path"):
        _portable(policy[field], field)
    raw_targets = _sequence(policy["targets"], "targets")
    if len(raw_targets) != 3:
        _fail("RCF_TARGET_COUNT_INVALID", len(raw_targets))
    target_ids: set[str] = set()
    target_paths: set[str] = set()
    for position, raw_target in enumerate(raw_targets):
        target = _mapping(raw_target, f"targets[{position}]")
        _exact(target, _TARGET_FIELDS, f"targets[{position}]")
        target_id = _string(target["target_id"], "target_id")
        target_path = _portable(target["path"], "target.path")
        if target_id in target_ids or target_path in target_paths:
            _fail("RCF_TARGET_DUPLICATE", target_id)
        target_ids.add(target_id)
        target_paths.add(target_path)
        if target["format"] not in _TARGET_FORMATS:
            _fail("RCF_TARGET_FORMAT_INVALID", target["format"])
        if target["splitter"] not in _TARGET_SPLITTERS:
            _fail("RCF_TARGET_SPLITTER_INVALID", target["splitter"])
        _integer(target["byte_count"], "target.byte_count", minimum=1)
        _sha256(target["file_sha256"], "target.file_sha256")
        _sha256(target["lf_sha256"], "target.lf_sha256")
        _git_blob(target["git_blob"], "target.git_blob")
        _integer(target["entry_count"], "target.entry_count", minimum=1)
    contract = _mapping(policy["contract"], "contract")
    _exact(contract, _CONTRACT_FIELDS, "contract")
    if (
        contract["source_of_truth"] != "LEGACY_MONOLITH"
        or _boolean(contract["fragment_shadow_active"], "fragment_shadow_active")
        or _boolean(contract["aggregate_write_allowed"], "aggregate_write_allowed")
        or contract["fragment_identity"] != "FULL_ENTRY_RAW_SHA256"
        or contract["partition_identity"] != "RAW_SHA256_LOW_6_BITS"
        or contract["index_chain"] != "SHA256"
        or _integer(contract["coverage_required_percent"], "coverage") != 100
        or _boolean(contract["silent_drop_allowed"], "silent_drop_allowed")
        or contract["rollback_mode"] != "IGNORE_INACTIVE_SHADOW"
    ):
        _fail("RCF_CONTRACT_INVALID", contract)
    if policy["production_effect"] != "none" or policy["broker_action"] != "none":
        _fail("RCF_SAFETY_BOUNDARY_INVALID", policy["task_id"])
    return policy


def _source_bytes(root: Path, target: Mapping[str, Any]) -> bytes:
    portable = _portable(target["path"], "target.path")
    content = _regular_path(root, portable, "target").read_bytes()
    checks = {
        "byte_count": len(content),
        "file_sha256": _digest(content),
        "lf_sha256": _digest(content.replace(b"\r\n", b"\n")),
        "git_blob": _git_blob_id(content),
    }
    for field, actual in checks.items():
        if target[field] != actual:
            _fail(
                "RCF_SOURCE_SEAL_DRIFT",
                f"{portable}:{field}:expected={target[field]} actual={actual}",
            )
    return content


def _split_report_registry(content: bytes) -> list[tuple[str, bytes]]:
    matches = list(REPORT_MARKER_PATTERN.finditer(content))
    if not matches:
        _fail("RCF_REPORT_MARKER_MISSING", "reports")
    try:
        parsed = _mapping(
            load_strict_yaml_text(content.decode("utf-8"), label="report_registry"),
            "report_registry",
        )
    except UnicodeDecodeError as exc:
        raise ReportCatalogFlowAuthorityError(
            "RCF_UTF8_INVALID", "report_registry"
        ) from exc
    reports = _sequence(parsed.get("reports"), "report_registry.reports")
    parsed_ids: list[str] = []
    for position, raw_report in enumerate(reports):
        report = _mapping(raw_report, f"reports[{position}]")
        parsed_ids.append(_string(report.get("report_id"), f"reports[{position}].report_id"))
    marker_ids = [match.group(1).decode("ascii") for match in matches]
    if marker_ids != parsed_ids:
        _fail("RCF_REPORT_ORDER_OR_ID_DRIFT", "report_registry")
    if len(set(marker_ids)) != len(marker_ids):
        _fail("RCF_REPORT_ID_DUPLICATE", "report_registry")
    entries: list[tuple[str, bytes]] = [
        ("report_registry:prefix", content[: matches[0].start()])
    ]
    for position, match in enumerate(matches):
        end = matches[position + 1].start() if position + 1 < len(matches) else len(content)
        entries.append(
            (
                f"report_registry:report:{marker_ids[position]}",
                content[match.start() : end],
            )
        )
    return entries


def _split_markdown(target_id: str, content: bytes) -> list[tuple[str, bytes]]:
    blocks: list[bytes] = []
    current: list[bytes] = []
    for line in content.splitlines(keepends=True):
        current.append(line)
        if line.rstrip(b"\r\n") == b"":
            blocks.append(b"".join(current))
            current = []
    if current:
        blocks.append(b"".join(current))
    if not blocks or any(not block for block in blocks):
        _fail("RCF_MARKDOWN_BLOCK_INVALID", target_id)
    hashes = [_digest(block) for block in blocks]
    if len(set(hashes)) != len(hashes):
        _fail("RCF_MARKDOWN_BLOCK_DUPLICATE", target_id)
    return [
        (f"{target_id}:block:{raw_sha256}", block)
        for raw_sha256, block in zip(hashes, blocks, strict=True)
    ]


def split_target_entries(
    target: Mapping[str, Any], content: bytes
) -> list[tuple[str, bytes]]:
    target_id = _string(target["target_id"], "target_id")
    splitter = target["splitter"]
    if splitter == "YAML_REPORT_ITEMS_WITH_PREFIX_V1":
        entries = _split_report_registry(content)
    elif splitter == "EXACT_BLANK_LINE_BLOCKS_V1":
        entries = _split_markdown(target_id, content)
    else:
        _fail("RCF_TARGET_SPLITTER_INVALID", splitter)
    if len(entries) != target["entry_count"]:
        _fail(
            "RCF_ENTRY_COUNT_DRIFT",
            f"{target_id}:expected={target['entry_count']} actual={len(entries)}",
        )
    if b"".join(raw for _, raw in entries) != content:
        _fail("RCF_SPLITTER_NOT_LOSSLESS", target_id)
    entry_ids = [entry_id for entry_id, _ in entries]
    if len(set(entry_ids)) != len(entry_ids):
        _fail("RCF_ENTRY_ID_DUPLICATE", target_id)
    return entries


def _partition(raw_sha256: str, partition_count: int) -> str:
    return f"{int(raw_sha256[:2], 16) & (partition_count - 1):02x}"


def _entry_hash(entry_without_hash: Mapping[str, Any]) -> str:
    return _digest(_canonical_json(entry_without_hash, indent=None))


def _build_expected(
    root: Path, policy: Mapping[str, Any]
) -> tuple[dict[str, bytes], dict[str, Any], bytes]:
    partition_count = _integer(policy["partition_count"], "partition_count", minimum=1)
    fragment_root = _portable(policy["fragment_root"], "fragment_root")
    fragment_bytes_by_path: dict[str, bytes] = {}
    index_targets: list[dict[str, Any]] = []
    for raw_target in _sequence(policy["targets"], "targets"):
        target = _mapping(raw_target, "target")
        target_id = _string(target["target_id"], "target_id")
        content = _source_bytes(root, target)
        entries = split_target_entries(target, content)
        grouped: dict[str, list[tuple[str, str, bytes]]] = defaultdict(list)
        for entry_id, raw in entries:
            raw_sha = _digest(raw)
            grouped[_partition(raw_sha, partition_count)].append((entry_id, raw_sha, raw))
        raw_fragment_records: list[dict[str, Any]] = []
        for partition, partition_entries in sorted(grouped.items()):
            fragment = {
                "schema_version": FRAGMENT_SCHEMA,
                "task_id": policy["task_id"],
                "target_id": target_id,
                "partition": partition,
                "entries": [
                    {
                        "entry_id": entry_id,
                        "raw_sha256": raw_sha,
                        "raw_base64": base64.b64encode(raw).decode("ascii"),
                    }
                    for entry_id, raw_sha, raw in sorted(
                        partition_entries, key=lambda value: value[0]
                    )
                ],
            }
            fragment_bytes = _canonical_json(fragment, indent=None)
            fragment_sha = _digest(fragment_bytes)
            portable = (
                f"{fragment_root}/{target_id}/{partition}/{fragment_sha}.json"
            )
            fragment_bytes_by_path[portable] = fragment_bytes
            raw_fragment_records.append(
                {
                    "partition": partition,
                    "fragment_path": portable,
                    "fragment_sha256": fragment_sha,
                    "entry_count": len(partition_entries),
                }
            )
        previous = "0" * 64
        index_fragments: list[dict[str, Any]] = []
        for raw_fragment_record in raw_fragment_records:
            partial_fragment = {
                **raw_fragment_record,
                "previous_fragment_sha256": previous,
            }
            chain_sha = _entry_hash(partial_fragment)
            index_fragments.append(
                {**partial_fragment, "chain_sha256": chain_sha}
            )
            previous = chain_sha
        entry_order: list[dict[str, Any]] = []
        for entry_id, raw in entries:
            raw_sha = _digest(raw)
            entry_order.append(
                {
                    "entry_id": entry_id,
                    "raw_sha256": raw_sha,
                    "byte_count": len(raw),
                    "partition": _partition(raw_sha, partition_count),
                }
            )
        source_seal = {
            key: target[key]
            for key in ("byte_count", "file_sha256", "lf_sha256", "git_blob")
        }
        index_targets.append(
            {
                "target_id": target_id,
                "path": target["path"],
                "format": target["format"],
                "splitter": target["splitter"],
                "source_seal": source_seal,
                "entry_count": len(entries),
                "coverage_bytes": sum(len(raw) for _, raw in entries),
                "coverage_percent": 100,
                "fragment_count": len(grouped),
                "fragments": index_fragments,
                "entry_order": entry_order,
                "entry_order_sha256": _digest(
                    _canonical_json(entry_order, indent=None)
                ),
                "final_chain_sha256": previous,
            }
        )
    index = {
        "schema_version": INDEX_SCHEMA,
        "task_id": policy["task_id"],
        "status": "PASS",
        "source_of_truth": policy["contract"]["source_of_truth"],
        "partition_count": partition_count,
        "fragment_root": fragment_root,
        "targets": index_targets,
        "contract": dict(_mapping(policy["contract"], "contract")),
        "production_effect": policy["production_effect"],
        "broker_action": policy["broker_action"],
    }
    return fragment_bytes_by_path, index, _canonical_json(index, indent=2)


def _load_index(root: Path, policy: Mapping[str, Any]) -> dict[str, Any]:
    portable = _portable(policy["index_path"], "index_path")
    content = _regular_path(root, portable, "index").read_bytes()
    index = _strict_json(content, portable)
    if _canonical_json(index, indent=2) != content:
        _fail("RCF_INDEX_NON_CANONICAL", portable)
    _exact(index, _INDEX_FIELDS, "index")
    if index["schema_version"] != INDEX_SCHEMA or index["status"] != "PASS":
        _fail("RCF_INDEX_IDENTITY_INVALID", portable)
    if index["task_id"] != policy["task_id"]:
        _fail("RCF_INDEX_TASK_MISMATCH", index["task_id"])
    if index["source_of_truth"] != "LEGACY_MONOLITH":
        _fail("RCF_SOURCE_OF_TRUTH_CUTOVER_FORBIDDEN", index["source_of_truth"])
    if index["fragment_root"] != policy["fragment_root"]:
        _fail("RCF_FRAGMENT_ROOT_DRIFT", index["fragment_root"])
    if index["partition_count"] != policy["partition_count"]:
        _fail("RCF_PARTITION_COUNT_DRIFT", index["partition_count"])
    if index["contract"] != policy["contract"]:
        _fail("RCF_INDEX_CONTRACT_DRIFT", index["contract"])
    return index


def _decode_fragment(
    root: Path,
    *,
    target_id: str,
    partition: str,
    portable: str,
    expected_sha256: str,
    policy: Mapping[str, Any],
) -> dict[str, bytes]:
    fragment_root = _portable(policy["fragment_root"], "fragment_root")
    expected_path = f"{fragment_root}/{target_id}/{partition}/{expected_sha256}.json"
    if portable != expected_path:
        _fail("RCF_FRAGMENT_PATH_IDENTITY_INVALID", portable)
    content = _regular_path(root, portable, "fragment").read_bytes()
    if _digest(content) != expected_sha256:
        _fail("RCF_FRAGMENT_HASH_DRIFT", portable)
    fragment = _strict_json(content, portable)
    if _canonical_json(fragment, indent=None) != content:
        _fail("RCF_FRAGMENT_NON_CANONICAL", portable)
    _exact(fragment, _FRAGMENT_FIELDS, "fragment")
    if (
        fragment["schema_version"] != FRAGMENT_SCHEMA
        or fragment["task_id"] != policy["task_id"]
        or fragment["target_id"] != target_id
        or fragment["partition"] != partition
    ):
        _fail("RCF_FRAGMENT_IDENTITY_DRIFT", portable)
    decoded: dict[str, bytes] = {}
    for position, raw_entry in enumerate(_sequence(fragment["entries"], "fragment.entries")):
        entry = _mapping(raw_entry, f"fragment.entries[{position}]")
        _exact(entry, _FRAGMENT_ENTRY_FIELDS, f"fragment.entries[{position}]")
        entry_id = _string(entry["entry_id"], "fragment.entry_id")
        raw_sha = _sha256(entry["raw_sha256"], "fragment.raw_sha256")
        encoded = _string(entry["raw_base64"], "fragment.raw_base64")
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as exc:
            raise ReportCatalogFlowAuthorityError(
                "RCF_FRAGMENT_BASE64_INVALID", entry_id
            ) from exc
        if base64.b64encode(raw).decode("ascii") != encoded:
            _fail("RCF_FRAGMENT_BASE64_NON_CANONICAL", entry_id)
        if _digest(raw) != raw_sha:
            _fail("RCF_FRAGMENT_ENTRY_HASH_DRIFT", entry_id)
        if _partition(raw_sha, int(policy["partition_count"])) != partition:
            _fail("RCF_FRAGMENT_ENTRY_PARTITION_DRIFT", entry_id)
        if entry_id in decoded:
            _fail("RCF_FRAGMENT_ENTRY_DUPLICATE", entry_id)
        decoded[entry_id] = raw
    if list(decoded) != sorted(decoded):
        _fail("RCF_FRAGMENT_ENTRY_ORDER_INVALID", portable)
    return decoded


def render_shadow_bytes(
    target_id: str,
    repository_root: Path = Path("."),
) -> bytes:
    root = repository_root.resolve()
    policy = load_policy(root)
    index = _load_index(root, policy)
    raw_targets = _sequence(index["targets"], "index.targets")
    policy_target_ids = [
        _string(_mapping(target, "target")["target_id"], "target_id")
        for target in _sequence(policy["targets"], "policy.targets")
    ]
    actual_target_ids = [
        _string(_mapping(target, "index.target")["target_id"], "target_id")
        for target in raw_targets
    ]
    if actual_target_ids != policy_target_ids:
        _fail("RCF_INDEX_TARGET_ORDER_DRIFT", actual_target_ids)
    try:
        raw_target = next(
            target
            for target in raw_targets
            if _mapping(target, "index.target").get("target_id") == target_id
        )
    except StopIteration as exc:
        raise ReportCatalogFlowAuthorityError("RCF_TARGET_UNKNOWN", target_id) from exc
    target = _mapping(raw_target, "index.target")
    _exact(target, _INDEX_TARGET_FIELDS, "index.target")
    source_seal = _mapping(target["source_seal"], "source_seal")
    _exact(source_seal, _SOURCE_SEAL_FIELDS, "source_seal")
    _integer(target["entry_count"], "entry_count", minimum=1)
    _integer(target["coverage_bytes"], "coverage_bytes", minimum=1)
    if target["coverage_percent"] != 100:
        _fail("RCF_COVERAGE_PERCENT_INVALID", target["coverage_percent"])
    _integer(source_seal["byte_count"], "source_seal.byte_count", minimum=1)
    _sha256(source_seal["file_sha256"], "source_seal.file_sha256")
    _sha256(source_seal["lf_sha256"], "source_seal.lf_sha256")
    _git_blob(source_seal["git_blob"], "source_seal.git_blob")
    fragments = _sequence(target["fragments"], "index.target.fragments")
    if len(fragments) != target["fragment_count"]:
        _fail("RCF_INDEX_FRAGMENT_COUNT_DRIFT", target_id)
    fragment_cache: dict[str, dict[str, bytes]] = {}
    previous = "0" * 64
    last_partition = ""
    for position, raw_fragment in enumerate(fragments):
        fragment = _mapping(raw_fragment, f"fragments[{position}]")
        _exact(fragment, _INDEX_FRAGMENT_FIELDS, f"fragments[{position}]")
        partition = _string(fragment["partition"], "fragment.partition")
        if partition <= last_partition:
            _fail("RCF_FRAGMENT_PARTITION_ORDER_DRIFT", partition)
        last_partition = partition
        portable = _portable(fragment["fragment_path"], "fragment_path")
        fragment_sha = _sha256(fragment["fragment_sha256"], "fragment_sha256")
        if fragment["previous_fragment_sha256"] != previous:
            _fail("RCF_FRAGMENT_CHAIN_PREVIOUS_DRIFT", partition)
        partial_fragment = {
            key: fragment[key]
            for key in _INDEX_FRAGMENT_FIELDS - {"chain_sha256"}
        }
        chain_sha = _entry_hash(partial_fragment)
        if fragment["chain_sha256"] != chain_sha:
            _fail("RCF_FRAGMENT_CHAIN_HASH_DRIFT", partition)
        previous = chain_sha
        decoded = _decode_fragment(
            root,
            target_id=target_id,
            partition=partition,
            portable=portable,
            expected_sha256=fragment_sha,
            policy=policy,
        )
        if len(decoded) != fragment["entry_count"]:
            _fail("RCF_FRAGMENT_ENTRY_COUNT_DRIFT", partition)
        fragment_cache[partition] = decoded
    if target["final_chain_sha256"] != previous:
        _fail("RCF_FINAL_CHAIN_DRIFT", target_id)
    entry_order = _sequence(target["entry_order"], "index.target.entry_order")
    if len(entry_order) != target["entry_count"]:
        _fail("RCF_INDEX_ENTRY_COUNT_DRIFT", target_id)
    if _digest(_canonical_json(entry_order, indent=None)) != target["entry_order_sha256"]:
        _fail("RCF_ENTRY_ORDER_HASH_DRIFT", target_id)
    referenced: dict[str, str] = {}
    rendered: list[bytes] = []
    for order, raw_entry in enumerate(entry_order):
        entry = _mapping(raw_entry, f"entry_order[{order}]")
        _exact(entry, _INDEX_ENTRY_FIELDS, f"entry_order[{order}]")
        entry_id = _string(entry["entry_id"], "entry_id")
        if entry_id in referenced:
            _fail("RCF_INDEX_ENTRY_DUPLICATE", entry_id)
        raw_sha = _sha256(entry["raw_sha256"], "raw_sha256")
        partition = _string(entry["partition"], "partition")
        fragment_entries = fragment_cache.get(partition)
        if fragment_entries is None:
            _fail("RCF_ENTRY_PARTITION_MISSING", entry_id)
        if entry_id not in fragment_entries:
            _fail("RCF_FRAGMENT_ENTRY_MISSING", entry_id)
        raw = fragment_entries[entry_id]
        if _digest(raw) != raw_sha or len(raw) != entry["byte_count"]:
            _fail("RCF_ENTRY_RAW_IDENTITY_DRIFT", entry_id)
        referenced[entry_id] = partition
        rendered.append(raw)
    all_fragment_entries = {
        entry_id
        for fragment_entries in fragment_cache.values()
        for entry_id in fragment_entries
    }
    if all_fragment_entries != set(referenced):
        _fail(
            "RCF_FRAGMENT_COVERAGE_DRIFT",
            f"{target_id}:indexed={len(referenced)} stored={len(all_fragment_entries)}",
        )
    result = b"".join(rendered)
    if len(result) != target["coverage_bytes"] or len(result) != source_seal["byte_count"]:
        _fail("RCF_COVERAGE_BYTES_DRIFT", target_id)
    if _digest(result) != source_seal["file_sha256"]:
        _fail("RCF_RENDER_HASH_DRIFT", target_id)
    return result


def build_consumer_inventory(
    repository_root: Path,
    *,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    root = repository_root.resolve()
    target_paths = [
        _portable(_mapping(target, "target")["path"], "target.path")
        for target in _sequence(policy["targets"], "targets")
    ]
    records: list[dict[str, Any]] = []
    for root_name in _SCANNED_CONSUMER_ROOTS:
        scan_root = root / root_name
        for path in sorted(scan_root.rglob("*.py")):
            portable = path.relative_to(root).as_posix()
            text = path.read_text(encoding="utf-8")
            matches = {
                target_path: text.count(target_path)
                for target_path in target_paths
                if target_path in text
            }
            if not matches:
                continue
            if portable.endswith("report_catalog_flow_authority.py"):
                role = "SHADOW_AUTHORITY_IMPLEMENTATION"
                migration_status = "MIGRATED_SHADOW"
            elif portable.startswith("tests/"):
                role = "SOURCE_CONTRACT_TEST"
                migration_status = "RETAINED_UNTIL_OWNER_CUTOVER"
            elif portable.startswith("src/"):
                role = "RUNTIME_OR_LIBRARY_CONSUMER"
                migration_status = "PENDING_OWNER_CUTOVER"
            else:
                role = "DEVELOPER_TOOL_CONSUMER"
                migration_status = "PENDING_OWNER_CUTOVER"
            records.append(
                {
                    "path": portable,
                    "role": role,
                    "migration_status": migration_status,
                    "references": [
                        {"target_path": target_path, "literal_count": count}
                        for target_path, count in sorted(matches.items())
                    ],
                }
            )
    pending = sum(
        record["migration_status"] == "PENDING_OWNER_CUTOVER" for record in records
    )
    return {
        "schema_version": INVENTORY_SCHEMA,
        "task_id": policy["task_id"],
        "status": "PASS",
        "scan_roots": list(_SCANNED_CONSUMER_ROOTS),
        "target_paths": target_paths,
        "consumer_count": len(records),
        "pending_owner_cutover_count": pending,
        "cutover_ready": False,
        "source_of_truth": "LEGACY_MONOLITH",
        "consumers": records,
        "rollback_mode": "IGNORE_INACTIVE_SHADOW",
        "production_effect": "none",
        "broker_action": "none",
    }


def build_repository_authority(
    repository_root: Path = Path("."),
    *,
    write: bool,
) -> dict[str, Any]:
    root = repository_root.resolve()
    policy = load_policy(root)
    fragments, index, index_bytes = _build_expected(root, policy)
    inventory = build_consumer_inventory(root, policy=policy)
    inventory_bytes = _canonical_json(inventory, indent=2)
    if write:
        for portable, content in sorted(fragments.items()):
            write_bytes_atomic(root / Path(portable), content)
        write_bytes_atomic(root / Path(str(policy["index_path"])), index_bytes)
        write_bytes_atomic(
            root / Path(str(policy["consumer_inventory_path"])), inventory_bytes
        )
    return {
        "status": "PASS",
        "source_of_truth": "LEGACY_MONOLITH",
        "fragment_shadow_active": False,
        "target_count": len(index["targets"]),
        "entry_count": sum(target["entry_count"] for target in index["targets"]),
        "fragment_count": len(fragments),
        "index_path": policy["index_path"],
        "index_sha256": _digest(index_bytes),
        "consumer_inventory_path": policy["consumer_inventory_path"],
        "consumer_inventory_sha256": _digest(inventory_bytes),
        "fragment_paths": sorted(fragments),
        "fragment_sha256": {path: _digest(content) for path, content in fragments.items()},
    }


def validate_repository_authority(
    repository_root: Path = Path("."),
) -> dict[str, Any]:
    root = repository_root.resolve()
    expected = build_repository_authority(root, write=False)
    policy = load_policy(root)
    index_content = _regular_path(
        root, _portable(policy["index_path"], "index_path"), "index"
    ).read_bytes()
    if _digest(index_content) != expected["index_sha256"]:
        _fail("RCF_INDEX_STALE", policy["index_path"])
    inventory_content = _regular_path(
        root,
        _portable(policy["consumer_inventory_path"], "consumer_inventory_path"),
        "consumer_inventory",
    ).read_bytes()
    if _digest(inventory_content) != expected["consumer_inventory_sha256"]:
        _fail("RCF_CONSUMER_INVENTORY_STALE", policy["consumer_inventory_path"])
    inventory = _strict_json(inventory_content, str(policy["consumer_inventory_path"]))
    if _canonical_json(inventory, indent=2) != inventory_content:
        _fail("RCF_CONSUMER_INVENTORY_NON_CANONICAL", policy["consumer_inventory_path"])
    for portable in expected["fragment_paths"]:
        content = _regular_path(root, portable, "fragment").read_bytes()
        if _digest(content) != expected["fragment_sha256"][portable]:
            _fail("RCF_FRAGMENT_STALE", portable)
    for raw_target in _sequence(policy["targets"], "targets"):
        target = _mapping(raw_target, "target")
        target_id = _string(target["target_id"], "target_id")
        rendered = render_shadow_bytes(target_id, root)
        source = _source_bytes(root, target)
        if rendered != source:
            _fail("RCF_BYTE_PARITY_FAILED", target_id)
    return {key: value for key, value in expected.items() if key != "fragment_sha256"}
