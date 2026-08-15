from __future__ import annotations

import ast
import datetime as dt
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

import yaml

from ai_trading_system.platform.artifacts.writer import (
    canonical_json_bytes,
    write_bytes_atomic,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text, safe_load_yaml_text

DEFAULT_POLICY_PATH = Path("config/architecture/devx_006c_compatibility_authority.yaml")
INDEX_SCHEMA = "compatibility_authority_index.v1"
FRAGMENT_SCHEMA = "compatibility_authority_fragment.v1"
INVENTORY_SCHEMA = "compatibility_authority_consumer_inventory.v1"
SECTION_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_BLOB_PATTERN = re.compile(r"^[0-9a-f]{40}$")
LEGACY_REFERENCE = "arch_004_compatibility_baseline.yaml"

_POLICY_FIELDS = {
    "schema_version",
    "status",
    "task_id",
    "exact_start_base",
    "owner_decision",
    "legacy_prefix",
    "fragment_root",
    "index_path",
    "consumer_inventory_path",
    "contract",
    "production_effect",
    "broker_action",
}
_LEGACY_FIELDS = {
    "path",
    "byte_count",
    "file_sha256",
    "lf_sha256",
    "git_blob",
    "top_level_entry_count",
    "ordered_entry_ids_sha256",
    "mapping_replay_sha256",
    "grandfathered_duplicate_key_count",
    "grandfathered_duplicate_key_behavior",
}
_CONTRACT_FIELDS = {
    "legacy_append_allowed",
    "fragment_source_active",
    "dual_write",
    "fragment_identity",
    "index_chain",
    "rollback_mode",
}
_INDEX_FIELDS = {
    "schema_version",
    "task_id",
    "legacy_prefix",
    "genesis_chain_sha256",
    "entries",
    "final_chain_sha256",
    "contract",
}
_ENTRY_FIELDS = {
    "order",
    "section_id",
    "section_sha256",
    "fragment_path",
    "fragment_sha256",
    "previous_entry_sha256",
    "entry_sha256",
}
_FRAGMENT_FIELDS = {"schema_version", "section_id", "section_sha256", "section"}


class CompatibilityAuthorityError(ValueError):
    """Typed fail-closed compatibility authority contract failure."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _fail(code: str, detail: object) -> NoReturn:
    raise CompatibilityAuthorityError(code, str(detail))


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        _fail("AUTHORITY_MAPPING_REQUIRED", label)
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        _fail(
            "AUTHORITY_FIELDS_INVALID",
            f"{label}: expected={sorted(expected)} actual={sorted(value)}",
        )


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        _fail("AUTHORITY_STRING_REQUIRED", label)
    return value


def _sha256(value: object, label: str) -> str:
    result = _string(value, label)
    if SHA256_PATTERN.fullmatch(result) is None:
        _fail("AUTHORITY_SHA256_INVALID", label)
    return result


def _git_blob(value: object, label: str) -> str:
    result = _string(value, label)
    if GIT_BLOB_PATTERN.fullmatch(result) is None:
        _fail("AUTHORITY_GIT_BLOB_INVALID", label)
    return result


def _positive_or_zero_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail("AUTHORITY_INTEGER_INVALID", label)
    return value


def _canonical_mapping_bytes(value: Mapping[str, Any], *, sort_keys: bool) -> bytes:
    return canonical_json_bytes(
        value,
        sort_keys=sort_keys,
        indent=None,
        ensure_ascii=False,
        default=_json_default,
    )


def _json_default(value: object) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported compatibility value type: {type(value).__name__}")


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _git_blob_id(content: bytes) -> str:
    header = f"blob {len(content)}\0".encode("ascii")
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def _strict_json_bytes(content: bytes, label: str) -> dict[str, Any]:
    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("AUTHORITY_JSON_DUPLICATE_KEY", f"{label}:{key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> NoReturn:
        _fail("AUTHORITY_JSON_NON_FINITE", f"{label}:{value}")

    try:
        decoded = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CompatibilityAuthorityError("AUTHORITY_UTF8_INVALID", label) from exc
    try:
        value = json.loads(
            decoded,
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except CompatibilityAuthorityError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise CompatibilityAuthorityError("AUTHORITY_JSON_INVALID", label) from exc
    return _mapping(value, label)


def _portable_path(value: object, label: str) -> str:
    portable = _string(value, label)
    parsed = PurePosixPath(portable)
    if (
        parsed.is_absolute()
        or "\\" in portable
        or not parsed.parts
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or ":" in parsed.parts[0]
    ):
        _fail("AUTHORITY_PATH_INVALID", f"{label}:{portable}")
    return portable


def _regular_path(root: Path, portable: str, label: str) -> Path:
    root = root.resolve()
    cursor = root
    for part in PurePosixPath(portable).parts:
        cursor = cursor / part
        if cursor.is_symlink():
            _fail("AUTHORITY_PATH_SYMLINK", f"{label}:{portable}")
    try:
        resolved = cursor.resolve(strict=True)
    except FileNotFoundError as exc:
        raise CompatibilityAuthorityError("AUTHORITY_FILE_MISSING", portable) from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CompatibilityAuthorityError("AUTHORITY_PATH_ESCAPE", portable) from exc
    if not resolved.is_file():
        _fail("AUTHORITY_REGULAR_FILE_REQUIRED", f"{label}:{portable}")
    return resolved


def load_compatibility_policy(
    repository_root: Path = Path("."),
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    root = repository_root.resolve()
    portable = _portable_path(policy_path.as_posix(), "policy_path")
    path = _regular_path(root, portable, "policy")
    try:
        value = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=portable)
    except Exception as exc:
        if isinstance(exc, CompatibilityAuthorityError):
            raise
        raise CompatibilityAuthorityError("AUTHORITY_POLICY_INVALID", portable) from exc
    policy = _mapping(value, "policy")
    _exact_keys(policy, _POLICY_FIELDS, "policy")
    if policy["schema_version"] != "devx_006c_compatibility_authority_policy.v1":
        _fail("AUTHORITY_POLICY_SCHEMA", policy["schema_version"])
    if policy["status"] != "ACTIVE":
        _fail("AUTHORITY_POLICY_INACTIVE", policy["status"])
    _string(policy["task_id"], "task_id")
    if GIT_BLOB_PATTERN.fullmatch(_string(policy["exact_start_base"], "exact_start_base")) is None:
        _fail("AUTHORITY_BASE_INVALID", policy["exact_start_base"])
    _string(policy["owner_decision"], "owner_decision")
    legacy = _mapping(policy["legacy_prefix"], "legacy_prefix")
    _validate_legacy_seal_fields(legacy)
    _portable_path(policy["fragment_root"], "fragment_root")
    _portable_path(policy["index_path"], "index_path")
    _portable_path(policy["consumer_inventory_path"], "consumer_inventory_path")
    contract = _mapping(policy["contract"], "contract")
    _validate_contract(contract)
    if policy["production_effect"] != "none" or policy["broker_action"] != "none":
        _fail("AUTHORITY_PRODUCTION_BOUNDARY", "expected none/none")
    return policy


def _validate_legacy_seal_fields(legacy: Mapping[str, Any]) -> None:
    _exact_keys(legacy, _LEGACY_FIELDS, "legacy_prefix")
    _portable_path(legacy["path"], "legacy_prefix.path")
    _positive_or_zero_int(legacy["byte_count"], "legacy_prefix.byte_count")
    _sha256(legacy["file_sha256"], "legacy_prefix.file_sha256")
    _sha256(legacy["lf_sha256"], "legacy_prefix.lf_sha256")
    _git_blob(legacy["git_blob"], "legacy_prefix.git_blob")
    _positive_or_zero_int(legacy["top_level_entry_count"], "legacy_prefix.top_level_entry_count")
    _sha256(
        legacy["ordered_entry_ids_sha256"],
        "legacy_prefix.ordered_entry_ids_sha256",
    )
    _sha256(legacy["mapping_replay_sha256"], "legacy_prefix.mapping_replay_sha256")
    duplicate_count = _positive_or_zero_int(
        legacy["grandfathered_duplicate_key_count"],
        "legacy_prefix.grandfathered_duplicate_key_count",
    )
    duplicate_behavior = legacy["grandfathered_duplicate_key_behavior"]
    expected_behavior = "PYYAML_SAFE_LOADER_LAST_VALUE_WINS" if duplicate_count else "NONE"
    if duplicate_behavior != expected_behavior:
        _fail("AUTHORITY_LEGACY_DUPLICATE_CONTRACT", duplicate_count)


def _validate_contract(contract: Mapping[str, Any]) -> None:
    _exact_keys(contract, _CONTRACT_FIELDS, "contract")
    expected = {
        "legacy_append_allowed": False,
        "fragment_source_active": True,
        "dual_write": False,
        "fragment_identity": "CANONICAL_SECTION_SHA256",
        "index_chain": "SHA256",
        "rollback_mode": "FROZEN_LEGACY_PREFIX_ONLY",
    }
    if dict(contract) != expected:
        _fail("AUTHORITY_CONTRACT_INVALID", contract)


def load_immutable_compatibility_prefix_bytes(content: bytes) -> dict[str, Any]:
    """Parse explicitly frozen legacy bytes with their grandfathered YAML semantics."""

    try:
        value = safe_load_yaml_text(content.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise CompatibilityAuthorityError("AUTHORITY_LEGACY_PARSE_INVALID", "bytes") from exc
    return _mapping(value, "legacy_prefix")


def load_immutable_compatibility_prefix(
    repository_root: Path = Path("."),
    *,
    policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = repository_root.resolve()
    active_policy = dict(policy or load_compatibility_policy(root))
    legacy = _mapping(active_policy["legacy_prefix"], "legacy_prefix")
    portable = _portable_path(legacy["path"], "legacy_prefix.path")
    content = _regular_path(root, portable, "legacy_prefix").read_bytes()
    if len(content) != legacy["byte_count"]:
        _fail("AUTHORITY_LEGACY_SIZE_DRIFT", portable)
    if _digest(content) != legacy["file_sha256"]:
        _fail("AUTHORITY_LEGACY_HASH_DRIFT", portable)
    if _digest(content.replace(b"\r\n", b"\n")) != legacy["lf_sha256"]:
        _fail("AUTHORITY_LEGACY_LF_HASH_DRIFT", portable)
    if _git_blob_id(content) != legacy["git_blob"]:
        _fail("AUTHORITY_LEGACY_GIT_BLOB_DRIFT", portable)
    duplicate_count = _yaml_duplicate_key_count(content, portable)
    if duplicate_count != legacy["grandfathered_duplicate_key_count"]:
        _fail("AUTHORITY_LEGACY_DUPLICATE_COUNT_DRIFT", portable)
    baseline = load_immutable_compatibility_prefix_bytes(content)
    entry_ids = list(baseline)
    if len(entry_ids) != legacy["top_level_entry_count"]:
        _fail("AUTHORITY_LEGACY_ENTRY_COUNT_DRIFT", portable)
    ids_sha = _digest(("\n".join(entry_ids) + "\n").encode("utf-8"))
    if ids_sha != legacy["ordered_entry_ids_sha256"]:
        _fail("AUTHORITY_LEGACY_ORDER_DRIFT", portable)
    replay_sha = _digest(_canonical_mapping_bytes(baseline, sort_keys=False))
    if replay_sha != legacy["mapping_replay_sha256"]:
        _fail("AUTHORITY_LEGACY_REPLAY_DRIFT", portable)
    return baseline


def _yaml_duplicate_key_count(content: bytes, label: str) -> int:
    try:
        root_node = yaml.compose(content.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise CompatibilityAuthorityError("AUTHORITY_LEGACY_PARSE_INVALID", label) from exc
    duplicates = 0

    def visit(node: yaml.Node | None) -> None:
        nonlocal duplicates
        if isinstance(node, yaml.MappingNode):
            seen: set[str] = set()
            for key_node, value_node in node.value:
                key = str(getattr(key_node, "value", ""))
                if key in seen:
                    duplicates += 1
                else:
                    seen.add(key)
                visit(value_node)
        elif isinstance(node, yaml.SequenceNode):
            for child in node.value:
                visit(child)

    visit(root_node)
    return duplicates


def _genesis_chain(legacy: Mapping[str, Any]) -> str:
    return _digest(_canonical_mapping_bytes(legacy, sort_keys=True))


def _entry_hash(entry_without_hash: Mapping[str, Any]) -> str:
    return _digest(_canonical_mapping_bytes(entry_without_hash, sort_keys=True))


def render_fragment(
    *,
    section_id: str,
    section: Mapping[str, Any],
) -> tuple[str, dict[str, Any], bytes]:
    if SECTION_ID_PATTERN.fullmatch(section_id) is None:
        _fail("AUTHORITY_SECTION_ID_INVALID", section_id)
    section_value = dict(section)
    section_sha = _digest(_canonical_mapping_bytes(section_value, sort_keys=True))
    record: dict[str, Any] = {
        "schema_version": FRAGMENT_SCHEMA,
        "section_id": section_id,
        "section_sha256": section_sha,
        "section": section_value,
    }
    content = canonical_json_bytes(record, sort_keys=True, indent=2, ensure_ascii=False)
    relative = f"{section_sha[:2]}/{section_sha}.json"
    return relative, record, content


def render_index(
    *,
    policy: Mapping[str, Any],
    fragments: Sequence[tuple[str, str, Mapping[str, Any], bytes]],
) -> tuple[dict[str, Any], bytes]:
    legacy = _mapping(policy["legacy_prefix"], "legacy_prefix")
    previous = _genesis_chain(legacy)
    entries: list[dict[str, Any]] = []
    fragment_root = _portable_path(policy["fragment_root"], "fragment_root")
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    for order, (section_id, relative, record, content) in enumerate(fragments):
        if section_id in seen_ids:
            _fail("AUTHORITY_DUPLICATE_SECTION_ID", section_id)
        path = f"{fragment_root}/{relative}"
        if path in seen_paths:
            _fail("AUTHORITY_DUPLICATE_FRAGMENT_PATH", path)
        seen_ids.add(section_id)
        seen_paths.add(path)
        entry_without_hash: dict[str, Any] = {
            "order": order,
            "section_id": section_id,
            "section_sha256": _sha256(record["section_sha256"], "section_sha256"),
            "fragment_path": path,
            "fragment_sha256": _digest(content),
            "previous_entry_sha256": previous,
        }
        current = _entry_hash(entry_without_hash)
        entries.append({**entry_without_hash, "entry_sha256": current})
        previous = current
    index: dict[str, Any] = {
        "schema_version": INDEX_SCHEMA,
        "task_id": policy["task_id"],
        "legacy_prefix": dict(legacy),
        "genesis_chain_sha256": _genesis_chain(legacy),
        "entries": entries,
        "final_chain_sha256": previous,
        "contract": dict(policy["contract"]),
    }
    return index, canonical_json_bytes(index, sort_keys=True, indent=2, ensure_ascii=False)


def _load_index(root: Path, policy: Mapping[str, Any]) -> dict[str, Any]:
    portable = _portable_path(policy["index_path"], "index_path")
    content = _regular_path(root, portable, "index").read_bytes()
    index = _strict_json_bytes(content, portable)
    if content != canonical_json_bytes(index, sort_keys=True, indent=2, ensure_ascii=False):
        _fail("AUTHORITY_INDEX_NON_CANONICAL", portable)
    _exact_keys(index, _INDEX_FIELDS, "index")
    if index["schema_version"] != INDEX_SCHEMA or index["task_id"] != policy["task_id"]:
        _fail("AUTHORITY_INDEX_IDENTITY", portable)
    legacy = _mapping(index["legacy_prefix"], "index.legacy_prefix")
    _validate_legacy_seal_fields(legacy)
    if legacy != policy["legacy_prefix"]:
        _fail("AUTHORITY_INDEX_LEGACY_DRIFT", portable)
    contract = _mapping(index["contract"], "index.contract")
    _validate_contract(contract)
    if contract != policy["contract"]:
        _fail("AUTHORITY_INDEX_CONTRACT_DRIFT", portable)
    expected_genesis = _genesis_chain(legacy)
    if index["genesis_chain_sha256"] != expected_genesis:
        _fail("AUTHORITY_INDEX_GENESIS_DRIFT", portable)
    entries = index["entries"]
    if not isinstance(entries, list):
        _fail("AUTHORITY_INDEX_ENTRIES_INVALID", portable)
    previous = expected_genesis
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    for order, raw_entry in enumerate(entries):
        entry = _mapping(raw_entry, f"entry[{order}]")
        _exact_keys(entry, _ENTRY_FIELDS, f"entry[{order}]")
        if entry["order"] != order:
            _fail("AUTHORITY_INDEX_ORDER_INVALID", order)
        section_id = _string(entry["section_id"], f"entry[{order}].section_id")
        if SECTION_ID_PATTERN.fullmatch(section_id) is None:
            _fail("AUTHORITY_SECTION_ID_INVALID", section_id)
        fragment_path = _portable_path(entry["fragment_path"], f"entry[{order}].fragment_path")
        if section_id in seen_ids:
            _fail("AUTHORITY_DUPLICATE_SECTION_ID", section_id)
        if fragment_path in seen_paths:
            _fail("AUTHORITY_DUPLICATE_FRAGMENT_PATH", fragment_path)
        seen_ids.add(section_id)
        seen_paths.add(fragment_path)
        _sha256(entry["section_sha256"], f"entry[{order}].section_sha256")
        _sha256(entry["fragment_sha256"], f"entry[{order}].fragment_sha256")
        if entry["previous_entry_sha256"] != previous:
            _fail("AUTHORITY_INDEX_CHAIN_BROKEN", order)
        entry_without_hash = {key: entry[key] for key in _ENTRY_FIELDS - {"entry_sha256"}}
        expected_entry_hash = _entry_hash(entry_without_hash)
        if entry["entry_sha256"] != expected_entry_hash:
            _fail("AUTHORITY_INDEX_ENTRY_HASH_DRIFT", order)
        previous = expected_entry_hash
    if index["final_chain_sha256"] != previous:
        _fail("AUTHORITY_INDEX_FINAL_CHAIN_DRIFT", portable)
    return index


def _load_fragment(root: Path, entry: Mapping[str, Any]) -> dict[str, Any]:
    portable = _portable_path(entry["fragment_path"], "fragment_path")
    content = _regular_path(root, portable, "fragment").read_bytes()
    if _digest(content) != entry["fragment_sha256"]:
        _fail("AUTHORITY_FRAGMENT_HASH_DRIFT", portable)
    record = _strict_json_bytes(content, portable)
    if content != canonical_json_bytes(record, sort_keys=True, indent=2, ensure_ascii=False):
        _fail("AUTHORITY_FRAGMENT_NON_CANONICAL", portable)
    _exact_keys(record, _FRAGMENT_FIELDS, "fragment")
    if record["schema_version"] != FRAGMENT_SCHEMA:
        _fail("AUTHORITY_FRAGMENT_SCHEMA", portable)
    if record["section_id"] != entry["section_id"]:
        _fail("AUTHORITY_FRAGMENT_SECTION_ID_DRIFT", portable)
    section = _mapping(record["section"], f"{portable}.section")
    section_sha = _digest(_canonical_mapping_bytes(section, sort_keys=True))
    if record["section_sha256"] != section_sha or entry["section_sha256"] != section_sha:
        _fail("AUTHORITY_FRAGMENT_SECTION_HASH_DRIFT", portable)
    fragment_root = PurePosixPath("registry/architecture_compatibility_authority/fragments")
    expected = fragment_root / section_sha[:2] / f"{section_sha}.json"
    if PurePosixPath(portable) != expected:
        _fail("AUTHORITY_FRAGMENT_CONTENT_ADDRESS_DRIFT", portable)
    return section


def load_compatibility_authority(
    repository_root: Path = Path("."),
    *,
    include_fragments: bool | None = None,
) -> dict[str, Any]:
    root = repository_root.resolve()
    policy = load_compatibility_policy(root)
    baseline = load_immutable_compatibility_prefix(root, policy=policy)
    contract = _mapping(policy["contract"], "contract")
    use_fragments = (
        contract["fragment_source_active"] if include_fragments is None else include_fragments
    )
    if not use_fragments:
        return baseline
    index = _load_index(root, policy)
    for entry in index["entries"]:
        section_id = entry["section_id"]
        if section_id in baseline:
            _fail("AUTHORITY_SECTION_COLLIDES_WITH_LEGACY", section_id)
        baseline[section_id] = _load_fragment(root, entry)
    return baseline


def build_repository_authority(
    repository_root: Path = Path("."),
    *,
    write: bool,
) -> dict[str, Any]:
    root = repository_root.resolve()
    policy = load_compatibility_policy(root)
    legacy_baseline = load_immutable_compatibility_prefix(root, policy=policy)
    inventory = build_consumer_inventory(root, policy=policy)
    if inventory["status"] != "PASS":
        _fail("AUTHORITY_CONSUMER_CUTOVER_INCOMPLETE", inventory["status"])
    inventory_bytes = canonical_json_bytes(
        inventory,
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
    )
    section_id = "phase_devx_006c_compatibility_authority_fragmentation"
    c_source_paths = [
        "config/architecture/devx_006c_compatibility_authority.yaml",
        "docs/requirements/DEVX-006_Fragmented_Generated_Authority_and_Stable_Task_Shadow_v2.md",
        "docs/requirements/DEVX-006C_Compatibility_Authority_Fragmentation.md",
        "docs/task_register.md",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "inputs/architecture/arch_005_task_shadow_v2_index.yaml",
        (
            "registry/development_tasks_shadow/active/6d/"
            "6d8d0da0de81f10e89f99dc55533b0f53ac3787dcc59a693cb4ed8ca19f67fc1.yaml"
        ),
        (
            "registry/development_tasks_shadow/active/98/"
            "989bc4bfe58706d37f7b749b47ba03259688afcb2cac1cdf1fafb35b290130af.yaml"
        ),
        (
            "registry/development_tasks_shadow_v2/6d/"
            "6d8d0da0de81f10e89f99dc55533b0f53ac3787dcc59a693cb4ed8ca19f67fc1.yaml"
        ),
        (
            "registry/development_tasks_shadow_v2/98/"
            "989bc4bfe58706d37f7b749b47ba03259688afcb2cac1cdf1fafb35b290130af.yaml"
        ),
        "scripts/architecture_compatibility_authority.py",
        "src/ai_trading_system/external_request_cache_revalidation_coordination.py",
        "src/ai_trading_system/platform/architecture/bootstrap_handoff.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/atlas/test_historical_source_adapters.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_external_request_cache_revalidation_coordination.py",
        "tests/test_trading2452_architecture_contract.py",
    ]
    section: dict[str, Any] = {
        "schema_version": "devx_006c_compatibility_authority_fragmentation.v1",
        "task_id": policy["task_id"],
        "status": "ACTIVE",
        "exact_start_base": policy["exact_start_base"],
        "owner_decision": policy["owner_decision"],
        "legacy_prefix_sha256": policy["legacy_prefix"]["file_sha256"],
        "authority_contract": dict(policy["contract"]),
        "consumer_contract": {
            "current_authority_loader": (
                "ai_trading_system.platform.architecture.compatibility_authority."
                "load_compatibility_authority"
            ),
            "legacy_prefix_reader": (
                "ai_trading_system.platform.architecture.compatibility_authority."
                "load_immutable_compatibility_prefix"
            ),
            "growth_assuming_direct_consumer_count": 0,
            "runtime_legacy_append_writer_count": 0,
        },
        "superseded_live_source_paths": c_source_paths,
        "sources": [_source_record(root, path) for path in c_source_paths],
        "supersession": {
            "historical_hashes_rewritten": False,
            "current_hash_authority": f"{section_id}.sources",
        },
        "generated_fragment_authority": _task_shadow_fragment_authority(
            root,
            legacy_baseline,
        ),
        "production_effect": policy["production_effect"],
        "broker_action": policy["broker_action"],
    }
    relative, record, fragment_bytes = render_fragment(
        section_id=section_id,
        section=section,
    )
    rendered_fragments = [(section_id, relative, record, fragment_bytes)]
    latest_source_paths = c_source_paths
    d_policy_path = root / "config/architecture/devx_006d_report_catalog_flow_authority.yaml"
    if d_policy_path.exists():
        d_section_id, d_section = _devx_006d_section(
            root,
            policy=policy,
            legacy_baseline=legacy_baseline,
            inherited_source_paths=c_source_paths,
        )
        d_relative, d_record, d_fragment_bytes = render_fragment(
            section_id=d_section_id,
            section=d_section,
        )
        rendered_fragments.append((d_section_id, d_relative, d_record, d_fragment_bytes))
        latest_source_paths = list(d_section["superseded_live_source_paths"])
    s5_policy_path = root / "config/architecture/arch_005_s5_task_source_cutover.yaml"
    if s5_policy_path.exists():
        s5_section_id, s5_section = _arch_005_s5_section(
            root,
            policy=policy,
            inherited_source_paths=latest_source_paths,
        )
        s5_relative, s5_record, s5_fragment_bytes = render_fragment(
            section_id=s5_section_id,
            section=s5_section,
        )
        rendered_fragments.append((s5_section_id, s5_relative, s5_record, s5_fragment_bytes))
    index, index_bytes = render_index(
        policy=policy,
        fragments=rendered_fragments,
    )
    fragment_paths = [
        Path(policy["fragment_root"]) / Path(fragment[1]) for fragment in rendered_fragments
    ]
    latest_section_id, _, latest_record, latest_fragment_bytes = rendered_fragments[-1]
    fragment_path = fragment_paths[-1]
    index_path = Path(policy["index_path"])
    inventory_path = Path(policy["consumer_inventory_path"])
    if write:
        for path, rendered in zip(
            fragment_paths,
            (fragment[3] for fragment in rendered_fragments),
            strict=True,
        ):
            write_bytes_atomic(root / path, rendered)
        write_bytes_atomic(root / index_path, index_bytes)
        write_bytes_atomic(root / inventory_path, inventory_bytes)
        fragment_root = (root / _portable_path(policy["fragment_root"], "fragment_root")).resolve()
        fragment_root.relative_to(root)
        expected_fragments = {(root / path).resolve() for path in fragment_paths}
        for stale in sorted(fragment_root.rglob("*.json")):
            if stale.resolve() not in expected_fragments:
                stale.unlink()
    return {
        "status": "PASS",
        "fragment_path": fragment_path.as_posix(),
        "fragment_sha256": _digest(latest_fragment_bytes),
        "fragment_count": len(rendered_fragments),
        "index_path": index_path.as_posix(),
        "index_sha256": _digest(index_bytes),
        "consumer_inventory_path": inventory_path.as_posix(),
        "consumer_inventory_sha256": _digest(inventory_bytes),
        "section_id": latest_section_id,
        "section_sha256": latest_record["section_sha256"],
        "index": index,
    }


def _source_record(root: Path, portable: str) -> dict[str, str]:
    content = _regular_path(root, portable, "source").read_bytes().replace(b"\r\n", b"\n")
    return {
        "path": portable,
        "sha256": _digest(content),
        "hash_normalization": "git_eol_lf",
    }


def _devx_006d_section(
    root: Path,
    *,
    policy: Mapping[str, Any],
    legacy_baseline: Mapping[str, Any],
    inherited_source_paths: Sequence[str],
) -> tuple[str, dict[str, Any]]:
    section_id = "phase_devx_006d_report_catalog_flow_lossless_fragmentation"
    d_policy_path = "config/architecture/devx_006d_report_catalog_flow_authority.yaml"
    d_policy = _mapping(
        load_strict_yaml_text(
            _regular_path(root, d_policy_path, "devx_006d_policy").read_text(encoding="utf-8"),
            label=d_policy_path,
        ),
        "devx_006d_policy",
    )
    d_index_path = _portable_path(d_policy.get("index_path"), "devx_006d.index_path")
    d_inventory_path = _portable_path(
        d_policy.get("consumer_inventory_path"),
        "devx_006d.consumer_inventory_path",
    )
    d_index_content = _regular_path(root, d_index_path, "devx_006d_index").read_bytes()
    d_inventory_content = _regular_path(
        root,
        d_inventory_path,
        "devx_006d_inventory",
    ).read_bytes()
    d_index = _strict_json_bytes(d_index_content, d_index_path)
    d_inventory = _strict_json_bytes(d_inventory_content, d_inventory_path)
    if d_index.get("status") != "PASS" or d_index.get("source_of_truth") != ("LEGACY_MONOLITH"):
        _fail("AUTHORITY_DEVX_006D_INDEX_INVALID", d_index_path)
    if d_inventory.get("status") != "PASS" or d_inventory.get("cutover_ready") is not False:
        _fail("AUTHORITY_DEVX_006D_INVENTORY_INVALID", d_inventory_path)
    raw_targets = d_index.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        _fail("AUTHORITY_DEVX_006D_TARGETS_INVALID", d_index_path)
    target_summary: list[dict[str, Any]] = []
    fragment_count = 0
    entry_count = 0
    for position, raw_target in enumerate(raw_targets):
        target = _mapping(raw_target, f"devx_006d.targets[{position}]")
        source_seal = _mapping(
            target.get("source_seal"),
            f"devx_006d.targets[{position}].source_seal",
        )
        target_entry_count = _positive_or_zero_int(
            target.get("entry_count"),
            f"devx_006d.targets[{position}].entry_count",
        )
        target_fragment_count = _positive_or_zero_int(
            target.get("fragment_count"),
            f"devx_006d.targets[{position}].fragment_count",
        )
        entry_count += target_entry_count
        fragment_count += target_fragment_count
        target_summary.append(
            {
                "target_id": _string(target.get("target_id"), "devx_006d.target_id"),
                "path": _portable_path(target.get("path"), "devx_006d.target.path"),
                "file_sha256": _sha256(
                    source_seal.get("file_sha256"),
                    "devx_006d.target.file_sha256",
                ),
                "byte_count": _positive_or_zero_int(
                    source_seal.get("byte_count"),
                    "devx_006d.target.byte_count",
                ),
                "entry_count": target_entry_count,
                "fragment_count": target_fragment_count,
                "coverage_percent": _positive_or_zero_int(
                    target.get("coverage_percent"),
                    "devx_006d.target.coverage_percent",
                ),
            }
        )
    d_source_paths = [
        d_policy_path,
        "config/report_registry.yaml",
        "docs/artifact_catalog.md",
        "docs/requirements/DEVX-006_Fragmented_Generated_Authority_and_Stable_Task_Shadow_v2.md",
        "docs/requirements/DEVX-006D_Report_Catalog_Flow_Lossless_Fragmentation.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        d_index_path,
        d_inventory_path,
        "inputs/architecture/arch_004e_aggregate_shadow_index.yaml",
        "inputs/architecture/arch_004e_architecture_fitness.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_005_task_registry_baseline.yaml",
        "inputs/architecture/arch_005_task_shadow_index.yaml",
        "inputs/architecture/arch_005_task_shadow_v2_index.yaml",
        "scripts/architecture_report_catalog_flow_authority.py",
        "src/ai_trading_system/platform/architecture/compatibility_authority.py",
        "src/ai_trading_system/platform/architecture/report_catalog_flow_authority.py",
        "tests/test_arch_004_refactor_policy.py",
        "tests/test_arch_004g_deprecation.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_devx_006d_report_catalog_flow_authority.py",
    ]
    source_paths = list(dict.fromkeys([*inherited_source_paths, *d_source_paths]))
    return section_id, {
        "schema_version": "devx_006d_report_catalog_flow_lossless_fragmentation.v1",
        "task_id": _string(d_policy.get("task_id"), "devx_006d.task_id"),
        "status": "INACTIVE_SHADOW",
        "exact_start_base": _string(
            d_policy.get("exact_start_base"),
            "devx_006d.exact_start_base",
        ),
        "owner_decision": _string(
            d_policy.get("owner_decision"),
            "devx_006d.owner_decision",
        ),
        "legacy_prefix_sha256": policy["legacy_prefix"]["file_sha256"],
        "authority_contract": dict(_mapping(policy["contract"], "contract")),
        "report_catalog_flow_fragment_authority": {
            "source_of_truth": "LEGACY_MONOLITH",
            "fragment_shadow_active": False,
            "index_path": d_index_path,
            "index_sha256": _digest(d_index_content),
            "consumer_inventory_path": d_inventory_path,
            "consumer_inventory_sha256": _digest(d_inventory_content),
            "target_count": len(target_summary),
            "entry_count": entry_count,
            "fragment_count": fragment_count,
            "targets": target_summary,
            "rollback_mode": "IGNORE_INACTIVE_SHADOW",
        },
        "consumer_contract": {
            "inventory_status": d_inventory["status"],
            "consumer_count": _positive_or_zero_int(
                d_inventory.get("consumer_count"),
                "devx_006d.consumer_count",
            ),
            "pending_owner_cutover_count": _positive_or_zero_int(
                d_inventory.get("pending_owner_cutover_count"),
                "devx_006d.pending_owner_cutover_count",
            ),
            "cutover_ready": False,
        },
        "superseded_live_source_paths": source_paths,
        "sources": [_source_record(root, path) for path in source_paths],
        "supersession": {
            "historical_hashes_rewritten": False,
            "current_hash_authority": f"{section_id}.sources",
        },
        "generated_fragment_authority": _task_shadow_fragment_authority(
            root,
            legacy_baseline,
        ),
        "production_effect": d_policy.get("production_effect"),
        "broker_action": d_policy.get("broker_action"),
    }


def _task_shadow_fragment_authority(
    root: Path,
    legacy_baseline: Mapping[str, Any],
) -> dict[str, Any]:
    legacy_latest = _mapping(legacy_baseline[list(legacy_baseline)[-1]], "legacy_latest")
    inherited = dict(
        _mapping(
            legacy_latest.get("generated_fragment_authority"),
            "legacy_latest.generated_fragment_authority",
        )
    )
    index_path = _portable_path(inherited["index_path"], "task_shadow_v2.index_path")
    index = _mapping(
        load_strict_yaml_text(
            _regular_path(root, index_path, "task_shadow_v2_index").read_text(encoding="utf-8"),
            label=index_path,
        ),
        "task_shadow_v2_index",
    )
    fragments = index.get("fragments")
    fragment_count = _positive_or_zero_int(
        index.get("fragment_count"),
        "task_shadow_v2_index.fragment_count",
    )
    if index.get("status") != "PASS" or not isinstance(fragments, list):
        _fail("AUTHORITY_TASK_SHADOW_V2_INVALID", index_path)
    if len(fragments) != fragment_count:
        _fail("AUTHORITY_TASK_SHADOW_V2_COUNT_DRIFT", index_path)
    baseline_path = "inputs/architecture/arch_005_task_registry_baseline.yaml"
    task_baseline = _mapping(
        load_strict_yaml_text(
            _regular_path(root, baseline_path, "task_registry_baseline").read_text(
                encoding="utf-8"
            ),
            label=baseline_path,
        ),
        "task_registry_baseline",
    )
    inventory = _mapping(task_baseline.get("inventory"), "task_registry_baseline.inventory")
    inherited["fragment_count"] = fragment_count
    inherited["active_task_count"] = _positive_or_zero_int(
        inventory.get("active_task_count"),
        "task_registry_baseline.inventory.active_task_count",
    )
    inherited["completed_task_count"] = _positive_or_zero_int(
        inventory.get("completed_task_count"),
        "task_registry_baseline.inventory.completed_task_count",
    )
    return inherited


def _arch_005_s5_section(
    root: Path,
    *,
    policy: Mapping[str, Any],
    inherited_source_paths: Sequence[str],
) -> tuple[str, dict[str, Any]]:
    section_id = "phase_arch_005_s5_canonical_task_source_cutover"
    s5_policy_path = "config/architecture/arch_005_s5_task_source_cutover.yaml"
    s5_policy = _mapping(
        load_strict_yaml_text(
            _regular_path(root, s5_policy_path, "arch_005_s5_policy").read_text(encoding="utf-8"),
            label=s5_policy_path,
        ),
        "arch_005_s5_policy",
    )
    canonical = _mapping(s5_policy.get("canonical"), "arch_005_s5_policy.canonical")
    index_path = _portable_path(canonical.get("index_path"), "arch_005_s5.index_path")
    manifest_path = _portable_path(
        canonical.get("manifest_path"),
        "arch_005_s5.manifest_path",
    )
    inventory_path = _portable_path(
        canonical.get("consumer_inventory_path"),
        "arch_005_s5.consumer_inventory_path",
    )
    index_content = _regular_path(root, index_path, "arch_005_s5_index").read_bytes()
    manifest_content = _regular_path(root, manifest_path, "arch_005_s5_manifest").read_bytes()
    inventory_content = _regular_path(root, inventory_path, "arch_005_s5_inventory").read_bytes()
    index = _mapping(
        load_strict_yaml_text(index_content.decode("utf-8"), label=index_path),
        "arch_005_s5_index",
    )
    manifest = _mapping(
        load_strict_yaml_text(manifest_content.decode("utf-8"), label=manifest_path),
        "arch_005_s5_manifest",
    )
    inventory = _mapping(
        load_strict_yaml_text(inventory_content.decode("utf-8"), label=inventory_path),
        "arch_005_s5_inventory",
    )
    if (
        index.get("status") != "PASS"
        or index.get("source_of_truth") != "ARCH_005_TASK_REGISTRY"
        or index.get("cutover_performed") is not True
    ):
        _fail("AUTHORITY_ARCH_005_S5_INDEX_INVALID", index_path)
    if manifest.get("status") != "PASS" or manifest.get("source_of_truth_after") != (
        "ARCH_005_TASK_REGISTRY"
    ):
        _fail("AUTHORITY_ARCH_005_S5_MANIFEST_INVALID", manifest_path)
    if (
        inventory.get("status") != "PASS"
        or inventory.get("manual_semantic_runtime_consumer_count") != 0
        or inventory.get("manual_writer_count") != 0
    ):
        _fail("AUTHORITY_ARCH_005_S5_INVENTORY_INVALID", inventory_path)
    s5_source_paths = [
        "AGENTS.md",
        s5_policy_path,
        "config/architecture/arch_004_g2_5_readiness.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        "config/architecture/arch_005_supervised_automation_policy.yaml",
        "docs/artifact_catalog.md",
        "docs/requirements/ARCH-005_Parallel_Development_Control_Plane.md",
        "docs/requirements/ARCH-005S5_Canonical_Task_Source_Cutover.md",
        "docs/requirements/DEVX-006_Fragmented_Generated_Authority_and_Stable_Task_Shadow_v2.md",
        "docs/system_flow.md",
        "docs/task_register.md",
        "docs/task_register_completed.md",
        index_path,
        manifest_path,
        inventory_path,
        "inputs/architecture/arch_005_s5_active_view_template.md",
        "inputs/architecture/arch_005_s5_completed_view_template.md",
        "inputs/architecture/arch_005_s5_rollback_rehearsal/rollback_rehearsal.yaml",
        "inputs/architecture/arch_005_s5_rollback_rehearsal/task_register.md",
        "inputs/architecture/arch_005_s5_rollback_rehearsal/task_register_completed.md",
        "scripts/architecture_arch005_control_plane.py",
        "scripts/architecture_arch005_registry.py",
        "scripts/architecture_arch005_task_source.py",
        "src/ai_trading_system/platform/architecture/__init__.py",
        "src/ai_trading_system/platform/architecture/parallel_control_dispatch.py",
        "src/ai_trading_system/platform/architecture/parallel_control_kernel.py",
        "src/ai_trading_system/platform/architecture/parallel_control_scheduler.py",
        "src/ai_trading_system/platform/architecture/supervised_automation.py",
        "src/ai_trading_system/cli_commands/feedback.py",
        "src/ai_trading_system/cli_commands/reports.py",
        "src/ai_trading_system/platform/architecture/task_registry_canonical.py",
        "src/ai_trading_system/reports/research_roadmap_dashboard.py",
        "src/ai_trading_system/reports/research_safety_boundary.py",
        "src/ai_trading_system/reports/task_register_consistency.py",
        "tests/test_arch_005_s5_task_source_cutover.py",
        "tests/test_arch_005_s2_kernel.py",
        "tests/test_arch_005_s4_dispatch.py",
        "tests/test_arch_005_s4a_supervised_automation.py",
        "tests/test_devx_006c_compatibility_authority.py",
        "tests/test_arch_005_task_registry_shadow.py",
        "tests/test_trading2452_architecture_contract.py",
    ]
    source_paths = list(dict.fromkeys([*inherited_source_paths, *s5_source_paths]))
    retired_shadow_authorities = _retired_task_shadow_authorities(root)
    retired_shadow_fragment_paths = [
        path
        for authority in retired_shadow_authorities
        for path in authority["fragment_paths"]
    ]
    superseded_paths = list(
        dict.fromkeys([*source_paths, *retired_shadow_fragment_paths])
    )
    task_count = _positive_or_zero_int(index.get("task_count"), "arch_005_s5.task_count")
    fragment_count = _positive_or_zero_int(
        index.get("fragment_count"),
        "arch_005_s5.fragment_count",
    )
    if task_count != fragment_count:
        _fail("AUTHORITY_ARCH_005_S5_TASK_FRAGMENT_COUNT", index_path)
    if (
        index.get("manual_row_move_workflow_enabled") is not False
        or _positive_or_zero_int(
            index.get("governance_cycle_count"),
            "arch_005_s5.governance_cycle_count",
        )
        < 2
    ):
        _fail("AUTHORITY_ARCH_005_S5_SELF_HOST_INCOMPLETE", index_path)
    return section_id, {
        "schema_version": "arch_005_s5_canonical_task_source_cutover.v1",
        "task_id": _string(s5_policy.get("task_id"), "arch_005_s5.task_id"),
        "status": "ACTIVE",
        "exact_start_base": _string(
            s5_policy.get("exact_start_base"),
            "arch_005_s5.exact_start_base",
        ),
        "owner_decision": _string(
            s5_policy.get("owner_decision"),
            "arch_005_s5.owner_decision",
        ),
        "legacy_prefix_sha256": policy["legacy_prefix"]["file_sha256"],
        "authority_contract": dict(_mapping(policy["contract"], "contract")),
        "task_registry_authority": {
            "source_of_truth": "ARCH_005_TASK_REGISTRY",
            "cutover_performed": True,
            "index_path": index_path,
            "index_sha256": _digest(index_content),
            "manifest_path": manifest_path,
            "manifest_sha256": _digest(manifest_content),
            "consumer_inventory_path": inventory_path,
            "consumer_inventory_sha256": _digest(inventory_content),
            "fragment_root": _portable_path(
                canonical.get("fragment_root"),
                "arch_005_s5.fragment_root",
            ),
            "task_count": task_count,
            "active_task_count": _positive_or_zero_int(
                index.get("active_task_count"),
                "arch_005_s5.active_task_count",
            ),
            "completed_task_count": _positive_or_zero_int(
                index.get("completed_task_count"),
                "arch_005_s5.completed_task_count",
            ),
            "fragment_count": fragment_count,
            "governance_cycle_count": _positive_or_zero_int(
                index.get("governance_cycle_count"),
                "arch_005_s5.governance_cycle_count",
            ),
            "manual_row_move_workflow_enabled": index.get("manual_row_move_workflow_enabled"),
            "final_chain_sha256": _sha256(
                index.get("final_chain_sha256"),
                "arch_005_s5.final_chain_sha256",
            ),
            "rollback_mode": "OWNER_REVIEWED_LEGACY_COMPATIBLE_SNAPSHOT_ONLY",
        },
        "retired_shadow_authorities": retired_shadow_authorities,
        "consumer_contract": {
            "inventory_status": inventory["status"],
            "consumer_count": _positive_or_zero_int(
                inventory.get("consumer_count"),
                "arch_005_s5.consumer_count",
            ),
            "manual_semantic_runtime_consumer_count": 0,
            "manual_writer_count": 0,
        },
        "superseded_live_source_paths": superseded_paths,
        "sources": [_source_record(root, path) for path in source_paths],
        "supersession": {
            "historical_hashes_rewritten": False,
            "current_hash_authority": f"{section_id}.sources",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _retired_task_shadow_authorities(root: Path) -> list[dict[str, Any]]:
    specs = (
        (
            "ARCH_005_TASK_SHADOW_V1",
            "inputs/architecture/arch_005_task_shadow_index.yaml",
            "registry/development_tasks_shadow",
        ),
        (
            "ARCH_005_TASK_SHADOW_V2",
            "inputs/architecture/arch_005_task_shadow_v2_index.yaml",
            "registry/development_tasks_shadow_v2",
        ),
    )
    authorities: list[dict[str, Any]] = []
    for authority_id, index_path, fragment_root in specs:
        index_file = _regular_path(root, index_path, f"{authority_id}.index")
        index_content = index_file.read_bytes()
        index = _mapping(
            load_strict_yaml_text(index_content.decode("utf-8"), label=index_path),
            f"{authority_id}.index",
        )
        raw_records = index.get("fragments")
        if not isinstance(raw_records, list) or not raw_records:
            _fail("AUTHORITY_ARCH_005_S5_SHADOW_INDEX_INVALID", index_path)
        fragment_paths: list[str] = []
        for position, raw_record in enumerate(raw_records):
            record = _mapping(raw_record, f"{authority_id}.fragments[{position}]")
            path = _portable_path(
                record.get("path"),
                f"{authority_id}.fragments[{position}].path",
            )
            if not path.startswith(f"{fragment_root}/"):
                _fail("AUTHORITY_ARCH_005_S5_SHADOW_PATH_INVALID", path)
            _regular_path(root, path, f"{authority_id}.fragment")
            fragment_paths.append(path)
        if len(fragment_paths) != len(set(fragment_paths)):
            _fail("AUTHORITY_ARCH_005_S5_SHADOW_PATH_DUPLICATE", authority_id)
        authorities.append(
            {
                "authority_id": authority_id,
                "status": "IMMUTABLE_FINAL_IMPORT_EVIDENCE",
                "index_path": index_path,
                "index_sha256": _digest(index_content),
                "index_checksum": _sha256(
                    index.get("index_checksum"),
                    f"{authority_id}.index_checksum",
                ),
                "fragment_root": fragment_root,
                "fragment_count": len(fragment_paths),
                "fragment_hash_authority": "INDEX_TRANSITIVE_SHA256",
                "direct_fragment_hash_expansion": False,
                "fragment_paths": fragment_paths,
            }
        )
    return authorities


def validate_repository_authority(repository_root: Path = Path(".")) -> dict[str, Any]:
    root = repository_root.resolve()
    expected = build_repository_authority(root, write=False)
    for path_key, sha_key in (
        ("fragment_path", "fragment_sha256"),
        ("index_path", "index_sha256"),
        ("consumer_inventory_path", "consumer_inventory_sha256"),
    ):
        portable = expected[path_key]
        content = _regular_path(root, portable, path_key).read_bytes()
        if _digest(content) != expected[sha_key]:
            _fail("AUTHORITY_GENERATED_STALE", portable)
    merged = load_compatibility_authority(root)
    if next(reversed(merged)) != expected["section_id"]:
        _fail("AUTHORITY_LATEST_SECTION_DRIFT", expected["section_id"])
    return {key: value for key, value in expected.items() if key not in {"index"}}


def build_consumer_inventory(
    repository_root: Path,
    *,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    base = _string(policy["exact_start_base"], "exact_start_base")
    base_paths = _git_lines(
        repository_root,
        ["grep", "-l", "-F", LEGACY_REFERENCE, base, "--", "src", "scripts", "tests"],
        allow_no_match=True,
    )
    base_paths = [line.removeprefix(f"{base}:") for line in base_paths]
    current_paths: list[str] = []
    for root_name in ("src", "scripts", "tests"):
        scan_root = repository_root / root_name
        for path in sorted(scan_root.rglob("*.py")):
            portable = path.relative_to(repository_root).as_posix()
            if LEGACY_REFERENCE in path.read_text(encoding="utf-8"):
                current_paths.append(portable)
    records: list[dict[str, Any]] = []
    for portable in sorted(set(base_paths) | set(current_paths)):
        base_text = _git_text(repository_root, base, portable)
        path = repository_root / Path(portable)
        current_text = path.read_text(encoding="utf-8") if path.is_file() else ""
        role, reason = _consumer_role(portable, current_text)
        base_semantic, base_raw = _python_access_counts(base_text)
        current_semantic, current_raw = _python_access_counts(current_text)
        records.append(
            {
                "path": portable,
                "role": role,
                "base_reference_count": base_text.count(LEGACY_REFERENCE),
                "current_reference_count": current_text.count(LEGACY_REFERENCE),
                "base_growth_assuming_read_count": base_semantic,
                "current_growth_assuming_read_count": current_semantic,
                "base_raw_legacy_read_count": base_raw,
                "current_raw_legacy_read_count": current_raw,
                "needs_ordered_mapping": role
                in {
                    "HISTORICAL_PREFIX_AND_MERGED_AUTHORITY_TEST",
                    "MERGED_AUTHORITY_TEST",
                },
                "needs_raw_legacy_bytes": current_raw > 0
                or role in {"IMMUTABLE_PREFIX_SNAPSHOT", "IMMUTABLE_PREFIX_VALIDATOR"},
                "needs_current_hash_lookup": role
                in {
                    "HISTORICAL_PREFIX_AND_MERGED_AUTHORITY_TEST",
                    "MERGED_AUTHORITY_TEST",
                },
                "needs_append_only_prefix_assertion": current_raw > 0,
                "migration_adapter": _migration_adapter(role),
                "validation": "focused pytest + repository authority freshness",
                "rollback": "load frozen legacy prefix only; never back-write fragments",
                "migration_status": (
                    "MIGRATED" if current_semantic == 0 else "BLOCKED_DIRECT_GROWTH_READ"
                ),
                "remaining_direct_read_reason": reason,
            }
        )
    growth_count = sum(row["current_growth_assuming_read_count"] for row in records)
    return {
        "schema_version": INVENTORY_SCHEMA,
        "task_id": policy["task_id"],
        "exact_base": base,
        "scan_roots": ["scripts", "src", "tests"],
        "legacy_reference": LEGACY_REFERENCE,
        "consumer_count": len(records),
        "growth_assuming_direct_consumer_count": growth_count,
        "runtime_legacy_append_writer_count": 0,
        "fixture_legacy_writer_count": sum(
            1 for row in records if row["role"] == "TEST_FIXTURE_WRITER"
        ),
        "status": "PASS" if growth_count == 0 else "FAIL",
        "consumers": records,
    }


def _consumer_role(portable: str, source: str) -> tuple[str, str]:
    if portable == ("src/ai_trading_system/platform/architecture/compatibility_authority.py"):
        return (
            "CANONICAL_AUTHORITY_LOADER",
            "sealed legacy path is resolved only through the strict policy contract",
        )
    if portable == "tests/test_arch_004_refactor_policy.py":
        return (
            "HISTORICAL_PREFIX_AND_MERGED_AUTHORITY_TEST",
            "raw reads remain only for immutable historical-prefix byte assertions",
        )
    if portable == "tests/test_trading2452_architecture_contract.py":
        return "MERGED_AUTHORITY_TEST", "none"
    if portable == "tests/test_devx_006c_compatibility_authority.py":
        return (
            "COMPATIBILITY_CUTOVER_CONTRACT_TEST",
            "reads sealed legacy bytes only to prove exact-base parity",
        )
    if portable == "src/ai_trading_system/platform/architecture/bootstrap_handoff.py":
        return (
            "IMMUTABLE_PREFIX_VALIDATOR",
            "ARCH-005 bootstrap handoff validates exact frozen tracked legacy bytes",
        )
    if portable == "scripts/architecture_arch005_handoff.py":
        return (
            "IMMUTABLE_PREFIX_SNAPSHOT",
            "ARCH-005 handoff snapshots the frozen tracked legacy artifact",
        )
    if "write_generated_architecture_artifact" in source and portable.startswith("tests/"):
        return "TEST_FIXTURE_WRITER", "temporary fixture only; no repository authority write"
    return "GOVERNANCE_PATH_REFERENCE", "path allowlist/reference only; no authority read"


def _python_access_counts(source: str) -> tuple[int, int]:
    if not source:
        return 0, 0
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return 0, 0
    semantic = 0
    raw = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "safe_load_yaml_path"
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "COMPATIBILITY_BASELINE_PATH"
        ):
            semantic += 1
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "read_bytes"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "COMPATIBILITY_BASELINE_PATH"
        ):
            raw += 1
    return semantic, raw


def _migration_adapter(role: str) -> str:
    if role in {
        "HISTORICAL_PREFIX_AND_MERGED_AUTHORITY_TEST",
        "MERGED_AUTHORITY_TEST",
    }:
        return "load_compatibility_authority"
    if role == "COMPATIBILITY_CUTOVER_CONTRACT_TEST":
        return "load_compatibility_authority + explicit sealed-prefix parity"
    if role == "IMMUTABLE_PREFIX_VALIDATOR":
        return "load_immutable_compatibility_prefix_bytes"
    if role == "IMMUTABLE_PREFIX_SNAPSHOT":
        return "tracked immutable-prefix bytes"
    return "none"


def _git_lines(
    root: Path,
    arguments: list[str],
    *,
    allow_no_match: bool,
) -> list[str]:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode == 1 and allow_no_match:
        return []
    if result.returncode != 0:
        _fail("AUTHORITY_GIT_COMMAND_FAILED", result.stderr.strip())
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _git_text(root: Path, base: str, portable: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{base}:{portable}"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.decode("utf-8")


__all__ = [
    "CompatibilityAuthorityError",
    "build_consumer_inventory",
    "build_repository_authority",
    "load_compatibility_authority",
    "load_compatibility_policy",
    "load_immutable_compatibility_prefix",
    "load_immutable_compatibility_prefix_bytes",
    "render_fragment",
    "render_index",
    "validate_repository_authority",
]
